# -*- coding: utf-8 -*-
# services/realtime_orchestrator.py
# مانیتورینگ هوشمند لحظه‌ای بازار با کنترل نرخ درخواست، پردازش هم‌زمان، Retry و Caching

import asyncio
import pytse_client as tse
import requests
import logging
import functools
import random
import json
import traceback
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from extensions import db
from models import HistoricalData, GoldenKeyResult, WeeklyWatchlistResult, PotentialBuyQueueResult # اضافه شدن مدل‌ها
import jdatetime
import time
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Tuple, Callable, Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ---------------------------------------------------
# تنظیمات پایه و متغیرها
# ---------------------------------------------------
TEHRAN_TZ = ZoneInfo("Asia/Tehran")
REFRESH_INTERVAL = 3                # هر ۳ ثانیه داده‌ی لحظه‌ای سهم‌های منتخب
BRS_FETCH_INTERVAL_MIN = 3          # هر 3 دقیقه برای کل بازار
MARKET_CLOSE_TIME = "16:00"         # ساعت پایان بازار برای ذخیره EOD
THREAD_POOL_SIZE = 10               # تعداد تردها برای pytse-client

BRS_API_KEY = os.getenv("BRS_API_KEY", "MOCK_KEY") # استفاده از Mock_KEY در صورت عدم وجود
BRS_API_URL = f"https://BrsApi.ir/Api/Tsetmc/AllSymbols.php?key={BRS_API_KEY}&type=1"
BRS_TIMEOUT = 30
TSETMC_TIMEOUT = 10 

# ---------------------------------------------------
# ۱. پیاده‌سازی مکانیزم Retry
# ---------------------------------------------------
def retry_on_exception(max_retries=3, delay=5, backoff=2.0, exceptions=(Exception,)):
    """Decorator برای اجرای مجدد یک تابع در صورت بروز خطا."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries, wait = 0, delay
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries > 1: # فقط از تلاش دوم لاگ retry بزن
                        logger.warning(f"Error in {func.__name__}: {e}. Retrying {retries}/{max_retries} after {wait:.2f}s...")
                    time.sleep(wait + random.uniform(0, 1))
                    wait *= backoff
            # اگر تمام تلاش‌ها ناموفق بود، خطا را صادر کن
            raise
        return wrapper
    return decorator

# ---------------------------------------------------
# ۲. پیاده‌سازی Mock Redis برای Caching
# ---------------------------------------------------
class RedisMock:
    """Mock ساده برای Redis که داده‌های لحظه‌ای را در حافظه نگه‌ می‌دارد."""
    def __init__(self):
        self.cache: Dict[str, str] = {}
        # logger.info("Using RedisMock for real-time caching.") # این لاگ برای محیط Production زیاد است

    def set_realtime_data(self, key: str, data: List[Dict[str, Any]]):
        """ذخیره داده‌های لحظه‌ای نمادها در یک کلید اصلی."""
        # تبدیل Timestamp/datetime به string برای JSON serialization
        def default_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            return str(obj)
        
        try:
            self.cache[key] = json.dumps(data, default=default_serializer, ensure_ascii=False)
        except Exception as e:
            logger.error(f"❌ Failed to serialize data for RedisMock: {e}")

# ---------------------------------------------------
# ۳. واکشی نمادهای منتخب از دیتابیس (نکته درخواستی)
# ---------------------------------------------------
def get_selected_symbols(session: Session) -> List[str]:
    """خواند symbol_name از جداول GoldenKey, Watchlist و BuyQueue."""
    symbols = set()
    try:
        # 1. GoldenKeyResult (فقط فعال‌ها)
        gk_symbols = session.query(GoldenKeyResult.symbol_name).filter(GoldenKeyResult.status == 'active').distinct().all()
        symbols.update(s[0] for s in gk_symbols)

        # 2. WeeklyWatchlistResult (فقط فعال‌ها)
        ww_symbols = session.query(WeeklyWatchlistResult.symbol_name).filter(WeeklyWatchlistResult.status == 'active').distinct().all()
        symbols.update(s[0] for s in ww_symbols)

        # 3. PotentialBuyQueueResult 
        pb_symbols = session.query(PotentialBuyQueueResult.symbol_name).distinct().all()
        symbols.update(s[0] for s in pb_symbols)
        
        if not symbols:
            logger.warning("⚠️ No active signals found in DB. Using fallback symbols for monitoring.")
            symbols.update(["خودرو", "فولاد", "شستا"])
        else:
            logger.info(f"✅ Loaded {len(symbols)} unique symbols from DB tables.")

    except Exception as e:
        logger.error(f"❌ Failed to fetch selected symbols from DB: {e}. Falling back to defaults.")
        # Fallback list in case of DB error
        symbols.update(["نوری", "فولاد", "خودرو", "وبملت", "شستا"])
        
    return sorted(list(symbols))

# ---------------------------------------------------
# ۴. واکشی داده‌های BrsApi.ir (کل بازار) - بدون تغییر عمده در منطق Retry
# ---------------------------------------------------
class BrsMarketFetcher:
    # ... (همانند قبل)
    def __init__(self):
        self.last_fetch_time = None
        self.cache: Dict[str, Any] = {}

    def should_fetch(self) -> bool:
        if self.last_fetch_time is None:
            return True
        # استفاده از datetime.now() بدون TZ در مقایسه با last_fetch_time (که با TZ است) کمی مشکل دارد. 
        # برای دقت، هر دو باید همسان باشند.
        now_tehran = datetime.now(TEHRAN_TZ)
        if self.last_fetch_time is None:
            return True
        # مقایسه با حذف TZ برای محاسبات ساده‌تر
        return (now_tehran.replace(tzinfo=None) - self.last_fetch_time.replace(tzinfo=None)) >= timedelta(minutes=BRS_FETCH_INTERVAL_MIN)

    @retry_on_exception(max_retries=3, delay=10, exceptions=(requests.RequestException,))
    def _fetch_with_retry(self) -> dict:
        response = requests.get(BRS_API_URL, timeout=BRS_TIMEOUT)
        response.raise_for_status() 
        return {item['l18']: item for item in response.json()}


    def fetch(self) -> dict:
        if not self.should_fetch():
            return self.cache

        try:
            self.cache = self._fetch_with_retry()
            self.last_fetch_time = datetime.now(TEHRAN_TZ)
            logger.info(f"🌐 BrsApi snapshot updated at {self.last_fetch_time.strftime('%H:%M:%S')} ({len(self.cache)} records)")
            return self.cache
        except Exception as e:
            logger.error(f"❌ Failed to fetch BrsApi snapshot after all retries: {e}. Using cached data.")
            return self.cache


# ---------------------------------------------------
# ۵. واکشی داده‌های زنده‌ی سهم‌های منتخب با pytse-client (با Order Book)
# ---------------------------------------------------
class RealtimeTickerFetcher:
    def __init__(self, symbols: list[str]):
        self.symbols = symbols
        self.executor = ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE)

    @retry_on_exception(max_retries=3, delay=5, exceptions=(Exception, RuntimeError, tse.exceptions.ClientResponseError))
    def _fetch_symbol_with_retry(self, symbol: str) -> dict | None:
        """واکشی داده‌ی لحظه‌ای برای یک نماد خاص با مکانیزم Retry و استخراج Order Book."""
        try:
            ticker = tse.Ticker(symbol)
            rt = ticker.get_ticker_real_time_info_response(timeout=TSETMC_TIMEOUT) 
            
            # استخراج داده‌های Order Book به صورت structured: (count, volume, price)
            buy_orders_raw = [(b.count, b.volume, b.price) for b in getattr(rt, "buy_orders", [])]
            sell_orders_raw = [(s.count, s.volume, s.price) for s in getattr(rt, "sell_orders", [])]
            
            return {
                "symbol": symbol,
                "last_price": getattr(rt, "last_price", None),
                "adj_close": getattr(rt, "adj_close", None),
                "yesterday_price": getattr(rt, "yesterday_price", None),
                "open_price": getattr(rt, "open_price", None),
                "high_price": getattr(rt, "high_price", None),
                "low_price": getattr(rt, "low_price", None),
                "volume": getattr(rt, "volume", None),
                "value": getattr(rt, "value", None),
                "count": getattr(rt, "count", None),
                "state": getattr(rt, "state", None),
                # Order Book Raw Data (برای نگاشت EOD و تحلیل فاز ۲)
                "buy_orders_raw": buy_orders_raw,    
                "sell_orders_raw": sell_orders_raw,
                # Individual/Corporate Summary 
                "individual_buy_vol": getattr(getattr(rt, "individual_trade_summary", None), "buy_vol", None),
                "individual_sell_vol": getattr(getattr(rt, "individual_trade_summary", None), "sell_vol", None),
                "corporate_buy_vol": getattr(getattr(rt, "corporate_trade_summary", None), "buy_vol", None),
                "corporate_sell_vol": getattr(getattr(rt, "corporate_trade_summary", None), "sell_vol", None),
                "timestamp": datetime.now(TEHRAN_TZ),
            }
        except RuntimeError: 
            # هندل کردن خطای خاص pytse-client برای نمادهای ممنوع/متوقف
            logger.debug(f"Realtime data unavailable for {symbol} (RuntimeError).")
            return None
        except Exception as e:
            # اگر Retry هم نتواند مشکل را حل کند، خطا صادر می‌شود
            raise

    def fetch_symbol(self, symbol: str) -> dict | None:
        """wrapper برای مدیریت خطاهای نهایی پس از Retry."""
        try:
            return self._fetch_symbol_with_retry(symbol)
        except Exception as e:
            logger.error(f"❌ Realtime fetch failed for {symbol} after all retries: {e}")
            return None


    async def fetch_all_async(self) -> list[dict]:
        loop = asyncio.get_running_loop()
        tasks = [
            loop.run_in_executor(self.executor, self.fetch_symbol, symbol)
            for symbol in self.symbols
        ]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

# ---------------------------------------------------
# ۶. ذخیره snapshot نهایی ساعت 16 (با Order Book)
# ---------------------------------------------------
def map_order_book_to_db(orders_raw: List[Tuple[int, int, float]], prefix_count: str, prefix_volume: str, prefix_price: str) -> Dict[str, Any]:
    """نگاشت لیست raw سفارشات (count, volume, price) به ستون‌های Order Book دیتابیس (تا 5 سطح)."""
    data = {}
    # فقط تا 5 سطح را نگاشت می‌کنیم
    for i in range(min(5, len(orders_raw))):
        count, volume, price = orders_raw[i]
        level = i + 1
        data[f"{prefix_count}{level}"] = count
        data[f"{prefix_volume}{level}"] = volume
        data[f"{prefix_price}{level}"] = price
    return data


def save_end_of_day_snapshot(session: Session, symbol_name: str, rt_data: dict, now: datetime):
    today = now.date()
    # بررسی عدم تکرار داده
    if session.query(HistoricalData).filter_by(symbol_name=symbol_name, date=today).first():
        logger.warning(f"Snapshot for {symbol_name} on {today} already exists. Skipping save.")
        return

    try:
        # 1. نگاشت سفارشات خرید (Demand) به zd/qd/pd
        demand_data = map_order_book_to_db(
            rt_data.get("buy_orders_raw", []), 'zd', 'qd', 'pd'
        )
        # 2. نگاشت سفارشات فروش (Supply) به zo/qo/po
        supply_data = map_order_book_to_db(
            rt_data.get("sell_orders_raw", []), 'zo', 'qo', 'po'
        )
        
        # 3. جمع‌آوری تمام داده‌ها
        hist_data = {
            "symbol_id": symbol_name, # استفاده از symbol_name به عنوان symbol_id
            "symbol_name": symbol_name,
            "date": today,
            "jdate": jdatetime.date.fromgregorian(date=today).strftime("%Y-%m-%d"),
            "open": rt_data.get("open_price"),
            "high": rt_data.get("high_price"),
            "low": rt_data.get("low_price"),
            "close": rt_data.get("last_price"), 
            "final": rt_data.get("adj_close"), 
            "yesterday_price": rt_data.get("yesterday_price"),
            "volume": rt_data.get("volume"),
            "value": rt_data.get("value"),
            "num_trades": rt_data.get("count"),
            "updated_at": now,
            **demand_data,
            **supply_data
        }
        
        session.add(HistoricalData(**hist_data))
        session.commit()
        logger.info(f"💾 EOD Snapshot saved for {symbol_name} successfully.")
    except SQLAlchemyError:
        session.rollback()
        logger.error(f"❌ DB Error saving EOD snapshot for {symbol_name}: {traceback.format_exc()}")
    except Exception:
        session.rollback()
        logger.error(f"❌ General Error saving EOD snapshot for {symbol_name}: {traceback.format_exc()}")

# ---------------------------------------------------
# ۷. Orchestrator اصلی
# ---------------------------------------------------
async def run_realtime_orchestrator(session: Session):
    """
    مدیریت هم‌زمان داده‌های BrsApi و pytse-client بدون تداخل.
    نمادهای منتخب در شروع از دیتابیس خوانده می‌شوند.
    """
    # ۱. واکشی نمادهای منتخب از دیتابیس (در هر اجرای جدید)
    selected_symbols = get_selected_symbols(session)
    if not selected_symbols:
        logger.warning("🛑 No symbols selected for monitoring. Orchestrator stopping.")
        return

    logger.info(f"🚀 شروع مانیتورینگ لحظه‌ای برای {len(selected_symbols)} نماد منتخب: {selected_symbols}")

    market_fetcher = BrsMarketFetcher()
    ticker_fetcher = RealtimeTickerFetcher(selected_symbols)
    realtime_cache = RedisMock() 
    REALTIME_CACHE_KEY = "market:realtime:tickers" 
    last_successful_ticker_data: Dict[str, dict] = {} 

    market_close_time_obj = datetime.strptime(MARKET_CLOSE_TIME, "%H:%M").time()
    EOD_SAVE_FINISHED = False

    try:
        while True:
            now_tehran = datetime.now(TEHRAN_TZ)
            
            # --- ۱. دریافت snapshot از BrsApi (در بازه مجاز فقط) ---
            # اگر BrsApi قطع شد، این فرآیند کرش نمی‌کند و از کش استفاده می‌کند.
            market_fetcher.fetch() 

            # --- ۲. گرفتن داده‌های لحظه‌ای نمادهای منتخب (فقط در ساعات بازار) ---
            if now_tehran.hour < 9 or now_tehran.hour >= 17:
                # خارج از ساعات بازار (برای کاهش بار CPU/API)
                logger.debug(f"Waiting for market hours (Current time: {now_tehran.strftime('%H:%M:%S')}).")
                await asyncio.sleep(60)
                continue


            ticker_data = await ticker_fetcher.fetch_all_async()
            
            # --- ۳. به‌روزرسانی Cache (برای استفاده فاز ۲) ---
            if ticker_data:
                realtime_cache.set_realtime_data(REALTIME_CACHE_KEY, ticker_data)
                
                for d in ticker_data:
                    last_successful_ticker_data[d['symbol']] = d
                # Log summary
                logger.debug(f"Real-time update successful for {len(ticker_data)} symbols.")
            else:
                logger.warning("TSETMC/pytse-client fetch failed for all symbols in this cycle.")


            # --- ۴. در پایان بازار snapshot ذخیره کن ---
            if (now_tehran.time() >= market_close_time_obj) and (not EOD_SAVE_FINISHED):
                logger.info("⏰ بازار بسته شد. در حال ذخیرهٔ داده‌های نهایی EOD...")
                
                # ذخیره آخرین داده موفقیت‌آمیز
                for symbol, d in last_successful_ticker_data.items():
                    save_end_of_day_snapshot(session, symbol, d, now_tehran)
                
                EOD_SAVE_FINISHED = True
                logger.info("✅ فرآیند EOD (End of Day) و ذخیره‌سازی Order Book کامل شد. مانیتورینگ متوقف می‌شود.")
                break # توقف Orchestrator پس از ذخیره EOD
            
            await asyncio.sleep(REFRESH_INTERVAL)

    except asyncio.CancelledError:
        logger.info("🛑 مانیتورینگ متوقف شد (cancelled).")
    except Exception as e:
        logger.error(f"❌ Critical error in orchestrator: {e}", exc_info=True)
    finally:
        # در تابع اصلی (مانند if __name__ == "__main__") باید session بسته شود
        pass 

# ---------------------------------------------------
# Entry Point
# ---------------------------------------------------
if __name__ == "__main__":
    # تنظیمات اولیه logging برای اجرای تستی
    from extensions import get_session_local
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # در محیط تست، باید ابتدا Flask App Context ایجاد شود تا DB مدل‌ها شناخته شوند.
    # در این مثال ساده، فرض می‌کنیم session از یک تابع helper که خارج از context است به دست آمده.
    
    # MOCK: در محیط واقعی، شما باید session را به تابع پاس دهید
    # session = db.session 
    # اگر خارج از Flask context هستید:
    # session = SessionLocal() 
    
    # برای اجرای آزمایشی، از یک Mock Session استفاده می‌کنیم:
    class MockSession:
        def query(self, *args, **kwargs):
            # در محیط تست، فرض می‌کنیم هیچ سیگنال فعالی وجود ندارد، لذا Fallback فعال می‌شود
            class MockQuery:
                def filter(self, *args, **kwargs): return self
                def distinct(self): return self
                def all(self): return [] # بازگرداندن لیست خالی
                def first(self): return None
            return MockQuery()
        def add(self, obj): pass
        def commit(self): pass
        def rollback(self): pass
        def close(self): pass
    
    print("Running Orchestrator in MOCK DB mode. Will use default symbols.")
    mock_session = MockSession()
    
    try:
        asyncio.run(run_realtime_orchestrator(mock_session))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"A final error occurred: {e}")