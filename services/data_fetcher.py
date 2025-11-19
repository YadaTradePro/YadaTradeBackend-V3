# -*- coding: utf-8 -*-
# services/data_fetcher.py
# مسئولیت: اجرای چرخه کامل بازسازی داده‌های پایه با مکانیزم‌های بازیابی، مدیریت خطا و Caching.

import logging
import pytse_client as tse
import pandas as pd
import jdatetime
import time
import gc
import json
import functools
import random
import os 
import re 
import numpy as np 
from datetime import datetime, date, timedelta 
from typing import List, Dict, Optional, Any, Tuple
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from extensions import db
from models import ComprehensiveSymbolData, HistoricalData, FundamentalData
from pytse_client import download_client_types_records
from sqlalchemy import text 

import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module='pytse_client')

# ----------------------------
# تنظیمات پایه و متغیرها
# ----------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PROGRESS_FILE = "rebuild_progress.json"
CLIENT_TYPES_CACHE_FILE = "client_types_cache.pkl" 
COMMIT_BATCH_SIZE = 100 

# نگاشت ستون‌های اصلی HistoricalData از خروجی Ticker.history
HISTORICAL_COLUMN_MAPPING = {
    'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 
    'Final': 'final', 'Close': 'close', 
    'Volume': 'volume', 'Value': 'value', 'Count': 'num_trades', 
    'PLast': 'yesterday_price', # این ستون در محاسبات زیر جایگزین می‌شود
    'PDrCotVal': 'yesterday',
}

# نگاشت ستون‌های حقیقی/حقوقی برای ادغام
CLIENT_TYPE_COLUMN_MAPPING = {
    "individual_buy_count": "buy_count_i", "corporate_buy_count": "buy_count_n",
    "individual_sell_count": "sell_count_i", "corporate_sell_count": "sell_count_n",
    "individual_buy_vol": "buy_i_volume", "corporate_buy_vol": "buy_n_volume",
    "individual_sell_vol": "sell_i_volume", "corporate_sell_vol": "sell_n_volume",
}

# ----------------------------
# ۱. توابع کمکی (Helper Functions)
# ----------------------------

def get_value_safely(obj: Any, key: str, default: Any = None) -> Any:
    """دسترسی ایمن به مقدار (دیکشنری یا آبجکت)."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)

def is_symbol_valid(symbol_name: str, market_type_name: str) -> bool:
    """بررسی اعتبار نماد."""
    try:
        if not symbol_name:
            return False
        market_lower = str(market_type_name).lower() if market_type_name else ''
        
        BAD_SUFFIXES = ('ح', 'ض', 'ص', 'و')  
        if (symbol_name.endswith(BAD_SUFFIXES) and len(symbol_name) > 1) or \
            re.search(r"\b(حق\s*تقدم|ح\.?\s*تقدم)\b", symbol_name, re.IGNORECASE):
            return False

        INVALID_MARKET_KEYWORDS = ['اختیار', 'آتی', 'مشتقه', 'تسهیلات']
        if any(keyword.lower() in market_lower for keyword in INVALID_MARKET_KEYWORDS):
            return False
        
        return True

    except Exception as e:
        logger.error(f"Error checking symbol validity {symbol_name}: {e}")
        return False 


def retry_on_exception(max_retries=5, delay=60, backoff=2.0):
    """
    Decorator برای اجرای مجدد یک تابع در صورت بروز خطا.
    max_retries=5 و delay=60 برای مدیریت timeout نمادهای سنگین.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries, wait = 0, delay
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except (Exception, OperationalError) as e:
                    retries += 1
                    logger.warning(f"Error in {func.__name__}: {e}. Retrying {retries}/{max_retries} after {wait:.2f}s...")
                    time.sleep(wait + random.uniform(0, 5)) # افزودن نویز بیشتر به تاخیر
                    wait *= backoff
            raise
        return wrapper
    return decorator

def to_jdate_safe(d: Any) -> Optional[str]:
    """تبدیل ایمن تاریخ میلادی (Timestamp/date) به شمسی."""
    if pd.isna(d):
        return None
    try:
        g_date = d.date() if hasattr(d, "date") else d
        if isinstance(g_date, str):
            g_date = datetime.strptime(g_date, "%Y-%m-%d").date()
        return jdatetime.date.fromgregorian(date=g_date).strftime("%Y-%m-%d")
    except Exception:
        return None

# ----------------------------
# ۱.۱. تابع کمکی دیباگ جدید
# ----------------------------

def log_error_dataframe(symbol_name: str, df: pd.DataFrame, error: Exception) -> None:
    """
    دفترچه DataFrame مشکل‌ساز و جزئیات خطا را در یک فایل JSON ذخیره می‌کند. 
    این فایل حاوی تمام داده‌های خامی است که باعث خطای float() شده است.
    """
    DEBUG_DIR = "debug_data"
    os.makedirs(DEBUG_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        # کپی DataFrame برای تغییرات مورد نیاز جهت سریال‌سازی
        df_log = df.copy()
        
        # تبدیل ستون‌های تاریخ به رشته برای JSON
        for col in df_log.select_dtypes(include=['datetime64', 'datetime64[ns]']).columns:
             df_log[col] = df_log[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # جایگزینی NaT (تاریخ نامعتبر) با یک رشته قابل تشخیص و NaN با None 
        # (تا نشان داده شود که این مقدار NaN بوده است)
        df_log = df_log.replace({pd.NaT: 'NaT_Found', np.nan: None})
        
        log_data = {
            "symbol": symbol_name,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": timestamp,
            "df_data": df_log.to_dict('records') # ذخیره تمام داده‌ها
        }

        filename = os.path.join(DEBUG_DIR, f"{symbol_name}_{timestamp}_error.json")
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, indent=4, ensure_ascii=False)
        logger.error(f"DEBUG: Saved problematic DataFrame for {symbol_name} to {filename}")

    except Exception as e:
        logger.error(f"DEBUG: Failed to save debug data for {symbol_name}. Secondary error: {e}")


# ----------------------------
# ۲. مدیریت Session و پاکسازی
# ----------------------------
# ... (توابع get_session_local، clear_all_tables، vacuum_database بدون تغییر) ...

def get_session_local() -> Session:
    """ایجاد session local با application context."""
    try:
        from flask import current_app
        with current_app.app_context():
            return sessionmaker(bind=db.engine)()
    except RuntimeError:
        try:
            return sessionmaker(bind=db.get_engine())()
        except AttributeError:
             return sessionmaker(bind=db.engine)()

def clear_all_tables(session: Session) -> None:
    """پاکسازی کامل داده‌های جداول هدف."""
    logger.warning("Step 0.2: Starting full TRUNCATE/DELETE on target tables...")
    try:
        session.query(HistoricalData).delete(synchronize_session='fetch')
        session.query(FundamentalData).delete(synchronize_session='fetch')
        session.query(ComprehensiveSymbolData).delete(synchronize_session='fetch')
        session.commit()
        logger.info("Step 0.2 Completed: Successfully cleared HistoricalData, FundamentalData, and ComprehensiveSymbolData tables.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error during table clear: {e}", exc_info=True)
        raise

def vacuum_database(session: Session) -> None:
    """اجرای دستور VACUUM برای بازیابی فضای اشغال شده در SQLite."""
    if session.bind.name == 'sqlite':
        logger.warning("Step 0.3: Starting VACUUM on SQLite database to reclaim space...")
        try:
            session.execute(text("VACUUM"))
            session.commit()
            logger.info("Step 0.3 Completed: Database VACUUM successful. File size should now be reduced.")
        except Exception as e:
            session.rollback()
            logger.error(f"Error during VACUUM command: {e}", exc_info=True)
            pass
    else:
        logger.info("Step 0.3: Skipping VACUUM as database is not SQLite.")


# ----------------------------
# ۳. مدیریت پیشرفت (Progress Management)
# ----------------------------
# ... (توابع save_progress، load_progress، delete_progress بدون تغییر) ...
def save_progress(progress_file: str, current_index: int) -> None:
    """ذخیره آخرین اندیس شروع."""
    try:
        with open(progress_file, "w", encoding="utf-8") as f:
            json.dump({"last_index": current_index}, f)
    except Exception as e:
        logger.warning(f"Failed to save progress to {progress_file}: {e}")

def load_progress(progress_file: str) -> int:
    """بارگذاری آخرین اندیس ذخیره شده."""
    try:
        with open(progress_file, "r", encoding="utf-8") as f:
            return json.load(f).get("last_index", 0)
    except FileNotFoundError:
        return 0
    except json.JSONDecodeError:
        logger.warning(f"Progress file {progress_file} is corrupted. Starting from index 0.")
        return 0
    
def delete_progress(progress_file: str) -> None:
    """حذف فایل پیشرفت."""
    try:
        if os.path.exists(progress_file):
            os.remove(os.path.abspath(progress_file)) 
            logger.info(f"Progress file {progress_file} deleted.")
    except Exception as e:
        logger.warning(f"Failed to delete progress file: {e}")


# ----------------------------
# ۴. توابع واکشی 
# ----------------------------

# Max retries increased to 5, delay increased to 60s for stability with large datasets (e.g., Khodro)
@retry_on_exception(max_retries=5, delay=60, backoff=2.0)
def download_all_symbols_wrapper() -> Dict[str, Any]:
    """واکشی اولیه تمام داده‌های نمادها."""
    logger.info("Step 1.1: Starting download of all symbol list from TSETMC...")
    data = tse.download(symbols="all", write_to_csv=False)
    logger.info(f"Step 1.1 Completed: Successfully downloaded {len(data)} basic symbol records.")
    return data

# Max retries increased to 5, delay increased to 60s for stability
@retry_on_exception(max_retries=5, delay=60, backoff=2.0)
def download_client_types_wrapper() -> Dict[str, pd.DataFrame]:
    """واکشی اولیه تمام رکوردهای حقیقی/حقوقی (با Caching)."""
    logger.info("Step 1.2: Starting download of all client-type (individual/legal) records...")
    client_types_all = download_client_types_records("all", write_to_csv=False)
    
    if client_types_all:
        logger.info(f"Step 1.2 Completed: Successfully downloaded client-type data for {len(client_types_all)} symbols.")
    else:
        logger.warning("Step 1.2 Completed: Client-type download returned EMPTY data.")
        
    return client_types_all

# ----------------------------
# ۵. توابع استخراج و درج داده‌ها
# ----------------------------

def extract_and_insert_symbol_data(session: Session, symbol_name: str, ticker: tse.Ticker) -> Optional[str]:
    """استخراج و درج داده‌های پایه نماد."""
    try:
        symbol_id = getattr(ticker, "index", None)
        if not symbol_id:
            return None

        now = datetime.now()
        
        market_data = getattr(ticker, "market", None) or getattr(ticker, "flow", None)
        
        float_percent = getattr(ticker, "float_shares", None)
        total_shares = getattr(ticker, "total_shares", None)
        float_shares_count = None
        
        if float_percent is not None and total_shares is not None:
            try:
                # تبدیل به float برای اطمینان
                fp = float(float_percent)
                ts = float(total_shares)
                float_shares_count = (fp * ts) / 100
            except (ValueError, TypeError):
                pass

        comp_data = {
            "symbol_id": symbol_id,
            "symbol_name": symbol_name,
            "company_name": getattr(ticker, "title", None),
            "isin": getattr(ticker, "isin", None),
            "tse_index": symbol_id,
            "market_type": market_data,
            "group_name": getattr(ticker, "group_name", None),
            "base_volume": getattr(ticker, "base_volume", None),
            "eps": getattr(ticker, "eps", None),
            "p_e_ratio": getattr(ticker, "p_e_ratio", None),
            "p_s_ratio": getattr(ticker, "p_s_ratio", None),
            "nav": getattr(ticker, "nav", None),
            "float_shares": float_shares_count, 
            "market_cap": getattr(ticker, "market_cap", None),
            "industry": getattr(ticker, "group_name", None),
            "capital": total_shares,
            "fiscal_year": getattr(ticker, "fiscal_year", None),
            "flow": getattr(ticker, "flow", None),
            "state": getattr(ticker, "state", None),
            "last_fundamental_update_date": now.date(),
            "updated_at": now
        }
        
        session.add(ComprehensiveSymbolData(**comp_data)) 
        return symbol_id

    except Exception as e:
        logger.error(f"Error processing Comprehensive data for {symbol_name}: {e}")
        return None

def extract_and_insert_historical_data(session: Session, symbol_id: str, symbol_name: str, ticker: tse.Ticker, client_types_all: Dict[str, pd.DataFrame]) -> int:
    """استخراج، غنی‌سازی و درج داده‌های تاریخی و فاندامنتال روزانه."""
    df = None # تضمین می کند که df تعریف شده باشد حتی اگر در ابتدا خطایی رخ دهد.
    try:
        df = ticker.history
        if df is None or df.empty:
            return 0

        df = df.copy()

        # استانداردسازی ستون تاریخ و نگاشت نام ستون‌ها
        if "date" not in df.columns:
            df.reset_index(inplace=True)
            if "Date" in df.columns:
                df.rename(columns={"Date": "date"}, inplace=True)
            elif "DATE" in df.columns:
                df.rename(columns={"DATE": "date"}, inplace=True)
        
        for source_col, target_col in HISTORICAL_COLUMN_MAPPING.items():
            if source_col in df.columns:
                df.rename(columns={source_col: target_col}, inplace=True)
            elif source_col.lower() in df.columns:
                df.rename(columns={source_col.lower(): target_col}, inplace=True)
        
        if 'final' not in df.columns and 'adjClose' in df.columns:
             df.rename(columns={'adjClose': 'final'}, inplace=True)
        
        # ادغام داده‌های حقیقی/حقوقی
        if client_types_all and symbol_name in client_types_all:
            df_client = client_types_all.get(symbol_name)
            if df_client is not None and not df_client.empty:
                df_client = df_client.rename(columns=CLIENT_TYPE_COLUMN_MAPPING)
                df_client["date"] = pd.to_datetime(df_client["date"], errors='coerce').dt.normalize()
                df["date"] = pd.to_datetime(df["date"], errors='coerce').dt.normalize()
                
                df = pd.merge(
                    df,
                    df_client[["date"] + list(CLIENT_TYPE_COLUMN_MAPPING.values())],
                    on="date",
                    how="left"
                )

        # ----------------------------------------------------------------------
        # FIX CRITICAL FOR NATYPE: تمیزکاری تاریخ و حذف سطرهای بدون تاریخ/قیمت/حجم
        if 'date' in df.columns:
            # 1. اطمینان از تبدیل تاریخ به فرمت Datetime64 و جایگزینی مقادیر نامعتبر با NaT
            df.loc[:, 'date'] = pd.to_datetime(df['date'], errors='coerce').dt.normalize()
            
            # 2. **تمیزکاری مضاعف**: حذف سطر‌هایی که تاریخ معتبری ندارند (NaT) یا ستون‌های عددی اصلی (final/volume) در آن‌ها NaN است.
            key_columns_to_check = ['date', 'final', 'volume']
            df.dropna(subset=[col for col in key_columns_to_check if col in df.columns], inplace=True) 
        
        if df.empty:
            logger.warning(f"Skipping {symbol_name}: DataFrame is empty after date/price cleanup.")
            return 0

        # ----------------------------------------------------------------------
        # FIX 2: تضمین نوع عددی و **رفع قطعی خطای NAType** با fillna(0.0)
        
        key_cols = ['open', 'high', 'low', 'final', 'close', 'volume', 'value', 'num_trades', 'yesterday']
        client_type_cols = list(CLIENT_TYPE_COLUMN_MAPPING.values())
        all_numeric_cols = key_cols + client_type_cols

        for col in all_numeric_cols:
            if col in df.columns:
                # 1. تبدیل مقادیر غیر عددی/رشته‌ای به NaN (استاندارد)
                df.loc[:, col] = pd.to_numeric(df.loc[:, col], errors='coerce')
                
                # 2. **دفاع مضاعف:** اگر ستون به دلیل نشت NaT نوع datetime/timedelta گرفته، آن را به float/NaN تبدیل کن.
                if df[col].dtype.kind in ('M', 'm'): # M: Datetime, m: Timedelta
                    df.loc[:, col] = df[col].astype(object) 
                    df.loc[:, col] = pd.to_numeric(df.loc[:, col], errors='coerce') 

                # 3. **FIX CRITICAL NATYPE (بهبودیافته):** تمام NaNs را با 0.0 جایگزین کن و نتیجه را صراحتاً به نوع float تبدیل کن.
                # این کار هشدار FutureWarning را حذف می‌کند.
                df.loc[:, col] = df[col].fillna(0.0)
                df.loc[:, col] = df[col].infer_objects(copy=False).astype(float)
                
                # 4. تبدیل نهایی به نوع دقیق (Int64 برای ستون‌های شمارشی)
                if col in ['num_trades'] + client_type_cols:
                     df.loc[:, col] = df[col].astype(pd.Int64Dtype()) 
                # else: (قبلاً به float تبدیل شده)

        df.sort_values(by='date', inplace=True) 

        # ----------------------------------------------------------------------
        # محاسبات ستون‌های HistoricalData
        
        # محاسبه yesterday_price بر اساس قیمت بسته شدن روز قبل
        df.loc[:, 'yesterday_price'] = df['close'].shift(1) 
        
        # برای اولین سطر (که shift(1) مقدار NaN می‌دهد)، از ستون 'yesterday' (PLast) استفاده کن
        if 'yesterday' in df.columns and not df.empty:
             df.loc[df.index[0], 'yesterday_price'] = df.loc[df.index[0], 'yesterday']
        
        # پر کردن باقی‌مانده NaN (اگر هنوز وجود دارد) با 0.0 برای جلوگیری از خطاهای محاسباتی
        df.loc[:, 'yesterday_price'] = df.loc[:, 'yesterday_price'].fillna(0.0).astype(float)
        
        # محاسبه MV
        if 'value' in df.columns:
             df.loc[:, 'mv'] = df['value']
        else:
             df.loc[:, 'mv'] = df['final'] * df['volume']

        # FIX: استفاده از replace(0, np.nan) برای جلوگیری از تقسیم بر صفر (فقط برای مخرج)
        # این هشدار FutureWarning را می‌دهد اما برای جلوگیری از تقسیم بر صفر (که خطا است) ضروری است.
        yesterday_safe = df['yesterday_price'].replace(0, np.nan) 

        # Price Changes
        df.loc[:, 'pcc'] = df['final'] - df['yesterday_price']
        df.loc[:, 'pcp'] = (df['pcc'] / yesterday_safe) * 100
        
        df.loc[:, 'plc'] = df['close'] - df['yesterday_price']
        df.loc[:, 'plp'] = (df['plc'] / yesterday_safe) * 100
            
        # ----------------------------------------------------------------------
        # محاسبات داده‌های فاندامنتال/سنتیمنت روزانه
        
        has_client_data = all(col in df.columns for col in ['buy_count_i', 'sell_count_i', 'buy_i_volume', 'sell_i_volume'])

        if has_client_data:
            # FIX: حفاظت قوی تقسیم بر صفر با replace(0, np.nan)
            buy_count_safe = df['buy_count_i'].astype(float).replace(0, np.nan)
            sell_count_safe = df['sell_count_i'].astype(float).replace(0, np.nan)
            
            # میانگین حجم
            df.loc[:, 'real_buy_vol_avg'] = df['buy_i_volume'] / buy_count_safe
            df.loc[:, 'real_sell_vol_avg'] = df['sell_i_volume'] / sell_count_safe
            
            # نسبت قدرت (مخرج: میانگین فروش)
            sell_avg_safe = df['real_sell_vol_avg'].replace(0, np.nan)
            df.loc[:, 'real_power_ratio'] = df['real_buy_vol_avg'] / sell_avg_safe
            
            # Daily Liquidity
            price_for_daily_calc = df['close'] 
            df.loc[:, 'real_buy_value'] = df['buy_i_volume'] * price_for_daily_calc
            df.loc[:, 'real_sell_value'] = df['sell_i_volume'] * price_for_daily_calc
            df.loc[:, 'daily_liquidity'] = df['real_buy_value'] - df['real_sell_value']
        else:
             df['real_power_ratio'] = None
             df['daily_liquidity'] = None

        # Volume Ratio 20 Day
        df.loc[:, 'volume_20d_avg'] = df['volume'].rolling(window=20, min_periods=1).mean()
        vol_avg_safe = df['volume_20d_avg'].replace(0, np.nan)
        df.loc[:, 'volume_ratio_20d'] = df['volume'] / vol_avg_safe
        
        # ----------------------------------------------------------------------
        # آماده‌سازی نهایی
        
        df["symbol_id"] = symbol_id
        df["symbol_name"] = symbol_name
        df["updated_at"] = datetime.now()
        df["jdate"] = df["date"].apply(to_jdate_safe)
        
        # ----------------------------------------------------------------------
        # درج FundamentalData (روزانه)
        
        fund_data_static = {
            "eps": getattr(ticker, "eps", None),
            "pe": getattr(ticker, "p_e_ratio", None),
            "group_pe_ratio": getattr(ticker, "group_p_e_ratio", None),
            "psr": getattr(ticker, "psr", None) or getattr(ticker, "p_s_ratio", None),
            "market_cap": getattr(ticker, "market_cap", None),
            "float_shares": getattr(ticker, "float_shares", None),
            "base_volume": getattr(ticker, "base_volume", None),
        }
        
        df_fund_to_insert = df.copy()
        for k, v in fund_data_static.items():
            if k not in df_fund_to_insert.columns:
                df_fund_to_insert[k] = v

        fund_model_cols = [c.name for c in FundamentalData.__table__.columns]
        df_fund_to_insert = df_fund_to_insert[[col for col in df_fund_to_insert.columns if col in fund_model_cols]]
        
        # تبدیل به دیکشنری و جایگزینی NaN با None
        fund_records = df_fund_to_insert.to_dict("records")
        cleaned_fund_records = []
        for record in fund_records:
            # جایگزینی مقادیر Pandas NaN (و 0.0 ناشی از fillna) با None برای پایگاه داده
            cleaned_record = {k: (None if pd.isna(v) or v == 0.0 else v) for k, v in record.items()} 
            cleaned_fund_records.append(cleaned_record)
            
        if cleaned_fund_records:
            session.bulk_insert_mappings(FundamentalData, cleaned_fund_records) 
            logger.info(f"Step 2.3.1: {len(cleaned_fund_records)} Fundamental records inserted for {symbol_name}.")
        
        # ----------------------------------------------------------------------
        # درج HistoricalData (روزانه)
        
        model_cols = [c.name for c in HistoricalData.__table__.columns]
        order_book_cols = [c.name for c in HistoricalData.__table__.columns if c.name.startswith(('zd', 'qd', 'pd', 'zo', 'qo', 'po'))]
        target_cols = [col for col in model_cols if col not in order_book_cols]
        
        for col in target_cols:
             if col not in df.columns:
                  df[col] = None

        df_to_insert = df[[col for col in df.columns if col in target_cols]]

        records = df_to_insert.to_dict("records")
        cleaned_records = []
        for record in records:
            # جایگزینی مقادیر Pandas NaN (و 0.0 ناشی از fillna) با None برای پایگاه داده
            cleaned_record = {k: (None if pd.isna(v) or v == 0.0 else v) for k, v in record.items()} 
            cleaned_records.append(cleaned_record)
            
        if cleaned_records:
            session.bulk_insert_mappings(HistoricalData, cleaned_records)
            return len(cleaned_records)

        return 0

    except Exception as e:
        # 🔴 تماس با تابع دیباگ در صورت بروز خطا
        if df is not None and not df.empty:
            log_error_dataframe(symbol_name, df, e)
        
        logger.error(f"Error processing Historical/Fundamental data for {symbol_name}: {e}")
        return -1

# ----------------------------
# ۶. تابع اصلی مدیریت چرخه بازسازی
# ----------------------------
# ... (تابع run_full_rebuild بدون تغییر) ...

def run_full_rebuild(batch_size: int = 50, commit_batch_size: int = COMMIT_BATCH_SIZE) -> Dict[str, Any]:
    """اجرای چرخه کامل بازسازی داده‌ها."""
    logger.info("--- Starting Full Weekly Data Rebuild Process ---")
    session = get_session_local()
    start_time = datetime.now()
    
    try:
        # گام ۰: مدیریت Resume
        start_index = load_progress(PROGRESS_FILE)
        
        if start_index == 0:
            logger.info("Step 0.1: Starting a FRESH rebuild (index 0).")
            clear_all_tables(session)
            vacuum_database(session)
        else:
            logger.warning(f"Step 0.1: Resuming rebuild from index {start_index}...")

        # گام ۱: واکشی داده‌های اولیه 
        all_tickers_data = download_all_symbols_wrapper()
        client_types_all = download_client_types_wrapper()
        
        # گام ۱.۳: فیلترینگ زودهنگام نمادها
        logger.info("Step 1.3: Starting early filtering of symbols (Hagh Taghodom, Invalid Markets)...")
        raw_symbol_names = list(all_tickers_data.keys())
        valid_symbol_names = []
        
        for symbol_name in raw_symbol_names:
            # استفاده از تابع ایمن برای خواندن مقادیر (چه دیکشنری چه آبجکت)
            ticker_data_basic = get_value_safely(all_tickers_data, symbol_name) if isinstance(all_tickers_data, dict) else None
            if ticker_data_basic is None and isinstance(all_tickers_data, dict):
                 ticker_data_basic = all_tickers_data.get(symbol_name)

            market_type = get_value_safely(ticker_data_basic, "market") or get_value_safely(ticker_data_basic, "flow")

            if is_symbol_valid(symbol_name, market_type):
                valid_symbol_names.append(symbol_name)
        
        symbol_names = valid_symbol_names
        total_symbols_after_filter = len(symbol_names)
        
        skipped_count = len(raw_symbol_names) - total_symbols_after_filter
        logger.info(f"Step 1.3 Completed: Found {len(raw_symbol_names)} raw symbols. {total_symbols_after_filter} VALID symbols remain (Skipped: {skipped_count}).")
        
        # تنظیم مجدد start_index
        if start_index >= total_symbols_after_filter:
            start_index = 0
            delete_progress(PROGRESS_FILE)
        
        symbols_processed = 0 
        historical_records_inserted = 0
        errors = 0
        
        current_symbol_count = start_index 
        
        # گام ۲: حلقه پردازش اصلی
        for i in range(start_index, total_symbols_after_filter, batch_size):
            batch_symbols = symbol_names[i:i + batch_size]
            current_batch_end_index = i + len(batch_symbols)

            logger.info(f"--- Step 2.0: Processing batch (Indices: {i} to {current_batch_end_index-1}) ---")

            for symbol_name in batch_symbols:
                
                current_symbol_count += 1
                
                try:
                    
                    logger.info(f"Step 2.1: Fetching full Ticker data for {symbol_name} (Symbol {current_symbol_count}/{total_symbols_after_filter}).")
                    
                    # 🔴 حفاظت در برابر کرش کتابخانه (مثل وسمنان)
                    try:
                        ticker = tse.Ticker(symbol_name)
                    except Exception as e:
                         logger.error(f"Skipping symbol {symbol_name} due to library crash (IndexError/etc): {e}")
                         errors += 1
                         continue

                    if not getattr(ticker, "index", None):
                        time.sleep(1)
                        try:
                            ticker = tse.Ticker(symbol_name)
                        except:
                            pass
                        if not getattr(ticker, "index", None):
                             # به جای Exception، لاگ می‌کنیم و ادامه می‌دهیم
                             logger.warning(f"Skipping {symbol_name}: Ticker created but Index missing.")
                             errors += 1
                             continue

                    # الف) درج Comprehensive
                    symbol_id = extract_and_insert_symbol_data(session, symbol_name, ticker)
                    logger.info(f"Step 2.2: Comprehensive data prepared for {symbol_name}.")
                    
                    if not symbol_id:
                        errors += 1
                        continue
                    
                    # ب) درج Historical و Fundamental
                    num_hist_records = extract_and_insert_historical_data(session, symbol_id, symbol_name, ticker, client_types_all)
                    
                    if num_hist_records >= 0: 
                        historical_records_inserted += max(0, num_hist_records)
                        symbols_processed += 1 
                        if num_hist_records > 0:
                             logger.info(f"Step 2.3: Data prepared for {symbol_name}.")
                        else:
                             logger.warning(f"Step 2.3: No historical records found for {symbol_name}.")

                    elif num_hist_records == -1:
                         errors += 1
                         
                    # Commit دسته‌ای
                    if symbols_processed > 0 and symbols_processed % commit_batch_size == 0:
                        session.commit()
                        logger.info(f"Step 2.4: Committed after {symbols_processed} successful symbols.")

                except Exception as e:
                    errors += 1
                    logger.error(f"Failed to process symbol {symbol_name} (Skipping): {e}")
                    session.rollback()
                    time.sleep(1)
                    continue
                
            # Commit نهایی دسته
            session.commit()
            logger.info(f"Step 2.4: Batch committed successfully.")
            save_progress(PROGRESS_FILE, current_batch_end_index)
            
            del batch_symbols
            gc.collect()
            time.sleep(1)

        # گام ۳: اتمام و خلاصه
        logger.info("Step 3.1: Full rebuild loop finished. Cleaning up progress file.")
        delete_progress(PROGRESS_FILE)
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        logger.info(f"--- FULL REBUILD COMPLETED ---")
        logger.info(
            f"Summary: Processed {symbols_processed} valid symbols, "
            f"Skipped {skipped_count} invalid/rights symbols, "
            f"{historical_records_inserted} history records. Errors: {errors}."
        )

        return {
            "status": "SUCCESS",
            "symbols_processed_valid": symbols_processed,
            "symbols_skipped": skipped_count,
            "historical_records": historical_records_inserted,
            "total_errors": errors,
            "execution_time_min": round(execution_time / 60, 2),
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        session.rollback()
        logger.error(f"Fatal error during rebuild process (Resuming from index {start_index} next time): {e}", exc_info=True)
        return {"status": "FAILED", "reason": "Fatal System Error", "error_message": str(e), "last_index": start_index}
    finally:
        session.close()

# ----------------------------
# توابع export
# ----------------------------
__all__ = [
    "get_session_local",
    "run_full_rebuild",
    # ... (سایر توابع)
]
