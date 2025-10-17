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
import os # برای مدیریت فایل کش
from datetime import datetime, date
from typing import List, Dict, Optional, Any, Tuple
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from extensions import db
from models import ComprehensiveSymbolData, HistoricalData, FundamentalData
from pytse_client import download_client_types_records 

# ----------------------------
# تنظیمات پایه و متغیرها
# ----------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PROGRESS_FILE = "rebuild_progress.json"
CLIENT_TYPES_CACHE_FILE = "client_types_cache.pkl" # فایل کش برای داده‌های حقیقی/حقوقی
COMMIT_BATCH_SIZE = 100 

# نگاشت ستون‌های اصلی HistoricalData از خروجی Ticker.history
HISTORICAL_COLUMN_MAPPING = {
    'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 
    'Final': 'final', 
    'Close': 'close', 
    'Volume': 'volume', 'Value': 'value', 'Count': 'num_trades', 
    'PLast': 'yesterday_price', 
}

# نگاشت ستون‌های حقیقی/حقوقی برای ادغام
CLIENT_TYPE_COLUMN_MAPPING = {
    "individual_buy_count": "buy_count_i", "corporate_buy_count": "buy_count_n",
    "individual_sell_count": "sell_count_i", "corporate_sell_count": "sell_count_n",
    "individual_buy_vol": "buy_i_volume", "corporate_buy_vol": "buy_n_volume",
    "individual_sell_vol": "sell_i_volume", "corporate_sell_vol": "sell_n_volume",
}

# ----------------------------
# ۱. مدیریت خطا و تاریخ
# ----------------------------
def retry_on_exception(max_retries=3, delay=5, backoff=2.0):
    """Decorator برای اجرای مجدد یک تابع در صورت بروز خطا (مثل قطع شبکه)."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries, wait = 0, delay
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except (Exception, tse.exceptions.ClientResponseError, OperationalError) as e:
                    retries += 1
                    logger.warning(f"Error in {func.__name__}: {e}. Retrying {retries}/{max_retries} after {wait:.2f}s...")
                    time.sleep(wait + random.uniform(0, 2))
                    wait *= backoff
            raise
        return wrapper
    return decorator

def to_jdate_safe(d: Any) -> Optional[str]:
    """تبدیل ایمن تاریخ میلادی (Timestamp/date) به شمسی (اصلاحیه 2)."""
    if pd.isna(d):
        return None
    try:
        # اگر Timestamp است، از .date() استفاده شود.
        g_date = d.date() if hasattr(d, "date") else d
        # اگر هنوز string است، به date تبدیل شود.
        if isinstance(g_date, str):
            g_date = datetime.strptime(g_date, "%Y-%m-%d").date()
            
        return jdatetime.date.fromgregorian(date=g_date).strftime("%Y-%m-%d")
    except Exception:
        return None

# ----------------------------
# ۲. مدیریت Session و پاکسازی
# ----------------------------
def get_session_local() -> Session:
    """ایجاد session دیتابیس در حالت داخل یا خارج از context Flask."""
    try:
        from flask import current_app
        if current_app:
            return db.session
    except RuntimeError:
        pass
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db.engine)
    return SessionLocal()

def clear_all_tables(session: Session) -> None:
    """پاکسازی کامل داده‌های جداول هدف."""
    logger.warning("Starting full TRUNCATE/DELETE on target tables...")
    try:
        session.query(HistoricalData).delete(synchronize_session='fetch')
        session.query(FundamentalData).delete(synchronize_session='fetch')
        session.query(ComprehensiveSymbolData).delete(synchronize_session='fetch')
        session.commit()
        logger.info("Successfully cleared HistoricalData, FundamentalData, and ComprehensiveSymbolData tables.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error during table clear: {e}", exc_info=True)
        raise 

# ----------------------------
# ۳. مدیریت پیشرفت (Progress Management)
# ----------------------------
def save_progress(progress_file: str, current_index: int) -> None:
    """ذخیره آخرین اندیس شروع برای بازیابی فرآیند."""
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
    """حذف فایل پیشرفت پس از اتمام موفقیت‌آمیز."""
    try:
        if os.path.exists(progress_file):
            os.remove(progress_file)
            logger.info(f"Progress file {progress_file} deleted.")
    except Exception as e:
        logger.warning(f"Failed to delete progress file: {e}")

# ----------------------------
# ۴. توابع واکشی با Caching
# ----------------------------

@retry_on_exception(max_retries=3, delay=30, backoff=2.0)
def download_all_symbols_wrapper() -> Dict[str, Any]:
    """واکشی اولیه تمام داده‌های نمادها (تاریخی و اطلاعات پایه) با Retry."""
    logger.info("Attempting to download all symbols from TSETMC with Retry (async_requests=False)...")
    return tse.download(symbols="all", write_to_csv=False, async_requests=False)

@retry_on_exception(max_retries=3, delay=30, backoff=2.0)
def download_client_types_wrapper() -> Dict[str, pd.DataFrame]:
    """واکشی اولیه تمام رکوردهای حقیقی/حقوقی با مکانیزم Retry و Caching (اصلاحیه 3)."""
    if os.path.exists(CLIENT_TYPES_CACHE_FILE):
        logger.info(f"Loading client-type records from cache: {CLIENT_TYPES_CACHE_FILE}")
        try:
            return pd.read_pickle(CLIENT_TYPES_CACHE_FILE)
        except Exception as e:
            logger.warning(f"Failed to load cache ({e}). Re-downloading and deleting corrupted cache.")
            os.remove(CLIENT_TYPES_CACHE_FILE)
            pass

    logger.info("Attempting to download all client-type (individual/legal) records with Retry...")
    client_types_all = download_client_types_records("all", write_to_csv=False)
    
    # Save to cache
    if client_types_all:
        try:
            pd.to_pickle(client_types_all, CLIENT_TYPES_CACHE_FILE)
            logger.info(f"Client-type records saved to cache: {CLIENT_TYPES_CACHE_FILE}")
        except Exception as e:
            logger.error(f"Failed to save client-type data to cache: {e}")
            
    return client_types_all


# ----------------------------
# ۵. توابع استخراج و درج داده‌ها
# ----------------------------

def extract_and_insert_symbol_data(session: Session, symbol_name: str, ticker: tse.Ticker) -> Optional[str]:
    """استخراج و درج داده‌های ComprehensiveSymbol و Fundamental (بدون Commit)."""
    try:
        symbol_id = getattr(ticker, "index", None) 
        if not symbol_id:
            logger.warning(f"Symbol {symbol_name} has no valid 'index'. Skipping comprehensive data.")
            return None

        now = datetime.now()

        # 1. داده‌های Comprehensive (با نگاشت کامل پیشنهادی)
        comp_data = {
            "symbol_id": symbol_id,
            "symbol_name": symbol_name,
            "company_name": getattr(ticker, "title", None),
            "isin": getattr(ticker, "isin", None),
            "tse_index": symbol_id, 
            "market_type": getattr(ticker, "market", None),
            "group_name": getattr(ticker, "group_name", None),
            "base_volume": getattr(ticker, "base_volume", None),
            "eps": getattr(ticker, "eps", None),
            "p_e_ratio": getattr(ticker, "p_e_ratio", None),
            "p_s_ratio": getattr(ticker, "p_s_ratio", None),
            "nav": getattr(ticker, "nav", None),
            "float_shares": getattr(ticker, "float_shares", None),
            "market_cap": getattr(ticker, "market_cap", None),
            "industry": getattr(ticker, "group_name", None), # industry = group_name
            "capital": getattr(ticker, "total_shares", None) or getattr(ticker, "capital", None), 
            "fiscal_year": getattr(ticker, "fiscal_year", None),
            "flow": getattr(ticker, "flow", None),
            "state": getattr(ticker, "state", None),
            "last_fundamental_update_date": now.date(), 
            "updated_at": now
        }
        session.add(ComprehensiveSymbolData(**comp_data))

        # 2. داده‌های Fundamental
        fund_data = {
            "symbol_id": symbol_id,
            "eps": comp_data.get("eps"),
            "pe": comp_data.get("p_e_ratio"),
            "group_pe_ratio": getattr(ticker, "group_p_e_ratio", None),
            "psr": getattr(ticker, "psr", None),
            "p_s_ratio": comp_data.get("p_s_ratio"),
            "market_cap": comp_data.get("market_cap"),
            "float_shares": comp_data.get("float_shares"),
            "base_volume": comp_data.get("base_volume"),
            "updated_at": now
        }
        session.add(FundamentalData(**fund_data))
        
        return symbol_id

    except Exception as e:
        logger.error(f"Error processing Comprehensive/Fundamental data for {symbol_name}: {e}")
        return None

def extract_and_insert_historical_data(session: Session, symbol_id: str, symbol_name: str, ticker: tse.Ticker, client_types_all: Dict[str, pd.DataFrame]) -> int:
    """استخراج، غنی‌سازی و درج داده‌های Historical (تاریخی) از آبجکت Ticker (بدون Commit)."""
    try:
        df = ticker.history 
        if df is None or df.empty:
            return 0

        df = df.copy()

        # 1.1. همسان‌سازی ستون تاریخ (اصلاحیه 1)
        if "date" not in df.columns:
            df.reset_index(inplace=True)
            if "Date" in df.columns:
                df.rename(columns={"Date": "date"}, inplace=True)
        
        # 1.2. نگاشت ستون‌های اصلی
        df = df.rename(columns=HISTORICAL_COLUMN_MAPPING)
        
        # 2. ادغام داده‌های حقیقی/حقوقی
        if client_types_all and symbol_name in client_types_all:
            df_client = client_types_all.get(symbol_name)
            if df_client is not None and not df_client.empty:
                df_client = df_client.rename(columns=CLIENT_TYPE_COLUMN_MAPPING)
                
                # استانداردسازی ستون تاریخ برای ادغام
                df_client["date"] = pd.to_datetime(df_client["date"]).dt.normalize()
                df["date"] = pd.to_datetime(df["date"]).dt.normalize()
                
                df = pd.merge(
                    df, 
                    df_client[["date"] + list(CLIENT_TYPE_COLUMN_MAPPING.values())],
                    on="date", 
                    how="left"
                )

        # 3. غنی‌سازی و به‌روزرسانی داده‌ها
        
        # الف) محاسبه mv (ارزش بازار)
        price_col = 'final' if 'final' in df.columns and not df['final'].isna().all() else 'close'
        if price_col in df.columns and 'volume' in df.columns:
            # استفاده از .loc برای جلوگیری از SettingWithCopyWarning
            df.loc[:, "mv"] = df[price_col] * df["volume"]
        else:
            df.loc[:, "mv"] = None

        # ب) به‌روزرسانی آخرین رکورد با داده‌های لحظه‌ای (برای دقت بالاتر)
        if not df.empty:
            last_index = df.index[-1]
            
            # Close (آخرین معامله)
            if 'close' in df.columns and hasattr(ticker, "last_price") and ticker.last_price is not None:
                df.loc[last_index, "close"] = ticker.last_price
            
            # Final (قیمت پایانی)
            if 'final' in df.columns and hasattr(ticker, "adj_close") and ticker.adj_close is not None:
                df.loc[last_index, "final"] = ticker.adj_close
                
            # Yesterday Price (قیمت دیروز)
            if 'yesterday_price' in df.columns and hasattr(ticker, "yesterday_price") and ticker.yesterday_price is not None:
                 df.loc[last_index, "yesterday_price"] = ticker.yesterday_price
                 
        # ج) اضافه کردن ستون‌های غایب
        for col in ['plc', 'plp', 'pcc', 'pcp']:
             if col not in df.columns:
                 df[col] = None

        # 4. آماده‌سازی برای درج
        df["symbol_id"] = symbol_id
        df["symbol_name"] = symbol_name
        df["updated_at"] = datetime.now()

        # تبدیل تاریخ‌ها و تمیزکاری (استفاده از تابع ایمن)
        df["jdate"] = df["date"].apply(to_jdate_safe) 
        df = df.where(pd.notnull(df), None)

        # فیلتر کردن ستون‌ها بر اساس مدل HistoricalData (حذف ستون‌های Order Book)
        model_cols = [c.name for c in HistoricalData.__table__.columns]
        order_book_cols = [c.name for c in HistoricalData.__table__.columns if c.name.startswith(('zd', 'qd', 'pd', 'zo', 'qo', 'po'))]
        
        target_cols = [col for col in model_cols if col not in order_book_cols]
        df_to_insert = df[[col for col in df.columns if col in target_cols]]

        # درج انبوه
        records = df_to_insert.to_dict("records")
        if records:
            session.bulk_insert_mappings(HistoricalData, records)
            return len(records)

        return 0

    except Exception as e:
        logger.error(f"Error processing Historical data for {symbol_name}: {e}")
        return -1


# ----------------------------
# ۶. تابع اصلی مدیریت چرخه بازسازی
# ----------------------------

def run_full_rebuild(batch_size: int = 50, commit_batch_size: int = COMMIT_BATCH_SIZE) -> Dict[str, Any]:
    """
    اجرای چرخه کامل بازسازی داده‌ها: پاکسازی، واکشی (یکباره) و درج در بچ‌های کوچک با قابلیت بازیابی.
    """
    session = get_session_local()
    start_time = datetime.now()
    
    try:
        # 1. بارگذاری پیشرفت (Progress)
        start_index = load_progress(PROGRESS_FILE)
        
        if start_index == 0:
            logger.info("Starting a fresh rebuild (index 0).")
            # اگر از ابتدا شروع می‌کنیم، جداول را پاکسازی می‌کنیم
            clear_all_tables(session)
            
        else:
            logger.info(f"Resuming rebuild from index {start_index}...")

        # 2. واکشی داده‌های اولیه (با Retry و Caching)
        all_tickers_data = download_all_symbols_wrapper()
        client_types_all = download_client_types_wrapper()
        
        symbol_names = list(all_tickers_data.keys())
        total_symbols = len(symbol_names)
        logger.info(f"Total {total_symbols} symbol names. Starting batch processing from index {start_index}...")

        symbols_processed = 0 # تعداد کل نمادهایی که تلاش برای پردازش آن‌ها شده
        historical_records_inserted = 0
        errors = 0 # شمارنده خطاها (اصلاحیه 4)
        
        # 3. پردازش بچ به بچ با شروع از start_index
        for i in range(start_index, total_symbols, batch_size):
            # i = شروع بچ فعلی
            batch_symbols = symbol_names[i:i + batch_size]
            current_batch_end_index = i + len(batch_symbols)

            logger.info(f"--- Processing batch (Indices: {i} to {current_batch_end_index-1}) of {total_symbols} symbols ---")

            # 3.1. پردازش نمادهای بچ
            for symbol_name in batch_symbols:
                symbols_processed += 1
                try:
                    # ایجاد Ticker از دیتای کش شده
                    ticker = tse.Ticker(symbol_name)
                    
                    # الف) درج Comprehensive و Fundamental
                    symbol_id = extract_and_insert_symbol_data(session, symbol_name, ticker)
                    if not symbol_id:
                        continue
                    
                    # ب) درج Historical (همراه با ادغام داده‌های حقیقی/حقوقی)
                    num_hist_records = extract_and_insert_historical_data(session, symbol_id, symbol_name, ticker, client_types_all)
                    
                    if num_hist_records > 0:
                        historical_records_inserted += num_hist_records

                    # Commit بچه‌ای برای مدیریت حافظه و دیتابیس
                    if symbols_processed % commit_batch_size == 0:
                        session.commit()
                        logger.info(f"Committed after {symbols_processed} symbols successfully.")

                except Exception as e:
                    errors += 1
                    logger.error(f"Failed to process symbol {symbol_name} ({errors} total errors). Rolling back symbol's data.")
                    session.rollback() 
                    time.sleep(1) 
                    continue
            
            # 3.2. ذخیره پیشرفت و Commit نهایی بچ
            session.commit() 
            save_progress(PROGRESS_FILE, current_batch_end_index)
            logger.info(f"Batch {i//batch_size + 1} finished and progress saved to index {current_batch_end_index}. Waiting...")

            del batch_symbols 
            gc.collect()
            time.sleep(1) 

        # 4. جمع‌بندی نهایی
        delete_progress(PROGRESS_FILE) 
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        logger.info(f"--- FULL REBUILD COMPLETED ---")
        logger.info(
            f"Rebuild summary: {symbols_processed - errors} symbols processed successfully, "
            f"{historical_records_inserted} historical records inserted. "
            f"Total Errors: {errors}. "
            f"Time taken: {round(execution_time / 60, 2)} mins."
        )

        return {
            "status": "SUCCESS",
            "symbols_processed_success": symbols_processed - errors,
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
]