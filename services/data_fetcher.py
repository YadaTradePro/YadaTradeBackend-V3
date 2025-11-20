# -*- coding: utf-8 -*-
# services/data_fetcher.py
# نسخه: B+ — بهینه‌شده، سازگار با get_stats و با fallback برای client_types
# مسئولیت: دریافت و پردازش داده‌های تاریخی و بنیادی (Historical & Fundamental)

import logging
import pytse_client as tse
from pytse_client import download_client_types_records, get_stats
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
from typing import List, Dict, Optional, Any, Tuple, Union
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import SQLAlchemyError, OperationalError, IntegrityError
from extensions import db
from models import ComprehensiveSymbolData, HistoricalData, FundamentalData
from sqlalchemy import text

import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module='pytse_client')

# ----------------------------
# تنظیمات پایه
# ----------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PROGRESS_FILE = "rebuild_progress.json"
COMMIT_BATCH_SIZE = 100
MAX_RETRY_BACKOFF = 300  # cap backoff

# نگاشت ستون‌های تاریخچه (Ticker.history) به دیتابیس
HISTORICAL_COLUMN_MAPPING = {
    'Date': 'date',
    'Open': 'open',
    'High': 'high',
    'Low': 'low',
    'Final': 'final',
    'adjClose': 'final',
    'Close': 'close',
    'Volume': 'volume',
    'Value': 'value',
    'Count': 'num_trades',
    'PLast': 'yesterday_price',
    'yesterday': 'yesterday_price',
}

CLIENT_TYPE_COLUMN_MAPPING = {
    "individual_buy_count": "buy_count_i",
    "corporate_buy_count": "buy_count_n",
    "individual_sell_count": "sell_count_i",
    "corporate_sell_count": "sell_count_n",
    "individual_buy_vol": "buy_i_volume",
    "corporate_buy_vol": "buy_n_volume",
    "individual_sell_vol": "sell_i_volume",
    "corporate_sell_vol": "sell_n_volume",
}

# ----------------------------
# ۱. توابع کمکی
# ----------------------------

def get_value_safely(obj: Any, key: str, default: Any = None) -> Any:
    """دریافت ایمن مقدار از دیکشنری یا آبجکت."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    try:
        return getattr(obj, key, default)
    except Exception:
        return default


def safe_get_ticker_attr(ticker_obj: Any, attr_name: str, default: Any = None) -> Any:
    """دریافت ایمن ویژگی از Ticker."""
    try:
        val = getattr(ticker_obj, attr_name, default)
        return val if val is not None else default
    except Exception:
        return default


def is_symbol_valid(symbol_name: str, market_type_name: str) -> bool:
    """بررسی اعتبار نماد (حذف حق تقدم، مشتقه و...)"""
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


def retry_on_exception(max_retries=5, delay=30, backoff=2.0, allowed_exceptions=(Exception, OperationalError)):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            wait = delay
            last_exc = None
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except allowed_exceptions as e:
                    last_exc = e
                    retries += 1
                    logger.warning(f"Error in {func.__name__}: {e}. Retrying {retries}/{max_retries} after {wait:.1f}s...")
                    sleep_time = wait + random.uniform(0, min(5, wait))
                    time.sleep(sleep_time)
                    wait = min(wait * backoff, MAX_RETRY_BACKOFF)
            logger.error(f"Max retries exceeded for {func.__name__}: {last_exc}")
            raise last_exc
        return wrapper
    return decorator


def to_jdate_safe(d: Any) -> Optional[str]:
    if d is None or (isinstance(d, float) and np.isnan(d)):
        return None
    try:
        if hasattr(d, "date"):
            g_date = d.date()
        else:
            g_date = d
        if isinstance(g_date, str):
            g_date = datetime.strptime(g_date, "%Y-%m-%d").date()
        return jdatetime.date.fromgregorian(date=g_date).strftime("%Y-%m-%d")
    except Exception:
        return None


def log_error_dataframe(symbol_name: str, df: pd.DataFrame, error: Exception) -> None:
    DEBUG_DIR = "debug_data"
    os.makedirs(DEBUG_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        df_log = df.copy()
        for col in df_log.select_dtypes(include=['datetime64', 'datetime64[ns]']).columns:
            df_log[col] = df_log[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        df_log = df_log.replace({pd.NaT: 'NaT_Found', np.nan: None})

        log_data = {
            "symbol": symbol_name,
            "error": str(error),
            "timestamp": timestamp,
            "df_data": df_log.head(10).to_dict('records')
        }
        with open(os.path.join(DEBUG_DIR, f"{symbol_name}_{timestamp}_error.json"), 'w', encoding='utf-8') as f:
            json.dump(log_data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.exception(f"Failed to write debug dataframe for {symbol_name}: {e}")


# ----------------------------
# ۲. مدیریت دیتابیس
# ----------------------------

def get_session_local() -> Session:
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
    logger.warning("Step 0.2: Clearing tables...")
    try:
        session.query(HistoricalData).delete(synchronize_session='fetch')
        session.query(FundamentalData).delete(synchronize_session='fetch')
        session.query(ComprehensiveSymbolData).delete(synchronize_session='fetch')
        session.commit()
        logger.info("Step 0.2 Completed: Tables cleared.")
    except Exception as e:
        session.rollback()
        logger.exception("Error clearing tables.")
        raise e


def vacuum_database(session: Session) -> None:
    try:
        if hasattr(session.bind, "name") and session.bind.name == 'sqlite':
            logger.warning("Step 0.3: Running VACUUM...")
            session.execute(text("VACUUM"))
            session.commit()
    except Exception:
        logger.exception("VACUUM failed or not applicable.")


# ----------------------------
# ۳. مدیریت پیشرفت
# ----------------------------

def save_progress(progress_file: str, current_index: int) -> None:
    try:
        with open(progress_file, "w", encoding="utf-8") as f:
            json.dump({"last_index": int(current_index)}, f)
    except Exception:
        logger.exception("Failed to save progress.")


def load_progress(progress_file: str) -> int:
    try:
        with open(progress_file, "r", encoding="utf-8") as f:
            return int(json.load(f).get("last_index", 0))
    except Exception:
        return 0


def delete_progress(progress_file: str) -> None:
    try:
        if os.path.exists(progress_file):
            os.remove(progress_file)
    except Exception:
        logger.exception("Failed to delete progress file.")


# ----------------------------
# ۴. توابع واکشی داده
# ----------------------------

@retry_on_exception(max_retries=5, delay=10, backoff=2.0)
def download_all_symbols_wrapper() -> List[str]:
    """دریافت لیست نمادها با استفاده از get_stats (پایدارتر از download(symbols='all'))."""
    logger.info("Step 1.1: Fetching symbols list via get_stats()...")
    try:
        df = get_stats(to_csv=False)
    except Exception as e:
        logger.exception(f"get_stats() failed: {e}")
        return []

    if df is None or df.empty:
        logger.error("get_stats() returned empty DataFrame.")
        return []

    if "symbol" not in df.columns:
        logger.error("get_stats() missing column 'symbol'.")
        return []

    symbols = df["symbol"].dropna().astype(str).unique().tolist()
    logger.info(f"Fetched {len(symbols)} symbols from TSETMC stats API.")
    return symbols


@retry_on_exception(max_retries=3, delay=5, backoff=2.0)
def download_client_types_wrapper() -> Dict[str, pd.DataFrame]:
    """Improved: do NOT fetch all at once — pytse is unstable.
    Instead, return an EMPTY dict. Client types will be fetched per-symbol.
    """
    logger.info("Step 1.2: Skipping global client types download — using per-symbol fetch.")
    return {}



# ----------------------------
# ۵. پردازش و درج داده‌ها
# ----------------------------

def _prepare_client_df_for_merge(df_client: pd.DataFrame) -> Optional[pd.DataFrame]:
    try:
        if df_client is None or not isinstance(df_client, pd.DataFrame) or df_client.empty:
            return None

        cols = [c.lower() for c in df_client.columns]
        if 'date' not in cols:
            for c in df_client.columns:
                if re.search(r"date", str(c), re.IGNORECASE):
                    df_client = df_client.rename(columns={c: 'date'})
                    break

        if 'date' not in df_client.columns:
            return None

        df_client['date'] = pd.to_datetime(df_client['date'], errors='coerce').dt.normalize()
        if df_client['date'].isna().all():
            return None

        rename_map = {}
        for src, tgt in CLIENT_TYPE_COLUMN_MAPPING.items():
            for col in df_client.columns:
                if col.lower() == src.lower() or re.sub(r'[_\s]', '', col.lower()) == re.sub(r'[_\s]', '', src.lower()):
                    rename_map[col] = tgt
        if rename_map:
            df_client = df_client.rename(columns=rename_map)

        keep_cols = ['date'] + list(set(CLIENT_TYPE_COLUMN_MAPPING.values()) & set(df_client.columns))
        df_client = df_client[keep_cols].copy()

        for c in df_client.columns:
            if c == 'date':
                continue
            df_client[c] = pd.to_numeric(df_client[c], errors='coerce').fillna(0)

        return df_client.reset_index(drop=True)
    except Exception:
        logger.exception("Error preparing client types DataFrame.")
        return None


def extract_and_insert_symbol_data(session: Session, symbol_name: str, ticker: tse.Ticker) -> Optional[str]:
    try:
        symbol_id = getattr(ticker, "index", None)
        if not symbol_id:
            return None

        now = datetime.now()
        market_data = safe_get_ticker_attr(ticker, "market") or safe_get_ticker_attr(ticker, "flow")

        float_percent = safe_get_ticker_attr(ticker, "float_shares")
        total_shares = safe_get_ticker_attr(ticker, "total_shares")
        float_shares_count = None
        try:
            if float_percent is not None and total_shares is not None:
                float_shares_count = (float(float_percent) * float(total_shares)) / 100
        except Exception:
            float_shares_count = None

        comp_data = {
            "symbol_id": symbol_id,
            "symbol_name": symbol_name,
            "company_name": safe_get_ticker_attr(ticker, "title"),
            "isin": safe_get_ticker_attr(ticker, "isin"),
            "tse_index": symbol_id,
            "market_type": market_data,
            "group_name": safe_get_ticker_attr(ticker, "group_name"),
            "base_volume": safe_get_ticker_attr(ticker, "base_volume"),
            "eps": safe_get_ticker_attr(ticker, "eps"),
            "p_e_ratio": safe_get_ticker_attr(ticker, "p_e_ratio"),
            "p_s_ratio": safe_get_ticker_attr(ticker, "p_s_ratio"),
            "nav": safe_get_ticker_attr(ticker, "nav"),
            "float_shares": float_shares_count,
            "market_cap": safe_get_ticker_attr(ticker, "market_cap"),
            "industry": safe_get_ticker_attr(ticker, "group_name"),
            "capital": total_shares,
            "fiscal_year": safe_get_ticker_attr(ticker, "fiscal_year"),
            "flow": safe_get_ticker_attr(ticker, "flow"),
            "state": safe_get_ticker_attr(ticker, "state"),
            "last_fundamental_update_date": now.date(),
            "updated_at": now
        }
        session.add(ComprehensiveSymbolData(**comp_data))
        return symbol_id
    except Exception as e:
        logger.exception(f"Error inserting Comprehensive data for {symbol_name}: {e}")
        return None


def extract_and_insert_historical_data(session: Session, symbol_id: str, symbol_name: str, ticker: tse.Ticker,
                                       client_types_all: Dict[str, pd.DataFrame]) -> int:
    df = None
    try:
        df = getattr(ticker, "history", None)
        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            return 0

        df = df.copy()

        if "date" not in df.columns:
            df.reset_index(inplace=True)
            if "Date" in df.columns:
                df.rename(columns={"Date": "date"}, inplace=True)
            elif "DATE" in df.columns:
                df.rename(columns={"DATE": "date"}, inplace=True)

        for source, target in HISTORICAL_COLUMN_MAPPING.items():
            if source in df.columns:
                df.rename(columns={source: target}, inplace=True)
            elif source.lower() in [c.lower() for c in df.columns]:
                for c in df.columns:
                    if c.lower() == source.lower():
                        df.rename(columns={c: target}, inplace=True)
                        break

        if 'final' not in df.columns and 'adjClose' in df.columns:
            df.rename(columns={'adjClose': 'final'}, inplace=True)

        # 2. client types: ابتدا از bulk dict تلاش کن، در غیر اینصورت از ticker.client_types استفاده کن
        df_client = None
        if client_types_all and isinstance(client_types_all, dict):
            raw_client = client_types_all.get(symbol_name) or client_types_all.get(str(symbol_id))
            if raw_client is not None:
                df_client = _prepare_client_df_for_merge(raw_client)

        if df_client is None:
            # fallback: تلاش برای خواندن از ticker.client_types (یک نماد احتمالاً اطلاعات دارد)
            try:
                raw_ct = getattr(ticker, 'client_types', None)
                if isinstance(raw_ct, pd.DataFrame) and not raw_ct.empty:
                    df_client = _prepare_client_df_for_merge(raw_ct)
            except Exception:
                pass

        if df_client is not None:
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.normalize()
            df = df.dropna(subset=['date'])
            if not df.empty:
                try:
                    df = pd.merge(df, df_client, on="date", how="left")
                except Exception:
                    logger.exception(f"Merge failed for client types on {symbol_name}. Skipping merge.")
        else:
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df.dropna(subset=['date'], inplace=True)

        if df.empty:
            return 0

        df.drop_duplicates(subset=['date'], keep='last', inplace=True)
        df.sort_values(by='date', inplace=True)

        numeric_cols = ['open', 'high', 'low', 'final', 'close', 'volume', 'value', 'num_trades',
                        'yesterday_price'] + list(CLIENT_TYPE_COLUMN_MAPPING.values())

        for col in numeric_cols:
            if col in df.columns:
                if col in ['num_trades'] + list(CLIENT_TYPE_COLUMN_MAPPING.values()):
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype(pd.Int64Dtype()).fillna(pd.NA)
                else:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype(float)

        if 'yesterday_price' not in df.columns or df['yesterday_price'].isna().all():
            if 'close' in df.columns:
                df['yesterday_price'] = df['close'].shift(1)
            else:
                df['yesterday_price'] = np.nan

        try:
            if not df.empty:
                first_idx = df.index[0]
                if pd.isna(df.at[first_idx, 'yesterday_price']):
                    if 'open' in df.columns and not pd.isna(df.at[first_idx, 'open']):
                        df.at[first_idx, 'yesterday_price'] = df.at[first_idx, 'open']
        except Exception:
            logger.debug("Failed to set first yesterday_price safe fallback.")

        if 'value' in df.columns and not df['value'].isna().all():
            df['mv'] = df['value'].astype(float)
        else:
            df['mv'] = (df['final'].fillna(0.0) * df['volume'].fillna(0.0)).astype(float)

        yesterday_safe = df['yesterday_price'].astype(float).replace(0, np.nan)

        df['pcc'] = df['final'].astype(float) - df['yesterday_price'].astype(float)
        df['pcp'] = (df['pcc'] / yesterday_safe) * 100

        df['plc'] = df['close'].astype(float) - df['yesterday_price'].astype(float)
        df['plp'] = (df['plc'] / yesterday_safe) * 100

        buy_i_vol = df.get('buy_i_volume', pd.Series(0)).astype(float) if 'buy_i_volume' in df.columns else pd.Series(0)
        sell_i_vol = df.get('sell_i_volume', pd.Series(0)).astype(float) if 'sell_i_volume' in df.columns else pd.Series(0)
        buy_count_i = df.get('buy_count_i', pd.Series(0)).astype(float) if 'buy_count_i' in df.columns else pd.Series(0)
        sell_count_i = df.get('sell_count_i', pd.Series(0)).astype(float) if 'sell_count_i' in df.columns else pd.Series(0)

        with np.errstate(divide='ignore', invalid='ignore'):
            real_buy_avg = buy_i_vol / buy_count_i.replace({0: np.nan})
            real_sell_avg = sell_i_vol / sell_count_i.replace({0: np.nan})
            df['real_buy_vol_avg'] = real_buy_avg.replace([np.inf, -np.inf], np.nan).fillna(0.0)
            df['real_sell_vol_avg'] = real_sell_avg.replace([np.inf, -np.inf], np.nan).fillna(0.0)

            df['real_power_ratio'] = df['real_buy_vol_avg'] / df['real_sell_vol_avg'].replace({0: np.nan})
            df['real_power_ratio'] = df['real_power_ratio'].replace([np.inf, -np.inf], np.nan).fillna(0.0)

        df['daily_liquidity'] = (buy_i_vol * df['close'].astype(float)) - (sell_i_vol * df['close'].astype(float))

        df['volume_20d_avg'] = df['volume'].rolling(window=20, min_periods=1).mean()
        vol_avg_safe = df['volume_20d_avg'].replace(0, np.nan)
        df['volume_ratio_20d'] = df['volume'].astype(float) / vol_avg_safe

        df["symbol_id"] = symbol_id
        df["symbol_name"] = symbol_name
        df["updated_at"] = datetime.now()
        df["jdate"] = df["date"].apply(to_jdate_safe)

        fund_static = {
            "eps": safe_get_ticker_attr(ticker, "eps"),
            "pe": safe_get_ticker_attr(ticker, "p_e_ratio"),
            "group_pe_ratio": safe_get_ticker_attr(ticker, "group_p_e_ratio"),
            "market_cap": safe_get_ticker_attr(ticker, "market_cap"),
            "base_volume": safe_get_ticker_attr(ticker, "base_volume"),
            "float_shares": safe_get_ticker_attr(ticker, "float_shares"),
        }

        df_fund = df.copy()
        for k, v in fund_static.items():
            df_fund[k] = v

        fund_cols = [c.name for c in FundamentalData.__table__.columns]
        fund_records = df_fund[[c for c in df_fund.columns if c in fund_cols]].to_dict("records")
        cleaned_fund = [{k: (None if pd.isna(v) else v) for k, v in r.items()} for r in fund_records]

        if cleaned_fund:
            try:
                session.bulk_insert_mappings(FundamentalData, cleaned_fund)
                logger.info(f"Step 2.3.1: {len(cleaned_fund)} Fundamental and Historical records inserted for {symbol_name}.")
            except IntegrityError:
                session.rollback()
                logger.warning(f"Duplicate Fundamental data for {symbol_name}, skipping bulk insert.")
            except Exception:
                session.rollback()
                logger.exception(f"Failed to bulk insert Fundamental for {symbol_name}.")

        hist_cols = [c.name for c in HistoricalData.__table__.columns]
        hist_records = df[[c for c in df.columns if c in hist_cols]].to_dict("records")
        cleaned_hist = [{k: (None if pd.isna(v) else v) for k, v in r.items()} for r in hist_records]

        if cleaned_hist:
            try:
                session.bulk_insert_mappings(HistoricalData, cleaned_hist)
                return len(cleaned_hist)
            except IntegrityError:
                session.rollback()
                logger.warning(f"Duplicate Historical data for {symbol_name}.")
                return 0
            except Exception:
                session.rollback()
                logger.exception(f"Failed to bulk insert Historical for {symbol_name}.")
                return -1

        return 0

    except Exception as e:
        session.rollback()
        if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
            log_error_dataframe(symbol_name, df, e)
        logger.exception(f"Error processing data for {symbol_name}: {e}")
        return -1


# ----------------------------
# ۶. تابع اصلی
# ----------------------------

def run_full_rebuild(batch_size: int = 50, commit_batch_size: int = COMMIT_BATCH_SIZE) -> Dict[str, Any]:
    logger.info("--- Starting Full Weekly Data Rebuild ---")
    session = get_session_local()
    start_time = datetime.now()

    try:
        start_index = load_progress(PROGRESS_FILE)
        if start_index == 0:
            logger.info("Step 0.1: FRESH rebuild.")
            clear_all_tables(session)
            vacuum_database(session)
        else:
            logger.warning(f"Step 0.1: Resuming from {start_index}...")

        # Symbols via get_stats
        all_symbols = download_all_symbols_wrapper()
        client_types = download_client_types_wrapper()

        raw_symbols = all_symbols if isinstance(all_symbols, list) else []

        valid_symbols = []
        for s in raw_symbols:
            try:
                # در این حالت دیگر داده متادیتا از all_symbols نداریم، بنابراین برای market_type یک تیکر سبک بساز
                try:
                    tmp_t = tse.Ticker(s)
                    m_type = safe_get_ticker_attr(tmp_t, 'market') or safe_get_ticker_attr(tmp_t, 'flow')
                except Exception:
                    m_type = None

                if is_symbol_valid(s, m_type):
                    valid_symbols.append(s)
            except Exception:
                logger.exception(f"Error evaluating symbol {s}, skipping.")

        total_valid = len(valid_symbols)
        logger.info(f"Step 1.3: {total_valid} VALID symbols.")

        if start_index >= total_valid:
            start_index = 0
            delete_progress(PROGRESS_FILE)

        processed = 0
        errors = 0
        commit_counter = 0

        # run in batches
        for i in range(start_index, total_valid, batch_size):
            batch = valid_symbols[i:i + batch_size]
            logger.info(f"--- Batch {i} to {i + len(batch)} ---")

            for idx, sym in enumerate(batch):
                global_idx = i + idx + 1
                try:
                    logger.info(f"Processing {sym} ({global_idx}/{total_valid})...")

                    try:
                        ticker = tse.Ticker(sym)
                    except Exception:
                        logger.warning(f"Skipping {sym}: Ticker init failed.")
                        errors += 1
                        continue

                    if not getattr(ticker, "index", None):
                        time.sleep(1)
                        try:
                            ticker = tse.Ticker(sym)
                        except Exception:
                            pass

                    if not getattr(ticker, "index", None):
                        logger.warning(f"Skipping {sym}: No Index.")
                        errors += 1
                        continue

                    sid = extract_and_insert_symbol_data(session, sym, ticker)
                    if not sid:
                        errors += 1
                        continue

                    res = extract_and_insert_historical_data(session, sid, sym, ticker, client_types)
                    if res >= 0:
                        processed += 1
                    else:
                        errors += 1

                    commit_counter += 1
                    if commit_counter >= commit_batch_size:
                        try:
                            session.commit()
                            logger.info(f"Committed at processed={processed}, errors={errors}")
                        except Exception:
                            session.rollback()
                            logger.exception("Commit failed, rollback performed.")
                        commit_counter = 0

                except Exception as e:
                    logger.exception(f"Failed {sym}: {e}")
                    session.rollback()
                    errors += 1

            try:
                session.commit()
            except Exception:
                session.rollback()
                logger.exception("Commit failed at batch end, rollback performed.")

            save_progress(PROGRESS_FILE, i + len(batch))
            gc.collect()

        try:
            session.commit()
        except Exception:
            session.rollback()
            logger.exception("Final commit failed, rollback performed.")

        delete_progress(PROGRESS_FILE)
        elapsed = datetime.now() - start_time
        logger.info(f"Full rebuild finished in {elapsed}. processed={processed}, errors={errors}")
        return {"status": "SUCCESS", "processed": processed, "errors": errors, "elapsed": str(elapsed)}

    except Exception as e:
        logger.exception(f"Fatal Error: {e}")
        return {"status": "FAILED", "error": str(e)}
    finally:
        try:
            session.close()
        except Exception:
            pass


__all__ = ["get_session_local", "run_full_rebuild"]
