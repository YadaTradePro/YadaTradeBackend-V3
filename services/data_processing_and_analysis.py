# -*- coding: utf-8 -*-
# services/data_processing_and_analysis.py
# مسئولیت: اجرای تحلیل‌های تکنیکال و تشخیص الگوهای شمعی بر اساس داده‌های موجود در دیتابیس.
# 💥 نسخه نهایی: اصلاح خطای 'Working outside of application context' با Lazy Initialization

import logging
import gc
import psutil
import time
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from contextlib import contextmanager
import threading # 👈 [جدید] ایمپورت مورد نیاز برای قفل

from sqlalchemy import func, distinct, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import or_

from flask import current_app

# --- وابستگی‌های پروژه ---
from extensions import db
from models import (
    HistoricalData, 
    TechnicalIndicatorData, 
    CandlestickPatternDetection, 
    ComprehensiveSymbolData
)

from services.technical_analysis_utils import (
        calculate_all_indicators, 
        check_candlestick_patterns
    )

# --- تنظیمات لاگینگ ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --- ثابت‌های مدیریت حافظه و بچ ---
DEFAULT_BATCH_SIZE = 200  # for DB bulk ops & symbol processing
MEMORY_LIMIT_MB = 1500  # warn threshold

# -----------------------------------------------------------
# 💥 [جدید] بخش ۱: مدیریت یکپارچه Session (نسخه Lazy-Loaded)
# -----------------------------------------------------------

# 👈 [اصلاح] SessionLocal را در زمان import مقداردهی اولیه نمی‌کنیم
SessionLocal: Optional[sessionmaker] = None
# 👈 [جدید] یک قفل برای جلوگیری از ایجاد همزمان SessionLocal توسط چند ترد
_session_lock = threading.Lock()

def _get_session_local() -> sessionmaker:
    """
    ایجاد و بازگرداندن SessionMaker به صورت Lazy (تنبل) و Thread-Safe.
    این تابع خطای 'Working outside of application context' را برطرف می‌کند
    زیرا 'db.engine' را تنها زمانی فراخوانی می‌کند که واقعاً به آن نیاز است.
    """
    global SessionLocal
    
    # اگر SessionLocal قبلاً ساخته شده، همان را برگردان
    if SessionLocal:
        return SessionLocal
    
    # اگر ساخته نشده، قفل را بگیر تا فقط یک ترد آن را بسازد
    with _session_lock:
        # دوباره چک کن که ترد دیگری آن را نساخته باشد
        if SessionLocal is None:
            try:
                # 💥 این کد اکنون در زمان اجرا فراخوانی می‌شود، نه در زمان import
                # در این لحظه، اپلیکیشن Flask راه‌اندازی شده و db.engine در دسترس است
                SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db.engine)
                logger.info("✅ SessionLocal (sessionmaker) با موفقیت مقداردهی اولیه شد.")
            except Exception as e:
                logger.error(f"❌ امکان اتصال به db.engine برای ساخت SessionLocal وجود ندارد: {e}", exc_info=True)
                # اگر engine در دسترس نباشد، اجازه ادامه کار را نده
                raise RuntimeError(f"امکان مقداردهی اولیه SessionLocal وجود ندارد: {e}")
        
        return SessionLocal


@contextmanager
def session_scope(external_session: Optional[Session] = None) -> Session:
    """
    مدیریت هوشمند Session برای استفاده در Flask-context یا خارج از آن.
    """
    session = None
    try:
        if external_session:
            # استفاده از Session موجود (از Flask route)
            logger.debug("Using external session (from Flask context).")
            yield external_session
        else:
            # ایجاد Session جدید (برای اسکریپت پس‌زمینه)
            
            # 👈 [اصلاح] استفاده از تابع lazy-initializer
            factory = _get_session_local()
            if not factory:
                 raise RuntimeError("SessionLocal factory could not be initialized.")
                 
            session = factory()
            logger.debug("Creating new local session for background task.")
            
            yield session
            
            logger.debug("Committing local session.")
            session.commit() # 👈 مدیریت خودکار Commit
            
    except Exception as e:
        logger.error(f"Error occurred in session scope: {e}. Rolling back.", exc_info=True)
        if session: # فقط Session ای که خودمان ساختیم را Rollback می‌کنیم
            session.rollback() # 👈 مدیریت خودکار Rollback
        raise e # انتشار مجدد خطا
    finally:
        if session: # فقط Session ای که خودمان ساختیم را Close می‌کنیم
            logger.debug("Closing local session.")
            session.close() # 👈 مدیریت خودکار Close


# -----------------------------------------------------------
# توابع کمکی مدیریت حافظه (بدون تغییر)
# -----------------------------------------------------------

def check_memory_usage_mb() -> float:
    """Return current process memory usage in MB (if psutil available)."""
    try:
        if psutil:
            proc = psutil.Process()
            mem = proc.memory_info().rss / (1024 * 1024)
            return mem
        else:
            return 0.0
    except Exception as e:
        logger.debug("Memory check failed: %s", e)
        return 0.0

def cleanup_memory():
    """پاکسازی حافظه"""
    try:
        gc.collect()
        current_memory = check_memory_usage_mb()
        if current_memory > MEMORY_LIMIT_MB:
            logger.warning(f"⚠️ مصرف حافظه بالا: {current_memory:.2f} MB")
    except Exception as e:
        logger.debug(f"خطا در پاکسازی حافظه: {e}")

# -----------------------------------------------------------
# تابع ذخیره‌سازی نتایج تحلیل تکنیکال
# -----------------------------------------------------------

def save_technical_indicators(db_session: Session, symbol_id: Union[int, str], df: pd.DataFrame):
    """
    ذخیره (درج یا به‌روزرسانی) نتایج تحلیل تکنیکال.
    این تابع دیگر Commit یا Rollback نمی‌کند.
    """
    symbol_id_str = str(symbol_id)
    
    logger.debug(f"💾 آماده‌سازی اندیکاتورها برای نماد: {symbol_id_str}")

    if 'symbol_id' not in df.columns:
        df['symbol_id'] = symbol_id_str
    else:
        df['symbol_id'] = df['symbol_id'].astype(str)

    df_unique = df.drop_duplicates(subset=['symbol_id', 'jdate'], keep='last').copy()
    df_to_save = df_unique.dropna(subset=['RSI', 'MACD', 'jdate'])

    if df_to_save.empty:
        logger.debug(f"⚠️ هیچ سطر معتبری برای ذخیره اندیکاتور {symbol_id_str} وجود نداشت.")
        return
    
    updates_count = 0
    inserts_count = 0
    
    records_dict = df_to_save.to_dict('records')
    
    existing_indicators_query = db_session.query(TechnicalIndicatorData).filter(
        TechnicalIndicatorData.symbol_id == symbol_id_str
    )
    
    existing_map = {
        indicator.jdate: indicator 
        for indicator in existing_indicators_query
    }

    for row in records_dict:
        jdate = row.get('jdate')
        if not jdate:
            continue

        row['symbol_id'] = str(row.get('symbol_id', symbol_id_str))
        existing = existing_map.get(jdate)

        if existing:
            # ✅ Update
            existing.close_price = row.get('close')
            existing.RSI = row.get('RSI')
            existing.MACD = row.get('MACD')
            existing.MACD_Signal = row.get('MACD_Signal')
            existing.MACD_Hist = row.get('MACD_Histogram')
            existing.SMA_20 = row.get('SMA_20')
            existing.SMA_50 = row.get('SMA_50')
            existing.Bollinger_High = row.get('Bollinger_Upper')
            existing.Bollinger_Low = row.get('Bollinger_Lower')
            existing.Bollinger_MA = row.get('Bollinger_Middle')
            existing.Volume_MA_20 = row.get('Volume_MA_20')
            existing.ATR = row.get('ATR')
            existing.Stochastic_K = row.get('Stochastic_K')
            existing.Stochastic_D = row.get('Stochastic_D')
            existing.squeeze_on = bool(row.get('squeeze_on'))
            existing.halftrend_signal = row.get('halftrend_signal')
            existing.resistance_level_50d = row.get('resistance_level_50d')
            existing.resistance_broken = bool(row.get('resistance_broken'))
            updates_count += 1
        else:
            # ✅ Insert
            indicator = TechnicalIndicatorData(
                symbol_id=row['symbol_id'],
                jdate=jdate,
                close_price=row.get('close'),
                RSI=row.get('RSI'),
                MACD=row.get('MACD'),
                MACD_Signal=row.get('MACD_Signal'),
                MACD_Hist=row.get('MACD_Histogram'),
                SMA_20=row.get('SMA_20'),
                SMA_50=row.get('SMA_50'),
                Bollinger_High=row.get('Bollinger_Upper'),
                Bollinger_Low=row.get('Bollinger_Lower'),
                Bollinger_MA=row.get('Bollinger_Middle'),
                Volume_MA_20=row.get('Volume_MA_20'),
                ATR=row.get('ATR'),
                Stochastic_K=row.get('Stochastic_K'),
                Stochastic_D=row.get('Stochastic_D'),
                squeeze_on=bool(row.get('squeeze_on')),
                halftrend_signal=row.get('halftrend_signal'),
                resistance_level_50d=row.get('resistance_level_50d'),
                resistance_broken=bool(row.get('resistance_broken'))
            )
            db_session.add(indicator) # 👈 فقط Add
            inserts_count += 1

    if inserts_count > 0 or updates_count > 0:
        logger.info(f"✅ اندیکاتورهای نماد {symbol_id_str} به Session اضافه/آپدیت شدند. (درج: {inserts_count}، بروزرسانی: {updates_count})")
    else:
        logger.debug(f"ℹ️ هیچ داده جدیدی برای نماد {symbol_id_str} یافت نشد.")


# -----------------------------------------------------------
# تابع اصلی اجرای تحلیل تکنیکال
# -----------------------------------------------------------

def run_technical_analysis(
    db_session: Optional[Session] = None,
    limit: int = None, 
    symbols_list: list = None, 
    batch_size: int = DEFAULT_BATCH_SIZE
) -> Tuple[int, str]:
    """
    اجرای تحلیل تکنیکال در بچ‌های کوچک.
    """
    with session_scope(external_session=db_session) as session:
        try:
            logger.info("📈 شروع تحلیل تکنیکال...")

            # 💥 راه حل: استفاده از session مستقل برای یافتن نمادها
            independent_session = None
            try:
                # ایجاد یک session کاملاً مستقل
                factory = _get_session_local()
                independent_session = factory()
                
                # یافتن نمادها با session مستقل
                symbol_query = independent_session.query(ComprehensiveSymbolData.symbol_id)
                
                if symbols_list:
                    symbols_list_str = [str(sym) for sym in symbols_list]
                    symbol_query = symbol_query.filter(ComprehensiveSymbolData.symbol_id.in_(symbols_list_str))

                all_symbols = [row[0] for row in symbol_query.all()]
                logger.info(f"🔍 [SESSION-INDEPENDENT] {len(all_symbols)} نماد یافت شد")
                
                # اگر بازهم صفر بود، از HistoricalData استفاده کن
                if not all_symbols:
                    logger.warning("⚠️ استفاده از HistoricalData برای یافتن نمادها...")
                    historical_query = independent_session.query(distinct(HistoricalData.symbol_id))
                    all_symbols = [row[0] for row in historical_query.all()]
                    logger.info(f"🔍 [HISTORICAL-FALLBACK] {len(all_symbols)} نماد یافت شد")
                    
            except Exception as e:
                logger.error(f"❌ خطا در session مستقل: {e}")
                # Fallback: استفاده از session اصلی
                symbol_query = session.query(ComprehensiveSymbolData.symbol_id)
                all_symbols = [row[0] for row in symbol_query.all()] if symbol_query else []
                
            finally:
                if independent_session:
                    independent_session.close()

            total_symbols = len(all_symbols)
            logger.info(f"🔍 نهایی: {total_symbols} نماد برای تحلیل تکنیکال")

            if total_symbols == 0:
                logger.error("❌ هیچ نمادی برای تحلیل یافت نشد!")
                return 0, "هیچ نمادی برای تحلیل یافت نشد"

            if limit is not None:
                all_symbols = all_symbols[:limit]
                total_symbols = len(all_symbols)
                logger.info(f"محدودیت {limit} اعمال شد. {total_symbols} نماد پردازش می‌شود.")

            processed_count = 0
            success_count = 0
            error_count = 0

            for i in range(0, total_symbols, batch_size):
                batch_symbols = all_symbols[i:i + batch_size]
                logger.info(f"📦 پردازش بچ {i // batch_size + 1}: نمادهای {i + 1} تا {min(i + batch_size, total_symbols)}")

                # 🔧 استفاده از batch symbols برای کوئری
                query = session.query(
                    HistoricalData.symbol_id, HistoricalData.symbol_name, HistoricalData.date, HistoricalData.jdate, 
                    HistoricalData.open, HistoricalData.close, HistoricalData.high, HistoricalData.low, 
                    HistoricalData.volume, HistoricalData.final, HistoricalData.yesterday_price, HistoricalData.plc, 
                    HistoricalData.plp, HistoricalData.pcc, HistoricalData.pcp, HistoricalData.mv, 
                    HistoricalData.buy_count_i, HistoricalData.buy_count_n, HistoricalData.sell_count_i, 
                    HistoricalData.sell_count_n, HistoricalData.buy_i_volume, HistoricalData.buy_n_volume, 
                    HistoricalData.sell_i_volume, HistoricalData.sell_n_volume
                ).filter(HistoricalData.symbol_id.in_(batch_symbols))

                query = query.order_by(HistoricalData.symbol_id, HistoricalData.date)
                historical_data = query.all()

                if not historical_data:
                    logger.warning(f"⚠️ هیچ داده‌ای برای بچ {i // batch_size + 1} یافت نشد.")
                    continue
                
                columns = [
                    'symbol_id', 'symbol_name', 'date', 'jdate', 'open', 'close', 'high', 'low', 'volume',
                    'final', 'yesterday_price', 'plc', 'plp', 'pcc', 'pcp', 'mv',
                    'buy_count_i', 'buy_count_n', 'sell_count_i', 'sell_count_n',
                    'buy_i_volume', 'buy_n_volume', 'sell_i_volume', 'sell_n_volume'
                ]
                df = pd.DataFrame(historical_data, columns=columns)
                
                if df.empty:
                    logger.warning(f"⚠️ DataFrame خالی برای بچ {i // batch_size + 1}")
                    continue
                    
                grouped = df.groupby('symbol_id')

                for symbol_id, group_df in grouped:
                    processed_count += 1
                    try:
                        if len(group_df) < 5:
                            logger.debug(f"⚠️ داده کافی برای نماد {symbol_id} وجود ندارد ({len(group_df)} رکورد)")
                            continue
                            
                        df_indicators = calculate_all_indicators(group_df.copy())
                        save_technical_indicators(session, symbol_id, df_indicators)  # ✅ اکنون تعریف شده
                        success_count += 1

                        if processed_count % 10 == 0:
                            logger.info(f"📊 پیشرفت تحلیل: {processed_count}/{total_symbols} نماد")

                    except Exception as e:
                        error_count += 1
                        logger.error(f"❌ خطا در تحلیل نماد {symbol_id}: {e}", exc_info=True)

                del df
                del historical_data
                cleanup_memory()

            logger.info(f"✅ تحلیل تکنیکال کامل شد. موفق: {success_count} | خطا: {error_count}")
            return success_count, f"تحلیل کامل شد. {success_count} موفق، {error_count} خطا"

        except Exception as e:
            error_msg = f"❌ خطا در اجرای تحلیل تکنیکال: {e}"
            logger.error(error_msg, exc_info=True)
            return 0, error_msg


# -----------------------------------------------------------
# تابع اصلی اجرای تشخیص الگوهای شمعی
# -----------------------------------------------------------

def run_candlestick_detection(
    db_session: Optional[Session] = None,
    limit: int = None, 
    symbols_list: list = None
) -> int:
    """
    اجرای تشخیص الگوهای شمعی.
    مدیریت Session اکنون به صورت خودکار توسط 'session_scope' انجام می‌شود.
    """
    with session_scope(external_session=db_session) as session:
        try:
            logger.info("🕯️ شروع تشخیص الگوهای شمعی...")
            
            # 🔧 استفاده از session مستقل برای یافتن نمادها
            independent_session = None
            try:
                factory = _get_session_local()
                independent_session = factory()
                
                base_query = independent_session.query(ComprehensiveSymbolData.symbol_id)
                
                if symbols_list:
                    symbols_list_str = [str(sym) for sym in symbols_list]
                    base_query = base_query.filter(ComprehensiveSymbolData.symbol_id.in_(symbols_list_str))
                    
                symbol_ids_raw = [s[0] for s in base_query.all()]
                symbol_ids_to_process = [str(symbol_id) for symbol_id in symbol_ids_raw]
                
                # Fallback به HistoricalData
                if not symbol_ids_to_process:
                    logger.warning("⚠️ استفاده از HistoricalData برای یافتن نمادها...")
                    historical_query = independent_session.query(distinct(HistoricalData.symbol_id))
                    symbol_ids_raw = [s[0] for s in historical_query.all()]
                    symbol_ids_to_process = [str(symbol_id) for symbol_id in symbol_ids_raw]
                    
            except Exception as e:
                logger.error(f"❌ خطا در session مستقل: {e}")
                base_query = session.query(ComprehensiveSymbolData.symbol_id)
                symbol_ids_raw = [s[0] for s in base_query.all()] if base_query else []
                symbol_ids_to_process = [str(symbol_id) for symbol_id in symbol_ids_raw]
                
            finally:
                if independent_session:
                    independent_session.close()
            
            if not symbol_ids_to_process:
                logger.warning("⚠️ هیچ نمادی برای تشخیص الگوهای شمعی یافت نشد.")
                return 0
                
            if limit is not None:
                symbol_ids_to_process = symbol_ids_to_process[:limit]
                
            logger.info(f"🔍 یافت شد {len(symbol_ids_to_process)} نماد برای تشخیص الگوهای شمعی")

            success_count = 0
            records_to_insert = []
            processed_count = 0
            today_jdate_str = None

            for symbol_id in symbol_ids_to_process:
                try:
                    historical_data_query = session.query(HistoricalData).filter(
                        HistoricalData.symbol_id == symbol_id
                    ).order_by(HistoricalData.date.desc()).limit(30)
                    
                    historical_data = historical_data_query.all() 
                    
                    if len(historical_data) < 5:
                        logger.debug(f"⚠️ داده کافی برای نماد {symbol_id} وجود ندارد ({len(historical_data)} رکورد)")
                        continue 

                    df = pd.DataFrame([row.__dict__ for row in historical_data])
                    if '_sa_instance_state' in df.columns:
                        df = df.drop(columns=['_sa_instance_state']) 
                    
                    df.sort_values(by='date', inplace=True) 

                    today_record_dict = df.iloc[-1].to_dict()
                    yesterday_record_dict = df.iloc[-2].to_dict()
                    
                    patterns = check_candlestick_patterns(
                        today_record_dict, 
                        yesterday_record_dict, 
                        df
                    )
                    
                    if patterns:
                        now = datetime.now()
                        current_jdate = today_record_dict['jdate']
                        if today_jdate_str is None:
                            today_jdate_str = current_jdate

                        for pattern in patterns:
                            records_to_insert.append({
                                'symbol_id': symbol_id,
                                'jdate': current_jdate,
                                'pattern_name': pattern,
                                'created_at': now, 
                                'updated_at': now
                            })
                        success_count += 1
                        logger.debug(f"✅ الگوهای یافت شده برای نماد {symbol_id}: {patterns}")
                    
                    processed_count += 1
                    if processed_count % 100 == 0:
                        logger.info(f"🕯️ پیشرفت تشخیص الگوهای شمعی: {processed_count}/{len(symbol_ids_to_process)} نماد")

                except Exception as e:
                    logger.error(f"❌ خطا در تشخیص الگوهای شمعی برای نماد {symbol_id}: {e}", exc_info=True)
                    
            logger.info(f"✅ تشخیص الگوهای شمعی برای {success_count} نماد (با {len(records_to_insert)} الگو) انجام شد.")
                    
            # 3. ذخیره نتایج در دیتابیس (تراکنش یکپارچه)
            if records_to_insert:
                if not today_jdate_str:
                    today_jdate_str = records_to_insert[0]['jdate'] 

                processed_symbol_ids_set = list({record['symbol_id'] for record in records_to_insert})
                
                # ب) حذف رکوردهای قدیمی
                logger.info(f"🗑️ در حال حذف الگوهای شمعی قبلی برای {len(processed_symbol_ids_set)} نماد در تاریخ {today_jdate_str}...")
                
                session.query(CandlestickPatternDetection).filter(
                    CandlestickPatternDetection.symbol_id.in_(processed_symbol_ids_set),
                    CandlestickPatternDetection.jdate == today_jdate_str
                ).delete(synchronize_session=False) 
                
                # ج) درج رکوردهای جدید
                session.bulk_insert_mappings(CandlestickPatternDetection, records_to_insert)
                logger.info(f"✅ {len(records_to_insert)} الگوی شمعی با موفقیت در Session درج شد.")
                
            else:
                logger.info("ℹ️ هیچ الگوی شمعی جدیدی یافت نشد.")

            return success_count

        except Exception as e:
             logger.error(f"❌ خطای کلی در اجرای تشخیص الگوهای شمعی: {e}", exc_info=True)
             return 0

# -----------------------------------------------------------
# توابع Export
# -----------------------------------------------------------

__all__ = [
    # توابع اصلی اجرا کننده تحلیل
    'run_technical_analysis',
    'run_candlestick_detection',
    
    # توابع کمکی مورد نیاز
    # 'get_session_local', (حذف شد)
    'cleanup_memory',
    'save_technical_indicators'
]
