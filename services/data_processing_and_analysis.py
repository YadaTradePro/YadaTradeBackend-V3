# -*- coding: utf-8 -*-
# services/data_processing_and_analysis.py
# مسئولیت: اجرای تحلیل‌های تکنیکال و تشخیص الگوهای شمعی بر اساس داده‌های موجود در دیتابیس.

import logging
import gc
import psutil
import time
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from contextlib import contextmanager
import threading

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
DEFAULT_BATCH_SIZE = 200 # for DB bulk ops & symbol processing
MEMORY_LIMIT_MB = 1500 # warn threshold

# -----------------------------------------------------------
# بخش ۱: مدیریت یکپارچه Session (نسخه Lazy-Loaded)
# -----------------------------------------------------------

SessionLocal: Optional[sessionmaker] = None
_session_lock = threading.Lock()

def _get_session_local() -> sessionmaker:
    """
    ایجاد و بازگرداندن SessionMaker به صورت Lazy (تنبل) و Thread-Safe.
    """
    global SessionLocal
    
    if SessionLocal:
        return SessionLocal
    
    with _session_lock:
        if SessionLocal is None:
            try:
                # 💥 فراخوانی db.engine در زمان نیاز
                SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db.engine)
                logger.info("✅ SessionLocal (sessionmaker) با موفقیت مقداردهی اولیه شد.")
            except Exception as e:
                logger.error(f"❌ امکان اتصال به db.engine برای ساخت SessionLocal وجود ندارد: {e}", exc_info=True)
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
            # 💡 استفاده از Session موجود
            logger.debug("Using external session (from Flask context).")
            yield external_session
        else:
            # 💡 ایجاد Session جدید
            factory = _get_session_local()
            if not factory:
                raise RuntimeError("SessionLocal factory could not be initialized.")
                
            session = factory()
            logger.debug("Creating new local session for background task.")
            
            yield session
            
            # 🚨 تنها زمانی Commit می‌کنیم که خودمان Session را ساخته باشیم (نه Session ورودی از Flask)
            logger.debug("Committing final local session.")
            session.commit() # 👈 مدیریت خودکار Commit نهایی
            
    except Exception as e:
        logger.error(f"Error occurred in session scope: {e}. Rolling back.", exc_info=True)
        if session: 
            session.rollback() # 👈 مدیریت خودکار Rollback
        raise e 
    finally:
        if session: 
            logger.debug("Closing local session.")
            session.close() # 👈 مدیریت خودکار Close


# -----------------------------------------------------------
# توابع کمکی مدیریت حافظه
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
# توابع جدید مدیریت دیتابیس (پاکسازی و بهینه‌سازی)
# -----------------------------------------------------------

def clear_and_vacuum_table(session: Session, model_class: Any):
    """
    پاک کردن کامل محتوای یک جدول (Delete) و سپس اجرای بهینه‌سازی (VACUUM/OPTIMIZE)
    🚨 این تابع پس از حذف، یک Commit صریح انجام می‌دهد تا فضای دیسک را آزاد کند.
    """
    table_name = model_class.__tablename__
    logger.info(f"🗑️ شروع عملیات پاکسازی و بهینه‌سازی برای جدول: **{table_name}**")

    try:
        # 1. حذف کامل رکوردها
        delete_count = session.query(model_class).delete()
        logger.info(f"✅ {delete_count} رکورد از جدول {table_name} حذف شد.")
        
        # 2. ثبت حذف (Commit صریح) - ضروری برای آزاد شدن فضای دیسک قبل از درج مجدد
        session.commit()
        logger.debug(f"💾 Commit حذف رکوردهای جدول {table_name} انجام شد.")
        
        # 3. اجرای VACUUM/OPTIMIZE برای بازپس‌گیری فضای آزاد شده
        dialect = session.bind.dialect.name
        
        if dialect == 'postgresql':
            # VACUUM FULL در PostgreSQL نیاز به Commit جداگانه دارد و زمان‌بر است.
            # برای حفظ تراکنش، از VACUUM معمولی استفاده می‌کنیم و فقط Delete را Commit می‌کنیم.
            try:
                # استفاده از کانکشن مجزا برای VACUUM FULL
                engine = session.bind
                connection = engine.raw_connection()
                try:
                    cursor = connection.cursor()
                    cursor.execute(f"VACUUM FULL ANALYZE {table_name};")
                    connection.commit()
                    logger.info(f"✅ PostgreSQL **VACUUM FULL ANALYZE** بر روی {table_name} اجرا شد (در کانکشن مجزا).")
                finally:
                    connection.close()
            except Exception as e:
                 logger.error(f"❌ خطای VACUUM FULL در PostgreSQL: {e}")
                 # در صورت شکست VACUUM، ادامه می‌دهیم.
        
        elif dialect in ('mysql', 'sqlite'):
            with session.bind.begin() as connection:
                if dialect == 'mysql':
                    optimize_command = text(f"OPTIMIZE TABLE {table_name};")
                    connection.execute(optimize_command)
                    logger.info(f"✅ MySQL **OPTIMIZE TABLE** بر روی {table_name} اجرا شد.")
                elif dialect == 'sqlite':
                    vacuum_command = text("VACUUM;")
                    connection.execute(vacuum_command)
                    logger.info(f"✅ SQLite **VACUUM** اجرا شد.")
        else:
            logger.warning(f"⚠️ بهینه‌سازی پایگاه داده برای {dialect} پشتیبانی نمی‌شود.")

        logger.info(f"🎉 عملیات پاکسازی و بهینه‌سازی جدول {table_name} با موفقیت به پایان رسید.")

    except SQLAlchemyError as e:
        logger.error(f"❌ خطای SQLAlchemy در پاکسازی یا بهینه‌سازی جدول {table_name}: {e}", exc_info=True)
        session.rollback()
        raise

# -----------------------------------------------------------
# تابع ذخیره‌سازی نتایج تحلیل تکنیکال
# -----------------------------------------------------------

def save_technical_indicators(db_session: Session, symbol_id: Union[int, str], df: pd.DataFrame):
    """
    ذخیره (درج یا به‌روزرسانی) نتایج تحلیل تکنیکال.
    این تابع Commit یا Rollback نمی‌کند و فرض بر این است که تابع فراخواننده این کار را انجام می‌دهد.
    """
    symbol_id_str = str(symbol_id)
    
    logger.debug(f"💾 آماده‌سازی اندیکاتورها برای نماد: {symbol_id_str}")

    if 'symbol_id' not in df.columns:
        df['symbol_id'] = symbol_id_str
    else:
        df['symbol_id'] = df['symbol_id'].astype(str)

    # 💡 اصلاح: برای جلوگیری از مشکلات تکراری، استفاده از drop_duplicates
    df_unique = df.drop_duplicates(subset=['symbol_id', 'jdate'], keep='last').copy()
    
    # 💡 اصلاح: استفاده از MACD_Hist به عنوان ستون حیاتی در کنار RSI
    df_to_save = df_unique.dropna(subset=['RSI', 'MACD_Histogram', 'jdate']) 

    if df_to_save.empty:
        logger.debug(f"⚠️ هیچ سطر معتبری برای ذخیره اندیکاتور {symbol_id_str} وجود نداشت.")
        return
        
    updates_count = 0
    inserts_count = 0
    
    records_dict = df_to_save.to_dict('records')
    
    # 💡 اصلاح: فچ کردن فقط رکوردهای موجود با تاریخ‌های موجود در DataFrame
    jdates_in_df = df_to_save['jdate'].unique().tolist()
    
    existing_indicators_query = db_session.query(TechnicalIndicatorData).filter(
        TechnicalIndicatorData.symbol_id == symbol_id_str,
        TechnicalIndicatorData.jdate.in_(jdates_in_df) # 👈 محدود کردن فچ
    )
    
    existing_map = {
        indicator.jdate: indicator 
        for indicator in existing_indicators_query
    }

    for row in records_dict:
        jdate = row.get('jdate')
        if not jdate:
            continue

        # 💡 اطمینان از تطابق نام ستون‌ها با مدل دیتابیس
        data_to_save = {
            'close_price': row.get('close'),
            'RSI': row.get('RSI'),
            'MACD': row.get('MACD'),
            'MACD_Signal': row.get('MACD_Signal'),
            'MACD_Hist': row.get('MACD_Histogram'),
            'SMA_20': row.get('SMA_20'),
            'SMA_50': row.get('SMA_50'),
            'Bollinger_High': row.get('Bollinger_Upper'),
            'Bollinger_Low': row.get('Bollinger_Lower'),
            'Bollinger_MA': row.get('Bollinger_Middle'),
            'Volume_MA_20': row.get('Volume_MA_20'),
            'ATR': row.get('ATR'),
            'Stochastic_K': row.get('Stochastic_K'),
            'Stochastic_D': row.get('Stochastic_D'),
            'squeeze_on': bool(row.get('squeeze_on')),
            'halftrend_signal': row.get('halftrend_signal'),
            'resistance_level_50d': row.get('resistance_level_50d'),
            'resistance_broken': bool(row.get('resistance_broken')),
            'updated_at': datetime.now()
        }

        existing = existing_map.get(jdate)

        if existing:
            # ✅ Update
            for key, value in data_to_save.items():
                setattr(existing, key, value)
            updates_count += 1
        else:
            # ✅ Insert
            data_to_save.update({
                'symbol_id': row.get('symbol_id', symbol_id_str),
                'jdate': jdate,
                'created_at': datetime.now()
            })
            indicator = TechnicalIndicatorData(**data_to_save)
            db_session.add(indicator)
            inserts_count += 1

    if inserts_count > 0 or updates_count > 0:
        logger.info(f"✅ اندیکاتورهای نماد {symbol_id_str} به Session اضافه/آپدیت شدند. (درج: {inserts_count}، بروزرسانی: {updates_count})")
    else:
        logger.debug(f"ℹ️ هیچ داده جدیدی برای نماد {symbol_id_str} یافت نشد.")


# -----------------------------------------------------------
# تابع اصلی اجرای تحلیل تکنیکال (با اضافه شدن پاکسازی و Vacuum)
# -----------------------------------------------------------

def run_technical_analysis(
    db_session: Optional[Session] = None,
    limit: int = None, 
    symbols_list: list = None, 
    batch_size: int = DEFAULT_BATCH_SIZE
) -> Tuple[int, str]:
    """
    اجرای تحلیل تکنیکال در بچ‌های کوچک.
    🔄 ابتدا جدول TechnicalIndicatorData را کامل پاک می‌کند و VACUUM می‌کند.
    """
    # 💡 اگر session ورودی داده نشده، از session_scope استفاده کن، در غیر این صورت، از session ورودی استفاده کن.
    # این ساختار تضمین می‌کند که session_scope Commit/Rollback/Close را مدیریت کند.
    with session_scope(external_session=db_session) as session:
        try:
            logger.info("📈 شروع تحلیل تکنیکال...")

            # 💥 بخش جدید: پاکسازی و بهینه‌سازی (TechnicalIndicatorData)
            clear_and_vacuum_table(session, TechnicalIndicatorData)

            # ⚙️ بخش یافتن نمادها (بدون تغییر)
            independent_session = None
            try:
                factory = _get_session_local()
                independent_session = factory()
                symbol_query = independent_session.query(ComprehensiveSymbolData.symbol_id)
                if symbols_list:
                    symbols_list_str = [str(sym) for sym in symbols_list]
                    symbol_query = symbol_query.filter(ComprehensiveSymbolData.symbol_id.in_(symbols_list_str))
                all_symbols = [row[0] for row in symbol_query.all()]
                if not all_symbols:
                    historical_query = independent_session.query(distinct(HistoricalData.symbol_id))
                    all_symbols = [row[0] for row in historical_query.all()]
            except Exception as e:
                logger.error(f"❌ خطا در session مستقل: {e}")
                symbol_query = session.query(ComprehensiveSymbolData.symbol_id)
                all_symbols = [row[0] for row in symbol_query.all()] if symbol_query else []
            finally:
                if independent_session:
                    independent_session.close()

            total_symbols = len(all_symbols)
            if limit is not None:
                all_symbols = all_symbols[:limit]
                total_symbols = len(all_symbols)

            processed_count = 0
            success_count = 0
            error_count = 0

            for i in range(0, total_symbols, batch_size):
                batch_symbols = all_symbols[i:i + batch_size]
                logger.info(f"📦 پردازش بچ {i // batch_size + 1}: نمادهای {i + 1} تا {min(i + batch_size, total_symbols)}")

                # ⚙️ کوئری داده‌های تاریخی بچ (بدون تغییر)
                query = session.query(
                    HistoricalData.symbol_id, HistoricalData.symbol_name, HistoricalData.date, HistoricalData.jdate, 
                    HistoricalData.open, HistoricalData.close, HistoricalData.high, HistoricalData.low, 
                    HistoricalData.volume, HistoricalData.final, HistoricalData.yesterday_price, HistoricalData.plc, 
                    HistoricalData.plp, HistoricalData.pcc, HistoricalData.pcp, HistoricalData.mv, 
                    HistoricalData.buy_count_i, HistoricalData.buy_count_n, HistoricalData.sell_count_i, 
                    HistoricalData.sell_count_n, HistoricalData.buy_i_volume, HistoricalData.buy_n_volume, 
                    HistoricalData.sell_i_volume, HistoricalData.sell_n_volume
                ).filter(HistoricalData.symbol_id.in_(batch_symbols)).order_by(HistoricalData.symbol_id, HistoricalData.date)
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
                
                grouped = df.groupby('symbol_id')

                for symbol_id, group_df in grouped:
                    processed_count += 1
                    try:
                        if len(group_df) < 5: continue
                        
                        df_indicators = calculate_all_indicators(group_df.copy())
                        save_technical_indicators(session, symbol_id, df_indicators)
                        success_count += 1

                        if processed_count % 10 == 0:
                            logger.info(f"📊 پیشرفت تحلیل: {processed_count}/{total_symbols} نماد")

                    except Exception as e:
                        error_count += 1
                        logger.error(f"❌ خطا در تحلیل نماد {symbol_id}: {e}", exc_info=True)

                # 💥 Commit بچه‌ای
                try:
                    session.commit() # 👈 Commit پس از پردازش موفقیت آمیز تمام نمادهای بچ
                    logger.info(f"💾 بچ {i // batch_size + 1} با موفقیت Commit شد.")
                except Exception as e:
                    session.rollback() # اگر Commit بچه‌ای شکست خورد، Rollback کن
                    logger.error(f"❌ خطای Commit در بچ {i // batch_size + 1}: {e}. Rollback شد.", exc_info=True)

                del df
                del historical_data
                cleanup_memory()

            logger.info(f"✅ تحلیل تکنیکال کامل شد. موفق: {success_count} | خطا: {error_count}")
            return success_count, f"تحلیل کامل شد. {success_count} موفق، {error_count} خطا"

        except Exception as e:
            error_msg = f"❌ خطای عمومی در اجرای تحلیل تکنیکال: {e}"
            logger.error(error_msg, exc_info=True)
            # Rollback در این سطح توسط session_scope مدیریت می‌شود
            return 0, error_msg


# -----------------------------------------------------------
# تابع اصلی اجرای تشخیص الگوهای شمعی (با اضافه شدن Vacuum)
# -----------------------------------------------------------

def run_candlestick_detection(
    db_session: Optional[Session] = None,
    limit: int = None, 
    symbols_list: list = None
) -> int:
    """
    اجرای تشخیص الگوهای شمعی.
    🔄 ابتدا جدول CandlestickPatternDetection را کامل پاک می‌کند و VACUUM می‌کند.
    """
    with session_scope(external_session=db_session) as session:
        try:
            logger.info("🕯️ شروع تشخیص الگوهای شمعی...")
            
            # 💥 بخش جدید: پاکسازی و بهینه‌سازی (CandlestickPatternDetection)
            clear_and_vacuum_table(session, CandlestickPatternDetection)
            
            # ⚙️ بخش یافتن نمادها (بدون تغییر)
            independent_session = None
            try:
                factory = _get_session_local()
                independent_session = factory()
                base_query = independent_session.query(ComprehensiveSymbolData.symbol_id)
                if symbols_list:
                    symbols_list_str = [str(sym) for sym in symbols_list]
                    base_query = base_query.filter(ComprehensiveSymbolData.symbol_id.in_(symbols_list_str))
                symbol_ids_to_process = [str(s[0]) for s in base_query.all()]
                if not symbol_ids_to_process:
                    historical_query = independent_session.query(distinct(HistoricalData.symbol_id))
                    symbol_ids_to_process = [str(s[0]) for s in historical_query.all()]
            except Exception as e:
                # ... Fallback logic ...
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

            # ⚙️ حلقه پردازش نمادها (بدون تغییر در منطق اصلی)
            for symbol_id in symbol_ids_to_process:
                try:
                    historical_data_query = session.query(HistoricalData).filter(
                        HistoricalData.symbol_id == symbol_id
                    ).order_by(HistoricalData.date.desc()).limit(30)
                    
                    historical_data = historical_data_query.all() 
                    
                    if len(historical_data) < 5: 
                        logger.debug(f"⚠️ داده کافی برای نماد {symbol_id} وجود ندارد")
                        continue 

                    df = pd.DataFrame([row.__dict__ for row in historical_data])
                    if '_sa_instance_state' in df.columns:
                        df = df.drop(columns=['_sa_instance_state']) 
                    df.sort_values(by='date', inplace=True) 

                    today_record_dict = df.iloc[-1].to_dict()
                    yesterday_record_dict = df.iloc[-2].to_dict()
                    
                    patterns = check_candlestick_patterns(today_record_dict, yesterday_record_dict, df)
                    
                    if patterns:
                        now = datetime.now()
                        current_jdate = today_record_dict['jdate']
                        for pattern in patterns:
                            records_to_insert.append({
                                'symbol_id': symbol_id,
                                'jdate': current_jdate,
                                'pattern_name': pattern,
                                'created_at': now, 
                                'updated_at': now
                            })
                        success_count += 1
                        
                    processed_count += 1
                    if processed_count % 100 == 0:
                        logger.info(f"🕯️ پیشرفت تشخیص الگوهای شمعی: {processed_count}/{len(symbol_ids_to_process)} نماد")

                except Exception as e:
                    logger.error(f"❌ خطا در تشخیص الگوهای شمعی برای نماد {symbol_id}: {e}", exc_info=True)
            
            logger.info(f"✅ تشخیص الگوهای شمعی برای {success_count} نماد (با {len(records_to_insert)} الگو) انجام شد.")
                        
            # 3. ذخیره نتایج در دیتابیس
            if records_to_insert:
                logger.info(f"💾 در حال درج {len(records_to_insert)} رکورد جدید...")
                
                # 🔍 بررسی تکراری‌ها قبل از درج (بدون تغییر)
                unique_records = {}
                duplicates_count = 0
                for record in records_to_insert:
                    key = (record['symbol_id'], record['jdate'], record['pattern_name'])
                    if key in unique_records:
                        duplicates_count += 1
                    else:
                        unique_records[key] = record
                
                if duplicates_count > 0:
                    records_to_insert = list(unique_records.values())
                    logger.warning(f"⚠️ {duplicates_count} رکورد تکراری حذف شد. {len(records_to_insert)} رکورد برای درج باقی ماند.")
                
                # درج رکوردهای جدید
                session.bulk_insert_mappings(CandlestickPatternDetection, records_to_insert)
                # 💡 Commit نهایی توسط session_scope در انتهای تابع انجام می‌شود
                logger.info(f"✅ {len(records_to_insert)} الگوی شمعی با موفقیت درج شد.")
                
            else:
                logger.info("ℹ️ هیچ الگوی شمعی جدیدی یافت نشد.")

            return success_count

        except Exception as e:
            logger.error(f"❌ خطای کلی در اجرای تشخیص الگوهای شمعی: {e}", exc_info=True)
            # Rollback در این سطح توسط session_scope مدیریت می‌شود
            return 0
