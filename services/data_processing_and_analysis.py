# -*- coding: utf-8 -*-
# services/data_processing_and_analysis.py
# مسئولیت: اجرای تحلیل‌های تکنیکال و تشخیص الگوهای شمعی بر اساس داده‌های موجود در دیتابیس.
# این فایل داده‌ای را واکشی نمی‌کند، بلکه داده‌های پردازش شده (HistoricalData) را می‌خواند،
# تحلیل می‌کند و نتایج را در جداول (TechnicalIndicatorData, CandlestickPatternDetection) ذخیره می‌کند.

import logging
import gc
import psutil
import time
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union

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
# توابع کمکی (Session و مدیریت حافظه)
# -----------------------------------------------------------

def get_session_local():
    """ایجاد session local با application context"""
    try:
        from flask import current_app
        with current_app.app_context():
            return sessionmaker(bind=db.engine)()
    except RuntimeError:
        # اگر خارج از application context هستیم
        try:
            # تلاش برای گرفتن موتور از db.get_engine() اگر در فایل extensions موجود باشد
            return sessionmaker(bind=db.get_engine())()
        except AttributeError:
            # Fallback نهایی اگر db.get_engine() وجود نداشته باشد
            logger.error("Could not create session outside of Flask context and db.get_engine() not found.")
            # استفاده از db.session ممکن است در سناریوهای خاصی کار کند اما امن نیست
            return db.session 


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
# تابع اصلی اجرای تحلیل تکنیکال
# -----------------------------------------------------------

def run_technical_analysis(
    db_session: Session, 
    limit: int = None, 
    symbols_list: list = None, 
    batch_size: int = DEFAULT_BATCH_SIZE
) -> Tuple[int, str]:
    """
    اجرای تحلیل تکنیکال در بچ‌های کوچک برای جلوگیری از مصرف زیاد حافظه.
    این تابع داده‌ها را از HistoricalData می‌خواند، با technical_analysis_utils.calculate_all_indicators
    تحلیل می‌کند و با save_technical_indicators در TechnicalIndicatorData ذخیره می‌نماید.
    """
    try:
        logger.info("📈 شروع تحلیل تکنیکال...")

        # دریافت لیست یکتا از نمادها در HistoricalData
        symbol_ids_query = db_session.query(HistoricalData.symbol_id).distinct()
        
        if symbols_list:
            # symbols_list می‌تواند شامل symbol_id (str) یا id (int) باشد
            # ما در اینجا به symbol_id (که در HistoricalData است) نیاز داریم
            symbol_ids_query = symbol_ids_query.filter(HistoricalData.symbol_id.in_(symbols_list))

        all_symbols = [row[0] for row in symbol_ids_query.all()]
        
        total_symbols = len(all_symbols)
        logger.info(f"🔍 مجموع {total_symbols} نماد برای تحلیل تکنیکال یافت شد")

        if limit is not None:
            all_symbols = all_symbols[:limit]
            total_symbols = len(all_symbols)
            logger.info(f"محدودیت {limit} اعمال شد. {total_symbols} نماد پردازش می‌شود.")

        processed_count = 0
        success_count = 0
        error_count = 0

        # اجرای تحلیل در بچ‌های Nتایی
        for i in range(0, total_symbols, batch_size):
            batch_symbols = all_symbols[i:i + batch_size]
            logger.info(f"📦 پردازش بچ {i // batch_size + 1}: نمادهای {i + 1} تا {min(i + batch_size, total_symbols)}")

            # واکشی تمام داده‌های تاریخی مورد نیاز برای این بچ
            query = db_session.query(
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
                logger.warning(f"⚠️ هیچ داده‌ای برای این بچ یافت نشد.")
                continue

            # تعریف ستون‌ها بر اساس کوئری بالا
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
                    # --- فراخوانی تابع تحلیل از Utils ---
                    # group_df شامل تمام داده‌های تاریخی مرتب شده برای یک نماد است
                    df_indicators = calculate_all_indicators(group_df.copy())
                    
                    # --- ذخیره نتایج در دیتابیس ---
                    save_technical_indicators(db_session, symbol_id, df_indicators)
                    success_count += 1

                    if processed_count % 10 == 0:
                         logger.info(f"📊 پیشرفت تحلیل: {processed_count}/{total_symbols} نماد")

                except Exception as e:
                    error_count += 1
                    logger.error(f"❌ خطا در تحلیل نماد {symbol_id}: {e}", exc_info=True)
                    db_session.rollback()

            # 🔹 آزادسازی حافظه‌ی DataFrame بعد از هر بچ
            del df
            del historical_data
            cleanup_memory()

        logger.info(f"✅ تحلیل تکنیکال کامل شد. موفق: {success_count} | خطا: {error_count}")
        return success_count, f"تحلیل کامل شد. {success_count} موفق، {error_count} خطا"

    except Exception as e:
        error_msg = f"❌ خطا در اجرای تحلیل تکنیکال: {e}"
        logger.error(error_msg, exc_info=True)
        db_session.rollback()
        return 0, error_msg

# -----------------------------------------------------------
# تابع ذخیره‌سازی نتایج تحلیل تکنیکال
# -----------------------------------------------------------

def save_technical_indicators(db_session: Session, symbol_id: Union[int, str], df: pd.DataFrame):
    """
    ذخیره (درج یا به‌روزرسانی) نتایج تحلیل تکنیکال محاسبه شده در جدول TechnicalIndicatorData.
    🔧 اصلاح شده برای پشتیبانی از هر دو نوع داده symbol_id (int و string)
    """
    # 🔧 اصلاح: تضمین string بودن symbol_id بدون در نظر گرفتن نوع ورودی
    symbol_id_str = str(symbol_id)
    
    logger.debug(f"💾 ذخیره اندیکاتورها برای نماد: {symbol_id_str} (ورودی: {symbol_id}, نوع: {type(symbol_id)})")

    # 🔧 اصلاح: تضمین وجود ستون symbol_id به صورت string
    if 'symbol_id' not in df.columns:
        df['symbol_id'] = symbol_id_str
    else:
        # تبدیل تمام symbol_id ها به string برای یکپارچگی
        df['symbol_id'] = df['symbol_id'].astype(str)

    # حذف تکراری‌ها فقط داخل DataFrame
    df_unique = df.drop_duplicates(subset=['symbol_id', 'jdate'], keep='last').copy()
    
    # فیلتر کردن ردیف‌هایی که اندیکاتورهای اصلی (مانند RSI) برای آن‌ها محاسبه نشده (معمولاً روزهای اول)
    df_to_save = df_unique.dropna(subset=['RSI', 'MACD', 'jdate'])

    if df_to_save.empty:
        logger.debug(f"⚠️ پس از پاکسازی، هیچ سطر معتبری برای ذخیره اندیکاتورهای نماد {symbol_id_str} وجود نداشت.")
        return
    
    updates_count = 0
    inserts_count = 0
    
    try: 
        # تبدیل DataFrame به دیکشنری برای Upsert
        records_dict = df_to_save.to_dict('records')
        
        # 🔧 اصلاح: فیلتر کردن با symbol_id به صورت string
        existing_indicators_query = db_session.query(TechnicalIndicatorData).filter(
            TechnicalIndicatorData.symbol_id == symbol_id_str
        )
        
        # استفاده از jdate برای ایجاد یک map جهت دسترسی سریع
        existing_map = {
            indicator.jdate: indicator 
            for indicator in existing_indicators_query
        }

        for row in records_dict:
            jdate = row.get('jdate')
            if not jdate:
                continue

            # 🔧 اصلاح: تضمین string بودن symbol_id در هر رکورد
            row['symbol_id'] = str(row.get('symbol_id', symbol_id_str))

            existing = existing_map.get(jdate)

            # 2. منطق درج یا به‌روزرسانی (Upsert)
            if existing:
                # ✅ Update رکورد موجود
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
                existing.halftrend_signal = row.get('halftrend_signal') # در utils جدید halftrend_trend است
                existing.resistance_level_50d = row.get('resistance_level_50d')
                existing.resistance_broken = bool(row.get('resistance_broken'))
                
                updates_count += 1
            else:
                # ✅ Insert رکورد جدید
                indicator = TechnicalIndicatorData(
                    symbol_id=row['symbol_id'],  # 🔧 استفاده از symbol_id تضمین شده string
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
                    halftrend_signal=row.get('halftrend_signal'), # در utils جدید halftrend_trend است
                    resistance_level_50d=row.get('resistance_level_50d'),
                    resistance_broken=bool(row.get('resistance_broken'))
                )
                db_session.add(indicator)
                inserts_count += 1

        # Commit کردن تراکنش پس از اتمام حلقه
        db_session.commit()
        if inserts_count > 0 or updates_count > 0:
            logger.info(f"✅ اندیکاتورهای نماد {symbol_id_str} با موفقیت ذخیره/بروزرسانی شدند. (درج: {inserts_count}، بروزرسانی: {updates_count})")
        else:
            logger.debug(f"ℹ️ هیچ داده جدیدی برای نماد {symbol_id_str} یافت نشد.")
        
    except Exception as e:
        db_session.rollback()
        logger.error(f"❌ خطا در ذخیره اندیکاتورها برای نماد {symbol_id_str}: {e}", exc_info=True)


# -----------------------------------------------------------
# تابع اصلی اجرای تشخیص الگوهای شمعی
# -----------------------------------------------------------

def run_candlestick_detection(
    db_session: Session, 
    limit: int = None, 
    symbols_list: list = None
) -> int:
    """
    اجرای تشخیص الگوهای شمعی برای نمادها با استفاده از داده‌های تاریخی و ذخیره نتایج.
    از استراتژی حذف و درج (Delete & Insert) برای جلوگیری از تکرار استفاده می‌کند.
    🔧 اصلاح شده برای پشتیبانی از هر دو نوع داده symbol_id
    """
    try:
        logger.info("🕯️ شروع تشخیص الگوهای شمعی...")
        
        # 1. دریافت لیست symbol_id های فعال
        # 🔧 اصلاح: تضمین string بودن symbol_id ها
        base_query = db_session.query(HistoricalData.symbol_id).distinct()
        
        if symbols_list:
            # 🔧 تبدیل تمام symbol_id های ورودی به string برای تطابق
            symbols_list_str = [str(sym) for sym in symbols_list]
            base_query = base_query.filter(HistoricalData.symbol_id.in_(symbols_list_str))
            
        symbol_ids_raw = [s[0] for s in base_query.all()]
        symbol_ids_to_process = [str(symbol_id) for symbol_id in symbol_ids_raw]  # 🔧 تبدیل به string
        
        logger.info(f"🔍 نمونه symbol_id ها: {symbol_ids_to_process[:3]}")  # 🔍 دیباگ
        
        if not symbol_ids_to_process:
            logger.warning("⚠️ هیچ نمادی برای تشخیص الگوهای شمعی یافت نشد.")
            return 0
            
        if limit is not None:
            symbol_ids_to_process = symbol_ids_to_process[:limit]
            
        logger.info(f"🔍 یافت شد {len(symbol_ids_to_process)} نماد برای تشخیص الگوهای شمعی")

        success_count = 0
        records_to_insert = []
        
        # 2. حلقه زدن روی هر نماد و فچ و پردازش جداگانه
        processed_count = 0
        today_jdate_str = None

        for symbol_id in symbol_ids_to_process:
            
            try:
                # 💡 نقطه کلیدی: فچ داده‌های تاریخی فقط برای یک نماد
                # 🔧 اصلاح: فیلتر کردن با symbol_id به صورت string
                historical_data_query = db_session.query(HistoricalData).filter(
                    HistoricalData.symbol_id == symbol_id
                ).order_by(HistoricalData.date.desc()).limit(30)
                
                historical_data = historical_data_query.all() 
                
                if len(historical_data) < 5:
                    logger.debug(f"⚠️ داده کافی برای نماد {symbol_id} وجود ندارد ({len(historical_data)} رکورد)")
                    continue 

                # 💡 تبدیل به DataFrame
                df = pd.DataFrame([row.__dict__ for row in historical_data])
                if '_sa_instance_state' in df.columns:
                    df = df.drop(columns=['_sa_instance_state']) 
                
                # مرتب‌سازی برای اطمینان از اینکه iloc[-1] روز جدید است
                df.sort_values(by='date', inplace=True) 

                # استخراج داده‌های لازم:
                today_record_dict = df.iloc[-1].to_dict()
                yesterday_record_dict = df.iloc[-2].to_dict()
                
                # --- فراخوانی تابع تشخیص الگو از Utils ---
                patterns = check_candlestick_patterns(
                    today_record_dict, 
                    yesterday_record_dict, 
                    df
                )
                
                # ذخیره الگوهای یافت‌شده
                if patterns:
                    now = datetime.now()
                    current_jdate = today_record_dict['jdate']
                    if today_jdate_str is None:
                         today_jdate_str = current_jdate

                    for pattern in patterns:
                        records_to_insert.append({
                            'symbol_id': symbol_id,  # 🔧 استفاده از symbol_id که از قبل string است
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
                # ادامه می‌دهیم تا بقیه نمادها پردازش شوند
                
        logger.info(f"✅ تشخیص الگوهای شمعی برای {success_count} نماد (با {len(records_to_insert)} الگو) انجام شد.")
                
        # 3. ذخیره نتایج در دیتابیس (استراتژی Delete & Insert)
        if records_to_insert:
            # الف) استخراج تاریخ و لیست نمادهای پردازش شده
            if not today_jdate_str:
                 today_jdate_str = records_to_insert[0]['jdate'] 

            processed_symbol_ids_set = list({record['symbol_id'] for record in records_to_insert})
            
            # ب) حذف رکوردهای قدیمی
            try:
                logger.info(f"🗑️ در حال حذف الگوهای شمعی قبلی برای {len(processed_symbol_ids_set)} نماد در تاریخ {today_jdate_str}...")
                
                db_session.query(CandlestickPatternDetection).filter(
                    CandlestickPatternDetection.symbol_id.in_(processed_symbol_ids_set),
                    CandlestickPatternDetection.jdate == today_jdate_str
                ).delete(synchronize_session=False) 
                
                db_session.commit()
                
            except Exception as e:
                db_session.rollback()
                logger.error(f"❌ خطا در حذف رکوردهای قدیمی الگوهای شمعی: {e}", exc_info=True)
                return success_count
                
            # ج) درج رکوردهای جدید
            try:
                db_session.bulk_insert_mappings(CandlestickPatternDetection, records_to_insert)
                db_session.commit()
                logger.info(f"✅ {len(records_to_insert)} الگوی شمعی با موفقیت درج شد.")
            except Exception as e:
                 db_session.rollback()
                 logger.error(f"❌ خطا در درج رکوردهای جدید الگوهای شمعی: {e}", exc_info=True)
                 
        else:
            logger.info("ℹ️ هیچ الگوی شمعی جدیدی یافت نشد.")

        return success_count

    except Exception as e:
         logger.error(f"❌ خطای کلی در اجرای تشخیص الگوهای شمعی: {e}", exc_info=True)
         db_session.rollback()
         return 0

# -----------------------------------------------------------
# توابع Export
# -----------------------------------------------------------

__all__ = [
    # توابع اصلی اجرا کننده تحلیل
    'run_technical_analysis',
    'run_candlestick_detection',
    
    # توابع کمکی مورد نیاز
    'get_session_local',
    'cleanup_memory',
    'save_technical_indicators' # (اگرچه داخلی است، اما ممکن است مفید باشد)
]
