# -*- coding: utf-8 -*-
# services/data_analysis_service.py
# مسئولیت: اجرای تحلیل‌های تکنیکال، فاندامنتال و شمعی و ذخیره‌سازی نتایج در دیتابیس.

from sqlalchemy.orm import Session
from sqlalchemy import func, distinct
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple

# فرض می‌کنیم models.py و technical_analysis_utils.py در دسترس هستند
from models import HistoricalData, TechnicalIndicatorData, CandlestickPatternDetection, FundamentalData
from services.technical_analysis_utils import calculate_all_indicators, check_candlestick_patterns, calculate_fundamental_metrics
from datetime import datetime

# تنظیمات لاگینگ
logger = logging.getLogger(__name__)

# -----------------------------------------------------------
# توابع کمکی دیتابیس
# -----------------------------------------------------------

def save_technical_indicators(db_session: Session, symbol_id: str, df_indicators: pd.DataFrame):
    """
    ذخیره/به‌روزرسانی نتایج اندیکاتورهای تکنیکال در دیتابیس برای یک نماد.
    از استراتژی حذف و درج برای جلوگیری از تکرار استفاده می‌کند (Delete & Insert).
    """
    try:
        # فیلتر کردن رکوردهای نهایی که اندیکاتورهایشان NaN نیست
        df_to_save = df_indicators.dropna(subset=['RSI', 'MACD', 'SMA_20', 'Bollinger_Middle'])

        if df_to_save.empty:
            # logger.warning(f"⚠️ هیچ رکورد معتبری برای ذخیره اندیکاتورهای {symbol_id} وجود ندارد.")
            return 0
        
        # استخراج ستون‌های مورد نیاز برای ذخیره در TechnicalIndicatorData
        indicators_columns = [col for col in df_to_save.columns if col not in HistoricalData.__table__.columns]
        
        records_to_insert = []
        now = datetime.now()

        for index, row in df_to_save.iterrows():
            # ایجاد دیکشنری رکورد جدید
            record = {
                'symbol_id': symbol_id,
                'jdate': row['jdate'],
                'date': row['date'],
                'updated_at': now,
            }
            # اضافه کردن مقادیر اندیکاتورها
            for col in indicators_columns:
                # اطمینان از اینکه مقدار float است
                record[col.lower()] = float(row[col]) if pd.notna(row[col]) else None
            
            records_to_insert.append(record)
            
        if not records_to_insert:
            return 0

        # حذف رکوردهای قدیمی برای نماد جاری
        db_session.query(TechnicalIndicatorData).filter(
            TechnicalIndicatorData.symbol_id == symbol_id
        ).delete(synchronize_session=False)

        # درج رکوردهای جدید
        db_session.bulk_insert_mappings(TechnicalIndicatorData, records_to_insert)
        
        # db_session.commit() # ❌ COMMIT باید در تابع اصلی run_technical_analysis انجام شود.
        return len(records_to_insert)

    except SQLAlchemyError as e:
        logger.error(f"❌ خطای دیتابیس در ذخیره اندیکاتورهای {symbol_id}: {e}", exc_info=True)
        raise # اجازه می‌دهیم خطا به تابع اصلی برود تا rollback انجام شود


# -----------------------------------------------------------
# تابع اصلی اجرای تحلیل تکنیکال
# -----------------------------------------------------------

def run_technical_analysis(db_session: Session, symbols_list: Optional[List[str]] = None, batch_size: int = 200) -> Tuple[int, str]:
    """
    اجرای تحلیل تکنیکال در بچ‌های کوچک برای جلوگیری از مصرف زیاد حافظه.
    """
    try:
        logger.info("📈 شروع تحلیل تکنیکال...")

        # دریافت لیست یکتا از نمادها
        symbol_ids_query = db_session.query(HistoricalData.symbol_id).distinct()
        if symbols_list:
            symbol_ids_query = symbol_ids_query.filter(HistoricalData.symbol_id.in_(symbols_list))

        all_symbols = [str(row[0]) for row in symbol_ids_query.all()]
        total_symbols = len(all_symbols)
        logger.info(f"🔍 مجموع {total_symbols} نماد برای تحلیل تکنیکال یافت شد.")

        processed_count = 0
        success_count = 0
        error_count = 0
        
        # تعریف تمام ستون‌های مورد نیاز برای تحلیل
        columns_to_fetch = [
            HistoricalData.symbol_id, HistoricalData.symbol_name, HistoricalData.date, HistoricalData.jdate, 
            HistoricalData.open, HistoricalData.close, HistoricalData.high, HistoricalData.low, 
            HistoricalData.volume, HistoricalData.final, HistoricalData.yesterday_price, 
            HistoricalData.buy_count_i, HistoricalData.buy_count_n, HistoricalData.sell_count_i, 
            HistoricalData.sell_count_n, HistoricalData.buy_i_volume, HistoricalData.buy_n_volume, 
            HistoricalData.sell_i_volume, HistoricalData.sell_n_volume
        ]

        # اجرای تحلیل در بچ‌های 200تایی
        for i in range(0, total_symbols, batch_size):
            batch_symbols = all_symbols[i:i + batch_size]
            logger.info(f"📦 پردازش بچ {i // batch_size + 1}: نمادهای {i + 1} تا {min(i + batch_size, total_symbols)}")

            # کوئری برای فچ داده‌های بچ جاری
            query = db_session.query(*columns_to_fetch).filter(
                HistoricalData.symbol_id.in_(batch_symbols)
            ).order_by(HistoricalData.symbol_id, HistoricalData.date)
            
            # 💡 استفاده از Pandas برای خواندن کوئری از دیتابیس (بهتر برای دیتابیس‌های بزرگ)
            # اگرچه در اینجا به دلیل سادگی همان historical_data = query.all() را نگه می‌داریم.
            historical_data = query.all()

            if not historical_data:
                logger.warning(f"⚠️ هیچ داده‌ای برای این بچ یافت نشد.")
                continue

            # تبدیل نتایج کوئری به DataFrame
            df = pd.DataFrame(historical_data, columns=[col.key for col in columns_to_fetch])

            grouped = df.groupby('symbol_id')

            for symbol_id, group_df in grouped:
                try:
                    # 1. محاسبه تمام اندیکاتورها (فراخوانی از technical_analysis_utils)
                    df_indicators = calculate_all_indicators(group_df)
                    
                    # 2. ذخیره نتایج در دیتابیس
                    save_technical_indicators(db_session, str(symbol_id), df_indicators)
                    db_session.commit() # Commit برای هر نماد (یا هر بچ) امنیت بیشتری دارد
                    
                    success_count += 1
                
                except Exception as e:
                    error_count += 1
                    logger.error(f"❌ خطا در تحلیل/ذخیره نماد {symbol_id}: {e}", exc_info=True)
                    db_session.rollback() # Rollback در صورت بروز خطا برای نماد خاص

                processed_count += 1
                if processed_count % 50 == 0:
                    logger.info(f"📊 پیشرفت تحلیل: {processed_count}/{total_symbols} نماد")
            
            # 🔹 آزادسازی حافظه‌ی DataFrame بعد از هر بچ
            del df
            import gc
            gc.collect()

        logger.info(f"✅ تحلیل تکنیکال کامل شد. موفق: {success_count} | خطا: {error_count}")
        return success_count, f"تحلیل کامل شد. {success_count} موفق، {error_count} خطا"

    except Exception as e:
        error_msg = f"❌ خطای کلی در اجرای تحلیل تکنیکال: {e}"
        logger.error(error_msg, exc_info=True)
        db_session.rollback()
        return 0, error_msg

# -----------------------------------------------------------
# تابع اصلی اجرای تشخیص الگوهای شمعی
# -----------------------------------------------------------

def run_candlestick_detection(db_session: Session, symbols_list: Optional[List[str]] = None) -> Tuple[int, str]:
    """
    اجرای تشخیص الگوهای شمعی برای نمادها با فچ داده‌های محدود (مثلاً ۳۰ روز اخیر) برای هر نماد.
    """
    try:
        logger.info("🕯️ شروع تشخیص الگوهای شمعی...")
        
        # دریافت لیست symbol_id های فعال
        base_query = db_session.query(HistoricalData.symbol_id).distinct()
        if symbols_list:
            base_query = base_query.filter(HistoricalData.symbol_id.in_(symbols_list)) 
            
        symbol_ids_to_process = [str(s[0]) for s in base_query.all()]
        
        if not symbol_ids_to_process:
            logger.warning("⚠️ هیچ نمادی برای تشخیص الگوهای شمعی یافت نشد.")
            return 0, "هیچ نمادی برای تشخیص الگوهای شمعی یافت نشد."
            
        logger.info(f"🔍 یافت شد {len(symbol_ids_to_process)} نماد برای تشخیص الگوهای شمعی.")

        success_count = 0
        records_to_insert = []
        
        processed_count = 0
        for symbol_id in symbol_ids_to_process:
            try:
                # 💡 فچ داده‌های تاریخی فقط برای ۳۰ روز اخیر نماد جاری
                historical_data_query = db_session.query(HistoricalData).filter(
                    HistoricalData.symbol_id == symbol_id
                ).order_by(HistoricalData.date.desc()).limit(30) 
                
                historical_data = historical_data_query.all() 
                
                if len(historical_data) < 5: 
                    continue 

                # تبدیل به DataFrame و مرتب‌سازی بر اساس تاریخ صعودی
                df = pd.DataFrame([row.__dict__ for row in historical_data])
                df.sort_values(by='date', inplace=True) 
                
                # استخراج رکورد امروز و دیروز
                today_record_dict = df.iloc[-1].to_dict()
                yesterday_record_dict = df.iloc[-2].to_dict()
                
                # فراخوانی تابع تشخیص الگو (فراخوانی از technical_analysis_utils)
                patterns = check_candlestick_patterns(
                    today_record_dict, 
                    yesterday_record_dict, 
                    df # کل DataFrame محدود شده (مثلاً ۳۰ روزه)
                )
                
                # ذخیره الگوهای یافت‌شده
                if patterns:
                    now = datetime.now()
                    current_jdate = today_record_dict['jdate']
                    for pattern in patterns:
                        records_to_insert.append({
                            'symbol_id': str(symbol_id),
                            'jdate': current_jdate,
                            'pattern_name': pattern,
                            'created_at': now, 
                            'updated_at': now
                        })
                    success_count += 1
                
                processed_count += 1
                if processed_count % 50 == 0:
                    logger.info(f"🕯️ پیشرفت تشخیص الگوهای شمعی: {processed_count}/{len(symbol_ids_to_process)} نماد")

            except Exception as e:
                logger.error(f"❌ خطا در تشخیص الگوهای شمعی برای نماد {symbol_id}: {e}", exc_info=True)
                # Rollback نمی‌کنیم چون bulk insert در پایان انجام می‌شود
                
        logger.info(f"✅ تشخیص الگوهای شمعی برای {success_count} نماد با الگو انجام شد.")
                
        # ------------------
        # 3. ذخیره نتایج در دیتابیس (Delete & Insert)
        # ------------------
        if records_to_insert:
            # الف) استخراج تاریخ و لیست نمادهای پردازش شده
            # از آنجایی که این تابع روزانه اجرا می‌شود، فرض می‌کنیم jdate همه رکوردها یکسان است.
            last_jdate = records_to_insert[0]['jdate'] 
            processed_symbol_ids = list({record['symbol_id'] for record in records_to_insert})
            
            # ب) حذف رکوردهای قدیمی
            try:
                db_session.query(CandlestickPatternDetection).filter(
                    CandlestickPatternDetection.symbol_id.in_(processed_symbol_ids),
                    CandlestickPatternDetection.jdate == last_jdate
                ).delete(synchronize_session=False) 
                
                db_session.commit()
                logger.info(f"🗑️ الگوهای شمعی قبلی ({len(processed_symbol_ids)} نماد) برای {last_jdate} حذف شدند.")
                
            except Exception as e:
                db_session.rollback()
                logger.error(f"❌ خطا در حذف رکوردهای قدیمی الگوهای شمعی: {e}", exc_info=True)
                return success_count, "خطا در حذف رکوردهای قدیمی"
                
            # ج) درج رکوردهای جدید
            db_session.bulk_insert_mappings(CandlestickPatternDetection, records_to_insert)
            db_session.commit()
            logger.info(f"✅ {len(records_to_insert)} الگوی شمعی با موفقیت درج شد.")
        else:
            logger.info("ℹ️ هیچ الگوی شمعی جدیدی یافت نشد.")

        return success_count, f"تشخیص الگوهای شمعی با موفقیت انجام شد. {success_count} نماد دارای الگو"

    except Exception as e:
        error_msg = f"❌ خطای کلی در اجرای تشخیص الگوهای شمعی: {e}"
        logger.error(error_msg, exc_info=True)
        db_session.rollback()
        return 0, error_msg

# -----------------------------------------------------------
# تابع اصلی اجرای تحلیل فاندامنتال (بر اساس ستون‌های موجود)
# -----------------------------------------------------------

def run_fundamental_analysis(db_session: Session, symbols_list: Optional[List[str]] = None, batch_size: int = 200) -> Tuple[int, str]:
    """
    اجرای تحلیل فاندامنتال (بر اساس داده‌های موجود در HistoricalData) و ذخیره‌سازی نتایج.
    """
    try:
        logger.info("💰 شروع تحلیل فاندامنتال (قدرت خریدار/حجم)...")

        # این تابع از همان منطق بچ run_technical_analysis استفاده می‌کند
        symbol_ids_query = db_session.query(HistoricalData.symbol_id).distinct()
        if symbols_list:
            symbol_ids_query = symbol_ids_query.filter(HistoricalData.symbol_id.in_(symbols_list))

        all_symbols = [str(row[0]) for row in symbol_ids_query.all()]
        total_symbols = len(all_symbols)
        logger.info(f"🔍 مجموع {total_symbols} نماد برای تحلیل فاندامنتال یافت شد.")

        processed_count = 0
        success_count = 0
        error_count = 0
        
        # ستون‌های مورد نیاز برای فاندامنتال (قدرت خریدار/حجم)
        columns_to_fetch = [
            HistoricalData.symbol_id, HistoricalData.date, HistoricalData.jdate, 
            HistoricalData.volume, HistoricalData.final, 
            HistoricalData.buy_count_i, HistoricalData.sell_count_i, 
            HistoricalData.buy_i_volume, HistoricalData.sell_i_volume
        ]

        for i in range(0, total_symbols, batch_size):
            batch_symbols = all_symbols[i:i + batch_size]
            
            query = db_session.query(*columns_to_fetch).filter(
                HistoricalData.symbol_id.in_(batch_symbols)
            ).order_by(HistoricalData.symbol_id, HistoricalData.date)
            
            historical_data = query.all()

            if not historical_data:
                continue

            df = pd.DataFrame(historical_data, columns=[col.key for col in columns_to_fetch])
            
            # برای محاسبه Volume_MA_20 نیاز به Volume_MA_20 داریم که در calculate_all_indicators محاسبه می‌شود.
            # برای ساده‌سازی، اینجا Volume_MA_20 را به صورت جداگانه حساب می‌کنیم.
            
            df['Volume_MA_20'] = df.groupby('symbol_id')['volume'].transform(lambda x: calculate_volume_ma(x, 20))


            grouped = df.groupby('symbol_id')

            for symbol_id, group_df in grouped:
                try:
                    # 1. محاسبه معیارهای فاندامنتال/جریان سرمایه (فراخوانی از technical_analysis_utils)
                    df_metrics = calculate_fundamental_metrics(group_df)
                    
                    # 2. ذخیره نتایج
                    records_to_insert = []
                    now = datetime.now()

                    # فقط آخرین رکورد (امروز) را ذخیره می‌کنیم
                    last_row = df_metrics.iloc[-1]
                    
                    record = {
                        'symbol_id': str(symbol_id),
                        'jdate': last_row['jdate'],
                        'date': last_row['date'],
                        'real_power_ratio': float(last_row['Real_Power_Ratio']) if pd.notna(last_row['Real_Power_Ratio']) else None,
                        'volume_ratio_20d': float(last_row['Volume_Ratio_20d']) if pd.notna(last_row['Volume_Ratio_20d']) else None,
                        'daily_liquidity': float(last_row['Daily_Liquidity']) if pd.notna(last_row['Daily_Liquidity']) else None,
                        'updated_at': now,
                    }
                    records_to_insert.append(record)

                    # حذف رکوردهای قبلی امروز
                    db_session.query(FundamentalData).filter(
                        FundamentalData.symbol_id == str(symbol_id),
                        FundamentalData.jdate == last_row['jdate']
                    ).delete(synchronize_session=False)

                    # درج رکورد جدید
                    db_session.bulk_insert_mappings(FundamentalData, records_to_insert)
                    db_session.commit()
                    
                    success_count += 1
                
                except Exception as e:
                    error_count += 1
                    logger.error(f"❌ خطا در تحلیل فاندامنتال/ذخیره نماد {symbol_id}: {e}", exc_info=True)
                    db_session.rollback()

                processed_count += 1
            
            del df
            import gc
            gc.collect()

        logger.info(f"✅ تحلیل فاندامنتال کامل شد. موفق: {success_count} | خطا: {error_count}")
        return success_count, f"تحلیل فاندامنتال کامل شد. {success_count} موفق، {error_count} خطا"

    except Exception as e:
        error_msg = f"❌ خطای کلی در اجرای تحلیل فاندامنتال: {e}"
        logger.error(error_msg, exc_info=True)
        db_session.rollback()
        return 0, error_msg

# -----------------------------------------------------------
# Export functions for services
# -----------------------------------------------------------

__all__ = [
    'run_technical_analysis',
    'run_candlestick_detection',
    'run_fundamental_analysis'
]
