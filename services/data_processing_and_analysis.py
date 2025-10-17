# -*- coding: utf-8 -*-
# services/data_processing_and_analysis.py
# مسئول اجرای تحلیل‌های تکنیکال و الگوی کندل و ذخیره نتایج در DB
from typing import List, Optional, Tuple, Any
from datetime import datetime
import jdatetime
import pandas as pd
import numpy as np
import logging
import traceback
import ta # کتابخانه استاندارد برای محاسبات اندیکاتورهای تکنیکال
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

# فرض بر این است که این مدل‌ها در فایل models.py و توابع کمکی در utils.py موجود هستند.
from extensions import db
from models import HistoricalData, ComprehensiveSymbolData, TechnicalIndicatorData, CandlestickPatternDetection
from services.technical_analysis_utils import (
    calculate_stochastic, 
    calculate_squeeze_momentum, 
    calculate_halftrend, 
    calculate_support_resistance_break, 
    check_candlestick_patterns
)

logger = logging.getLogger(__name__)

# ---------------------------
# Helpers (توابع کمکی)
# ---------------------------
def get_session_local():
    """ایجاد session محلی برای دسترسی به دیتابیس در خارج از کانتکست Flask."""
    try:
        from flask import current_app
        with current_app.app_context():
            from sqlalchemy.orm import sessionmaker
            return sessionmaker(bind=db.engine)()
    except RuntimeError:
        from sqlalchemy.orm import sessionmaker
        return sessionmaker(bind=db.get_engine())()

def to_jdate(dt: datetime) -> str:
    """تبدیل شیء datetime به تاریخ جلالی (YYYY-MM-DD)."""
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    return jdatetime.date.fromgregorian(date=dt.date()).strftime("%Y-%m-%d")

# ----------------------------------------------------
# Core processing: Technical & Pattern Analysis (منطق اصلی تحلیل)
# ----------------------------------------------------

def run_technical_analysis(
    db_session: Session,
    limit: Optional[int] = None,
    specific_symbols_list: Optional[List[str]] = None,
    days_limit: Optional[int] = None
) -> Tuple[int, str]:
    """
    اجرای محاسبات اندیکاتورهای تکنیکال و تشخیص الگوهای کندل بر روی داده‌های تاریخی.
    نتایج در TechnicalIndicatorData و CandlestickPatternDetection ذخیره می‌شوند.
    """
    processed_symbols = 0
    indicator_count = 0
    pattern_count = 0
    
    # 1. تعیین نمادها برای پردازش (فقط نمادهایی که داده تاریخی دارند)
    try:
        query = db_session.query(ComprehensiveSymbolData).filter(
            ComprehensiveSymbolData.last_historical_update_date != None
        )
        
        if specific_symbols_list:
            conds = []
            for idf in specific_symbols_list:
                if str(idf).isdigit():
                    conds.append(ComprehensiveSymbolData.tse_index == str(idf))
                else:
                    conds.append(ComprehensiveSymbolData.symbol_name == idf)
            query = query.filter(or_(*conds))
            
        if limit:
            query = query.limit(limit)
            
        symbols = query.all()
        if not symbols:
            return 0, "No symbols with historical data found for analysis."

        logger.info(f"Starting technical analysis for {len(symbols)} symbols.")

    except Exception as e:
        logger.error(f"Error fetching symbols for analysis: {e}")
        db_session.rollback()
        return 0, str(e)


    # 2. حلقه‌ی اصلی پردازش و تحلیل
    for sym in symbols:
        try:
            # الف) واکشی داده‌های تاریخی مورد نیاز (OHLCV)
            historical_data = db_session.query(HistoricalData).filter(
                HistoricalData.symbol_id == sym.symbol_id
            ).order_by(HistoricalData.date.asc()).all()

            if not historical_data:
                logger.info(f"Skipping {sym.symbol_name}: No historical data.")
                continue

            # تبدیل به DataFrame
            df_data = [{
                'date': h.date,
                'jdate': h.jdate,
                'open_price': h.open,
                'high_price': h.high,
                'low_price': h.low,
                'close_price': h.close,
                'volume': h.volume
            } for h in historical_data]

            df = pd.DataFrame(df_data)
            
            for col in ['open_price', 'high_price', 'low_price', 'close_price', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
            if days_limit and days_limit > 0:
                 df = df.iloc[-days_limit:]
            
            # --- ب) محاسبه اندیکاتورهای تکنیکال استاندارد (با ta) ---
            
            df['RSI'] = ta.momentum.RSIIndicator(df['close_price'], window=14).rsi()
            
            macd_indicator = ta.trend.MACD(df['close_price'])
            df['MACD'] = macd_indicator.macd()
            df['MACD_Signal'] = macd_indicator.macd_signal()
            df['MACD_Hist'] = macd_indicator.macd_diff()
            
            df['SMA_20'] = ta.trend.sma_indicator(df['close_price'], window=20)
            df['SMA_50'] = ta.trend.sma_indicator(df['close_price'], window=50)
            
            bb_indicator = ta.volatility.BollingerBands(df['close_price'], window=20, window_dev=2)
            df['Bollinger_High'] = bb_indicator.bollinger_hband()
            df['Bollinger_Low'] = bb_indicator.bollinger_lband()
            df['Bollinger_MA'] = bb_indicator.bollinger_mavg()
            
            df['Volume_MA_20'] = ta.trend.sma_indicator(df['volume'], window=20)
            df['ATR'] = ta.volatility.AverageTrueRange(df['high_price'], df['low_price'], df['close_price'], window=14).average_true_range()


            # --- ج) محاسبه اندیکاتورهای پیشرفته (با Utils) ---
            
            df['Stochastic_K'], df['Stochastic_D'] = calculate_stochastic(df['high_price'], df['low_price'], df['close_price'])
            
            # squeeze_on
            df['squeeze_on'], _ = calculate_squeeze_momentum(df) 
            
            # halftrend_signal
            _, trend_series = calculate_halftrend(df) 
            df['halftrend_signal'] = trend_series.fillna(0).astype(int)

            
            # Support/Resistance Break
            df['resistance_level_50d'], df['resistance_broken'] = calculate_support_resistance_break(df, window=50)

            
            # --- د) تشخیص الگوهای کندل ---
            patterns_detected = check_candlestick_patterns(df.copy()) # خروجی: لیست از (jdate, pattern_name)
            
            
            # --- ه) آماده‌سازی و ذخیره‌سازی Technical Indicator (با Merge برای Upsert) ---
            
            df = df.replace([np.inf, -np.inf], None)
            
            for _, row in df.iterrows():
                indicator_data = TechnicalIndicatorData(
                    symbol_id=sym.symbol_id,
                    jdate=row['jdate'],
                    close_price=row['close_price'],
                    RSI=row.get('RSI'),
                    MACD=row.get('MACD'),
                    MACD_Signal=row.get('MACD_Signal'),
                    MACD_Hist=row.get('MACD_Hist'),
                    SMA_20=row.get('SMA_20'),
                    SMA_50=row.get('SMA_50'),
                    Bollinger_High=row.get('Bollinger_High'),
                    Bollinger_Low=row.get('Bollinger_Low'),
                    Bollinger_MA=row.get('Bollinger_MA'),
                    Volume_MA_20=row.get('Volume_MA_20'),
                    ATR=row.get('ATR'),
                    Stochastic_K=row.get('Stochastic_K'),
                    Stochastic_D=row.get('Stochastic_D'),
                    squeeze_on=bool(row.get('squeeze_on')),
                    halftrend_signal=row.get('halftrend_signal'),
                    resistance_level_50d=row.get('resistance_level_50d'),
                    resistance_broken=bool(row.get('resistance_broken'))
                )
                db_session.merge(indicator_data) 
                indicator_count += 1 

            # --- و) ذخیره Candlestick Patterns (با Merge برای Upsert) ---
            
            for jdate, pattern_name in patterns_detected:
                pattern_obj = CandlestickPatternDetection(
                    symbol_id=sym.symbol_id,
                    jdate=jdate,
                    pattern_name=pattern_name
                )
                db_session.merge(pattern_obj)
                pattern_count += 1

            
            db_session.commit()
            processed_symbols += 1
            logger.info(f"Successfully analyzed {sym.symbol_name}. Indicators: {len(df)}, Patterns: {len(patterns_detected)}")

        except Exception as e:
            logger.error(f"Critical error during technical analysis for {sym.symbol_name}: {e}", exc_info=True)
            db_session.rollback()
            continue

    # 3. جمع‌بندی نهایی
    msg = f"Analysis completed for {processed_symbols} symbols. Total Indicator Records: {indicator_count}. Total Pattern Records: {pattern_count}."
    logger.info(msg)
    return processed_symbols, msg


# ---------------------------
# Exports (فقط توابع تحلیلی و کمکی عمومی)
# ---------------------------
__all__ = [
    "run_technical_analysis",
    "get_session_local"
]