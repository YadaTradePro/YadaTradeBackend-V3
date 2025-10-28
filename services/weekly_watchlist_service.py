# -*- coding: utf-8 -*-
# services/weekly_watchlist_service.py

# --- 💡 G-CleanUp: تمام ایمپورت‌ها به سطح بالا منتقل شدند ---
from extensions import db
from models import (
    HistoricalData, ComprehensiveSymbolData, TechnicalIndicatorData, 
    WeeklyWatchlistResult, SignalsPerformance, AggregatedPerformance, 
    GoldenKeyResult, MLPrediction, DailyIndexData, DailySectorPerformance
)
from flask import current_app
import pandas as pd
from datetime import datetime, timedelta, date
import jdatetime
import uuid
from sqlalchemy import func, text
import logging
import json
import numpy as np
from types import SimpleNamespace
from typing import List, Dict, Tuple, Optional
import time
from sqlalchemy import desc

# 💡 G-Performance: ایمپورت برای پردازش موازی
from joblib import Parallel, delayed

# 💡 G-Sentiment: ایمپورت برای تحلیل سنتیمنت
from services.index_data_fetcher import get_market_indices

# 💡 G-CleanUp: 
ایمپورت ابزارهای لازم
from services.technical_analysis_utils import (
    get_today_jdate_str, normalize_value, calculate_rsi, 
    calculate_macd, calculate_sma, calculate_bollinger_bands, 
    calculate_volume_ma, calculate_atr, calculate_smart_money_flow, 
    convert_gregorian_to_jalali, calculate_z_score
)

# تنظیمات لاگینگ برای این ماژول
logger = logging.getLogger(__name__)

# IMPROVEMENT: Lookback period and minimum history days 
# adjusted for better indicator quality
TECHNICAL_DATA_LOOKBACK_DAYS = 120
MIN_REQUIRED_HISTORY_DAYS = 50

## 💡 G-All: بازبینی کامل دیکشنری فیلترها
# REVISED: New filter weights dictionary with descriptions for clarity
FILTER_WEIGHTS = {
    # --- High-Impact Leading & Breakout Signals ---
    "Power_Thrust_Signal": {
        "weight": 5,
     "description": "سیگنال قدرت: ترکیبی از حجم معاملات انفجاری، ورود پول هوشمند سنگین در یک روز مثبت (نادیده گرفتن صف خرید/فروش)."
},
    "RSI_Positive_Divergence": {
        "weight": 5,
        "description": "واگرایی مثبت در RSI، یک سیگنال پیشرو قوی برای احتمال بازگشت روند نزولی به صعودی."
},
    "Resistance_Broken": {
        "weight": 5,
        "description": "شکست یک سطح مقاومت کلیدی، نشانه‌ای از قدرت خریداران و پتانسیل شروع یک حرکت صعودی جدید."
},
    "Static_Resistance_Broken": {
        "weight": 5,
        "description": "شکست یک مقاومت مهم استاتیک (کلاسیک)، سیگنالی بسیار معتبر برای ادامه رشد."
},
    "Squeeze_Momentum_Fired_Long": {
        "weight": 4,
        "description": "اندیکاتور Squeeze Momentum سیگنال خرید (خروج از فشردگی) صادر کرده است که نشان‌دهنده احتمال یک حرکت انفجاری است."
},
    "Stochastic_Bullish_Cross_Oversold": {
        "weight": 4,
        "description": "تقاطع صعودی در اندیکاتور استوکاستیک در ناحیه اشباع فروش، یک سیگنال خرید کلاسیک و معتبر."
},
    "Consolidation_Breakout_Candidate": {
        "weight": 3,
        "description": "سهم در فاز تراکم و نوسان کم قرار دارد و آماده یک حرکت قوی (شکست) است."
},
    "Near_Static_Support": {
        "weight": 3,
        "description": "قیمت در نزدیکی یک سطح حمایتی استاتیک معتبر قرار دارد که ریسک به ریوارد مناسبی برای ورود فراهم می‌کند."
},
    "Bollinger_Lower_Band_Touch": {
        "weight": 1,
        "description": "قیمت به باند پایین بولینگر برخورد کرده است که می‌تواند نشانه بازگشت کوتاه‌مدت قیمت باشد."
},

    # --- Trend Confirmation & Strength Signals ---
    "IsInLeadingSector": {
        "weight": 4,
        "description": "سهم متعلق به یکی از صنایع پیشرو و مورد توجه بازار است که شانس موفقیت را افزایش می‌دهد."
},
    "Strong_Uptrend_Confirmed": {
        "weight": 3,
        "description": "تایید روند صعودی قوی: SMA_20 > SMA_50 و قیمت بالاتر از SMA_20 است."
},
    "Buy_The_Dip_SMA50": {
        "weight": 3,
        "description": "پاداش پولبک: قیمت در یک روند صعودی، به SMA_50 پولبک زده (در محدوده 0% تا 3% بالای آن)."
},
    "Positive_Real_Money_Flow_Trend_10D": {
        "weight": 3,
        "description": "برآیند ورود پول هوشمند (حقیقی) در ۱۰ روز گذشته مثبت بوده است."
},
    "Heavy_Individual_Buy_Pressure": {
        "weight": 3,
        "description": "سرانه خرید حقیقی‌ها در روز آخر به طور قابل توجهی بالاتر از سرانه فروش بوده است (ورود پول سنگین)."
},
    "MACD_Bullish_Cross_Confirmed": {
        "weight": 2,
        "description": "خط MACD خط سیگنال خود را به سمت بالا قطع کرده که تاییدی بر شروع روند صعودی است."
},
    "HalfTrend_Buy_Signal": {
        "weight": 2,
        "description": "اندیکاتور HalfTrend سیگنال خرید صادر کرده است."
},
    "High_Volume_On_Up_Day": {
        "weight": 2,
        "description": "حجم معاملات در یک روز مثبت به طور معناداری افزایش یافته که نشان از حمایت قوی از رشد قیمت دارد (نادیده گرفتن صف)."
},
    "Volume_MA_Is_Rising": { # 💡 G-Volume: بهبود یافته
        "weight": 2,
        "description": "روند میانگین حجم: شیب (Slope) میانگین حجم معاملات (20 روزه) صعودی است."
},

    # --- 💡 G-Fundamental: بازگرداندن فیلتر ساده فاندامنتال ---
    "Reasonable_PE": {
        "weight": 1, 
        "description": "نسبت P/E سهم کمتر از 15 است و سهم حباب قیمتی ندارد."
},
    
    # --- ML Prediction Filter ---
    "ML_Predicts_Uptrend": {
        "weight": 2,
        "description": "مدل یادگیری ماشین، احتمال بالایی برای یک روند صعودی پیش‌بینی کرده است."
},

    # --- Penalties & Negative Scores (Crucial for avoiding peaks) ---
    "Price_Below_SMA200": { # 💡 G-Trend: فیلتر جدید روند بلندمدت
        "weight": -5,
        "description": "جریمه روند بلندمدت: قیمت پایین‌تر از میانگین متحرک ۲۰۰ روزه است."
},
    "Strong_Downtrend_Confirmed": {
        "weight": -4,
        "description": "جریمه روند نزولی: SMA_20 پایین‌تر از SMA_50 قرار دارد."
},
    "MACD_Negative_Divergence": {
        "weight": -4,
        "description": "جریمه واگرایی منفی: قیمت سقف جدید زده اما MACD سقف پایین‌تری ثبت کرده (نشانه ضعف شدید روند)."
},
    "RSI_Is_Overbought": {
        "weight": -4,
        "description": "جریمه اشباع خرید: RSI در ناحیه اشباع خرید قرار دارد که ریسک اصلاح قیمت را افزایش می‌دهد."
},
    "Price_Too_Stretched_From_SMA50": {
        "weight": -3,
        "description": "جریمه فاصله زیاد: قیمت فاصله زیادی از میانگین متحرک ۵۰ روزه گرفته که احتمال بازگشت به میانگین را بالا می‌برد."
},
    "Negative_Real_Money_Flow_Trend_10D": {
        "weight": -2,
        "description": "جریمه خروج پول: برآیند ورود پول هوشمند در ۱۰ روز گذشته منفی بوده است (خروج پول)."
},
    "Signal_Against_Market_Trend": {
        "weight": -2,
        "description": "جریمه خلاف جهت بازار: سیگنال صعودی (مثل MACD Cross) در یک بازار خرسی صادر شده است."
}
}


# کمک‌کننده: بازگرداندن یک سری close قابل‌اعتماد از historical DF
def _get_close_series_from_hist_df(hist_df):
    """
    Accepts a historical dataframe and returns a numeric pandas Series of close prices.
    Tries common column names: 'close_price', 'close', 'final'
    """
    if hist_df is None or hist_df.empty:
        return pd.Series(dtype=float)

    for col in ['close_price', 'close', 'final']:
        if col in hist_df.columns:
            ser = pd.to_numeric(hist_df[col], errors='coerce').dropna()
        if not ser.empty:
                return ser
    return pd.Series(dtype=float)



# --- NEW: Market Sentiment Analysis Function ---
def _get_market_sentiment() -> str:
    """
    Determines short-term market sentiment based on the average daily change of major indices over the last 3 trading days.
    Uses data from DailyIndexData table.
    Returns: 'Bullish', 'Neutral', or 'Bearish'.
    """
    try:
    # Get last 3 distinct jdates
        last_jdates_query = db.session.query(DailyIndexData.jdate).distinct().order_by(DailyIndexData.jdate.desc()).limit(3).all()
        if not last_jdates_query:
            logger.warning("No index data available, defaulting to Neutral")
            return "Neutral"

        last_jdates = [row[0] for row in last_jdates_query]

        # 💡 G-Sentiment: افزودن 'TEDPIX' به لیست بررسی
        index_types_to_check = ['Total_Index', 'Equal_Weighted_Index', 'TEDPIX']
      index_data = db.session.query(DailyIndexData).filter(
            DailyIndexData.jdate.in_(last_jdates),
            DailyIndexData.index_type.in_(index_types_to_check)
        ).all()

        if len(index_data) < 2:  # At least one per index
            logger.warning(f"Insufficient index data for types {index_types_to_check}, defaulting to Neutral")
            return "Neutral"

        # 
Group by index_type
        # 💡 G-Sentiment: استفاده از 'Total_Index' یا 'TEDPIX' به عنوان شاخص کل
        total_changes = [d.percent_change for d in index_data if d.index_type == 'Total_Index' or d.index_type == 'TEDPIX']
        equal_changes = [d.percent_change for d in index_data if d.index_type == 'Equal_Weighted_Index']

        if len(total_changes) < 1 or len(equal_changes) < 1:
            logger.warning("Missing data for one index type (Total or Equal), defaulting to Neutral")
           return "Neutral"

        avg_total = np.mean(total_changes)
        avg_equal = np.mean(equal_changes)

        logger.info(f"Market Sentiment Averages over last {len(last_jdates)} days: Total {avg_total:.2f}%, Equal {avg_equal:.2f}%")

        if avg_total > 0.3 and avg_equal > 0.3:
            logger.info(f"Market Sentiment: Bullish (Total: {avg_total:.2f}%, Equal: {avg_equal:.2f}%)")
            return "Bullish"
    elif avg_total < -0.3 and avg_equal < -0.3:
            logger.info(f"Market Sentiment: Bearish (Total: {avg_total:.2f}%, Equal: {avg_equal:.2f}%)")
            # 💡 FIX: Indent Error (خط ۱۶۸)
            return "Bearish"
        else:
            logger.info(f"Market Sentiment: Neutral (Total: {avg_total:.2f}%, Equal: {avg_equal:.2f}%)")
            return "Neutral"
   except Exception as e:
        logger.error(f"Error fetching market sentiment from DB: {e}, defaulting to Neutral")
        return "Neutral"


# --- REVISED: Filter Functions ---
def _check_market_condition_filters(hist_df, tech_df):
    """
    Checks for individual stock conditions like overbought state 
or consolidation.
"""
    satisfied_filters, reason_parts = [], {"market_condition": []}
    if not is_data_sufficient(tech_df, 1) or not is_data_sufficient(hist_df, MIN_REQUIRED_HISTORY_DAYS):
        return satisfied_filters, reason_parts

    last_tech = tech_df.iloc[-1]
    close_ser = _get_close_series_from_hist_df(hist_df)
    if close_ser.empty:
        return satisfied_filters, reason_parts
    last_close = close_ser.iloc[-1]
    last_hist = hist_df.iloc[-1]

    # --- Check 1: RSI Overbought Condition (Penalize only if weak) ---
    if hasattr(last_tech, 'RSI') and last_tech.RSI is not None and last_tech.RSI > 70:
       is_negative_divergence = (
            hasattr(last_tech, 'RSI_Divergence') and last_tech.RSI_Divergence == "Negative"
        )
        historical_volume_series = hist_df.tail(10)['volume']
        average_volume = historical_volume_series.mean() if not historical_volume_series.empty else 0
        is_high_volume = last_hist['volume'] > average_volume * 1.5 if average_volume > 0 else False

        if is_negative_divergence or not is_high_volume:
           satisfied_filters.append("RSI_Is_Overbought")
            reason_parts["market_condition"].append(
                f"RSI ({last_tech.RSI:.2f}) overbought with weakness."
            )
        else:
            reason_parts["market_condition"].append(
                f"RSI ({last_tech.RSI:.2f}) overbought but supported by strong volume (no penalty)."
    )

    # --- Check 2 & 3: Price Stretch (Penalize) OR Buy The Dip (Reward) ---
    sma50 = _get_attr_safe(last_tech, 'SMA_50')
    sma20 = _get_attr_safe(last_tech, 'SMA_20')
    
    if sma50 is not None and sma50 > 0 and sma20 is not None:
        stretch_percent = ((last_close - sma50) / sma50) * 100
        is_uptrend = sma20 > sma50

        # 💡 G-2: جریمه فاصله زیاد (شرط 
        if stretch_percent > 20:
            satisfied_filters.append("Price_Too_Stretched_From_SMA50")
            reason_parts["market_condition"].append(
                f"Price is overextended ({stretch_percent:.1f}%) from SMA50."
            )
        
        
        # 💡 G-2: پاداش پولبک (شرط جدید)
      elif is_uptrend and 0 <= stretch_percent <= 3:
            satisfied_filters.append("Buy_The_Dip_SMA50")
            reason_parts["market_condition"].append(
                f"Pullback signal: Price is near SMA50 ({stretch_percent:.1f}%) in an uptrend."
            )

    # --- Check 4: Consolidation Pattern (Reward) ---
    if hasattr(last_tech, 'ATR'):
        atr_series = pd.to_numeric(tech_df['ATR'].dropna())

        if len(atr_series) > 30:
            recent_atr_avg = atr_series.tail(10).mean()
            historical_atr_avg = atr_series.tail(30).mean()
            if recent_atr_avg < (historical_atr_avg * 0.7):
                satisfied_filters.append("Consolidation_Breakout_Candidate")
                reason_parts["market_condition"].append(
                "Stock is in a low-volatility consolidation phase."
                )

    return satisfied_filters, reason_parts

# تغییر در is_data_sufficient: انعطاف‌پذیرتر و مقاوم‌تر
def is_data_sufficient(data_df, min_len):
    """
    Checks if the provided DataFrame is not empty and has at least min_len records.
"""
    if data_df is None or data_df.empty:
        return False
    return len(data_df) >= min_len

def convert_jalali_to_gregorian_timestamp(jdate_str):
    """
    Converts a Jalali date string (YYYY-MM-DD) to a pandas Timestamp (Gregorian).
"""
    if pd.notna(jdate_str) and isinstance(jdate_str, str):
        try:
            jy, jm, jd = map(int, jdate_str.split('-'))
            gregorian_date = jdatetime.date(jy, jm, jd).togregorian()
            return pd.Timestamp(gregorian_date)
        except ValueError:
            return pd.NaT
    return pd.NaT

# Helper function to safely get a value
def _get_attr_safe(rec, attr, default=None):
   val = getattr(rec, attr, default)
    if isinstance(val, (pd.Series, pd.DataFrame)):
        # 💡 FIX: (Bug) باید آخرین مقدار (iloc[-1]) برگردانده شود، نه اولین (iloc[0])
        return val.iloc[-1] if not val.empty else default
    return val

# --- REFACTORED: Technical filters are broken down into smaller functions ---

# 💡 G-Fix: تابع کمکی جدید برای محاسبه صحیح واگرایی
def _find_divergence(price_series: pd.Series, indicator_series: pd.Series, lookback: int, check_type: str) -> bool:
    """
    تابع کمکی برای یافتن واگرایی ساده (جایگزین منطق اشتباه قبلی)
    check_type: 'positive_rsi' (RD+) یا 'negative' (RD-)
    """
    if len(price_series) < lookback or len(indicator_series) < lookback:
        return False
        
    price_series = pd.to_numeric(price_series.tail(lookback), errors='coerce').dropna()
    indicator_series = pd.to_numeric(indicator_series.tail(lookback), errors='coerce').dropna()
    
    # اطمینان از هم‌راستایی ایندکس‌ها پس از حذف NA
    common_index = price_series.index.intersection(indicator_series.index)
    if len(common_index) < lookback:
         return False # داده مشترک کافی وجود ندارد
         
    price_series = price_series.loc[common_index]
    indicator_series = indicator_series.loc[common_index]

    if price_series.empty or indicator_series.empty or len(price_series) < 2:
        return False

    last_price = price_series.iloc[-1]
    last_indicator = indicator_series.iloc[-1]
    
    lookback_prices = price_series.iloc[:-1]
    lookback_indicators = indicator_series.iloc[:-1]

    if lookback_prices.empty:
        return False

    try:
        if check_type == 'positive_rsi':
            # RD+: قیمت کف پایین‌تر می‌سازد، اندیکاتور کف بالاتر می‌سازد.
            min_price_idx = lookback_prices.idxmin()
            min_price_val = lookback_prices.loc[min_price_idx]
            indicator_at_min_price = lookback_indicators.loc[min_price_idx]

            if pd.isna(min_price_val) or pd.isna(indicator_at_min_price) or pd.isna(last_price) or pd.isna(last_indicator):
                return False

            # ۱. قیمت کف پایین‌تر زده
            price_makes_lower_low = last_price < min_price_val
            # ۲. اندیکاتور کف بالاتر زده
            indicator_makes_higher_low = last_indicator > indicator_at_min_price
            # ۳. سیگنال در ناحیه کف (مثلاً RSI < 50) معتبرتر است
            is_at_bottom = last_indicator < 50
            
            return price_makes_lower_low and indicator_makes_higher_low and is_at_bottom

        elif check_type == 'negative':
            # RD-: قیمت سقف بالاتر می‌سازد، اندیکاتور سقف پایین‌تر می‌سازد.
            max_price_idx = lookback_prices.idxmax()
            max_price_val = lookback_prices.loc[max_price_idx]
            indicator_at_max_price = lookback_indicators.loc[max_price_idx]

            if pd.isna(max_price_val) or pd.isna(indicator_at_max_price) or pd.isna(last_price) or pd.isna(last_indicator):
                return False

            # ۱. قیمت سقف بالاتر زده
            price_makes_higher_high = last_price > max_price_val
            # ۲. اندیکاتور سقف پایین‌تر زده
            indicator_makes_lower_high = last_indicator < indicator_at_max_price
            
            return price_makes_higher_high and indicator_makes_lower_high

    except Exception as e:
        # e.g., if series is empty or idxmin fails
        logger.warning(f"Divergence check failed: {e}")
        return False
        
    return False


def _check_oscillator_signals(tech_df: pd.DataFrame, close_ser: pd.Series, technical_rec, prev_tech_rec):
    """
    Checks oscillator-based signals like RSI and Stochastic.
    💡 G-Fix: شامل محاسبه صحیح واگرایی با نگاه به گذشته (جایگزین منطق اشتباه ۲ روزه).
    """

    satisfied_filters, reason_parts = [], {"technical": []}
    
    # تعریف دوره زمانی برای بررسی واگرایی
    DIVERGENCE_LOOKBACK = 20 

    # --- RSI Positive Divergence ---
    rsi_series = tech_df['RSI'] if 'RSI' in tech_df.columns else pd.Series(dtype=float)
    
    if _find_divergence(close_ser, rsi_series, DIVERGENCE_LOOKBACK, 'positive_rsi'):
        current_rsi = _get_attr_safe(technical_rec, 'RSI')
        satisfied_filters.append("RSI_Positive_Divergence")
        reason_parts["technical"].append(f"Positive divergence on RSI ({current_rsi:.2f}).")

    
# --- 💡 G-4: MACD Negative Divergence (Penalty) ---
    macd_series = tech_df['MACD'] if 'MACD' in tech_df.columns else pd.Series(dtype=float)
        
    if _find_divergence(close_ser, macd_series, DIVERGENCE_LOOKBACK, 'negative'):
        current_macd = _get_attr_safe(technical_rec, 'MACD')
        satisfied_filters.append("MACD_Negative_Divergence")
        reason_parts["technical"].append(f"Negative divergence on MACD (Price up, MACD down).")

    # --- Stochastic Oscillator ---
    current_stoch_k = _get_attr_safe(technical_rec, 
'Stochastic_K')
    current_stoch_d = _get_attr_safe(technical_rec, 'Stochastic_D')
    prev_stoch_k = _get_attr_safe(prev_tech_rec, 'Stochastic_K')
    prev_stoch_d = _get_attr_safe(prev_tech_rec, 'Stochastic_D')
    if all(x is not None for x in [current_stoch_k, current_stoch_d, prev_stoch_k, prev_stoch_d]):
        if current_stoch_k > current_stoch_d and prev_stoch_k <= prev_stoch_d and current_stoch_d < 25:
            satisfied_filters.append("Stochastic_Bullish_Cross_Oversold")
            reason_parts["technical"].append("Stochastic bullish cross in oversold area.")
            
return satisfied_filters, reason_parts

def _check_trend_signals(technical_rec, prev_tech_rec, last_close_val, market_sentiment: str):
    """
    Checks trend-following signals.
"""
    satisfied_filters, reason_parts = [], {"technical": []}
    
    # --- 💡 G-1: Trend State Definition (using SMA) ---
    sma20 = _get_attr_safe(technical_rec, 'SMA_20')
    sma50 = _get_attr_safe(technical_rec, 'SMA_50')
    is_uptrend = False

    if sma20 is not None and sma50 is not None:
        if sma20 > sma50:
            is_uptrend = True
            if last_close_val > sma20:
               satisfied_filters.append("Strong_Uptrend_Confirmed")
                reason_parts["technical"].append(f"Strong Uptrend (SMA20 > SMA50, Close > SMA20).")
        else:
            satisfied_filters.append("Strong_Downtrend_Confirmed") # Penalty
            reason_parts["technical"].append(f"Downtrend Confirmed (SMA20 < SMA50).")

    # --- 💡 G-Trend: Long-Term Trend Penalty (SMA200) ---
    sma200 = _get_attr_safe(technical_rec, 'SMA_200') # فرض می‌کنیم SMA_200 
محاسبه و ذخیره شده
    if sma200 is not None and last_close_val < sma200:
        satisfied_filters.append("Price_Below_SMA200")
        reason_parts["technical"].append(f"Penalty: Price is below long-term SMA200.")

    # --- MACD Cross ---
    current_macd = _get_attr_safe(technical_rec, 'MACD')
    current_macd_signal = _get_attr_safe(technical_rec, 'MACD_Signal')
    prev_macd = _get_attr_safe(prev_tech_rec, 'MACD')
    prev_macd_signal = _get_attr_safe(prev_tech_rec, 'MACD_Signal')
    macd_crossed = False
    if all(x is not None for x in [current_macd, current_macd_signal, prev_macd, prev_macd_signal]):
    if current_macd > current_macd_signal and prev_macd <= prev_macd_signal:
            satisfied_filters.append("MACD_Bullish_Cross_Confirmed")
            # 💡 FIX: Indent Error (خط ۴۲۰)
            macd_crossed = True
        
    # --- HalfTrend ---
    current_halftrend = _get_attr_safe(technical_rec, 'halftrend_signal')
    prev_halftrend = _get_attr_safe(prev_tech_rec, 'halftrend_signal')
    halftrend_buy = False
    if current_halftrend == 1 and prev_halftrend != 1:
      satisfied_filters.append("HalfTrend_Buy_Signal")
        halftrend_buy = True
        
    # --- Dynamic Resistance Break (from model) ---
    resistance_broken = _get_attr_safe(technical_rec, 'resistance_broken')
    if resistance_broken:
        satisfied_filters.append("Resistance_Broken")
        res_level = _get_attr_safe(technical_rec, 'resistance_level_50d', 'N/A')
        reason_parts["technical"].append(f"Broke a key dynamic resistance level around {res_level}.")

    # --- 💡 G-3: Market Sentiment Penalty (Expanded) ---
    if market_sentiment 
== "Bearish" and (macd_crossed or halftrend_buy or resistance_broken):
        satisfied_filters.append("Signal_Against_Market_Trend")
        reason_parts["technical"].append(f"Penalty: Bullish signal detected in a Bearish market.")

    return satisfied_filters, reason_parts

def _check_volatility_signals(hist_df, technical_rec, last_close_val):
    """Checks volatility-based signals like Bollinger Bands and Squeeze Momentum."""
    satisfied_filters, reason_parts = [], {"technical": []}

    # Bollinger Lower Band
    bollinger_low = _get_attr_safe(technical_rec, 'Bollinger_Low')
    if bollinger_low is not None and last_close_val < bollinger_low:
        satisfied_filters.append("Bollinger_Lower_Band_Touch")

    # Squeeze 
Momentum
    prev_tech_rec = hist_df.iloc[-2] if len(hist_df) > 1 else technical_rec
    current_squeeze_on = _get_attr_safe(technical_rec, 'squeeze_on')
    prev_squeeze_on = _get_attr_safe(prev_tech_rec, 'squeeze_on')
    if current_squeeze_on == False and prev_squeeze_on == True:
        satisfied_filters.append("Squeeze_Momentum_Fired_Long")
        reason_parts["technical"].append("Squeeze Momentum indicator fired long.")
        
    return satisfied_filters, reason_parts

def _check_volume_signals(hist_df, tech_df, technical_rec, close_ser):
    """
    Checks for volume-based signals including Z-Score (last day spike) 
    and Volume MA trend 
(sustained interest).
    """
    satisfied_filters, reason_parts = [], {"technical": []}
    
    # 💡 G-Volume: چک کردن قفل صف (Locked Market)
    is_locked_market = False
    if not hist_df.empty:
        last_hist = hist_df.iloc[-1]
        if last_hist['high'] == last_hist['low']:
            is_locked_market = True
            reason_parts["technical"].append(f"Note: Market was locked (High == Low). Volume spikes ignored.")
    
   # 1. High Volume On Up Day (Z-Score)
    if 'volume' in hist_df.columns and len(hist_df) >= 20 and len(close_ser) > 1:
        volume_z_score = calculate_z_score(pd.to_numeric(hist_df['volume'], errors='coerce').dropna().iloc[-20:])
        # 💡 G-Volume: سیگنال فقط در صورتی فعال می‌شود که صف نباشد
        if volume_z_score is not None and volume_z_score > 1.5 and close_ser.iloc[-1] > close_ser.iloc[-2] and not is_locked_market:
            satisfied_filters.append("High_Volume_On_Up_Day")
           reason_parts["technical"].append(f"High volume (Z-Score: {volume_z_score:.2f}) on a positive day.")

    # 2. 💡 G-Volume: Volume MA Is Rising (Using Slope)
    if is_data_sufficient(tech_df, 10): # حداقل ۱۰ روز داده برای محاسبه شیب
        try:
            # ۱۰ دیتای آخر میانگین حجم ۲۰ روزه
            vol_ma_series = pd.to_numeric(tech_df['Volume_MA_20'].dropna().tail(10))
            if len(vol_ma_series) >= 5: # حداقل ۵ نقطه برای رگرسیون
              x = np.arange(len(vol_ma_series))
                # محاسبه شیب خط رگرسیون
                slope = np.polyfit(x, vol_ma_series, 1)[0] 
                
                # اگر شیب مثبت باشد (روند صعودی)
           if slope > 0:
                    satisfied_filters.append("Volume_MA_Is_Rising")
                    reason_parts["technical"].append(f"Volume MA (20d) slope is positive ({slope:,.0f}).")
        except Exception as e:
            logger.warning(f"Could not calculate volume MA slope: {e}")
           pass 
            
    return satisfied_filters, reason_parts

def _check_technical_filters(hist_df, tech_df, market_sentiment: str):
    """
    Checks all technical indicators by calling specialized sub-functions.
(Coordinator function)
    """
    all_satisfied_filters, all_reason_parts = [], {"technical": []}
    if not is_data_sufficient(tech_df, 2) or not is_data_sufficient(hist_df, MIN_REQUIRED_HISTORY_DAYS):
        return all_satisfied_filters, all_reason_parts

    technical_rec = tech_df.iloc[-1]
    prev_tech_rec = tech_df.iloc[-2]
    
    close_ser = _get_close_series_from_hist_df(hist_df)
    last_close_val = close_ser.iloc[-1] if not close_ser.empty else None
    if last_close_val is None:
        return all_satisfied_filters, all_reason_parts

    # 💡 FIX: Indent Error (خط ۳۵۰)
    # 
Call sub-functions and aggregate results
    for func in [_check_oscillator_signals, _check_trend_signals, _check_volatility_signals, _check_volume_signals]:
        # Adjust arguments as needed per function signature
        
        # 💡 G-Fix: اصلاح فراخوانی تابع _check_oscillator_signals برای ارسال tech_df کامل
        if func == _check_oscillator_signals:
             satisfied, reasons = func(tech_df, close_ser, technical_rec, prev_tech_rec)
        
        elif func == _check_trend_signals:
             satisfied, reasons = func(technical_rec, prev_tech_rec, last_close_val, market_sentiment)
         
        elif func == _check_volatility_signals:
            satisfied, reasons = func(hist_df, technical_rec, last_close_val)
        
        elif func == _check_volume_signals:
            satisfied, reasons = func(hist_df, tech_df, technical_rec, close_ser)
        else:
            continue

   all_satisfied_filters.extend(satisfied)
        if "technical" in reasons:
            all_reason_parts.setdefault("technical", []).extend(reasons["technical"])

    return all_satisfied_filters, all_reason_parts

# --- END REFACTORED SECTION ---

# 💡 G-Fundamental: تابع بازگردانده شده برای فیلتر ساده P/E
def _check_simple_fundamental_filters(symbol_data_rec):
    """
    بررسی فیلترهای ساده فاندامنتال (P/E) برای جلوگیری از سهام حبابی.
"""
    satisfied_filters, reason_parts = [], {"fundamental": []}
    if symbol_data_rec:
        # pe_ratio از ComprehensiveSymbolData خوانده می‌شود
        pe = getattr(symbol_data_rec, 'pe_ratio', None)
        if pe is not None and 0 < pe < 15:
            satisfied_filters.append("Reasonable_PE")
            reason_parts["fundamental"].append(f"P/E ({pe:.2f}) is reasonable (< 15).")
    return satisfied_filters, reason_parts

def _check_smart_money_filters(hist_df):
    """REVISED: Now also 
checks for heavy individual buy pressure on the last day."""
    satisfied_filters = []
    reason_parts = {"smart_money": []}
    trend_lookback = 10
    if hist_df is None or hist_df.empty or 'buy_i_volume' not in hist_df.columns:
        return satisfied_filters, reason_parts

    # Check 1: 10-Day Real Money Flow Trend
    if len(hist_df) >= trend_lookback:
        smart_money_flow_df = calculate_smart_money_flow(hist_df)
        if not smart_money_flow_df.empty and len(smart_money_flow_df) >= trend_lookback:
          trend_net_flow = smart_money_flow_df['individual_net_flow'].iloc[-trend_lookback:].sum()
            if trend_net_flow > 0:
                satisfied_filters.append("Positive_Real_Money_Flow_Trend_10D")
                reason_parts["smart_money"].append(f"Positive real money inflow over the last {trend_lookback} days.")
            elif trend_net_flow < 0:
                satisfied_filters.append("Negative_Real_Money_Flow_Trend_10D")
    
   # NEW Check 2: Heavy Individual Buy Pressure on the last day
    last_day = hist_df.iloc[-1]
    required_cols = ['buy_i_volume', 'buy_count_i', 'sell_i_volume', 'sell_count_i']
    if all(col in last_day and pd.notna(last_day[col]) for col in required_cols):
        if last_day['buy_count_i'] > 0 and last_day['sell_count_i'] > 0:
            per_capita_buy = last_day['buy_i_volume'] / last_day['buy_count_i']
            per_capita_sell = last_day['sell_i_volume'] / last_day['sell_count_i']
            

            # Check if per capita buy is 2.5x greater than sell
            if per_capita_buy > (per_capita_sell * 2.5):
                satisfied_filters.append("Heavy_Individual_Buy_Pressure")
                reason_parts["smart_money"].append(f"Significant buy pressure detected (Per capita buy: {per_capita_buy:,.0f} vs sell: {per_capita_sell:,.0f}).")

    return satisfied_filters, reason_parts


def _check_power_thrust_signal(hist_df, close_ser):
    """
    Checks for a 
Power Thrust Signal: a combination of high volume,
    heavy individual buy pressure, and a positive closing day.
"""
    satisfied_filters, reason_parts = [], {"power_thrust": []}
    
    # اطمینان از وجود داده کافی برای تحلیل
    if hist_df is None or len(hist_df) < 20 or close_ser.empty or len(close_ser) < 2:
        return satisfied_filters, reason_parts

    last_day = hist_df.iloc[-1]
    
    # --- 💡 G-Volume: چک کردن قفل صف ---
    is_locked_market = last_day['high'] == last_day['low']
    if is_locked_market:
        return satisfied_filters, reason_parts # سیگنال قدرت 
در صف خرید/فروش، معتبر نیست

    # --- شرط ۱: روز معاملاتی مثبت ---
    is_up_day = close_ser.iloc[-1] > close_ser.iloc[-2]
    if not is_up_day:
        return satisfied_filters, reason_parts # اگر روز 
مثبت نیست، ادامه نده

    # --- شرط ۲: حجم معاملات بسیار بالا (استفاده از Z-Score) ---
    volume_series = pd.to_numeric(hist_df['volume'], errors='coerce').dropna().tail(20)
    volume_z_score = calculate_z_score(volume_series)
    # آستانه بالا برای حجم، مثلا Z-Score بیشتر از 1.8
    is_high_volume = volume_z_score is not None and volume_z_score > 
1.8 
    
    # --- شرط ۳: فشار خرید سنگین حقیقی‌ها ---
    is_heavy_buy_pressure = False
    required_cols = ['buy_i_volume', 'buy_count_i', 'sell_i_volume', 'sell_count_i']
    if all(col in last_day and pd.notna(last_day[col]) for col in required_cols):
        if last_day['buy_count_i'] > 0 and last_day['sell_count_i'] > 0:
            per_capita_buy = last_day['buy_i_volume'] / last_day['buy_count_i']
            per_capita_sell = last_day['sell_i_volume'] / last_day['sell_count_i']
        # آستانه بالا برای سرانه خرید، مثلا ۲.۵ برابر سرانه فروش
            if per_capita_buy > (per_capita_sell * 2.5):
                is_heavy_buy_pressure = True

    # --- ترکیب نهایی ---
    if is_high_volume and is_heavy_buy_pressure:
        satisfied_filters.append("Power_Thrust_Signal")
        reason_parts["power_thrust"].append(
            f"Power signal detected! 
High Volume (Z-Score: {volume_z_score:.2f}) and Heavy Buy Pressure."
        )
            
    return satisfied_filters, reason_parts


# --- NEW: Functions for new strategic filters --- 
def _get_leading_sectors():
    """ 
    با کوئری به جدول DailySectorPerformance، لیست صنایع پیشرو (مثلاً 4 
صنعت برتر) در آخرین روز تحلیل شده را برمی‌گرداند.
"""
    try:
        latest_date_query = db.session.query(func.max(DailySectorPerformance.jdate)).scalar()
        if not latest_date_query:
            logger.warning("هیچ داده‌ای در جدول تحلیل صنایع یافت نشد. از لیست پیش‌فرض استفاده می‌شود.")
            return {"خودرو و ساخت قطعات"} # Fallback

        # دریافت 4 صنعت برتر در آخرین روز
        leading_sectors_query = db.session.query(DailySectorPerformance.sector_name)\
           .filter(DailySectorPerformance.jdate == latest_date_query)\
            .order_by(DailySectorPerformance.rank.asc())\
            .limit(4).all()
        leading_sectors = {row[0] for row in leading_sectors_query}
        logger.info(f"صنایع پیشرو شناسایی شده از دیتابیس: {leading_sectors}")
        return leading_sectors
    except Exception as e:
        logger.error(f"خطا در دریافت صنایع پیشرو از دیتابیس: {e}")
        return {"خودرو و ساخت 
قطعات"} # Fallback in case of error

def _check_sector_strength_filter(symbol_sector, leading_sectors):
    """Checks if the symbol belongs to a leading sector."""
    satisfied_filters, reason_parts = [], {"sector_strength": []}
    if symbol_sector in leading_sectors:
        satisfied_filters.append("IsInLeadingSector")
        reason_parts["sector_strength"].append(f"Symbol is in a leading sector: {symbol_sector}.")
    return satisfied_filters, reason_parts

def _check_static_levels_filters(technical_rec, last_close_val):
    """ 
    Checks for proximity to static support or breakout of static resistance.
This assumes 'static_support_level' and 'static_resistance_level' fields are pre-calculated and available in the TechnicalIndicatorData record.
"""
    satisfied_filters, reason_parts = [], {"static_levels": []}

    is_rec_valid = technical_rec is not None and ( 
        not isinstance(technical_rec, pd.Series) or not technical_rec.empty 
    )
    if not is_rec_valid or last_close_val is None or last_close_val <= 0:
        return satisfied_filters, reason_parts

    # 1. Check for proximity to static support
    support_level = _get_attr_safe(technical_rec, 'static_support_level')

    if support_level and support_level > 0:
        distance = 
last_close_val - support_level
        proximity_percent = abs(distance) / support_level 
        if proximity_percent <= 0.02 and distance >= -0.005 * support_level: # نزدیک یا کمی بالاتر از سطح (حداکثر 0.5% زیر سطح)
            satisfied_filters.append("Near_Static_Support")
            reason_parts["static_levels"].append(
                f"Price is near a major static support level at {support_level:,.0f} (Proximity: {proximity_percent*100:.1f}%)."
      )

    # 2. Check for breakout of static resistance
    resistance_level = _get_attr_safe(technical_rec, 'resistance_level_50d')

    if resistance_level and resistance_level > 0:
        if last_close_val > resistance_level and last_close_val < 1.03 * resistance_level: # شرط دوم: قیمت بیش از حد از مقاومت دور نشده باشد (شکست تازه و معتبر)
            satisfied_filters.append("Static_Resistance_Broken")
            reason_parts["static_levels"].append(
          f"Price broke a major static resistance level at {resistance_level:,.0f}."
            )

    return satisfied_filters, reason_parts
# --- END NEW STRATEGIC FILTERS --- 


# 💡 G-Performance: تابع کمکی برای اجرای موازی
def _analyze_single_symbol(
    symbol: ComprehensiveSymbolData,
    hist_groups: Dict[str, pd.DataFrame],
    tech_groups: Dict[str, pd.DataFrame],
    ml_prediction_set: set,
    leading_sectors: set,
    market_sentiment: str,
    score_threshold: int,
    today_jdate: str
) -> Optional[Dict]:
    """
    هسته 
منطق تحلیل برای یک نماد واحد.
    این تابع برای اجرای موازی (joblib) طراحی شده است.
"""
    
    # 💡 G-Performance: این تابع کمکی باید در داخل تابع اصلی تعریف شود
    # یا به عنوان یک تابع مستقل در سطح بالا (که در اینجا انتخاب شده)
    def run_check(check_func, *args):
        filters, reasons = check_func(*args)
        all_satisfied_filters.extend(filters)
        all_reason_parts.update(reasons)

    try:
        symbol_hist_df = hist_groups.get(symbol.symbol_id, pd.DataFrame()).copy()
        symbol_tech_df = tech_groups.get(symbol.symbol_id, pd.DataFrame()).copy()

    if len(symbol_hist_df) < MIN_REQUIRED_HISTORY_DAYS:
            logger.debug(f"Skipped {symbol.symbol_name} ({symbol.symbol_id}): insufficient history ({len(symbol_hist_df)} < {MIN_REQUIRED_HISTORY_DAYS})")
            return None

        last_close_series = _get_close_series_from_hist_df(symbol_hist_df)
        entry_price = float(last_close_series.iloc[-1]) if not last_close_series.empty else None
        if entry_price is None or pd.isna(entry_price):
            logger.warning(f"Skipping {symbol.symbol_name} ({symbol.symbol_id}) due to missing entry price.")
         return None

        all_satisfied_filters = []
        all_reason_parts = {}

        technical_rec = symbol_tech_df.iloc[-1] if not symbol_tech_df.empty else None
        
        # 💡 G-Fundamental: از خود آبجکت symbol برای P/E استفاده می‌کنیم
        symbol_data_rec = symbol 

        # --- Run All Filters ---
    run_check(_check_sector_strength_filter, getattr(symbol, 'sector_name', ''), leading_sectors)
        run_check(_check_technical_filters, symbol_hist_df, symbol_tech_df, market_sentiment)
        run_check(_check_market_condition_filters, symbol_hist_df, symbol_tech_df)
        run_check(_check_static_levels_filters, technical_rec, entry_price)
        run_check(_check_simple_fundamental_filters, symbol_data_rec) # 💡 G-Fundamental
        run_check(_check_smart_money_filters, symbol_hist_df)
        run_check(_check_power_thrust_signal, symbol_hist_df, last_close_series)

        # ML Filter
        if symbol.symbol_id in ml_prediction_set:
           all_satisfied_filters.append("ML_Predicts_Uptrend")
            all_reason_parts.setdefault("ml_signal", []).append("ML model predicts a high-probability uptrend.")

        # Calculate Weighted Score
        score = sum(FILTER_WEIGHTS.get(f, {}).get('weight', 0) for f in all_satisfied_filters)

        if score >= score_threshold:
            return {
                "symbol_id": symbol.symbol_id,
           "symbol_name": symbol.symbol_name,
                "entry_price": entry_price,
                "entry_date": date.today(),
                "jentry_date": today_jdate,
                "outlook": "Bullish",
                "reason_json": json.dumps(all_reason_parts, ensure_ascii=False),
          "satisfied_filters": json.dumps(list(set(all_satisfied_filters)), ensure_ascii=False),
                "score": score
            }
        
        return None # Score below threshold
    
    except Exception as e:
        logger.error(f"Error processing symbol {symbol.symbol_id}: {e}", exc_info=True)
        return None


def run_weekly_watchlist_selection():
    """
  انتخاب نمادها برای واچ‌لیست هفتگی با استفاده از داده‌های حجیم،
    امتیازدهی دینامیک و فیلترهای پیشرفته استراتژیک.
"""
    # 💡 G-CleanUp: ایمپورت‌ها قبلاً به بالا منتقل شدند
    
    logger.info("🚀 Starting Weekly Watchlist selection process...")
    start_time = time.time()

    # Step 1: Determine market sentiment and leading sectors
    market_sentiment = _get_market_sentiment()
    leading_sectors = _get_leading_sectors()

    if market_sentiment == "Bullish":
        score_threshold = 7
    elif market_sentiment == "Neutral":
        score_threshold = 8
    else:
    score_threshold = 10

    logger.info(f"📈 Market sentiment: {market_sentiment}, Score threshold: >= {score_threshold}")
    logger.info(f"🏭 Leading sectors identified: {leading_sectors}")

    # Step 2: Bulk Data Fetching
    allowed_market_types = ['بورس', 'فرابورس', 'پایه فرابورس', 'بورس کالا', 'بورس انرژی']

    # Fetch active symbols
    symbols_to_analyze = ComprehensiveSymbolData.query.filter(
        ComprehensiveSymbolData.market_type.in_(allowed_market_types)
    ).all()

    if not symbols_to_analyze:
        logger.warning("⚠️ No symbols found for analysis.")
        return False, "No active 
symbols available for analysis."
    
    # 💡 FIX: Indent Error (خط ۵۵۰)
    symbol_ids = [s.symbol_id for s in symbols_to_analyze]
    total_symbols = len(symbol_ids)
    logger.info(f"🧩 Found {total_symbols} active symbols for analysis.")

    fetch_start = time.time()

    # Calculate lookback date
    today_greg = datetime.now().date()
    # 💡 G-Trend: افزایش lookback برای پوشش SMA200 (اگرچه داده‌های تکنیکال ممکن است هنوز محدود باشند)
    lookback_greg = today_greg - timedelta(days=max(TECHNICAL_DATA_LOOKBACK_DAYS * 2, 250))
    lookback_jdate_str = convert_gregorian_to_jalali(lookback_greg)
 if not lookback_jdate_str:
        logger.error("❌ Could not determine lookback date. 
Aborting.")
        return False, "Failed to calculate lookback date."
    # Fetch Historical Data
    hist_records = HistoricalData.query.filter(
        HistoricalData.symbol_id.in_(symbol_ids),
        HistoricalData.jdate >= lookback_jdate_str
    ).all()
    hist_df = pd.DataFrame([h.__dict__ for h in hist_records]).drop(columns=['_sa_instance_state'], errors='ignore')

    # Fetch Technical Data
    tech_records = TechnicalIndicatorData.query.filter(
        TechnicalIndicatorData.symbol_id.in_(symbol_ids),
        TechnicalIndicatorData.jdate >= lookback_jdate_str
    ).all()
    tech_df = pd.DataFrame([t.__dict__ for t 
in tech_records]).drop(columns=['_sa_instance_state'], errors='ignore')

    # 💡 G-CleanUp: حذف واکشی فاندامنتال/کندل
    today_jdate = get_today_jdate_str()

    # Fetch ML Predictions
    ml_predictions = MLPrediction.query.filter(
        MLPrediction.symbol_id.in_(symbol_ids),
        MLPrediction.jprediction_date == today_jdate,
        MLPrediction.predicted_trend == 'Uptrend'
    ).all()

    fetch_end = time.time()
    logger.info(f"📥 Bulk fetch time: {fetch_end - fetch_start:.2f}s (hist: {len(hist_df)}, tech: {len(tech_df)} rows)")

    # Grouping Data
    group_start = time.time()
    hist_groups = {k: v.sort_values(by='jdate') for k, v in hist_df.groupby("symbol_id")} if not hist_df.empty else {}

    tech_groups = {k: v.sort_values(by='jdate') for k, v in tech_df.groupby("symbol_id")} if not tech_df.empty else {}
    ml_prediction_set = {rec.symbol_id for rec in ml_predictions}

    group_end = time.time()
    logger.info(f"📊 Data grouping completed: {group_end - group_start:.2f}s. 
Groups: hist={len(hist_groups)}, tech={len(tech_groups)}")

    logger.info("📊 Data grouping completed. Beginning scoring and selection...")

    # Step 3: 💡 G-Performance: Scoring (Parallelized)
    loop_start = time.time()
    
    # استفاده از n_jobs=-1 برای استفاده از تمام هسته‌ها
    # verbose=5 برای نمایش لاگ پیشرفت
    parallel = Parallel(n_jobs=-1, verbose=5) 
    
    results = parallel(delayed(_analyze_single_symbol)(
        symbol,
        hist_groups,
        tech_groups,
    ml_prediction_set,
        leading_sectors,
        market_sentiment,
        score_threshold,
        today_jdate
    ) for symbol in symbols_to_analyze)
    
    # فیلتر کردن نتایج None (سهامی که رد شدند)
    watchlist_candidates = [res for res in results if res is not None]

    loop_end = time.time()
    processed_count = total_symbols # joblib همه را پردازش می‌کند
    skipped_count = total_symbols - len(results) # (تخمین، چون Noneها هم در results هستند)


    logger.info(f"🔄 Parallel loop completed: Processed {processed_count}/{total_symbols} symbols. 
Loop time: {loop_end - loop_start:.2f}s")

    logger.info(f"✅ {len(watchlist_candidates)} symbols passed the threshold ({score_threshold}). Saving top 8...")

    # Step 4: Save results
    watchlist_candidates.sort(key=lambda x: x['score'], reverse=True)
    final_watchlist = watchlist_candidates[:8]

    saved_count = 0
    for candidate in final_watchlist:
        existing_result = WeeklyWatchlistResult.query.filter_by(
            symbol_id=candidate['symbol_id'], jentry_date=candidate['jentry_date']
        ).first()

        if existing_result:
       existing_result.entry_price = candidate['entry_price']
            existing_result.outlook = candidate['outlook']
            existing_result.reason = candidate['satisfied_filters']
            existing_result.probability_percent = min(100, candidate['score'] * 4)
            existing_result.created_at = datetime.now()
            setattr(existing_result, 'score', candidate['score']) # 💡 G-Fix: ذخیره امتیاز در آپدیت
        else:
        existing_result = WeeklyWatchlistResult(
                signal_unique_id=str(uuid.uuid4()),
                symbol_id=candidate['symbol_id'],
                symbol_name=candidate['symbol_name'],
                entry_price=candidate['entry_price'],
                entry_date=candidate['entry_date'],
          jentry_date=candidate['jentry_date'],
                outlook=candidate['outlook'],
                reason=candidate['satisfied_filters'],
                probability_percent=min(100, candidate['score'] * 4),
                status='active',
                score=candidate['score'] # 💡 G-Fix: ذخیره امتیاز در ردیف جدید
      )
        db.session.add(existing_result)
        saved_count += 1

    try:
        db.session.commit()
        message = f"Weekly Watchlist selection completed. 
Saved {saved_count} symbols."
        logger.info(message)
        total_time = time.time() - start_time
        logger.info(f"⏱️ Full process time: {total_time:.2f}s")
        return True, message
    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ Database commit failed: {e}", exc_info=True)
        return False, "Database commit failed."



def get_weekly_watchlist_results(jdate_str: Optional[str] = None):
    """
    بازیابی آخرین نتایج واچ‌لیست 
هفتگی (یا تاریخ مشخص‌شده در صورت ارسال).
    خروجی شامل جزئیات نمادها و آخرین تاریخ به‌روزرسانی است.
"""

    logger.info("📊 Retrieving latest weekly watchlist results...")

    # --- Step 1: تعیین آخرین تاریخ ---
    if not jdate_str:
        latest_record = WeeklyWatchlistResult.query.order_by(WeeklyWatchlistResult.jentry_date.desc()).first()
        if not latest_record or not latest_record.jentry_date:
            logger.warning("⚠️ No weekly watchlist results found.")
            return {"top_watchlist_stocks": [], "last_updated": "نامشخص"}
        # 💡 FIX: Indent Error (خط ۶۸۰)
      jdate_str = latest_record.jentry_date

    logger.info(f"🗓 Latest Weekly Watchlist results date: {jdate_str}")

    # --- Step 2: واکشی نتایج ---
    results = WeeklyWatchlistResult.query.filter_by(jentry_date=jdate_str)\
                                         .order_by(WeeklyWatchlistResult.score.desc().nullslast(), WeeklyWatchlistResult.created_at.desc()).all()

    if not results:
        logger.warning("⚠️ No results found for this date.")
     return {"top_watchlist_stocks": [], "last_updated": jdate_str}

    # --- Step 3: واکشی نام شرکت‌ها برای نمادهای واچ‌لیست ---
    symbol_ids_in_watchlist = [r.symbol_id for r in results]
    company_name_map = {}

    if symbol_ids_in_watchlist:
        company_name_records = ComprehensiveSymbolData.query.filter(
            ComprehensiveSymbolData.symbol_id.in_(symbol_ids_in_watchlist)
        ).with_entities(
            ComprehensiveSymbolData.symbol_id,
            ComprehensiveSymbolData.company_name
  ).all()
        company_name_map = {sid: cname for sid, cname in company_name_records}

    # --- Step 4: ساخت خروجی نهایی ---
    output_stocks = []
    for r in results:
        output_stocks.append({
            "signal_unique_id": r.signal_unique_id,
            "symbol_id": r.symbol_id,
            "symbol_name": r.symbol_name,
         "company_name": company_name_map.get(r.symbol_id, r.symbol_name),
            "outlook": r.outlook,
            "reason": r.reason,
            "entry_price": r.entry_price,
            "jentry_date": r.jentry_date,
            "exit_price": r.exit_price,
            "jexit_date": r.jexit_date,
            "profit_loss_percentage": r.profit_loss_percentage,
          "status": r.status,
            "probability_percent": getattr(r, "probability_percent", None),
            "score": getattr(r, "score", None)
        })

    # --- Step 5: مرتب‌سازی بر اساس امتیاز ---
    # 💡 G-Fix: اطمینان از مرتب‌سازی حتی اگر score برابر با None باشد
    output_stocks.sort(key=lambda x: (x.get("score") or 0), reverse=True)

    logger.info(f"✅ Retrieved and enriched {len(output_stocks)} weekly watchlist results for {jdate_str}.")

# --- Step 6: بازگرداندن پاسخ نهایی ---
    return {
        "top_watchlist_stocks": output_stocks,
        "last_updated": jdate_str
    }
}
