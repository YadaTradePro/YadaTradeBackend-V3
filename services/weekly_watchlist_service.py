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
from sqlalchemy import func, text, and_
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
from services.technical_analysis_utils import (
    get_today_jdate_str, normalize_value, calculate_rsi, 
    calculate_macd, calculate_sma, calculate_bollinger_bands, 
    calculate_volume_ma, calculate_atr, calculate_smart_money_flow, 
    convert_gregorian_to_jalali, calculate_z_score, calculate_static_support
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
    "Near_Major_Static_Support": {
        "weight": 5, 
        "description": "نزدیکی به حمایت اصلی: قیمت در محدوده 0% تا 3% بالای سطح حمایت استاتیک کلیدی قرار دارد (ورود با ریسک کم)."
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
        "weight": -7,
        "description": "جریمه روند بلندمدت: قیمت پایین‌تر از میانگین متحرک ۲۰۰ روزه است."
    },
    "Strong_Downtrend_Confirmed": {
        "weight": -4,
        "description": "جریمه روند نزولی: SMA_20 پایین‌تر از SMA_50 قرار دارد."
    },
    "MACD_Negative_Divergence": {
        "weight": -6,
        "description": "جریمه واگرایی منفی: قیمت سقف جدید زده اما MACD سقف پایین‌تری ثبت کرده (نشانه ضعف شدید روند)."
    },
    "RSI_Is_Overbought": {
        "weight": -8,
        "description": "جریمه اشباع خرید: RSI در ناحیه اشباع خرید قرار دارد که ریسک اصلاح قیمت را افزایش می‌دهد."
    },
    "Price_Too_Stretched_From_SMA50": {
        "weight": -5,
        "description": "جریمه فاصله زیاد: قیمت فاصله زیادی از میانگین متحرک ۵۰ روزه گرفته که احتمال بازگشت به میانگین را بالا می‌برد."
    },
    "Negative_Real_Money_Flow_Trend_10D": {
        "weight": -3,
        "description": "جریمه خروج پول: برآیند ورود پول هوشمند در ۱۰ روز گذشته منفی بوده است (خروج پول)."
    },
    "Signal_Against_Market_Trend": {
        "weight": -2,
        "description": "جریمه خلاف جهت بازار: سیگنال صعودی (مثل MACD Cross) در یک بازار خرسی صادر شده است."
    },
    "Break_Below_Static_Support": { 
        "weight": -8, 
        "description": "شکست حمایت اصلی: قیمت به زیر سطح حمایت استاتیک کلیدی سقوط کرده که نشانه ضعف شدید یا تغییر روند است."
    },
    
    # --- 💡 NEW FILTERS ADDED HERE (درخواست شما) ---
    "Price_Near_50Day_High": {
        "weight": -4, # جریمه متوسط
        "description": "جریمه نزدیکی به سقف ۵۰ روزه: قیمت بسیار نزدیک به سقف اخیر است (کمتر از ۵٪ فاصله) و پتانسیل اصلاح دارد."
    },
    "Price_Far_From_Static_Support_160D": {
        "weight": -8, # جریمه سنگین
        "description": "جریمه فاصله زیاد از حمایت ۱۶۰ روزه: قیمت بیش از ۲۵٪ بالاتر از حمایت استاتیک ۱۶۰ روزه قرار دارد (خطر حباب)."
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

        # Group by index_type
        # 💡 G-Sentiment: استفاده از 'Total_Index' یا 'TEDPIX' به عنوان شاخص کل
        total_changes = [d.percent_change for d in index_data if d.index_type == 'Total_Index' or d.index_type == 'TEDPIX']
        equal_changes = [d.percent_change for d in index_data if d.index_type == 'Equal_Weighted_Index']

        # 💡 FIX: Indentation error corrected
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
            return "Bearish"
        else:
            logger.info(f"Market Sentiment: Neutral (Total: {avg_total:.2f}%, Equal: {avg_equal:.2f}%)")
            return "Neutral"
            
    # 💡 FIX: Indentation error corrected (moved to be aligned with try)
    except Exception as e:
        logger.error(f"Error fetching market sentiment from DB: {e}, defaulting to Neutral")
        return "Neutral"


# --- REVISED: Filter Functions ---
def _check_market_condition_filters(hist_df, tech_df):
    """
    Checks for individual stock conditions like overbought state 
    or consolidation.
    Also includes the NEW 50-day High Proximity Penalty.
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

        # 💡 G-2: جریمه فاصله زیاد (شرط) 
        if stretch_percent > 15:
            satisfied_filters.append("Price_Too_Stretched_From_SMA50")
            reason_parts["market_condition"].append(
                f"Price is overextended ({stretch_percent:.1f}%) from SMA50."
            )
        
        # 💡 G-2: پاداش پولبک (شرط) 
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

    # --- 💡 NEW FILTER 1: High Price Proximity Penalty (50-day High) ---
    # هدف: جریمه نمادهایی که به سقف ۵۰ روزه بسیار نزدیک هستند (Gap > -5%)
    if len(close_ser) >= 50:
        recent_high_50d = close_ser.tail(50).max()
        if recent_high_50d > 0:
            gap_percent = ((last_close - recent_high_50d) / recent_high_50d) * 100
            
            # اگر فاصله (Gap) بیشتر از -5% باشد (یعنی مثلا -3%، -1% یا 0%)
            if gap_percent > -5:
                satisfied_filters.append("Price_Near_50Day_High")
                reason_parts["market_condition"].append(
                    f"Penalty: Price is too close to 50-day high (Gap: {gap_percent:.1f}%)."
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
    
    # 💡 FIX: Indentation error corrected
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
            min_price_idx = lookback_prices.idxmin() # Fix: Indent
            min_price_val = lookback_prices.loc[min_price_idx]
            indicator_at_min_price = lookback_indicators.loc[min_price_idx]

            if pd.isna(min_price_val) or pd.isna(indicator_at_min_price) or pd.isna(last_price) or pd.isna(last_indicator):
                return False

            # ۱.
            price_makes_lower_low = last_price < min_price_val # Fix: Indent
            # ۲.
            indicator_makes_higher_low = last_indicator > indicator_at_min_price # Fix: Indent
            # ۳.
            is_at_bottom = last_indicator < 50 # Fix: Indent
            
            return price_makes_lower_low and indicator_makes_higher_low and is_at_bottom

        elif check_type == 'negative':
            # RD-: قیمت سقف بالاتر می‌سازد، اندیکاتور سقف پایین‌تر می‌سازد.
            max_price_idx = lookback_prices.idxmax() # Fix: Indent
            max_price_val = lookback_prices.loc[max_price_idx]
            indicator_at_max_price = lookback_indicators.loc[max_price_idx]

            if pd.isna(max_price_val) or pd.isna(indicator_at_max_price) or pd.isna(last_price) or pd.isna(last_indicator):
                return False

            # ۱.
            price_makes_higher_high = last_price > max_price_val # Fix: Indent
            # ۲.
            indicator_makes_lower_high = last_indicator < indicator_at_max_price # Fix: Indent
            
            return price_makes_higher_high and indicator_makes_lower_high

    except Exception as e:
        # e.g., if series is empty or idxmin fails
        logger.warning(f"Divergence check failed: {e}")
        return False
        
    # 💡 FIX: Indentation error corrected (aligned with the function body)
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
    current_stoch_k = _get_attr_safe(technical_rec, 'Stochastic_K')
    current_stoch_d = _get_attr_safe(technical_rec, 'Stochastic_D')
    prev_stoch_k = _get_attr_safe(prev_tech_rec, 'Stochastic_K')
    prev_stoch_d = _get_attr_safe(prev_tech_rec, 'Stochastic_D')
    if all(x is not None for x in [current_stoch_k, current_stoch_d, prev_stoch_k, prev_stoch_d]):
        if current_stoch_k > current_stoch_d and prev_stoch_k <= prev_stoch_d and current_stoch_d < 25:
            satisfied_filters.append("Stochastic_Bullish_Cross_Oversold")
            reason_parts["technical"].append("Stochastic bullish cross in oversold area.")
            
    # 💡 FIX: Indentation error corrected
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
    sma200 = _get_attr_safe(technical_rec, 'SMA_200') 
    # فرض می‌کنیم SMA_200 محاسبه و ذخیره شده
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
        # 💡 FIX: Indentation error corrected
        if current_macd > current_macd_signal and prev_macd <= prev_macd_signal:
            satisfied_filters.append("MACD_Bullish_Cross_Confirmed")
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
    if market_sentiment == "Bearish" and (macd_crossed or halftrend_buy or resistance_broken):
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

    # Squeeze Momentum
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
    and Volume MA trend (sustained interest).
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
            if len(vol_ma_series) >= 5: # حداقل ۵ 
                x = np.arange(len(vol_ma_series))
                # محاسبه شیب خط رگرسیون
                slope = np.polyfit(x, vol_ma_series, 1)[0] 
                
                # اگر شیب مثبت باشد 
                if slope > 0:
                    satisfied_filters.append("Volume_MA_Is_Rising")
                    reason_parts["technical"].append(f"Volume MA (20d) slope is positive ({slope:,.0f}).")
        except Exception as e:
            logger.warning(f"Could not calculate volume MA slope: {e}")
            pass
            
    # 💡 FIX: Indentation error corrected
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

    # 💡 FIX: Indent Error (خط ۳۵۰) (Ensuring the for loop is at the correct level)
    # Call sub-functions and aggregate results
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

        # 💡 FIX: Indentation error corrected (this belongs in the loop)
        all_satisfied_filters.extend(satisfied) # Fix: Indent
        if "technical" in reasons:
            all_reason_parts.setdefault("technical", []).extend(reasons["technical"]) # Fix: Indent

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
    """REVISED: Now also checks for heavy individual buy pressure on the last day."""
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
                reason_parts["smart_money"].append(f"Negative real money outflow over the last {trend_lookback} days.")

    #NEW Check 2: Heavy Individual Buy Pressure on the last day
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
    Checks for a Power Thrust Signal: a combination of high volume,
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
        return satisfied_filters, reason_parts # سیگنال قدرت در صف خرید/فروش، معتبر نیست

    
    # --- شرط ۱: روز معاملاتی مثبت ---
    is_up_day = close_ser.iloc[-1] > close_ser.iloc[-2]
    if not is_up_day:
        return satisfied_filters, reason_parts # اگر روز مثبت نیست، ادامه نده

    # --- شرط ۲: حجم معاملات بسیار بالا 
    # (استفاده از Z-Score) ---
    volume_series = pd.to_numeric(hist_df['volume'], errors='coerce').dropna().tail(20)
    volume_z_score = calculate_z_score(volume_series)
    # آستانه بالا برای حجم، مثلا Z-Score بیشتر از 1.8
    is_high_volume = volume_z_score is not None and volume_z_score > 1.8

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
            f"Power signal detected! High Volume (Z-Score: {volume_z_score:.2f}) and Heavy Buy Pressure."
        )
    return satisfied_filters, reason_parts

# --- NEW: Functions for new strategic filters ---
def _get_leading_sectors():
    """ 
    با کوئری به جدول DailySectorPerformance، لیست صنایع پیشرو (مثلاً 4 صنعت برتر) در آخرین روز تحلیل شده را برمی‌گرداند.
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
        # 💡 FIX: Indentation error corrected
        return {"خودرو و ساخت قطعات"} # Fallback in case of error

def _check_sector_strength_filter(symbol_sector, leading_sectors):
    """Checks if the symbol belongs to a leading sector."""
    satisfied_filters, reason_parts = [], {"sector_strength": []}
    if symbol_sector in leading_sectors:
        satisfied_filters.append("IsInLeadingSector")
        reason_parts["sector_strength"].append(f"Symbol is in a leading sector: {symbol_sector}.")
    return satisfied_filters, reason_parts

def _check_static_levels_filters(hist_df: pd.DataFrame, technical_rec: pd.Series, last_close_val: float) -> Tuple[List[str], Dict[str, List[str]]]:
    """ 
    تلفیق منطق: محاسبه دینامیک حمایت استاتیک و بررسی مقاومت استاتیک از پیش محاسبه شده.
    """
    satisfied_filters, reason_parts = [], {"static_levels": []}

    # --- A. محاسبه دینامیک حمایت استاتیک --- (منطق جدید)
    # این تابع را فرض می‌کنیم که با DataFrame کار می‌کند
    MAJOR_STATIC_SUPPORT = calculate_static_support(hist_df, lookback_period=120)

    if MAJOR_STATIC_SUPPORT is not None:
        reason_parts["static_levels"].append(f"حمایت استاتیک اصلی شناسایی شده: {MAJOR_STATIC_SUPPORT:,.0f}")
        
        # فیلتر نزدیکی به حمایت (Near_Major_Static_Support)
        PROXIMITY_THRESHOLD = 0.07 # 7%
        distance_from_support = ((last_close_val - MAJOR_STATIC_SUPPORT) / MAJOR_STATIC_SUPPORT) * 100
        
        if 0 <= distance_from_support <= PROXIMITY_THRESHOLD * 100:
            satisfied_filters.append("Near_Major_Static_Support") # نام فیلتر جدید
            
        # فیلتر جریمه شکست حمایت (Break_Below_Static_Support)
        if last_close_val < MAJOR_STATIC_SUPPORT:
            satisfied_filters.append("Break_Below_Static_Support") # نام فیلتر جریمه

    # --- B. بررسی مقاومت استاتیک (منطق قدیمی شما) ---
    # 📢 توجه: این بخش همچنان نیاز به فیلد 'static_resistance_level' در technical_rec دارد.
    static_resistance = _get_attr_safe(technical_rec, 'static_resistance_level')
    if static_resistance is not None and static_resistance > 0:
        distance_from_resistance = ((last_close_val - static_resistance) / static_resistance) * 100
        if distance_from_resistance > 1:
            satisfied_filters.append("Static_Resistance_Broken")
            reason_parts["static_levels"].append(f"Static resistance broken ({static_resistance:,.0f}, {distance_from_resistance:.1f}% above).")

    # --- 💡 NEW FILTER 2: Static Support Distance Penalty (160D) ---
    # هدف: جریمه نمادهایی که فاصله بسیار زیادی (بیش از ۲۵٪) از حمایت استاتیک ۱۶۰ روزه گرفته‌اند
    static_support_160 = calculate_static_support(hist_df, lookback_period=160)
    if static_support_160 is not None and static_support_160 > 0:
        diff_percent_160 = ((last_close_val - static_support_160) / static_support_160) * 100
        
        if diff_percent_160 > 25:
            satisfied_filters.append("Price_Far_From_Static_Support_160D")
            reason_parts["static_levels"].append(
                f"Penalty: Price is {diff_percent_160:.1f}% above 160-day static support (Risk of bubble)."
            )

    return satisfied_filters, reason_parts

def _check_ml_prediction_filter(symbol_id):
    """
    Checks the MLPrediction table for a positive prediction for the symbol.
    """
    satisfied_filters, reason_parts = [], {"ml_prediction": []}

    try:
        # 💡 G-ML: استفاده از latest_prediction_date
        latest_prediction_date = db.session.query(func.max(MLPrediction.jprediction_date)).scalar()
        
        if not latest_prediction_date:
            return satisfied_filters, reason_parts

        prediction = db.session.query(MLPrediction).filter(
            MLPrediction.symbol_id == symbol_id,
            MLPrediction.jprediction_date == latest_prediction_date
        ).first()

        # 💡 G-ML: شرط برای روند صعودی: 'Uptrend' و اطمینان بالا (مثلاً > 60%)
        if prediction and prediction.prediction == 'Uptrend' and prediction.probability_percent > 60:
            satisfied_filters.append("ML_Predicts_Uptrend")
            reason_parts["ml_prediction"].append(f"ML predicts uptrend (Prob: {prediction.probability_percent:.1f}%).")

    except Exception as e:
        logger.error(f"Error checking ML prediction for {symbol_id}: {e}")
        pass

    return satisfied_filters, reason_parts

class WeeklyWatchlistService:
    """
    سرویس اصلی برای محاسبه و تولید لیست هفتگی سهام مستعد رشد (Watchlist).
    """

    def __init__(self):
        # 💡 فرض: تابع _get_leading_sectors تعریف شده است
        self.leading_sectors = _get_leading_sectors() 

    def _get_history_and_tech_data(self, symbol_id, symbol_name, days_back=200):
        # 💡 G-Fix: تبدیل start_date گرگوری به رشته جلالی
        start_date = (datetime.now().date() - timedelta(days=days_back))
        # 💡 فرض: تابع convert_gregorian_to_jalali تعریف شده است
        start_jdate_str = convert_gregorian_to_jalali(start_date)

        hist_df, tech_df = pd.DataFrame(), pd.DataFrame()
        
        # 💡 G-Fix: استفاده از یک نشست موقت جدید برای هر Thread
        local_session = db.session.session_factory()
        
        try:
            # --- واکشی داده‌های تاریخی (HistoricalData) ---
            # فیلتر بر اساس رشته جلالی
        
            hist_records = local_session.query(HistoricalData).filter(
                HistoricalData.symbol_id == symbol_id,
                HistoricalData.jdate >= start_jdate_str 
            ).order_by(HistoricalData.jdate.asc()).all()
            
            # --- واکشی داده‌های اندیکاتورهای فنی (TechnicalIndicatorData) ---
            # فیلتر بر اساس رشته جلالی
            tech_records = local_session.query(TechnicalIndicatorData).filter(
                TechnicalIndicatorData.symbol_id == symbol_id,
                TechnicalIndicatorData.jdate >= start_jdate_str 
            ).order_by(TechnicalIndicatorData.jdate.asc()).all()

            if hist_records:
                hist_df = pd.DataFrame([r.__dict__ for r in hist_records])
                # 💡 G-Fix: تبدیل jdate به ایندکس زمانی گرگوری
                hist_df['jdate'] = hist_df['jdate'].astype(str) # اطمینان از اینکه رشته است
                # 💡 فرض: تابع convert_jalali_to_gregorian_timestamp تعریف شده است
                hist_df['date'] = hist_df['jdate'].apply(convert_jalali_to_gregorian_timestamp)
                hist_df.set_index('date', inplace=True)
                hist_df = hist_df.drop(columns=['_sa_instance_state'], errors='ignore')
                
            if tech_records:
                tech_df = pd.DataFrame([r.__dict__ for r in tech_records])
                # 💡 G-Fix: تبدیل jdate به ایندکس زمانی گرگوری
                tech_df['jdate'] = tech_df['jdate'].astype(str) # اطمینان از اینکه رشته است
                # 💡 فرض: تابع convert_jalali_to_gregorian_timestamp تعریف شده است
                tech_df['date'] = tech_df['jdate'].apply(convert_jalali_to_gregorian_timestamp)
                tech_df.set_index('date', inplace=True)
                tech_df = tech_df.drop(columns=['_sa_instance_state'], errors='ignore')

            # 💡 Log پیشنهادی شما
            logger.debug(f"Symbol {symbol_name} ({symbol_id}) - hist_rows: {len(hist_df) if isinstance(hist_df, pd.DataFrame) else 0}, tech_rows: {len(tech_df) if isinstance(tech_df, pd.DataFrame) else 0}, start_jdate_str: {start_jdate_str}")
                
        except Exception as e:
            logger.error(f"Error fetching data for {symbol_name} ({symbol_id}): {e}")
            hist_df, tech_df = pd.DataFrame(), pd.DataFrame() # برگرداندن داده‌های خالی در صورت بروز خطا

        finally:
            # 🚨 بستن صریح نشست موقت (حل مشکل همزمانی/Locking)
            local_session.close() 

        return hist_df, tech_df

    def _analyze_symbol(self, symbol_data_rec):
        """
        آنالیز کامل یک نماد بر اساس داده‌های تاریخی و فنی.
        خروجی: (symbol_id, symbol_name, outlook, score, reason)
        """
        symbol_id = symbol_data_rec.symbol_id
        symbol_name = symbol_data_rec.symbol_name
        
        # 1. واکشی داده‌ها
        hist_df, tech_df = self._get_history_and_tech_data(symbol_id, symbol_name)

        # 💡 فرض: توابع is_data_sufficient و MIN_REQUIRED_HISTORY_DAYS تعریف شده‌اند
        if not is_data_sufficient(hist_df, MIN_REQUIRED_HISTORY_DAYS):
            logger.warning(f"Insufficient historical data for {symbol_name}.")
            return None
        
        # 💡 G-Check: بررسی کفایت داده‌های فنی برای اجرای تحلیل
        if not is_data_sufficient(tech_df, 2):
            logger.warning(f"Insufficient technical data for {symbol_name}.")
            return None
            
        # 2. تعیین سنتیمنت بازار
        # 💡 فرض: تابع _get_market_sentiment تعریف شده است
        market_sentiment = _get_market_sentiment() 
        
        # 3. اجرای تمام فیلترها
        
        # 3.1. Technical Filters
        # 💡 فرض: تابع _check_technical_filters تعریف شده است
        tech_filters, tech_reasons = _check_technical_filters(hist_df, tech_df, market_sentiment)
        
        # 3.2. Market/Volatility Filters
        # 💡 فرض: تابع _check_market_condition_filters تعریف شده است
        market_filters, market_reasons = _check_market_condition_filters(hist_df, tech_df)
        
        # 3.3. Smart Money Filters
        # 💡 فرض: تابع _check_smart_money_filters تعریف شده است
        money_filters, money_reasons = _check_smart_money_filters(hist_df)
        
        # 3.4. Power Thrust Filter
        # 💡 G-Fix: اطمینان از ارسال close_ser صحیح
        # 💡 فرض: تابع _get_close_series_from_hist_df و _check_power_thrust_signal تعریف شده‌اند
        close_ser = _get_close_series_from_hist_df(hist_df) 
        power_filters, power_reasons = _check_power_thrust_signal(hist_df, close_ser)
        
        # 3.5. Static Levels Filters
        last_tech = tech_df.iloc[-1]
        last_close = close_ser.iloc[-1]
        # 📢 تغییر: ارسال دیتافریم تاریخی (hist_df) برای محاسبه دینامیک حمایت
        # 💡 فرض: تابع _check_static_levels_filters تعریف شده است
        static_filters, static_reasons = _check_static_levels_filters(hist_df, last_tech, last_close)
        
        # 3.6. Sector Strength Filter
        # 💡 فرض: تابع _check_sector_strength_filter تعریف شده است
        sector_filters, sector_reasons = _check_sector_strength_filter(symbol_data_rec.group_name, self.leading_sectors)

        # 3.7. Simple Fundamental Filter
        # 💡 فرض: تابع _check_simple_fundamental_filters تعریف شده است
        fundamental_filters, fundamental_reasons = _check_simple_fundamental_filters(symbol_data_rec)

        # 3.8. ML Prediction Filter
        # 💡 فرض: تابع _check_ml_prediction_filter تعریف شده است
        ml_filters, ml_reasons = _check_ml_prediction_filter(symbol_id)
        
        # 4. تجمیع نتایج
        all_satisfied_filters = tech_filters + market_filters + money_filters + power_filters + static_filters + sector_filters + fundamental_filters + ml_filters
        
        all_reasons = {
            "technical": tech_reasons.get("technical", []),
            "market_condition": market_reasons.get("market_condition", []),
            "smart_money": money_reasons.get("smart_money", []),
            "power_thrust": power_reasons.get("power_thrust", []),
            "static_levels": static_reasons.get("static_levels", []),
            "sector_strength": sector_reasons.get("sector_strength", []),
            "fundamental": fundamental_reasons.get("fundamental", []),
            "ml_prediction": ml_reasons.get("ml_prediction", []),
        }

        # 5. محاسبه امتیاز نهایی (Score Calculation)
        # 💡 فرض: ثابت FILTER_WEIGHTS تعریف شده است
        total_score = sum(
            FILTER_WEIGHTS.get(f, {}).get("weight", 0) for f in all_satisfied_filters
        )
        
        # 6. تعیین چشم‌انداز (Outlook)
        if total_score >= 10 and len([f for f in all_satisfied_filters if FILTER_WEIGHTS.get(f, {}).get("weight", 0) > 0]) >= 4:
            outlook = "Strong Buy"
        elif total_score >= 5:
            outlook = "Buy"
        elif total_score >= 0:
            outlook = "Neutral"
        elif total_score >= -5:
            outlook = "Sell"
        else:
            outlook = "Strong Sell"
            
        # 7. ساخت رشته دلایل (Reason String Construction)
        positive_filters_reasons = []
        negative_filters_reasons = []
        
        for filter_name in all_satisfied_filters:
            weight = FILTER_WEIGHTS.get(filter_name, {}).get("weight", 0)
            description = FILTER_WEIGHTS.get(filter_name, {}).get("description", "بدون توضیحات")
            
            if weight > 0:
                positive_filters_reasons.append(f"✅ {filter_name} (+{weight}): {description}")
            elif weight < 0:
                negative_filters_reasons.append(f"❌ {filter_name} ({weight}): {description}")
            else:
                pass # weight 0 is ignored

        final_reasons_list = [
            f"امتیاز نهایی: {total_score}",
            f"چشم‌انداز بازار: {market_sentiment}",
            "--- سیگنال‌های مثبت ---",
        ] + positive_filters_reasons + [
            "--- سیگنال‌های منفی (جریمه‌ها) ---",
        ] + negative_filters_reasons + [
            "--- توضیحات فنی تکمیلی ---",
        ] + all_reasons["technical"] + all_reasons["market_condition"] + all_reasons["smart_money"] + all_reasons["power_thrust"] + all_reasons["static_levels"] + all_reasons["sector_strength"] + all_reasons["fundamental"] + all_reasons["ml_prediction"]
        
        # 💡 G-CleanUp: حذف دلایل تکراری و خالی
        final_reasons_list = [r for r in final_reasons_list if r not in ["--- سیگنال‌های منفی (جریمه‌ها) ---", "--- توضیحات فنی تکمیلی ---"]]
        final_reasons_list = list(dict.fromkeys(r for r in final_reasons_list if r.strip()))
        
        final_reason_string = "\n".join(final_reasons_list)

        return SimpleNamespace(
            symbol_id=symbol_id,
            symbol_name=symbol_name,
            outlook=outlook,
            score=total_score,
            reason=final_reason_string
        )


    def _save_result(self, result: SimpleNamespace):
        """
        ذخیره نتیجه آنالیز در جدول WeeklyWatchlistResult.
        """
        # 💡 FIX: تعریف متغیر today_gregorian_date برای رفع NameError
        today_gregorian_date = datetime.now().date() 
        # 💡 فرض: تابع get_today_jdate_str تعریف شده است
        today_jdate = get_today_jdate_str()
        
        try:
            # 💡 G-Check: جلوگیری از تکرار تحلیل در یک تاریخ
            
            # جستجوی رکورد موجود برای امروز
            existing_record = db.session.query(WeeklyWatchlistResult).filter(
                WeeklyWatchlistResult.symbol_id == result.symbol_id,
                # 💡 FIX: اصلاح jentry_date به jdate (فیلد موجود در مدل شما)
                WeeklyWatchlistResult.jentry_date == today_jdate, 
                WeeklyWatchlistResult.status == 'Open'
            ).first()

            if existing_record:
                # 💡 G-Update: به روز رسانی رکورد موجود
                existing_record.outlook = result.outlook
                existing_record.score = result.score
                existing_record.reason = result.reason
                existing_record.entry_price = result.entry_price
                existing_record.probability_percent = result.probability_percent
                existing_record.updated_at = datetime.utcnow()
                logger.info(f"Updated watchlist for {result.symbol_name} (Score: {result.score})")
            else:
                # 💡 G-Create: ایجاد رکورد جدید
                new_result = WeeklyWatchlistResult(
                    signal_unique_id=str(uuid.uuid4()),
                    symbol_id=result.symbol_id,
                    symbol_name=result.symbol_name,
                    outlook=result.outlook,
                    score=result.score,
                    reason=result.reason,
                    entry_price=result.entry_price,

                    # 💥 FIX: استفاده از متغیر تعریف شده و فیلد صحیح (jdate)
                    entry_date=today_gregorian_date, 
                    jentry_date=today_jdate, # <-- فیلد صحیح در مدل
                    status='Open', # تمام سیگنال‌های هفتگی باز تلقی می‌شوند
                    probability_percent=result.probability_percent
                )
                db.session.add(new_result)
                logger.info(f"Added new watchlist signal for {result.symbol_name} (Score: {result.score})")
                
            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error saving result for {result.symbol_name}: {e}")
            return False

    def _process_one_symbol(self, symbol_data_rec):
        """
        تابع بسته‌بندی برای پردازش یک نماد و مدیریت خطاها و لاگینگ.
        """
        # 💡 G-Fix: اضافه کردن قیمت ورود (Entry Price)
        # 💡 فرض: تابع _get_history_and_tech_data در بالا تعریف شده است
        hist_df, _ = self._get_history_and_tech_data(symbol_data_rec.symbol_id, symbol_data_rec.symbol_name)
        # 💡 فرض: تابع _get_close_series_from_hist_df تعریف شده است
        close_ser = _get_close_series_from_hist_df(hist_df)
        entry_price = close_ser.iloc[-1] if not close_ser.empty else None
        
        # اگر قیمت امروز وجود ندارد، پردازش نکن
        if entry_price is None:
            logger.warning(f"Skipping {symbol_data_rec.symbol_name}: Could not determine entry price.")
            return None

        analysis_result = self._analyze_symbol(symbol_data_rec)
        
        if analysis_result is None:
            return None

        # 💡 G-Update: به‌روزرسانی شیء نتیجه با قیمت ورود و احتمال
        analysis_result.entry_price = entry_price
        # 💡 G-Fix: اطمینان از وجود فیلد probability_percent در ML
        ml_prediction_reason = next((r for r in analysis_result.reason.split('\n') if "ML predicts uptrend" in r), None)
        if ml_prediction_reason:
            try:
                prob_str = ml_prediction_reason.split('(Prob: ')[1].split('%')[0]
                analysis_result.probability_percent = float(prob_str)
            except:
                analysis_result.probability_percent = None
        else:
            analysis_result.probability_percent = None

        if analysis_result.outlook in ["Strong Buy", "Buy"]:
            # self._save_result(analysis_result) <-- این خط حذف شد و به run_watchlist_generation منتقل شد
            return analysis_result
        
        return None

    # -----------------------------------------------------------------
    # --- تابع اصلی تولید واچ لیست (با اعمال فیلتر صندوق‌ها) ---
    # -----------------------------------------------------------------
    def run_watchlist_generation(self, parallel=True, max_workers=8):
        """
        فرایند اصلی تولید واچ لیست.
        این تابع نمادها را تحلیل کرده، نتایج را بر اساس امتیاز (Score) مرتب می‌کند،
        و فقط 8 سیگنال برتر را در جدول WeeklyWatchlistResult ذخیره می‌نماید.
        """
        logger.info("Starting Weekly Watchlist Generation...")
        start_time = time.time()
        
        # 💡 G-Fix: گرفتن کانتکست اپلیکیشن برای استفاده در پردازش موازی
        app = current_app._get_current_object()
        
        # --- Step 1: واکشی لیست نمادهای معتبر (با اعمال فیلتر صندوق‌ها) ---
        try:
            logger.info("Fetching valid symbols from ComprehensiveSymbolData table (excluding funds)...")
            
            # --- لیست کلمات کلیدی برای حذف صندوق‌ها و اوراق ---
            EXCLUDE_KEYWORDS = [
                'صندوق', 'سرمایه گذاری قابل معامله', 'اوراق', 'تسهیلات', 'اجاره', 
                'مشارکت', 'گواهی', 'کالا', 'انرژی', 'اختیار', 'آتی', 'سلف', 'بدهی', 
                'ریالی', 'ارزی', 'قابل انتقال'
            ]

            # ساخت شروط فیلترینگ (NOT LIKE)
            conditions = []
            for kw in EXCLUDE_KEYWORDS:
                # فیلتر بر اساس نام صنعت، گروه و نام شرکت
                conditions.append(~ComprehensiveSymbolData.industry.like(f'%{kw}%'))
                conditions.append(~ComprehensiveSymbolData.group_name.like(f'%{kw}%'))
                conditions.append(~ComprehensiveSymbolData.company_name.like(f'%{kw}%'))
            
            # اجرای کوئری با اعمال تمام شروط
            query = db.session.query(ComprehensiveSymbolData).filter(and_(*conditions))
            active_symbols = query.all()
            
            # 💡 G-Fix: اطمینان از وجود symbol_id و symbol_name (این فیلتر همچنان لازم است)
            active_symbols = [s for s in active_symbols if s.symbol_id and s.symbol_name]
            
            if not active_symbols:
                logger.warning("No VALID symbols found after filtering funds and non-stocks.")
                return [] 
                
            logger.info(f"Found {len(active_symbols)} valid symbols to analyze after filtering.")
        
        except Exception as e:
            logger.error(f"Error fetching and filtering symbols: {e}")
            return [] 

        # --- Step 2: پردازش موازی یا ترتیبی (فقط تولید نتیجه، بدون ذخیره) ---
        
        if parallel:
            logger.info(f"Starting parallel processing with {max_workers} workers.")
            
            # 💡 توجه: _process_one_symbol_with_context نباید دیگر _save_result را فراخوانی کند.
            results = Parallel(n_jobs=max_workers, backend='threading', verbose=0)(
                delayed(self._process_one_symbol_with_context)(symbol, app) for symbol in active_symbols
            )
            
        else:
            logger.info("Starting sequential processing.")
            results = []
            for symbol in active_symbols:
                # 💡 توجه: _process_one_symbol نباید دیگر _save_result را فراخوانی کند.
                results.append(self._process_one_symbol(symbol))

        # --- Step 3: جمع‌بندی، مرتب‌سازی و محدود کردن نتایج ---
        
        # حذف نتایج None (سیگنال‌های ضعیف)
        successful_results = [r for r in results if r is not None]
        
        logger.info(f"Found {len(successful_results)} successful signals before final filtering.")

        # مرتب‌سازی بر اساس امتیاز (Score) به صورت نزولی
        # 💡 G-Sort: از 'score' استفاده می‌کنیم، اگر وجود نداشت -1 فرض می‌شود
        successful_results.sort(key=lambda x: getattr(x, 'score', -1), reverse=True)
        
        # محدود کردن به 8 نماد برتر
        top_n = 8
        top_results_to_save = successful_results[:top_n]
        
        logger.info(f"Selecting and saving ONLY the Top {len(top_results_to_save)} signals based on Score.")

        # --- Step 4: ذخیره‌سازی فقط 8 نماد برتر ---
        # 💡 G-Save: ذخیره‌سازی در این مرحله انجام می‌شود
        for result in top_results_to_save:
            # 💡 توجه: فراخوانی _save_result در این مرحله برای Top 8
            self._save_result(result) 
            
        # --- Step 5: پایان فرایند ---
        logger.info(f"Watchlist Generation Completed.")
        logger.info(f"Total time elapsed: {time.time() - start_time:.2f} seconds.")
        
        return top_results_to_save


    def _process_one_symbol_with_context(self, symbol, app):
        """
        یک Wrapper برای اجرای _process_one_symbol در یک Application Context جدید.
        با اجرای db.session.remove() درون کانتکست فعال، خطای Runtime را حل می‌کند.
        """
        result = None
        
        # 💡 G-Fix: تضمین فعال بودن کانتکست در تمام مراحل دسترسی به دیتابیس
        with app.app_context():
            try:
                # 1. اجرای کار اصلی (دسترسی به دیتابیس در اینجا مجاز است)
                result = self._process_one_symbol(symbol)
            finally:
                # 2. 🚨 G-Fix: حذف صریح نشست دیتابیس باید قبل از خروج از بلوک with انجام شود.
                db.session.remove() 
        return result


    def get_latest_watchlist(self, limit=10, include_history=False):
        """
        بازگرداندن آخرین واچ لیست (سیگنال‌های 'Open' با بالاترین امتیاز).
        """
        logger.info(f"Fetching latest watchlist results (Limit: {limit}, Include History: {include_history})")
        
        # --- Step 1: تنظیم کوئری پایه و اعمال فیلتر (FIX: فیلتر قبل از Limit/Order) ---
        query = db.session.query(WeeklyWatchlistResult)
        
        # 💡 FIX: فیلتر کردن بر اساس وضعیت (Open) را قبل از مرتب سازی و محدودیت اعمال می کنیم.
        if not include_history:
            # تنها سیگنال‌های فعال (Open) را برمی‌گرداند.
            query = query.filter(WeeklyWatchlistResult.status == 'Open')

        # اعمال مرتب‌سازی و محدودیت
        query = query.order_by(
            WeeklyWatchlistResult.score.desc(),
            # 💡 FIX: اصلاح jentry_date به jdate (فیلد موجود در مدل شما)
            WeeklyWatchlistResult.jentry_date.desc()
        ).limit(limit)

        # --- Step 2: اجرای کوئری ---
        try:
            results = query.all()
        except Exception as e:
            logger.error(f"Error fetching latest watchlist: {e}")
            return []

        # --- Step 3: دریافت نام کامل شرکت‌ها ---
        symbol_ids = [r.symbol_id for r in results]
        company_name_map = {}
        if symbol_ids:
            company_name_records = db.session.query(
                ComprehensiveSymbolData.symbol_id,
                ComprehensiveSymbolData.company_name
            ).filter(ComprehensiveSymbolData.symbol_id.in_(symbol_ids)).all()
            
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
                # 💡 FIX: اصلاح jentry_date به jdate
                "jentry_date": r.jentry_date, 
                "exit_price": r.exit_price,
                "jexit_date": r.jexit_date,
                "profit_loss_percentage": r.profit_loss_percentage,
                "status": r.status,
                "probability_percent": getattr(r, "probability_percent", None),
                "score": getattr(r, "score", None)
            })

        # --- Step 5: مرتب‌سازی بر اساس امتیاز ---
        # این مرتب‌سازی دوباره برای اطمینان از خروجی مرتب و سازگار با خروجی API است.
        output_stocks.sort(key=lambda x: x.get('score') if x.get('score') is not None else -100, reverse=True)

        logger.info(f"Successfully retrieved {len(output_stocks)} watchlist results.")
        return output_stocks
