# -*- coding: utf-8 -*-
# services/weekly_watchlist_service.py
from extensions import db
from models import HistoricalData, ComprehensiveSymbolData, TechnicalIndicatorData, FundamentalData, WeeklyWatchlistResult, SignalsPerformance, AggregatedPerformance, GoldenKeyResult, CandlestickPatternDetection, MLPrediction, FinancialRatiosData
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
from models import DailySectorPerformance
from typing import List, Dict, Tuple, Optional # NEW: Added for professional type hinting
import time


# NEW: Import for market sentiment analysis
from services.index_data_fetcher import get_market_indices

# Import utility functions
from services.technical_analysis_utils import get_today_jdate_str, normalize_value, calculate_rsi, calculate_macd, calculate_sma, calculate_bollinger_bands, calculate_volume_ma, calculate_atr, calculate_smart_money_flow, check_candlestick_patterns, convert_gregorian_to_jalali, calculate_z_score

# Import analysis_service for aggregated performance calculation
from services import performance_service

# تنظیمات لاگینگ برای این ماژول
logger = logging.getLogger(__name__)

# IMPROVEMENT: Lookback period and minimum history days 
# adjusted for better indicator quality
TECHNICAL_DATA_LOOKBACK_DAYS = 120
MIN_REQUIRED_HISTORY_DAYS = 50

# REVISED: New filter weights dictionary with descriptions for clarity
FILTER_WEIGHTS = {
    # --- High-Impact Leading & Breakout Signals ---
    "Power_Thrust_Signal": {
        "weight": 5,
        "description": "سیگنال قدرت: ترکیبی از حجم معاملات انفجاری، ورود پول هوشمند سنگین در یک روز مثبت."
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
        "weight": 2,
        "description": "قیمت به باند پایین بولینگر برخورد کرده است که می‌تواند نشانه بازگشت کوتاه‌مدت قیمت باشد."
    },

    # --- Trend Confirmation & Strength Signals ---
    "IsInLeadingSector": {
        "weight": 4,
        "description": "سهم متعلق به یکی از صنایع پیشرو و مورد توجه بازار است که شانس موفقیت را افزایش می‌دهد."
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
        "description": "حجم معاملات در یک روز مثبت به طور معناداری افزایش یافته که نشان از حمایت قوی از رشد قیمت دارد."
    },
    "Price_Above_SMA50": {
        "weight": 1,
        "description": "قیمت بالاتر از میانگین متحرک ۵۰ روزه خود قرار دارد که نشانه روند صعودی میان‌مدت است."
    },

    # --- Fundamental Quality Filters ---
    "Reasonable_PE": {
        "weight": 1, 
        "description": "نسبت P/E سهم در محدوده منطقی قرار دارد و سهم از نظر بنیادی ارزنده است."
    },
    "Fundamental_PE_vs_Group": { # ✅ NEW
        "weight": 2, 
        "description": "نسبت P/E سهم از P/E میانگین گروه کوچکتر است که نشانه ارزندگی نسبی است."
    },
    "Reasonable_PS": {
        "weight": 1,
        "description": "نسبت P/S سهم در محدوده منطقی قرار دارد."
    },
    "Positive_EPS": {
        "weight": 0.5,
        "description": "سود به ازای هر سهم (EPS) شرکت مثبت است."
    },

    # --- Advanced Flow/Ratio Filters --- # ✅ NEW SECTION
    "Advanced_Strong_Real_Buyer_Ratio": {
        "weight": 3,
        "description": "نسبت قدرت خریدار حقیقی به فروشنده حقیقی در روز آخر بالا بوده است."
    },
    "Advanced_Volume_Surge_Ratio": {
        "weight": 2,
        "description": "حجم معاملات روز آخر نسبت به میانگین ۲۰ روزه جهش قابل توجهی داشته است."
    },
    
    # --- Candlestick Filters ---
    "Bullish_Engulfing_Detected": {
        "weight": 3,
        "description": "الگوی کندلی پوشاننده صعودی (Bullish Engulfing) مشاهده شده است."
    },
    "Hammer_Detected": {
        "weight": 3,
        "description": "الگوی کندلی چکش (Hammer) که یک الگوی بازگشتی صعودی است، مشاهده شده است."
    },
    "Morning_Star_Detected": {
        "weight": 4,
        "description": "الگوی کندلی ستاره صبحگاهی (Morning Star)، یک الگوی بازگشتی بسیار معتبر، مشاهده شده است."
    },
    
    # --- ML Prediction Filter ---
    "ML_Predicts_Uptrend": {
        "weight": 2,
        "description": "مدل یادگیری ماشین، احتمال بالایی برای یک روند صعودی پیش‌بینی کرده است."
    },

    # --- Penalties & Negative Scores (Crucial for avoiding peaks) ---
    "RSI_Is_Overbought": {
        "weight": -4,
        "description": "جریمه منفی: RSI در ناحیه اشباع خرید قرار دارد که ریسک اصلاح قیمت را افزایش می‌دهد."
    },
    "Price_Too_Stretched_From_SMA50": {
        "weight": -3,
        "description": "جریمه منفی: قیمت فاصله زیادی از میانگین متحرک ۵۰ روزه گرفته که احتمال بازگشت به میانگین را بالا می‌برد."
    },
    "Negative_Real_Money_Flow_Trend_10D": {
        "weight": -2,
        "description": "جریمه منفی: برآیند ورود پول هوشمند در ۱۰ روز گذشته منفی بوده است (خروج پول)."
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

        # Fetch data for these dates and types
        index_data = db.session.query(DailyIndexData).filter(
            DailyIndexData.jdate.in_(last_jdates),
            DailyIndexData.index_type.in_(['Total_Index', 'Equal_Weighted_Index'])
        ).all()

        if len(index_data) < 2:  # At least one per index
            logger.warning("Insufficient index data for both types, defaulting to Neutral")
            return "Neutral"

        # Group by index_type
        total_changes = [d.percent_change for d in index_data if d.index_type == 'Total_Index']
        equal_changes = [d.percent_change for d in index_data if d.index_type == 'Equal_Weighted_Index']

        if len(total_changes) < 1 or len(equal_changes) < 1:
            logger.warning("Missing data for one index type, defaulting to Neutral")
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
    except Exception as e:
        logger.error(f"Error fetching market sentiment from DB: {e}, defaulting to Neutral")
        return "Neutral"

# --- REVISED: Filter Functions ---
def _check_market_condition_filters(hist_df, tech_df):
    """
    Checks for individual stock conditions like overbought state or consolidation.
    """
    satisfied_filters, reason_parts = [], {"market_condition": []}
    if tech_df is None or tech_df.empty or hist_df is None or len(hist_df) < 50:
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

    # --- Check 2: Price is too far from its SMA50 (Penalize) ---
    if hasattr(last_tech, 'SMA_50') and last_tech.SMA_50 is not None and last_tech.SMA_50 > 0:
        stretch_percent = ((last_close - last_tech.SMA_50) / last_tech.SMA_50) * 100
        if stretch_percent > 20:
            satisfied_filters.append("Price_Too_Stretched_From_SMA50")
            reason_parts["market_condition"].append(
                f"Price is overextended ({stretch_percent:.1f}%) from SMA50."
            )

    # --- Check 3: Consolidation Pattern (Reward) ---
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
        return val.iloc[0] if not val.empty else default
    return val

# --- REFACTORED: Technical filters are broken down into smaller functions ---

def _check_oscillator_signals(technical_rec, prev_tech_rec, close_ser):
    """Checks oscillator-based signals like RSI and Stochastic."""
    satisfied_filters, reason_parts = [], {"technical": []}
    
    # RSI Positive Divergence
    current_rsi = _get_attr_safe(technical_rec, 'RSI')
    prev_rsi = _get_attr_safe(prev_tech_rec, 'RSI')
    if current_rsi is not None and prev_rsi is not None and len(close_ser) > 1:
        if current_rsi > prev_rsi and close_ser.iloc[-1] < close_ser.iloc[-2]:
            satisfied_filters.append("RSI_Positive_Divergence")
            reason_parts["technical"].append(f"Positive divergence on RSI ({current_rsi:.2f}).")

    # Stochastic Oscillator
    current_stoch_k = _get_attr_safe(technical_rec, 'Stochastic_K')
    current_stoch_d = _get_attr_safe(technical_rec, 'Stochastic_D')
    prev_stoch_k = _get_attr_safe(prev_tech_rec, 'Stochastic_K')
    prev_stoch_d = _get_attr_safe(prev_tech_rec, 'Stochastic_D')
    if all(x is not None for x in [current_stoch_k, current_stoch_d, prev_stoch_k, prev_stoch_d]):
        if current_stoch_k > current_stoch_d and prev_stoch_k <= prev_stoch_d and current_stoch_d < 25:
            satisfied_filters.append("Stochastic_Bullish_Cross_Oversold")
            reason_parts["technical"].append("Stochastic bullish cross in oversold area.")
            
    return satisfied_filters, reason_parts

def _check_trend_signals(technical_rec, prev_tech_rec, last_close_val):
    """Checks trend-following signals like MACD, SMA, HalfTrend, and Resistance breaks."""
    satisfied_filters, reason_parts = [], {"technical": []}

    # MACD Cross
    current_macd = _get_attr_safe(technical_rec, 'MACD')
    current_macd_signal = _get_attr_safe(technical_rec, 'MACD_Signal')
    prev_macd = _get_attr_safe(prev_tech_rec, 'MACD')
    prev_macd_signal = _get_attr_safe(prev_tech_rec, 'MACD_Signal')
    if all(x is not None for x in [current_macd, current_macd_signal, prev_macd, prev_macd_signal]):
        if current_macd > current_macd_signal and prev_macd <= prev_macd_signal:
            satisfied_filters.append("MACD_Bullish_Cross_Confirmed")

    # Price vs SMA50
    sma50 = _get_attr_safe(technical_rec, 'SMA_50')
    if sma50 is not None and last_close_val > sma50:
        satisfied_filters.append("Price_Above_SMA50")
        
    # HalfTrend
    current_halftrend = _get_attr_safe(technical_rec, 'halftrend_signal')
    prev_halftrend = _get_attr_safe(prev_tech_rec, 'halftrend_signal')
    if current_halftrend == 1 and prev_halftrend != 1:
        satisfied_filters.append("HalfTrend_Buy_Signal")
        
    # Dynamic Resistance Break
    resistance_broken = _get_attr_safe(technical_rec, 'resistance_broken')
    if resistance_broken:
        satisfied_filters.append("Resistance_Broken")
        res_level = _get_attr_safe(technical_rec, 'resistance_level_50d', 'N/A')
        reason_parts["technical"].append(f"Broke a key dynamic resistance level around {res_level}.")

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

def _check_volume_signals(hist_df, close_ser):
    """Checks for volume-based signals."""
    satisfied_filters, reason_parts = [], {"technical": []}
    
    if 'volume' in hist_df.columns and len(hist_df) >= 20 and len(close_ser) > 1:
        volume_z_score = calculate_z_score(pd.to_numeric(hist_df['volume'], errors='coerce').dropna().iloc[-20:])
        if volume_z_score is not None and volume_z_score > 1.5 and close_ser.iloc[-1] > close_ser.iloc[-2]:
            satisfied_filters.append("High_Volume_On_Up_Day")
            reason_parts["technical"].append(f"High volume (Z-Score: {volume_z_score:.2f}) on a positive day.")
            
    return satisfied_filters, reason_parts

def _check_technical_filters(hist_df, tech_df):
    """
    Checks all technical indicators by calling specialized sub-functions.
    (Coordinator function)
    """
    all_satisfied_filters, all_reason_parts = [], {"technical": []}
    if tech_df is None or tech_df.empty or len(tech_df) < 2:
        return all_satisfied_filters, all_reason_parts

    technical_rec = tech_df.iloc[-1]
    prev_tech_rec = tech_df.iloc[-2]
    
    close_ser = _get_close_series_from_hist_df(hist_df)
    last_close_val = close_ser.iloc[-1] if not close_ser.empty else None
    if last_close_val is None:
        return all_satisfied_filters, all_reason_parts

    # Call sub-functions and aggregate results
    for func in [_check_oscillator_signals, _check_trend_signals, _check_volatility_signals, _check_volume_signals]:
        # Adjust arguments as needed per function signature
        if func in [_check_oscillator_signals, _check_trend_signals]:
             satisfied, reasons = func(technical_rec, prev_tech_rec, close_ser if func == _check_oscillator_signals else last_close_val)
        elif func == _check_volatility_signals:
            # Note: _check_volatility_signals originally took hist_df, technical_rec, last_close_val,
            # but the new refactored argument list for technical filters is used here.
            # Assuming tech_df passed to _check_volatility_signals should be hist_df here for consistency.
            satisfied, reasons = func(hist_df, technical_rec, last_close_val)
        else: # _check_volume_signals
            satisfied, reasons = func(hist_df, close_ser)

        all_satisfied_filters.extend(satisfied)
        if "technical" in reasons:
            all_reason_parts["technical"].extend(reasons["technical"])

    return all_satisfied_filters, all_reason_parts

# --- END REFACTORED SECTION ---

# ✅ REPLACED: Updated _check_fundamental_filters for professional P/E checks
def _check_fundamental_filters(fundamental_rec):
    """
    بررسی معیارهای اصلی فاندامنتال (P/E، P/S، EPS) با رویکرد ارزش‌گذاری حرفه‌ای‌تر (مقایسه با گروه).
    """
    satisfied_filters = []
    reason_parts = {"fundamental": []}
    
    # تنظیمات آستانه
    MAX_PE_RATIO_ABSOLUTE = 15.0  # P/E مطلق
    MAX_PE_GROUP_RATIO = 1.25     # P/E نسبت به P/E گروه (حداکثر 1.25 برابر)
    MAX_PS_RATIO = 3.0            # P/S مطلق

    if fundamental_rec:
        # 1. بررسی P/E
        # استفاده از getattr برای انعطاف‌پذیری بین pe_ratio و pe
        pe = getattr(fundamental_rec, 'pe_ratio', getattr(fundamental_rec, 'pe', None))
        group_pe = getattr(fundamental_rec, 'group_pe_ratio', None)
        
        # فیلتر P/E مطلق
        if pe is not None and 0 < pe < MAX_PE_RATIO_ABSOLUTE:
            satisfied_filters.append("Reasonable_PE")
            reason_parts["fundamental"].append(f"P/E ({pe:.2f}) در محدوده منطقی است (حداکثر {MAX_PE_RATIO_ABSOLUTE}).")
        
        # فیلتر P/E نسبت به گروه
        if pe is not None and group_pe is not None and group_pe > 0:
            pe_vs_group = pe / group_pe
            if pe_vs_group < MAX_PE_GROUP_RATIO:
                satisfied_filters.append("Fundamental_PE_vs_Group")
                reason_parts["fundamental"].append(f"P/E سهم ({pe_vs_group:.2f}x) کمتر از {MAX_PE_GROUP_RATIO}x P/E گروه است.")

        # 2. بررسی P/S
        ps = getattr(fundamental_rec, 'p_s_ratio', getattr(fundamental_rec, 'psr', None))
        if ps is not None and 0 < ps < MAX_PS_RATIO:
            satisfied_filters.append("Reasonable_PS")
            reason_parts["fundamental"].append(f"P/S ({ps:.2f}) در محدوده منطقی است (حداکثر {MAX_PS_RATIO}).")

        # 3. بررسی EPS مثبت
        eps = getattr(fundamental_rec, 'eps', None)
        if eps is not None and eps > 0:
            satisfied_filters.append("Positive_EPS")
    
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
    
    # NEW Check 2: Heavy Individual Buy Pressure on the last day
    last_day = hist_df.iloc[-1]
    required_cols = ['buy_i_volume', 'buy_i_count', 'sell_i_volume', 'sell_i_count']
    if all(col in last_day and pd.notna(last_day[col]) for col in required_cols):
        if last_day['buy_i_count'] > 0 and last_day['sell_i_count'] > 0:
            per_capita_buy = last_day['buy_i_volume'] / last_day['buy_i_count']
            per_capita_sell = last_day['sell_i_volume'] / last_day['sell_i_count']
            
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
    
    # --- شرط ۱: روز معاملاتی مثبت ---
    is_up_day = close_ser.iloc[-1] > close_ser.iloc[-2]
    if not is_up_day:
        return satisfied_filters, reason_parts # اگر روز مثبت نیست، ادامه نده

    # --- شرط ۲: حجم معاملات بسیار بالا (استفاده از Z-Score) ---
    volume_series = pd.to_numeric(hist_df['volume'], errors='coerce').dropna().tail(20)
    volume_z_score = calculate_z_score(volume_series)
    # آستانه بالا برای حجم، مثلا Z-Score بیشتر از 1.8
    is_high_volume = volume_z_score is not None and volume_z_score > 1.8 
    
    # --- شرط ۳: فشار خرید سنگین حقیقی‌ها ---
    is_heavy_buy_pressure = False
    required_cols = ['buy_i_volume', 'buy_i_count', 'sell_i_volume', 'sell_i_count']
    if all(col in last_day and pd.notna(last_day[col]) for col in required_cols):
        if last_day['buy_i_count'] > 0 and last_day['sell_i_count'] > 0:
            per_capita_buy = last_day['buy_i_volume'] / last_day['buy_i_count']
            per_capita_sell = last_day['sell_i_volume'] / last_day['sell_i_count']
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


def _check_candlestick_filters(pattern_rec):
    satisfied_filters, reason_parts = [], {"candlestick": []}
    if not pattern_rec:
        return satisfied_filters, reason_parts

    pattern_name = pattern_rec.pattern_name
    if "Bullish Engulfing" in pattern_name:
        satisfied_filters.append("Bullish_Engulfing_Detected")
        reason_parts["candlestick"].append(f"Detected: {pattern_name}")
    if "Hammer" in pattern_name:
        satisfied_filters.append("Hammer_Detected")
        reason_parts["candlestick"].append(f"Detected: {pattern_name}")
    if "Morning Star" in pattern_name:
        satisfied_filters.append("Morning_Star_Detected")
        reason_parts["candlestick"].append(f"Detected: {pattern_name}")

    return satisfied_filters, reason_parts

# ✅ NEW FUNCTION (Replaces the old _check_advanced_fundamental_filters)
def _check_money_flow_and_advanced_ratios(hist_df: pd.DataFrame, tech_df: pd.DataFrame) -> Tuple[List[str], Dict[str, str]]:
    """
    بررسی فیلترهای جریان پول (قدرت خریدار حقیقی) و نسبت‌های حجمی (Volume Surge) که بر اساس داده‌های تاریخی محاسبه می‌شوند.
    این تابع جایگزین _check_advanced_fundamental_filters شده است.
    """
    satisfied_filters = []
    reasons = {"advanced_flow": []}

    if hist_df.empty or tech_df.empty:
        reasons["advanced_flow"].append("Missing historical or technical data for advanced flow analysis.")
        return satisfied_filters, reasons

    # آخرین رکورد را برای محاسبات می‌گیریم
    last_hist = hist_df.iloc[-1]
    last_tech = tech_df.iloc[-1]

    # تنظیمات آستانه
    MIN_REAL_POWER_RATIO = 1.3  # حداقل نسبت قدرت خریدار حقیقی به فروشنده حقیقی
    MIN_VOLUME_SURGE_RATIO = 1.5 # حداقل نسبت حجم روز به میانگین ۲۰ روزه

    # 1. محاسبه قدرت خریدار حقیقی
    required_flow_cols = ['buy_i_volume', 'buy_i_count', 'sell_i_volume', 'sell_i_count']
    real_power_ratio = np.nan

    if all(col in last_hist and pd.notna(last_hist[col]) for col in required_flow_cols):
        buy_i_count = last_hist.get('buy_i_count', 0)
        sell_i_count = last_hist.get('sell_i_count', 0)
        buy_i_volume = last_hist.get('buy_i_volume', 0)
        sell_i_volume = last_hist.get('sell_i_volume', 0)
        
        buy_power_real = buy_i_volume / buy_i_count if buy_i_count > 0 else 0
        sell_power_real = sell_i_volume / sell_i_count if sell_i_count > 0 else 0
        
        # در صورت تقسیم بر صفر، نسبت را بی‌نهایت در نظر می‌گیریم (مثبت) یا 1.0 (اگر هر دو صفر باشند)
        with np.errstate(divide='ignore', invalid='ignore'):
            real_power_ratio = buy_power_real / sell_power_real if sell_power_real > 0 else (np.inf if buy_power_real > 0 else 1.0)
    
    # 2. محاسبه جهش حجم
    volume_ma_20 = last_tech.get('Volume_MA_20')
    current_volume = last_hist.get('volume')
    volume_surge_ratio = np.nan
    if volume_ma_20 is not None and volume_ma_20 > 0 and current_volume is not None:
         volume_surge_ratio = current_volume / volume_ma_20

    # 🚨 اعمال فیلترها

    # 1. فیلتر قدرت خریدار
    if not pd.isna(real_power_ratio) and real_power_ratio >= MIN_REAL_POWER_RATIO:
        satisfied_filters.append("Advanced_Strong_Real_Buyer_Ratio")
        reasons["advanced_flow"].append(f"Real Power Ratio is {real_power_ratio:.2f} (Min: {MIN_REAL_POWER_RATIO}).")

    # 2. فیلتر جهش حجم
    if not pd.isna(volume_surge_ratio) and volume_surge_ratio >= MIN_VOLUME_SURGE_RATIO:
        satisfied_filters.append("Advanced_Volume_Surge_Ratio")
        reasons["advanced_flow"].append(f"Volume Surge is {volume_surge_ratio:.2f}x 20d MA (Min: {MIN_VOLUME_SURGE_RATIO}x).")
        
    return satisfied_filters, reasons

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
        return {"خودرو و ساخت قطعات"} # Fallback in case of error

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

    # --- اصلاح: رفع خطای Pandas ValueError (خط 526 در لاگ شما) ---
    # technical_rec می‌تواند None یا یک Pandas Series باشد.
    is_rec_valid = technical_rec is not None and ( 
        not isinstance(technical_rec, pd.Series) or not technical_rec.empty 
    )
    # last_close_val باید یک مقدار عددی باشد، پس بررسی None کافی است.
    if not is_rec_valid or last_close_val is None or last_close_val <= 0:
        return satisfied_filters, reason_parts

    # 1. Check for proximity to static support
    # (استفاده از _get_attr_safe تضمین می‌کند که مقدار عددی از Series یا شیء ORM استخراج شود)
    support_level = _get_attr_safe(technical_rec, 'static_support_level')

    # اگر سطح حمایت معتبر باشد و قیمت به آن نزدیک باشد
    if support_level and support_level > 0:
        # فاصله‌ی قیمت از سطح حمایت (مثبت برای بالای حمایت، منفی برای پایین)
        distance = last_close_val - support_level
        # میزان نوسان (درصد) نسبت به قیمت حمایت
        proximity_percent = abs(distance) / support_level 
        # اگر در محدوده ۲٪ حول سطح حمایت باشد (ریسک به ریوارد مناسب)
        if proximity_percent <= 0.02 and distance >= -0.005 * support_level: # نزدیک یا کمی بالاتر از سطح (حداکثر 0.5% زیر سطح)
            satisfied_filters.append("Near_Static_Support")
            reason_parts["static_levels"].append(
                f"Price is near a major static support level at {support_level:,.0f} (Proximity: {proximity_percent*100:.1f}%)."
            )

    # 2. Check for breakout of static resistance
    resistance_level = _get_attr_safe(technical_rec, 'static_resistance_level')

    # اگر سطح مقاومت معتبر باشد
    if resistance_level and resistance_level > 0:
        # قیمت بسته شدن بالاتر از سطح مقاومت باشد.
        # نکته: برای یک چک قوی باید قیمت روز قبل را هم چک کنیم که زیر مقاومت بوده باشد.
        # اما با فرض سادگی و موجود نبودن قیمت روز قبل: 
        if last_close_val > resistance_level and last_close_val < 1.03 * resistance_level: # شرط دوم: قیمت بیش از حد از مقاومت دور نشده باشد (شکست تازه و معتبر)
            satisfied_filters.append("Static_Resistance_Broken")
            reason_parts["static_levels"].append(
                f"Price broke a major static resistance level at {resistance_level:,.0f}."
            )

    return satisfied_filters, reason_parts
# --- END NEW STRATEGIC FILTERS --- 

def run_weekly_watchlist_selection():
    """
    انتخاب نمادها برای واچ‌لیست هفتگی با استفاده از داده‌های حجیم،
    امتیازدهی دینامیک و فیلترهای پیشرفته استراتژیک.
    """
    from models import (
        ComprehensiveSymbolData,
        HistoricalData,
        TechnicalIndicatorData,
        FundamentalData,
        CandlestickPatternDetection,
        MLPrediction,
        WeeklyWatchlistResult,
        FinancialRatiosData
    )
    from sqlalchemy import func
    import pandas as pd
    import jdatetime
    import uuid
    from datetime import datetime, timedelta, date
    import json
    import time  # ← اضافه: برای timing logs

    logger.info("🚀 Starting Weekly Watchlist selection process...")
    start_time = time.time()  # ← اضافه: Total timer

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
        return False, "No active symbols available for analysis."

    symbol_ids = [s.symbol_id for s in symbols_to_analyze]
    total_symbols = len(symbol_ids)  # ← اضافه: Counter برای تعداد واردشده
    logger.info(f"🧩 Found {total_symbols} active symbols for analysis.")  # ← Log تعداد entered

    fetch_start = time.time()  # ← اضافه: Fetch timer

    # Calculate lookback date
    today_greg = datetime.now().date()
    lookback_greg = today_greg - timedelta(days=TECHNICAL_DATA_LOOKBACK_DAYS * 2)
    lookback_jdate_str = convert_gregorian_to_jalali(lookback_greg)
    if not lookback_jdate_str:
        logger.error("❌ Could not determine lookback date. Aborting.")
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
    tech_df = pd.DataFrame([t.__dict__ for t in tech_records]).drop(columns=['_sa_instance_state'], errors='ignore')

    # Fetch Fundamental Data
    fundamental_records = FundamentalData.query.filter(
        FundamentalData.symbol_id.in_(symbol_ids)
    ).all()

    # Fetch Candlestick Patterns
    today_jdate = get_today_jdate_str()
    candlestick_records = CandlestickPatternDetection.query.filter(
        CandlestickPatternDetection.symbol_id.in_(symbol_ids),
        CandlestickPatternDetection.jdate == today_jdate
    ).all()

    # Fetch ML Predictions
    ml_predictions = MLPrediction.query.filter(
        MLPrediction.symbol_id.in_(symbol_ids),
        MLPrediction.jprediction_date == today_jdate,
        MLPrediction.predicted_trend == 'Uptrend'
    ).all()

    fetch_end = time.time()  # ← اضافه: End fetch timer
    logger.info(f"📥 Bulk fetch time: {fetch_end - fetch_start:.2f}s (hist: {len(hist_df)}, tech: {len(tech_df)} rows)")  # ← Log fetch time & rows

    # Grouping Data
    group_start = time.time()  # ← اضافه: Group timer
    hist_groups = {k: v.sort_values(by='jdate') for k, v in hist_df.groupby("symbol_id")} if not hist_df.empty else {}
    tech_groups = {k: v.sort_values(by='jdate') for k, v in tech_df.groupby("symbol_id")} if not tech_df.empty else {}
    fundamental_map = {rec.symbol_id: rec for rec in fundamental_records}
    candlestick_map = {rec.symbol_id: rec for rec in candlestick_records}
    ml_prediction_set = {rec.symbol_id for rec in ml_predictions}

    group_end = time.time()  # ← اضافه
    logger.info(f"📊 Data grouping completed: {group_end - group_start:.2f}s. Groups: hist={len(hist_groups)}, tech={len(tech_groups)}")  # ← Log group time & counts

    logger.info("📊 Data grouping completed. Beginning scoring and selection...")

    # Step 3: Scoring
    watchlist_candidates = []
    processed_count = 0  # ← اضافه: Counter برای processed (full analysis)
    skipped_count = 0    # ← اضافه: Counter برای skipped
    loop_start = time.time()  # ← اضافه: Loop timer
    
    for symbol in symbols_to_analyze:
        symbol_hist_df = hist_groups.get(symbol.symbol_id, pd.DataFrame()).copy()
        symbol_tech_df = tech_groups.get(symbol.symbol_id, pd.DataFrame()).copy()

        if len(symbol_hist_df) < MIN_REQUIRED_HISTORY_DAYS:
            skipped_count += 1  # ← اضافه
            logger.debug(f"Skipped {symbol.symbol_name} ({symbol.symbol_id}): insufficient history ({len(symbol_hist_df)} < {MIN_REQUIRED_HISTORY_DAYS})")  # ← Log skip (debug, low spam)
            continue

        last_close_series = _get_close_series_from_hist_df(symbol_hist_df)
        entry_price = float(last_close_series.iloc[-1]) if not last_close_series.empty else None
        if entry_price is None or pd.isna(entry_price):
            skipped_count += 1  # ← اضافه
            logger.warning(f"Skipping {symbol.symbol_name} ({symbol.symbol_id}) due to missing entry price.")  # موجود + symbol_id برای debug
            continue

        all_satisfied_filters = []
        all_reason_parts = {}

        def run_check(check_func, *args):
            filters, reasons = check_func(*args)
            all_satisfied_filters.extend(filters)
            all_reason_parts.update(reasons)

        technical_rec = symbol_tech_df.iloc[-1] if not symbol_tech_df.empty else None
        fundamental_rec = fundamental_map.get(symbol.symbol_id)
        pattern_rec = candlestick_map.get(symbol.symbol_id)

        # Run Filters
        run_check(_check_sector_strength_filter, getattr(symbol, 'sector_name', ''), leading_sectors)
        run_check(_check_technical_filters, symbol_hist_df, symbol_tech_df)
        run_check(_check_market_condition_filters, symbol_hist_df, symbol_tech_df)
        run_check(_check_static_levels_filters, technical_rec, entry_price)
        run_check(_check_fundamental_filters, fundamental_rec)
        run_check(_check_money_flow_and_advanced_ratios, symbol_hist_df, symbol_tech_df)
        run_check(_check_smart_money_filters, symbol_hist_df)
        run_check(_check_power_thrust_signal, symbol_hist_df, last_close_series)
        run_check(_check_candlestick_filters, pattern_rec)

        # ML Filter
        if symbol.symbol_id in ml_prediction_set:
            all_satisfied_filters.append("ML_Predicts_Uptrend")
            all_reason_parts.setdefault("ml_signal", []).append("ML model predicts a high-probability uptrend.")

        # Calculate Weighted Score
        score = sum(FILTER_WEIGHTS.get(f, {}).get('weight', 0) for f in all_satisfied_filters)

        if score >= score_threshold:
            watchlist_candidates.append({
                "symbol_id": symbol.symbol_id,
                "symbol_name": symbol.symbol_name,
                "entry_price": entry_price,
                "entry_date": date.today(),
                "jentry_date": today_jdate,
                "outlook": "Bullish",
                "reason_json": json.dumps(all_reason_parts, ensure_ascii=False),
                "satisfied_filters": json.dumps(list(set(all_satisfied_filters)), ensure_ascii=False),
                "score": score
            })

        processed_count += 1  # ← اضافه: هر symbol که به filters رسید (حتی اگر score < threshold) – full analysis count
        
        # Log progress every 100 symbols
        if processed_count % 100 == 0:
            avg_loop_time = (time.time() - loop_start) / processed_count
            logger.info(f"Progress: {processed_count}/{total_symbols} symbols analyzed (avg {avg_loop_time:.3f}s per symbol)")

    loop_end = time.time()  # ← اضافه
    logger.info(f"🔄 Loop completed: Processed {processed_count}/{total_symbols} symbols (skipped {skipped_count}). Loop time: {loop_end - loop_start:.2f}s")  # ← Log counts & time

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
            )
        db.session.add(existing_result)
        saved_count += 1

    try:
        db.session.commit()
        message = f"Weekly Watchlist selection completed. Saved {saved_count} symbols."
        logger.info(message)
        total_time = time.time() - start_time  # ← اضافه
        logger.info(f"⏱️ Full process time: {total_time:.2f}s")  # ← Log total time
        return True, message
    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ Database commit failed: {e}", exc_info=True)
        return False, "Database commit failed."



def get_weekly_watchlist_results(jdate_str: Optional[str] = None):
    """
    بازیابی آخرین نتایج واچ‌لیست هفتگی (یا تاریخ مشخص‌شده در صورت ارسال).
    خروجی شامل جزئیات نمادها و آخرین تاریخ به‌روزرسانی است.
    """

    logger.info("📊 Retrieving latest weekly watchlist results...")

    # --- Step 1: تعیین آخرین تاریخ ---
    if not jdate_str:
        latest_record = WeeklyWatchlistResult.query.order_by(WeeklyWatchlistResult.jentry_date.desc()).first()
        if not latest_record or not latest_record.jentry_date:
            logger.warning("⚠️ No weekly watchlist results found.")
            return {"top_watchlist_stocks": [], "last_updated": "نامشخص"}
        jdate_str = latest_record.jentry_date

    logger.info(f"🗓 Latest Weekly Watchlist results date: {jdate_str}")

    # --- Step 2: واکشی نتایج ---
    results = WeeklyWatchlistResult.query.filter_by(jentry_date=jdate_str)\
                                         .order_by(WeeklyWatchlistResult.created_at.desc()).all()

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
    output_stocks.sort(key=lambda x: (x.get("score") or 0), reverse=True)

    logger.info(f"✅ Retrieved and enriched {len(output_stocks)} weekly watchlist results for {jdate_str}.")

    # --- Step 6: بازگرداندن پاسخ نهایی ---
    return {
        "top_watchlist_stocks": output_stocks,
        "last_updated": jdate_str
    }
