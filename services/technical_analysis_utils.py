# -*- coding: utf-8 -*-
# analysis/technical_analysis_utils.py
# توابع خام محاسبه اندیکاتورهای تکنیکال، فاندامنتال و تشخیص الگوهای شمعی.
# این توابع هیچ وابستگی‌ای به دیتابیس ندارند و تنها با DataFrame/Series کار می‌کنند.

import pandas as pd
import numpy as np
import logging
import jdatetime
import datetime
from sqlalchemy import func
from functools import lru_cache
from typing import Union, List, Dict, Optional, Tuple, Any
import time 

# تنظیمات لاگینگ
logger = logging.getLogger(__name__)

# --- توابع عمومی و تبدیل تاریخ ---

def convert_gregorian_to_jalali(gregorian_date_obj: Union[datetime.date, datetime.datetime, Any]) -> Optional[str]:
    """
    تبدیل یک شیء datetime.date یا datetime.datetime به رشته تاریخ جلالی (YYYY-MM-DD).
    """
    try:
        if pd.isna(gregorian_date_obj):
            return None

        if isinstance(gregorian_date_obj, datetime.datetime):
            gregorian_dt = gregorian_date_obj
        elif isinstance(gregorian_date_obj, datetime.date):
            gregorian_dt = datetime.datetime(gregorian_date_obj.year, gregorian_date_obj.month, gregorian_date_obj.day)
        else:
            logger.warning(f"نوع ورودی نامعتبر برای تبدیل تاریخ: {type(gregorian_date_obj)}")
            return None

        jdate_obj = jdatetime.date.fromgregorian(
            year=gregorian_dt.year,
            month=gregorian_dt.month,
            day=gregorian_dt.day
        ).strftime('%Y-%m-%d')

        return jdate_obj
    except (ValueError, TypeError) as e:
        logger.error(f"خطا در تبدیل تاریخ میلادی به جلالی: {e} - ورودی: {gregorian_date_obj}")
        return None
    except Exception as e:
        logger.error(f"خطای ناشناخته در تبدیل تاریخ میلادی به جلالی: {e} - ورودی: {gregorian_date_obj}")
        return None

def get_today_jdate_str() -> str:
    """
    بازگرداندن تاریخ امروز به فرمت جلالی (شمسی) به صورت رشته YYYY-MM-DD.
    """
    return jdatetime.date.today().strftime('%Y-%m-%d')

def normalize_value(val: Any) -> Optional[Union[float, int]]:
    """
    نرمال‌سازی یک مقدار، با مدیریت لیست‌ها، Pandas Series و فرمت‌های رشته‌ای خاص
    برای استخراج یک مقدار عددی اسکالر.
    """
    if isinstance(val, (list, pd.Series)):
        return val.iloc[0] if len(val) > 0 else None
    elif isinstance(val, str):
        if 'Name:' in val:
            try:
                parts = val.split()
                for part in parts:
                    if part.replace('.', '', 1).isdigit():
                        return float(part)
            except ValueError:
                logger.warning(f"خطا در تبدیل رشته '{val}' به عدد.")
                return None
        try:
            return float(val)
        except ValueError:
            logger.warning(f"خطا در تبدیل رشته '{val}' به عدد.")
            return None
    return val

# --- تنظیمات API و تاخیر ---
DEFAULT_PER_SYMBOL_DELAY: float = 0.3 # تاخیر پیش‌فرض ۰.۳ ثانیه بین هر درخواست API
DEFAULT_REQUEST_TIMEOUT: int = 15 # Timeout پیش‌فرض برای درخواست‌های HTTP (ثانیه)

def safe_sleep(seconds: float, log_message: str = "") -> None:
    """
    تأخیر ایمن با قابلیت لاگ کردن.
    """
    if seconds > 0:
        message = f"در حال تاخیر به مدت {seconds:.2f} ثانیه..."
        if log_message:
            message += f" ({log_message})"
        logger.debug(message)
        time.sleep(seconds)

# -----------------------------------------------------------
# توابع کمکی فنی (Technical Indicator Calculation Utilities)
# -----------------------------------------------------------

def calculate_sma(series: pd.Series, period: int) -> pd.Series:
    """محاسبه میانگین متحرک ساده (SMA)."""
    return series.rolling(window=period, min_periods=1).mean()

def calculate_volume_ma(series: pd.Series, period: int) -> pd.Series:
    """محاسبه میانگین متحرک حجم."""
    return series.rolling(window=period, min_periods=1).mean()

def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """محاسبه RSI."""
    delta = series.diff()
    # محاسبه میانگین Gain/Loss به روش استاندارد RSI
    gain = (delta.where(delta > 0, 0)).rolling(window=period, min_periods=1).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=1).mean()

    # مدیریت تقسیم بر صفر
    with np.errstate(divide='ignore', invalid='ignore'):
        rs = gain / loss

    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """محاسبه MACD (خط MACD، خط سیگنال و هیستوگرام)."""
    ema_fast = series.ewm(span=fast, adjust=False, min_periods=1).mean()
    ema_slow = series.ewm(span=slow, adjust=False, min_periods=1).mean()
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(span=signal, adjust=False, min_periods=1).mean()
    macd_histogram = macd - macd_signal
    return macd, macd_signal, macd_histogram

def calculate_bollinger_bands(series: pd.Series, period: int = 20, std_dev: int = 2) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """محاسبه Bollinger Bands (بالایی، میانی و پایینی)."""
    middle = series.rolling(window=period, min_periods=1).mean()
    std = series.rolling(window=period, min_periods=1).std()
    upper = middle + (std * std_dev)
    lower = middle - (std * std_dev)
    return upper, middle, lower

def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """محاسبه ATR (Average True Range)."""
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    # ATR از TR به دست می‌آید، باید از EMA یا SMA برای میانگین استفاده کرد.
    atr = tr.ewm(span=period, adjust=False, min_periods=1).mean()
    return atr

def calculate_smart_money_flow(df: pd.DataFrame) -> pd.DataFrame:
    """
    محاسبه معیارهای جریان پول هوشمند از داده‌های تاریخی.
    Args:
        df (pd.DataFrame): DataFrame شامل ستون‌های 'buy_i_volume', 'sell_i_volume',
                         'buy_count_i', 'sell_count_i', 'value'.
    Returns:
        pd.DataFrame: DataFrameای حاوی معیارهای محاسبه شده.
    """
    required_cols = ['buy_i_volume', 'sell_i_volume', 'buy_count_i', 'sell_count_i', 'value']
    missing_columns = [col for col in required_cols if col not in df.columns]
    
    df_copy = df.copy()
    if missing_columns:
        logger.warning(f"ستون‌های مورد نیاز برای محاسبه جریان پول هوشمند یافت نشدند: {missing_columns}.")
        for col in missing_columns:
            df_copy[col] = np.nan
    
    for col in required_cols:
        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)

    df_copy['individual_buy_power'] = df_copy['buy_i_volume'] / df_copy['sell_i_volume'].replace(0, np.nan)
    df_copy['individual_buy_power'] = df_copy['individual_buy_power'].replace([np.inf, -np.inf], np.nan).fillna(0)

    df_copy['individual_net_flow'] = df_copy['buy_i_volume'] - df_copy['sell_i_volume']

    df_copy['individual_buy_per_trade'] = df_copy['buy_i_volume'] / df_copy['buy_count_i'].replace(0, np.nan)
    df_copy['individual_sell_per_trade'] = df_copy['sell_i_volume'] / df_copy['sell_count_i'].replace(0, np.nan)
    df_copy['individual_buy_per_trade'] = df_copy['individual_buy_per_trade'].replace([np.inf, -np.inf], np.nan).fillna(0)
    df_copy['individual_sell_per_trade'] = df_copy['individual_sell_per_trade'].replace([np.inf, -np.inf], np.nan).fillna(0)

    if 'jdate' in df_copy.columns:
        return df_copy[['jdate', 'individual_buy_power', 'individual_net_flow', 'individual_buy_per_trade', 'individual_sell_per_trade']].copy()
    else:
        return df_copy[['individual_buy_power', 'individual_net_flow', 'individual_buy_per_trade', 'individual_sell_per_trade']].copy()

def calculate_z_score(series: pd.Series) -> Optional[float]:
    """
    محاسبه Z-Score برای یک pandas Series.
    Args:
        series (pd.Series): سری داده‌های عددی.
    Returns:
        Optional[float]: مقدار Z-Score آخرین نقطه داده یا None.
    """
    series_cleaned = pd.to_numeric(series, errors='coerce').dropna()
    if series_cleaned.empty or len(series_cleaned) < 2:
        return None
    
    mean = series_cleaned.mean()
    std = series_cleaned.std()
    
    if std == 0:
        return 0.0
        
    z_score = (series_cleaned.iloc[-1] - mean) / std
    return float(z_score)

# --- توابع جدید ---

def calculate_stochastic(high: pd.Series, low: pd.Series, close: pd.Series, window: int = 14, smooth_k: int = 3, smooth_d: int = 3) -> Tuple[pd.Series, pd.Series]:
    """محاسبه Stochastic Oscillator (%K و %D)."""
    # اطمینان از اینکه داده‌ها برای محاسبه کافی هستند
    if len(close.dropna()) < window:
        nan_series = pd.Series([np.nan] * len(close), index=close.index)
        return nan_series, nan_series

    low_min = low.rolling(window=window).min()
    high_max = high.rolling(window=window).max()

    # محاسبه %K
    k = 100 * ((close - low_min) / (high_max - low_min).replace(0, np.nan))

    # هموارسازی %K برای %D
    d = k.rolling(window=smooth_k).mean()

    return k, d

def calculate_squeeze_momentum(df: pd.DataFrame, bb_window=20, bb_std=2, kc_window=20, kc_mult=1.5) -> Tuple[pd.Series, pd.Series]:
    """محاسبه Squeeze Momentum Indicator."""
    close = pd.to_numeric(df['close'].squeeze(), errors='coerce')
    high = pd.to_numeric(df['high'].squeeze(), errors='coerce')
    low = pd.to_numeric(df['low'].squeeze(), errors='coerce')

    # 1. Bollinger Bands
    bb_ma = calculate_sma(close, bb_window)
    bb_std_dev = close.rolling(window=bb_window).std()
    bb_upper = bb_ma + (bb_std_dev * bb_std)
    bb_lower = bb_ma - (bb_std_dev * bb_std)

    # 2. Keltner Channels
    atr = calculate_atr(high, low, close, kc_window)
    kc_ma = calculate_sma(close, kc_window)
    kc_upper = kc_ma + (atr * kc_mult)
    kc_lower = kc_ma - (atr * kc_mult)

    # 3. Squeeze condition (1: Squeeze ON, 0: Squeeze OFF)
    squeeze_on = (bb_lower > kc_lower) & (bb_upper < kc_upper)

    # 4. Momentum (بر اساس قیمت بسته‌شدن و میانگین بالاترین-پایین‌ترین)
    # TTM Momentum (ساده شده): از یک میانگین ساده‌شده از قیمت‌ها استفاده می‌کند.
    avg_price = (high + low + close) / 3
    momentum = avg_price - calculate_sma(avg_price, bb_window)

    return squeeze_on.astype(int).reindex(df.index), momentum.reindex(df.index)

def calculate_halftrend(df: pd.DataFrame, amplitude=2, channel_deviation=2) -> Tuple[pd.Series, pd.Series]:
    """محاسبه اندیکاتور HalfTrend (روند و سیگنال)."""
    try:
        # استفاده از نام‌های اصلی ستون‌ها
        high = pd.to_numeric(df['high'].squeeze(), errors='coerce')
        low = pd.to_numeric(df['low'].squeeze(), errors='coerce')
        close = pd.to_numeric(df['close'].squeeze(), errors='coerce')

        # محاسبه ATR با پریود پیش فرض 14 (از 100 در کد قبلی صرف نظر می‌کنیم تا استاندارد باشد)
        atr = calculate_atr(high, low, close, period=14)

        # محاسبه MA از High و Low در بازه Amplitude
        high_ma = high.rolling(window=amplitude).mean()
        low_ma = low.rolling(window=amplitude).mean()

        # آماده سازی ستون‌های موقت
        trend = np.zeros(len(df), dtype=int)
        next_trend = np.zeros(len(df), dtype=int)

        close_list = close.to_list()
        low_ma_list = low_ma.to_list()
        high_ma_list = high_ma.to_list()

        for i in range(1, len(df)):
            # مدیریت مقادیر NaN
            prev_low_ma = low_ma_list[i-1] if i > 0 and not pd.isna(low_ma_list[i-1]) else close_list[i-1] if i > 0 else close_list[i]
            prev_high_ma = high_ma_list[i-1] if i > 0 and not pd.isna(high_ma_list[i-1]) else close_list[i-1] if i > 0 else close_list[i]

            # منطق اصلی (بازسازی‌شده برای سادگی و بر اساس منطق اصلی HalfTrend)
            # 1 = صعودی (Buy/Long), -1 = نزولی (Sell/Short)

            if next_trend[i-1] == 1:
                if close_list[i] < prev_low_ma:
                    trend[i] = -1
                else:
                    trend[i] = 1
            else: # next_trend[i-1] == -1
                if close_list[i] > prev_high_ma:
                    trend[i] = 1
                else:
                    trend[i] = -1

            # تعیین روند بعدی
            if trend[i] == trend[i-1]:
                next_trend[i] = trend[i-1]
            else:
                next_trend[i] = trend[i]

        halftrend_trend = pd.Series(next_trend, index=df.index, dtype=int)

        # سیگنال: تغییر از -1 به 1 (خرید) یا بالعکس (فروش).
        halftrend_signal = (halftrend_trend != halftrend_trend.shift(1)).astype(int)

        return halftrend_trend, halftrend_signal

    except Exception as e:
        logger.error(f"خطای بحرانی در پردازش HalfTrend برای یک نماد: {e}", exc_info=True)
        nan_series = pd.Series([np.nan] * len(df), index=df.index)
        return nan_series, nan_series

def calculate_support_resistance_break(df: pd.DataFrame, window=50) -> Tuple[pd.Series, pd.Series]:
    """محاسبه ساده شکست مقاومت (مقاومت 50 روزه)."""
    close = pd.to_numeric(df['close'].squeeze(), errors='coerce')
    high = pd.to_numeric(df['high'].squeeze(), errors='coerce')

    # مقاومت: بالاترین High در N روز گذشته (به جز امروز)
    resistance = high.shift(1).rolling(window=window).max()

    # سیگنال شکست: قیمت پایانی امروز > مقاومت
    resistance_broken = close > resistance

    # 1: شکست رخ داده، 0: شکست رخ نداده
    resistance_broken_int = resistance_broken.astype(int)

    return resistance.astype(float).reindex(df.index), resistance_broken_int.reindex(df.index)

# -----------------------------------------------------------
# تابع اصلی تجمیع اندیکاتورها - نسخه بهبود یافته
# -----------------------------------------------------------
def calculate_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    محاسبه تمام اندیکاتورهای تکنیکال و فاندامنتال مورد نیاز و اضافه کردن آنها به DataFrame.
    """
    # اطمینان از اینکه دیتافریم خالی نیست و دارای ستون‌های ضروری است
    required_cols = {'open', 'high', 'low', 'close', 'volume'}
    if df.empty or not required_cols.issubset(df.columns):
        logger.warning("DataFrame خالی است یا ستون‌های لازم را ندارد.")
        return df

    try:
        # ایجاد کپی از DataFrame برای جلوگیری از SettingWithCopyWarning
        df_result = df.copy()
        
        # تبدیل ستون‌ها به نوع عددی و حذف مقادیر نامعتبر
        for col in required_cols:
            if not pd.api.types.is_numeric_dtype(df_result[col]):
                df_result[col] = pd.to_numeric(df_result[col], errors='coerce')
        
        # حذف ردیف‌هایی که مقادیر ضروری ندارند
        df_result.dropna(subset=list(required_cols), inplace=True)

        if df_result.empty:
            logger.warning("پس از تبدیل و پاکسازی، دیتای معتبری برای محاسبه اندیکاتورها باقی نماند.")
            return df

        # --- محاسبات اندیکاتورهای استاندارد ---
        df_result['RSI'] = calculate_rsi(df_result['close'])

        macd, signal, histogram = calculate_macd(df_result['close'])
        df_result['MACD'] = macd
        df_result['MACD_Signal'] = signal
        df_result['MACD_Histogram'] = histogram

        df_result['SMA_20'] = calculate_sma(df_result['close'], 20)
        df_result['SMA_50'] = calculate_sma(df_result['close'], 50)

        upper, middle, lower = calculate_bollinger_bands(df_result['close'])
        df_result['Bollinger_Upper'] = upper
        df_result['Bollinger_Middle'] = middle
        df_result['Bollinger_Lower'] = lower

        df_result['Volume_MA_20'] = calculate_volume_ma(df_result['volume'], 20)
        df_result['ATR'] = calculate_atr(df_result['high'], df_result['low'], df_result['close'])

        # --- محاسبات اندیکاتورهای پیشرفته ---
        stochastic_k, stochastic_d = calculate_stochastic(df_result['high'], df_result['low'], df_result['close'])
        df_result['Stochastic_K'] = stochastic_k
        df_result['Stochastic_D'] = stochastic_d

        # محاسبه Squeeze Momentum
        squeeze_on, squeeze_momentum = calculate_squeeze_momentum(df_result)
        df_result['squeeze_on'] = squeeze_on
        df_result['squeeze_momentum'] = squeeze_momentum

        # محاسبه HalfTrend
        halftrend_trend, halftrend_signal = calculate_halftrend(df_result)
        df_result['halftrend_trend'] = halftrend_trend
        df_result['halftrend_signal'] = halftrend_signal

        # محاسبه شکست مقاومت
        resistance_level, resistance_broken = calculate_support_resistance_break(df_result)
        df_result['resistance_level_50d'] = resistance_level
        df_result['resistance_broken'] = resistance_broken

        # --- محاسبات فاندامنتال و سنتیمنت ---
        df_result = calculate_fundamental_metrics(df_result)
        df_result = calculate_market_sentiment(df_result)
        df_result = detect_anomalies(df_result)

        logger.info("✅ محاسبه اندیکاتورها با موفقیت انجام شد")
        return df_result

    except Exception as e:
        logger.error(f"❌ خطای بحرانی در محاسبه اندیکاتورها: {e}", exc_info=True)
        # در صورت بروز خطای بحرانی، DataFrame اصلی را برمی‌گردانیم
        return df

# -----------------------------------------------------------
# توابع تشخیص الگوهای شمعی (Candlestick Pattern Detection) - نسخه حرفه‌ای
# -----------------------------------------------------------

def check_candlestick_patterns(today_record: dict, yesterday_record: dict, historical_df: pd.DataFrame) -> List[str]:
    """
    تشخیص الگوهای شمعی با پیاده‌سازی حرفه‌ای و منطق بازار سرمایه.
    """
    patterns = []
    
    # استخراج داده‌های امروز و دیروز
    close = float(today_record['close'])
    open_ = float(today_record['open'])
    high = float(today_record['high'])
    low = float(today_record['low'])
    volume = float(today_record['volume'])
    
    prev_close = float(yesterday_record['close'])
    prev_open = float(yesterday_record['open'])
    prev_high = float(yesterday_record['high'])
    prev_low = float(yesterday_record['low'])
    
    # محاسبات پایه
    body = abs(close - open_)
    total_range = high - low
    lower_shadow = min(open_, close) - low
    upper_shadow = high - max(open_, close)
    
    # میانگین حجم 20 روزه برای فیلتر نویز
    volume_ma_20 = historical_df['volume'].tail(20).mean() if len(historical_df) >= 20 else volume
    
    # 🔧 فیلتر اولیه: حذف سهم‌های با حجم بسیار پایین (نویز)
    if volume < volume_ma_20 * 0.3:
        return patterns  # حجم بسیار کم - احتمال نویز بالا
    
    # 1. **دوجی (Doji) - تعریف دقیق‌تر**
    if total_range > 0:
        body_to_range_ratio = body / total_range
        
        # دوجی واقعی: بدنه بسیار کوچک (کمتر از 5% از کل range)
        if body_to_range_ratio < 0.05:
            # فیلتر حجم: دوجی با حجم بالا معتبرتر است
            if volume >= volume_ma_20 * 0.7:
                # تشخیص نوع دوجی
                if upper_shadow > lower_shadow * 2:
                    patterns.append("Gravestone_Doji (دوجی سنگ قبر)")
                elif lower_shadow > upper_shadow * 2:
                    patterns.append("Dragonfly_Doji (دوجی سنجاقک)")
                else:
                    patterns.append("Doji (دوجی)")
    
    # 2. **چکش (Hammer) و مرد آویزان (Hanging Man)**
    if total_range > 0 and body > 0:
        lower_shadow_ratio = lower_shadow / total_range
        upper_shadow_ratio = upper_shadow / total_range
        body_ratio = body / total_range
        
        # چکش: سایه پایینی حداقل 2 برابر بدنه، سایه بالایی کوچک
        is_hammer_shape = (lower_shadow_ratio >= 0.6 and 
                          upper_shadow_ratio <= 0.1 and 
                          body_ratio <= 0.3)
        
        if is_hammer_shape:
            # تشخیص روند برای تمایز چکش و مرد آویزان
            if len(historical_df) >= 5:
                short_trend = calculate_trend(historical_df.tail(5))
                if short_trend < -0.1:  # روند نزولی
                    patterns.append("Hammer (چکش)")
                elif short_trend > 0.1:  # روند صعودی  
                    patterns.append("Hanging_Man (مرد آویزان)")
    
    # 3. **پوشای صعودی (Bullish Engulfing) - منطق دقیق‌تر**
    if (prev_close < prev_open and  # روز قبل نزولی
        close > open_ and           # امروز صعودی
        open_ < prev_close and      # باز شدن امروز پایین‌تر از بسته شدن دیروز
        close > prev_open):         # بسته شدن امروز بالاتر از باز شدن دیروز
        
        # فیلتر اندازه: بدنه امروز باید حداقل 1.5 برابر بدنه دیروز باشد
        today_body = close - open_
        yesterday_body = prev_open - prev_close  # منفی چون نزولی است
        if today_body > abs(yesterday_body) * 1.5:
            patterns.append("Bullish_Engulfing (پوشای صعودی)")
    
    # 4. **پوشای نزولی (Bearish Engulfing)**
    if (prev_close > prev_open and  # روز قبل صعودی
        close < open_ and           # امروز نزولی
        open_ > prev_close and      # باز شدن امروز بالاتر از بسته شدن دیروز  
        close < prev_open):         # بسته شدن امروز پایین‌تر از باز شدن دیروز
        
        today_body = open_ - close
        yesterday_body = prev_close - prev_open
        if today_body > yesterday_body * 1.5:
            patterns.append("Bearish_Engulfing (پوشای نزولی)")
    
    # 5. **ستاره صبحگاهی (Morning Star) - منطق دقیق‌تر**
    if len(historical_df) >= 3:
        day3 = historical_df.iloc[-1]  # امروز
        day2 = historical_df.iloc[-2]  # دیروز  
        day1 = historical_df.iloc[-3]  # پریروز
        
        day1_close = float(day1['close'])
        day1_open = float(day1['open'])
        day2_close = float(day2['close']) 
        day2_open = float(day2['open'])
        day3_close = float(day3['close'])
        day3_open = float(day3['open'])
        
        # 🔧 اصلاح: تعریف day3_range
        day1_range = float(day1['high']) - float(day1['low'])
        day2_range = float(day2['high']) - float(day2['low'])
        day3_range = float(day3['high']) - float(day3['low'])  # این خط اضافه شد
        
        # روز اول: شمع بزرگ نزولی
        day1_body = abs(day1_close - day1_open)
        is_day1_bearish = (day1_close < day1_open and 
                          day1_body > day1_range * 0.6)
        
        # روز دوم: شمع کوچک با گپ نزولی
        day2_body = abs(day2_close - day2_open)
        is_day2_small = (day2_body < day2_range * 0.3)
        has_gap_down = (day2_open < day1_close)  # گپ نزولی
        
        # روز سوم: شمع بزرگ صعودی که وارد بدنه روز اول شود
        day3_body = day3_close - day3_open  # مثبت چون صعودی
        is_day3_bullish = (day3_close > day3_open and 
                          day3_body > day3_range * 0.6)
        penetrates_day1 = (day3_close > (day1_open + day1_close) / 2)
        
        if is_day1_bearish and is_day2_small and has_gap_down and is_day3_bullish and penetrates_day1:
            patterns.append("Morning_Star (ستاره صبحگاهی)")
    
    # 6. **ستاره عصرگاهی (Evening Star)**
    if len(historical_df) >= 3:
        day3 = historical_df.iloc[-1]  # امروز
        day2 = historical_df.iloc[-2]  # دیروز
        day1 = historical_df.iloc[-3]  # پریروز
        
        day1_close = float(day1['close'])
        day1_open = float(day1['open'])
        day2_close = float(day2['close'])
        day2_open = float(day2['open']) 
        day3_close = float(day3['close'])
        day3_open = float(day3['open'])
        
        # 🔧 اصلاح: تعریف day3_range
        day1_range = float(day1['high']) - float(day1['low'])
        day2_range = float(day2['high']) - float(day2['low'])
        day3_range = float(day3['high']) - float(day3['low'])  # این خط اضافه شد
        
        # روز اول: شمع بزرگ صعودی
        day1_body = day1_close - day1_open
        is_day1_bullish = (day1_close > day1_open and 
                          day1_body > day1_range * 0.6)
        
        # روز دوم: شمع کوچک با گپ صعودی  
        day2_body = abs(day2_close - day2_open)
        is_day2_small = (day2_body < day2_range * 0.3)
        has_gap_up = (day2_open > day1_close)  # گپ صعودی
        
        # روز سوم: شمع بزرگ نزولی که وارد بدنه روز اول شود
        day3_body = day3_open - day3_close  # مثبت چون نزولی
        is_day3_bearish = (day3_close < day3_open and 
                          day3_body > day3_range * 0.6)
        penetrates_day1 = (day3_close < (day1_open + day1_close) / 2)
        
        if is_day1_bullish and is_day2_small and has_gap_up and is_day3_bearish and penetrates_day1:
            patterns.append("Evening_Star (ستاره عصرگاهی)")
    
    # 7. **شوتینگ استار (Shooting Star)**
    if total_range > 0 and close < open_:  # شمع نزولی
        upper_shadow_ratio = upper_shadow / total_range
        body_ratio = body / total_range
        
        if (upper_shadow_ratio >= 0.6 and  # سایه بالایی بلند
            body_ratio <= 0.3 and          # بدنه کوچک
            lower_shadow < body):          # سایه پایینی کوتاه
            
            patterns.append("Shooting_Star (شوتینگ استار)")

    # 8. **هارامی (Harami) - الگوی بازگشتی کوچک**
    if (abs(prev_close - prev_open) > 0 and  # کندل قبلی بدنه بزرگی دارد
        body < abs(prev_close - prev_open) * 0.5 and  # کندل امروز داخل بدنه دیروز است
        min(open_, close) > min(prev_open, prev_close) and
        max(open_, close) < max(prev_open, prev_close)):
        
        if prev_close < prev_open and close > open_:  # هارامی صعودی
            patterns.append("Bullish_Harami (هارامی صعودی)")
        elif prev_close > prev_open and close < open_:  # هارامی نزولی
            patterns.append("Bearish_Harami (هارامی نزولی)")
    
    # 9. **پیرسینگ لاین (Piercing Line) و دارک کلود کاور (Dark Cloud Cover)**
    if prev_close < prev_open:  # روز قبل نزولی
        today_body = close - open_
        yesterday_body = prev_open - prev_close
        penetration = (close - prev_close) / (prev_open - prev_close)
        if penetration > 0.5:  # کندل امروز حداقل 50% کندل دیروز را پوشش دهد
            patterns.append("Piercing_Line (پیرسینگ لاین)")
    
    elif prev_close > prev_open:  # روز قبل صعودی
        today_body = open_ - close
        yesterday_body = prev_close - prev_open
        penetration = (prev_close - close) / (prev_close - prev_open)
        if penetration > 0.5:  # کندل امروز حداقل 50% کندل دیروز را پوشش دهد
            patterns.append("Dark_Cloud_Cover (دارک کلود کاور)")
    
    return patterns

def calculate_trend(df: pd.DataFrame, period: int = 5) -> float:
    """
    محاسبه روند قیمتی برای تشخیص جهت بازار.
    """
    if len(df) < period:
        return 0.0
    
    recent_data = df.tail(period)
    start_price = float(recent_data.iloc[0]['close'])
    end_price = float(recent_data.iloc[-1]['close'])
    
    if start_price > 0:
        return (end_price - start_price) / start_price
    return 0.0

def is_valid_pattern(patterns: List[str], volume: float, avg_volume: float) -> bool:
    """
    اعتبارسنجی الگوهای تشخیص داده شده بر اساس فیلترهای حجم و نویز.
    """
    if not patterns:
        return False
    
    # فیلتر حجم: الگو با حجم پایین اعتبار کمتری دارد
    volume_ratio = volume / avg_volume if avg_volume > 0 else 1.0
    
    # برای الگوهای مهم، حجم باید بالاتر از میانگین باشد
    important_patterns = ['Bullish_Engulfing', 'Bearish_Engulfing', 'Morning_Star', 'Evening_Star']
    
    for pattern in patterns:
        if pattern.split(' ')[0] in important_patterns and volume_ratio < 0.8:
            return False
    
    return True

# -----------------------------------------------------------
# تابع Placeholder برای تحلیل فاندامنتال
# -----------------------------------------------------------

def calculate_fundamental_metrics(df: pd.DataFrame, fundamental_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    محاسبه شاخص‌های فاندامنتال پیشرفته با استفاده از داده‌های موجود.
    """
    df_copy = df.copy()
    
    try:
        # 1. نسبت‌های قدرت بازارگردانی
        if all(col in df_copy.columns for col in ['buy_i_volume', 'sell_i_volume', 'buy_count_i', 'sell_count_i']):
            # میانگین حجم هر معامله حقیقی
            df_copy['Avg_Buy_Volume_Per_Trade'] = df_copy['buy_i_volume'] / df_copy['buy_count_i'].replace(0, np.nan)
            df_copy['Avg_Sell_Volume_Per_Trade'] = df_copy['sell_i_volume'] / df_copy['sell_count_i'].replace(0, np.nan)
            
            # نسبت قدرت خریدار به فروشنده
            df_copy['Buy_Sell_Power_Ratio'] = (df_copy['buy_i_volume'] / df_copy['sell_i_volume'].replace(0, np.nan)).fillna(1)
            
            # شاخص تمرکز خرید/فروش
            df_copy['Trade_Concentration'] = (df_copy['buy_i_volume'] + df_copy['sell_i_volume']) / df_copy['volume'].replace(0, np.nan)
        
        # 2. شاخص‌های نقدشوندگی پیشرفته
        if 'volume' in df_copy.columns and 'final' in df_copy.columns:
            # ارزش معاملات روزانه
            df_copy['Daily_Trade_Value'] = df_copy['volume'] * df_copy['final']
            
            # نوسان قیمتی روزانه
            df_copy['Daily_Price_Range'] = (df_copy['high'] - df_copy['low']) / df_copy['final'].replace(0, np.nan)
            
            # شاخص نقدشوندگی (بر اساس ارزش و حجم)
            df_copy['Liquidity_Score'] = (df_copy['Daily_Trade_Value'] * df_copy['volume']) / (df_copy['high'] - df_copy['low']).replace(0, np.nan)
        
        # 3. شاخص‌های مومنتوم فاندامنتال
        if 'close' in df_copy.columns:
            # بازده روزانه
            df_copy['Daily_Return'] = df_copy['close'].pct_change()
            
            # نوسان بازده (20 روزه)
            df_copy['Return_Volatility_20d'] = df_copy['Daily_Return'].rolling(window=20).std()
            
            # شاخص قدرت نسبی (RSI) مبتنی بر بازده
            df_copy['Return_RSI'] = calculate_rsi(df_copy['close'])
        
        # 4. ادغام داده‌های فاندامنتال خارجی اگر موجود باشد
        if fundamental_df is not None:
            # این بخش می‌تواند داده‌های EPS, P/E, P/B را ادغام کند
            # فعلاً به عنوان placeholder باقی می‌ماند
            pass
            
        # 5. شاخص سلامت معاملاتی
        if all(col in df_copy.columns for col in ['num_trades', 'volume']):
            # میانگین حجم هر معامله
            df_copy['Avg_Volume_Per_Trade'] = df_copy['volume'] / df_copy['num_trades'].replace(0, np.nan)
            
            # شاخص فعالیت معاملاتی
            df_copy['Trade_Activity_Index'] = df_copy['num_trades'] / df_copy['num_trades'].rolling(window=20).mean()
        
        # 6. Z-Score برای شناسایی outliers
        if 'volume' in df_copy.columns:
            df_copy['Volume_Z_Score'] = calculate_z_score(df_copy['volume'])
        
        # 7. شاخص فشار خرید/فروش
        if all(col in df_copy.columns for col in ['plc', 'volume']):
            # فشار قیمت-حجم
            df_copy['Price_Volume_Pressure'] = df_copy['plc'] * df_copy['volume'] / df_copy['volume'].rolling(window=20).mean()
        
    except Exception as e:
        logger.error(f"خطا در محاسبه شاخص‌های فاندامنتال: {e}")
    
    return df_copy

# -----------------------------------------------------------
# توابع کمکی برای تحلیل پیشرفته
# -----------------------------------------------------------

def calculate_market_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    """
    محاسبه شاخص‌های سنتیمنت بازار.
    """
    df_copy = df.copy()
    
    # 1. شاخص قدرت خریداران حقیقی
    if all(col in df_copy.columns for col in ['buy_i_volume', 'sell_i_volume']):
        df_copy['Individual_Net_Flow'] = df_copy['buy_i_volume'] - df_copy['sell_i_volume']
        df_copy['Individual_Net_Flow_MA'] = df_copy['Individual_Net_Flow'].rolling(window=5).mean()
    
    # 2. شاخص تمرکز معاملات
    if all(col in df_copy.columns for col in ['buy_i_volume', 'buy_n_volume', 'volume']):
        df_copy['Individual_Dominance'] = (df_copy['buy_i_volume'] + df_copy['sell_i_volume']) / df_copy['volume'].replace(0, np.nan)
    
    # 3. شاخص مومنتوم جمعی
    if 'close' in df_copy.columns:
        df_copy['Price_Momentum'] = df_copy['close'] / df_copy['close'].shift(5) - 1
        df_copy['Volume_Momentum'] = df_copy['volume'] / df_copy['volume'].shift(5) - 1
    
    return df_copy

def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    شناسایی anomalies در داده‌های معاملاتی.
    """
    df_copy = df.copy()
    
    # 1. آنومالی حجم
    if 'volume' in df_copy.columns:
        volume_ma = df_copy['volume'].rolling(window=20).mean()
        volume_std = df_copy['volume'].rolling(window=20).std()
        df_copy['Volume_Anomaly'] = (df_copy['volume'] > (volume_ma + 2 * volume_std)).astype(int)
    
    # 2. آنومالی قیمت
    if 'close' in df_copy.columns:
        returns = df_copy['close'].pct_change()
        returns_std = returns.rolling(window=20).std()
        df_copy['Price_Anomaly'] = (abs(returns) > (3 * returns_std)).astype(int)
    
    # 3. آنومالی ارزش معاملات
    if all(col in df_copy.columns for col in ['volume', 'final']):
        trade_value = df_copy['volume'] * df_copy['final']
        trade_value_ma = trade_value.rolling(window=20).mean()
        trade_value_std = trade_value.rolling(window=20).std()
        df_copy['Trade_Value_Anomaly'] = (trade_value > (trade_value_ma + 2 * trade_value_std)).astype(int)
    
    return df_copy

# لیست توابعی که برای import شدن در سرویس اصلی مجاز هستند
__all__ = [
    'calculate_all_indicators',
    'check_candlestick_patterns',
    'calculate_fundamental_metrics',
    'calculate_market_sentiment',
    'detect_anomalies',
    'calculate_sma',
    'calculate_rsi',
    'calculate_macd',
    'calculate_bollinger_bands',
    'calculate_atr',
    'calculate_stochastic',
    'calculate_z_score'
]
