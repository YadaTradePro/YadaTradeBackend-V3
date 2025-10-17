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





# --- توابع اضافه شده برای سرویس Weekly Watchlist ---

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

def check_candlestick_patterns(today_candle_data: Dict[str, Union[float, int]], 
                              yesterday_candle_data: Dict[str, Union[float, int]], 
                              historical_data: pd.DataFrame) -> List[str]:
    """
    بررسی الگوهای شمعی رایج و پیشرفته با تأیید حجم.
    Args:
        today_candle_data (dict): دیکشنری با 'open', 'high', 'low', 'close', 'volume' برای امروز.
        yesterday_candle_data (dict): دیکشنری با 'open', 'high', 'low', 'close', 'volume' برای دیروز.
        historical_data (pd.DataFrame): DataFrame کامل داده‌های تاریخی شامل 'close_price' و 'volume'.
    Returns:
        List[str]: لیستی از نام الگوهای شمعی شناسایی شده.
    """
    detected_patterns = []

    if not all(k in today_candle_data and k in yesterday_candle_data for k in ['open', 'high', 'low', 'close']):
        logger.warning("داده‌های شمعی ناقص برای بررسی الگوهای شمعی.")
        return detected_patterns

    is_in_downtrend = False
    if 'close' in historical_data.columns and len(historical_data) >= 10:
        recent_closes = historical_data['close'].iloc[-10:]
        if not recent_closes.empty and recent_closes.iloc[0] > recent_closes.iloc[-1]:
            is_in_downtrend = True
            
    is_in_uptrend = False
    if 'close' in historical_data.columns and len(historical_data) >= 10:
        recent_closes = historical_data['close'].iloc[-10:]
        if not recent_closes.empty and recent_closes.iloc[0] < recent_closes.iloc[-1]:
            is_in_uptrend = True
            
    volume_t = today_candle_data.get('volume', 0)
    avg_volume = historical_data['volume'].iloc[-20:].mean() if 'volume' in historical_data.columns else 0
    is_high_volume = volume_t > 1.5 * avg_volume if avg_volume > 0 else False
    
    open_t, high_t, low_t, close_t = today_candle_data['open'], today_candle_data['high'], today_candle_data['low'], today_candle_data['close']
    open_y, high_y, low_y, close_y = yesterday_candle_data['open'], yesterday_candle_data['high'], yesterday_candle_data['low'], yesterday_candle_data['close']

    # --- الگوی Hammer ---
    body_t = abs(close_t - open_t)
    range_t = high_t - low_t
    lower_shadow_t = min(open_t, close_t) - low_t
    upper_shadow_t = high_t - max(open_t, close_t)
    if (range_t > 0 and body_t > 0 and body_t < (0.3 * range_t) and 
        lower_shadow_t >= 2 * body_t and upper_shadow_t < 0.1 * body_t and 
        is_in_downtrend):
        detected_patterns.append("Hammer")

    # --- الگوی Bullish Engulfing ---
    if (close_y < open_y and close_t > open_t and open_t < close_y and close_t > open_y and 
        is_in_downtrend and is_high_volume):
        detected_patterns.append("Bullish Engulfing (با تأیید حجم)")

    # --- الگوی Morning Star (نیاز به داده سه روزه) ---
    if len(historical_data) >= 3:
        day1 = historical_data.iloc[-3]
        day2 = historical_data.iloc[-2]
        day3 = historical_data.iloc[-1]
        
        if day1['close'] < day1['open']:
            if abs(day2['close'] - day2['open']) < (day2['high'] - day2['low']) * 0.2:
                if (day3['close'] > day3['open'] and
                    day3['close'] > (day1['open'] + day1['close']) / 2):
                    if is_in_downtrend:
                        detected_patterns.append("Morning Star")
                        
    # --- الگوی Evening Star (نیاز به داده سه روزه) ---
    if len(historical_data) >= 3:
        day1 = historical_data.iloc[-3]
        day2 = historical_data.iloc[-2]
        day3 = historical_data.iloc[-1]
        
        if day1['close'] > day1['open']:
            if abs(day2['close'] - day2['open']) < (day2['high'] - day2['low']) * 0.2:
                if (day3['close'] < day3['open'] and
                    day3['close'] < (day1['open'] + day1['close']) / 2):
                    if is_in_uptrend:
                        detected_patterns.append("Evening Star")

    return detected_patterns



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
# تابع اصلی تجمیع اندیکاتورها
# -----------------------------------------------------------
def calculate_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    محاسبه تمام اندیکاتورهای تکنیکال مورد نیاز و اضافه کردن آنها به DataFrame.
    """

    # اطمینان از اینکه دیتافریم خالی نیست و دارای ستون‌های ضروری است
    required_cols = {'open', 'high', 'low', 'close', 'volume'}
    if df.empty or not required_cols.issubset(df.columns):
        logger.warning("DataFrame خالی است یا ستون‌های لازم را ندارد.")
        return df

    try:
        # تبدیل ستون‌ها به نوع عددی و حذف مقادیر نامعتبر
        for col in required_cols:
            if not pd.api.types.is_numeric_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df.dropna(subset=list(required_cols), inplace=True)

        if df.empty:
            logger.warning("پس از تبدیل و پاکسازی، دیتای معتبری برای محاسبه اندیکاتورها باقی نماند.")
            return df

        # --- محاسبات اندیکاتورهای استاندارد ---
        df['RSI'] = calculate_rsi(df['close'])

        macd, signal, histogram = calculate_macd(df['close'])
        df['MACD'] = macd
        df['MACD_Signal'] = signal
        df['MACD_Histogram'] = histogram

        df['SMA_20'] = calculate_sma(df['close'], 20)
        df['SMA_50'] = calculate_sma(df['close'], 50)

        upper, middle, lower = calculate_bollinger_bands(df['close'])
        df['Bollinger_Upper'] = upper
        df['Bollinger_Middle'] = middle
        df['Bollinger_Lower'] = lower

        df['Volume_MA_20'] = calculate_volume_ma(df['volume'], 20)
        df['ATR'] = calculate_atr(df['high'], df['low'], df['close'])

        # --- محاسبات اندیکاتورهای پیشرفته ---
        stochastic_k, stochastic_d = calculate_stochastic(df['high'], df['low'], df['close'])
        df['Stochastic_K'] = stochastic_k
        df['Stochastic_D'] = stochastic_d

        # محاسبه Squeeze Momentum
        df['squeeze_on'], df['squeeze_momentum'] = calculate_squeeze_momentum(df)

        # محاسبه HalfTrend
        df['halftrend_trend'], df['halftrend_signal'] = calculate_halftrend(df)

        # محاسبه شکست مقاومت
        df['resistance_level_50d'], df['resistance_broken'] = calculate_support_resistance_break(df)

    except Exception as e:
        logger.error(f"❌ خطای بحرانی در پردازش داده‌ها یا محاسبه اندیکاتورهای استاندارد: {e}", exc_info=True)
        # در صورت بروز خطای بحرانی، DataFrame را با داده‌های موجود برمی‌گردانیم

    return df

# -----------------------------------------------------------
# توابع تشخیص الگوهای شمعی (Candlestick Pattern Detection)
# -----------------------------------------------------------

def check_candlestick_patterns(today_record: dict, yesterday_record: dict, historical_df: pd.DataFrame) -> List[str]:
    """
    تشخیص الگوهای شمعی با استفاده از داده‌های روز جاری و قبلی و کل تاریخچه (برای الگوهای چندروزه).

    ورودی:
    - today_record: دیکشنری رکورد امروز.
    - yesterday_record: دیکشنری رکورد دیروز.
    - historical_df: کل DataFrame تاریخی (مرتب شده بر اساس تاریخ).

    خروجی: لیستی از نام الگوهای یافت‌شده.
    """
    patterns = []

    # ⚠️ توجه: در واقعیت، برای تشخیص دقیق الگوها، نیاز به یک کتابخانه مثل TA-Lib یا پیاده‌سازی‌های دقیق است.
    # در اینجا، چند الگوی ساده به عنوان نمونه پیاده‌سازی می‌شوند:

    close = today_record['close']
    open_ = today_record['open']
    high = today_record['high']
    low = today_record['low']

    prev_close = yesterday_record['close']
    prev_open = yesterday_record['open']

    # 1. صخره (Doji - ساده): باز شدن و بسته شدن نزدیک به هم
    if abs(open_ - close) / (high - low) < 0.1 and (high - low) > 0:
        patterns.append("Doji (صخره)")

    # 2. چکش (Hammer - ساده): سایه پایینی بلند، بدنه کوچک در بالا
    body = abs(open_ - close)
    lower_shadow = min(open_, close) - low
    upper_shadow = high - max(open_, close)

    if lower_shadow > 2 * body and upper_shadow < 0.5 * body:
        patterns.append("Hammer (چکش)")

    # 3. پوشای صعودی (Bullish Engulfing - دو روزه)
    # الف) روز اول: نزولی (بسته شدن < باز شدن)
    # ب) روز دوم: صعودی (بسته شدن > باز شدن)
    # ج) بدنه روز دوم، بدنه روز اول را کاملاً پوشانده
    if prev_close < prev_open and close > open_:
        if close > prev_open and open_ < prev_close:
            patterns.append("Bullish_Engulfing (پوشای صعودی)")

    # 4. ستاره صبحگاهی (Morning Star - سه روزه)
    if len(historical_df) >= 3:
        day3 = historical_df.iloc[-1] # امروز
        day2 = historical_df.iloc[-2] # دیروز
        day1 = historical_df.iloc[-3] # دو روز پیش

        # الف) روز اول: شمع بزرگ نزولی
        cond1 = day1['close'] < day1['open'] and abs(day1['close'] - day1['open']) > (day1['high'] - day1['low']) * 0.5
        # ب) روز دوم: شمع کوچک (دوجی یا اسپینینگ تاپ) و دارای گپ نزولی
        cond2 = abs(day2['close'] - day2['open']) < (day2['high'] - day2['low']) * 0.3 and day2['high'] < day1['low']
        # ج) روز سوم: شمع بزرگ صعودی که حداقل نصف بدنه روز اول را می‌پوشاند
        cond3 = day3['close'] > day3['open'] and day3['close'] > (day1['open'] + day1['close']) / 2

        if cond1 and cond2 and cond3:
            patterns.append("Morning_Star (ستاره صبحگاهی)")

    # افزودن الگوهای بیشتر بر اساس نیاز...

    return patterns

# -----------------------------------------------------------
# تابع Placeholder برای تحلیل فاندامنتال
# -----------------------------------------------------------

def calculate_fundamental_metrics(df: pd.DataFrame, fundamental_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    محاسبه شاخص‌های فاندامنتال بر اساس داده‌های موجود در DataFrame تاریخی و داده‌های فاندامنتال مجزا.

    ⚠️ توجه: برای پیاده‌سازی کامل، باید ستون‌های فاندامنتال (مانند EPS, P/E, DPS) به HistoricalData اضافه شوند
    یا از یک جدول/فایل جداگانه فراخوانی شوند.
    """
    # در این مرحله، ما تنها از داده‌های موجود در HistoricalData استفاده می‌کنیم (مانند نسبت‌های قدرت خریدار/فروشنده)

    # 1. محاسبه قدرت خریدار حقیقی / فروشنده حقیقی (بر اساس حجم و تعداد)
    buy_power_real = df['buy_i_volume'] / df['buy_count_i'].replace(0, np.nan)
    sell_power_real = df['sell_i_volume'] / df['sell_count_i'].replace(0, np.nan)

    # نسبت قدرت خرید به فروش
    df['Real_Buyer_Power'] = buy_power_real.fillna(0)
    df['Real_Seller_Power'] = sell_power_real.fillna(0)

    with np.errstate(divide='ignore', invalid='ignore'):
        df['Real_Power_Ratio'] = (buy_power_real / sell_power_real).fillna(1)

    # 2. محاسبه حجم/میانگین حجم
    df['Volume_Ratio_20d'] = df['volume'] / df['Volume_MA_20'].replace(0, np.nan)

    # 3. نقدشوندگی (Liquidity) - اندازه‌گیری سادگی معامله
    # Liquidity = (Volume * Price) / Number_of_Shares_Float
    # از آنجایی که تعداد سهام شناور نداریم، از حجم معاملاتی استفاده می‌کنیم.
    df['Daily_Liquidity'] = df['volume'] * df['final']

    return df

# لیست توابعی که برای import شدن در سرویس اصلی مجاز هستند
__all__ = [
    'calculate_all_indicators',
    'check_candlestick_patterns',
    'calculate_fundamental_metrics'
]