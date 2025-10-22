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


شما کاملاً درست می‌گویید و من عذرخواهی می‌کنم. خروجی 58 الگو در یک روز، مصداق بارز "نویز" (Noise) و سیگنال کاذب است.

مشکل دقیقاً از دو الگویی است که در مرحله قبل اضافه یا اصلاح کردیم (Spinning_Top و Four_Price_Doji). منطق آن‌ها بیش از حد ساده‌انگارانه بود و هر کندل شبیه به فرفره یا هر نماد متوقف را شناسایی می‌کرد، بدون اینکه به اهمیت (Significance) آن توجه کند.

تحلیل مشکل و راه‌حل نهایی
مشکل Four_Price_Doji (دوجی چهار قیمت):

اشکال قبلی: فیلتر حجم (> 10% میانگین) بسیار ضعیف بود. این باعث می‌شد هر سهمی که به هر دلیلی (حتی عدم معامله یا پر کردن حجم مبنای ناچیز) متوقف بوده، به عنوان یک الگوی معنادار شناسایی شود.

اصلاح نهایی: این الگو (که نشانه‌ی صف خرید یا فروش قفل شده است) تنها زمانی اهمیت دارد که حجم معاملات آن واقعاً قابل توجه باشد.

فیلتر جدید: حجم معامله امروز باید بیشتر از 50% میانگین حجم 20 روزه باشد و همچنین به صورت مطلق بیشتر از 500,000 سهم باشد. این کار تمام نمادهای کم‌اهمیت و متوقف که صف معناداری ندارند را حذف می‌کند.

مشکل Spinning_Top (فرفره):

اشکال قبلی: ما فقط "شکل" فرفره را شناسایی کردیم. اما یک فرفره به خودی خود یک الگو نیست؛ بلکه "بخشی" از یک الگو است که نشانه‌ی بلاتکلیفی است.

اصلاح نهایی: این الگو تنها زمانی معنادار است که پس از یک روند قوی رخ دهد (که نشانه‌ی توقف روند است) و با حجم بالا تایید شود (که نشانه‌ی جدال واقعی خریدار و فروشنده است).

فیلتر جدید (۱ - روند): این الگو فقط در صورتی شناسایی می‌شود که در 5 روز گذشته یک روند قوی (بیش از 8% رشد یا افت) وجود داشته باشد.

فیلتر جدید (۲ - حجم): حجم معاملات امروز باید بیشتر از 80% میانگین حجم 20 روزه باشد.

این دو تغییر، تعداد الگوهای کاذب را به شدت کاهش می‌دهد و فقط سیگنال‌هایی که از نظر آماری "مهم" هستند را به شما نشان می‌دهد.

کد نهایی و تصحیح‌شده (نسخه ۴)
Python

import pandas as pd
from typing import List

# ===============================
# توابع اصلی تشخیص الگوها شمعی
# ===============================

def check_candlestick_patterns(today_record: dict, yesterday_record: dict, historical_df: pd.DataFrame) -> List[str]:
    """
    (نسخه ۴)
    تشخیص الگوهای شمعی با فیلترهای سخت‌گیرانه‌تر برای حجم و روند
    جهت حذف سیگنال‌های کاذب (Noise)
    """
    patterns = []

    # استخراج داده‌های امروز و دیروز (محافظت در برابر None/NaN)
    try:
        close = float(today_record.get('close', 0) or 0)
        open_ = float(today_record.get('open', 0) or 0)
        high = float(today_record.get('high', 0) or 0)
        low = float(today_record.get('low', 0) or 0)
        volume = float(today_record.get('volume', 0) or 0)

        prev_close = float(yesterday_record.get('close', 0) or 0)
        prev_open = float(yesterday_record.get('open', 0) or 0)
        prev_high = float(yesterday_record.get('high', 0) or 0)
        prev_low = float(yesterday_record.get('low', 0) or 0)
        prev_volume = float(yesterday_record.get('volume', 0) or 0)
    except Exception:
        return patterns 

    # محاسبات پایه
    body = abs(close - open_)
    total_range = max(0.0, high - low)
    lower_shadow = min(open_, close) - low
    upper_shadow = high - max(open_, close)

    # میانگین حجم 20 روزه و میانگین رنج 5 روزه
    volume_ma_20 = historical_df['volume'].tail(20).mean() if len(historical_df) >= 20 else (volume or 1)
    avg_range_5d = calculate_average_range(historical_df, 5)

    if volume_ma_20 <= 0:
        volume_ma_20 = volume or 1.0

    # فیلتر اولیه: حذف نمادهای با حجم بسیار پایین (غیر از دوجی چهار قیمت)
    if total_range > 0 and volume < (volume_ma_20 * 0.25):
         return patterns

    # --- Trend Definition (shared by most patterns) ---
    trend_5d_ending_yesterday = 0.0
    trend_10d_ending_yesterday = 0.0
    
    if len(historical_df) >= 6: 
        trend_df_5d = historical_df.iloc[-6:-1]
        trend_5d_ending_yesterday = calculate_trend(trend_df_5d, period=5)
    
    if len(historical_df) >= 11: 
        trend_df_10d = historical_df.iloc[-11:-1]
        trend_10d_ending_yesterday = calculate_trend(trend_df_10d, period=10)

    # --- Doji (اصلاح شده) ---
    patterns.extend(check_doji_patterns(
        open_, close, high, low, volume,
        total_range, body, upper_shadow, lower_shadow,
        volume_ma_20, avg_range_5d, trend_10d_ending_yesterday, historical_df
    ))
    
    # --- Spinning Top (اصلاح شده) ---
    patterns.extend(check_spinning_top(
        total_range, body, upper_shadow, lower_shadow, avg_range_5d,
        volume, volume_ma_20, trend_5d_ending_yesterday
    ))

    # --- Hammer / Hanging Man ---
    patterns.extend(check_hammer_hanging_man(
        open_, close, high, low,
        total_range, body, upper_shadow, lower_shadow,
        trend_5d_ending_yesterday, historical_df
    ))
    
    # --- Inverted Hammer / Shooting Star ---
    patterns.extend(check_inverted_hammer_shooting_star(
        open_, close, high, low,
        total_range, body, upper_shadow, lower_shadow,
        trend_5d_ending_yesterday, historical_df
    ))

    # --- Engulfing (Bullish / Bearish) ---
    patterns.extend(check_engulfing_patterns(
        open_, close, prev_open, prev_close,
        high, low, prev_high, prev_low,
        body, total_range, volume, prev_volume, 
        volume_ma_20, avg_range_5d, trend_5d_ending_yesterday
    ))

    # --- Morning Star / Evening Star ---
    patterns.extend(check_star_patterns(historical_df))

    # --- Harami ---
    patterns.extend(check_harami_pattern(
        open_, close, high, low,
        prev_open, prev_close, prev_high, prev_low,
        volume, prev_volume, avg_range_5d, trend_10d_ending_yesterday
    ))

    # --- Piercing Line / Dark Cloud Cover ---
    patterns.extend(check_piercing_darkcloud_patterns(
        open_, close, high, low,
        prev_open, prev_close, prev_high, prev_low,
        volume, prev_volume, avg_range_5d, trend_5d_ending_yesterday
    ))

    return patterns


# ===============================
# الگوهای فرعی و فیلترهای تخصصی
# ===============================

def check_doji_patterns(open_, close, high, low, volume,
                        total_range, body, upper_shadow, lower_shadow,
                        volume_ma_20, avg_range_5d, trend_10d, historical_df):
    """(اصلاح شده) فیلتر حجم بسیار سخت‌گیرانه‌تر برای دوجی چهار قیمت"""
    patterns = []

    # (اصلاح ۱) بررسی دوجی چهار قیمت (مخصوص بازار ایران - صف خرید/فروش)
    if total_range <= 0:
        # O=H=L=C
        # (اصلاح نهایی) فیلتر حجم بسیار سخت‌گیرانه‌تر:
        # 1. حجم باید حداقل 50% میانگین 20 روزه باشد
        # 2. حجم باید حداقل 500,000 سهم باشد (حذف نمادهای بی‌اهمیت)
        is_significant_volume = (volume / (volume_ma_20 or 1) > 0.5) and (volume > 500000)
        
        if body == 0 and is_significant_volume:
            patterns.append("Four_Price_Doji (دوجی چهار قیمت)")
        return patterns 

    body_to_range_ratio = body / total_range if total_range > 0 else 1.0

    # فیلتر سخت‌گیرانه دوجی: بدنه باید کمتر از 7% کل رنج باشد
    if body_to_range_ratio >= 0.07:
        return patterns

    # فیلتر رنج: کندل نباید خیلی کوچک باشد
    if total_range < (avg_range_5d * 0.35 if avg_range_5d > 0 else 0):
        return patterns
    
    # فیلتر حجم: حجم نباید خیلی کمتر از میانگین باشد
    volume_ratio = volume / volume_ma_20 if volume_ma_20 > 0 else 1
    if volume_ratio < 0.5:
        return patterns

    # بررسی موقعیت در روند (برای دوجی سنگ قبر و سنجاقک)
    if len(historical_df) >= 10:
        recent_high_10d = historical_df['high'].tail(10).max()
        recent_low_10d = historical_df['low'].tail(10).min()
        is_near_high = (high >= recent_high_10d * 0.97)
        is_near_low = (low <= recent_low_10d * 1.03)

        lower_shadow_ratio = lower_shadow / total_range
        upper_shadow_ratio = upper_shadow / total_range

        # تعریف سخت‌گیرانه‌تر: سایه اصلی > 65%، سایه مخالف < 15%
        if upper_shadow_ratio > 0.65 and lower_shadow_ratio < 0.15 and trend_10d > 0.1 and is_near_high:
            patterns.append("Gravestone_Doji (دوجی سنگ قبر)")
        elif lower_shadow_ratio > 0.65 and upper_shadow_ratio < 0.15 and trend_10d < -0.1 and is_near_low:
            patterns.append("Dragonfly_Doji (دوجی سنجاقک)")

    return patterns

def check_spinning_top(total_range, body, upper_shadow, lower_shadow, avg_range_5d,
                       volume, volume_ma_20, trend_5d):
    """(اصلاح نهایی) تشخیص فرفره فقط در صورت وجود روند و حجم معنادار"""
    patterns = []
    
    # 1. فیلتر: شکل (Shape)
    if total_range <= 0 or total_range < avg_range_5d * 0.4:
        return patterns 
    body_ratio = body / total_range
    if body_ratio < 0.07 or body_ratio > 0.30:
        return patterns
    if lower_shadow < body or upper_shadow < body:
        return patterns
    max_shadow = max(lower_shadow, upper_shadow)
    min_shadow = min(lower_shadow, upper_shadow)
    if max_shadow == 0: return patterns
    if (min_shadow * 3) < max_shadow:
        return patterns

    # 2. (اصلاح نهایی) فیلتر: زمینه (Context) - باید پس از یک روند قوی باشد
    is_significant_trend = abs(trend_5d) > 0.08 # حداقل 8% حرکت در 5 روز
    if not is_significant_trend:
        return patterns
        
    # 3. (اصلاح نهایی) فیلتر: اهمیت (Significance) - حجم باید نشانه‌ی جدال باشد
    is_significant_volume = (volume / (volume_ma_20 or 1) > 0.8) # حداقل 80% میانگین
    if not is_significant_volume:
        return patterns

    patterns.append("Spinning_Top (فرفره)")
    return patterns


def check_hammer_hanging_man(open_, close, high, low,
                             total_range, body, upper_shadow, lower_shadow,
                             trend_5d, historical_df):
    """تشخیص چکش و مرد آویزان با تعریف کلاسیک (سایه 2 برابر بدنه)"""
    patterns = []
    
    if total_range <= 0 or body <= (total_range * 0.05):
        return patterns

    is_shape = (lower_shadow >= 2 * body) and (upper_shadow <= total_range * 0.10)
    
    if is_shape:
        # Hammer (Bullish Reversal): Needs Downtrend
        if trend_5d < -0.08: 
            recent_low = historical_df['low'].tail(10).min()
            if low <= recent_low * 1.02: 
                patterns.append("Hammer (چکش)")
                
        # Hanging Man (Bearish Reversal): Needs Uptrend
        elif trend_5d > 0.08: 
            recent_high = historical_df['high'].tail(10).max()
            if high >= recent_high * 0.98: 
                patterns.append("Hanging_Man (مرد آویزان)")
                
    return patterns

def check_inverted_hammer_shooting_star(open_, close, high, low,
                                        total_range, body, upper_shadow, lower_shadow,
                                        trend_5d, historical_df):
    """تشخیص چکش وارونه و ستاره ثاقب با تعریف کلاسیک"""
    patterns = []
    
    if total_range <= 0 or body <= (total_range * 0.05):
        return patterns

    is_shape = (upper_shadow >= 2 * body) and (lower_shadow <= total_range * 0.10)
    
    if is_shape:
        # Inverted Hammer (Bullish Reversal): Needs Downtrend
        if trend_5d < -0.08:
            patterns.append("Inverted_Hammer (چکش وارونه)")
                
        # Shooting Star (Bearish Reversal): Needs Uptrend
        elif trend_5d > 0.08:
            recent_high = historical_df['high'].tail(10).max()
            if high >= recent_high * 0.98: 
                patterns.append("Shooting_Star (شوتینگ استار)")
                
    return patterns


def check_engulfing_patterns(open_, close, prev_open, prev_close,
                             high, low, prev_high, prev_low,
                             body, total_range, volume, prev_volume, 
                             volume_ma_20, avg_range_5d, trend_5d):
    """تشخیص پوشا با فیلتر روند و بدنه قوی‌تر"""
    patterns = []

    if total_range <= 0 or (prev_high - prev_low) <= 0:
        return patterns

    today_body = abs(close - open_)
    yesterday_body = abs(prev_close - prev_open)

    if today_body < avg_range_5d * 0.1 or yesterday_body < avg_range_5d * 0.1:
        return patterns

    is_uptrend = trend_5d > 0.08 
    is_downtrend = trend_5d < -0.08 

    # ---------- Bullish Engulfing (پوشای صعودی) ----------
    is_pattern_shape_bullish = (prev_close < prev_open and close > open_)
    
    if is_downtrend and is_pattern_shape_bullish:
        full_engulf = (open_ <= prev_close + (0.001 * prev_close)) and (close >= prev_open - (0.001 * prev_open))
        if full_engulf and today_body > yesterday_body: 
            patterns.append("Bullish_Engulfing (پوشای صعودی)")

    # ---------- Bearish Engulfing (پوشای نزولی) ----------
    is_pattern_shape_bearish = (prev_close > prev_open and close < open_)
    
    if is_uptrend and is_pattern_shape_bearish:
        full_engulf = (open_ >= prev_close - (0.001 * prev_close)) and (close <= prev_open + (0.001 * prev_open))
        
        yesterday_range = max(0.0, prev_high - prev_low)
        yesterday_body_ratio = (yesterday_body / yesterday_range) if yesterday_range > 0 else 0
        today_body_ratio = (today_body / total_range) if total_range > 0 else 0
        
        cond_yesterday_significant = yesterday_body_ratio >= 0.30 
        cond_today_significant = today_body_ratio >= 0.30 

        if full_engulf and today_body > yesterday_body and cond_yesterday_significant and cond_today_significant:
            patterns.append("Bearish_Engulfing (پوشای نزولی)")

    return patterns


def check_star_patterns(historical_df: pd.DataFrame):
    """تشخیص Morning و Evening Star با فیلتر روند سخت‌گیرانه‌تر"""
    patterns = []
    if len(historical_df) < 8: 
        return patterns

    trend_df = historical_df.iloc[-8:-3]
    trend_5d = calculate_trend(trend_df, period=5)
    
    is_uptrend = trend_5d > 0.08 
    is_downtrend = trend_5d < -0.08
    
    d1, d2, d3 = historical_df.iloc[-3], historical_df.iloc[-2], historical_df.iloc[-1]
    
    try:
        d1_open, d1_close = float(d1['open']), float(d1['close'])
        d2_open, d2_close = float(d2['open']), float(d2['close'])
        d3_open, d3_close = float(d3['open']), float(d3['close'])
        
        d1_body = abs(d1_close - d1_open)
        d2_body = abs(d2_close - d2_open)
        d3_body = abs(d3_close - d3_open)

        if d1_body == 0 or d3_body == 0: return patterns
        if (d2_body > d1_body * 0.4) or (d2_body > d3_body * 0.4): return patterns 

        # --- Morning Star (Bullish Reversal) ---
        if (is_downtrend and 
            (d1_close < d1_open) and (d3_close > d3_open) and 
            (max(d2_open, d2_close) < d1_close) and 
            (d3_close > (d1_open + d1_close) / 2)): 
            
            patterns.append("Morning_Star (ستاره صبحگاهی)")

        # --- Evening Star (Bearish Reversal) ---
        if (is_uptrend and
            (d1_close > d1_open) and (d3_close < d3_open) and 
            (min(d2_open, d2_close) > d1_close) and 
            (d3_close < (d1_open + d1_close) / 2)): 
            
            patterns.append("Evening_Star (ستاره عصرگاهی)")
    except Exception:
        pass

    return patterns


def check_harami_pattern(open_, close, high, low, prev_open, prev_close, prev_high, prev_low,
                         volume, prev_volume, avg_range_5d, trend_10d):
    """تشخیص هارامی با افزودن فیلتر رنگ کندل دوم"""
    patterns = []

    body_today = abs(close - open_)
    body_yesterday = abs(prev_close - prev_open)
    range_today = high - low
    range_yesterday = prev_high - prev_low

    if not (min(open_, close) > min(prev_open, prev_close) and
            max(open_, close) < max(prev_open, prev_close)):
        return patterns

    if range_yesterday <= 0 or range_today <= 0:
        return patterns

    if (body_yesterday / range_yesterday) < 0.40:
        return patterns

    if (body_today / range_today) > 0.40:
        return patterns

    is_uptrend = trend_10d > 0.08
    is_downtrend = trend_10d < -0.08
    
    # Bullish Harami: Day 1 Red, Day 2 Green, in Downtrend
    if (is_downtrend and 
        (prev_close < prev_open) and (close > open_)): 
        if volume < prev_volume * 0.9: 
            patterns.append("Bullish_Harami (هارامی صعودی)")
    
    # Bearish Harami: Day 1 Green, Day 2 Red, in Uptrend
    elif (is_uptrend and 
          (prev_close > prev_open) and (close < open_)): 
        if volume < prev_volume * 0.9: 
            patterns.append("Bearish_Harami (هارامی نزولی)")

    return patterns


def check_piercing_darkcloud_patterns(open_, close, high, low, prev_open, prev_close, prev_high, prev_low,
                                      volume, prev_volume, avg_range_5d, trend_5d):
    """
    تشخیص پیرسینگ و دارک کلود با فیلتر گپ (Gap) منطقی‌تر برای بازار ایران
    """
    patterns = []

    is_uptrend = trend_5d > 0.08 
    is_downtrend = trend_5d < -0.08
    
    try:
        body_today = abs(close - open_)
        body_yesterday = abs(prev_close - prev_open)
        range_today = high - low
        range_yesterday = prev_high - prev_low

        if range_today <= 0 or range_yesterday <= 0 or body_today <= 0 or body_yesterday <= 0:
            return patterns
        
        if (body_today / range_today) < 0.4 or (body_yesterday / range_yesterday) < 0.4:
            return patterns
    except Exception:
        return patterns
    
    # --- Piercing Line (Bullish Reversal) ---
    try:
        midpoint_day1_body = (prev_open + prev_close) / 2
        
        if (is_downtrend and
            (prev_close < prev_open) and (close > open_) and
            (open_ < prev_close) and 
            (close > midpoint_day1_body) and
            (close < prev_open)): 
            
            patterns.append("Piercing_Line (پیرسینگ لاین)")
    except Exception:
        pass

    # --- Dark Cloud Cover (Bearish Reversal) ---
    try:
        midpoint_day1_body = (prev_open + prev_close) / 2
        
        if (is_uptrend and
            (prev_close > prev_open) and (close < open_) and
            (open_ > prev_close) and 
            (close < midpoint_day1_body) and
            (close > prev_open)): 
            
            patterns.append("Dark_Cloud_Cover (دارک کلود کاور)")
    except Exception:
        pass

    return patterns


# ===============================
# توابع کمکی
# ===============================

def calculate_average_range(df, period=5):
    """محاسبه میانگین رنج (High - Low) در n دوره اخیر"""
    try:
        if len(df) < period:
            period = len(df)
        if period == 0:
            return 0
            
        ranges = df['high'].tail(period) - df['low'].tail(period)
        return ranges.mean()
    except Exception:
        return 0


def calculate_trend(df: pd.DataFrame, period: int = 5) -> float:
    """محاسبه درصد تغییر قیمت پایانی در n دوره"""
    if df is None or len(df) < 2:
        return 0.0
    
    if len(df) < period:
        period = len(df)
        
    recent = df.tail(period)
    
    try:
        start_price = float(recent.iloc[0]['close'])
        end_price = float(recent.iloc[-1]['close'])
        
        if start_price == 0:
            return 0.0
            
        return (end_price - start_price) / start_price
    except Exception:
        return 0.0

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
