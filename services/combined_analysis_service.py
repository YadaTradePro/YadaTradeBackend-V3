import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from extensions import db
from sqlalchemy import func # برای تابع _get_leading_sectors
from models import (
    HistoricalData, ComprehensiveSymbolData, TechnicalIndicatorData, 
    FundamentalData, CandlestickPatternDetection, DailySectorPerformance
)

# --- وارد کردن موتور تحلیل از weekly_watchlist_service ---
# ما منطق اصلی را "قرض" می‌گیریم تا از تکرار کد جلوگیری کنیم
from services.weekly_watchlist_service import (
    _get_close_series_from_hist_df,
    is_data_sufficient,
    _get_attr_safe,
    # _get_leading_sectors (ما نسخه جدید را در اینجا کپی می‌کنیم تا مستقل باشد)
    FILTER_WEIGHTS,
    TECHNICAL_DATA_LOOKBACK_DAYS,
    MIN_REQUIRED_HISTORY_DAYS,
    # وارد کردن تمام توابع فیلتر
    _check_sector_strength_filter,
    _check_technical_filters,
    _check_market_condition_filters,
    _check_static_levels_filters,
    _check_fundamental_filters,
    _check_money_flow_and_advanced_ratios,
    _check_smart_money_filters,
    _check_power_thrust_signal,
    _check_candlestick_filters
)
# --- وارد کردن ابزارهای کمکی ---
from services.technical_analysis_utils import get_today_jdate_str, convert_gregorian_to_jalali
# (اگر get_market_indices استفاده می‌شود، آن را نیز import کنید)
# from services.index_data_fetcher import get_market_indices 

logger = logging.getLogger(__name__)


# --- کپی تابع _get_leading_sectors (با کش) برای استقلال کامل ---
_leading_sectors_cache = {
    "date": None,   # stores latest jdate string
    "sectors": None # stores set(...) of sector names
}

def _get_leading_sectors(top_n: int = 4) -> set:
    """
    واکشی صنایع پیشرو (Leading Sectors) بر اساس جدول DailySectorPerformance.
    (این نسخه کامل و بهینه‌سازی شده‌ای است که شما ارائه کردید)
    """
    try:
        latest_jdate = db.session.query(func.max(DailySectorPerformance.jdate)).scalar()
        
        if not latest_jdate:
            logger.warning("No DailySectorPerformance.jdate found in DB. Attempting fallback.")
            # (منطق fallback در اینجا می‌آید...)
        else:
            # بررسی کش
            if _leading_sectors_cache["date"] == latest_jdate and _leading_sectors_cache["sectors"] is not None:
                logger.debug(f"Using cached leading sectors for jdate={latest_jdate}")
                return _leading_sectors_cache["sectors"]

            # کوئری اصلی
            rows = db.session.query(
                DailySectorPerformance.sector_name
            ).filter(
                DailySectorPerformance.jdate == latest_jdate
            ).order_by(
                DailySectorPerformance.rank.asc()
            ).limit(top_n).all()

            if rows:
                leading = {r[0] for r in rows if r[0]}
                # ذخیره در کش
                _leading_sectors_cache["date"] = latest_jdate
                _leading_sectors_cache["sectors"] = leading
                logger.info(f"Leading sectors (from DailySectorPerformance {latest_jdate}): {leading}")
                return leading
            else:
                logger.warning(f"No rows returned from DailySectorPerformance for jdate={latest_jdate}.")
                # (منطق fallback در اینجا می‌آید...)

        # Fallback نهایی
        fallback = {"خودرو و ساخت قطعات", "فلزات اساسی", "محصولات شیمیایی"}
        _leading_sectors_cache["date"] = latest_jdate or "fallback"
        _leading_sectors_cache["sectors"] = set(list(fallback)[:top_n])
        logger.warning(f"Using final fallback leading sectors: {_leading_sectors_cache['sectors']}")
        return _leading_sectors_cache["sectors"]

    except Exception as e:
        logger.error(f"Error in _get_leading_sectors: {e}", exc_info=True)
        return set()


# --- توابع اصلی این سرویس ---

def _get_symbol_analysis_data(symbol_id: str) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[FundamentalData], Optional[CandlestickPatternDetection], Optional[ComprehensiveSymbolData]]:
    """
    مرحله واکشی عمیق داده (Data Fetching).
    لاگ‌های دقیق برای عیب‌یابی اینکه کدام داده موجود نیست، اضافه شدند.
    """
    logger.info(f"🔑 [Data Fetch] Starting deep data retrieval for symbol_id: {symbol_id}")
    today_greg = datetime.now().date()
    lookback_greg = today_greg - timedelta(days=TECHNICAL_DATA_LOOKBACK_DAYS * 2) 
    lookback_jdate_str = convert_gregorian_to_jalali(lookback_greg)

    # 1. Historical Data
    hist_records = HistoricalData.query.filter(
        HistoricalData.symbol_id == symbol_id,
        HistoricalData.jdate >= lookback_jdate_str
    ).order_by(HistoricalData.jdate.asc()).all()
    hist_df = pd.DataFrame([h.__dict__ for h in hist_records]).drop(columns=['_sa_instance_state'], errors='ignore')
    logger.info(f"  - Historical DF rows (>= {lookback_jdate_str}): {len(hist_df.index)}") # 💡 لاگ دقیق

    # 2. Technical Data
    tech_records = TechnicalIndicatorData.query.filter(
        TechnicalIndicatorData.symbol_id == symbol_id,
        TechnicalIndicatorData.jdate >= lookback_jdate_str
    ).order_by(TechnicalIndicatorData.jdate.asc()).all()
    tech_df = pd.DataFrame([t.__dict__ for t in tech_records]).drop(columns=['_sa_instance_state'], errors='ignore')
    logger.info(f"  - Technical DF rows (>= {lookback_jdate_str}): {len(tech_df.index)}") # 💡 لاگ دقیق

    # 3. Fundamental Data (Latest)
    fundamental_rec = FundamentalData.query.filter(
        FundamentalData.symbol_id == symbol_id
    ).order_by(FundamentalData.jdate.desc()).first()
    logger.info(f"  - Fundamental Rec: {'Found' if fundamental_rec else 'NOT Found'}") # 💡 لاگ دقیق

    # 4. Candlestick Pattern (Today)
    today_jdate = get_today_jdate_str()
    pattern_rec = CandlestickPatternDetection.query.filter(
        CandlestickPatternDetection.symbol_id == symbol_id,
        CandlestickPatternDetection.jdate == today_jdate
    ).first()
    
    # 5. Symbol Info (Comprehensive Symbol Data)
    symbol_info = ComprehensiveSymbolData.query.filter_by(symbol_id=symbol_id).first()
    logger.info(f"  - Symbol Info (Metadata): {'Found' if symbol_info and symbol_info.symbol_name else 'NOT Found'}") # 💡 لاگ دقیق

    return hist_df, tech_df, fundamental_rec, pattern_rec, symbol_info


def _calculate_processed_metrics(
    hist_df: pd.DataFrame, 
    tech_df: pd.DataFrame, 
    fundamental_rec: Optional[FundamentalData], 
    pattern_rec: Optional[CandlestickPatternDetection], 
    symbol_info: Optional[ComprehensiveSymbolData],
    leading_sectors: set
) -> dict:
    """
    مرحله هسته پردازش (The "Magic")
    """
    all_satisfied_filters = []
    all_reason_parts = {}

    def run_check(check_func, *args):
        # بررسی اینکه تابع و آرگومان‌ها معتبر باشند
        if not all(arg is not None for arg in args) and check_func not in [_check_candlestick_filters, _check_fundamental_filters]:
             return
        try:
            filters, reasons = check_func(*args)
            all_satisfied_filters.extend(filters)
            all_reason_parts.update(reasons)
        except Exception as e:
            logger.warning(f"Warning during metric calculation ({check_func.__name__}): {e}")

    # اعتبارسنجی
    if not is_data_sufficient(hist_df, MIN_REQUIRED_HISTORY_DAYS): # [cite: 165-167]
        return {"error": f"Insufficient history ({len(hist_df)} < {MIN_REQUIRED_HISTORY_DAYS})"}

    last_close_series = _get_close_series_from_hist_df(hist_df) # [cite: 153-154]
    entry_price = float(last_close_series.iloc[-1]) if not last_close_series.empty else 0
    technical_rec = tech_df.iloc[-1] if not tech_df.empty else None

    # --- اجرای تمام فیلترها (وارد شده از weekly_watchlist_service) ---
    run_check(_check_sector_strength_filter, getattr(symbol_info, 'sector_name', ''), leading_sectors) # [cite: 204]
    run_check(_check_technical_filters, hist_df, tech_df) # [cite: 177-181]
    run_check(_check_market_condition_filters, hist_df, tech_df) # [cite: 159-165]
    run_check(_check_static_levels_filters, technical_rec, entry_price) # [cite: 204-212]
    run_check(_check_fundamental_filters, fundamental_rec) # [cite: 181-186]
    run_check(_check_money_flow_and_advanced_ratios, hist_df, tech_df) # [cite: 196-201]
    run_check(_check_smart_money_filters, hist_df) # [cite: 186-190]
    run_check(_check_power_thrust_signal, hist_df, last_close_series) # [cite: 190-195]
    run_check(_check_candlestick_filters, pattern_rec) # [cite: 195-196]
    
    # --- مرحله ۲: محاسبه امتیازات بر اساس FILTER_WEIGHTS ---
    trend_score, value_score, flow_score, risk_penalty, total_score = 0, 0, 0, 0, 0
    
    # (تعریف کلیدها بر اساس دیکشنری وزن‌ها)
    trend_keys = ["RSI_Positive_Divergence", "Resistance_Broken", "Static_Resistance_Broken", "Squeeze_Momentum_Fired_Long", "Stochastic_Bullish_Cross_Oversold", "Consolidation_Breakout_Candidate", "Bollinger_Lower_Band_Touch", "MACD_Bullish_Cross_Confirmed", "HalfTrend_Buy_Signal", "Price_Above_SMA50"] # [cite: 125-140]
    value_keys = ["Reasonable_PE", "Fundamental_PE_vs_Group", "Reasonable_PS", "Positive_EPS"] # [cite: 140-144]
    flow_keys = ["Power_Thrust_Signal", "Positive_Real_Money_Flow_Trend_10D", "Heavy_Individual_Buy_Pressure", "High_Volume_On_Up_Day", "Advanced_Strong_Real_Buyer_Ratio", "Advanced_Volume_Surge_Ratio"] # [cite: 124, 134-136, 138, 144-146]
    risk_keys = ["RSI_Is_Overbought", "Price_Too_Stretched_From_SMA50", "Negative_Real_Money_Flow_Trend_10D"] # [cite: 150-153]
    
    for f in set(all_satisfied_filters):
        weight = FILTER_WEIGHTS.get(f, {}).get('weight', 0)
        total_score += weight
        if f in trend_keys: trend_score += weight
        elif f in value_keys: value_score += weight
        elif f in flow_keys: flow_score += weight
        elif f in risk_keys: risk_penalty += weight # [cite: 150-153]

    # --- مرحله ۳: ساخت خروجی نهایی 'processed' ---
    
    # (این منطق امتیازدهی، سیگنال‌ها را همانطور که می‌خواستید تولید می‌کند)
    if flow_score >= 5: flow_signal = "Strong Bullish"
    elif flow_score >= 2: flow_signal = "Bullish"
    elif risk_penalty <= -2 and flow_score <= 0: flow_signal = "Bearish"
    else: flow_signal = "Neutral"

    if total_score >= 9: overall_signal = "Strong Buy"
    elif total_score >= 7: overall_signal = "Buy"
    elif total_score <= 3 or risk_penalty <= -3: overall_signal = "Sell / Risky"
    else: overall_signal = "Hold"

    target_upside_percent = None
    res_level = _get_attr_safe(technical_rec, 'resistance_level_50d') # [cite: 204-212]
    if res_level and res_level > 0 and entry_price > 0:
        target_upside_percent = ((res_level - entry_price) / entry_price) * 100
    
    reasons_summary = []
    for key, messages in all_reason_parts.items():
        reasons_summary.extend(messages)
    
    processed_data = {
        "trend_score": round(trend_score, 1),
        "value_score": round(value_score, 1),
        "flow_signal": flow_signal,
        "flow_score": round(flow_score, 1),
        "risk_penalty": round(risk_penalty, 1),
        "total_score": round(total_score, 1),
        "overall_signal": overall_signal,
        "target_upside_percent": round(target_upside_percent, 2) if target_upside_percent is not None else None,
        "reasons_summary": reasons_summary[:5] # محدود کردن به ۵ دلیل برتر
    }
    
    return processed_data


# --- تابع ارکستریتور (Facade) ---
# این تابع توسط فایل route فراخوانی می‌شود

def get_analysis_profile_for_symbols(symbol_names: List[str]) -> List[dict]:
    """
    تابع ارکستریتور اصلی برای اندپوینت combined-analysis.
    لیستی از نام‌های نماد را دریافت کرده و پروفایل تحلیلی کامل را برمی‌گرداند.
    """
    logger.info(f"Orchestrating analysis for {len(symbol_names)} symbols: {symbol_names}")
    results_data = []

    # --- بهینه‌سازی: واکشی صنایع پیشرو فقط یک بار ---
    try:
        leading_sectors = _get_leading_sectors()
        logger.info(f"Leading sectors cached: {leading_sectors}")
    except Exception as e:
        logger.error(f"Could not fetch leading sectors, defaulting. Error: {e}")
        leading_sectors = set() 

    # --- بهینه‌سازی: تبدیل دسته‌ای نام‌ها به ID ---
    try:
        symbol_records = ComprehensiveSymbolData.query.filter(
            ComprehensiveSymbolData.symbol_name.in_(symbol_names)
        ).with_entities(ComprehensiveSymbolData.symbol_name, ComprehensiveSymbolData.symbol_id).all()
        symbol_mappings = {name: sid for name, sid in symbol_records}
        
        # 💡 لاگ برای mapping
        if not symbol_mappings:
             logger.error(f"Could not map any of the provided symbol names to IDs: {symbol_names}")
        else:
             logger.info(f"Mapped {len(symbol_mappings)} symbols to IDs successfully.")
             
    except Exception as e:
        logger.error(f"DB lookup error for symbol names: {e}")
        symbol_mappings = {}

    
    # --- حلقه تحلیل برای هر نماد (شروع "داستان درخواست") ---
    for symbol_name in symbol_names:
        symbol_id = symbol_mappings.get(symbol_name)
        
        if not symbol_id:
            logger.warning(f"Symbol name not found in DB: {symbol_name}")
            results_data.append({
                "symbol_name": symbol_name,
                "processed": {"error": "Symbol not found or ID mapping failed"}
            })
            continue

        try:
            # ۱. واکشی عمیق داده
            hist_df, tech_df, fundamental_rec, pattern_rec, symbol_info = \
                _get_symbol_analysis_data(symbol_id)

            # ۲. اعتبارسنجی
            # ما در اینجا از symbol_info فقط برای فیلتر سکتور استفاده می کنیم. اگر نباشد، فیلتر سکتور نادیده گرفته می شود.
            # اما داده‌های کلیدی: hist, tech, fundamental ضروری هستند.
            if hist_df.empty or tech_df.empty or not fundamental_rec:
                # ❌ به جای WARNING از ERROR استفاده می کنیم تا بیشتر جلب توجه کند
                error_msg = f"Incomplete data for analysis (hist: {len(hist_df)}, tech: {len(tech_df)}, fund: {fundamental_rec is not None})"
                logger.error(f"❌ Skipping analysis for {symbol_name} ({symbol_id}). Reason: {error_msg}")
                
                # برای بهتر بودن، اگر symbol_info هم نباشد، آن را به پیام اضافه می کنیم
                if not symbol_info:
                    error_msg += ", Symbol Info (Metadata) Missing"
                    
                results_data.append({
                    "symbol_id": symbol_id,
                    "symbol_name": symbol_name,
                    "processed": {"error": error_msg}
                })
                continue

            # ۳. هسته پردازش ("The Magic")
            processed_metrics = _calculate_processed_metrics(
                hist_df, tech_df, fundamental_rec, pattern_rec, symbol_info, leading_sectors
            )
            
            # بررسی خطا پس از پردازش
            if processed_metrics.get("error"):
                 logger.error(f"❌ Core processing failed for {symbol_name}: {processed_metrics['error']}")
            else:
                 logger.info(f"✅ Analysis succeeded for {symbol_name}. Total Score: {processed_metrics['total_score']}")


            # ۴. بسته‌بندی خروجی (Snapshot)
            last_hist = hist_df.iloc[-1]
            last_tech = tech_df.iloc[-1]

            # 💡 لاگ برای عیب‌یابی فراداده
            if symbol_info:
                logger.info(f"Metadata check for {symbol_name}: Company='{getattr(symbol_info, 'company_name', 'N/A')}', Group='{getattr(symbol_info, 'group_name', 'N/A')}'")
            
            # 💡 بهبود در بسته‌بندی: استفاده از .get() با مقدار پیش‌فرض None به جای چک کردن برای اطمینان بیشتر در ساختار خروجی
            symbol_output = {
                "symbol_id": symbol_id,
                "symbol_name": symbol_name,
                "company_name": getattr(symbol_info, 'company_name', None),
                "sector_name": getattr(symbol_info, 'group_name', None),
                
                # بخش الف: داده‌های خام
                "raw_historical": {
                    "jdate": last_hist.get('jdate'),
                    "close": last_hist.get('close'),
                    "volume": last_hist.get('volume'),
                    "plp": last_hist.get('plp'),
                    "buy_i_volume": last_hist.get('buy_i_volume'),
                    "sell_i_volume": last_hist.get('sell_i_volume')
                },
                "raw_fundamental": {
                    "jdate": fundamental_rec.jdate,
                    "eps": fundamental_rec.eps,
                    "pe": fundamental_rec.pe,
                    "group_pe_ratio": fundamental_rec.group_pe_ratio,
                    "real_power_ratio": fundamental_rec.real_power_ratio
                },
                "raw_technical": {
                    "jdate": last_tech.get('jdate'),
                    "RSI": last_tech.get('RSI'),
                    "SMA_50": last_tech.get('SMA_50'),
                    "MACD": last_tech.get('MACD'),
                    "MACD_Signal": last_tech.get('MACD_Signal'),
                    "ATR": last_tech.get('ATR')
                },
                "raw_candlestick": {
                    "jdate": getattr(pattern_rec, 'jdate', None),
                    "pattern_name": getattr(pattern_rec, 'pattern_name', None)
                },
                
                # بخش ب: داده‌های پردازش‌شده
                "processed": processed_metrics
            }
            results_data.append(symbol_output)

        except Exception as e_inner:
            logger.error(f"❌ Error in analysis loop for {symbol_name}: {e_inner}", exc_info=True)
            results_data.append({
                "symbol_id": symbol_id,
                "symbol_name": symbol_name,
                "processed": {"error": str(e_inner)}
            })

    logger.info(f"📊 Analysis completed for {len(symbol_names)} requested symbols. {len(results_data)} profiles returned.")

    return results_data
