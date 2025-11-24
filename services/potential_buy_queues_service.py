# services/potential_buy_queues_service.py
from extensions import db
from models import HistoricalData, ComprehensiveSymbolData, TechnicalIndicatorData, PotentialBuyQueueResult
from flask import current_app
import pandas as pd
from datetime import datetime
import jdatetime
from services.technical_analysis_utils import (
    get_today_jdate_str,
    calculate_z_score,
    calculate_smart_money_flow,
)
import json
import logging
from typing import List, Dict, Optional
from sqlalchemy import desc

logger = logging.getLogger(__name__)


def get_numeric_value(data_row, key, default=0):
    """Safe numeric extraction"""
    value = data_row.get(key, default)
    return value if pd.notna(value) else default


def get_reliable_price(data_row):
    """Use final or close price"""
    final_price = data_row.get("final")
    close_price = data_row.get("close")
    if pd.notna(final_price) and final_price > 0:
        return float(final_price)
    if pd.notna(close_price) and close_price > 0:
        return float(close_price)
    return 0.0


def convert_jalali_to_gregorian_for_pandas(jdate_str):
    if pd.isna(jdate_str) or not isinstance(jdate_str, str):
        return pd.NaT
    try:
        jy, jm, jd = map(int, jdate_str.split("-"))
        return jdatetime.date(jy, jm, jd).togregorian()
    except Exception:
        return pd.NaT


def run_potential_buy_queue_analysis_and_save():
    """
    تحلیل سهام بر اساس فیلتر Power_Thrust_Signal و چند شاخص مکمل.
    داده‌ها فقط از HistoricalData و TechnicalIndicatorData استخراج می‌شوند و نتایج در PotentialBuyQueueResult ذخیره می‌شوند.
    """
    current_app.logger.info("🚀 Running Power_Thrust_Signal Analysis...")

    symbols = ComprehensiveSymbolData.query.all()
    today_jdate_str = get_today_jdate_str()

    # --- اضافه کردن منطق فیلتر صندوق‌ها ---
    fund_keywords = ["صندوق", "سرمایه", "سرمایه گذاری", "اعتبار", "آتیه", "یکتا", "دارایی", "اختصاصی", "کمند", "پاداش", "ارمغان"]
    fund_symbol_ids_to_ignore = {
        s.symbol_id for s in symbols if any(keyword in s.symbol_name for keyword in fund_keywords)
    }
    # --------------------------------------

    # پاک‌سازی نتایج قبلی
    try:
        PotentialBuyQueueResult.query.delete()
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error clearing table: {e}")

    candidates = []

    for symbol in symbols:
        symbol_id = symbol.symbol_id
        symbol_name = symbol.symbol_name

        # حذف نمادهای حق تقدم
        if "ح" in symbol_name or "حق" in symbol_name:
            continue
            
        # حذف نمادهای صندوق‌های سرمایه‌گذاری
        if symbol_id in fund_symbol_ids_to_ignore:
            continue

        # داده‌های اخیر
        historical = (
            HistoricalData.query.filter_by(symbol_id=symbol_id)
            .order_by(HistoricalData.jdate.desc())
            .limit(60)
            .all()
        )
        technical = (
            TechnicalIndicatorData.query.filter_by(symbol_id=symbol_id)
            .order_by(TechnicalIndicatorData.jdate.desc())
            .limit(60)
            .all()
        )

        if len(historical) < 25 or len(technical) < 25:
            continue

        hist_df = pd.DataFrame([r.__dict__ for r in historical]).drop(
            columns=["_sa_instance_state"], errors="ignore"
        )
        tech_df = pd.DataFrame([r.__dict__ for r in technical]).drop(
            columns=["_sa_instance_state"], errors="ignore"
        )

        hist_df["greg_date"] = hist_df["jdate"].apply(convert_jalali_to_gregorian_for_pandas)
        tech_df["greg_date"] = tech_df["jdate"].apply(convert_jalali_to_gregorian_for_pandas)
        hist_df = hist_df.dropna(subset=["greg_date"]).sort_values(by="greg_date")
        tech_df = tech_df.dropna(subset=["greg_date"]).sort_values(by="greg_date")

        merged_df = pd.merge(hist_df, tech_df, on="jdate", how="left", suffixes=("_hist", "_tech"))
        if merged_df.empty or len(merged_df) < 3:
            continue

        latest = merged_df.iloc[-1]

        close_price = get_reliable_price(latest)
        if close_price <= 0:
            continue

        # ---- شرایط کلیدی تحلیل ----
        reasons = []
        probability = 0.0

        # --- قدرت خریدار حقیقی ---
        buy_count_i = get_numeric_value(latest, "buy_count_i")
        sell_count_i = get_numeric_value(latest, "sell_count_i")
        real_buy_power_ratio = 0.0

        if buy_count_i > 0 and sell_count_i > 0:
            buy_per_trade = get_numeric_value(latest, "buy_i_volume") / buy_count_i
            sell_per_trade = get_numeric_value(latest, "sell_i_volume") / sell_count_i
            if sell_per_trade > 0:
                real_buy_power_ratio = buy_per_trade / sell_per_trade

        if real_buy_power_ratio > 2.0:
            reasons.append("قدرت خریدار حقیقی بالا")
            probability += 20

        # --- افزایش حجم معاملات ---
        vol_today = get_numeric_value(latest, "volume")
        volume_z = calculate_z_score(hist_df["volume"])
        if volume_z is not None and volume_z > 2.0:
            reasons.append("افزایش حجم غیرعادی (Z-Score > 2)")
            probability += 20

        # --- قیمت مثبت ---
        plp = get_numeric_value(latest, "plp")
        if plp > 0:
            reasons.append("قیمت پایانی مثبت")
            probability += 10

        # --- RSI ---
        rsi_today = get_numeric_value(latest, "RSI_tech")
        prev_rsi = get_numeric_value(merged_df.iloc[-2], "RSI_tech")
        if rsi_today > prev_rsi and rsi_today < 70:
            reasons.append("RSI در حال رشد")
            probability += 10

        # --- MACD ---
        macd_t = get_numeric_value(latest, "MACD_tech")
        macd_s = get_numeric_value(latest, "MACD_Signal_tech")
        prev_macd_t = get_numeric_value(merged_df.iloc[-2], "MACD_tech")
        prev_macd_s = get_numeric_value(merged_df.iloc[-2], "MACD_Signal_tech")
        if macd_t > macd_s and prev_macd_t <= prev_macd_s:
            reasons.append("تقاطع صعودی MACD")
            probability += 15

        # --- Power_Thrust_Signal ---
        is_volume_spike = volume_z is not None and volume_z > 2.0
        is_strong_buy_power = real_buy_power_ratio > 2.5
        is_positive_close = plp > 0

        if is_volume_spike and is_strong_buy_power and is_positive_close:
            reasons.append("Power_Thrust_Signal (ورود قدرتمند)")
            probability += 30

        probability = min(probability, 100)

        if probability >= 35 and reasons:
            candidate = {
                "symbol_id": symbol_id,
                "symbol_name": symbol_name,
                "jdate": today_jdate_str,
                "current_price": close_price,
                "real_buyer_power_ratio": real_buy_power_ratio,
                "matched_filters": json.dumps(reasons, ensure_ascii=False),
                "probability_percent": probability,
                "timestamp": datetime.now(),
                "reason": ", ".join(reasons),
                "group_type": "general",
            }
            candidates.append(candidate)

    # ذخیره نتایج
    saved = 0
    for c in sorted(candidates, key=lambda x: x["probability_percent"], reverse=True)[:20]:
        db.session.add(PotentialBuyQueueResult(**c))
        saved += 1

    try:
        db.session.commit()
        current_app.logger.info(f"✅ Saved {saved} Power_Thrust results for {today_jdate_str}")
        return True, f"Saved {saved} results"
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"DB commit failed: {e}", exc_info=True)
        return False, str(e)


# ---------------------------------------------------------------------------------
# 🆕 تابع جدید برای بازیابی نتایج ذخیره‌شده (رفع خطای AttributeError)
# ---------------------------------------------------------------------------------
def get_potential_buy_queues_data(filters: Optional[Dict] = None) -> List[Dict]:
    """
    بازیابی نتایج تحلیل Power_Thrust ذخیره‌شده از جدول PotentialBuyQueueResult.
    """
    if filters is None:
        filters = {}
        
    limit = filters.get('limit', 20)
    
    try:
        query = PotentialBuyQueueResult.query
        
        # مرتب‌سازی بر اساس درصد احتمال (از بالا به پایین)
        query = query.order_by(desc(PotentialBuyQueueResult.probability_percent))
        query = query.limit(limit)
        
        results = query.all()
        
        # تبدیل نتایج به لیست دیکشنری برای پاسخ API
        result_list = []
        for res in results:
            data = {}
            # تبدیل آبجکت SQLAlchemy به دیکشنری
            for col in res.__table__.columns:
                data[col.name] = getattr(res, col.name)
            
            # هندل کردن فیلدهای خاص برای فرمت API
            if 'timestamp' in data and data['timestamp']:
                data['timestamp'] = data['timestamp'].isoformat()
            if 'matched_filters' in data and data['matched_filters']:
                # تبدیل رشته JSON ذخیره‌شده به آبجکت Python
                try:
                    data['matched_filters'] = json.loads(data['matched_filters'])
                except json.JSONDecodeError:
                    data['matched_filters'] = []
                
            result_list.append(data)
            
        logger.info(f"✅ Retrieved {len(result_list)} potential buy queue results.")
        return result_list
        
    except Exception as e:
        logger.error(f"❌ Error retrieving potential buy queues data: {e}", exc_info=True)
        return []


def get_defined_filters():
    """فهرست فیلترهای فعال"""
    return [
        {"name": "قدرت خریدار حقیقی بالا", "category": "تابلویی"},
        {"name": "افزایش حجم غیرعادی (Z-Score > 2)", "category": "حجم"},
        {"name": "قیمت پایانی مثبت", "category": "قیمت"},
        {"name": "RSI در حال رشد", "category": "اندیکاتور"},
        {"name": "تقاطع صعودی MACD", "category": "اندیکاتور"},
        {"name": "Power_Thrust_Signal (ورود قدرتمند)", "category": "سیگنال ترکیبی"},
    ]
