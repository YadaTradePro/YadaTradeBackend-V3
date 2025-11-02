# -*- coding: utf-8 -*-
# services/market_analysis_service.py

import logging
from datetime import datetime, timedelta
import jdatetime
from sqlalchemy.exc import SQLAlchemyError
import json
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np 

from models import (
    HistoricalData,
    ComprehensiveSymbolData,
    AggregatedPerformance,
    WeeklyWatchlistResult,
    DailySectorPerformance,
    DailyIndexData, # 💡 اضافه شد
)

# Import Jinja2 for templating
from jinja2 import Environment, FileSystemLoader, Template

# Import necessary modules
from extensions import db
# ❌ حذف شد: دیگر نیازی به تابع خارجی نیست، داده‌ها از دیتابیس خوانده می‌شوند
# from services.iran_market_data import fetch_iran_market_indices 

# تنظیمات لاگینگ
logger = logging.getLogger(__name__)


daily_template = None
weekly_template = None

try:
    template_loader = FileSystemLoader('services/templates')
    template_env = Environment(loader=template_loader)
    daily_template = template_env.get_template('daily_summary.j2')
    weekly_template = template_env.get_template('weekly_summary.j2')
    logger.info("✅ قالب‌های Jinja2 با موفقیت از فایل‌ها بارگذاری شدند.")
except Exception as e:
    logger.error(f"❌ خطای بارگذاری قالب‌های Jinja2: {e}. استفاده از قالب‌های درون‌حافظه‌ای.", exc_info=True)

    # Fallback به قالب‌های درون حافظه - قالب روزانه به طور کامل بازنویسی شده است
    DAILY_TEMPLATE_STRING = """
📊 **تحلیل روزانه بازار | {{ jdate }}**

**نمای کلی بازار:**
- **شاخص کل:** `{{ sentiment.total_index.value }}` ({{ sentiment.total_index.status }})
- **شاخص هم‌وزن:** `{{ sentiment.equal_weighted_index.value }}` ({{ sentiment.equal_weighted_index.status }})
- **ارزش معاملات خرد:** **{{ '%.1f'|format(sentiment.trade_value.retail / 1e10) }}** هزار میلیارد تومان (همت)

---

**نبض بازار (سنتیمنت):**
- **جریان پول حقیقی:** {{ sentiment.money_flow.status_text }} به ارزش **{{ '%.2f'|format(sentiment.money_flow.net_value_billion_toman) }}** میلیارد تومان
- **قدرت خریدار حقیقی:** سرانه خرید **{{ '{:,.0f}'.format(sentiment.per_capita.buy) }}** م.تومان در مقابل سرانه فروش **{{ '{:,.0f}'.format(sentiment.per_capita.sell) }}** م.تومان. ({{ sentiment.per_capita.status_text }})
- **وضعیت کلی:** **{{ sentiment.market_breadth.positive_symbols }}** نماد مثبت در برابر **{{ sentiment.market_breadth.negative_symbols }}** نماد منفی.

---

**خلاصه عملکرد صنایع برتر (Top 3):**
{% if sector_summary %}
{% for sector in sector_summary %}
- **{{ loop.index }}. {{ sector.sector_name }}**: **{{ sector.flow_status }}** پول با ارزش **{{ sector.flow_value_text }}**
{% endfor %}
{% else %}
- **توجه:** اطلاعات عملکرد صنایع برتر برای امروز در دسترس نیست.
{% endif %}

---

**نمادهای منتخب روز:**
{% if all_symbols %}
{{ symbols_text }}
{% else %}
- امروز نماد جدیدی در لیست منتخب شناسایی نشد.
{% endif %}
"""
    # قالب هفتگی برای هماهنگی، کمی خلاصه‌تر می‌شود
    WEEKLY_TEMPLATE_STRING = """
📅 **تحلیل هفتگی بازار | {{ jdate }}**

**عملکرد کلی هفته:**
- **جریان پول حقیقی:** در مجموع هفته، {{ smart_money_flow_text }}.
- **عملکرد سبد منتخب:** نرخ برد سیگنال‌ها **{{ '%.1f'|format(indices_data.win_rate|default(0)) }}%** بوده است.

---

**خلاصه عملکرد صنایع برتر (Top 3):**
{% if sector_summary %}
{% for sector in sector_summary %}
- **{{ loop.index }}. {{ sector.sector_name }}**: **{{ sector.flow_status }}** پول با ارزش **{{ sector.flow_value_text }}**
{% endfor %}
{% else %}
- **توجه:** اطلاعات عملکرد صنایع برتر برای هفته در دسترس نیست.
{% endif %}

---

**ارزیابی سیگنال‌های هفته:**
{% if all_symbols %}
{{ symbols_text }}
{% else %}
- در این هفته سیگنال جدیدی در لیست منتخب وجود نداشت.
{% endif %}
"""
    daily_template = Template(DAILY_TEMPLATE_STRING)
    weekly_template = Template(WEEKLY_TEMPLATE_STRING)
    logger.info("✅ قالب‌های Jinja2 با موفقیت از رشته‌های درون‌حافظه‌ای بارگذاری شدند.")


# -----------------------------------------------------------------------------
# توابع کمکی (Helper Functions) - بدون تغییرات عمده
# -----------------------------------------------------------------------------

def _safe_dataframe_from_orm(rows: List[Any], cols: List[str]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=cols)
    data = [{c: getattr(r, c, None) for c in cols} for r in rows]
    return pd.DataFrame(data)

def _choose_price_col(df: pd.DataFrame) -> str:
    for c in ('close', 'final'):
        if c in df.columns and df[c].notna().any() and df[c].mean() > 0:
            return c
    df['dummy_price'] = 1000 
    return 'dummy_price'

def _get_day_type() -> str:
    j_today = jdatetime.date.today()
    day_name = j_today.strftime('%A') 
    if day_name in ('Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday'):
        return 'daily'
    if day_name in ('Thursday', 'Friday'):
        return 'weekly'
    return 'no_analysis_day'

def _calculate_pnl(entry_price: float, exit_price: Optional[float]) -> Optional[float]:
    if not entry_price or entry_price == 0 or exit_price is None:
        return None
    return round(((exit_price - entry_price) / entry_price) * 100, 2)

def _get_formatted_symbols_text(symbols: List[Any], is_weekly: bool) -> str:
    if not symbols:
        return ""
    text_parts = []
    for symbol_data in symbols:
        symbol_name = symbol_data.symbol_name
        reasons = getattr(symbol_data, 'reasons', '{}')
        if not isinstance(reasons, str):
            reasons = json.dumps(reasons, ensure_ascii=False)

        if not is_weekly:
            daily_change = getattr(symbol_data, 'daily_change_percent', None)
            status_text = ""
            if daily_change is not None:
                status_text = f" (رشد **{daily_change:.2f}%**)" if daily_change > 0 else f" (کاهش **{abs(daily_change):.2f}%**)"
            text_parts.append(f"- **{symbol_name}**: {reasons}{status_text}")
        else:
            pnl_percent = getattr(symbol_data, 'profit_loss_percentage', None)
            status_text = "(فعال)"
            if pnl_percent is not None:
                status_text = f"(**{pnl_percent:.2f}%** سود)" if pnl_percent > 0 else f"(**{abs(pnl_percent):.2f}%** زیان)"
            text_parts.append(f"- **{symbol_name}**: {reasons} {status_text}")
    return "\n".join(text_parts)


# --------------------------------
# تابع جدید برای خلاصه صنایع (با آرگومان تاریخ)
# --------------------------------

def _get_top_sectors_summary(db_session: db.session, analysis_jdate_str: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    اطلاعات صنایع برتر را برای تاریخ مشخص یا بازه‌ی آخر هفته برمی‌گرداند.
    📅 اگر روز اجرای تحلیل در ایام هفته (شنبه تا چهارشنبه) باشد:
        ➤ فقط داده همان روز بازگردانده می‌شود.
    📆 اگر روز اجرا پنج‌شنبه یا جمعه باشد:
        ➤ داده‌های تجمیعی از شنبه تا چهارشنبه همان هفته محاسبه می‌شود.
    """

    try:
        day_type = _get_day_type()
        logger.info(f"🧭 نوع روز فعلی برای خلاصه صنایع: {day_type}")

        if day_type == 'daily':
            # ----------------------------
            # 🔹 حالت روزانه: فقط داده همان روز
            # ----------------------------
            top_sectors = (
                DailySectorPerformance.query
                .filter_by(jdate=analysis_jdate_str)
                .order_by(DailySectorPerformance.rank.asc())
                .limit(limit)
                .all()
            )

            if not top_sectors:
                logger.info(f"ℹ️ داده‌ای برای عملکرد صنایع در تاریخ {analysis_jdate_str} وجود ندارد (لیست خالی).")
                return []

            json_sectors_list = []
            for sector in top_sectors:
                net_flow_billion = float(sector.net_money_flow or 0) / 1e10
                sector_data = {
                    'sector_name': sector.sector_name,
                    'net_money_flow_billion': round(net_flow_billion, 2),
                    'flow_status': 'ورود' if net_flow_billion > 0 else ('خروج' if net_flow_billion < 0 else 'خنثی'),
                    'flow_value_text': f"{abs(net_flow_billion):.2f} م.تومان",
                }
                json_sectors_list.append(sector_data)

            logger.info(f"✅ {len(json_sectors_list)} رکورد صنعت برای تاریخ {analysis_jdate_str} بازیابی شد.")
            return json_sectors_list

        elif day_type in ('weekly', 'no_analysis_day'):
            # ----------------------------
            # 🔹 حالت هفتگی: جمع ۵ روز آخر هفته معاملاتی (شنبه تا چهارشنبه)
            # ----------------------------

            # پیدا کردن آخرین روز معاملاتی
            last_trading_day = (
                db_session.query(DailySectorPerformance.jdate)
                .distinct()
                .order_by(DailySectorPerformance.jdate.desc())
                .first()
            )

            if not last_trading_day:
                logger.warning("❌ هیچ تاریخ معاملاتی در جدول DailySectorPerformance یافت نشد.")
                return []

            last_jdate = last_trading_day[0]

            # محاسبه شروع هفته (۴ روز قبل)
            try:
                jd = jdatetime.datetime.strptime(last_jdate, "%Y-%m-%d").date()
            except Exception:
                # پشتیبانی از فرمت YYYY/MM/DD
                jd = jdatetime.datetime.strptime(last_jdate.replace('/', '-'), "%Y-%m-%d").date()

            start_jdate = (jd - jdatetime.timedelta(days=4)).strftime("%Y-%m-%d")

            logger.info(f"📅 محدوده هفتگی صنایع از {start_jdate} تا {last_jdate}")

            # واکشی داده‌های بین شنبه تا چهارشنبه همان هفته
            week_records = (
                DailySectorPerformance.query
                .filter(DailySectorPerformance.jdate >= start_jdate)
                .filter(DailySectorPerformance.jdate <= last_jdate)
                .all()
            )

            if not week_records:
                logger.warning(f"⚠️ هیچ داده صنعتی بین {start_jdate} تا {last_jdate} یافت نشد.")
                return []

            # تجمیع پول صنایع
            df_week = pd.DataFrame([{
                'sector_name': r.sector_name,
                'net_money_flow': float(r.net_money_flow or 0)
            } for r in week_records])

            df_agg = (
                df_week.groupby('sector_name', as_index=False)
                .agg({'net_money_flow': 'sum'})
                .sort_values(by='net_money_flow', ascending=False)
                .head(limit)
            )

            result = []
            for _, row in df_agg.iterrows():
                net_bil = row['net_money_flow'] / 1e10
                result.append({
                    'sector_name': row['sector_name'],
                    'net_money_flow_billion': round(net_bil, 2),
                    'flow_status': 'ورود' if net_bil > 0 else ('خروج' if net_bil < 0 else 'خنثی'),
                    'flow_value_text': f"{abs(net_bil):.2f} م.تومان",
                })

            logger.info(f"✅ خلاصه هفتگی {len(result)} صنعت برتر از {start_jdate} تا {last_jdate} محاسبه شد.")
            return result

        else:
            logger.warning("⚠️ نوع روز نامشخص بود، لیست خالی بازگردانده شد.")
            return []

    except Exception as e:
        logger.error(f"❌ خطا در تولید خلاصه صنایع: {e}", exc_info=True)
        return []

#تابع نگاشت (Mapping)
def _map_watchlist_result_to_dict(result_obj: 'SignalsPerformance') -> Dict[str, Any]:
    """
    یک آبجکت ORM (احتمالاً SignalsPerformance) را به دیکشنری استاندارد تبدیل می‌کند.
    """
    # توجه: daily_change_percent یک فیلد موقتی است که با setattr اضافه شده است (برای گزارش روزانه).
    daily_change = getattr(result_obj, 'daily_change_percent', None)
    
    # 💡 تمام ستون‌های لازم را به صورت صریح از آبجکت استخراج می‌کنیم
    return {
        'signal_unique_id': result_obj.signal_unique_id,
        'symbol_id': result_obj.symbol_id,
        'symbol_name': result_obj.symbol_name,
        'entry_price': float(result_obj.entry_price) if result_obj.entry_price is not None else None,
        'jentry_date': result_obj.jentry_date,
        'status': result_obj.status,
        'daily_change_percent': float(daily_change) if daily_change is not None else None,
        # افزودن سایر فیلدهای مورد نیاز فرانت‌اند
        'outlook': result_obj.outlook,
        'reason': result_obj.reason,
        'exit_price': float(result_obj.exit_price) if result_obj.exit_price is not None else None,
        'jexit_date': result_obj.jexit_date,
        'profit_loss_percentage': float(result_obj.profit_loss_percentage) if result_obj.profit_loss_percentage is not None else None,
        'probability_percent': float(result_obj.profit_loss_percentage) if result_obj.profit_loss_percentage is not None else None,
    }


# -----------------------------------------------------------------------------
# تابع اصلی جدید برای تحلیل سنتیمنت بازار
# -----------------------------------------------------------------------------

def _analyze_market_sentiment(df: pd.DataFrame, indices_data_from_db: Dict) -> Dict: # 💡 تغییر نام آرگومان
    """
    DataFrame داده‌های روزانه را تحلیل کرده و یک دیکشنری جامع از معیارهای سنتیمنت بازار برمی‌گرداند.
    محاسبات جریان پول، سرانه و ارزش معاملات فقط برای نمادهای بورس و فرابورس انجام می‌شود.
    """
    
    # 🔑 تابع کمکی داخلی برای تبدیل ایمن درصد به عدد (رفع خطای TypeError)
    def _get_safe_percent(index_data: Dict, key: str = 'percent_change') -> float: # 💡 تغییر کلید به percent_change
        """مقدار درصد را به float تبدیل می‌کند، اگر None یا نامعتبر بود، 0.0 برمی‌گرداند."""
        value = index_data.get(key)
        try:
            # اگر value، None نباشد، آن را به float تبدیل کن
            if value is not None:
                return float(value)
            return 0.0
        except (TypeError, ValueError):
            # اگر تبدیل موفق نبود، 0.0 برگردان
            return 0.0

    sentiment_data = {}
    
    # 0. آماده‌سازی DataFrame و فیلتر کردن نمادها
    
    # 0.1 تبدیل ستون‌های عددی
    numeric_cols = [
        'value', 'volume', 'plp',
        'buy_i_volume', 'sell_i_volume', 'buy_count_i', 'sell_count_i',
    ]
    for col in numeric_cols:
        if col in df.columns:
            # اطمینان از تبدیل به نوع عددی استاندارد برای جلوگیری از خطای NumPy در ادامه
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(float) 

    
    # بازارهای مورد نظر برای تحلیل سنتیمنت (بازارهای اصلی)
    bourse_market_types = ['بورس', 'فرابورس'] 
    
    # بازیابی symbol_id های نمادهای بورس و فرابورس
    valid_symbol_ids = db.session.query(ComprehensiveSymbolData.symbol_id).filter(
        ComprehensiveSymbolData.market_type.in_(bourse_market_types)
    ).all()
    
    # تبدیل به مجموعه برای جستجوی سریعتر
    valid_ids_set = {id_[0] for id_ in valid_symbol_ids}

    # فیلتر کردن DataFrame داده‌های روزانه
    df_filtered = df[df['symbol_id'].isin(valid_ids_set)].copy() # 💡 استفاده از .copy() برای جلوگیری از SettingWithCopyWarning
    
    if df_filtered.empty:
        logger.warning("❌ هیچ نماد بورس/فرابورسی برای تحلیل سنتیمنت یافت نشد.")
        # اگر داده‌ای برای تحلیل اصلی نداریم، فقط شاخص‌ها را برمی‌گردانیم
        sentiment_data.update({
             'trade_value': {'retail': 0},
             'money_flow': {'net_value_billion_toman': 0, 'status_text': "اطلاعات جریان پول برای بازارهای اصلی موجود نیست."},
             'per_capita': {'buy': 0, 'sell': 0, 'status_text': "اطلاعات سرانه برای بازارهای اصلی موجود نیست."},
             'market_breadth': {'positive_symbols': 0, 'negative_symbols': 0},
        })
        # 💡 ۱. تحلیل شاخص‌ها: شاخص‌ها را از indices_data_from_db اضافه می‌کنیم حتی اگر بقیه داده‌ها نباشند
        total_index_data = indices_data_from_db.get('Total_Index', {})
        total_percent = _get_safe_percent(total_index_data)

        sentiment_data['total_index'] = {
            'value': total_index_data.get('value', 'N/A'),
            'status': 'صعودی' if total_percent > 0 else ('نزولی' if total_percent < 0 else 'بدون تغییر')
        }

        equal_weighted_index_data = indices_data_from_db.get('Equal_Weighted_Index', {})
        equal_percent = _get_safe_percent(equal_weighted_index_data)

        sentiment_data['equal_weighted_index'] = {
            'value': equal_weighted_index_data.get('value', 'N/A'),
            'status': 'صعودی' if equal_percent > 0 else ('نزولی' if equal_percent < 0 else 'بدون تغییر')
        }

        return sentiment_data
    
    # ۱. تحلیل شاخص‌ها (💡 تغییر: استفاده از داده‌های DB)
    total_index = indices_data_from_db.get('Total_Index', {})
    # 🔑 استفاده از تابع کمکی برای ایمن‌سازی
    total_percent = _get_safe_percent(total_index)
    
    sentiment_data['total_index'] = {
        'value': total_index.get('value', 'N/A'),
        # مقایسه با استفاده از مقدار ایمن شده total_percent
        'status': 'صعودی' if total_percent > 0 else ('نزولی' if total_percent < 0 else 'بدون تغییر')
    }
    
    equal_weighted_index = indices_data_from_db.get('Equal_Weighted_Index', {})
    # 🔑 استفاده از تابع کمکی برای ایمن‌سازی
    equal_percent = _get_safe_percent(equal_weighted_index)
    
    sentiment_data['equal_weighted_index'] = {
        'value': equal_weighted_index.get('value', 'N/A'),
        # مقایسه با استفاده از مقدار ایمن شده equal_percent
        'status': 'صعودی' if equal_percent > 0 else ('نزولی' if equal_percent < 0 else 'بدون تغییر')
    }

    # ۲. تحلیل ارزش معاملات (فقط بورس و فرابورس)
    # total_trade_value = df_filtered['value'].sum() # دیگر نیازی به نمایش ارزش کل نیست
    retail_trade_value = df_filtered[df_filtered['volume'] > 1]['value'].sum()
    sentiment_data['trade_value'] = {'retail': float(retail_trade_value)} 

    # ۳. تحلیل جریان پول حقیقی (فقط بورس و فرابورس)
    price_col = _choose_price_col(df_filtered) # 💡 استفاده از df_filtered
    df_filtered['net_real_value'] = (df_filtered['buy_i_volume'] - df_filtered['sell_i_volume']) * df_filtered[price_col]
    net_money_flow_value = df_filtered['net_real_value'].sum()
    
    status_text = "جریان پول حقیقی در **بورس و فرابورس** تقریباً **خنثی** بود"
    if net_money_flow_value > 1e10: # بیش از ۱ میلیارد تومان
        status_text = "**ورود پول حقیقی** در **بورس و فرابورس** را شاهد بودیم" # 💡 ویرایش تایتل
    elif net_money_flow_value < -1e10:
        status_text = "**خروج پول حقیقی** از **بورس و فرابورس** را شاهد بودیم" # 💡 ویرایش تایتل
    
    net_value_billion_toman = float(net_money_flow_value) / 1e10

    sentiment_data['money_flow'] = {
        'net_value_billion_toman': net_value_billion_toman,
        'status_text': status_text,
    }

    # ۴. تحلیل سرانه خرید و فروش (فقط بورس و فرابورس)
    total_buy_value_i = (df_filtered['buy_i_volume'] * df_filtered[price_col]).sum()
    total_sell_value_i = (df_filtered['sell_i_volume'] * df_filtered[price_col]).sum()
    total_buyers_i = df_filtered['buy_count_i'].sum()
    total_sellers_i = df_filtered['sell_count_i'].sum()
    
    per_capita_buy = (total_buy_value_i / total_buyers_i / 1e7) if total_buyers_i > 0 else 0
    per_capita_sell = (total_sell_value_i / total_sellers_i / 1e7) if total_sellers_i > 0 else 0

    per_capita_status = "قدرت خریداران و فروشندگان متعادل بود"
    if per_capita_buy > per_capita_sell * 1.2:
        per_capita_status = "قدرت **خریداران** بیشتر بود"
    elif per_capita_sell > per_capita_buy * 1.2:
        per_capita_status = "قدرت **فروشندگان** بیشتر بود"
        
    sentiment_data['per_capita'] = {
        'buy': float(per_capita_buy), 
        'sell': float(per_capita_sell), 
        'status_text': per_capita_status,
    }

    # ۵. تحلیل وضعیت کلی بازار (مثبت/منفی) (فقط بورس و فرابورس)
    positive_symbols = len(df_filtered[df_filtered['plp'] > 0])
    negative_symbols = len(df_filtered[df_filtered['plp'] < 0])
    sentiment_data['market_breadth'] = {
        'positive_symbols': int(positive_symbols), 
        'negative_symbols': int(negative_symbols), 
    }
    
    return sentiment_data


# -----------------------------------------------------------------------------
# توابع اصلی تحلیل (بازنویسی شده)
# -----------------------------------------------------------------------------

def _get_daily_indices(jdate_str: str) -> Dict[str, Any]:
    """
    داده‌های شاخص روزانه را از دیتابیس بازیابی کرده و در فرمت دیکشنری مورد نیاز برمی‌گرداند.
    """
    indices_data = {}
    try:
        # 💡 بازیابی داده‌ها از مدل جدید DailyIndexData
        index_records = DailyIndexData.query.filter_by(jdate=jdate_str).all()
        
        if not index_records:
            logger.warning(f"❌ هیچ داده شاخصی برای روز {jdate_str} در دیتابیس یافت نشد.")
            return {
                'Total_Index': {'value': 'N/A', 'percent_change': 0.0},
                'Equal_Weighted_Index': {'value': 'N/A', 'percent_change': 0.0}
            }
            
        for record in index_records:
            indices_data[record.index_type] = {
                # 💡 استفاده از percent_change به جای percent
                'value': float(record.value), 
                'percent_change': float(record.percent_change)
            }
            
        # 💡 اطمینان از وجود دو شاخص کلیدی
        if 'Total_Index' not in indices_data:
            indices_data['Total_Index'] = {'value': 'N/A', 'percent_change': 0.0}
        if 'Equal_Weighted_Index' not in indices_data:
            indices_data['Equal_Weighted_Index'] = {'value': 'N/A', 'percent_change': 0.0}
            
    except Exception as e:
        logger.error(f"❌ خطای بازیابی داده‌های شاخص روزانه از دیتابیس: {e}", exc_info=True)
        # در صورت خطا، مقادیر پیش‌فرض برگردانده می‌شود
        return {
            'Total_Index': {'value': 'N/A', 'percent_change': 0.0},
            'Equal_Weighted_Index': {'value': 'N/A', 'percent_change': 0.0}
        }
        
    return indices_data


def _generate_daily_summary() -> Dict[str, Any]: 
    logger.info("شروع فرآیند تولید تحلیل جامع روزانه بازار...")
    try:
        # 1. پیدا کردن آخرین روز معاملاتی (برای تحلیل امروز)
        last_trading_day = db.session.query(HistoricalData.jdate).distinct().order_by(HistoricalData.jdate.desc()).first()
        if not last_trading_day:
            return {"status": "error", "message": "❌ هیچ داده‌ای برای تحلیل روزانه موجود نیست."}
        
        analysis_date_jdate_str = last_trading_day[0]
        logger.info(f"تحلیل برای آخرین روز معاملاتی ({analysis_date_jdate_str}) انجام می‌شود.")

        # 2. بازیابی داده‌های تاریخی روز جاری
        required_cols = [
            # 💡 ستون PLP (درصد تغییر روزانه) اضافه شد
            'symbol_id', 'value', 'volume', 'close', 'final', 'plp',
            'buy_i_volume', 'sell_i_volume', 'buy_count_i', 'sell_count_i'
        ]
        historical_rows = HistoricalData.query.with_entities(
            *[getattr(HistoricalData, col) for col in required_cols]
        ).filter(HistoricalData.jdate == analysis_date_jdate_str).all()

        if not historical_rows:
            return {"status": "error", "message": f"❌ داده‌های تاریخی برای روز {analysis_date_jdate_str} یافت نشد."}

        df = _safe_dataframe_from_orm(historical_rows, required_cols)

        # 3. بازیابی و تحلیل سنتیمنت بازار
        # 💡 تغییر: استفاده از تابع جدید برای بازیابی شاخص‌ها از دیتابیس
        indices_data = _get_daily_indices(analysis_date_jdate_str) 
        sentiment_analysis_result = _analyze_market_sentiment(df, indices_data)
        
        # 4. پیدا کردن روز معاملاتی قبل (برای محاسبه pnl روزانه)
        # این بخش دیگر برای محاسبه مستقیم درصد تغییر لازم نیست اما حفظ می‌شود
        prev_trading_day = db.session.query(HistoricalData.jdate).distinct().filter(
            HistoricalData.jdate < analysis_date_jdate_str
        ).order_by(HistoricalData.jdate.desc()).first()
        prev_jdate_str = prev_trading_day[0] if prev_trading_day else None

        # 5. بازیابی نمادهای واچ‌لیست فعال (نمادهایی که هنوز بسته نشده‌اند)
        weekly_watchlist_results = WeeklyWatchlistResult.query.filter(
            WeeklyWatchlistResult.exit_price.is_(None) 
        ).all()
        
        # 6. محاسبه/بازیابی تغییرات روزانه برای نمادهای فعال (اصلاح شده)
        # 💡 از ستون 'plp' در HistoricalData روز جاری استفاده می‌کنیم.
        for symbol in weekly_watchlist_results:
            # داده‌های روز جاری (شامل ستون 'plp')
            today_data_series = df[df['symbol_id'] == symbol.symbol_id]
            today_data = today_data_series.iloc[0] if not today_data_series.empty else None
            
            daily_change = None 
            
            if today_data is not None and 'plp' in today_data:
                # 💡 مستقیماً مقدار 'plp' (درصد تغییر روزانه) را از داده‌های امروز می‌خوانیم
                daily_change = today_data['plp']
            
            # مقدار موقتی را به شیء ORM اضافه می‌کنیم
            setattr(symbol, 'daily_change_percent', daily_change)
        
        # 7. خلاصه صنایع برتر
        # 💡 اصلاح: ارسال analysis_date_jdate_str به تابع تحلیل صنایع
        sector_summary_list = _get_top_sectors_summary(db.session, analysis_date_jdate_str, limit=5) 
        
        # 8. تبدیل لیست آبجکت‌های ORM واچ‌لیست به لیست دیکشنری‌ها 
        # 🚨 رفع باگ انتقال داده: اطمینان از انتقال فیلد موقتی
        final_symbols_list = []
        for symbol in weekly_watchlist_results:
            # 1. تبدیل فیلدهای اصلی دیتابیس به دیکشنری
            symbol_dict = _map_watchlist_result_to_dict(symbol) 
            
            # 2. 💡 اضافه کردن فیلد محاسبه شده موقتی که در حافظه ذخیره شده است
            # این فیلد مستقیماً از شیء ORM قابل دسترسی است
            calculated_change = getattr(symbol, 'daily_change_percent', None)
            if calculated_change is not None:
                symbol_dict['daily_change_percent'] = calculated_change
            
            final_symbols_list.append(symbol_dict)
        
        # 9. ایجاد خروجی نهایی
        data_for_template = {
            'jdate': analysis_date_jdate_str,
            'sentiment': sentiment_analysis_result,
            'sector_summary': sector_summary_list, 
            'all_symbols': final_symbols_list, # 👈 استفاده از لیست تبدیل شده
            'symbols_text': _get_formatted_symbols_text(weekly_watchlist_results, is_weekly=False),
            # 💡 status باید در تابع اصلی نهایی‌سازی شود
            'status': 'success'
        }
        
        return data_for_template
    
    except Exception as e:
        logger.error(f"❌ خطای ناشناخته در تولید تحلیل روزانه: {e}", exc_info=True)
        return {"status": "error", "message": "❌ متأسفانه به دلیل خطای فنی، امکان تولید تحلیل روزانه وجود ندارد."}


# -----------------------------------------------------------------------------
# تابع تحلیل هفتگی (اصلاح شده برای خروجی Dict[str, Any] و sector_summary)
# -----------------------------------------------------------------------------

def _generate_weekly_summary() -> Dict[str, Any]: # 💡 تغییر نوع بازگشتی
    logger.info("شروع فرآیند تولید تحلیل هفتگی بازار...")
    try:
        # 0. تعریف تاریخ گزارش نهایی (برای فیلد 'jdate' در خروجی)
        last_trading_day_record = db.session.query(HistoricalData.jdate).distinct().order_by(HistoricalData.jdate.desc()).first()
        # 🚨 تعریف متغیر: analysis_date_jdate_str
        analysis_date_jdate_str = last_trading_day_record[0] if last_trading_day_record else jdatetime.date.today().strftime('%Y-%m-%d')

        # 1. پیدا کردن ۵ روز معاملاتی آخر برای تحلیل هفتگی
        last_5_days_query = db.session.query(HistoricalData.jdate).distinct().order_by(HistoricalData.jdate.desc()).limit(5)
        last_5_days = [d[0] for d in last_5_days_query.all()]
        if not last_5_days:
            # 💡 در صورت نبود داده تاریخی، بازگشت موفق با تاریخ امروز
            return {"status": "info", "jdate": analysis_date_jdate_str, "message": "❌ داده کافی برای تحلیل هفتگی وجود ندارد. گزارش خالی منتشر می‌شود."}

        start_date_j = min(last_5_days)
        
        # 2. بازیابی آمار تجمیعی (مثلاً Win Rate)
        aggregated_data = AggregatedPerformance.query.filter(
            AggregatedPerformance.period_type == 'weekly'
        ).order_by(AggregatedPerformance.created_at.desc()).first()
        indices_for_template = {'win_rate': float(getattr(aggregated_data, 'win_rate', 0))}
        
        # 3. محاسبه جریان پول هفتگی و **سنتیمنت تجمیعی**
        historical_rows = HistoricalData.query.filter(HistoricalData.jdate.in_(last_5_days)).all()
        # 💡 استفاده از تابع کمکی برای ساخت DataFrame امن‌تر است، اما فرض می‌کنیم DataFrame موجود است
        df = pd.DataFrame([row.__dict__ for row in historical_rows])
        
        # 💡 آماده‌سازی ستون‌ها در صورت خالی بودن
        for col in ['buy_i_volume', 'sell_i_volume', 'buy_count_i', 'sell_count_i', 'plp', 'value']:
            if col not in df.columns:
                df[col] = 0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(float)
        
        total_net_real_money_flow = 0
        
        # 🚨 بلوک جدید: محاسبه سنتیمنت تجمیعی هفتگی
        sentiment = {
            'total_index': {'value': None, 'status': None, 'weekly_change_percent': 0.0},
            'equal_weighted_index': {'value': None, 'status': None, 'weekly_change_percent': 0.0},
            'trade_value': {'retail': 0.0},
            'money_flow': {'net_value_billion_toman': 0.0, 'status_text': 'فاقد اطلاعات'},
            'per_capita': {'buy': 0.0, 'sell': 0.0, 'status_text': 'فاقد اطلاعات'},
            'market_breadth': {'positive_symbols': 0, 'negative_symbols': 0}
        }
        
        if not df.empty:
            price_col = _choose_price_col(df)
            
            # --- الف. جریان پول هفتگی ---
            df['net_real_value_flow'] = (df['buy_i_volume'].fillna(0) - df['sell_i_volume'].fillna(0)) * df[price_col].fillna(0)
            total_net_real_money_flow = float(df['net_real_value_flow'].sum())
            
            smart_money_text = f"شاهد {'ورود' if total_net_real_money_flow > 0 else 'خروج'} پول حقیقی به ارزش تقریبی **{abs(total_net_real_money_flow) / 1e10:.2f}** میلیارد تومان بودیم"
            sentiment['money_flow']['net_value_billion_toman'] = total_net_real_money_flow / 1e10
            sentiment['money_flow']['status_text'] = smart_money_text

            # --- ب. ارزش معاملات خرد هفتگی ---
            # جمع کل ارزش معاملات برای ۵ روز
            sentiment['trade_value']['retail'] = float(df['value'].sum())
            
            # --- ج. سرانه خرید و فروش هفتگی ---
            total_buy_value_i = (df['buy_i_volume'] * df[price_col]).sum()
            total_sell_value_i = (df['sell_i_volume'] * df[price_col]).sum()
            total_buyers_i = df['buy_count_i'].sum()
            total_sellers_i = df['sell_count_i'].sum()
            
            if total_buyers_i > 0 and total_sellers_i > 0:
                # سرانه خرید و فروش تجمیعی (به میلیون تومان)
                per_capita_buy = (total_buy_value_i / total_buyers_i / 1e7)
                per_capita_sell = (total_sell_value_i / total_sellers_i / 1e7)
                
                per_capita_status = "قدرت خریداران و فروشندگان متعادل بود"
                if per_capita_buy > per_capita_sell * 1.2:
                    per_capita_status = "قدرت خریداران بیشتر بود"
                elif per_capita_sell > per_capita_buy * 1.2:
                    per_capita_status = "قدرت فروشندگان بیشتر بود"
                    
                sentiment['per_capita'] = {
                    'buy': float(per_capita_buy), 
                    'sell': float(per_capita_sell), 
                    'status_text': per_capita_status,
                }
            
            # --- د. وضعیت کلی نمادها (مثبت/منفی) ---
            positive_symbols_count = len(df[df['plp'] > 0])
            negative_symbols_count = len(df[df['plp'] < 0])

            sentiment['market_breadth'] = {
                'positive_symbols': int(positive_symbols_count), 
                'negative_symbols': int(negative_symbols_count), 
            }
        
        # --- ه. عملکرد هفتگی شاخص‌ها ---
        last_index_data_jdate = analysis_date_jdate_str # آخرین روز معاملاتی هفته
        first_index_data_jdate = last_5_days[-1] # اولین روز معاملاتی هفته (قدیمی‌ترین تاریخ در لیست)
        
        last_indices = _get_daily_indices(last_index_data_jdate)
        first_indices = _get_daily_indices(first_index_data_jdate)
        
        # 💡 تابع کمکی برای تبدیل ایمن به float
        def safe_float_convert(value):
            if isinstance(value, (int, float)):
                return value
            if isinstance(value, str) and value.upper() in ('N/A', 'NONE', 'NULL'):
                return 0.0
            try:
                return float(value)
            except (ValueError, TypeError):
                return 0.0
        
        total_index_start = safe_float_convert(first_indices.get('Total_Index', {}).get('value'))
        total_index_end = safe_float_convert(last_indices.get('Total_Index', {}).get('value'))
        equal_index_start = safe_float_convert(first_indices.get('Equal_Weighted_Index', {}).get('value'))
        equal_index_end = safe_float_convert(last_indices.get('Equal_Weighted_Index', {}).get('value'))
        
        def calculate_weekly_change(start, end):
            if start > 0:
                return round(((end - start) / start) * 100, 2)
            return 0.0
            
        total_weekly_change = calculate_weekly_change(total_index_start, total_index_end)
        equal_weekly_change = calculate_weekly_change(equal_index_start, equal_index_end)
        
        def get_status(change):
            return 'صعودی' if change > 0 else ('نزولی' if change < 0 else 'بدون تغییر')

        # پر کردن بخش شاخص‌ها در sentiment
        sentiment['total_index'] = {
            'value': total_index_end, 
            'status': get_status(total_weekly_change),
            'weekly_change_percent': total_weekly_change
        }
        sentiment['equal_weighted_index'] = {
            'value': equal_index_end,
            'status': get_status(equal_weekly_change),
            'weekly_change_percent': equal_weekly_change
        }
        
        # 4. بازیابی نتایج سهم‌ها (نمادهایی که ورود آنها در ۵ روز اخیر بوده است)
        # 🚨 اصلاح برای استفاده از SignalsPerformance: نمادهایی که در بازه هفتگی وارد شده‌اند
        weekly_signal_records = SignalsPerformance.query.filter(
            SignalsPerformance.jentry_date >= start_date_j
        ).all()
        
        # 5. خلاصه صنایع برتر (خروجی JSON List)
        sector_summary_list = _get_top_sectors_summary(db.session, analysis_date_jdate_str, limit=5) 
        
        # 6. ایجاد خروجی نهایی
        data_for_template = {
            'jdate': analysis_date_jdate_str,
            'indices_data': indices_for_template,
            'smart_money_flow_text': smart_money_text, 
            'sector_summary': sector_summary_list, 
            # 🚨 اصلاح: استفاده از لیست رکوردهای SignalsPerformance
            'all_symbols': [_map_watchlist_result_to_dict(r) for r in weekly_signal_records],
            'symbols_text': _get_formatted_symbols_text(weekly_signal_records, is_weekly=True),
            'sentiment': sentiment, 
            'status': 'success'
        }
        
        return data_for_template
        
    except Exception as e:
        logger.error(f"❌ خطای ناشناخته در تولید تحلیل هفتگی: {e}", exc_info=True)
        # 💡 بازگرداندن دیکشنری خطا
        return {"status": "error", "jdate": jdatetime.date.today().strftime('%Y-%m-%d'), "message": "❌ متأسفانه به دلیل خطای فنی، امکان تولید تحلیل هفتگی وجود ندارد."}

# -----------------------------------------------------------------------------
# تابع اصلی سرویس
# -----------------------------------------------------------------------------

def generate_market_summary() -> Dict[str, Any]:
    """
    تابع اصلی سرویس که بسته به روز هفته، تحلیل روزانه یا هفتگی را برمی‌گرداند.
    خروجی یک دیکشنری شامل داده‌های خام و رشته تحلیل نهایی رندر شده (rendered_summary) است.
    """
    logger.info("سرویس تحلیل بازار فراخوانی شد.")
    day_type = _get_day_type()
    
    data = {"status": "error", "message": "نوع تحلیل برای روز جاری قابل تشخیص نیست."}
    template_to_use = None
    
    if day_type == 'daily':
        data = _generate_daily_summary()
        template_to_use = daily_template
    elif day_type == 'weekly':
        data = _generate_weekly_summary()
        template_to_use = weekly_template
    elif day_type == 'no_analysis_day':
        logger.info("امروز پنجشنبه است؛ تحلیل بازار منتشر نمی‌شود.")
        data = {"status": "info", "message": "در روز پنجشنبه، بازار سرمایه فعال نیست و تحلیل روزانه منتشر نمی‌شود."}
    
    # 💡 مرحله نهایی: رندر کردن قالب و افزودن به دیکشنری خروجی
    if template_to_use and data.get("status") in ['success', 'render_error']:
        try:
            # 1. اطمینان از وجود 'symbols_text' برای رندرینگ (به خصوص در weekly)
            if 'symbols_text' not in data and 'all_symbols' in data:
                 data['symbols_text'] = _get_formatted_symbols_text(data['all_symbols'], is_weekly=(day_type == 'weekly'))
                 
            # 2. رندر کردن محتوا و ذخیره در فیلد جدید
            data['rendered_summary'] = template_to_use.render(data)
            data['status'] = 'success' # در صورت موفقیت‌آمیز بودن رندرینگ
            
        except Exception as e:
            logger.error(f"❌ خطای رندر کردن قالب Jinja2 برای تحلیل {day_type}: {e}", exc_info=True)
            # اگر رندر شکست خورد، اطلاعات خام را نگه دارید و یک پیام خطا اضافه کنید
            data['rendered_summary'] = f"❌ خطای رندر کردن گزارش: {e}"
            data['status'] = 'render_error'

    # بازگشت دیکشنری داده خام به همراه رشته رندر شده
    return data
