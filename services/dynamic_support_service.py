# services/dynamic_support_service.py
import pandas as pd
import numpy as np
import jdatetime
from sqlalchemy import desc, and_
import logging
import time
from datetime import date # برای استفاده در ذخیره سازی دیتابیس

# فرض می‌شود اینها در extensions/ یا جای مشابه تعریف شده‌اند
from extensions import db
# مدل‌ها
from models import HistoricalData, ComprehensiveSymbolData, DynamicSupportOpportunity # <-- مدل جدید اضافه شد

# تنظیمات لاگینگ
logger = logging.getLogger(__name__)

# ==========================================
# Global In-Memory Cache
# ==========================================
_ANALYSIS_CACHE = {
    "data": [],
    "last_run_timestamp": 0, 
    "last_run_jdate": None,
    "last_run_time": None
}
CACHE_TIMEOUT_SECONDS = 3 * 3600 

class StaticSupportAnalyzer:
    """
    تحلیلگر تک‌تیرانداز (Sniper Mode) با استفاده از دیتای بنیادی:
    1. فیلتر دقیق سهام (حذف صندوق‌ها و اوراق از طریق دیتابیس)
    2. حمایت استاتیک (کف ۱۶۰ روزه معتبر)
    3. نقدشوندگی (ارزش معاملات مناسب)
    4. پول هوشمند (Power Ratio > 1.5)
    """
    def __init__(self, db_session):
        self.session = db_session

    def get_symbol_history(self, symbol_id, days=160):
        try:
            query = self.session.query(
                HistoricalData.date,
                HistoricalData.final,
                HistoricalData.low,
                HistoricalData.volume,
                HistoricalData.buy_i_volume,
                HistoricalData.buy_count_i,
                HistoricalData.sell_i_volume,
                HistoricalData.sell_count_i
            ).filter(
                HistoricalData.symbol_id == symbol_id
            ).order_by(desc(HistoricalData.date)).limit(days)

            data = query.all()
            if not data or len(data) < 120:
                return pd.DataFrame()

            cols = [
                'date', 'final', 'low', 'volume',
                'buy_i_volume', 'buy_count_i', 'sell_i_volume', 'sell_count_i'
            ]
            df = pd.DataFrame(data, columns=cols)
            df = df.sort_values(by='date', ascending=True).reset_index(drop=True)
            
            numeric_cols = ['final', 'low', 'volume', 
                            'buy_i_volume', 'buy_count_i', 'sell_i_volume', 'sell_count_i']
            for col in numeric_cols:
                # Fill NaN with 0 for robustness, although data cleaning should ideally be done elsewhere
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
            return df
        except Exception:
            return pd.DataFrame()

    def calculate_metrics(self, df, base_volume):
        try:
            if df.empty: return None
            
            df = df[df['low'] > 0]
            if df.empty: return None

            # --- 1. تحلیل حمایت استاتیک (کف 160 روزه) ---
            min_low_160d = df['low'].min()
            current_price = df['final'].iloc[-1]

            if pd.isna(min_low_160d) or min_low_160d <= 0 or pd.isna(current_price):
                return None
            
            # --- 1.1. تایید اعتبار حمایت ---
            recent_40_days_df = df.iloc[-40:]
            if recent_40_days_df['low'].min() <= min_low_160d:
                 # اگر کف 160 روزه در 40 روز اخیر تکرار شده باشد، حمایت ضعیف است.
                 return None 

            distance_percentage = ((current_price - min_low_160d) / min_low_160d) * 100

            # --- 2. تحلیل نقدشوندگی و حجم ---
            last_row = df.iloc[-1]
            current_vol = last_row['volume']
            
            # ارزش معاملات روزانه: حداقل 10 میلیارد تومان
            trade_value_toman = current_vol * current_price
            MIN_TRADE_VALUE = 10_000_000_000 
            
            if trade_value_toman < MIN_TRADE_VALUE:
                return None

            # مقایسه با حجم مبنا: حداقل نصف حجم مبنا معامله شده باشد
            if base_volume and base_volume > 0:
                if current_vol < (base_volume * 0.5): 
                    return None

            # --- 3. تحلیل قدرت خریدار حقیقی (Smart Money) ---
            buy_vol = last_row['buy_i_volume']
            sell_vol = last_row['sell_i_volume']
            buy_count = last_row['buy_count_i']
            sell_count = last_row['sell_count_i']

            power_ratio = 1.0
            if buy_count > 0 and sell_count > 0 and buy_vol > 0:
                buy_per_capita = buy_vol / buy_count
                sell_per_capita = sell_vol / sell_count
                if sell_per_capita > 0:
                    power_ratio = buy_per_capita / sell_per_capita
            
            if pd.isna(power_ratio) or np.isinf(power_ratio):
                power_ratio = 1.0
            
            if pd.isna(distance_percentage) or np.isinf(distance_percentage):
                return None

            return {
                'current_price': float(current_price),
                'support_level': float(min_low_160d),
                'distance_percentage': float(distance_percentage),
                'power_ratio': float(round(power_ratio, 2)),
                'trade_value': float(trade_value_toman)
            }

        except Exception:
            return None

    def scan_market(self):
        logger.info("Starting Analysis: Finalized Logic (DB-Filter + Support Confirmation)")
        
        # === 1. دریافت لیست تمیز سهام از جدول جامع (بدون صندوق و اوراق) ===
        EXCLUDE_KEYWORDS = [
            'صندوق', 'سرمایه گذاری قابل معامله', 'اوراق', 'تسهیلات', 'اجاره', 
            'مشارکت', 'گواهی', 'کالا', 'انرژی', 'اختیار', 'آتی', 'سلف'
        ]
        
        conditions = []
        for kw in EXCLUDE_KEYWORDS:
            # جستجو در نام صنعت، نام گروه، و نام کامل شرکت
            conditions.append(~ComprehensiveSymbolData.industry.like(f'%{kw}%'))
            conditions.append(~ComprehensiveSymbolData.group_name.like(f'%{kw}%'))
            conditions.append(~ComprehensiveSymbolData.company_name.like(f'%{kw}%'))
        
        valid_symbols_list = []
        try:
            query = self.session.query(
                ComprehensiveSymbolData.symbol_id,
                ComprehensiveSymbolData.symbol_name,
                ComprehensiveSymbolData.base_volume
            ).filter(and_(*conditions))
            valid_symbols_list = query.all()
        except Exception as e:
            logger.error(f"DB Query Error: {e}")
            return []

        logger.info(f"Processing {len(valid_symbols_list)} symbols...")

        opportunities = []

        # === تنظیمات فیلتر تک‌تیرانداز ===
        MAX_DISTANCE = 28.0      # حداکثر 28% بالاتر از کف
        MIN_DISTANCE = 0.0
        MIN_POWER_RATIO = 1.50   # قدرت خریدار بالا

        for record in valid_symbols_list:
            symbol_id = record.symbol_id
            symbol_name = record.symbol_name
            base_vol = record.base_volume

            try:
                # 2. دریافت دیتا و محاسبه متریک‌ها
                df = self.get_symbol_history(symbol_id, days=160)
                if df.empty: continue

                analysis = self.calculate_metrics(df, base_vol)
                if not analysis: continue

                dist_pct = analysis['distance_percentage']
                p_ratio = analysis['power_ratio']

                # 3. اعمال شروط استراتژی
                in_buy_zone = MIN_DISTANCE <= dist_pct <= MAX_DISTANCE
                has_strong_buyer = p_ratio >= MIN_POWER_RATIO

                if in_buy_zone and has_strong_buyer:
                    opportunities.append({
                        'symbol_name': symbol_name,
                        'symbol_id': symbol_id,
                        'current_price': analysis['current_price'],
                        'support_level': analysis['support_level'], 
                        'distance_from_support': round(dist_pct, 2), 
                        'support_slope': p_ratio, # support_slope اینجا همان Power Ratio است.
                        'jdate': jdatetime.date.today().strftime('%Y-%m-%d')
                    })

            except Exception:
                continue

        opportunities.sort(key=lambda x: x['support_slope'], reverse=True)
        return opportunities

# ==========================================
# Service Function
# ==========================================

def save_opportunities_to_db(opportunities: list):
    """پاک کردن جدول و ذخیره فرصت‌های جدید در دیتابیس."""
    
    # 1. حذف داده‌های قدیمی (طبق درخواست)
    try:
        # برای جلوگیری از رشد بی‌رویه و نگهداری فقط نتایج روز جاری
        # (اگر بخواهیم تاریخچه داشته باشیم، این خط را باید تغییر دهیم یا حذف کنیم)
        db.session.query(DynamicSupportOpportunity).delete()
        db.session.commit()
        logger.info("Previous DynamicSupportOpportunity data cleared successfully.")
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error clearing previous opportunities: {e}")
        # ادامه می‌دهیم، شاید خطایی موقتی باشد.

    # 2. آماده‌سازی و ذخیره داده‌های جدید
    new_opportunities = []
    # تبدیل تاریخ جلالی روز جاری به میلادی برای ذخیره‌سازی در دیتابیس
    analysis_date_greg = jdatetime.date.today().togregorian()
    
    for opp in opportunities:
        # ساخت یک شیء از مدل جدید
        new_opp = DynamicSupportOpportunity(
            analysis_date=analysis_date_greg,
            symbol_id=opp['symbol_id'],
            symbol_name=opp['symbol_name'],
            current_price=opp['current_price'],
            support_level=opp['support_level'],
            distance_from_support=opp['distance_from_support'],
            power_ratio=opp['support_slope'] # در مدل: power_ratio، در خروجی: support_slope
        )
        new_opportunities.append(new_opp)

    try:
        # استفاده از bulk_save_objects برای افزایش سرعت
        db.session.bulk_save_objects(new_opportunities)
        db.session.commit()
        logger.info(f"{len(new_opportunities)} new DynamicSupportOpportunity saved to DB.")
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error saving new opportunities: {e}")

def get_dynamic_support_data(force_refresh=False):
    global _ANALYSIS_CACHE

    is_cache_valid = (time.time() - _ANALYSIS_CACHE["last_run_timestamp"]) < CACHE_TIMEOUT_SECONDS
    
    if not force_refresh and _ANALYSIS_CACHE["data"] and is_cache_valid:
        logger.info("Returning CACHED data.")
        return {
            "opportunities": _ANALYSIS_CACHE["data"],
            "last_updated_date": _ANALYSIS_CACHE["last_run_jdate"],
            "last_updated_time": _ANALYSIS_CACHE["last_run_time"]
        }

    if not is_cache_valid:
        logger.info("Cache expired. Running new analysis.")
    elif force_refresh:
        logger.info("Force refresh requested.")

    try:
        analyzer = StaticSupportAnalyzer(db.session) 
        results = analyzer.scan_market()
        
        # === گام جدید: ذخیره نتایج در دیتابیس ===
        save_opportunities_to_db(results)
        
        _ANALYSIS_CACHE = {
            "data": results,
            "last_run_timestamp": time.time(),
            "last_run_jdate": jdatetime.date.today().strftime('%Y-%m-%d'),
            "last_run_time": jdatetime.datetime.now().strftime('%H:%M:%S')
        }
        logger.info(f"Analysis complete. Found {len(results)} Top-Tier Gems.")
        
        return {
            "opportunities": results,
            "last_updated_date": _ANALYSIS_CACHE["last_run_jdate"],
            "last_updated_time": _ANALYSIS_CACHE["last_run_time"]
        }
        
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)
        if _ANALYSIS_CACHE["data"]:
             return {
                "opportunities": _ANALYSIS_CACHE["data"],
                "last_updated_date": _ANALYSIS_CACHE["last_run_jdate"],
                "last_updated_time": _ANALYSIS_CACHE["last_run_time"]
            }
        raise e