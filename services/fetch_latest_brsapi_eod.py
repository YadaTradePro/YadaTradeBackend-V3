# -*- coding: utf-8 -*-
# services/fetch_latest_brsapi_eod.py
# مسئول واکشی آخرین داده‌های EOD (شامل داده‌های اردر بوک) تمام نمادها از BRSAPI 
# و ذخیره‌سازی آن‌ها در دیتابیس (HistoricalData).

from extensions import db
from models import HistoricalData, ComprehensiveSymbolData 
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from datetime import date
import jdatetime
import pandas as pd
import requests
import logging
from typing import Optional, Tuple

# --- تنظیمات لاگینگ ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------------------
# تنظیمات اصلی
# --------------------------------------------------------------------------------
# !!! توجه: کلید API واقعی خود را در اینجا قرار دهید
API_KEY = "BvhdYHBjqiyIQ7eTuQBKN17ZuLpHkQZ1"
BRSAPI_ALL_SYMBOLS_URL = f"https://brsapi.ir/Api/Tsetmc/AllSymbols.php?key={API_KEY}&type=1"

# ##############################################################################
# بخش ۱: دریافت داده از BRSAPI
# ##############################################################################

def fetch_latest_brsapi_eod() -> Optional[pd.DataFrame]:
    """
    آخرین وضعیت معاملاتی (EOD) تمام نمادها را از وب‌سرویس BRSAPI دریافت می‌کند.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }
    
    logger.info("🌐 در حال دریافت آخرین وضعیت نمادها (EOD و اردر بوک) از BRSAPI...")
    
    try:
        response = requests.get(BRSAPI_ALL_SYMBOLS_URL, headers=headers, timeout=45)
        response.raise_for_status()
        
        data = response.json()
        
        if not data or (isinstance(data, dict) and data.get('Error')):
            logger.error(f"❌ خطای API از BRSAPI: {data}")
            return None

        df = pd.DataFrame(data)
        logger.info(f"✅ با موفقیت {len(df)} رکورد از BRSAPI دریافت شد.")
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ خطای درخواست از BRSAPI: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ خطای ناشناخته در دریافت داده از BRSAPI: {e}")
        return None

# ##############################################################################
# بخش ۲: تابع اصلی و هماهنگ‌کننده ذخیره‌سازی
# ##############################################################################

def update_daily_eod_from_brsapi(db_session: Session) -> Tuple[int, str]:
    """
    چرخه کامل آپدیت روزانه:
    1. دریافت داده‌های EOD از BRSAPI.
    2. مپ کردن ستون‌های BRSAPI به مدل HistoricalData.
    3. ذخیره/آپدیت داده‌ها در جدول HistoricalData با استفاده از Merge.
    """
    logger.info("⚡️ شروع چرخه کامل به‌روزرسانی داده‌های تاریخی روزانه و اردر بوک از BRSAPI...")
    
    # 1. دریافت داده‌های EOD
    df_eod = fetch_latest_brsapi_eod()
    if df_eod is None or df_eod.empty:
        return 0, "❌ دریافت داده از BRSAPI ناموفق بود یا دیتای خالی بازگشت."

    # 2. آماده‌سازی DataFrame و افزودن تاریخ
    today_gregorian = date.today()
    
    # ✅ FIX نهایی برای جلوگیری از Type Error SQLite: 
    # به جای انتساب مستقیم scalar، یک Series کامل از آبجکت‌های datetime.date ایجاد می‌کنیم 
    # تا مطمئن شویم Pandas آن را به رشته تبدیل نمی‌کند و SQLAlchemy آن را می‌پذیرد.
    df_eod['date'] = pd.Series([today_gregorian] * len(df_eod), index=df_eod.index)
    df_eod['jdate'] = jdatetime.date.fromgregorian(date=today_gregorian).strftime("%Y-%m-%d")
    
    # 🎯 نگاشت کامل ستون‌های BRSAPI به ستون‌های مدل HistoricalData
    column_mapping = {
        # Metadata & ID
        'id': 'tse_index',         # کد TSETMC برای نگاشت به symbol_id
        'l18': 'symbol_name',      # نام نماد 

        # Prices
        'pf': 'open',              # اولین قیمت (FirstPrice)
        'pmax': 'high',            # بیشترین قیمت
        'pmin': 'low',             # کمترین قیمت
        'pl': 'close',             # آخرین قیمت (Last Price - آخرین معامله)
        'pc': 'final',             # قیمت پایانی (Close Price)
        'py': 'yesterday_price',   # قیمت دیروز
        
        # Volume and Value
        'tvol': 'volume',          # حجم معاملات
        'tval': 'value',           # ارزش معاملات
        'tno': 'num_trades',       # تعداد معاملات

        # Price Changes
        'plc': 'plc',              # تغییر آخرین قیمت (Price Last Change)
        'plp': 'plp',              # درصد تغییر آخرین قیمت (Price Last Percent)
        'pcc': 'pcc',              # تغییر قیمت پایانی (Price Close Change)
        'pcp': 'pcp',              # درصد تغییر قیمت پایانی (Close Price Percent)
        'mv': 'mv',                # ارزش بازار (Market Value - با فرض وجود)

        # Real/Legal Shareholder Data (BRSAPI fields are in PascalCase/mixed)
        'Buy_CountI': 'buy_count_i', 
        'Buy_CountN': 'buy_count_n', 
        'Sell_CountI': 'sell_count_i', 
        'Sell_CountN': 'sell_count_n', 
        'Buy_I_Volume': 'buy_i_volume', 
        'Buy_N_Volume': 'buy_n_volume', 
        'Sell_I_Volume': 'sell_i_volume', 
        'Sell_N_Volume': 'sell_n_volume',
        
        # Order Book Data (5 levels - Demand/Bid) - تعداد خریدار, حجم خرید, قیمت خرید
        'zd1': 'zd1', 'qd1': 'qd1', 'pd1': 'pd1',
        'zd2': 'zd2', 'qd2': 'qd2', 'pd2': 'pd2',
        'zd3': 'zd3', 'qd3': 'qd3', 'pd3': 'pd3',
        'zd4': 'zd4', 'qd4': 'qd4', 'pd4': 'pd4',
        'zd5': 'zd5', 'qd5': 'qd5', 'pd5': 'pd5',
        
        # Order Book Data (5 levels - Offer/Ask) - تعداد فروشنده, حجم فروش, قیمت فروش
        'zo1': 'zo1', 'qo1': 'qo1', 'po1': 'po1',
        'zo2': 'zo2', 'qo2': 'qo2', 'po2': 'po2',
        'zo3': 'zo3', 'qo3': 'qo3', 'po3': 'po3',
        'zo4': 'zo4', 'qo4': 'qo4', 'po4': 'po4',
        'zo5': 'zo5', 'qo5': 'qo5', 'po5': 'po5',
    }
    df_eod.rename(columns=column_mapping, inplace=True)
    
    # 3. مپ کردن با شناسه‌های داخلی (symbol_id)
    tse_indices = df_eod['tse_index'].astype(str).unique().tolist()
    symbol_map = {
        str(tse): sid 
        for tse, sid in db_session.query(
            ComprehensiveSymbolData.tse_index, 
            ComprehensiveSymbolData.symbol_id
        ).filter(ComprehensiveSymbolData.tse_index.in_(tse_indices)).all()
    }
    
    df_eod['symbol_id'] = df_eod['tse_index'].astype(str).map(symbol_map)
    df_eod.dropna(subset=['symbol_id'], inplace=True)
    df_eod['symbol_id'] = df_eod['symbol_id'].astype(int)

    if df_eod.empty:
        return 0, "❌ هیچ‌کدام از نمادهای دریافتی در دیتابیس (ComprehensiveSymbolData) موجود نبودند."

    # 4. فیلتر کردن ستون‌های DataFrame برای مطابقت دقیق با مدل HistoricalData
    # این لیست باید با ستون‌های مدل HistoricalData شما مطابقت داشته باشد و 'id' را حذف کند.
    historical_columns = [col.name for col in HistoricalData.__table__.columns if col.name not in ('id', 'created_at', 'updated_at')]
    
    # اطمینان از اینکه فقط ستون‌های معتبر مدل در DataFrame باقی می‌مانند
    df_eod_filtered = df_eod[[col for col in historical_columns if col in df_eod.columns]]
    
    # 5. ذخیره‌سازی داده‌های تاریخی با استراتژی Merge (Upsert)
    total_processed_count = 0
    updated_symbol_ids = df_eod_filtered['symbol_id'].unique().tolist()
    
    try:
        records = df_eod_filtered.to_dict('records')
        
        for record in records:
            # db_session.merge() بر اساس کلید اصلی یا Unique Constraint (symbol_id, date)
            # رکورد را درج یا به‌روزرسانی می‌کند.
            db_session.merge(HistoricalData(**record))
        
        db_session.commit()
        total_processed_count = len(records)
        logger.info(f"✅ {total_processed_count} رکورد شامل داده‌های قیمت و اردر بوک با موفقیت در HistoricalData ذخیره/آپدیت شد.")
    
    except Exception as e:
        db_session.rollback()
        logger.error(f"❌ خطای دیتابیس در ذخیره EOD: {e}", exc_info=True)
        return 0, f"خطای دیتابیس در ذخیره EOD: {e}"

    # 6. آپدیت تاریخ آخرین به‌روزرسانی در ComprehensiveSymbolData
    if updated_symbol_ids:
        try:
            db_session.query(ComprehensiveSymbolData).filter(
                ComprehensiveSymbolData.symbol_id.in_(updated_symbol_ids)
            ).update(
                {ComprehensiveSymbolData.last_historical_update_date: today_gregorian}, 
                synchronize_session=False
            )
            db_session.commit()
            logger.info(f"✅ تاریخ آخرین به‌روزرسانی برای {len(updated_symbol_ids)} نماد در ComprehensiveSymbolData آپدیت شد.")
        except Exception as e:
            db_session.rollback()
            logger.error(f"❌ خطای دیتابیس در آپدیت ComprehensiveSymbolData: {e}", exc_info=True)
            
    message = f"✅ چرخه کامل آپدیت روزانه تاریخی برای {len(updated_symbol_ids)} نماد انجام شد. (ذخیره/آپدیت {total_processed_count} رکورد)."
    logger.info(message)
    return len(updated_symbol_ids), message


# ##############################################################################
# توابع Export شده
# ##############################################################################

__all__ = [
    'update_daily_eod_from_brsapi',
    'fetch_latest_brsapi_eod',
]