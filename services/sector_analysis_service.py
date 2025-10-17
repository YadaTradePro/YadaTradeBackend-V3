import pandas as pd
from extensions import db
from models import HistoricalData, ComprehensiveSymbolData, DailySectorPerformance, DailyIndexData # 💡 اضافه شد
from datetime import timedelta
import jdatetime
from sqlalchemy import func
import logging

logger = logging.getLogger(__name__)

def run_daily_sector_analysis():
    """
    صنایع را بر اساس میانگین ارزش معاملات و میانگین ارزش ورود پول حقیقی در ۵ روز معاملاتی گذشته 
    تحلیل و رتبه‌بندی کرده و نتایج را در دیتابیس ذخیره می‌کند.
    """
    logger.info("شروع فرآیند تحلیل و رتبه‌بندی روزانه صنایع...")
    today_jdate_str = jdatetime.date.today().strftime('%Y-%m-%d')

    # بهبود ۱: پیدا کردن ۵ روز معاملاتی آخر به صورت پویا و دقیق
    try:
        last_5_trading_days_query = db.session.query(
            HistoricalData.jdate
        ).distinct().order_by(HistoricalData.jdate.desc()).limit(5)
        
        last_5_days = [d[0] for d in last_5_trading_days_query.all()]

        if not last_5_days:
            logger.warning("هیچ روز معاملاتی در دیتابیس برای تحلیل صنایع یافت نشد.")
            return

        logger.info(f"تحلیل صنایع برای {len(last_5_days)} روز معاملاتی اخیر انجام می‌شود: {last_5_days}")
        
    except Exception as e:
        logger.error(f"خطا در پیدا کردن آخرین روزهای معاملاتی: {e}", exc_info=True)
        return

    # ۱. خواندن داده‌های مورد نیاز از دیتابیس
    # بهبود ۲: اضافه کردن قیمت پایانی (close) برای محاسبه ارزش جریان پول
    hist_records = db.session.query(
        HistoricalData.symbol_id,
        HistoricalData.jdate, 
        HistoricalData.value, # ارزش معاملات
        HistoricalData.buy_i_volume,
        HistoricalData.sell_i_volume,
        HistoricalData.close # <- ستون جدید برای محاسبه ارزش
    ).filter(HistoricalData.jdate.in_(last_5_days)).all()
    
    if not hist_records:
        logger.info("داده‌ای برای تحلیل صنعت در بازه زمانی مشخص شده یافت نشد.")
        return
        
    hist_df = pd.DataFrame(hist_records)

    # پیش‌پردازش داده‌ها برای جلوگیری از خطا
    numeric_cols = ['value', 'buy_i_volume', 'sell_i_volume', 'close']
    for col in numeric_cols:
        hist_df[col] = pd.to_numeric(hist_df[col], errors='coerce').fillna(0)

    # بهبود ۳: محاسبه «ارزش ریالی» جریان پول به جای حجم خالص
    hist_df['net_money_flow_value'] = (hist_df['buy_i_volume'] - hist_df['sell_i_volume']) * hist_df['close']

    # B. دریافت اطلاعات صنعت هر نماد (بدون تغییر)
    symbols_query = db.session.query(
        ComprehensiveSymbolData.symbol_id,
        ComprehensiveSymbolData.group_name
    ).all()
    symbols_df = pd.DataFrame(symbols_query, columns=['symbol_id', 'group_name'])

    # C. ترکیب دو DataFrame در Pandas (بدون تغییر)
    df = pd.merge(hist_df, symbols_df, on='symbol_id', how='left')
    df = df.dropna(subset=['group_name'])
    
    if df.empty:
        logger.warning("داده‌ای با اطلاعات صنعت مرتبط یافت نشد. تحلیل متوقف شد.")
        return

    # ۲. گروه‌بندی و محاسبه معیارها برای هر صنعت (میانگین عملکرد روزانه)
    daily_sector_performance = df.groupby(['jdate', 'group_name']).agg(
        daily_trade_value=('value', 'sum'),
        # استفاده از ستون جدید و دقیق برای جریان پول
        daily_net_money_flow=('net_money_flow_value', 'sum') 
    ).reset_index()

    sector_performance = daily_sector_performance.groupby('group_name').agg(
        avg_daily_trade_value=('daily_trade_value', 'mean'), 
        avg_daily_net_money_flow=('daily_net_money_flow', 'mean'),
    ).reset_index()

    # ۳. رتبه‌بندی صنایع (بدون تغییر در منطق)
    sector_performance['value_rank'] = sector_performance['avg_daily_trade_value'].rank(ascending=False, method='min')
    sector_performance['flow_rank'] = sector_performance['avg_daily_net_money_flow'].rank(ascending=False, method='min')
    
    sector_performance['final_rank'] = sector_performance['value_rank'] + sector_performance['flow_rank']
    sector_performance = sector_performance.sort_values('final_rank').reset_index(drop=True)
    sector_performance['rank'] = sector_performance.index + 1 

    # ۴. ذخیره نتایج در دیتابیس (منطق Upsert)
    try:
        # 4.1. حذف رکوردهای قدیمی برای تاریخ امروز (Upsert تمیز)
        db.session.query(DailySectorPerformance).filter(
            DailySectorPerformance.jdate == today_jdate_str
        ).delete(synchronize_session=False) # استفاده از synchronize_session=False برای کارایی بهتر
        logger.info(f"✅ رکوردهای تحلیل صنعت برای تاریخ {today_jdate_str} حذف شدند تا جایگزین شوند.")

        # 4.2. آماده‌سازی داده‌ها برای درج گروهی (Bulk Insert)
        records_to_insert = []
        for _, row in sector_performance.iterrows():
            records_to_insert.append({
                'jdate': today_jdate_str, 
                'sector_name': row['group_name'],
                'total_trade_value': row['avg_daily_trade_value'],
                'net_money_flow': row['avg_daily_net_money_flow'],
                'rank': int(row['rank']) # اطمینان از نوع داده صحیح
            })

        # 4.3. درج گروهی داده‌های جدید
        if records_to_insert:
            db.session.bulk_insert_mappings(DailySectorPerformance, records_to_insert)
            db.session.commit()
            logger.info(f"✅ تحلیل و رتبه‌بندی {len(records_to_insert)} صنعت با موفقیت برای تاریخ {today_jdate_str} ذخیره شد.")
        else:
            # اگر رکوردی برای درج وجود نداشت، تراکنش را بازگردانی می‌کنیم
            db.session.rollback()
            logger.info("هیچ رکوردی برای ذخیره در تحلیل صنایع تولید نشد.")

    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ خطا در ذخیره‌سازی نتایج تحلیل صنعت: {e}", exc_info=True)