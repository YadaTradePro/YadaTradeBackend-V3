import os
import time
import logging
from functools import wraps
from typing import Optional 
from main import create_app
from extensions import scheduler
from config import SessionLocal 
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError 

from services.fetch_latest_brsapi_eod import update_daily_eod_from_brsapi
from services.data_fetcher import run_full_rebuild
from services.data_processing_and_analysis import run_technical_analysis, run_candlestick_detection 

from services.ml_prediction_service import (
    generate_and_save_predictions_for_watchlist,
    update_ml_prediction_outcomes
)

from services.weekly_watchlist_service import run_weekly_watchlist_selection, evaluate_weekly_watchlist_performance
from services.golden_key_service import run_golden_key_analysis_and_save
from services.potential_buy_queues_service import run_potential_buy_queue_analysis_and_save

from services import market_analysis_service
from services.index_data_processor import store_market_indices_data
from services.sector_analysis_service import run_daily_sector_analysis

from services.data_fetcher import run_full_rebuild

# ----------------- Logging Setup -----------------
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=[
    logging.FileHandler("scheduler.log", encoding="utf-8"),
    logging.StreamHandler()
])

logger = logging.getLogger(__name__)

# ----------------- App Context and Error Handling Decorator -----------------
app = create_app()

def with_context_and_error_handling(func):
    """
    Decorator to run a function inside a Flask app context, provide a DB session (if needed), 
    and handle potential exceptions (including DB transaction management).
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # بررسی می‌کنیم که آیا تابع ورودی به db_session نیاز دارد یا خیر
        needs_db_session = 'db_session' in func.__code__.co_varnames
        db_session: Optional[Session] = None
        
        with app.app_context():
            logger.info(f"✅ Executing job '{func.__name__}' inside Flask app context.")
            
            # 2. ایجاد Database Session در صورت نیاز
            if needs_db_session:
                try:
                    db_session = SessionLocal() 
                    kwargs['db_session'] = db_session # 🛠️ تزریق db_session
                except Exception as e:
                    logger.error(f"❌ Failed to create DB Session for job '{func.__name__}': {e}", exc_info=True)
                    # اگر نتواند سشن ایجاد کند، Job نباید اجرا شود
                    raise 
                
            try:
                # 3. اجرای تابع با آرگومان‌های به‌روز شده
                result = func(*args, **kwargs)
                
                # 4. Commit کردن تغییرات دیتابیس
                if db_session:
                    db_session.commit()
                    logger.info(f"✅ Job '{func.__name__}' completed successfully and changes committed.")
                else:
                    logger.info(f"✅ Job '{func.__name__}' completed successfully.")
                return result
                
            except SQLAlchemyError as e:
                # 5. Rollback در صورت بروز خطای دیتابیس
                if db_session:
                    db_session.rollback()
                logger.error(f"❌ Database Error in job '{func.__name__}': {e}", exc_info=True)
                raise 
                
            except Exception as e:
                # 5. Rollback و مدیریت خطاهای عمومی
                if db_session:
                    db_session.rollback()
                logger.error(f"❌ An error occurred while running job '{func.__name__}': {e}", exc_info=True)
                raise
                
            finally:
                # 6. بستن سشن در هر صورت
                if db_session:
                    db_session.close() 
                
    return wrapper



# ----------------- Custom Flow Definition -----------------

@with_context_and_error_handling
def run_daily_analysis_flow(db_session: Session = None):
    """
    اجرای ترتیبی فرآیند آپدیت داده‌ها و تحلیل روزانه. 
    این تابع نیاز به db_session دارد تا بتواند آن را به توابع پایینی پاس دهد.
    """
    logger.info("🎬 شروع 'Daily Analysis Flow': EOD Update -> Technical Analysis -> Candlestick Detection")

    # 1. به‌روزرسانی داده‌های پایان روز (EOD)
    logger.info("➡️ مرحله 1/3: فراخوانی update_daily_eod_from_brsapi")
    # توابع سرویس باید آرگومان db_session را بپذیرند.
    update_daily_eod_from_brsapi(db_session=db_session) 
    logger.info("✅ مرحله 1/3: update_daily_eod_from_brsapi با موفقیت انجام شد.")

    # 2. اجرای تحلیل تکنیکال (نیاز به داده‌های به‌روز شده دارد)
    logger.info("➡️ مرحله 2/3: فراخوانی run_technical_analysis")
    run_technical_analysis(db_session=db_session) 
    logger.info("✅ مرحله 2/3: run_technical_analysis با موفقیت انجام شد.")

    # 3. اجرای تشخیص الگوهای کندل استیک (نیاز به داده‌های به‌روز و نتایج تحلیل تکنیکال دارد)
    logger.info("➡️ مرحله 3/3: فراخوانی run_candlestick_detection")
    run_candlestick_detection(db_session=db_session)
    logger.info("✅ مرحله 3/3: run_candlestick_detection با موفقیت انجام شد.")
    
    logger.info("🎉 پایان موفقیت‌آمیز 'Daily Analysis Flow'.")



# ----------------- Job Definitions -----------------
# 💡 توجه: تمام توابعی که آرگومان db_session را می‌پذیرند، در run_scheduler_app
# به صورت خودکار توسط with_context_and_error_handling پوشانده خواهند شد.
JOBS = [
    # 🟢 وظایف روزانه (روزهای بازار) 

   
    # 1. جریان اصلی تحلیل (پس از پایان بازار) - این تابع خود دکوریت شده است (Flow)
    {"id": "daily_analysis_flow_job", "func": run_daily_analysis_flow, "trigger": "cron", "day_of_week": "sat, sun, mon, tue, wed", "hour": 16, "minute": 30},

    
    
    # 2.مرحله ۱: واکشی و ذخیره داده‌های شاخص روزانه (مشتمل بر get_market_indices)
    {"id": "index_data_update_job", "func": store_market_indices_data, "trigger": "cron", "day_of_week": "sat, sun, mon, tue, wed", "hour": 17, "minute": 30},
        # 🚀 مرحله ۲: اجرای تحلیل بخش‌ها (نیاز به داده‌های شاخص به‌روز دارد)
    {"id": "daily_sector_analysis_job", "func": run_daily_sector_analysis, "trigger": "cron", "day_of_week": "sat, sun, mon, tue, wed", "hour": 17, "minute": 37},


    # 3. تولید پیش‌بینی‌های ML (پس از تکمیل تمامی تحلیل‌ها) 
    {"id": "ml_generate_predictions_job", "func": generate_and_save_predictions_for_watchlist, "trigger": "cron", "day_of_week": "sat, sun, mon, tue, wed", "hour": 17, "minute": 40},
    # بروزرسانی نتایج ML (قبل از شروع بازار یا هر زمان که داده‌ها به‌روز هستند) 
    {"id": "ml_update_outcomes_job", "func": update_ml_prediction_outcomes, "trigger": "cron", "day_of_week": "sat, sun, mon, tue, wed", "hour": 17, "minute": 45},
    

    # 4. صف‌های خرید بالقوه 
    {"id": "potential_buy_queues_job", "func": run_potential_buy_queue_analysis_and_save, "trigger": "cron", "day_of_week": "sat, sun, mon, tue, wed", "hour": 19, "minute": 10},
    # 4. فیلترهای کلید طلایی (به صورت دوره‌ای) 
    {"id": "run_golden_key_filters_job", "func": run_golden_key_analysis_and_save, "trigger": "cron", "day_of_week": "sun, tue, wed", "hour": 19, "minute": 30},



    

    # 🟡 وظایف هفتگی (آخر هفته)

    # ارزیابی - (چهارشنبه شب)
    {"id": "weekly_watchlist_performance_job", "func": evaluate_weekly_watchlist_performance, "trigger": "cron", "day_of_week": "wed", "hour": 22, "minute": 0},
    # انتخاب - (چهارشنبه شب)
    {"id": "weekly_watchlist_selection_job", "func": run_weekly_watchlist_selection, "trigger": "cron", "day_of_week": "wed", "hour": 22, "minute": 30},



    # 🟡 وظایف هفتگی (پایان هفته - شامل بازسازی کامل)
    {"id": "weekly_full_rebuild_job", 
     "func": run_full_rebuild, 
     "trigger": "cron", 
     "day_of_week": "fri", 
     "hour": 4, 
     "minute": 0, 
     "coalesce": True, 
     "max_instances": 1, 
     "kwargs": { 
         "batch_size": 50, 
         "commit_batch_size": 100
     }},

    
]

TIMEZONE = "Asia/Tehran"

# ----------------- Scheduler Runner -----------------
def run_scheduler_app():
    """Runs the APScheduler in a standalone process."""
    app.config["SCHEDULER_RUN"] = True
    
    scheduler.init_app(app)

    for job in JOBS:
        try:
            # 💡 اعمال دکوراتور با منطق هوشمند: 
            # توابعی که خود دکوریت نشده‌اند را پوشانده تا Context و Session دیتابیس را دریافت کنند.
            func_to_schedule = job["func"]

            # چون run_daily_analysis_flow قبلاً با @with_context_and_error_handling دکور شده، 
            # آن را مستثنی می‌کنیم تا از تداخل Context جلوگیری شود.
            if job["id"] != "daily_analysis_flow_job":
                # ✅ اعمال دکوراتور برای تمام توابع دیگر، شامل run_full_rebuild
                func_to_schedule = with_context_and_error_handling(job["func"])
            
            scheduler.add_job(
                id=job["id"],
                func=func_to_schedule,
                trigger=job["trigger"],
                replace_existing=True,
                timezone=TIMEZONE,
                # ارسال تمام آرگومان‌های دیگر (مثل day_of_week, hour, minute, coalesce, kwargs)
                **{k: v for k, v in job.items() if k not in ["id", "func", "trigger"]}
            )
            logger.info(f"✅ Job registered: {job['id']}")
        except Exception as e:
            logger.error(f"❌ Failed to add job {job['id']}: {e}", exc_info=True)

    scheduler.start()
    logger.info("🚀 APScheduler started in a separate process.")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("🛑 Scheduler has been shut down.")

if __name__ == "__main__":
    run_scheduler_app()
