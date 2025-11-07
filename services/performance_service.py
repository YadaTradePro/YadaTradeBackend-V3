# services/performance_service.py

from extensions import db
from models import SignalsPerformance, AggregatedPerformance, WeeklyWatchlistResult, ComprehensiveSymbolData, HistoricalData
from datetime import datetime, timedelta, date
import jdatetime
import pandas as pd
import logging
from sqlalchemy import func, and_
from typing import Optional, Tuple, List, Dict
import uuid

# Import utility functions (FIX: Removed the missing function 'get_last_market_day_date')
from services.technical_analysis_utils import get_today_jdate_str, convert_gregorian_to_jalali 

logger = logging.getLogger(__name__)
SIGNAL_SOURCE_WATCHLIST = 'WeeklyWatchlistService'

# --------------------------------------------------------------------------------
# --- Helper Functions (REVISED) ---
# --------------------------------------------------------------------------------

def _get_last_market_day_date_gregorian(current_date: date) -> date:
    """
    Returns the last market day (non-Thursday/Friday) on or before current_date.
    For Iran: Skips Thu(3)/Fri(4); assumes Sat-Wed open. Max 14-day lookback for long holidays.
    Future: Integrate variable holidays via calendar or price check.
    """
    day = current_date
    max_lookback = 14  # Extended for safety (e.g., Nowruz holidays)
    
    for i in range(max_lookback):
        # Skip fixed holidays: Thu=3, Fri=4
        if day.weekday() in (3, 4):
            day -= timedelta(days=1)
            logger.debug(f"Skipped holiday {day + timedelta(days=1)} (weekday {day.weekday()})")
            continue
        # Future: Add variable holidays check (e.g., from API/calendar)
        # if is_variable_holiday(day): day -= timedelta(days=1); continue
        
        # Validate with price data (optional, but recommended)
        # if not has_eod_price(day): day -= timedelta(days=1); continue
        
        logger.debug(f"Found market day: {day} (weekday {day.weekday()})")
        return day
    
    # Fallback: Conservative back
    logger.warning(f"Max lookback reached; fallback to {current_date - timedelta(days=1)}")
    return current_date - timedelta(days=1)

# --- Helper functions for safe date/datetime formatting and data fetching ---
def safe_date_format(date_obj, fmt='%Y-%m-%d'):
    """
    Safely formats a date or datetime object to a string.
    Returns None if the object is not a valid date/datetime.
    """
    if isinstance(date_obj, (datetime, date)):
        return date_obj.strftime(fmt)
    return None

def get_latest_symbol_price(symbol_id: str) -> Optional[Tuple[Optional[float], date]]:
    """
    Retrieves the latest available market price and its date.
    Prioritizes price on the calculated last market day. If not found,
    searches for the absolute latest price available in the database.
    Returns: A tuple of (price: Optional[float], price_date_gregorian: date).
    """
    current_greg_date = date.today()
    last_market_date_gregorian = _get_last_market_day_date_gregorian(current_greg_date)
    
    # 1. تلاش برای بازیابی قیمت در آخرین روز بازار محاسبه شده
    latest_price_data = db.session.query(HistoricalData).filter(
        HistoricalData.symbol_id == symbol_id,
        HistoricalData.date == last_market_date_gregorian
    ).first()

    price = None
    price_date = last_market_date_gregorian # فرض اولیه، تاریخ آخرین روز بازار است

    if latest_price_data:
        price = latest_price_data.final if latest_price_data.final is not None else latest_price_data.close
    
    # 2. اگر قیمتی برای تاریخ محاسبه‌شده پیدا نشد، آخرین قیمت موجود را بازیابی کن
    if price is None and symbol_id != "any_symbol":
        logger.warning(
            f"No price found for {symbol_id} on {safe_date_format(price_date)}. "
            f"Searching for the absolute latest price in DB."
        )
        # کوئری برای آخرین قیمت موجود در دیتابیس برای این نماد
        latest_price_data = db.session.query(HistoricalData).filter(
            HistoricalData.symbol_id == symbol_id
        ).order_by(HistoricalData.date.desc()).first()
        
        if latest_price_data:
            price = latest_price_data.final if latest_price_data.final is not None else latest_price_data.close
            price_date = latest_price_data.date # تاریخ واقعی قیمت را جایگزین کن
            logger.info(f"Found latest price for {symbol_id} on {safe_date_format(price_date)}.")

    if price is None:
        logger.warning(f"Could not retrieve price for symbol_id: {symbol_id} even using the absolute latest data.")

    # برای نماد ساختگی، همیشه تاریخ آخرین روز بازار محاسبه‌شده را برگردان
    if symbol_id == "any_symbol":
        return (price, last_market_date_gregorian)
        
    # برای نمادهای واقعی، قیمت و تاریخ واقعی آن قیمت را برگردان
    return (price, price_date)


def calculate_pnl_percent(entry_price: float, exit_price: float) -> float:
    """Calculates profit/loss percentage."""
    if entry_price is None or entry_price == 0:
        return 0.0
    return ((exit_price - entry_price) / entry_price) * 100.0

def check_sufficient_price_data(symbol_id: str, entry_date: date, required_days: int = 3) -> bool:
    """Checks if there are at least 'required_days' of price data after entry_date."""
    count = db.session.query(HistoricalData).filter(
        HistoricalData.symbol_id == symbol_id,
        HistoricalData.date > entry_date
    ).count()
    return count >= required_days

def determine_final_signal_status(profit_loss_percent: Optional[float]) -> str:
    """
    تعیین وضعیت نهایی (closed_win/closed_loss/closed_neutral) بر اساس درصد سود/زیان.
    آستانه کوچک 0.005% برای مدیریت خطاهای گرد کردن در نظر گرفته شده است.
    """
    if profit_loss_percent is None:
        return 'evaluated' # وضعیت موقت یا خطا
    
    if profit_loss_percent > 0.005: 
        return 'closed_win'
    elif profit_loss_percent < -0.005:
        return 'closed_loss'
    else:
        return 'closed_neutral'

# --------------------------------------------------------------------------------
# --- Core Performance Pipeline (Combined Three-Phase Function - REVISED) ---
# --------------------------------------------------------------------------------

def run_weekly_performance_pipeline(days_to_lookback: int = 7) -> Tuple[bool, str]:
    """
    Runs the full three-phase performance pipeline for WeeklyWatchlistService:
    Phase 0: Delete evaluated/expired records from WeeklyWatchlistResult (Clean up).
    Phase 1: Close ALL active WeeklyWatchlist signals (with 3-day data check) and mark as 'evaluated'.
    Phase 2: Create/Update SignalsPerformance records based on Phase 1 closures, setting final status (win/loss).
    Phase 3: Calculate and update AggregatedPerformance (weekly, monthly, annual).
    """
    signal_source = SIGNAL_SOURCE_WATCHLIST
    logger.info(f"--- Starting Three-Phase Performance Pipeline for {signal_source} ---")
    
    try:
        # --- PHASE 0: Clean up WeeklyWatchlistResult (DELETION LOGIC) ---
        # اجرای منطق حذف: پاک کردن دیتای قبلی WeeklyWatchlistResult که ارزیابی شده است
        
        # ⬅️ تغییر کلیدی: حذف تمام رکوردهای ارزیابی شده (evaluated) و منقضی شده (expired)
        deleted_count = db.session.query(WeeklyWatchlistResult).filter(
            WeeklyWatchlistResult.status.in_(['evaluated', 'expired'])
        ).delete(synchronize_session=False)
        
        logger.info(f"Phase 0 Complete: {deleted_count} records DELETED from WeeklyWatchlistResult (Status: evaluated/expired).")
        # توجه: commit در انتهای تابع انجام می شود تا عملیات حذف با سایر تغییرات همزمان باشد

        # 1. Date Determination
        today_greg = jdatetime.date.today().togregorian()
        
        # تاریخ آخرین قیمت و خروج (استفاده از "any_symbol" فقط برای تعیین تاریخ)
        exit_price_dummy, exit_date_greg = get_latest_symbol_price("any_symbol")
        
        # بررسی قطعی تاریخ
        if exit_date_greg is None:
             return False, "Failed to determine the latest market date for pipeline execution."
        
        exit_jdate_str = convert_gregorian_to_jalali(exit_date_greg)
        
        # متغیرهای فعال مربوط به فیلتر تاریخ حذف شدند
        
        logger.info(f"Pipeline Exit Date: {exit_jdate_str}. Checking ALL active signals for closure.")

        # --- PHASE 1: Close WeeklyWatchlist Signals (Mark as 'evaluated') ---
        
        # انتخاب تمام سیگنال‌های فعال (بدون محدودیت تاریخ ورود)
        active_results = WeeklyWatchlistResult.query.filter(
            WeeklyWatchlistResult.status == 'Open',
            # شرط محدودکننده تاریخ ورود حذف شد
        ).all()
        
        closed_count = 0
        signals_to_process = []
        
        for result in active_results:
            symbol_id = result.symbol_id
            entry_price = result.entry_price
            # ⬅️ اصلاح: حذف متد .date()
            entry_date_greg = result.entry_date 

            # 1.1. Minimum 3 days data check
            if not check_sufficient_price_data(symbol_id, entry_date_greg, required_days=3):
                logger.debug(f"Skipping {result.symbol_name}: Less than 3 prices available after entry date.")
                continue
                
            # 1.2. Get latest price (Exit Price)
            current_exit_price, _ = get_latest_symbol_price(symbol_id)

            if current_exit_price is None or entry_price is None:
                logger.warning(f"Skipping closure for {result.symbol_name}: Missing exit price or entry price.")
                continue
                
            # 1.3. Calculate P/L and Update WeeklyWatchlistResult
            profit_loss_percent = calculate_pnl_percent(entry_price, current_exit_price)
            
            result.status = 'evaluated' # وضعیت موقت برای WeeklyWatchlistResult (آماده برای حذف در دور بعد)
            result.exit_price = current_exit_price
            result.exit_date = exit_date_greg
            result.jexit_date = exit_jdate_str
            result.profit_loss_percentage = round(profit_loss_percent, 2)
            db.session.add(result)
            
            signals_to_process.append(result)
            closed_count += 1
            logger.debug(f"P1: Closed {result.symbol_name}. Status: 'evaluated', P/L: {profit_loss_percent:.2f}%")

        logger.info(f"Phase 1 Complete: {closed_count} signals updated in WeeklyWatchlistResult to 'evaluated'.")

        # --- PHASE 2: Update/Create SignalsPerformance Records (APPEND ONLY) ---
        
        sp_updated_count = 0
        sp_created_count = 0
        
        for result in signals_to_process:
            # ⬅️ تغییر کلیدی فاز ۲: محاسبه و ثبت وضعیت نهایی عملکرد
            final_status = determine_final_signal_status(result.profit_loss_percentage)
            
            # Check if SignalsPerformance record already exists (based on signal_unique_id)
            performance_record = SignalsPerformance.query.filter_by(signal_id=result.signal_unique_id).first()
            
            if performance_record:
                # Update existing record 
                performance_record.status = final_status # ⬅️ اصلاح: استفاده از وضعیت نهایی
                performance_record.exit_price = result.exit_price
                performance_record.exit_date = result.exit_date
                performance_record.jexit_date = result.jexit_date
                performance_record.profit_loss_percent = result.profit_loss_percentage
                performance_record.evaluated_at = datetime.now()
                db.session.add(performance_record)
                sp_updated_count += 1
                logger.debug(f"P2: Updated SignalsPerformance for {result.symbol_name}. Status: {final_status}")
            else:
                # Create new SignalsPerformance record 
                new_sp = SignalsPerformance(
                    # Signal Data
                    # NOTE: Added safety check for signal_unique_id and symbol_name
                    signal_id=result.signal_unique_id if hasattr(result, 'signal_unique_id') and result.signal_unique_id else str(uuid.uuid4()),
                    symbol_id=result.symbol_id,
                    symbol_name=result.symbol_name,
                    signal_source=signal_source,
                    entry_date=result.entry_date,
                    jentry_date=result.jentry_date,
                    entry_price=result.entry_price,
                    outlook=result.outlook,
                    reason=getattr(result, 'reason', None), 
                    probability_percent=getattr(result, 'probability_percent', None),
                    
                    # Exit/Evaluation Data
                    exit_date=result.exit_date,
                    jexit_date=result.jexit_date,
                    exit_price=result.exit_price,
                    profit_loss_percent=result.profit_loss_percentage,
                    status=final_status, # ⬅️ اصلاح: استفاده از وضعیت نهایی
                    created_at=result.created_at, 
                    evaluated_at=datetime.now()
                )
                db.session.add(new_sp)
                sp_created_count += 1
                logger.debug(f"P2: Created new SignalsPerformance for {result.symbol_name}. Status: {final_status}")

        logger.info(f"Phase 2 Complete: {sp_updated_count} records updated, {sp_created_count} records created in SignalsPerformance.")


        # --- PHASE 3: Calculate and Update AggregatedPerformance (DELETE/INSERT) ---
        
        report_date_str = get_today_jdate_str()
        
        # 3.1. Define aggregation periods and process them
        aggregation_periods = [
            ('weekly', timedelta(days=7)),
            ('monthly', timedelta(days=30)),
            ('annual', timedelta(days=365))
        ]
        
        for period_type, delta in aggregation_periods:
            start_date_greg_agg = exit_date_greg - delta
            start_jdate_str_agg = convert_gregorian_to_jalali(start_date_greg_agg)
            end_jdate_str_agg = exit_jdate_str
            
            logger.info(f"P3: Calculating {period_type} performance from {start_jdate_str_agg} to {end_jdate_str_agg}.")

            # Query: Filter SignalsPerformance based on jexit_date AND FINAL statuses
            signals_in_period = SignalsPerformance.query.filter(
                SignalsPerformance.signal_source == signal_source,
                # ⬅️ تغییر کلیدی فاز ۳: فیلتر بر اساس وضعیت‌های نهایی
                SignalsPerformance.status.in_(['closed_win', 'closed_loss', 'closed_neutral']), 
                SignalsPerformance.jexit_date >= start_jdate_str_agg,
                SignalsPerformance.jexit_date <= end_jdate_str_agg 
            ).all()

            # 3.2. Performance Metrics Calculation
            if not signals_in_period:
                total_signals, successful_signals, win_rate, total_profit_percent, total_loss_percent, average_profit_per_win, average_loss_per_loss, net_profit_percent = 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
                logger.warning(f"P3: No 'evaluated' signals found for {period_type} aggregation.")
            else:
                total_signals = len(signals_in_period)
                
                # تعیین Win/Loss بر اساس profit_loss_percent ثبت شده (معیار > 0.005% سود)
                successful_signals = sum(1 for s in signals_in_period if s.profit_loss_percent is not None and s.profit_loss_percent > 0.005)
                
                win_rate = (successful_signals / total_signals) * 100 if total_signals > 0 else 0.0
                
                winning_signals = [s for s in signals_in_period if s.profit_loss_percent is not None and s.profit_loss_percent > 0]
                losing_signals = [s for s in signals_in_period if s.profit_loss_percent is not None and s.profit_loss_percent < 0]
                
                total_profit_percent = sum(s.profit_loss_percent for s in winning_signals)
                total_loss_percent = abs(sum(s.profit_loss_percent for s in losing_signals))
                
                average_profit_per_win = (total_profit_percent / len(winning_signals)) if winning_signals else 0.0
                average_loss_per_loss = (sum(s.profit_loss_percent for s in losing_signals) / len(losing_signals)) if losing_signals else 0.0 # این مقدار منفی است
                
                net_profit_percent = total_profit_percent + average_loss_per_loss 
                logger.debug(f"P3 ({period_type}): Total: {total_signals}, Win Rate: {win_rate:.2f}%, Net P/L: {net_profit_percent:.2f}%")

            # 3.3. Save/Update Aggregated Performance (Delete/Insert logic)
            
            # **اجرای منطق حذف/درج جدید**
            # حذف رکوردهای قدیمی برای period_type و report_date امروز
            db.session.query(AggregatedPerformance).filter(
                AggregatedPerformance.report_date == report_date_str,
                AggregatedPerformance.period_type == period_type,
                AggregatedPerformance.signal_source == signal_source
            ).delete(synchronize_session=False) # Delete previous records for today's report
            
            # درج رکورد جدید
            new_agg_perf = AggregatedPerformance(
                report_date=report_date_str,
                period_type=period_type,
                signal_source=signal_source,
                total_signals=total_signals,
                successful_signals=successful_signals,
                win_rate=round(win_rate, 2),
                total_profit_percent=round(total_profit_percent, 2),
                total_loss_percent=round(total_loss_percent, 2),
                average_profit_per_win=round(average_profit_per_win, 2),
                average_loss_per_loss=round(average_loss_per_loss, 2),
                net_profit_percent=round(net_profit_percent, 2),
                created_at=datetime.now()
            )
            db.session.add(new_agg_perf)
            logger.info(f"P3: Calculated and added NEW AggregatedPerformance record for {period_type}.")

        # --- FINAL COMMIT ---
        
        logger.info("Phase 4: Attempting FINAL COMMIT for all three phases.")
        db.session.commit()
        message = f"Pipeline SUCCESS: {closed_count} signals evaluated. Aggregated performance (weekly, monthly, annual) successfully updated."
        logger.info(message)
        return True, message
        
    except Exception as e:
        db.session.rollback()
        error_message = f"Pipeline FAILED on FINAL COMMIT: {e}. ALL changes have been rolled back."
        logger.error(error_message, exc_info=True)
        return False, error_message









def get_aggregated_performance_reports(period_type: Optional[str] = None, signal_source: Optional[str] = None) -> List[Dict]:
    """Retrieves filtered aggregated performance reports."""
    logger.info(f"Retrieving aggregated performance reports (Period: {period_type}, Source: {signal_source}).")
    
    query = AggregatedPerformance.query
    
    if period_type:
        query = query.filter_by(period_type=period_type)
    
    # Default to the primary source if no filter is provided for better front-end use
    if not signal_source:
        query = query.filter_by(signal_source=SIGNAL_SOURCE_WATCHLIST)
    else:
        query = query.filter_by(signal_source=signal_source)
        
    reports = query.order_by(AggregatedPerformance.report_date.desc(), AggregatedPerformance.created_at.desc()).all()
    
    output = []
    for r in reports:
        output.append({
            'report_id': r.id,
            'report_date': r.report_date,
            'period_type': r.period_type,
            'signal_source': r.signal_source,
            'total_signals': r.total_signals,
            'successful_signals': r.successful_signals,
            'win_rate': r.win_rate,
            'total_profit_percent': r.total_profit_percent,
            'total_loss_percent': r.total_loss_percent,
            'net_profit_percent': r.net_profit_percent,
            'average_profit_per_win': r.average_profit_per_win,
            'average_loss_per_loss': r.average_loss_per_loss,
            'created_at': safe_date_format(r.created_at, '%Y-%m-%d %H:%M:%S'),
            'updated_at': safe_date_format(r.updated_at, '%Y-%m-%d %H:%M:%S')
        })
    
    logger.info(f"Retrieved {len(output)} aggregated performance reports.")
    return output


def get_overall_performance_summary() -> Dict:
    """Calculates application performance summary based on WeeklyWatchlistService."""
    logger.info("Calculating application performance summary based on WeeklyWatchlistService.")
    
    signal_source = SIGNAL_SOURCE_WATCHLIST

    # ⬅️ اصلاح وضعیت‌های بسته در Overall Summary: باید با وضعیت‌های جدید هماهنگ باشد
    all_signals = SignalsPerformance.query.filter(
        SignalsPerformance.status.in_(['closed_win', 'closed_loss', 'closed_neutral', 'closed_expired']),
        SignalsPerformance.signal_source == signal_source
    ).all()
    
    overall_summary_data = {
        "total_signals_evaluated": 0,
        "overall_win_rate": 0.0,
        "average_profit_per_win_overall": 0.0,
        "average_loss_per_loss_overall": 0.0,
        "overall_net_profit_percent": 0.0
    }
    signals_by_source_data = {} # Only one source for now, but kept for scalability

    if all_signals:
        df = pd.DataFrame([s.__dict__ for s in all_signals]).drop(columns=['_sa_instance_state'], errors='ignore')
        # Ensure profit_loss_percent is numeric for calculations
        df['profit_loss_percent'] = pd.to_numeric(df['profit_loss_percent'], errors='coerce').fillna(0)

        overall_summary_data["total_signals_evaluated"] = len(df)
        total_wins = df[df['profit_loss_percent'] > 0].shape[0]
        
        overall_summary_data["overall_win_rate"] = round((total_wins / len(df)) * 100 if len(df) > 0 else 0.0, 2)
        overall_summary_data["overall_net_profit_percent"] = round(df['profit_loss_percent'].sum(), 2)

        winning_signals_pl = df[df['profit_loss_percent'] > 0]['profit_loss_percent']
        losing_signals_pl = df[df['profit_loss_percent'] < 0]['profit_loss_percent']

        overall_summary_data["average_profit_per_win_overall"] = round(winning_signals_pl.mean() if not winning_signals_pl.empty else 0.0, 2)
        overall_summary_data["average_loss_per_loss_overall"] = round(losing_signals_pl.mean() if not losing_signals_pl.empty else 0.0, 2)
        
        # Breakdown by source (only one source now, but structure maintained)
        for source in df['signal_source'].unique():
            source_df = df[df['signal_source'] == source]
            source_total = len(source_df)
            source_wins = source_df[source_df['profit_loss_percent'] > 0].shape[0]
            source_losses = source_df[source_df['profit_loss_percent'] < 0].shape[0]
            source_neutral = source_df[source_df['profit_loss_percent'] == 0].shape[0]
            source_win_rate = (source_wins / source_total) * 100 if source_total > 0 else 0.0
            source_net_profit = source_df['profit_loss_percent'].sum()
            
            signals_by_source_data[source] = {
                "total_signals": source_total,
                "wins": source_wins,
                "losses": source_losses,
                "neutral": source_neutral,
                "win_rate": round(source_win_rate, 2),
                "net_profit_percent": round(source_net_profit, 2)
            }

    today_jdate_str = get_today_jdate_str()

    # Retrieve latest Weekly and Monthly aggregated reports for today
    latest_weekly_report = AggregatedPerformance.query.filter_by(
        report_date=today_jdate_str,
        period_type='weekly',
        signal_source=signal_source
    ).first()

    latest_monthly_report = AggregatedPerformance.query.filter_by(
        report_date=today_jdate_str,
        period_type='monthly',
        signal_source=signal_source
    ).first()

    annual_profit_loss = get_annual_profit_loss_summary()

    summary = {
        "overall_performance": overall_summary_data,
        "signals_by_source": signals_by_source_data,
        "weekly_performance": {
            "win_rate": latest_weekly_report.win_rate if latest_weekly_report else 0.0,
            "net_profit_percent": latest_weekly_report.net_profit_percent if latest_weekly_report else 0.0
        },
        "monthly_performance": {
            "win_rate": latest_monthly_report.win_rate if latest_monthly_report else 0.0,
            "net_profit_percent": latest_monthly_report.net_profit_percent if latest_monthly_report else 0.0
        },
        "annual_profit_loss": annual_profit_loss,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    logger.info("Application performance summary calculated successfully.")
    return summary

def get_annual_profit_loss_summary() -> float:
    """Calculates annual profit/loss summary for WeeklyWatchlistService."""
    logger.info("Calculating annual profit/loss summary for WeeklyWatchlistService.")
    
    current_jalali_year = jdatetime.date.today().year
    start_of_jalali_year_str = f"{current_jalali_year}-01-01"
    signal_source = SIGNAL_SOURCE_WATCHLIST

    # ⬅️ اصلاح وضعیت‌های بسته در Annual Summary: باید با وضعیت‌های جدید هماهنگ باشد
    annual_signals = SignalsPerformance.query.filter(
        SignalsPerformance.status.in_(['closed_win', 'closed_loss', 'closed_neutral', 'closed_expired']),
        SignalsPerformance.jentry_date >= start_of_jalali_year_str,
        SignalsPerformance.signal_source == signal_source
    ).all()

    total_annual_profit_loss_percent = 0.0
    if annual_signals:
        df_annual = pd.DataFrame([s.__dict__ for s in annual_signals]).drop(columns=['_sa_instance_state'], errors='ignore')
        df_annual['profit_loss_percent'] = pd.to_numeric(df_annual['profit_loss_percent'], errors='coerce').fillna(0)
        total_annual_profit_loss_percent = round(df_annual['profit_loss_percent'].sum(), 2)
    
    logger.info(f"Annual profit/loss from {start_of_jalali_year_str} to {get_today_jdate_str()}: {total_annual_profit_loss_percent:.2f}%")
    return total_annual_profit_loss_percent


def get_detailed_signals_performance(status_filter: Optional[str] = None, period_filter: Optional[str] = None) -> List[Dict]:
    """Retrieves detailed SignalsPerformance records with optional filters."""
    logger.info(f"Retrieving detailed SignalsPerformance records (Status: {status_filter}, Period: {period_filter}).")
    query = SignalsPerformance.query.filter(SignalsPerformance.signal_source == SIGNAL_SOURCE_WATCHLIST)

    if status_filter:
        query = query.filter(SignalsPerformance.status == status_filter)

    # Added logic for 'previous_week' filter based on jexit_date
    if period_filter == 'previous_week':
        # فرض: هفته قبلی از 14 روز قبل شروع و در 7 روز قبل به اتمام رسیده است.
        today_greg = jdatetime.date.today().togregorian()
        exit_date_greg = _get_last_market_day_date_gregorian(today_greg) # استفاده از آخرین روز معاملاتی برای مرجع

        end_date_greg = exit_date_greg - timedelta(days=7)
        start_date_greg = exit_date_greg - timedelta(days=14)

        end_jdate_str = convert_gregorian_to_jalali(end_date_greg)
        start_jdate_str = convert_gregorian_to_jalali(start_date_greg)

        if start_jdate_str and end_jdate_str:
            # ⬅️ اصلاح: فیلتر بر اساس وضعیت‌های نهایی
            query = query.filter(
                SignalsPerformance.jexit_date >= start_jdate_str,
                SignalsPerformance.jexit_date <= end_jdate_str,
                SignalsPerformance.status.in_(['closed_win', 'closed_loss', 'closed_neutral', 'closed_expired'])
            )
            logger.info(f"Filtering for previous week: jexit_date from {start_jdate_str} to {end_jdate_str}")
        else:
            logger.warning("Could not determine Jalali date range for 'previous_week' filter.")
            return []
    
    # Filter only closed signals unless an explicit status is requested
    if not status_filter:
         # ⬅️ اصلاح: فیلتر بر اساس وضعیت‌های نهایی
         query = query.filter(
             SignalsPerformance.status.in_(['closed_win', 'closed_loss', 'closed_neutral', 'closed_expired'])
         )

    signals = query.order_by(SignalsPerformance.exit_date.desc(), SignalsPerformance.created_at.desc()).all()

    output = []
    for s in signals:
        output.append({
            'signal_id': s.signal_id,
            'symbol_id': s.symbol_id,
            'symbol_name': s.symbol_name,
            'outlook': s.outlook,
            'reason': s.reason,
            'entry_price': s.entry_price,
            'jentry_date': s.jentry_date,
            'entry_date': safe_date_format(s.entry_date),
            'exit_price': s.exit_price,
            'jexit_date': s.jexit_date,
            'exit_date': safe_date_format(s.exit_date),
            'profit_loss_percent': s.profit_loss_percent,
            'status': s.status,
            'signal_source': s.signal_source,
            'probability_percent': s.probability_percent,
            'created_at': safe_date_format(s.created_at, '%Y-%m-%d %H:%M:%S'),
            'updated_at': safe_date_format(s.updated_at, '%Y-%m-%d %H:%M:%S')
        })
    
    logger.info(f"Retrieved {len(output)} detailed SignalsPerformance records.")
    return output
