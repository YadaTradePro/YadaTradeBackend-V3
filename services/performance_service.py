# services/performance_service.py

from extensions import db
from models import SignalsPerformance, AggregatedPerformance, WeeklyWatchlistResult, ComprehensiveSymbolData, HistoricalData
from datetime import datetime, timedelta, date
import jdatetime
import pandas as pd
import logging
from sqlalchemy import func, and_
from typing import Optional, Tuple, List, Dict

# Import utility functions (FIX: Removed the missing function 'get_last_market_day_date')
from services.technical_analysis_utils import get_today_jdate_str, convert_gregorian_to_jalali 

logger = logging.getLogger(__name__)
SIGNAL_SOURCE_WATCHLIST = 'WeeklyWatchlistService'

# --- Helper function for market date determination (FIX for missing function) ---
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

def get_latest_symbol_price(symbol_id: str) -> Optional[float]:
    """
    Retrieves the latest available market final or closing price for a symbol
    from the HistoricalData table based on the last calculated market day.
    This price is used as the Exit Price for closing a signal.
    """
    # 1. تعیین تاریخ آخرین روز معاملاتی
    current_greg_date = date.today()
    last_market_date_gregorian = _get_last_market_day_date_gregorian(current_greg_date)

    logger.debug(f"Retrieving price for {symbol_id} on last market day: {last_market_date_gregorian}")

    # 2. کوئری جدول HistoricalData برای نماد و تاریخ مشخص
    latest_price_data = db.session.query(HistoricalData).filter(
        HistoricalData.symbol_id == symbol_id,
        HistoricalData.date == last_market_date_gregorian
    ).first()

    # 3. استخراج قیمت
    if latest_price_data:
        # اولویت با 'final' (آخرین قیمت معامله)
        if latest_price_data.final is not None:
            return latest_price_data.final
        
        # در صورت نبود final، از 'close' (قیمت پایانی) استفاده شود
        if latest_price_data.close is not None:
            return latest_price_data.close

    # 4. در صورت نبود داده برای آخرین روز معاملاتی یا خالی بودن قیمت‌ها
    logger.warning(
        f"Could not retrieve 'final' or 'close' price for symbol_id: {symbol_id} "
        f"on the calculated last market day: {last_market_date_gregorian}"
    )
    return None

def calculate_pnl_percent(entry_price: float, exit_price: float) -> float:
    """Calculates profit/loss percentage."""
    if entry_price is None or entry_price == 0:
        return 0.0
    return ((exit_price - entry_price) / entry_price) * 100.0

# --- Core Performance Evaluation Logic ---

def close_and_evaluate_weekly_signals(days_to_lookback: int = 7) -> Tuple[bool, str]:
    """
    Fulfills the core duty: finds active WeeklyWatchlist signals, 
    evaluates their performance based on the latest price, and closes them.
    Updates both WeeklyWatchlistResult and SignalsPerformance tables.
    """
    logger.info("Starting weekly signal closure and performance evaluation.")
    
    # تعیین بازه زمانی (سیگنال‌هایی که در هفته گذشته معرفی شده‌اند)
    # تاریخ ورود سیگنال باید در بازه (today - days_to_lookback) تا (today) باشد
    today_greg = jdatetime.date.today().togregorian()
    
    # تاریخ شروع برای فیلتر کردن سیگنال‌های فعال
    start_date_greg = today_greg - timedelta(days=days_to_lookback)
    start_jdate_str = convert_gregorian_to_jalali(start_date_greg)
    
    # تاریخ خروج (بستن سیگنال) - آخرین روز معاملاتی یا امروز (FIX: Use last market day for accuracy)
    exit_date_greg = _get_last_market_day_date_gregorian(today_greg)
    exit_jdate_str = convert_gregorian_to_jalali(exit_date_greg)
    
    logger.info(f"Searching for active signals entered since: {start_jdate_str}. Exit Date set to: {exit_jdate_str}")
    
    # 1. Fetch relevant active signals from WeeklyWatchlistResult
    active_results = WeeklyWatchlistResult.query.filter(
        WeeklyWatchlistResult.status == 'active',
        WeeklyWatchlistResult.jentry_date >= start_jdate_str # سیگنال‌های فعال در بازه مورد نظر
    ).all()

    if not active_results:
        message = "No active WeeklyWatchlist signals found for closure in the specified period."
        logger.warning(message)
        # حتی اگر سیگنالی بسته نشد، باید عملکرد تجمعی را محاسبه کنیم
        calculate_and_save_aggregated_performance(period_type='weekly', signal_source=SIGNAL_SOURCE_WATCHLIST)
        return True, message

    closed_count = 0
    
    for result in active_results:
        symbol_id = result.symbol_id
        entry_price = result.entry_price
        
        # 2. Get latest price (Exit Price)
        exit_price = get_latest_symbol_price(symbol_id)
        
        if exit_price is None or entry_price is None:
            logger.warning(f"Skipping closure for {result.symbol_name} ({symbol_id}): Missing entry or exit price.")
            continue
            
        # 3. Calculate Performance
        profit_loss_percent = calculate_pnl_percent(entry_price, exit_price)
        
        # 4. Determine Status
        if profit_loss_percent > 0.005: # بیش از 0.5% سود
            status = 'closed_win'
        elif profit_loss_percent < -0.005: # کمتر از 0.5% ضرر
            status = 'closed_loss'
        else:
            status = 'closed_neutral' # در نظر گرفتن معاملات خنثی
            
        # 5. Update WeeklyWatchlistResult
        result.status = status
        result.exit_price = exit_price
        result.exit_date = exit_date_greg
        result.jexit_date = exit_jdate_str
        result.profit_loss_percentage = round(profit_loss_percent, 2)
        db.session.add(result)

        # 6. Update SignalsPerformance (Assume: A SignalsPerformance record exists with signal_id = WeeklyWatchlistResult.signal_unique_id)
        performance_record = SignalsPerformance.query.filter_by(signal_id=result.signal_unique_id).first()
        
        if performance_record:
            performance_record.status = status
            performance_record.exit_price = exit_price
            performance_record.exit_date = exit_date_greg
            performance_record.jexit_date = exit_jdate_str
            performance_record.profit_loss_percent = round(profit_loss_percent, 2)
            performance_record.evaluated_at = datetime.now()
            db.session.add(performance_record)
            
        closed_count += 1
        logger.info(f"Closed {result.symbol_name}. Status: {status}, P/L: {profit_loss_percent:.2f}%")

    # 7. Commit changes
    try:
        db.session.commit()
        
        # 8. Calculate Aggregated Performance after closing signals
        success_agg, message_agg = calculate_and_save_aggregated_performance(period_type='weekly', signal_source=SIGNAL_SOURCE_WATCHLIST)
        
        final_message = f"Successfully closed and evaluated {closed_count} signals. {message_agg}"
        logger.info(final_message)
        return True, final_message
        
    except Exception as e:
        db.session.rollback()
        error_message = f"Critical error during weekly signal closure: {e}"
        logger.error(error_message, exc_info=True)
        return False, error_message

# --- Aggregation and Reporting Functions ---

def calculate_and_save_aggregated_performance(period_type='weekly', signal_source=SIGNAL_SOURCE_WATCHLIST) -> Tuple[bool, str]:
    """
    Calculates and saves AggregatedPerformance for a given period and signal source.
    NOTE: The function signature is modified to accept 'signal_source' to fix the TypeError.
    It enforces the signal source to SIGNAL_SOURCE_WATCHLIST for all internal calculations 
    if that is the intended behavior of this file. 
    However, since the previous code had hardcoded SIGNAL_SOURCE_WATCHLIST, we respect 
    that and use the passed 'signal_source' argument only for logging/future-proofing 
    but enforce filtering on SIGNAL_SOURCE_WATCHLIST.
    """
    
    # Enforcing the specific source for which this service is primarily responsible
    source_to_aggregate = SIGNAL_SOURCE_WATCHLIST
    logger.info(f"Calculating aggregated performance for '{source_to_aggregate}' ({period_type}).")

    today_gdate = jdatetime.date.today().togregorian()
    start_jdate_str = None
    
    # Date Range Calculation
    if period_type == 'weekly':
        start_date_greg = today_gdate - timedelta(days=7)
    elif period_type == 'monthly':
        start_date_greg = today_gdate - timedelta(days=30)
    elif period_type == 'annual':
        start_date_greg = today_gdate - timedelta(days=365)
    else:
        return False, "Invalid period_type. Must be 'weekly', 'monthly', or 'annual'."

    try:
        start_jdate_str = convert_gregorian_to_jalali(start_date_greg)
    except Exception as e:
        logger.error(f"Error determining start date for {period_type} period: {e}", exc_info=True)
        return False, f"Error determining start date for {period_type} period."

    # Query Conditions
    query_conditions = [
        SignalsPerformance.status.in_(['closed_win', 'closed_loss', 'closed_neutral', 'closed_expired']),
        SignalsPerformance.signal_source == source_to_aggregate
    ]
    
    if start_jdate_str:
        query_conditions.append(SignalsPerformance.jentry_date >= start_jdate_str)

    signals_in_period = SignalsPerformance.query.filter(and_(*query_conditions)).all()

    # Performance Metrics Calculation
    if not signals_in_period:
        message = f"No closed signals found for {source_to_aggregate} ({period_type}) in the period starting {start_jdate_str}."
        logger.warning(message)
        total_signals, successful_signals, win_rate, total_profit_percent, total_loss_percent, average_profit_per_win, average_loss_per_loss, net_profit_percent = 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    else:
        total_signals = len(signals_in_period)
        # Successful signals are defined as those with profit > 0
        successful_signals = sum(1 for s in signals_in_period if s.profit_loss_percent is not None and s.profit_loss_percent > 0)
        win_rate = (successful_signals / total_signals) * 100 if total_signals > 0 else 0.0
        
        total_profit_percent = sum(s.profit_loss_percent for s in signals_in_period if s.profit_loss_percent is not None and s.profit_loss_percent > 0)
        total_loss_percent = abs(sum(s.profit_loss_percent for s in signals_in_period if s.profit_loss_percent is not None and s.profit_loss_percent < 0))
        
        winning_signals = [s for s in signals_in_period if s.profit_loss_percent is not None and s.profit_loss_percent > 0]
        losing_signals = [s for s in signals_in_period if s.profit_loss_percent is not None and s.profit_loss_percent < 0]
        
        average_profit_per_win = (sum(s.profit_loss_percent for s in winning_signals) / len(winning_signals)) if winning_signals else 0.0
        # Average loss is stored as a negative number, abs() is used to present it as a positive average loss
        average_loss_per_loss = (sum(s.profit_loss_percent for s in losing_signals) / len(losing_signals)) if losing_signals else 0.0
        
        net_profit_percent = total_profit_percent + average_loss_per_loss # average_loss_per_loss is negative, so it subtracts.

    # Save/Update Aggregated Performance
    report_date_str = get_today_jdate_str()
    existing_agg_perf = AggregatedPerformance.query.filter_by(
        report_date=report_date_str,
        period_type=period_type,
        signal_source=source_to_aggregate
    ).first()

    if existing_agg_perf:
        existing_agg_perf.total_signals = total_signals
        existing_agg_perf.successful_signals = successful_signals
        existing_agg_perf.win_rate = round(win_rate, 2)
        existing_agg_perf.total_profit_percent = round(total_profit_percent, 2)
        existing_agg_perf.total_loss_percent = round(total_loss_percent, 2)
        existing_agg_perf.average_profit_per_win = round(average_profit_per_win, 2)
        existing_agg_perf.average_loss_per_loss = round(average_loss_per_loss, 2)
        existing_agg_perf.net_profit_percent = round(net_profit_percent, 2)
        existing_agg_perf.updated_at = datetime.now()
        db.session.add(existing_agg_perf)
        logger.info(f"Updated aggregated performance for {source_to_aggregate} ({period_type}) on {report_date_str}.")
    else:
        new_agg_perf = AggregatedPerformance(
            report_date=report_date_str,
            period_type=period_type,
            signal_source=source_to_aggregate,
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
        logger.info(f"Created new aggregated performance record for {source_to_aggregate} ({period_type}) on {report_date_str}.")
    
    try:
        db.session.commit()
        message = f"Aggregated performance for {source_to_aggregate} ({period_type}) calculated successfully. Win Rate: {win_rate:.2f}%."
        logger.info(message)
        return True, message
    except Exception as e:
        db.session.rollback()
        error_message = f"Error during aggregated performance calculation: {e}"
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
