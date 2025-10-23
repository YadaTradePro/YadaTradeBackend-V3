# routes/performance.py
from flask_restx import Namespace, Resource, fields, reqparse
from flask_jwt_extended import jwt_required
from flask import current_app, request
import logging

logger = logging.getLogger(__name__)

from services import performance_service
from extensions import db
from models import AggregatedPerformance, SignalsPerformance # اضافه کردن مدل‌ها

performance_ns = Namespace('performance', description='Performance metrics and pipeline operations')

# --------------------------------------------------------------------------------
# --- Models ---
# --------------------------------------------------------------------------------

# 1. مدل برای یک دوره زمانی (weekly/monthly/annual)
period_performance_model = performance_ns.model('PeriodPerformance', {
    'report_date': fields.String(description='Jalali report date'),
    'period_type': fields.String(description='Period type (weekly, monthly, annual)'),
    'total_signals': fields.Integer,
    'successful_signals': fields.Integer,
    'win_rate': fields.Float,
    'net_profit_percent': fields.Float,
    'total_profit_percent': fields.Float,
    'total_loss_percent': fields.Float,
    'average_profit_per_win': fields.Float,
    'average_loss_per_loss': fields.Float,
    'updated_at': fields.String(description='Timestamp of last update')
})

# 2. مدل نهایی برای Aggregated Performance (درخواست کاربر)
aggregated_summary_output_model = performance_ns.model('AggregatedSummaryOutput', {
    'weekly': fields.Nested(period_performance_model, description='Weekly aggregation results'),
    'monthly': fields.Nested(period_performance_model, description='Monthly aggregation results'),
    'annual': fields.Nested(period_performance_model, description='Annual aggregation results'),
})

# 3. مدل برای Signals Performance
detailed_signal_performance_model = performance_ns.model('DetailedSignalPerformanceModel', {
    'signal_id': fields.String,
    'symbol_id': fields.String,
    'symbol_name': fields.String,
    'signal_source': fields.String,
    'outlook': fields.String,
    'entry_price': fields.Float,
    'jentry_date': fields.String,
    'exit_price': fields.Float,
    'jexit_date': fields.String,
    'profit_loss_percent': fields.Float,
    'status': fields.String, # 'active', 'evaluated', closed_...
    'updated_at': fields.String
})


# --------------------------------------------------------------------------------
# --- 1. POST Endpoint: Execute Performance Pipeline ---
# --------------------------------------------------------------------------------

@performance_ns.route('/run-pipeline')
class RunPipelineResource(Resource):
    @performance_ns.doc(security='Bearer Auth', description='Executes the full 3-phase performance pipeline: 1. Close signals in WeeklyWatchlist (to evaluated), 2. Update SignalsPerformance, 3. Calculate AggregatedPerformance (weekly, monthly, annual).')
    @jwt_required()
    @performance_ns.response(200, 'Performance pipeline executed successfully.')
    @performance_ns.response(500, 'Error during pipeline execution.')
    def post(self):
        current_app.logger.info("API call: Initiating full performance pipeline.")
        try:
            # فراخوانی تابع واحدی که هر 3 فاز را اجرا می‌کند
            success, message = performance_service.run_weekly_performance_pipeline() 
            
            if success:
                return {"message": message}, 200
            else:
                return {"message": message}, 500
        except Exception as e:
            current_app.logger.error(f"Error during pipeline execution: {e}", exc_info=True)
            return {"message": f"A critical error occurred: {str(e)}"}, 500

# --------------------------------------------------------------------------------
# --- 2. GET Endpoint: Aggregated Performance (Weekly, Monthly, Annual) ---
# --------------------------------------------------------------------------------

@performance_ns.route('/aggregated')
class AggregatedPerformanceResource(Resource):
    @performance_ns.doc(security='Bearer Auth', description='Retrieves the latest Weekly, Monthly, and Annual performance reports.')
    @jwt_required() 
    @performance_ns.marshal_with(aggregated_summary_output_model) 
    def get(self):
        logger.info("API call: Retrieving Aggregated Performance Summary.")
        
        # ما نیاز داریم که AggregatedPerformance را بر اساس report_date آخرین روز معاملاتی (یا امروز) فیلتر کنیم
        
        # 1. پیدا کردن تاریخ گزارش (آخرین تاریخ موجود)
        latest_report = db.session.query(AggregatedPerformance.report_date).order_by(
            AggregatedPerformance.report_date.desc()
        ).first()

        if not latest_report:
            return {"weekly": {}, "monthly": {}, "annual": {}}, 200

        latest_report_date = latest_report[0]
        
        # 2. کوئری کردن رکوردهای هفتگی، ماهانه و سالانه برای آن تاریخ
        all_periods = AggregatedPerformance.query.filter(
            AggregatedPerformance.report_date == latest_report_date
        ).all()
        
        # 3. تبدیل به فرمت درخواستی (weekly, monthly, annual)
        summary_data = {
            'weekly': {},
            'monthly': {},
            'annual': {}
        }
        
        def format_record(record):
            return {
                "report_date": record.report_date,
                "period_type": record.period_type,
                "total_signals": record.total_signals,
                "successful_signals": record.successful_signals,
                "win_rate": record.win_rate,
                "net_profit_percent": record.net_profit_percent,
                "total_profit_percent": record.total_profit_percent,
                "total_loss_percent": record.total_loss_percent,
                "average_profit_per_win": record.average_profit_per_win,
                "average_loss_per_loss": record.average_loss_per_loss,
                "updated_at": record.updated_at.strftime('%Y-%m-%d %H:%M:%S') if record.updated_at else None,
            }

        for record in all_periods:
            if record.period_type in summary_data:
                summary_data[record.period_type] = format_record(record)
                
        # 4. بازگرداندن داده‌ها
        return summary_data, 200

# --------------------------------------------------------------------------------
# --- 3. GET Endpoint: Latest Updated Signals Performance Details ---
# --------------------------------------------------------------------------------

@performance_ns.route('/signals-details') 
class SignalsDetailsResource(Resource):
    @performance_ns.doc(security='Bearer Auth', description='Retrieves a list of SignalsPerformance records, showing the latest entry for each signal (ordered by updated_at).')
    @jwt_required() 
    @performance_ns.marshal_list_with(detailed_signal_performance_model) 
    def get(self):
        logger.info("API call: Retrieving Latest Updated Signals Performance.")
        
        # هدف: نمایش رکوردهایی که اخیراً به‌روزرسانی شده‌اند
        
        # 1. کوئری: انتخاب تمام سیگنال‌ها
        # 2. مرتب‌سازی بر اساس updated_at نزولی
        # 3. برای جلوگیری از بازگرداندن تمام تاریخچه، می‌توانیم آن را به آخرین 'n' رکورد محدود کنیم (مثلاً 50 تای آخر)
        # اگر منظور شما واقعاً 'آخرین رکوردهایی که آپدیت شدند' است (مثل سیگنال‌هایی که همین حالا evaluated شدند):

        try:
            # اینجا می‌توانیم سیگنال‌هایی که در 24 ساعت گذشته یا اخیراً evaluated شدند را بازگردانیم
            # ساده‌ترین راه برای 'نشان دادن آخرین رکوردها' این است که بر اساس updated_at مرتب کنیم
            
            # برای مثال، 100 رکورد آخر که بروزرسانی شده‌اند را باز می‌گردانیم:
            latest_signals = db.session.query(SignalsPerformance).order_by(
                SignalsPerformance.updated_at.desc()
            ).limit(100).all()
            
            # تبدیل به فرمت دیکشنری
            signals_details = []
            for signal in latest_signals:
                signals_details.append({
                    'signal_id': signal.signal_id,
                    'symbol_id': signal.symbol_id,
                    'symbol_name': signal.symbol_name,
                    'signal_source': signal.signal_source,
                    'outlook': signal.outlook,
                    'entry_price': signal.entry_price,
                    'jentry_date': signal.jentry_date,
                    'exit_price': signal.exit_price,
                    'jexit_date': signal.jexit_date,
                    'profit_loss_percent': signal.profit_loss_percent,
                    'status': signal.status,
                    'updated_at': signal.updated_at.strftime('%Y-%m-%d %H:%M:%S') if signal.updated_at else None,
                })
                
            return signals_details, 200
        except Exception as e:
            logger.error(f"Error retrieving latest signals performance: {e}", exc_info=True)
            return {"message": f"An error occurred while retrieving signals performance: {str(e)}"}, 500
