# routes/weekly_watchlist.py
from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import jwt_required
from flask import current_app
import logging 
import uuid 


logger = logging.getLogger(__name__)


# --- FIX 1: ایمپورت کردن کلاس به جای ماژول ---
from services.weekly_watchlist_service import WeeklyWatchlistService
# (سرویس پرفورمنس فعلا استفاده نمی‌شود اما آن را نگه می‌داریم)
from services import performance_service 

weekly_watchlist_ns = Namespace('weekly_watchlist', description='Weekly Watchlist operations')
weekly_watchlist_result_model = weekly_watchlist_ns.model('WeeklyWatchlistResultModel', {
    'signal_unique_id': fields.String(description='Unique ID for the signal'),
    #'symbol': fields.String(description='Symbol ID'), # Corrected to use 'symbol' directly from the object
    'company_name': fields.String(description='Company Name'),
    'symbol_name': fields.String(description='Symbol Name'),
    'entry_price': fields.Float(description='Entry Price'),
    'jentry_date': fields.String(description='Jalali Entry Date'),
    'outlook': fields.String(description='Outlook'),
    'reason': fields.String(description='Reason'),
    'probability_percent': fields.Float(description='Probability Percent'),
    'status': fields.String(description='Status (active, closed_win, closed_loss, closed_neutral)'),
    'exit_price': fields.Float(description='Exit Price'),
    'jexit_date': fields.String(description='Jalali Exit Date'),
    'profit_loss_percentage': fields.Float(description='Profit/Loss Percentage'),
    'created_at': fields.String(description='Creation Timestamp'),
    'updated_at': fields.String(description='Last Updated Timestamp')
})
weekly_watchlist_response_model = weekly_watchlist_ns.model('WeeklyWatchlistResponse', {
    'top_watchlist_stocks': fields.List(fields.Nested(weekly_watchlist_result_model), description='List of top Weekly Watchlist stocks'),
    'last_updated': fields.String(description='Timestamp of last update')
})

@weekly_watchlist_ns.route('/run_selection')
class RunWeeklyWatchlistSelectionResource(Resource):
    @weekly_watchlist_ns.doc(security='Bearer Auth')
    @jwt_required() 
    def post(self):
        logger.info("Received manual request to run Weekly Watchlist selection.")
        try:
            # --- FIX 2: ابتدا یک نمونه از سرویس بسازید ---
            service_instance = WeeklyWatchlistService()
            
            # --- FIX 3: متد را روی نمونه صدا بزنید ---
            # (متد جدید فقط لیست نتایج را برمی‌گرداند )
            selected_symbols_objects = service_instance.run_watchlist_generation(parallel=True)
            
            # --- FIX 4: پیام را به صورت دستی بسازید ---
            message = f"Weekly Watchlist selection completed. Found {len(selected_symbols_objects)} strong signals."
            
            # (اختیاری اما پیشنهادی): تبدیل آبجکت‌های SimpleNamespace به دیکشنری برای ارسال امن در JSON
            selected_symbols_list = [vars(s) for s in selected_symbols_objects]
            
            return {"message": message, "selected_symbols": selected_symbols_list}, 200 
        except Exception as e:
            logger.error(f"Error running Weekly Watchlist selection: {e}", exc_info=True)
            return {"message": f"An error occurred during Weekly Watchlist selection: {str(e)}"}, 500

        
    
@weekly_watchlist_ns.route('/results')
class GetWeeklyWatchlistResultsResource(Resource):
    @weekly_watchlist_ns.doc(security='Bearer Auth')
    @jwt_required() 
    @weekly_watchlist_ns.marshal_with(weekly_watchlist_response_model)
    def get(self):
        logger.info("API call: Retrieving Weekly Watchlist Results.")
        try:
            # --- FIX 5: ساخت نمونه از سرویس ---
            service_instance = WeeklyWatchlistService()
            
            # --- FIX 6: فراخوانی متد روی نمونه ---
            # (این متد فقط لیست سهام را برمی‌گرداند )
            results_list = service_instance.get_latest_watchlist()
            
            # --- FIX 7: ساخت دیکشنری خروجی به صورت دستی تا با مدل مطابقت کند ---
            last_updated_date = "نامشخص"
            if results_list:
                # سرویس بر اساس تاریخ مرتب کرده است [cite: 151]
                last_updated_date = results_list[0].get('jentry_date', 'نامشخص')
                
            response_data = {
                "top_watchlist_stocks": results_list,
                "last_updated": last_updated_date
            }
            
            return response_data, 200
        except Exception as e:
            logger.error(f"Error retrieving Weekly Watchlist results: {e}", exc_info=True)
            # 💡 نکته: مدل marshal_with در زمان خطا ممکن است باعث خطای 500 شود
            # بهتر است در بلوک except یک دیکشنری خالی مطابق مدل برگردانید
            return {"message": f"An error occurred while retrieving Weekly Watchlist results: {str(e)}"}, 500
