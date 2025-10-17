# -*- coding: utf-8 -*-
# routes/data_fetching_routes.py
# مسئول: تعریف اندپوینت‌های مربوط به واکشی داده، تحلیل تکنیکال، و پیش‌بینی‌های ML
# این فایل برای کار با ساختار جدید سرویس‌ها بازنویسی شده است.

import logging
import traceback
from datetime import date
import jdatetime

# ✅ اصلاح: jsonify از flask حذف شد زیرا با Flask-RESTX نیازی به آن نیست و باعث خطای TypeError می‌شود.
from flask_restx import Namespace, Resource, fields, reqparse

from extensions import db
from sqlalchemy.orm import sessionmaker, Session

# =========================
# Namespace برای Flask-RESTX
# =========================
data_ns = Namespace('data', description='عملیات واکشی داده، تحلیل و پیش‌بینی')

# =========================
# توابع کمکی
# =========================
def get_session_local() -> Session:
    """یک Session محلی برای دیتابیس ایجاد می‌کند تا از کانتکست Flask مستقل باشد."""
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db.engine)
    return SessionLocal()

def parse_date(value: str) -> date | None:
    """تاریخ را از فرمت رشته (YYYY-MM-DD) شمسی یا میلادی به شیء date میلادی تبدیل می‌کند."""
    if not isinstance(value, str) or not value:
        return None
    try:
        # 1. تلاش برای پارس کردن به عنوان تاریخ میلادی
        return date.fromisoformat(value)
    except ValueError:
        # 2. تلاش برای پارس کردن به عنوان تاریخ شمسی
        try:
            j_year, j_month, j_day = map(int, value.split('-'))
            return jdatetime.date(j_year, j_month, j_day).togregorian()
        except Exception:
            return None

# =========================
# --- API Models for Swagger/RESTX Documentation ---
# =========================
# مدل‌های داده برای مستندسازی خودکار API (بدون تغییر)
historical_data_model = data_ns.model('HistoricalData', {
    'date': fields.String(description='Gregorian date (YYYY-MM-DD)'),
    'jdate': fields.String(description='Persian date (YYYY-MM-DD)'),
    'open': fields.Float,
    'high': fields.Float,
    'low': fields.Float,
    'close': fields.Float,
    'last_price': fields.Float,
    'volume': fields.Integer,
    # ... سایر فیلدها در اینجا قرار می‌گیرند ...
    'buyers_count': fields.Integer
})

ml_prediction_model = data_ns.model('MLPredictionModel', {
    'id': fields.Integer(readOnly=True, description='The unique identifier of the prediction'),
    'symbol_id': fields.String(required=True, description='The ID of the stock symbol'),
    'symbol_name': fields.String(required=True, description='The name of the stock symbol'),
    'prediction_date': fields.String(required=True, description='Gregorian date when the prediction was made'),
    'jprediction_date': fields.String(required=True, description='Jalali date when the prediction was made'),
    'prediction_period_days': fields.Integer(description='Number of days for the prediction horizon'),
    'predicted_trend': fields.String(required=True, description='Predicted trend: Uptrend, Downtrend, or Sideways'),
    'prediction_probability': fields.Float(required=True, description='Probability/confidence of the predicted trend'),
    'actual_trend_outcome': fields.String(description='Actual trend outcome after the period'),
    'is_prediction_accurate': fields.Boolean(description='True if prediction was accurate'),
    'model_version': fields.String(description='Version of the ML model used'),
})


# =================================================================================
# --- Parsers for API Endpoints ---
# =================================================================================
# پارسر برای اندپوینت بازسازی کامل داده‌ها
rebuild_parser = reqparse.RequestParser()
rebuild_parser.add_argument('batch_size', type=int, default=50, help='تعداد نمادها در هر بچ پردازشی')
rebuild_parser.add_argument('commit_batch_size', type=int, default=100, help='تعداد نمادهای پردازش‌شده قبل از هر commit')

# پارسر برای اندپوینت‌های تحلیل که می‌توانند لیست نمادها را دریافت کنند
analysis_parser = reqparse.RequestParser()
analysis_parser.add_argument('limit', type=int, required=False, help='محدودیت تعداد نمادها برای پردازش')
analysis_parser.add_argument('days_limit', type=int, required=False, help='محدودیت تعداد روزهای تاریخی برای تحلیل هر نماد')
analysis_parser.add_argument(
    'specific_symbols',
    type=list,
    location='json',
    required=False,
    help='لیستی از نام یا کد نمادهای خاص برای تحلیل (مثال: ["خودرو", "خساپا"])'
)

# پارسر برای دریافت داده‌های تاریخی یک نماد
historical_data_parser = reqparse.RequestParser()
historical_data_parser.add_argument('days', type=int, default=61, help='تعداد روزهای معاملاتی اخیر برای واکشی')
historical_data_parser.add_argument('start_date', type=str, help='تاریخ شروع بازه ( شمسی یا میلادی YYYY-MM-DD)')
historical_data_parser.add_argument('end_date', type=str, help='تاریخ پایان بازه ( شمسی یا میلادی YYYY-MM-DD)')

# پارسر برای اندپوینت تولید پیش‌بینی ML
ml_generate_parser = reqparse.RequestParser()
ml_generate_parser.add_argument('prediction_date', type=str, required=False, help='تاریخ میلادی برای تولید پیش‌بینی (YYYY-MM-DD)')
ml_generate_parser.add_argument('prediction_period_days', type=int, default=7, help='دوره پیش‌بینی به روز')


# =================================================================================
# --- Import سرویس‌ها (بر اساس ساختار جدید) ---
# =================================================================================
from services.data_fetcher import run_full_rebuild
from services.fetch_latest_brsapi_eod import update_daily_eod_from_brsapi
from services.historical_data_service import get_historical_data_for_symbol
from services.data_processing_and_analysis import run_technical_analysis
from services.data_analysis_service import run_fundamental_analysis
from services.ml_prediction_service import (
    generate_and_save_predictions_for_watchlist,
    update_ml_prediction_outcomes,
    get_all_ml_predictions,
    get_ml_predictions_for_symbol,
)

# =================================================================================
# --- Endpoints ---
# =================================================================================

# -----------------------------------------------
# ۱. اندپوینت‌های مربوط به واکشی و بازسازی داده
# -----------------------------------------------
@data_ns.route('/fetch/full-rebuild')
class FullRebuildResource(Resource):
    @data_ns.doc('run_full_rebuild')
    @data_ns.expect(rebuild_parser)
    def post(self):
        """
        اجرای چرخه کامل بازسازی داده‌ها از TSETMC.
        این عملیات سنگین، تمام داده‌های پایه را پاک کرده و از نو واکشی و ذخیره می‌کند.
        """
        logger = logging.getLogger(__name__)
        logger.info("🌀 [API] Starting full data rebuild process...")
        try:
            args = rebuild_parser.parse_args()
            result = run_full_rebuild(
                batch_size=args['batch_size'],
                commit_batch_size=args['commit_batch_size']
            )
            # ✅ اصلاح: حذف jsonify
            return result, 200
        except Exception as e:
            logger.error(f"❌ Fatal error in full-rebuild endpoint: {e}\n{traceback.format_exc()}")
            # ✅ اصلاح: حذف jsonify
            return {"status": "error", "message": f"An unexpected error occurred: {e}"}, 500

@data_ns.route('/fetch/daily-eod-update')
class DailyEODUpdateResource(Resource):
    @data_ns.doc('run_daily_eod_update')
    def post(self):
        """
        واکشی و ذخیره آخرین داده‌های روزانه (EOD) و اطلاعات صف سفارش از BRSAPI.
        این عملیات باید روزانه پس از پایان بازار اجرا شود.
        """
        logger = logging.getLogger(__name__)
        logger.info("⚡️ [API] Starting daily EOD update from BRSAPI...")
        session = get_session_local()
        try:
            count, message = update_daily_eod_from_brsapi(session)
            # ✅ اصلاح: حذف jsonify
            return {"status": "success", "updated_symbols": count, "message": message}, 200
        except Exception as e:
            logger.error(f"❌ Error in daily-eod-update endpoint: {e}\n{traceback.format_exc()}")
            # ✅ اصلاح: حذف jsonify
            return {"status": "error", "message": str(e)}, 500
        finally:
            session.close()

# -----------------------------------
# ۲. اندپوینت‌های مربوط به تحلیل داده
# -----------------------------------
@data_ns.route('/analysis/technical')
class RunTechnicalAnalysisResource(Resource):
    @data_ns.doc('run_technical_analysis')
    @data_ns.expect(analysis_parser)
    def post(self):
        """
        اجرای تحلیل تکنیکال و تشخیص الگوهای کندل برای نمادها.
        می‌تواند برای تمام نمادها یا لیست مشخصی اجرا شود.
        """
        logger = logging.getLogger(__name__)
        logger.info("📊 [API] Running comprehensive technical analysis...")
        session = get_session_local()
        try:
            args = analysis_parser.parse_args()
            processed_count, message = run_technical_analysis(
                db_session=session,
                limit=args.get('limit'),
                specific_symbols_list=args.get('specific_symbols'),
                days_limit=args.get('days_limit')
            )
            # ✅ اصلاح: حذف jsonify
            return {"status": "success", "processed_symbols": processed_count, "message": message}, 200
        except Exception as e:
            logger.error(f"❌ Error in technical analysis endpoint: {e}\n{traceback.format_exc()}")
            # ✅ اصلاح: حذف jsonify
            return {"status": "error", "message": str(e)}, 500
        finally:
            session.close()

@data_ns.route('/analysis/fundamental')
class RunFundamentalAnalysisResource(Resource):
    @data_ns.doc('run_fundamental_analysis')
    @data_ns.expect(analysis_parser)
    def post(self):
        """
        محاسبه معیارهای فاندامنتال (مانند قدرت خریدار حقیقی) بر اساس داده‌های تاریخی.
        """
        logger = logging.getLogger(__name__)
        logger.info("💰 [API] Running fundamental metrics analysis...")
        session = get_session_local()
        try:
            args = analysis_parser.parse_args()
            success_count, message = run_fundamental_analysis(
                db_session=session,
                symbols_list=args.get('specific_symbols')
            )
            # ✅ اصلاح: حذف jsonify
            return {"status": "success", "processed_symbols": success_count, "message": message}, 200
        except Exception as e:
            logger.error(f"❌ Error in fundamental analysis endpoint: {e}\n{traceback.format_exc()}")
            # ✅ اصلاح: حذف jsonify
            return {"status": "error", "message": str(e)}, 500
        finally:
            session.close()

# ------------------------------------------
# ۳. اندپوینت‌های مربوط به پیش‌بینی ML
# ------------------------------------------
@data_ns.route('/ml-predictions/generate')
class GenerateMLPredictionsResource(Resource):
    @data_ns.doc('generate_ml_predictions')
    @data_ns.expect(ml_generate_parser)
    def post(self):
        """
        تولید و ذخیره پیش‌بینی‌های روند آینده نمادها با استفاده از مدل ML.
        """
        logger = logging.getLogger(__name__)
        logger.info("🤖 [API] Generating ML trend predictions...")
        try:
            args = ml_generate_parser.parse_args()
            prediction_date = parse_date(args['prediction_date']) if args['prediction_date'] else date.today()

            success, message = generate_and_save_predictions_for_watchlist(
                prediction_date_greg=prediction_date,
                prediction_period_days=args['prediction_period_days']
            )
            if success:
                # ✅ اصلاح: حذف jsonify
                return {"status": "success", "message": message}, 200
            else:
                # ✅ اصلاح: حذف jsonify
                return {"status": "error", "message": message}, 400
        except Exception as e:
            logger.error(f"❌ Error in generate-ml-predictions endpoint: {e}\n{traceback.format_exc()}")
            # ✅ اصلاح: حذف jsonify
            return {"status": "error", "message": str(e)}, 500

@data_ns.route('/ml-predictions/update-outcomes')
class UpdateMLOutcomesResource(Resource):
    @data_ns.doc('update_ml_outcomes')
    def post(self):
        """
        به‌روزرسانی نتایج واقعی پیش‌بینی‌های گذشته برای ارزیابی دقت مدل.
        این اندپوینت باید به صورت دوره‌ای (روزانه) فراخوانی شود.
        """
        logger = logging.getLogger(__name__)
        logger.info("🎯 [API] Updating ML prediction outcomes...")
        try:
            success, message = update_ml_prediction_outcomes()
            if success:
                # ✅ اصلاح: حذف jsonify
                return {"status": "success", "message": message}, 200
            else:
                # ✅ اصلاح: حذف jsonify
                return {"status": "error", "message": message}, 400
        except Exception as e:
            logger.error(f"❌ Error in update-ml-outcomes endpoint: {e}\n{traceback.format_exc()}")
            # ✅ اصلاح: حذف jsonify
            return {"status": "error", "message": str(e)}, 500

@data_ns.route('/ml-predictions/all')
class GetAllMLPredictionsResource(Resource):
    @data_ns.doc('get_all_ml_predictions')
    @data_ns.marshal_list_with(ml_prediction_model)
    def get(self):
        """دریافت لیست تمام پیش‌بینی‌های ML ذخیره شده در دیتابیس."""
        logger = logging.getLogger(__name__)
        logger.info("📚 [API] Fetching all ML predictions...")
        try:
            predictions = get_all_ml_predictions()
            return predictions, 200
        except Exception as e:
            logger.error(f"❌ Error getting all ML predictions: {e}\n{traceback.format_exc()}")
            # ✅ اصلاح: حذف jsonify
            return {"status": "error", "message": str(e)}, 500

@data_ns.route('/ml-predictions/<string:symbol_id>')
class GetSymbolMLPredictionResource(Resource):
    @data_ns.doc('get_symbol_ml_prediction')
    @data_ns.marshal_with(ml_prediction_model)
    def get(self, symbol_id: str):
        """دریافت آخرین پیش‌بینی ML برای یک نماد مشخص."""
        logger = logging.getLogger(__name__)
        logger.info(f"🔍 [API] Fetching latest ML prediction for symbol: {symbol_id}")
        try:
            prediction = get_ml_predictions_for_symbol(symbol_id)
            if prediction:
                return prediction, 200
            return {"message": f"No prediction found for symbol {symbol_id}"}, 404
        except Exception as e:
            logger.error(f"❌ Error getting ML prediction for {symbol_id}: {e}\n{traceback.format_exc()}")
            # ✅ اصلاح: حذف jsonify
            return {"status": "error", "message": str(e)}, 500

# ------------------------------------
# ۴. اندپوینت‌های مربوط به دریافت داده
# ------------------------------------
@data_ns.route('/historical/<string:symbol_identifier>')
class GetHistoricalDataResource(Resource):
    @data_ns.doc('get_historical_data')
    @data_ns.expect(historical_data_parser)
    @data_ns.marshal_list_with(historical_data_model)
    def get(self, symbol_identifier: str):
        """
        دریافت داده‌های تاریخی تجمیع‌شده برای یک نماد مشخص.
        نماد می‌تواند نام (خودرو) یا کد TSETMC باشد.
        """
        logger = logging.getLogger(__name__)
        logger.info(f"📈 [API] Request for historical data for symbol: {symbol_identifier}")
        try:
            args = historical_data_parser.parse_args()
            start_date_obj = parse_date(args.get('start_date'))
            end_date_obj = parse_date(args.get('end_date'))

            data = get_historical_data_for_symbol(
                symbol_identifier=symbol_identifier,
                start_date=start_date_obj,
                end_date=end_date_obj,
                days=args['days']
            )

            if data is None:
                # ✅ اصلاح: حذف jsonify
                return {"status": "error", "message": "Failed to retrieve data due to an internal error."}, 500
            if not data:
                # ✅ اصلاح: حذف jsonify
                return {"status": "not_found", "message": "No data found for the specified symbol or date range."}, 404

            return data, 200
        except Exception as e:
            logger.error(f"❌ Error in get_historical_data endpoint for {symbol_identifier}: {e}\n{traceback.format_exc()}")
            # ✅ اصلاح: حذف jsonify
            return {"status": "error", "message": str(e)}, 500

# ---------------------------
# Health Check
# ---------------------------
@data_ns.route('/health')
class HealthResource(Resource):
    def get(self):
        """بررسی وضعیت سلامت سرویس و اتصال به دیتابیس."""
        db_ok = False
        try:
            db.session.execute('SELECT 1')
            db_ok = True
        except Exception as e:
            logging.getLogger(__name__).error(f"Health check DB connection failed: {e}")

        # ✅ اصلاح: حذف jsonify
        return {
            "status": "ok",
            "message": "API is running.",
            "db_connection": "successful" if db_ok else "failed"
        }, 200