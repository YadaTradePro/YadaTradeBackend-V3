# -*- coding: utf-8 -*-
# routes/data_fetching_routes.py
# مسئول: تعریف اندپوینت‌های مربوط به واکشی داده، تحلیل تکنیکال، و پیش‌بینی‌های ML
# 💥 نسخه نهایی ۲: افزودن commit صریح در routes برای اطمینان از ذخیره‌سازی

import logging
import traceback
from datetime import date
import jdatetime
from flask_restx import Namespace, Resource, fields, reqparse
from extensions import db # 👈 استفاده مستقیم از db
from sqlalchemy.orm import sessionmaker, Session # Session همچنان برای Type Hinting لازم است

from flask_jwt_extended import jwt_required, get_jwt_identity
from flask import request, current_app
from flask_cors import cross_origin
import datetime

from werkzeug.exceptions import HTTPException

# =========================
# Namespace برای Flask-RESTX
# =========================
data_ns = Namespace('data', description='Data Fetching, Analysing and ML Predictions')

# =========================
# توابع کمکی
# =========================

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
# --- API Models --- (بدون تغییر)
# =========================
# Historical Data Model
historical_data_model = data_ns.model('HistoricalData', {
    'symbol_id': fields.String(required=True, description='شناسه نماد (Persian short name)'),
    'symbol_name': fields.String(description='نام نماد (Persian short name)'),
    'jdate': fields.String(description='تاریخ شمسی (YYYY-MM-DD)'),
    'date': fields.String(description='تاریخ میلادی (YYYY-MM-DD)'),
    'open': fields.Float(description='قیمت آغازین'),
    'high': fields.Float(description='بیشترین قیمت'),
    'low': fields.Float(description='کمترین قیمت'),
    'close': fields.Float(description='قیمت پایانی'),
    'final': fields.Float(description='قیمت نهایی'),
    'yesterday_price': fields.Float(description='قیمت دیروز'),
    'volume': fields.Integer(description='حجم معاملات'),
    'value': fields.Float(description='ارزش معاملات'),
    'num_trades': fields.Integer(description='تعداد معاملات'),
    'plc': fields.Float(description='تغییر قیمت پایانی'),
    'plp': fields.Float(description='درصد تغییر قیمت پایانی'),
    'pcc': fields.Float(description='تغییر قیمت نهایی'),
    'pcp': fields.Float(description='درصد تغییر قیمت نهایی'),
    'mv': fields.Float(description='ارزش بازار'),
    'eps': fields.Float(description='EPS'),
    'pe': fields.Float(description='P/E'),
    'buy_count_i': fields.Integer(description='تعداد خریداران حقیقی'),
    'buy_count_n': fields.Integer(description='تعداد خریداران حقوقی'),
    'sell_count_i': fields.Integer(description='تعداد فروشندگان حقیقی'),
    'sell_count_n': fields.Integer(description='تعداد فروشندگان حقوقی'),
    'buy_i_volume': fields.Integer(description='حجم خرید حقیقی'),
    'buy_n_volume': fields.Integer(description='حجم خرید حقوقی'),
    'sell_i_volume': fields.Integer(description='حجم فروش حقیقی'),
    'sell_n_volume': fields.Integer(description='حجم فروش حقوقی'),
})

# Comprehensive Symbol Info
comprehensive_symbol_data_model = data_ns.model('ComprehensiveSymbolData', {
    'symbol_id': fields.String(required=True),
    'symbol_name': fields.String(required=True),
    'company_name': fields.String,
    'isin': fields.String,
    'market_type': fields.String,
    'flow': fields.String,
    'industry': fields.String,
    'capital': fields.String,
    'legal_shareholder_percentage': fields.Float,
    'real_shareholder_percentage': fields.Float,
    'float_shares': fields.Float,
    'base_volume': fields.Float,
    'group_name': fields.String,
    'description': fields.String,
    'last_historical_update_date': fields.String,
})

# Technical Indicators
technical_indicator_model = data_ns.model('TechnicalIndicatorData', {
    'symbol_id': fields.String(required=True),
    'jdate': fields.String(required=True),
    'close_price': fields.Float,
    'RSI': fields.Float,
    'MACD': fields.Float,
    'MACD_Signal': fields.Float,
    'MACD_Hist': fields.Float,
    'SMA_20': fields.Float,
    'SMA_50': fields.Float,
    'Bollinger_High': fields.Float,
    'Bollinger_Low': fields.Float,
    'Bollinger_MA': fields.Float,
    'Volume_MA_20': fields.Float,
    'ATR': fields.Float,
    'Stochastic_K': fields.Float,
    'Stochastic_D': fields.Float,
    'squeeze_on': fields.Boolean,
    'halftrend_signal': fields.Integer,
    'resistance_level_50d': fields.Float,
    'resistance_broken': fields.Boolean
})

# Fundamental Data
fundamental_data_model = data_ns.model('FundamentalData', {
    'symbol_id': fields.String(required=True),
    'last_updated': fields.DateTime,
    'eps': fields.Float,
    'pe': fields.Float,
    'group_pe_ratio': fields.Float,
    'psr': fields.Float,
    'p_s_ratio': fields.Float,
    'market_cap': fields.Float,
    'base_volume': fields.Float,
    'float_shares': fields.Float,
    'jdate': fields.String,
    'date': fields.Date,
    'real_power_ratio': fields.Float,
    'volume_ratio_20d': fields.Float,
    'daily_liquidity': fields.Float
})

# --- ML Prediction ---
ml_prediction_model = data_ns.model('MLPredictionModel', {
    'id': fields.Integer(readOnly=True),
    'symbol_id': fields.String(required=True),
    'symbol_name': fields.String(required=True),
    'prediction_date': fields.String(required=True),
    'jprediction_date': fields.String(required=True),
    'prediction_period_days': fields.Integer,
    'predicted_trend': fields.String(required=True),
    'prediction_probability': fields.Float(required=True),
    'predicted_price_at_period_end': fields.Float,
    'actual_price_at_period_end': fields.Float,
    'actual_trend_outcome': fields.String,
    'is_prediction_accurate': fields.Boolean,
    'signal_source': fields.String,
    'model_version': fields.String,
    'created_at': fields.String,
    'updated_at': fields.String
})

# =================================================================================
# --- Parsers --- (بدون تغییر)
# =================================================================================
# پارسر برای اندپوینت بازسازی کامل داده‌ها
rebuild_parser = reqparse.RequestParser()
rebuild_parser.add_argument('batch_size', type=int, default=50, help='تعداد نمادها در هر بچ پردازشی')
rebuild_parser.add_argument('commit_batch_size', type=int, default=100, help='تعداد نمادهای پردازش‌شده قبل از هر commit')

# پارسر برای اندپوینت‌های تحلیل
analysis_parser = reqparse.RequestParser()
analysis_parser.add_argument('limit', type=int, required=False, help='محدودیت تعداد نمادها برای پردازش')
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
# --- Import سرویس‌ها --- (بدون تغییر)
# =================================================================================
from services.data_fetcher import run_full_rebuild
from services.fetch_latest_brsapi_eod import update_daily_eod_from_brsapi
from services.historical_data_service import get_historical_data_for_symbol
from services.data_processing_and_analysis import (
    run_technical_analysis,
    run_candlestick_detection
)
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
        """
        logger = logging.getLogger(__name__)
        logger.info("🌀 [API] Starting full data rebuild process...")
        try:
            args = rebuild_parser.parse_args()
            result = run_full_rebuild(
                batch_size=args['batch_size'],
                commit_batch_size=args['commit_batch_size']
            )
            return result, 200
        except Exception as e:
            logger.error(f"❌ Fatal error in full-rebuild endpoint: {e}\n{traceback.format_exc()}")
            return {"status": "error", "message": f"An unexpected error occurred: {e}"}, 500

@data_ns.route('/fetch/daily-eod-update')
class DailyEODUpdateResource(Resource):
    @data_ns.doc('run_daily_eod_update')
    def post(self):
        """
        واکشی و ذخیره آخرین داده‌های روزانه (EOD) از BRSAPI.
        """
        logger = logging.getLogger(__name__)
        logger.info("⚡️ [API] Starting daily EOD update from BRSAPI...")
        try:
            # پاس دادن db.session
            count, message = update_daily_eod_from_brsapi(db.session)
            #
            # 💥 [جدید] Commit صریح پس از اجرای موفق
            #
            db.session.commit()
            logger.info("✅ Daily EOD update committed successfully.")
            return {"status": "success", "updated_symbols": count, "message": message}, 200
        except Exception as e:
            #
            # 💥 [جدید] Rollback صریح در صورت خطا
            #
            logger.error(f"❌ Error in daily-eod-update endpoint: {e}\n{traceback.format_exc()}. Rolling back...")
            db.session.rollback()
            return {"status": "error", "message": str(e)}, 500
        # finally بلوک حذف شد چون مدیریت Session توسط Flask انجام می‌شود

# -----------------------------------
# ۲. اندپوینت‌های مربوط به تحلیل داده
# -----------------------------------
@data_ns.route('/analysis/technical')
class RunTechnicalAnalysisResource(Resource):
    @data_ns.doc('run_technical_analysis')
    @data_ns.expect(analysis_parser)
    def post(self):
        """
        اجرای تحلیل تکنیکال برای نمادها.
        """
        logger = logging.getLogger(__name__)
        logger.info("📊 [API] Running comprehensive technical analysis...")
        try:
            args = analysis_parser.parse_args()
            
            # پاس دادن db.session
            processed_count, message = run_technical_analysis(
                db_session=db.session,
                limit=args.get('limit'),
                symbols_list=args.get('specific_symbols')
            )
            #
            # 💥 [جدید] Commit صریح پس از اجرای موفق
            #
            db.session.commit()
            logger.info(f"✅ Technical analysis for {processed_count} symbols committed successfully.")
            return {"status": "success", "processed_symbols": processed_count, "message": message}, 200
        except Exception as e:
            #
            # 💥 [جدید] Rollback صریح در صورت خطا
            #
            logger.error(f"❌ Error in technical analysis endpoint: {e}\n{traceback.format_exc()}. Rolling back...")
            db.session.rollback()
            return {"status": "error", "message": str(e)}, 500
        # finally بلوک حذف شد

@data_ns.route('/analysis/candlesticks')
class RunCandlestickDetectionResource(Resource):
    @data_ns.doc('run_candlestick_detection')
    @data_ns.expect(analysis_parser)
    def post(self):
        """
        اجرای تشخیص الگوهای شمعی برای آخرین روز.
        """
        logger = logging.getLogger(__name__)
        logger.info("🕯️ [API] Running Candlestick Pattern Detection...")
        try:
            args = analysis_parser.parse_args()
            
            # پاس دادن db.session
            processed_count = run_candlestick_detection(
                db_session=db.session,
                limit=args.get('limit'),
                symbols_list=args.get('specific_symbols')
            )
            #
            # 💥 [جدید] Commit صریح پس از اجرای موفق (شامل Delete و Insert الگوها)
            #
            db.session.commit()
            logger.info(f"✅ Candlestick detection results (for {processed_count} symbols) committed successfully.")
            return {
                "status": "success",
                "symbols_with_patterns_found": processed_count,
                "message": f"Candlestick detection completed. Found patterns for {processed_count} symbols."
            }, 200
        except Exception as e:
            #
            # 💥 [جدید] Rollback صریح در صورت خطا
            #
            logger.error(f"❌ Error in candlestick detection endpoint: {e}\n{traceback.format_exc()}. Rolling back...")
            db.session.rollback()
            return {"status": "error", "message": str(e)}, 500
        # finally بلوک حذف شد

# ------------------------------------------
# ۳. اندپوینت‌های مربوط به پیش‌بینی ML
# ------------------------------------------
# فرض می‌کنیم توابع سرویس ML به درستی Session را مدیریت می‌کنند یا commit صریح دارند
# در غیر این صورت، مشابه بالا باید commit/rollback در route اضافه شود.

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
            # 🚦 نکته: اگر تابع generate_and_save... خودش commit نکند، باید اینجا commit کنید
            # db.session.commit()
            if success:
                return {"status": "success", "message": message}, 200
            else:
                # اگر تابع خودش rollback نکرده باشد، اینجا rollback کنید
                # db.session.rollback()
                return {"status": "error", "message": message}, 400
        except Exception as e:
            logger.error(f"❌ Error in generate-ml-predictions endpoint: {e}\n{traceback.format_exc()}")
            db.session.rollback() # Rollback در صورت خطای پیش‌بینی نشده
            return {"status": "error", "message": str(e)}, 500

@data_ns.route('/ml-predictions/update-outcomes')
class UpdateMLOutcomesResource(Resource):
    @data_ns.doc('update_ml_outcomes')
    def post(self):
        """
        به‌روزرسانی نتایج واقعی پیش‌بینی‌های گذشته.
        """
        logger = logging.getLogger(__name__)
        logger.info("🎯 [API] Updating ML prediction outcomes...")
        try:
            success, message = update_ml_prediction_outcomes()
            # 🚦 نکته: اگر تابع update_ml_prediction_outcomes خودش commit نکند، باید اینجا commit کنید
            # db.session.commit()
            if success:
                return {"status": "success", "message": message}, 200
            else:
                # db.session.rollback()
                return {"status": "error", "message": message}, 400
        except Exception as e:
            logger.error(f"❌ Error in update-ml-outcomes endpoint: {e}\n{traceback.format_exc()}")
            db.session.rollback()
            return {"status": "error", "message": str(e)}, 500

@data_ns.route('/ml-predictions/all')
class GetAllMLPredictionsResource(Resource):
    @data_ns.doc('get_all_ml_predictions')
    @data_ns.marshal_list_with(ml_prediction_model)
    def get(self):
        """دریافت لیست تمام پیش‌بینی‌های ML."""
        logger = logging.getLogger(__name__)
        logger.info("📚 [API] Fetching all ML predictions...")
        try:
            predictions = get_all_ml_predictions() # این تابع فقط query می‌زند، commit/rollback ندارد
            return predictions, 200
        except Exception as e:
            logger.error(f"❌ Error getting all ML predictions: {e}\n{traceback.format_exc()}")
            # نیازی به rollback نیست چون فقط خواندن بوده
            return {"status": "error", "message": str(e)}, 500

@data_ns.route('/ml-predictions/<string:symbol_id>')
class GetSymbolMLPredictionResource(Resource):
    @data_ns.doc('get_symbol_ml_prediction')
    @data_ns.marshal_with(ml_prediction_model)
    def get(self, symbol_id: str):
        """دریافت آخرین پیش‌بینی ML برای یک نماد."""
        logger = logging.getLogger(__name__)
        logger.info(f"🔍 [API] Fetching latest ML prediction for symbol: {symbol_id}")
        try:
            prediction = get_ml_predictions_for_symbol(symbol_id) # فقط query
            if prediction:
                return prediction, 200
            return {"message": f"No prediction found for symbol {symbol_id}"}, 404
        except Exception as e:
            logger.error(f"❌ Error getting ML prediction for {symbol_id}: {e}\n{traceback.format_exc()}")
            return {"status": "error", "message": str(e)}, 500

# ------------------------------------
# ۴. اندپوینت‌های مربوط به دریافت داده
# ------------------------------------
# این اندپوینت فقط داده می‌خواند، نیازی به commit/rollback صریح ندارد
@data_ns.route('/stock-history/<string:symbol_input>')
@data_ns.param('symbol_input', 'شناسه یا نام نماد (مثال: خودرو)')
class StockHistoryResource(Resource):
    @data_ns.doc(security='Bearer Auth', parser=historical_data_parser)
    @jwt_required()
    @data_ns.response(200, 'Historical data fetched successfully')
    @data_ns.response(400, 'Invalid date format')
    @data_ns.response(404, 'No data found for symbol')
    def get(self, symbol_input):
        """
        واکشی سابقه معاملات یک نماد.
        """
        try:
            args = historical_data_parser.parse_args()
            days = args['days']
            start_date_str = args['start_date']
            end_date_str = args['end_date']
            
            start_date = parse_date(start_date_str)
            end_date = parse_date(end_date_str)

            if (start_date_str and start_date is None) or (end_date_str and end_date is None):
                data_ns.abort(400, "Invalid date format. Please use YYYY-MM-DD (Gregorian or Jalali).")

            history_data = get_historical_data_for_symbol(
                symbol_input,
                start_date=start_date,
                end_date=end_date,
                days=days
            )
            
            if history_data is None:
                current_app.logger.error(f"Service returned None for {symbol_input}")
                data_ns.abort(500, "Internal server error during data retrieval. Service returned None.")

            if not history_data:
                data_ns.abort(404, f"No historical data found for symbol: {symbol_input} in the specified range.")

            return {"history": history_data}, 200
            
        except HTTPException as e:
            raise e
            
        except Exception as e:
            current_app.logger.error(f"An unexpected critical error occurred for {symbol_input}: {e}", exc_info=True)
            data_ns.abort(500, f"An unexpected critical error occurred: {str(e)}")

# ---------------------------
# Health Check (بدون تغییر)
# ---------------------------
from sqlalchemy import text

@data_ns.route('/health')
class HealthResource(Resource):
    def get(self):
        """بررسی وضعیت سلامت سرویس و اتصال به دیتابیس."""
        from sqlalchemy import text
        db_ok = False
        try:
            db.session.execute(text('SELECT 1'))
            db_ok = True
        except Exception as e:
            logging.getLogger(__name__).error(f"Health check DB connection failed: {e}")

        return {
            "status": "ok",
            "message": "API is running.",
            "db_connection": "successful" if db_ok else "failed"
        }, 200
