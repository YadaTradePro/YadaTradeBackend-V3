# routes/combined_analysis_route.py
import logging
from flask_restx import Namespace, Resource, fields, reqparse

import traceback

# --- وارد کردن تابع ارکستریتور از سرویس جدید ---
from services.combined_analysis_service import get_analysis_profile_for_symbols

logger = logging.getLogger(__name__)



# =========================
# Namespace برای Flask-RESTX
# =========================
SymbolAnalysis_ns = Namespace('SymbolAnalysis', description='Demand Analysis Engine')

# =================================================================================
# تعریف مدل‌های DTO (Data Transfer Object) برای Swagger
# =================================================================================
raw_historical_model = SymbolAnalysis_ns.model('RawHistorical', {
    'jdate': fields.String,
    'close': fields.Float,
    'volume': fields.Float,
    'plp': fields.Float,
    'buy_i_volume': fields.Float,
    'sell_i_volume': fields.Float
})

raw_fundamental_model = SymbolAnalysis_ns.model('RawFundamental', {
    'jdate': fields.String,
    'eps': fields.Float,
    'pe': fields.Float,
    'group_pe_ratio': fields.Float,
    'real_power_ratio': fields.Float
})

raw_technical_model = SymbolAnalysis_ns.model('RawTechnical', {
    'jdate': fields.String,
    'RSI': fields.Float,
    'SMA_50': fields.Float,
    'MACD': fields.Float,
    'MACD_Signal': fields.Float,
    'ATR': fields.Float
})

raw_candlestick_model = SymbolAnalysis_ns.model('RawCandlestick', {
    'jdate': fields.String,
    'pattern_name': fields.String
})

processed_model = SymbolAnalysis_ns.model('ProcessedMetrics', {
    'trend_score': fields.Float(description="امتیاز روند (تکنیکال)"),
    'value_score': fields.Float(description="امتیاز ارزش (بنیادی)"),
    'flow_signal': fields.String(description="سیگنال جریان پول (Bullish/Neutral/Bearish)"),
    'flow_score': fields.Float(description="امتیاز جریان پول (حقیقی و حجم)"),
    'risk_penalty': fields.Float(description="امتیاز منفی ریسک (اشباع خرید، کشیدگی)"),
    'total_score': fields.Float(description="امتیاز نهایی (جمع کل)"),
    'overall_signal': fields.String(description="سیگنال کلی (Buy/Hold/Sell)"),
    'target_upside_percent': fields.Float(description="درصد سود تا مقاومت استاتیک بعدی"),
    'reasons_summary': fields.List(fields.String, description="خلاصه‌ای از دلایل سیگنال"),
    'error': fields.String(description="در صورت بروز خطا در تحلیل این نماد، این فیلد پر می‌شود")
})

symbol_analysis_profile_model = SymbolAnalysis_ns.model('SymbolAnalysisProfile', {
    'symbol_id': fields.String,
    'symbol_name': fields.String(required=True),
    'company_name': fields.String,
    'sector_name': fields.String,
    'raw_historical': fields.Nested(raw_historical_model),
    'raw_fundamental': fields.Nested(raw_fundamental_model),
    'raw_technical': fields.Nested(raw_technical_model),
    'raw_candlestick': fields.Nested(raw_candlestick_model),
    'processed': fields.Nested(processed_model)
})

combined_analysis_response_model = SymbolAnalysis_ns.model('CombinedAnalysisResponse', {
    'status': fields.String(example="success"),
    'SymbolAnalysis': fields.List(fields.Nested(symbol_analysis_profile_model)),
    'message': fields.String(description="پیام خطا در صورت شکست کلی")
})

# =================================================================================
# تعریف Parser ورودی
# =================================================================================
combined_analysis_parser = reqparse.RequestParser()
combined_analysis_parser.add_argument(
    'symbols', type=str, required=True,
    help='Comma-separated list of symbol names (e.g., "خودرو,خساپا")', 
    location='args', # اطمینان از اینکه از query string خوانده می‌شود
    action='split'
)

# =================================================================================
# تعریف اندپوینت (Controller)
# =================================================================================
@SymbolAnalysis_ns.route('/combined-analysis')
class CombinedAnalysisResource(Resource):

    @SymbolAnalysis_ns.doc('get_combined_analysis_profile')
    @SymbolAnalysis_ns.expect(combined_analysis_parser)
    @SymbolAnalysis_ns.marshal_with(combined_analysis_response_model)
    def get(self):
        """
        واکشی پروفایل تحلیلی کامل (خام + پردازش‌شده) بر اساس *نام نماد*.
        """
        logger.info("🔍 [API] Received request for combined analysis profile...")

        try:
            args = combined_analysis_parser.parse_args()
            symbol_names = args['symbols']

            if not symbol_names:
                logger.warning("No symbol names provided.")
                return {"status": "error", "SymbolAnalysis": []}, 400 # 💡 اصلاح شده

            # --- تمام منطق پیچیده به سرویس سپرده می‌شود ---
            results_data = get_analysis_profile_for_symbols(symbol_names)
            
            # ✅ اصلاح: نام کلید "data" به "SymbolAnalysis" تغییر یافت تا با مدل مطابقت داشته باشد
            logger.info(f"✅ [API] Returning {len(results_data)} analysis profiles.")
            return {"status": "success", "SymbolAnalysis": results_data}, 200

        except Exception as e_outer:
            logger.error(f"❌ Critical error in combined-analysis endpoint: {e_outer}", exc_info=True)
            # بازگرداندن خطا در فرمت مدل
            return {"status": "error", "message": str(e_outer), "SymbolAnalysis": []}, 500
            