import logging
from flask_restx import Namespace, Resource, fields, reqparse
from extensions import db
import traceback
import time
from sqlalchemy import and_, func, text, desc

from flask import request 

from models import CandlestickPatternDetection, ComprehensiveSymbolData, HistoricalData, TechnicalIndicatorData, FundamentalData

from services.combined_analysis_service import get_analysis_profile_for_symbols
from services.market_analysis_orchestrator import run_comprehensive_market_analysis

logger = logging.getLogger(__name__)

# =========================
# Namespace برای Flask-RESTX
# =========================
SymbolAnalysis_ns = Namespace('SymbolAnalysis', description='Demand Analysis Engine')

# =================================================================================
# تعریف مدل‌های DTO (Data Transfer Object) برای Swagger (بدون تغییر، اما به‌روزرسانی برای هم‌خوانی با خروجی‌های جدید)
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

# به‌روزرسانی مدل برای هم‌خوانی با خروجی‌های جدید (technical_analysis, fundamental_analysis, sentiment_analysis)
# symbol_results حالا Raw برای پوشش nested dicts (trend_analysis, momentum_analysis, etc.)
comprehensive_analysis_response_model = SymbolAnalysis_ns.model('ComprehensiveAnalysisResponse', {
    'status': fields.String(example="success"),
    'analysis_type': fields.String(description="نوع تحلیل انجام شده: Comprehensive & Filtered"),
    'execution_time': fields.Float(description="زمان اجرا به ثانیه"),
    'timestamp': fields.String(description="زمان تولید گزارش"),
    'market_overview': fields.Raw(description="نمای کلی بازار (شاخص‌ها، صنایع برتر و...)"),
    'global_anomalies': fields.Raw(attribute='anomalies_detected', description="آنومالی‌های شناسایی شده در کل بازار (با confidence و recommendation)"),
    'symbol_results': fields.Raw(description="نتایج تحلیلی نمادهای درخواست شده (Technical: trend/momentum/volume/pattern; Fundamental: valuation/capital_flow; Sentiment: score/outlook)")
})

combined_analysis_response_model = SymbolAnalysis_ns.model('CombinedAnalysisResponse', {
    'status': fields.String(example="success"),
    'SymbolAnalysis': fields.List(fields.Nested(symbol_analysis_profile_model)),
    'message': fields.String(description="پیام خطا در صورت شکست کلی")
})

candlestick_pattern_model = SymbolAnalysis_ns.model('CandlestickPattern', {
    'symbol_id': fields.String(description="شناسه نماد"),
    'symbol_name': fields.String(description="نام نماد"),
    'company_name': fields.String(description="نام شرکت"),
    'sector_name': fields.String(description="نام صنعت"),
    'jdate': fields.String(description="تاریخ شمسی"),
    'pattern_name': fields.String(description="نام الگوی کندل استیک"),
    'pattern_name_english': fields.String(description="نام انگلیسی الگو"),
    'pattern_name_persian': fields.String(description="نام فارسی الگو"),
    'pattern_type': fields.String(description="نوع الگو (Bullish/Bearish/Neutral)"),
    'created_at': fields.String(description="زمان ایجاد رکورد"),
    'technical_data': fields.Raw(description="خلاصه داده‌های تکنیکال مرتبط با الگو")
})

candlestick_response_model = SymbolAnalysis_ns.model('CandlestickResponse', {
    'status': fields.String(example="success"),
    'count': fields.Integer(description="تعداد کل الگوهای یافت شده"),
    'symbols_count': fields.Integer(description="تعداد نمادهای دارای الگو"),
    'patterns': fields.List(fields.Nested(candlestick_pattern_model)),
    'timestamp': fields.String(description="زمان تولید پاسخ")
})

# =================================================================================
# تعریف Parser ورودی
# =================================================================================
combined_analysis_parser = reqparse.RequestParser()
combined_analysis_parser.add_argument(
    'symbols', type=str, required=True,
    help='Comma-separated list of symbol names (e.g., "خودرو,خساپا")', 
    location='args', # از query string خوانده می‌شود (همانند قبل)
    action='split'
)

# ... (سایر Parserها: candlestick_parser, advanced_candlestick_parser) ...
candlestick_parser = reqparse.RequestParser()
candlestick_parser.add_argument(
    'symbol_id', type=str, required=False,
    help='شناسه نماد خاص (در صورت عدم ارسال، همه نمادها برگردانده می‌شوند)',
    location='args'
)
candlestick_parser.add_argument(
    'pattern_type', type=str, required=False,
    choices=['bullish', 'bearish', 'neutral', 'all'],
    default='all',
    help='فیلتر بر اساس نوع الگو',
    location='args'
)
candlestick_parser.add_argument(
    'limit', type=int, required=False,
    help='محدودیت تعداد نتایج',
    location='args'
)


# Parser جدید برای فیلترهای پیشرفته (حفظ شد)
advanced_candlestick_parser = reqparse.RequestParser()
advanced_candlestick_parser.add_argument(
    'symbol_id', type=str, required=False,
    help='شناسه نماد خاص',
    location='args'
)
advanced_candlestick_parser.add_argument(
    'pattern_type', type=str, required=False,
    choices=['bullish', 'bearish', 'neutral', 'all'],
    default='all',
    help='فیلتر بر اساس نوع الگو',
    location='args'
)
advanced_candlestick_parser.add_argument(
    'min_volume_ratio', type=float, required=False, default=1.0,
    help='حداقل نسبت حجم به میانگین حجم (مثلاً 1.5 یعنی حجم 50% بیشتر از میانگین)',
    location='args'
)
advanced_candlestick_parser.add_argument(
    'min_trade_value', type=float, required=False, default=5.0,
    help='حداقل ارزش معاملات (میلیارد تومان)',
    location='args'
)
advanced_candlestick_parser.add_argument(
    'trend_direction', type=str, required=False,
    choices=['bullish', 'bearish', 'any'],
    default='any',
    help='فیلتر بر اساس روند',
    location='args'
)
advanced_candlestick_parser.add_argument(
    'institutional_power', type=str, required=False,
    choices=['buying', 'selling', 'any'],
    default='any',
    help='فیلتر بر اساس قدرت حقوقی',
    location='args'
)
advanced_candlestick_parser.add_argument(
    'limit', type=int, required=False,
    help='محدودیت تعداد نتایج',
    location='args'
)

# =================================================================================
# تعریف اندپوینت‌ها (Controller)
# =================================================================================

@SymbolAnalysis_ns.route('/combined-analysis')
class CombinedAnalysisResource(Resource):

    @SymbolAnalysis_ns.doc('get_combined_analysis_profile')
    @SymbolAnalysis_ns.expect(combined_analysis_parser)
    @SymbolAnalysis_ns.marshal_with(combined_analysis_response_model)
    def get(self):
        """
        واکشی پروفایل تحلیلی کامل (خام + پردازش‌شده) بر اساس *نام نماد* (Query Params).
        """
        logger.info("🔍 [API] Received request for combined analysis profile...")

        try:
            # ⭐ منطق خواندن از Query String با reqparse (همانند قبل و کارآمد)
            args = combined_analysis_parser.parse_args()
            symbol_names = args['symbols']

            if not symbol_names:
                logger.warning("No symbol names provided.")
                return {"status": "error", "SymbolAnalysis": []}, 400

            results_data = get_analysis_profile_for_symbols(symbol_names)
            
            logger.info(f"✅ [API] Returning {len(results_data)} analysis profiles.")
            return {"status": "success", "SymbolAnalysis": results_data}, 200

        except Exception as e_outer:
            logger.error(f"❌ Critical error in combined-analysis endpoint: {e_outer}", exc_info=True)
            return {"status": "error", "message": str(e_outer), "SymbolAnalysis": []}, 500



# =================================================================================
# اندپوینت تحلیل جامع (Comprehensive Analysis)
# =================================================================================

@SymbolAnalysis_ns.route('/comprehensive-analysis')
class ComprehensiveAnalysisResource(Resource):
    
    @SymbolAnalysis_ns.doc('get_comprehensive_market_analysis')
    @SymbolAnalysis_ns.expect(combined_analysis_parser)
    @SymbolAnalysis_ns.marshal_with(comprehensive_analysis_response_model)
    def get(self):
        """اجرای تحلیل جامع و لحظه‌ای بازار"""
        logger.info("🔥 [API] Received request for comprehensive market analysis...")
        start_time = time.time()
        
        try:
            args = combined_analysis_parser.parse_args()
            symbol_names = args.get('symbols', [])

            if not symbol_names:
                logger.warning("No symbol names provided.")
                return {
                    "status": "error",
                    "analysis_type": "real_time",
                    "execution_time": 0.0,
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "market_overview": {},
                    "global_anomalies": [],
                    "symbol_results": {"error_message": "No symbols provided for analysis."}
                }, 400

            # 🚀 اجرای تحلیل اصلی
            analysis_data = run_comprehensive_market_analysis(db.session, symbol_ids=symbol_names)

            # 🧩 تابع پاک‌سازی بازگشتی
            def sanitize_json(obj):
                import numpy as np
                if isinstance(obj, dict):
                    return {k: sanitize_json(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [sanitize_json(v) for v in obj]
                elif isinstance(obj, (np.bool_, bool)):
                    return bool(obj)
                elif obj is None or (isinstance(obj, float) and (obj != obj)):  # NaN check
                    return None
                elif isinstance(obj, (int, float, str)):
                    return obj
                else:
                    try:
                        # اگر نوع پیچیده بود و __dict__ دارد
                        return sanitize_json(obj.__dict__)
                    except Exception:
                        return str(obj)

            # پاک‌سازی کل خروجی Orchestrator
            analysis_data = sanitize_json(analysis_data)

            # 🧩 ساخت پاسخ نهایی
            execution_time = time.time() - start_time
            symbol_results = {
                "technical": analysis_data.get('technical_analysis', {}),
                "fundamental": analysis_data.get('fundamental_analysis', {}),
                "sentiment": analysis_data.get('sentiment_analysis', {})
            }

            response = {
                "status": "success" if 'error' not in analysis_data else "error",
                "analysis_type": "Comprehensive & Real-Time",
                "execution_time": round(execution_time, 3),
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "market_overview": analysis_data.get('market_overview', {}),
                "global_anomalies": analysis_data.get('anomalies_detected', []),
                "symbol_results": symbol_results
            }

            response = sanitize_json(response)  # دوباره تمیز برای اطمینان

            if response['status'] == 'error':
                logger.error(f"❌ Analysis error: {analysis_data.get('error', 'Unknown error')}")
                return response, 500

            logger.info(f"✅ [API] Comprehensive analysis completed in {execution_time:.3f}s.")
            return response, 200

        except Exception as e_outer:
            logger.error(f"❌ Critical error in comprehensive-analysis endpoint: {e_outer}", exc_info=True)
            return {
                "status": "error",
                "analysis_type": "real_time",
                "execution_time": round(time.time() - start_time, 3),
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "market_overview": {},
                "global_anomalies": [],
                "symbol_results": {"error_message": f"Critical server error: {str(e_outer)}"}
            }, 500




# =================================================================================
# اندپوینت کندل های پیشرفته (بدون تغییر)
# =================================================================================         

@SymbolAnalysis_ns.route('/candlestick-patterns/advanced')
class AdvancedCandlestickPatternsResource(Resource):
    
    @SymbolAnalysis_ns.doc('get_valuable_candlestick_patterns')
    @SymbolAnalysis_ns.expect(advanced_candlestick_parser)
    @SymbolAnalysis_ns.marshal_with(candlestick_response_model)
    def get(self):
        """
        دریافت لیست نمادهای باارزش دارای الگوهای کندل استیک با فیلترهای پیشرفته
        """
        logger.info("🎯 [API] دریافت درخواست برای کندل استیک‌های باارزش...")
        
        try:
            args = advanced_candlestick_parser.parse_args()
            symbol_id = args.get('symbol_id')
            pattern_type = args.get('pattern_type', 'all')
            min_volume_ratio = args.get('min_volume_ratio', 1.0)
            min_trade_value = args.get('min_trade_value', 5.0)  # میلیارد تومان
            trend_direction = args.get('trend_direction', 'any')
            institutional_power = args.get('institutional_power', 'any')
            limit = args.get('limit')
            
            # کوئری پایه با join جدول‌های مختلف برای دسترسی به داده‌های تکنیکال
            base_query = db.session.query(
                CandlestickPatternDetection,
                ComprehensiveSymbolData.symbol_name,
                ComprehensiveSymbolData.company_name,
                ComprehensiveSymbolData.group_name,
                HistoricalData.close,
                HistoricalData.volume,
                HistoricalData.value,
                HistoricalData.buy_i_volume,
                HistoricalData.sell_i_volume,
                HistoricalData.plp
            ).join(
                ComprehensiveSymbolData,
                CandlestickPatternDetection.symbol_id == ComprehensiveSymbolData.symbol_id
            ).join(
                HistoricalData,
                and_(
                    CandlestickPatternDetection.symbol_id == HistoricalData.symbol_id,
                    CandlestickPatternDetection.jdate == HistoricalData.jdate
                )
            )
            
            # 🔧 اعمال فیلترهای پیشرفته
            
            # فیلتر حذف صندوق‌های سرمایه گذاری
            excluded_sectors = ['صندوق', 'اهرمی', 'سرمایه‌گذاری‌', 'سرمایه گذاری', 'سرمايه گذاريها', 'ETF']
            for excluded_sector in excluded_sectors:
                base_query = base_query.filter(
                    ~ComprehensiveSymbolData.group_name.ilike(f'%{excluded_sector}%')
                )
            logger.info("✅ فیلتر حذف صندوق‌ها اعمال شد")

            # فیلتر حجم معاملات
            if min_volume_ratio > 1.0:
                from sqlalchemy import func
                # استفاده از subquery برای محاسبه میانگین حجمی برای هر نماد 
                avg_volume_subquery = db.session.query(
                    HistoricalData.symbol_id,
                    func.avg(HistoricalData.volume).label('avg_volume_all_time')
                ).group_by(HistoricalData.symbol_id).subquery()
                
                # به کوئری اصلی join می‌کنیم
                base_query = base_query.join(
                    avg_volume_subquery,
                    CandlestickPatternDetection.symbol_id == avg_volume_subquery.c.symbol_id
                ).filter(
                    HistoricalData.volume >= avg_volume_subquery.c.avg_volume_all_time * min_volume_ratio
                )
            
            # فیلتر ارزش معاملات
            if min_trade_value > 0:
                base_query = base_query.filter(
                    HistoricalData.value >= min_trade_value * 1e9
                )
            
            # فیلتر روند (فرض بر وجود HistoricalData.sma_20)
            if trend_direction != 'any':
                if trend_direction == 'bullish':
                    base_query = base_query.filter(
                        HistoricalData.close > func.coalesce(HistoricalData.sma_20, HistoricalData.close)
                    )
                elif trend_direction == 'bearish':
                    base_query = base_query.filter(
                        HistoricalData.close < func.coalesce(HistoricalData.sma_20, HistoricalData.close)
                    )
            
            # فیلتر قدرت حقوقی
            if institutional_power != 'any':
                if institutional_power == 'buying':
                    base_query = base_query.filter(
                        HistoricalData.buy_i_volume > HistoricalData.sell_i_volume
                    )
                elif institutional_power == 'selling':
                    base_query = base_query.filter(
                        HistoricalData.buy_i_volume < HistoricalData.sell_i_volume
                    )
            
            # فیلترهای پایه
            if symbol_id:
                base_query = base_query.filter(CandlestickPatternDetection.symbol_id == symbol_id)
            
            if pattern_type != 'all':
                base_query = self._apply_pattern_type_filter(base_query, pattern_type)
            
            # مرتب‌سازی بر اساس ارزش معاملات (باارزش‌ترین اول)
            base_query = base_query.order_by(HistoricalData.value.desc())
            
            if limit:
                base_query = base_query.limit(limit)
            
            # اجرای کوئری
            results = base_query.all()
            
            logger.info(f"✅ یافت شد {len(results)} کندل استیک باارزش")

            # 🔥 حذف رکوردهای تکراری بر اساس ترکیب symbol_id + pattern_name + jdate
            unique_patterns = {}
            for row in results:
                # Unpack تمام 10 فیلد از کوئری
                candlestick, symbol_name, company_name, group_name, close, volume, value, buy_i_volume, sell_i_volume, plp = row
                key = (candlestick.symbol_id, candlestick.pattern_name, candlestick.jdate)
                if key not in unique_patterns:
                    unique_patterns[key] = row
            
            unique_results = list(unique_patterns.values())
            logger.info(f"🔥 پس از حذف تکراری‌ها: {len(unique_results)} الگوی منحصربفرد")
            
            # پردازش نتایج
            patterns_list = []
            for row in unique_results:
                # Unpack تمام 10 فیلد از unique_results
                candlestick, symbol_name, company_name, group_name, close, volume, value_traded, buy_i_volume, sell_i_volume, plp = row
                
                pattern_name = candlestick.pattern_name
                pattern_name_english, pattern_name_persian = self._extract_pattern_names(pattern_name)
                pattern_type_detected = self._detect_pattern_type(pattern_name_english)
                
                patterns_list.append({
                    'symbol_id': candlestick.symbol_id,
                    'symbol_name': symbol_name,
                    'company_name': company_name,
                    'sector_name': group_name,  # استفاده از group_name به عنوان sector_name
                    'jdate': candlestick.jdate,
                    'pattern_name': pattern_name,
                    'pattern_name_english': pattern_name_english,
                    'pattern_name_persian': pattern_name_persian,
                    'pattern_type': pattern_type_detected,
                    'created_at': candlestick.created_at.isoformat() if candlestick.created_at else None,
                    # اضافه کردن داده‌های تکنیکال برای نمایش
                    'technical_data': {
                        'close_price': close,
                        'volume': volume,
                        'trade_value_billion': round(value_traded / 1e9, 2) if value_traded else 0,
                        'institutional_net': buy_i_volume - sell_i_volume if buy_i_volume and sell_i_volume else 0,
                        'plp': plp
                    }
                })
            
            unique_symbols = len(set([p['symbol_id'] for p in patterns_list]))
            
            response = {
                'status': 'success',
                'count': len(patterns_list),
                'symbols_count': unique_symbols,
                'patterns': patterns_list,
                'filters_applied': {
                    'min_volume_ratio': min_volume_ratio,
                    'min_trade_value_billion': min_trade_value,
                    'trend_direction': trend_direction,
                    'institutional_power': institutional_power
                },
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            return response, 200
            
        except Exception as e:
            logger.error(f"❌ خطا در دریافت کندل استیک‌های باارزش: {e}\n{traceback.format_exc()}")
            return {
                'status': 'error',
                'count': 0,
                'symbols_count': 0,
                'patterns': [],
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'error_message': str(e)
            }, 500
    
    # توابع کمکی (حفظ شدند)
    def _apply_pattern_type_filter(self, query, pattern_type):
        """اعمال فیلتر نوع الگو"""
        bullish_patterns = ['Hammer', 'Inverted_Hammer', 'Bullish_Engulfing', 'Piercing_Line', 
                            'Morning_Star', 'Three_White_Soldiers', 'Bullish_Harami', 'Dragonfly_Doji']
        bearish_patterns = ['Shooting_Star', 'Bearish_Engulfing', 'Evening_Star', 'Dark_Cloud_Cover',
                            'Three_Black_Crows', 'Bearish_Harami', 'Gravestone_Doji']
        neutral_patterns = ['Doji', 'Spinning_Top']
        
        if pattern_type == 'bullish':
            return query.filter(
                CandlestickPatternDetection.pattern_name.contains('Bullish') |
                CandlestickPatternDetection.pattern_name.in_(bullish_patterns)
            )
        elif pattern_type == 'bearish':
            return query.filter(
                CandlestickPatternDetection.pattern_name.contains('Bearish') |
                CandlestickPatternDetection.pattern_name.in_(bearish_patterns)
            )
        elif pattern_type == 'neutral':
            return query.filter(CandlestickPatternDetection.pattern_name.in_(neutral_patterns))
        
        return query
    
    def _extract_pattern_names(self, pattern_name):
        """استخراج نام انگلیسی و فارسی از الگو"""
        if ' (' in pattern_name and ')' in pattern_name:
            pattern_name_english = pattern_name.split(' (')[0]
            pattern_name_persian = pattern_name.split(' (')[1].replace(')', '')
        else:
            pattern_name_english = pattern_name
            pattern_name_persian = pattern_name
        return pattern_name_english, pattern_name_persian
    
    def _detect_pattern_type(self, pattern_name):
        """تشخیص نوع الگو"""
        bullish_keywords = ['Hammer', 'Inverted_Hammer', 'Bullish', 'Piercing', 'Morning', 'Dragonfly']
        bearish_keywords = ['Shooting', 'Bearish', 'Evening', 'Dark_Cloud', 'Gravestone']
        
        pattern_lower = pattern_name.lower()
        
        for keyword in bullish_keywords:
            if keyword.lower() in pattern_lower:
                return 'bullish'
        
        for keyword in bearish_keywords:
            if keyword.lower() in pattern_lower:
                return 'bearish'
        
        return 'neutral'


# 🔥 اندپوینت برای دریافت آمار کندل استیک‌ها (بدون تغییر)
@SymbolAnalysis_ns.route('/candlestick-patterns/stats')
class CandlestickPatternsStatsResource(Resource):
    
    @SymbolAnalysis_ns.doc('get_candlestick_patterns_statistics')
    def get(self):
        """
        دریافت آمار و گزارش از الگوهای کندل استیک شناسایی شده
        """
        logger.info("📊 [API] دریافت درخواست برای آمار کندل استیک‌ها...")
        
        try:
            # آمار کلی
            total_patterns = db.session.query(CandlestickPatternDetection).count()
            unique_symbols = db.session.query(CandlestickPatternDetection.symbol_id).distinct().count()
            
            # آمار بر اساس نوع الگو
            patterns_by_type = db.session.query(
                CandlestickPatternDetection.pattern_name
            ).all()
            
            bullish_count = 0
            bearish_count = 0
            neutral_count = 0
            
            # ⬅️ از یک نمونه موقت برای دسترسی به متد خصوصی تشخیص الگو استفاده می‌کنیم
            detector = AdvancedCandlestickPatternsResource() 
            
            for pattern in patterns_by_type:
                pattern_type = detector._detect_pattern_type(pattern[0]) 
                if pattern_type == 'bullish':
                    bullish_count += 1
                elif pattern_type == 'bearish':
                    bearish_count += 1
                else:
                    neutral_count += 1
            
            # محبوب‌ترین الگوها
            from sqlalchemy import func
            popular_patterns = db.session.query(
                CandlestickPatternDetection.pattern_name,
                func.count(CandlestickPatternDetection.pattern_name).label('count')
            ).group_by(
                CandlestickPatternDetection.pattern_name
            ).order_by(
                func.count(CandlestickPatternDetection.pattern_name).desc()
            ).limit(10).all()
            
            stats = {
                'status': 'success',
                'total_patterns': total_patterns,
                'unique_symbols': unique_symbols,
                'pattern_type_distribution': {
                    'bullish': bullish_count,
                    'bearish': bearish_count,
                    'neutral': neutral_count
                },
                'popular_patterns': [
                    {'pattern_name': pattern[0], 'count': pattern[1]} 
                    for pattern in popular_patterns
                ],
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            return stats, 200
            
        except Exception as e:
            logger.error(f"❌ خطا در دریافت آمار کندل استیک‌ها: {e}")
            return {
                'status': 'error',
                'error_message': str(e),
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }, 500
