# routes/combined_analysis_route.py
import logging
from flask_restx import Namespace, Resource, fields, reqparse
from extensions import db
import traceback
import time
from sqlalchemy import and_

# اضافه کردن import برای مدل CandlestickPatternDetection
from models import CandlestickPatternDetection, ComprehensiveSymbolData, HistoricalData

from services.combined_analysis_service import get_analysis_profile_for_symbols
from services.market_analysis_orchestrator import run_comprehensive_market_analysis

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

# 🔥 مدل جدید برای تحلیل جامع بازار
comprehensive_analysis_response_model = SymbolAnalysis_ns.model('ComprehensiveAnalysisResponse', {
    'status': fields.String(example="success"),
    'analysis_type': fields.String(description="نوع تحلیل انجام شده"),
    'technical_analysis': fields.Raw(description="نتایج تحلیل تکنیکال"),
    'fundamental_analysis': fields.Raw(description="نتایج تحلیل فاندامنتال"),
    'sentiment_analysis': fields.Raw(description="نتایج تحلیل سنتیمنت"),
    'market_overview': fields.Raw(description="نمای کلی بازار"),
    'anomalies_detected': fields.Raw(description="آنومالی‌های شناسایی شده"),
    'execution_time': fields.Float(description="زمان اجرا به ثانیه"),
    'timestamp': fields.String(description="زمان تولید گزارش")
})

combined_analysis_response_model = SymbolAnalysis_ns.model('CombinedAnalysisResponse', {
    'status': fields.String(example="success"),
    'SymbolAnalysis': fields.List(fields.Nested(symbol_analysis_profile_model)),
    'message': fields.String(description="پیام خطا در صورت شکست کلی")
})

# 🔥 مدل جدید برای کندل استیک
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
    'created_at': fields.String(description="زمان ایجاد رکورد")
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
    location='args', # اطمینان از اینکه از query string خوانده می‌شود
    action='split'
)

# 🔥 Parser جدید برای تحلیل جامع
comprehensive_analysis_parser = reqparse.RequestParser()
comprehensive_analysis_parser.add_argument(
    'symbols', type=list, location='json', required=False,
    help='لیست نمادهای خاص برای تحلیل (در صورت عدم ارسال، همه نمادها تحلیل می‌شوند)'
)
comprehensive_analysis_parser.add_argument(
    'limit', type=int, location='json', required=False,
    help='محدودیت تعداد نمادها برای تحلیل'
)
comprehensive_analysis_parser.add_argument(
    'analysis_types', type=list, location='json', required=False, default=['technical', 'fundamental', 'sentiment'],
    help='انواع تحلیل‌های مورد نیاز (technical, fundamental, sentiment)'
)

# 🔥 Parser جدید برای کندل استیک
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


# 🔥 Parser جدید برای فیلترهای پیشرفته
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
        واکشی پروفایل تحلیلی کامل (خام + پردازش‌شده) بر اساس *نام نماد*.
        """
        logger.info("🔍 [API] Received request for combined analysis profile...")

        try:
            args = combined_analysis_parser.parse_args()
            symbol_names = args['symbols']

            if not symbol_names:
                logger.warning("No symbol names provided.")
                return {"status": "error", "SymbolAnalysis": []}, 400

            # --- تمام منطق پیچیده به سرویس سپرده می‌شود ---
            results_data = get_analysis_profile_for_symbols(symbol_names)
            
            logger.info(f"✅ [API] Returning {len(results_data)} analysis profiles.")
            return {"status": "success", "SymbolAnalysis": results_data}, 200

        except Exception as e_outer:
            logger.error(f"❌ Critical error in combined-analysis endpoint: {e_outer}", exc_info=True)
            return {"status": "error", "message": str(e_outer), "SymbolAnalysis": []}, 500



# =================================================================================
# اندپوینت آنالیز همگانی یک سهم
# =================================================================================

@SymbolAnalysis_ns.route('/comprehensive-analysis')
class ComprehensiveAnalysisResource(Resource):
    
    @SymbolAnalysis_ns.doc('run_comprehensive_market_analysis')
    @SymbolAnalysis_ns.expect(comprehensive_analysis_parser)
    @SymbolAnalysis_ns.marshal_with(comprehensive_analysis_response_model)
    def post(self):
        """
        اجرای تحلیل جامع تکنیکال-فاندامنتال-سنتیمنت برای نمادها
        """
        import time
        start_time = time.time()
        
        logger.info("🎯 [API] Starting comprehensive market analysis...")
        
        try:
            args = comprehensive_analysis_parser.parse_args()
            symbol_ids = args.get('symbols')
            limit = args.get('limit')
            analysis_types = args.get('analysis_types', ['technical', 'fundamental', 'sentiment'])
            
            logger.info(f"📊 تحلیل جامع برای: {len(symbol_ids) if symbol_ids else 'همه'} نماد | محدودیت: {limit} | انواع تحلیل: {analysis_types}")
            
            # اجرای تحلیل جامع
            analysis_results = run_comprehensive_market_analysis(
                db_session=db.session,
                symbol_ids=symbol_ids,
                limit=limit
            )
            
            execution_time = time.time() - start_time
            
            # ساخت پاسخ
            response = {
                'status': 'success',
                'analysis_type': 'comprehensive_technical_fundamental_sentiment',
                'technical_analysis': analysis_results.get('technical_analysis', {}),
                'fundamental_analysis': analysis_results.get('fundamental_analysis', {}),
                'sentiment_analysis': analysis_results.get('sentiment_analysis', {}),
                'market_overview': analysis_results.get('market_overview', {}),
                'anomalies_detected': analysis_results.get('anomalies_detected', []),
                'execution_time': round(execution_time, 2),
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            logger.info(f"✅ تحلیل جامع با موفقیت تکمیل شد. زمان اجرا: {execution_time:.2f} ثانیه")
            return response, 200
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"❌ خطا در تحلیل جامع بازار: {e}\n{traceback.format_exc()}")
            
            return {
                'status': 'error',
                'analysis_type': 'comprehensive_technical_fundamental_sentiment',
                'technical_analysis': {},
                'fundamental_analysis': {},
                'sentiment_analysis': {},
                'market_overview': {},
                'anomalies_detected': [],
                'execution_time': round(execution_time, 2),
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'error_message': str(e)
            }, 500

# 🔥 اندپوینت GET برای تحلیل جامع (اختیاری)
@SymbolAnalysis_ns.route('/comprehensive-analysis/status')
class ComprehensiveAnalysisStatusResource(Resource):
    
    @SymbolAnalysis_ns.doc('get_comprehensive_analysis_status')
    def get(self):
        """
        دریافت وضعیت سرویس تحلیل جامع
        """
        return {
            'status': 'active',
            'service': 'Comprehensive Market Analysis',
            'capabilities': [
                'Technical Analysis (RSI, MACD, Bollinger Bands, etc.)',
                'Fundamental Analysis (Valuation, Financial Health, Growth)',
                'Market Sentiment Analysis',
                'Anomaly Detection',
                'Market Overview'
            ],
            'version': '1.0',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }, 200




# =================================================================================
# اندپوینت کندل های پیشرفته
# =================================================================================        

# 🔥 اندپوینت پیشرفته برای کندل استیک‌های باارزش
@SymbolAnalysis_ns.route('/candlestick-patterns/advanced')
class AdvancedCandlestickPatternsResource(Resource):
    
    @SymbolAnalysis_ns.doc('get_valuable_candlestick_patterns')
    @SymbolAnalysis_ns.expect(advanced_candlestick_parser)
    @SymbolAnalysis_ns.marshal_with(candlestick_response_model)
    def get(self):
        """
        دریافت لیست نمادهای باارزش دارای الگوهای کندل استیک
        
        این اندپوینت فقط نمادهایی را برمی‌گرداند که:
        - حجم معاملات قابل توجهی دارند
        - نقدشوندگی خوبی دارند  
        - در روند مناسبی قرار دارند
        - قدرت خرید سالمی دارند
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
            
            # فیلتر حذف صندوق‌های سرمایه گذاری - این رو اول از همه اعمال کنید
            excluded_sectors = ['صندوق', 'اهرمی', 'سرمایه‌گذاری‌', 'سرمایه گذاری', 'سرمايه گذاريها']
            for excluded_sector in excluded_sectors:
                base_query = base_query.filter(
                    ~ComprehensiveSymbolData.group_name.ilike(f'%{excluded_sector}%')
                )
            logger.info("✅ فیلتر حذف صندوق‌ها اعمال شد")

            # فیلتر حجم معاملات
            if min_volume_ratio > 1.0:
                # محاسبه میانگین حجم 20 روزه (نیاز به subquery دارد)
                from sqlalchemy import func
                avg_volume_subquery = db.session.query(
                    HistoricalData.symbol_id,
                    func.avg(HistoricalData.volume).label('avg_volume_20d')
                ).group_by(HistoricalData.symbol_id).subquery()
                
                base_query = base_query.join(
                    avg_volume_subquery,
                    CandlestickPatternDetection.symbol_id == avg_volume_subquery.c.symbol_id
                ).filter(
                    HistoricalData.volume >= avg_volume_subquery.c.avg_volume_20d * min_volume_ratio
                )
            
            # فیلتر ارزش معاملات
            if min_trade_value > 0:
                base_query = base_query.filter(
                    HistoricalData.value >= min_trade_value * 1e9  # تبدیل به ریال
                )
            
            # فیلتر روند
            if trend_direction != 'any':
                # استفاده از داده‌های تکنیکال برای تشخیص روند
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
            
            # 🔍 لاگ آمار الگوها برای دیباگ
            doji_count = len([p for p in patterns_list if 'Doji' in p['pattern_name']])
            engulfing_count = len([p for p in patterns_list if 'Engulfing' in p['pattern_name']])
            other_count = len(patterns_list) - doji_count - engulfing_count
            
            logger.info(f"📊 آمار الگوها: دوجی: {doji_count}, پوشا: {engulfing_count}, سایر: {other_count}")
            
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





# 🔥 اندپوینت برای دریافت آمار کندل استیک‌ها
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
            
            for pattern in patterns_by_type:
                pattern_type = CandlestickPatternsResource()._detect_pattern_type(pattern[0])
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
