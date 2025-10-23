# -*- coding: utf-8 -*-
# services/market_analysis_orchestrator.py
# مسئولیت: تحلیل Real-time جامع با استفاده از داده‌های از پیش محاسبه شده

import logging
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Any
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct, text
import jdatetime

from scipy import stats
from sqlalchemy import desc

from models import (
    HistoricalData, TechnicalIndicatorData, CandlestickPatternDetection,
    FundamentalData, DailyIndexData, DailySectorPerformance,
    SentimentData, ComprehensiveSymbolData 
)

logger = logging.getLogger(__name__)

class MarketAnalysisOrchestrator:
    """
    کلاس اصلی برای تحلیل Real-time با استفاده از داده‌های از پیش محاسبه شده
    """
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.today_jdate = self._get_today_jdate()

    # ----------------------------------------------------------------------
    #                     توابع کمکی عمومی (Utility Functions)
    # ----------------------------------------------------------------------
    
    def _get_today_jdate(self) -> str:
        """دریافت تاریخ امروز به صورت شمسی"""
        return jdatetime.date.today().strftime('%Y-%m-%d')
    
    def _safe_float(self, val: Any) -> float:
        """🔸 تابع کمکی: تبدیل مقدار به float، با مدیریت None و NaN. (تعریف عمومی)"""
        if val is None:
            return np.nan
        try:
            return float(val)
        except (ValueError, TypeError):
            return np.nan
            
    def _safe_division(self, numerator: Optional[float], denominator: Optional[float], default_value: float = 0.0) -> float:
        """🔸 تابع کمکی: تقسیم ایمن، جلوگیری از تقسیم بر صفر و مدیریت None/NaN. (تعریف عمومی)"""
        if numerator is None or denominator is None:
            return default_value
        if denominator == 0:
            return default_value
        
        # تبدیل ورودی‌ها به float ایمن
        num = self._safe_float(numerator)
        den = self._safe_float(denominator)
        
        if np.isnan(num) or np.isnan(den) or den == 0:
            return default_value
            
        try:
            return num / den
        except (TypeError, ZeroDivisionError):
            return default_value

    # ----------------------------------------------------------------------
    #                     توابع اصلی اجرا
    # ----------------------------------------------------------------------
    
    def run_comprehensive_analysis(self, symbol_ids: Optional[List[str]] = None, 
                                     limit: Optional[int] = None) -> Dict[str, Any]:
        """
        اجرای تحلیل جامع Real-time با استفاده از داده‌های موجود در دیتابیس
        """
        try:
            logger.info("🎯 شروع تحلیل جامع Real-time...")
            
            # 1. یافتن نمادها برای تحلیل (اینجا symbol_ids لیست نام نمادها است)
            symbols_to_analyze = self._get_symbols_for_analysis(symbol_ids, limit) # symbols_to_analyze اکنون لیست IDهای داخلی است
            logger.info(f"🔍 {len(symbols_to_analyze)} نماد برای تحلیل Real-time انتخاب شدند")
            
            results = {
                'technical_analysis': {},
                'fundamental_analysis': {},
                'sentiment_analysis': {},
                'market_overview': {},
                'anomalies_detected': [],
                'analysis_type': 'real_time'
            }
            
            # 2. تحلیل تکنیکال (با استفاده از داده‌های از پیش محاسبه شده)
            results['technical_analysis'] = self._run_technical_analysis(symbols_to_analyze)
        
            # 3. تحلیل فاندامنتال 
            results['fundamental_analysis'] = self._run_fundamental_analysis(symbols_to_analyze)
            
            # 4. تحلیل سنتیمنت 
            results['sentiment_analysis'] = self._run_sentiment_analysis(symbols_to_analyze)
            
            # 5. نمای کلی بازار 
            results['market_overview'] = self._get_market_overview() 
            
            # 6. شناسایی anomalies 
            results['anomalies_detected'] = self._detect_market_anomalies(symbols_to_analyze) 
            
            logger.info("✅ تحلیل جامع Real-time با موفقیت تکمیل شد")
            return results
            
        except Exception as e:
            logger.error(f"❌ خطا در تحلیل جامع Real-time: {e}")
            return {'error': str(e), 'analysis_type': 'real_time'}
    
    def _get_symbols_for_analysis(self, symbol_ids: Optional[List[str]] = None, 
                                     limit: Optional[int] = None) -> List[str]:
        """
        انتخاب نمادها برای تحلیل
        نکته: symbol_ids دریافتی از API در این متد در واقع نام نمادها (Ticker) هستند.
        خروجی: لیست IDهای داخلی نمادهای معتبر
        """
        query = self.db_session.query(ComprehensiveSymbolData.symbol_id)
        
        if symbol_ids:
            # 🚀 اصلاح کلیدی: فیلتر کردن بر اساس نام نماد (symbol_name)
            query = query.filter(ComprehensiveSymbolData.symbol_name.in_(symbol_ids))
        
        # 🔸 بهبود Performance: اعمال limit در سطح query اگر symbol_ids خالی باشد (برای واکشی سریعتر)
        if not symbol_ids and limit:
            query = query.limit(limit)

        symbols = [row[0] for row in query.all()]
        
        # اگر symbol_ids تعیین شده باشد، limit نهایی در پایتون انجام می‌شود.
        # (این حالت معمولاً نباید رخ دهد چون symbol_ids مشخص است، اما برای ایمنی منطق قبلی حفظ می‌شود)
        if symbol_ids and limit and len(symbols) > limit:
            symbols = symbols[:limit]
            
        return symbols
    
    # ----------------------------------------------------------------------
    #                     توابع تحلیل تکنیکال (فاز ۲)
    # ----------------------------------------------------------------------

    def _run_technical_analysis(self, symbol_ids: List[str]) -> Dict[str, Any]:
        """اجرای تحلیل تکنیکال Real-time با استفاده از داده‌های موجود"""
        logger.info("📊 در حال اجرای تحلیل تکنیکال Real-time...")
        
        technical_results = {
            'trend_analysis': {},
            'pattern_analysis': {},
            'momentum_analysis': {},
            'volume_analysis': {}
        }
        
        for symbol_id in symbol_ids:
            try:
                latest_indicators = self._get_latest_technical_indicators(symbol_id)
                if not latest_indicators:
                    continue
                
                technical_results['trend_analysis'][symbol_id] = self._analyze_trend_from_indicators(latest_indicators)
                technical_results['pattern_analysis'][symbol_id] = self._analyze_patterns_from_indicators(latest_indicators)
                technical_results['momentum_analysis'][symbol_id] = self._analyze_momentum_from_indicators(latest_indicators)
                technical_results['volume_analysis'][symbol_id] = self._analyze_volume_from_indicators(latest_indicators)
            
            except Exception as e:
                logger.error(f"❌ خطا در تحلیل تکنیکال نماد {symbol_id}: {e}")
                continue
        
        return technical_results
    
    
    def _get_yesterday_close(self, symbol_id: str) -> Optional[float]:
        hist = self.db_session.query(HistoricalData.close).filter(
            HistoricalData.symbol_id == symbol_id
        ).order_by(HistoricalData.date.desc()).offset(1).first()
        return hist[0] if hist else None
    
    
    
    def _get_latest_technical_indicators(self, symbol_id: str) -> Optional[Dict[str, Any]]:
        """دریافت آخرین اندیکاتورهای تکنیکال از دیتابیس"""
        try:
            indicator = self.db_session.query(TechnicalIndicatorData).filter(
                TechnicalIndicatorData.symbol_id == symbol_id
            ).order_by(TechnicalIndicatorData.jdate.desc()).first()
            
            if indicator:
                # تبدیل شیء مدل به دیکشنری برای استفاده آسان‌تر
                return {
                    'rsi': indicator.RSI,
                    'macd': indicator.MACD,
                    'macd_signal': indicator.MACD_Signal,
                    'macd_histogram': indicator.MACD_Hist,
                    'sma_20': indicator.SMA_20,
                    'sma_50': indicator.SMA_50,
                    'bollinger_high': indicator.Bollinger_High,
                    'bollinger_low': indicator.Bollinger_Low,
                    'bollinger_ma': indicator.Bollinger_MA,
                    'volume_ma_20': indicator.Volume_MA_20,
                    'atr': indicator.ATR,
                    'stochastic_k': indicator.Stochastic_K,
                    'stochastic_d': indicator.Stochastic_D,
                    'squeeze_on': indicator.squeeze_on,
                    'halftrend_signal': indicator.halftrend_signal,
                    'resistance_level_50d': indicator.resistance_level_50d,
                    'resistance_broken': indicator.resistance_broken,
                    'close_price': indicator.close_price,
                    'jdate': indicator.jdate,
                    'symbol_id': indicator.symbol_id # برای فراخوانی در توابع بعدی
                }
            return None
            
        except Exception as e:
            logger.error(f"❌ خطا در دریافت اندیکاتورهای نماد {symbol_id}: {e}")
            return None
            
    def _get_trend_data(self, symbol_id: str) -> Dict[str, Any]:
        """دریافت داده‌های روند از TechnicalIndicatorData و محاسبه جامع (historical)"""
        try:
            # Query 10 روز اخیر (جدید به قدیم)
            tech_data = self.db_session.query(TechnicalIndicatorData).filter(
                TechnicalIndicatorData.symbol_id == symbol_id
            ).order_by(desc(TechnicalIndicatorData.jdate)).limit(10).all()
            
            if not tech_data:
                return {}
            
            # 🔸 استفاده از self._safe_float برای استخراج ایمن داده‌ها
            close_prices = [self._safe_float(row.close_price) for row in tech_data]
            sma20_vals = [self._safe_float(row.SMA_20) for row in tech_data]
            sma50_vals = [self._safe_float(row.SMA_50) for row in tech_data]
            rsi_vals = [self._safe_float(row.RSI) for row in tech_data]
            macd_vals = [self._safe_float(row.MACD) for row in tech_data]
            macd_signal_vals = [self._safe_float(row.MACD_Signal) for row in tech_data]
            macd_hist_vals = [self._safe_float(row.MACD_Hist) for row in tech_data]
            atr_vals = [self._safe_float(row.ATR) for row in tech_data]
            halftrend_signals = [row.halftrend_signal for row in tech_data if row.halftrend_signal is not None]
            resistance_broken = tech_data[0].resistance_broken if tech_data else False  # latest
            
            # valid lists (non-NaN)
            valid_closes = [v for v in close_prices if not np.isnan(v)]
            valid_sma20 = [v for v in sma20_vals if not np.isnan(v)]
            valid_sma50 = [v for v in sma50_vals if not np.isnan(v)]
            valid_rsi = [v for v in rsi_vals if not np.isnan(v)]
            valid_macd_hist = [v for v in macd_hist_vals if not np.isnan(v)]
            
            if len(valid_closes) < 2:
                return {}
            
            # Latest values
            latest_close = valid_closes[0]
            latest_sma20 = valid_sma20[0] if valid_sma20 else 0
            latest_sma50 = valid_sma50[0] if valid_sma50 else 0
            latest_rsi = valid_rsi[0]
            latest_macd = macd_vals[0] if macd_vals and not np.isnan(macd_vals[0]) else 0
            latest_macd_signal = macd_signal_vals[0] if macd_signal_vals and not np.isnan(macd_signal_vals[0]) else 0
            latest_macd_hist = valid_macd_hist[0]
            latest_atr = atr_vals[0] if atr_vals and not np.isnan(atr_vals[0]) else 0
            
            # Chrono (قدیم به جدید)
            closes_chrono = valid_closes[::-1]
            sma20_chrono = valid_sma20[::-1] if valid_sma20 else []
            sma50_chrono = valid_sma50[::-1] if valid_sma50 else []
            
            days = np.arange(len(closes_chrono))
            slope_price, _, _, _, r_price = stats.linregress(days, closes_chrono)
            slope_sma20 = stats.linregress(days, sma20_chrono)[0] if len(sma20_chrono) >= 2 else 0
            
            # trend_direction: ترکیب SMA + slope
            sma_bullish = latest_sma20 > latest_sma50
            price_bullish = slope_price > 0
            trend_direction = 'صعودی' if (sma_bullish and price_bullish) else \
                             'نزولی' if (not sma_bullish or not price_bullish) else 'خنثی'
            
            # trend_strength: weighted score 0-1
            strength = 0.0
            # 30% slope price (normalized)
            norm_slope = self._safe_division(abs(slope_price), np.std(closes_chrono)) if len(closes_chrono) > 1 else 0
            strength += 0.3 * min(norm_slope, 1)
            # 20% RSI extreme
            if latest_rsi > 65 or latest_rsi < 35:
                strength += 0.2
            # 20% MACD hist abs > avg
            avg_hist = np.mean(valid_macd_hist)
            if abs(latest_macd_hist) > abs(avg_hist) * 1.2:
                strength += 0.2
            # 15% BB
            bol_high = tech_data[0].Bollinger_High
            bol_low = tech_data[0].Bollinger_Low
            if bol_high and bol_low:
                bb_range = bol_high - bol_low
                # 🔸 استفاده از self._safe_division برای محاسبه موقعیت BB
                bb_pos = self._safe_division(latest_close - bol_low, bb_range, 0.5) 
                if bb_pos > 0.8 or bb_pos < 0.2:
                    strength += 0.15
            # 15% ATR
            avg_atr = np.mean([v for v in atr_vals if not np.isnan(v)])
            if latest_atr > avg_atr * 1.5:
                strength += 0.15
            
            strength = min(strength, 1.0)
            trend_confidence = abs(r_price)
            
            # سیگنال‌ها
            rsi_signal = 'خرید' if latest_rsi < 35 else 'فروش' if latest_rsi > 65 else 'خنثی'
            macd_signal = 'خرید' if latest_macd > latest_macd_signal and slope_sma20 > 0 else 'فروش'
            
            # overall برای ارزندگی
            oversold_trend = (trend_direction == 'نزولی' and latest_rsi < 40 and strength < 0.5)
            bullish_resist = resistance_broken and any(s == 1 for s in halftrend_signals[-3:])
            overall_trend_signal = 'خرید' if oversold_trend or bullish_resist else \
                                   'فروش' if trend_direction == 'نزولی' and strength > 0.6 else 'نگهدار'
            
            return {
                'trend_direction': trend_direction,
                'trend_strength': round(strength, 2),
                'trend_confidence': round(trend_confidence, 2),
                'rsi_signal': rsi_signal,
                'macd_signal': macd_signal,
                'overall_trend_signal': overall_trend_signal,
                'price_slope': round(slope_price, 2),
                'oversold_trend': oversold_trend
            }
            
        except Exception as e:
            logger.error(f"❌ خطا در دریافت داده‌های روند نماد {symbol_id}: {e}")
            return {}

    def _analyze_trend_from_indicators(self, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """تحلیل روند بر اساس اندیکاتورهای موجود (بهبودیافته)"""
        symbol_id = indicators.get('symbol_id')
        if not symbol_id:
            return {'error': 'symbol_id not found'}
        
        trend_data = self._get_trend_data(symbol_id)
        
        return {
            'trend_direction': trend_data.get('trend_direction', 'خنثی'),
            'trend_strength': trend_data.get('trend_strength', 0),
            'trend_confidence': trend_data.get('trend_confidence', 0),
            'rsi_signal': trend_data.get('rsi_signal', 'خنثی'),
            'macd_signal': trend_data.get('macd_signal', 'خنثی'),
            'overall_trend_signal': trend_data.get('overall_trend_signal', 'نگهدار'),
            'oversold_trend': trend_data.get('oversold_trend', False)
        }


    def _analyze_patterns_from_indicators(self, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """تحلیل الگوها بر اساس اندیکاتورهای موجود"""
        # دریافت آخرین الگوهای شمعی از دیتابیس
        candlestick_patterns = self._get_latest_candlestick_patterns(indicators.get('symbol_id'))
        
        return {
            'candlestick_patterns': candlestick_patterns,
            'bollinger_squeeze': bool(indicators.get('squeeze_on', 0)),
            'resistance_break': bool(indicators.get('resistance_broken', 0))
        }
    
    def _get_latest_candlestick_patterns(self, symbol_id: str) -> List[str]:
        """دریافت آخرین الگوهای شمعی از دیتابیس (برای آخرین روز موجود)"""
        try:
            # واکشی آخرین تاریخ موجود برای الگو
            latest_jdate_result = self.db_session.query(
                func.max(CandlestickPatternDetection.jdate)
            ).filter(
                CandlestickPatternDetection.symbol_id == symbol_id
            ).scalar()

            if not latest_jdate_result:
                return []
                
            patterns = self.db_session.query(CandlestickPatternDetection).filter(
                CandlestickPatternDetection.symbol_id == symbol_id,
                CandlestickPatternDetection.jdate == latest_jdate_result
            ).all()
            
            return [pattern.pattern_name for pattern in patterns]
        except Exception as e:
            logger.error(f"❌ خطا در دریافت الگوهای شمعی نماد {symbol_id}: {e}")
            return []
    
    def _get_momentum_data(self, symbol_id: str) -> Dict[str, Any]:
        """دریافت داده‌های momentum از TechnicalIndicatorData و محاسبه روند جامع"""
        try:
            # Query 10 روز اخیر (جدید به قدیم)
            tech_data = self.db_session.query(TechnicalIndicatorData).filter(
                TechnicalIndicatorData.symbol_id == symbol_id
            ).order_by(desc(TechnicalIndicatorData.jdate)).limit(10).all()
            
            if not tech_data:
                return {}
            
            # 🔸 استفاده از self._safe_float برای استخراج ایمن داده‌ها
            # استخراج مقادیر (float, handle None)
            rsi_vals = [self._safe_float(row.RSI) for row in tech_data]
            macd_vals = [self._safe_float(row.MACD) for row in tech_data]
            stoch_k_vals = [self._safe_float(row.Stochastic_K) for row in tech_data]
            stoch_d_vals = [self._safe_float(row.Stochastic_D) for row in tech_data]
            halftrend_signals = [row.halftrend_signal for row in tech_data if row.halftrend_signal is not None]
            
            # فقط روزهای معتبر (non-NaN)
            valid_rsi = [v for v in rsi_vals if not np.isnan(v)]
            valid_macd = [v for v in macd_vals if not np.isnan(v)]
            valid_stoch_k = [v for v in stoch_k_vals if not np.isnan(v)]
            
            if len(valid_rsi) < 2:
                return {}
            
            # مقادیر فعلی (latest)
            latest_rsi = valid_rsi[0]
            latest_macd = valid_macd[0] if valid_macd else 0
            latest_stoch_k = valid_stoch_k[0]
            latest_stoch_d = stoch_d_vals[0] if stoch_d_vals and not np.isnan(stoch_d_vals[0]) else latest_stoch_k
            
            # روند با slope (chrono: معکوس)
            rsi_chrono = valid_rsi[::-1]
            macd_chrono = valid_macd[::-1] if valid_macd else []
            stoch_chrono = valid_stoch_k[::-1]
            
            days = np.arange(len(rsi_chrono))
            slope_rsi, _, _, _, _ = stats.linregress(days, rsi_chrono)
            slope_macd = stats.linregress(days, macd_chrono)[0] if len(macd_chrono) >= 2 else 0
            slope_stoch = stats.linregress(days, stoch_chrono)[0]
            
            # نرمال‌سازی مقادیر
            norm_rsi = self._safe_division(latest_rsi, 100)
            norm_macd = 1 / (1 + np.exp(self._safe_division(-latest_macd, 50)))  # sigmoid normalization - safe_division used inside
            norm_stoch = self._safe_division(latest_stoch_k, 100)
            
            # تقویت امتیاز با halftrend
            momentum_score = (0.4 * norm_rsi + 0.3 * norm_macd + 0.3 * norm_stoch)
            if any(s == 1 for s in halftrend_signals[-3:]) and slope_rsi > 0:
                momentum_score += 0.05
            
            # ارزیابی قدرت کلی
            overall_momentum = 'قوی' if momentum_score > 0.6 and slope_rsi > 0 else \
                               'ضعیف' if momentum_score < 0.4 or slope_rsi < 0 else 'متوسط'
            
            # چک شرایط oversold / divergence
            oversold = latest_rsi < 35 or (latest_rsi < 40 and latest_stoch_k < 40)
            bullish_div = slope_rsi > 0 and latest_macd < 0  # divergence مثبت
            
            # سیگنال مومنتوم
            momentum_signal = 'خرید' if oversold and (bullish_div or any(s == 1 for s in halftrend_signals[-3:])) else \
                              'فروش' if momentum_score < 0.3 and slope_rsi < -1 else 'نگهدار'
            
            return {
                'rsi_momentum': round(latest_rsi, 2),
                'macd_momentum': round(latest_macd, 2),
                'stochastic_momentum': round(latest_stoch_k, 2),
                'momentum_score': round(momentum_score, 2),
                'overall_momentum': overall_momentum,
                'momentum_signal': momentum_signal,
                'rsi_slope': round(slope_rsi, 2),
                'oversold': oversold
            }
            
        except Exception as e:
            logger.error(f"❌ خطا در دریافت داده‌های momentum نماد {symbol_id}: {e}")
            return {}

    def _analyze_momentum_from_indicators(self, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """تحلیل مومنتوم بر اساس اندیکاتورهای موجود (بهبودیافته)"""
        symbol_id = indicators.get('symbol_id')
        if not symbol_id:
            return {'error': 'symbol_id not found'}
        
        momentum_data = self._get_momentum_data(symbol_id)
        
        return {
            'rsi_momentum': momentum_data.get('rsi_momentum', 50),
            'macd_momentum': momentum_data.get('macd_momentum', 0),
            'stochastic_momentum': momentum_data.get('stochastic_momentum', 50),
            'momentum_score': momentum_data.get('momentum_score', 0.5),
            'overall_momentum': momentum_data.get('overall_momentum', 'متوسط'),
            'momentum_signal': momentum_data.get('momentum_signal', 'نگهدار'),
            'oversold': momentum_data.get('oversold', False)
        }

    
    def _analyze_volume_from_indicators(self, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """تحلیل حجم بر اساس اندیکاتورهای موجود"""
        # دریافت داده‌های حجم از HistoricalData
        volume_data = self._get_volume_data(indicators.get('symbol_id'))
        
        return {
            'volume_ratio': volume_data.get('volume_ratio', 0),
            'volume_trend': volume_data.get('volume_trend', 'خنثی'),
            'unusual_volume': volume_data.get('unusual_volume', False),
            'decreasing_range': volume_data.get('decreasing_range', False)  # جدید
        }
    
    def _get_volume_data(self, symbol_id: str) -> Dict[str, Any]:
        """دریافت داده‌های حجم از HistoricalData و محاسبه نسبت و روند با accuracy بالاتر"""
        try:
            # دریافت 10 روز آخر برای تحلیل جامع‌تر
            historical_data = self.db_session.query(HistoricalData).filter(
                HistoricalData.symbol_id == symbol_id
            ).order_by(HistoricalData.date.desc()).limit(10).all()
        
            if not historical_data:
                return {}
        
            volumes = [self._safe_float(row.volume) for row in historical_data if row.volume]  # float برای دقت
            volumes = [v for v in volumes if not np.isnan(v)]
            if len(volumes) < 2:
                return {}
        
            # volumes[0] = جدیدترین (امروز)
            latest_volume = volumes[0]
        
            # برای 5 روز اخیر (جدید به قدیم)
            volumes_5d = volumes[:5]
            avg_5d = np.mean(volumes_5d)
            # avg_past_4 = np.mean(volumes_5d[1:]) if len(volumes_5d) > 1 else 0 # این متغیر استفاده نشده بود
        
            # معکوس برای chronological (قدیم به جدید)
            volumes_chrono = volumes[::-1]
            volumes_5d_chrono = volumes_chrono[-5:]  # 5 روز اخیر chrono
            volumes_10d_chrono = volumes_chrono[-10:] if len(volumes_chrono) >= 10 else volumes_chrono
        
            # شیب رگرسیون برای trend (روی 5 روز برای حساسیت، اما چک 10 روز برای stability)
            days_5 = np.arange(len(volumes_5d_chrono))
            slope_5, _, _, _, _ = stats.linregress(days_5, volumes_5d_chrono)
            slope_10, _, _, _, _ = stats.linregress(np.arange(len(volumes_10d_chrono)), volumes_10d_chrono)
        
            # trend: نزولی اگر هر دو slope منفی (رنج کاهشی محکم)
            volume_trend = 'نزولی' if (slope_5 < 0 and slope_10 < 0) else \
                        'صعودی' if (slope_5 > 0 and slope_10 > 0) else 'خنثی'
        
            # چک رنج کاهش: درصد روزهای کاهشی در 5 روز > 60%
            decreasing_count = sum(1 for i in range(1, len(volumes_5d_chrono)) if volumes_5d_chrono[i] < volumes_5d_chrono[i-1])
            decreasing_ratio = self._safe_division(decreasing_count, len(volumes_5d_chrono) - 1) if len(volumes_5d_chrono) > 1 else 0
            decreasing_range = decreasing_ratio > 0.6
        
            # unusual: > 1.5x میانگین 10 روز (dynamic)
            avg_10d = np.mean(volumes_10d_chrono)
            unusual_volume = latest_volume > avg_10d * 1.5
        
            return {
                # 🔸 استفاده از self._safe_division
                'volume_ratio': round(self._safe_division(latest_volume, avg_5d), 3),
                'volume_trend': volume_trend,
                'unusual_volume': unusual_volume,
                'decreasing_range': decreasing_range,  # جدید: برای مطمئن شدن از رنج کاهشی
                'slope_5d': round(slope_5, 2),  # اختیاری: برای debug
                'decreasing_ratio': round(decreasing_ratio, 2)  # اختیاری
            }
        
        except Exception as e:
            logger.error(f"❌ خطا در دریافت داده‌های حجم نماد {symbol_id}: {e}")
            return {}

    # ----------------------------------------------------------------------
    #                     توابع تحلیل فاندامنتال (فاز ۳) 
    # ----------------------------------------------------------------------

    def _run_fundamental_analysis(self, symbol_ids: List[str]) -> Dict[str, Any]:
        """اجرای تحلیل فاندامنتال Real-time با استفاده از داده‌های موجود"""
        logger.info("🏛️ در حال اجرای تحلیل فاندامنتال Real-time...")
        
        fundamental_results = {}
        
        for symbol_id in symbol_ids:
            try:
                latest_fund_data = self._get_latest_fundamental_data(symbol_id)
                if not latest_fund_data:
                    fundamental_results[symbol_id] = {'status': 'داده‌ای یافت نشد'}
                    continue
                
                # ارزیابی بر اساس ضرایب مالی
                valuation = self._evaluate_financial_ratios(latest_fund_data)
                
                # تحلیل جریان سرمایه و نقدینگی
                capital_flow = self._analyze_daily_capital_flow(latest_fund_data)
                
                # ترکیب نتایج
                fundamental_results[symbol_id] = {
                    'valuation': valuation,
                    'capital_flow': capital_flow,
                    'market_cap': latest_fund_data.get('market_cap'),
                    'float_shares': latest_fund_data.get('float_shares'),
                    'updated_jdate': latest_fund_data.get('jdate')
                }
            except Exception as e:
                logger.error(f"❌ خطا در تحلیل فاندامنتال نماد {symbol_id}: {e}")
                continue
                
        return fundamental_results

    def _get_latest_fundamental_data(self, symbol_id: str) -> Optional[Dict[str, Any]]:
        """دریافت آخرین داده‌های فاندامنتال از دیتابیس"""
        try:
            # از آنجایی که FundamentalData شامل داده‌های مالی و روزانه است، آخرین رکورد را واکشی می‌کنیم
            fund_data = self.db_session.query(FundamentalData).filter(
                FundamentalData.symbol_id == symbol_id
            ).order_by(FundamentalData.jdate.desc()).first()

            
            if fund_data:
                # تبدیل شیء مدل به دیکشنری
                return {c.name: getattr(fund_data, c.name) for c in fund_data.__table__.columns}
            return None
            
        except Exception as e:
            logger.error(f"❌ خطا در واکشی داده‌های فاندامنتال نماد {symbol_id}: {e}")
            return None

    def _evaluate_financial_ratios(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """ارزیابی سریع نسبت‌های مالی (P/E, EPS)"""
        pe = data.get('pe')
        group_pe = data.get('group_pe_ratio')
        
        valuation_status = 'نامشخص'
        if pe and group_pe and pe > 0:
            if pe < group_pe * 0.9:
                valuation_status = 'جذاب (P/E پایین‌تر از گروه)'
            elif pe > group_pe * 1.1:
                valuation_status = 'گران (P/E بالاتر از گروه)'
            else:
                valuation_status = 'خنثی (نزدیک میانگین گروه)'
        elif pe and pe > 0 and pe < 10:
             valuation_status = 'ارزان بالقوه'

        return {
            'eps': data.get('eps'),
            'pe': pe,
            'group_pe': group_pe,
            'valuation_status': valuation_status
        }
        
    def _analyze_daily_capital_flow(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """تحلیل جریان سرمایه (Real Power و Liquidity)"""
        real_power = data.get('real_power_ratio')
        liquidity = data.get('daily_liquidity')
        
        capital_flow_status = 'خنثی'
        if real_power and real_power > 1.2:
            capital_flow_status = 'ورود پول قوی (قدرت خریدار)'
        elif real_power and real_power < 0.8:
            capital_flow_status = 'خروج پول قوی (قدرت فروشنده)'
            
        return {
            'real_power_ratio': real_power,
            'daily_liquidity': liquidity,
            'capital_flow_status': capital_flow_status
        }

    # ----------------------------------------------------------------------
    #                     توابع تحلیل سنتیمنت (فاز ۴) 
    # ----------------------------------------------------------------------

    def _run_sentiment_analysis(self, symbol_ids: List[str]) -> Dict[str, Any]:
        """ تولید سنتیمنت سهم بر اساس نشانگرهای جریان سرمایه و حجم (با استفاده از FundamentalData و volume_data).
        """
        logger.info("💬 در حال اجرای تحلیل سنتیمنت Real-time...")
        sentiment_results = {}
        
        for symbol_id in symbol_ids:
            try:
                fund_data = self._get_latest_fundamental_data(symbol_id)
                volume_data = self._get_volume_data(symbol_id)
                
                if not fund_data and not volume_data:
                    sentiment_results[symbol_id] = {'status': 'داده‌ای یافت نشد'}
                    continue
                    
                # استفاده از volume_ratio محاسبه شده (۵ روزه)
                volume_ratio = volume_data.get('volume_ratio')
                
                sentiment_score, sentiment_outlook = self._calculate_composite_sentiment(
                    real_power=fund_data.get('real_power_ratio') if fund_data else None,
                    volume_ratio=volume_ratio,
                    liquidity=fund_data.get('daily_liquidity') if fund_data else None
                )
                
                sentiment_results[symbol_id] = {
                    'sentiment_score': round(sentiment_score, 2),
                    'outlook': sentiment_outlook,
                    'real_power_signal': self._get_real_power_signal(fund_data.get('real_power_ratio') if fund_data else None),
                    'volume_excitement': self._get_volume_excitement(volume_ratio)
                }
            except Exception as e:
                logger.error(f"❌ خطا در تحلیل سنتیمنت نماد {symbol_id}: {e}")
                continue
                
        return sentiment_results

    def _calculate_composite_sentiment(self, real_power: Optional[float], volume_ratio: Optional[float], 
                                         liquidity: Optional[float]) -> Tuple[float, str]:
        """محاسبه امتیاز سنتیمنت ترکیبی (۰ تا ۱۰۰) و تعیین دیدگاه."""
        score = 50.0 # شروع از خنثی (Neutral)
        
        # 1. Real Power Ratio (وزن بالا: 40%)
        if real_power is not None:
            if real_power > 1.5:
                score += 20
            elif real_power > 1.0:
                score += 5
            elif real_power < 0.5:
                score -= 20
            elif real_power < 1.0:
                score -= 5
                
        # 2. Volume Ratio (وزن متوسط: 30%)
        if volume_ratio is not None:
            if volume_ratio > 2.0:
                score += 15
            elif volume_ratio > 1.0:
                score += 5
            elif volume_ratio < 0.5:
                score -= 10
                
        # 3. Daily Liquidity (وزن متوسط: 30%)
        # آستانه‌ها فرضی و بر اساس عرف بازار ایران هستند (بر حسب ریال)
        if liquidity is not None and liquidity > 10000000000:
            score += 10
        elif liquidity is not None and liquidity < 1000000000:
            score -= 10

        final_score = max(0, min(100, score))
        
        if final_score >= 70:
            outlook = 'بسیار مثبت/هیجانی'
        elif final_score >= 60:
            outlook = 'مثبت'
        elif final_score <= 30:
            outlook = 'بسیار منفی/وحشت'
        elif final_score <= 40:
            outlook = 'منفی'
        else:
            outlook = 'خنثی/بلاتکلیف'
            
        return final_score, outlook

    def _get_real_power_signal(self, real_power: Optional[float]) -> str:
        """سیگنال ساده از قدرت خریدار حقیقی"""
        if real_power is None:
            return 'نامشخص'
        if real_power > 1.3:
            return 'خریدار قوی'
        if real_power < 0.7:
            return 'فروشنده قوی'
        return 'خنثی'

    def _get_volume_excitement(self, volume_ratio: Optional[float]) -> str:
        """سیگنال ساده از نسبت حجم"""
        if volume_ratio is None:
            return 'نامشخص'
        if volume_ratio > 2.0:
            return 'هیجان بالا (غیرعادی)'
        if volume_ratio > 1.2:
            return 'توجه بالاتر از میانگین'
        return 'عادی'

    # ----------------------------------------------------------------------
    # توابع نمای کلی و شناسایی ناهنجاری (فاز ۵ و ۶)
    # ----------------------------------------------------------------------

    def _get_market_overview(self) -> Dict[str, Any]:
        """ واکشی و تحلیل داده‌های کلی بازار و سکتورها."""
        logger.info("P5: Implementing Market Overview.")
        
        # 1. وضعیت شاخص کل
        latest_jdate = self.db_session.query(func.max(DailyIndexData.jdate)).scalar()
        index_status = 'نامشخص'
        
        if latest_jdate:
            # واکشی داده‌های شاخص کل (Total_Index) در آخرین تاریخ
            index_data = self.db_session.query(DailyIndexData).filter(
                DailyIndexData.jdate == latest_jdate,
                DailyIndexData.index_type == 'Total_Index' # فرض می‌کنیم شاخص کل با این نام ذخیره می‌شود
            ).first()
            
            # 🚀 استفاده از percent_change به جای daily_change
            if index_data and index_data.percent_change is not None:
                daily_change = index_data.percent_change
                if daily_change > 0.0:
                    index_status = f"مثبت ({daily_change:.2f}%)"
                elif daily_change < 0.0:
                    index_status = f"منفی ({daily_change:.2f}%)"
                else:
                    index_status = 'خنثی'
        
        # 2. سکتور برتر (بر اساس کمترین رتبه: rank.asc())
        top_sector_data = self.db_session.query(DailySectorPerformance).filter(
            DailySectorPerformance.jdate == latest_jdate # فیلتر بر اساس آخرین تاریخ
        ).order_by(DailySectorPerformance.rank.asc()).first()
        
        top_sector = top_sector_data.sector_name if top_sector_data else None
        
        return {
            'overall_status': index_status,
            'top_sector': top_sector
        }

    def _detect_market_anomalies(self, symbol_ids: List[str]) -> List[Dict[str, Any]]:
        """ شناسایی ناهنجاری‌ها (حجم غیرعادی + تکنیکال + squeeze + gap).
        منطق: - خودش از دیتابیس حجم/اندیکاتورهای تاریخی می‌گیرد (بدون وابستگی بیرونی).
        - محاسبات آماری (میانگین/انحراف معیار/z-score) با پایتون خالص انجام می‌شود.
        - انواع ناهنجاری‌ها ترکیبی‌اند و confidence جمع‌شونده (وزن‌دار) صادر می‌شود.
        """
        logger.info("P6: Detecting anomalies with enhanced logic (DB-only).")
        anomalies: List[Dict[str, Any]] = []

        # پارامترهای تنظیمی (قابل تبدیل به مقادیر کلاس/کانفیگ)
        VOLUME_LOOKBACK = 30
        BOL_LOOKBACK = 20
        MACD_HIST_LOOKBACK = 10
        VOLUME_Z_THRESHOLD = 2.0
        VOLUME_RATIO_THRESHOLD = 1.5
        GAP_PCT_THRESHOLD = 0.05

        # توابع کمکی محلی
        def _mean(arr: List[float]) -> float:
            return sum(arr) / len(arr) if arr else 0.0

        def _std(arr: List[float], mean_val: float = None) -> float:
            if not arr:
                return 0.0
            if mean_val is None:
                mean_val = _mean(arr)
            var = self._safe_division(sum((x - mean_val) ** 2 for x in arr), len(arr))
            return var ** 0.5

        for symbol_id in symbol_ids:
            try:
                # --- 1) حجم و قیمت تاریخی ---
                hist_rows = (
                    self.db_session.query(HistoricalData)
                    .filter(HistoricalData.symbol_id == symbol_id)
                    .order_by(desc(HistoricalData.date))
                    .limit(VOLUME_LOOKBACK + 1)  # +1 برای محاسبه گپ دیروز به امروز
                    .all()
                )
                if len(hist_rows) < 2:
                    continue  # نیاز به حداقل ۲ روز

                volumes = [self._safe_float(row.volume) for row in hist_rows if row.volume is not None]
                closes = [self._safe_float(getattr(row, 'close', getattr(row, 'close_price', np.nan))) for row in hist_rows]
                opens = [self._safe_float(getattr(row, 'open', getattr(row, 'open_price', np.nan))) for row in hist_rows]

                # حذف مقادیر NaN
                volumes = [v for v in volumes if not np.isnan(v)]
                closes = [c for c in closes if not np.isnan(c)]
                opens = [o for o in opens if not np.isnan(o)]

                if len(volumes) < 5 or len(closes) < 2 or len(opens) < 1:
                    continue

                # --- محاسبات حجم ---
                latest_volume = volumes[0]
                mean_vol = _mean(volumes)
                std_vol = _std(volumes, mean_vol)

                vol_z = self._safe_division(latest_volume - mean_vol, std_vol)
                vol_ratio = self._safe_division(latest_volume, mean_vol)

                # --- محاسبه گپ ---
                latest_close = closes[0]
                yesterday_close = closes[1]
                latest_open = opens[0]

                gap_pct = self._safe_division(latest_open - yesterday_close, yesterday_close)
                is_gap_up = gap_pct >= GAP_PCT_THRESHOLD
                is_gap_down = gap_pct <= -GAP_PCT_THRESHOLD

                # --- 2) اندیکاتورهای تکنیکال (Real-time) ---
                latest_indicators = self._get_latest_technical_indicators(symbol_id)
                if not latest_indicators:
                    continue

                squeeze_flag = bool(latest_indicators.get('squeeze_on', False))
                latest_macd_hist = self._safe_float(latest_indicators.get('macd_histogram'))

                tech_data_hist = (
                    self.db_session.query(TechnicalIndicatorData.MACD_Hist)
                    .filter(TechnicalIndicatorData.symbol_id == symbol_id)
                    .order_by(desc(TechnicalIndicatorData.jdate))
                    .limit(MACD_HIST_LOOKBACK)
                    .all()
                )
                macd_hist_vals = [self._safe_float(row[0]) for row in tech_data_hist]
                valid_hist = [v for v in macd_hist_vals if not np.isnan(v)]

                macd_hist_spike = False
                if len(valid_hist) >= 3:
                    avg_hist_past = _mean(valid_hist[1:])
                    if abs(latest_macd_hist) > abs(avg_hist_past) * 2.0:
                        macd_hist_spike = True

                # RSI Extreme
                latest_rsi = self._safe_float(latest_indicators.get('rsi', 50.0))
                rsi_extreme = (latest_rsi < 30) or (latest_rsi > 70)

                # --- 3) محاسبه Confidence و جمع‌بندی ---
                confidence_score = 0.0
                anomaly_type = []

                # حجم غیرعادی
                unusual_by_volume = (vol_z >= VOLUME_Z_THRESHOLD) or (vol_ratio >= VOLUME_RATIO_THRESHOLD)
                if unusual_by_volume:
                    confidence_score += 0.4
                    anomaly_type.append('حجم غیرعادی')

                # گپ قیمتی
                if is_gap_up:
                    confidence_score += 0.2
                    anomaly_type.append('گپ صعودی بزرگ')
                elif is_gap_down:
                    confidence_score += 0.2
                    anomaly_type.append('گپ نزولی بزرگ')

                # فشردگی بولینگر
                if squeeze_flag:
                    confidence_score += 0.15
                    anomaly_type.append('فشردگی بولینگر')

                # اسپایک MACD
                if macd_hist_spike:
                    confidence_score += 0.1
                    anomaly_type.append('اسپایک MACD')

                # اشباع RSI
                if rsi_extreme:
                    confidence_score += 0.15
                    anomaly_type.append('اشباع RSI')

                # --- 4) ذخیره ناهنجاری ---
                if confidence_score >= 0.4 or unusual_by_volume:
                    anomalies.append({
                        'symbol_id': symbol_id,
                        'anomaly_type': ", ".join(anomaly_type),
                        'confidence': round(float(confidence_score), 2),
                        'details': {
                            'latest_volume': float(latest_volume),
                            'volume_mean': float(mean_vol),
                            'volume_std': round(float(std_vol), 2),
                            'volume_z': round(float(vol_z), 2),
                            'volume_ratio': round(float(vol_ratio), 2),
                            'squeeze': bool(squeeze_flag),
                            'macd_hist_spike': bool(macd_hist_spike),
                            'latest_rsi': float(latest_rsi),
                            'gap_pct': round(float(gap_pct), 4)
                        }
                    })

            except Exception as e:
                logger.error(f"❌ خطا در شناسایی anomaly برای {symbol_id}: {e}")
                continue

        # ✅ ایمن‌سازی خروجی برای JSON (رفع خطای np.bool_)
        return [self._ensure_json_safe(a) for a in anomalies]


    
# ----------------------------------------------------------------------
#                     تابع اصلی برای استفاده در API
# ----------------------------------------------------------------------

def run_comprehensive_market_analysis(db_session: Session, symbol_ids: Optional[List[str]] = None, 
                                     limit: Optional[int] = None) -> Dict[str, Any]:
    """
    اجرای تحلیل جامع Real-time بازار - تابع اصلی برای استفاده در API
    """
    orchestrator = MarketAnalysisOrchestrator(db_session)
    return orchestrator.run_comprehensive_analysis(symbol_ids, limit)

__all__ = ['MarketAnalysisOrchestrator', 'run_comprehensive_market_analysis']
