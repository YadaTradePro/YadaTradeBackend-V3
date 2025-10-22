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

from models import (
    HistoricalData, TechnicalIndicatorData, CandlestickPatternDetection,
    FundamentalData, DailyIndexData, DailySectorPerformance,
    SentimentData, FinancialRatiosData, ComprehensiveSymbolData
)

logger = logging.getLogger(__name__)

class MarketAnalysisOrchestrator:
    """
    کلاس اصلی برای تحلیل Real-time با استفاده از داده‌های از پیش محاسبه شده
    """
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.today_jdate = self._get_today_jdate()
    
    def _get_today_jdate(self) -> str:
        """دریافت تاریخ امروز به صورت شمسی"""
        import jdatetime
        return jdatetime.date.today().strftime('%Y-%m-%d')
    
    def run_comprehensive_analysis(self, symbol_ids: Optional[List[str]] = None, 
                                 limit: Optional[int] = None) -> Dict[str, Any]:
        """
        اجرای تحلیل جامع Real-time با استفاده از داده‌های موجود در دیتابیس
        """
        try:
            logger.info("🎯 شروع تحلیل جامع Real-time...")
            
            # 1. یافتن نمادها برای تحلیل
            symbols_to_analyze = self._get_symbols_for_analysis(symbol_ids, limit)
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
        """انتخاب نمادها برای تحلیل"""
        query = self.db_session.query(ComprehensiveSymbolData.symbol_id)
        
        if symbol_ids:
            query = query.filter(ComprehensiveSymbolData.symbol_id.in_(symbol_ids))
        
        symbols = [row[0] for row in query.all()]
        
        if limit and len(symbols) > limit:
            symbols = symbols[:limit]
            
        return symbols
    
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
                # دریافت آخرین اندیکاتورهای محاسبه شده از دیتابیس
                latest_indicators = self._get_latest_technical_indicators(symbol_id)
                if not latest_indicators:
                    continue
                
                # تحلیل روند
                trend_analysis = self._analyze_trend_from_indicators(latest_indicators)
                technical_results['trend_analysis'][symbol_id] = trend_analysis
                
                # تحلیل الگوها
                pattern_analysis = self._analyze_patterns_from_indicators(latest_indicators)
                technical_results['pattern_analysis'][symbol_id] = pattern_analysis
                
                # تحلیل مومنتوم
                momentum_analysis = self._analyze_momentum_from_indicators(latest_indicators)
                technical_results['momentum_analysis'][symbol_id] = momentum_analysis
                
                # تحلیل حجم
                volume_analysis = self._analyze_volume_from_indicators(latest_indicators)
                technical_results['volume_analysis'][symbol_id] = volume_analysis
                
            except Exception as e:
                logger.error(f"❌ خطا در تحلیل تکنیکال نماد {symbol_id}: {e}")
                continue
        
        return technical_results
    
    def _get_latest_technical_indicators(self, symbol_id: str) -> Optional[Dict[str, Any]]:
        """دریافت آخرین اندیکاتورهای تکنیکال از دیتابیس"""
        try:
            indicator = self.db_session.query(TechnicalIndicatorData).filter(
                TechnicalIndicatorData.symbol_id == symbol_id
            ).order_by(TechnicalIndicatorData.jdate.desc()).first()
            
            if indicator:
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
                    'jdate': indicator.jdate
                }
            return None
            
        except Exception as e:
            logger.error(f"❌ خطا در دریافت اندیکاتورهای نماد {symbol_id}: {e}")
            return None
    
    def _analyze_trend_from_indicators(self, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """تحلیل روند بر اساس اندیکاتورهای موجود"""
        return {
            'trend_direction': 'صعودی' if indicators.get('sma_20', 0) > indicators.get('sma_50', 0) else 'نزولی',
            'trend_strength': self._calculate_trend_strength_from_indicators(indicators),
            'rsi_signal': 'خرید' if indicators.get('rsi', 50) < 30 else 'فروش' if indicators.get('rsi', 50) > 70 else 'خنثی',
            'macd_signal': 'خرید' if indicators.get('macd', 0) > indicators.get('macd_signal', 0) else 'فروش'
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
        """دریافت آخرین الگوهای شمعی از دیتابیس"""
        try:
            patterns = self.db_session.query(CandlestickPatternDetection).filter(
                CandlestickPatternDetection.symbol_id == symbol_id,
                CandlestickPatternDetection.jdate == self.today_jdate
            ).all()
            
            return [pattern.pattern_name for pattern in patterns]
        except Exception as e:
            logger.error(f"❌ خطا در دریافت الگوهای شمعی نماد {symbol_id}: {e}")
            return []
    
    def _analyze_momentum_from_indicators(self, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """تحلیل مومنتوم بر اساس اندیکاتورهای موجود"""
        return {
            'rsi_momentum': indicators.get('rsi', 50),
            'macd_momentum': indicators.get('macd', 0),
            'stochastic_momentum': indicators.get('stochastic_k', 50),
            'overall_momentum': 'قوی' if indicators.get('rsi', 50) > 60 else 'ضعیف' if indicators.get('rsi', 50) < 40 else 'متوسط'
        }
    
    def _analyze_volume_from_indicators(self, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """تحلیل حجم بر اساس اندیکاتورهای موجود"""
        # دریافت داده‌های حجم از HistoricalData
        volume_data = self._get_volume_data(indicators.get('symbol_id'))
        
        return {
            'volume_ratio': volume_data.get('volume_ratio', 0),
            'volume_trend': volume_data.get('volume_trend', 'خنثی'),
            'unusual_volume': volume_data.get('unusual_volume', False)
        }
    
    def _get_volume_data(self, symbol_id: str) -> Dict[str, Any]:
        """دریافت داده‌های حجم از HistoricalData"""
        try:
            # دریافت 5 روز آخر برای تحلیل حجم
            historical_data = self.db_session.query(HistoricalData).filter(
                HistoricalData.symbol_id == symbol_id
            ).order_by(HistoricalData.date.desc()).limit(5).all()
            
            if not historical_data:
                return {}
            
            volumes = [row.volume for row in historical_data if row.volume]
            if not volumes:
                return {}
            
            avg_volume = sum(volumes) / len(volumes)
            latest_volume = volumes[0]
            
            return {
                'volume_ratio': latest_volume / avg_volume if avg_volume > 0 else 0,
                'volume_trend': 'صعودی' if latest_volume > sum(volumes[1:])/len(volumes[1:]) else 'نزولی',
                'unusual_volume': latest_volume > avg_volume * 1.5
            }
            
        except Exception as e:
            logger.error(f"❌ خطا در دریافت داده‌های حجم نماد {symbol_id}: {e}")
            return {}
    
    # بقیه توابع (_run_fundamental_analysis, _run_sentiment_analysis, etc.)
    # بدون تغییر باقی می‌مانند اما از داده‌های دیتابیس استفاده می‌کنند
    
    def _calculate_trend_strength_from_indicators(self, indicators: Dict[str, Any]) -> float:
        """محاسبه قدرت روند بر اساس اندیکاتورها"""
        strength = 0.0
        
        # معیار RSI
        rsi = indicators.get('rsi', 50)
        if rsi > 70 or rsi < 30:
            strength += 0.4
        
        # معیار MACD
        macd_hist = indicators.get('macd_histogram', 0)
        if abs(macd_hist) > 0.5:
            strength += 0.3
        
        # معیار Bollinger Bands
        close = indicators.get('close_price', 0)
        bollinger_high = indicators.get('bollinger_high', 0)
        bollinger_low = indicators.get('bollinger_low', 0)
        
        if bollinger_high > 0 and bollinger_low > 0:
            bb_position = (close - bollinger_low) / (bollinger_high - bollinger_low)
            if bb_position > 0.8 or bb_position < 0.2:
                strength += 0.3
        
        return min(strength, 1.0)

# تابع اصلی برای استفاده در routes (بدون تغییر)
def run_comprehensive_market_analysis(db_session: Session, symbol_ids: Optional[List[str]] = None, 
                                    limit: Optional[int] = None) -> Dict[str, Any]:
    """
    اجرای تحلیل جامع Real-time بازار - تابع اصلی برای استفاده در API
    """
    orchestrator = MarketAnalysisOrchestrator(db_session)
    return orchestrator.run_comprehensive_analysis(symbol_ids, limit)

__all__ = ['MarketAnalysisOrchestrator', 'run_comprehensive_market_analysis']