"""
Stock State Detector (Robustness Enhanced Version)

Detects market state based on stock's own historical volatility with:
- Dynamic thresholds based on ATR
- Volume confirmation with OBV
- Volatility percentile analysis
- Detailed scoring system
"""

import os
import logging
from typing import List, Tuple
from .market_regime import (
    MarketState,
    MarketRegime,
    MARKET_STATE_CONFIG,
    get_market_config
)

logger = logging.getLogger(__name__)


class StockStateDetector:
    """
    Stock market state detector based on its own historical volatility

    Features:
    1. Dynamic thresholds: Based on stock's own historical volatility
    2. Volume confirmation: Price-volume relationship analysis
    3. Volatility percentile: Position relative to its own history
    """

    def __init__(self, lookback_days: int = 180):
        """
        Initialize the detector

        Args:
            lookback_days: Number of trading days to look back (default 180, approximately 9 months)
            Reduced from 240 to 180 to accommodate stocks with limited historical data
        """
        self.lookback_days = lookback_days
        # Use current config from thread-safe getter
        self.config = get_market_config()

    def detect_state(self, data, stock_code: str = "", verbose: bool = True) -> MarketState:
        """
        Detect stock market state

        Args:
            data: backtrader data source
            stock_code: Stock code for logging
            verbose: Whether to output detailed logs

        Returns:
            MarketState object
        """
        # Ensure sufficient data
        if len(data) < self.lookback_days:
            if verbose:
                logger.warning(f"Insufficient data for {stock_code}: {len(data)} < {self.lookback_days}")
            return MarketState(MarketRegime.NEUTRAL, 0.5, 0, 0, 0, 0, 0)

        # Extract data (convert to lists for calculation)
        close = [data.close[-i] for i in range(self.lookback_days, 0, -1)]
        high = [data.high[-i] for i in range(self.lookback_days, 0, -1)]
        low = [data.low[-i] for i in range(self.lookback_days, 0, -1)]
        volume = [data.volume[-i] for i in range(self.lookback_days, 0, -1)]

        current_date = data.datetime.date(0)

        # Calculate individual scores
        trend_score, trend_details = self._calculate_trend_score(close)
        momentum_score, momentum_details = self._calculate_momentum_score(close, high, low)
        position_score, position_details = self._calculate_position_score(close, high, low)
        volume_score, volume_details = self._calculate_volume_score(close, volume)
        volatility_score, volatility_details = self._calculate_volatility_score(close, high, low)

        # Total score
        total_score = trend_score + momentum_score + position_score + volume_score + volatility_score

        # Determine market state
        if total_score >= self.config['bull_threshold']:
            regime = MarketRegime.BULL
            confidence = min(1.0, (total_score - self.config['bull_threshold']) / 30)
        elif total_score <= self.config['bear_threshold']:
            regime = MarketRegime.BEAR
            confidence = min(1.0, (self.config['bear_threshold'] - total_score) / self.config['bear_threshold'])
        else:
            regime = MarketRegime.NEUTRAL
            confidence = 0.5

        # Output detailed logs (skip in quiet mode for optimization)
        if verbose:
            # Check for quiet mode (used during optimization)
            quiet_mode = os.environ.get('BACKTEST_QUIET_MODE', '0') == '1'
            if not quiet_mode:
                self._log_detection_details(
                    stock_code, current_date,
                    trend_score, trend_details,
                    momentum_score, momentum_details,
                    position_score, position_details,
                    volume_score, volume_details,
                    volatility_score, volatility_details,
                    total_score, regime, confidence
                )

        return MarketState(
            regime=regime,
            confidence=confidence,
            ma_trend=trend_score,
            momentum=momentum_score,
            position_ratio=position_score / 20 if position_score > 0 else 0,
            volume_confirm=volume_score,
            volatility=volatility_score
        )

    def _calculate_trend_score(self, close: List[float]) -> Tuple[int, dict]:
        """
        Calculate trend score (30 points)

        Scoring:
        - MA20 > MA60 > MA120: 30 points (perfect bull)
        - MA20 > MA60: 15 points (short-term up)
        - MA20 < MA60 < MA120: -30 points (perfect bear)
        - MA20 < MA60: -15 points (short-term down)
        """
        ma20 = sum(close[-20:]) / 20
        ma60 = sum(close[-60:]) / 60
        ma120 = sum(close[-120:]) / 120

        score = 0
        arrangement = ""
        if ma20 > ma60 > ma120:
            score = 30
            arrangement = "完美多头"
        elif ma20 > ma60:
            score = 15
            arrangement = "短期向上"
        elif ma20 < ma60 < ma120:
            score = -30
            arrangement = "完美空头"
        elif ma20 < ma60:
            score = -15
            arrangement = "短期向下"
        else:
            score = 0
            arrangement = "混乱"

        details = {
            'ma20': ma20,
            'ma60': ma60,
            'ma120': ma120,
            'arrangement': arrangement
        }

        return score, details

    def _calculate_momentum_score(self, close: List[float], high: List[float], low: List[float]) -> Tuple[int, dict]:
        """
        Calculate momentum score (25 points) - Dynamic threshold

        Scoring:
        - ROC > 1.5 × ATR%: 25 points (strong momentum)
        - ROC > 0: 10 points (weak momentum)
        - ROC < -1.5 × ATR%: -25 points (strong negative momentum)
        - else: -10 points (weak negative momentum)
        """
        roc_period = self.config['roc_period']
        roc_20 = (close[-1] - close[-roc_period]) / close[-roc_period] * 100

        # Calculate ATR percentage
        atr_20 = self._calculate_atr(high[-roc_period:], low[-roc_period:], close[-roc_period:])
        atr_pct = atr_20 / close[-1] * 100 if close[-1] > 0 else 0

        # Dynamic threshold
        dynamic_threshold = self.config['roc_dynamic_multiplier'] * atr_pct

        # Determine score
        if roc_20 > dynamic_threshold:
            score = 25
            momentum_type = "强势动量"
        elif roc_20 > 0:
            score = 10
            momentum_type = "弱势动量"
        elif roc_20 < -dynamic_threshold:
            score = -25
            momentum_type = "强势负动量"
        else:
            score = -10
            momentum_type = "弱势负动量"

        details = {
            'roc_20': roc_20,
            'atr_20': atr_20,
            'atr_pct': atr_pct,
            'dynamic_threshold': dynamic_threshold,
            'momentum_type': momentum_type
        }

        return score, details

    def _calculate_position_score(self, close: List[float], high: List[float], low: List[float]) -> Tuple[int, dict]:
        """
        Calculate position score (20 points)

        Scoring:
        - Near/breaking 60-day high: 20 points
        - Upper 70%: 15 points
        - Upper 50%: 10 points
        - Upper 30%: 5 points
        - Below 30%: 0 points
        """
        lookback = 60
        high_60 = max(high[-lookback:])
        low_60 = min(low[-lookback:])

        if high_60 == low_60:
            return 10, {'position_ratio': 0.5, 'range': [low_60, high_60]}

        position_ratio = (close[-1] - low_60) / (high_60 - low_60)

        # Breakthrough bonus
        if close[-1] > high_60 * 0.98:  # Near or breaking 60-day high
            score = 20
            position_type = "突破区域"
        elif position_ratio > 0.7:
            score = 15
            position_type = "中上部"
        elif position_ratio > 0.5:
            score = 10
            position_type = "中部"
        elif position_ratio > 0.3:
            score = 5
            position_type = "中下部"
        else:
            score = 0
            position_type = "底部"

        details = {
            'high_60': high_60,
            'low_60': low_60,
            'current_price': close[-1],
            'position_ratio': position_ratio,
            'position_type': position_type
        }

        return score, details

    def _calculate_volume_score(self, close: List[float], volume: List[float]) -> Tuple[int, dict]:
        """
        Calculate volume score (15 points)

        Scoring:
        - Up-day volume > down-day volume by 1.5x: 15 points
        - Up-day volume > down-day volume: 10 points
        - Up-day volume > 0.8x down-day volume: 5 points
        - Otherwise: 0 points
        + OBV new high bonus (max +5)
        """
        lookback = 10
        up_days_volume = []
        down_days_volume = []

        for i in range(-lookback, 0):
            if close[i] > close[i-1]:  # Up day
                up_days_volume.append(volume[i])
            elif close[i] < close[i-1]:  # Down day
                down_days_volume.append(volume[i])

        if up_days_volume and down_days_volume:
            avg_up_volume = sum(up_days_volume) / len(up_days_volume)
            avg_down_volume = sum(down_days_volume) / len(down_days_volume)
            volume_ratio = avg_up_volume / avg_down_volume if avg_down_volume > 0 else 1
        else:
            avg_up_volume = 0
            avg_down_volume = 0
            volume_ratio = 1

        # OBV simplified check
        obv_new_high = close[-1] > close[-5]  # Price at 5-day high

        # Base score
        if volume_ratio > 1.5:
            base_score = 15
            volume_type = "放量上涨"
        elif volume_ratio > 1.0:
            base_score = 10
            volume_type = "量增价涨"
        elif volume_ratio > 0.8:
            base_score = 5
            volume_type = "量价持平"
        else:
            base_score = 0
            volume_type = "缩量上涨"

        # OBV bonus
        obv_bonus = min(5, 5) if (obv_new_high and base_score > 0) else 0
        final_score = min(base_score + obv_bonus, 15)

        details = {
            'avg_up_volume': avg_up_volume,
            'avg_down_volume': avg_down_volume,
            'volume_ratio': volume_ratio,
            'volume_type': volume_type,
            'obv_new_high': obv_new_high,
            'obv_bonus': obv_bonus
        }

        return final_score, details

    def _calculate_volatility_score(self, close: List[float], high: List[float], low: List[float]) -> Tuple[int, dict]:
        """
        Calculate volatility score (10 points) - Percentile analysis

        Scoring:
        - ATR percentile < 40%: 10 points (volatility contraction, breakout signal)
        - ATR percentile > 80%: -10 points (abnormal high volatility, risk)
        - Otherwise: 0 points (normal volatility)
        """
        # Calculate historical ATR
        atr_history = []
        atr_period = 20
        history_length = self.lookback_days  # Use lookback_days instead of fixed 250

        for i in range(history_length, atr_period, -1):
            start_idx = max(0, len(high) - i)
            end_idx = max(atr_period, len(high) - i + atr_period)
            atr = self._calculate_atr(high[start_idx:end_idx], low[start_idx:end_idx], close[start_idx:end_idx])
            if atr > 0:
                atr_history.append(atr)

        if not atr_history:
            return 0, {'percentile': 50, 'volatility_type': '数据不足'}

        current_atr = self._calculate_atr(high[-atr_period:], low[-atr_period:], close[-atr_period:])
        percentile = (sum(x < current_atr for x in atr_history) / len(atr_history)) * 100

        # Determine score
        if percentile < self.config['volatility_percentile_low']:
            score = 10
            volatility_type = "收缩(突破前兆)"
        elif percentile > self.config['volatility_percentile_high']:
            score = -10
            volatility_type = "扩张(风险)"
        else:
            score = 0
            volatility_type = "正常"

        details = {
            'current_atr': current_atr,
            'atr_percentile': percentile,
            'volatility_type': volatility_type
        }

        return score, details

    def _calculate_atr(self, high: List[float], low: List[float], close: List[float]) -> float:
        """Calculate Average True Range"""
        if len(high) < 2 or len(low) < 2 or len(close) < 2:
            return 0

        tr_list = []
        for i in range(1, len(high)):
            tr = max(
                high[i] - low[i],
                abs(high[i] - close[i-1]),
                abs(low[i] - close[i-1])
            )
            tr_list.append(tr)

        return sum(tr_list) / len(tr_list) if tr_list else 0

    def _log_detection_details(
        self,
        stock_code: str,
        current_date,
        trend_score: int,
        trend_details: dict,
        momentum_score: int,
        momentum_details: dict,
        position_score: int,
        position_details: dict,
        volume_score: int,
        volume_details: dict,
        volatility_score: int,
        volatility_details: dict,
        total_score: int,
        regime: MarketRegime,
        confidence: float
    ):
        """Output market state detection logs - INFO for summary, DEBUG for details"""
        # INFO: 简洁的总分输出
        logger.info(f"[{stock_code}] 总分: {total_score}/100 → 市场状态: {regime.value} (置信度: {confidence:.2f})")

        # DEBUG: 详细得分信息（需要时通过调整日志级别查看）
        logger.debug("=" * 60)
        logger.debug(f"【市场状态检测详情】{stock_code} - {current_date}")
        logger.debug("-" * 60)
        logger.debug(f"趋势分 ({trend_score}/30):")
        logger.debug(f"  MA20={trend_details['ma20']:.2f}, MA60={trend_details['ma60']:.2f}, MA120={trend_details['ma120']:.2f}")
        logger.debug(f"  排列: {trend_details['arrangement']}")
        logger.debug(f"动能分 ({momentum_score}/25):")
        logger.debug(f"  ROC(20)={momentum_details['roc_20']:.2f}%")
        logger.debug(f"  ATR(20)={momentum_details['atr_20']:.2f} ({momentum_details['atr_pct']:.2f}%)")
        logger.debug(f"  动态阈值={momentum_details['dynamic_threshold']:.2f}%")
        logger.debug(f"  判断: {momentum_details['momentum_type']}")
        logger.debug(f"位置分 ({position_score}/20):")
        logger.debug(f"  60日区间: [{position_details['low_60']:.2f}, {position_details['high_60']:.2f}]")
        logger.debug(f"  当前价: {position_details['current_price']:.2f}")
        logger.debug(f"  位置比例: {position_details['position_ratio']*100:.1f}%")
        logger.debug(f"资金分 ({volume_score}/15):")
        logger.debug(f"  上涨日均量: {volume_details['avg_up_volume']:.0f}")
        logger.debug(f"  下跌日均量: {volume_details['avg_down_volume']:.0f}")
        logger.debug(f"  量比: {volume_details['volume_ratio']:.2f}")
        logger.debug(f"  类型: {volume_details['volume_type']}")
        logger.debug(f"波动分 ({volatility_score}/10):")
        logger.debug(f"  当前ATR: {volatility_details['current_atr']:.2f}")
        logger.debug(f"  ATR分位数: {volatility_details['atr_percentile']:.1f}%")
        logger.debug(f"  判断: {volatility_details['volatility_type']}")
        logger.debug("-" * 60)
        logger.debug(f"总分: {total_score}/100 → 市场状态: {regime.value} (置信度: {confidence:.2f})")
        logger.debug("=" * 60)
