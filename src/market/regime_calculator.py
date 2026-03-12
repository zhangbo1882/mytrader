"""
牛熊状态核心计算模块

所有维度计算逻辑的单一来源，供回测和API共同调用。
确保两边的计算结果完全一致。
"""

from typing import List, Tuple, Dict, Any, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class RegimeResult:
    """牛熊状态计算结果"""
    total_score: int
    regime: str  # 'bull', 'bear', 'neutral'
    trend_score: int
    momentum_score: int
    volume_score: int
    turnover_score: int
    volatility_score: int
    confidence: float
    details: Dict[str, Any]


class RegimeCalculator:
    """
    牛熊状态核心计算器

    统一的计算逻辑，供以下场景调用：
    1. StockStateDetector (回测) - 传入 backtrader 数据
    2. MarketRegimeService (API) - 传入普通列表数据

    所有维度计算方法都是静态方法，接受标准化的列表参数。
    """

    @staticmethod
    def calculate_ema(prices: List[float], period: int) -> float:
        """计算指数移动平均（EMA）"""
        if len(prices) < period:
            return sum(prices) / len(prices) if prices else 0
        k = 2.0 / (period + 1)
        ema = sum(prices[:period]) / period
        for price in prices[period:]:
            ema = price * k + ema * (1 - k)
        return ema

    @staticmethod
    def calculate_sma(prices: List[float], period: int) -> float:
        """计算简单移动平均（SMA）"""
        if len(prices) < period:
            return sum(prices) / len(prices) if prices else 0
        return sum(prices[-period:]) / period

    @staticmethod
    def calculate_trend_score(
        close: List[float],
        ma_short: int = 5,
        ma_medium: int = 10,
        ma_long: int = 20,
        ma_anchor: int = 60
    ) -> Tuple[int, Dict[str, Any]]:
        """
        趋势分（±30分）- EMA多空头排列 + SMA方向锚

        设计原理：
        - 用EMA代替SMA：A股散户普遍用MACD（基于EMA）
        - SMA锚作为中期方向验证：区分真正趋势与熊市反弹
        """
        ema_s = RegimeCalculator.calculate_ema(close, ma_short)
        ema_m = RegimeCalculator.calculate_ema(close, ma_medium)
        ema_l = RegimeCalculator.calculate_ema(close, ma_long)

        # SMA锚（用最近N天的SMA）
        anchor_window = close[-ma_anchor:] if len(close) >= ma_anchor else close
        sma_anchor = sum(anchor_window) / len(anchor_window)
        above_anchor = close[-1] > sma_anchor

        # 多空排列判断
        bullish_align = ema_s > ema_m > ema_l
        bearish_align = ema_s < ema_m < ema_l

        # 计算趋势分
        if bullish_align and above_anchor:
            score = 30  # 完美多头+锚上方
            trend_type = "完美多头+锚上方"
        elif bullish_align:
            score = 20  # 多头排列但锚下方
            trend_type = "多头排列(锚下方)"
        elif bearish_align:
            score = -30  # 空头排列
            trend_type = "空头排列"
        elif ema_s > ema_l:
            score = 15  # 短期上穿长期
            trend_type = "短期上穿长期"
        elif ema_s < ema_l:
            score = -15  # 短期下穿长期
            trend_type = "短期下穿长期"
        else:
            score = 5  # 震荡
            trend_type = "震荡"

        details = {
            'ema_s': ema_s,
            'ema_m': ema_m,
            'ema_l': ema_l,
            'sma_anchor': sma_anchor,
            'above_anchor': above_anchor,
            'bullish_align': bullish_align,
            'bearish_align': bearish_align,
            'trend_type': trend_type,
        }
        return score, details

    @staticmethod
    def calculate_momentum_score(
        close: List[float],
        high: List[float],
        low: List[float],
        roc_period: int = 10,
        roc_dynamic_multiplier: float = 1.5
    ) -> Tuple[int, Dict[str, Any]]:
        """
        动能分（±25分）- 两期ROC加速度

        设计原理：
        - 用两期ROC之差（加速度）而非单期ROC，与趋势分正交
        - 加速度比速度更敏感，能提前捕捉趋势衰竭
        """
        if len(close) < roc_period * 2 + 1:
            return 0, {'momentum_type': '数据不足'}

        # 两期ROC
        roc1 = (close[-1] - close[-roc_period]) / close[-roc_period] * 100
        roc2 = (close[-roc_period] - close[-roc_period * 2]) / close[-roc_period * 2] * 100

        # 加速度 = 近期变化率 - 远期变化率
        acceleration = roc1 - roc2

        # 动态阈值（基于近期波动）
        recent_range = max(high[-roc_period:]) - min(low[-roc_period:])
        base_price = close[-roc_period]
        dynamic_threshold = (recent_range / base_price * 100) * roc_dynamic_multiplier if base_price > 0 else 5

        # 计算动能分
        if acceleration > dynamic_threshold:
            score = 25
            momentum_type = "强势加速"
        elif acceleration > dynamic_threshold * 0.5:
            score = 15
            momentum_type = "温和加速"
        elif acceleration > 0:
            score = 5
            momentum_type = "微弱加速"
        elif acceleration > -dynamic_threshold * 0.5:
            score = -5
            momentum_type = "微弱减速"
        elif acceleration > -dynamic_threshold:
            score = -15
            momentum_type = "温和减速"
        else:
            score = -25
            momentum_type = "强势减速"

        details = {
            'roc1': roc1,
            'roc2': roc2,
            'acceleration': acceleration,
            'dynamic_threshold': dynamic_threshold,
            'momentum_type': momentum_type,
        }
        return score, details

    @staticmethod
    def calculate_volume_score(
        close: List[float],
        volume: List[float],
        volume_lookback: int = 20
    ) -> Tuple[int, Dict[str, Any]]:
        """
        量价分（20分）- 20日涨跌日量比 + 量价背离检测

        设计原理：
        - 20日（一个月）样本更可靠，不易被游资短期操纵
        - 量价背离（价格创新高但成交量萎缩）是经典顶部信号
        """
        actual_lookback = min(volume_lookback, len(close) - 1)

        up_days_volume = []
        down_days_volume = []
        for i in range(-actual_lookback, 0):
            if close[i] > close[i - 1]:
                up_days_volume.append(volume[i])
            elif close[i] < close[i - 1]:
                down_days_volume.append(volume[i])

        if up_days_volume and down_days_volume:
            avg_up_volume = sum(up_days_volume) / len(up_days_volume)
            avg_down_volume = sum(down_days_volume) / len(down_days_volume)
            volume_ratio = avg_up_volume / avg_down_volume if avg_down_volume > 0 else 2.0
        elif up_days_volume:
            avg_up_volume = sum(up_days_volume) / len(up_days_volume)
            avg_down_volume = 0
            volume_ratio = 2.0
        else:
            avg_up_volume = 0
            avg_down_volume = sum(down_days_volume) / len(down_days_volume) if down_days_volume else 0
            volume_ratio = 0.3

        # 基础分
        if volume_ratio > 2.0:
            base_score = 18
            volume_type = "强势量价配合"
        elif volume_ratio > 1.5:
            base_score = 14
            volume_type = "放量上涨"
        elif volume_ratio > 1.0:
            base_score = 10
            volume_type = "量增价涨"
        elif volume_ratio > 0.8:
            base_score = 5
            volume_type = "量价持平"
        else:
            base_score = 0
            volume_type = "缩量上涨(警惕)"

        # 量价背离检测
        price_at_5d_high = False
        volume_shrinking = False
        divergence_penalty = 0
        if len(close) >= 5 and len(volume) >= actual_lookback and actual_lookback > 0:
            price_at_5d_high = close[-1] >= max(close[-5:])
            recent_avg_vol = sum(volume[-5:]) / 5
            baseline_avg_vol = sum(volume[-actual_lookback:]) / actual_lookback
            volume_shrinking = recent_avg_vol < baseline_avg_vol * 0.8
            if price_at_5d_high and volume_shrinking:
                divergence_penalty = 5
                base_score = max(0, base_score - divergence_penalty)
                volume_type += "(量价背离！)"

        # OBV加成（3日新高且无背离）
        obv_new_high = len(close) >= 4 and close[-1] > close[-4]
        obv_bonus = 2 if (obv_new_high and base_score > 0 and not (price_at_5d_high and volume_shrinking)) else 0

        final_score = min(base_score + obv_bonus, 20)

        details = {
            'avg_up_volume': avg_up_volume,
            'avg_down_volume': avg_down_volume,
            'volume_ratio': volume_ratio,
            'volume_type': volume_type,
            'price_at_5d_high': price_at_5d_high,
            'volume_shrinking': volume_shrinking,
            'divergence_penalty': divergence_penalty,
            'obv_bonus': obv_bonus,
        }
        return final_score, details

    @staticmethod
    def calculate_turnover_score(
        turnover_rate: Optional[List[float]],
        turnover_lookback_short: int = 5,
        turnover_lookback_long: int = 60
    ) -> Tuple[int, Dict[str, Any]]:
        """
        换手率分（0~+15分，仅正分）

        设计原理：
        - 换手率高本身不是坏信号，高换手意味着市场活跃参与
        - 用5日/历史均值比值，自动适配蓝筹低换手与题材股高换手
        """
        if not turnover_rate or len(turnover_rate) < turnover_lookback_short:
            return 0, {'turnover_ratio': None, 'turnover_type': '无数据'}

        short_avg = sum(turnover_rate[-turnover_lookback_short:]) / turnover_lookback_short
        long_window = turnover_rate[-turnover_lookback_long:] if len(turnover_rate) >= turnover_lookback_long else turnover_rate
        long_avg = sum(long_window) / len(long_window)

        if long_avg <= 0:
            return 0, {'turnover_ratio': None, 'turnover_type': '基准为零'}

        ratio = short_avg / long_avg

        if ratio > 3.0:
            score = 8  # 爆量，谨慎给分
            turnover_type = "爆量换手"
        elif ratio > 2.0:
            score = 12
            turnover_type = "活跃换手"
        elif ratio > 1.5:
            score = 15  # 最佳区间
            turnover_type = "健康放量"
        elif ratio > 1.0:
            score = 10
            turnover_type = "温和放量"
        else:
            score = 5
            turnover_type = "缩量"

        details = {
            'short_avg': short_avg,
            'long_avg': long_avg,
            'turnover_ratio': ratio,
            'turnover_type': turnover_type,
        }
        return score, details

    @staticmethod
    def calculate_volatility_score(
        close: List[float],
        high: List[float],
        low: List[float],
        trend_score: int,
        atr_period: int = 5,
        volatility_percentile_low: int = 40,
        volatility_percentile_high: int = 80
    ) -> Tuple[int, Dict[str, Any]]:
        """
        波动分（±10分）- ATR百分位，与趋势方向联动

        设计原理：
        - 多头趋势中，低波动是好事（稳步上涨）
        - 空头趋势中，高波动是坏事（恐慌加剧）
        """
        if len(close) < atr_period + 1:
            return 0, {'volatility_type': '数据不足'}

        # 计算历史ATR
        history_length = min(len(close), 40)
        atr_history = []
        for i in range(history_length, atr_period, -1):
            start_idx = len(high) - i
            end_idx = len(high) - i + atr_period
            if start_idx >= 0 and end_idx <= len(high):
                atr = RegimeCalculator._calculate_atr(
                    high[start_idx:end_idx],
                    low[start_idx:end_idx],
                    close[start_idx:end_idx]
                )
                if atr > 0:
                    atr_history.append(atr)

        if not atr_history:
            return 0, {'percentile': 50, 'volatility_type': '数据不足'}

        # 计算当前ATR
        current_atr = RegimeCalculator._calculate_atr(
            high[-atr_period:], low[-atr_period:], close[-atr_period:]
        )
        percentile = (sum(x < current_atr for x in atr_history) / len(atr_history)) * 100

        # 计算波动分（与趋势联动）
        low_thr = volatility_percentile_low
        high_thr = volatility_percentile_high

        if trend_score > 0:  # 多头趋势
            if percentile < low_thr:
                score = 10  # 低波动+多头=稳步上涨，好事
                vol_type = "多头低波动(健康)"
            elif percentile < 70:
                score = 5
                vol_type = "多头中波动"
            else:
                score = -5  # 高波动+多头=可能过热
                vol_type = "多头高波动(警惕)"
        elif trend_score < 0:  # 空头趋势
            if percentile > high_thr:
                score = 5  # 高波动+空头=可能见底反弹
                vol_type = "空头高波动(可能反弹)"
            elif percentile > low_thr:
                score = -5
                vol_type = "空头中波动"
            else:
                score = -10  # 低波动+空头=阴跌不止
                vol_type = "空头低波动(危险)"
        else:  # 震荡
            score = 0
            vol_type = "震荡"

        details = {
            'current_atr': current_atr,
            'percentile': percentile,
            'volatility_type': vol_type,
        }
        return score, details

    @staticmethod
    def _calculate_atr(high: List[float], low: List[float], close: List[float]) -> float:
        """计算 ATR"""
        if len(high) < 2:
            return 0
        tr_list = []
        for i in range(1, len(high)):
            tr = max(
                high[i] - low[i],
                abs(high[i] - close[i - 1]),
                abs(low[i] - close[i - 1])
            )
            tr_list.append(tr)
        return sum(tr_list) / len(tr_list) if tr_list else 0

    @staticmethod
    def calculate_regime(
        close: List[float],
        high: List[float],
        low: List[float],
        volume: List[float],
        turnover_rate: Optional[List[float]] = None,
        config: Dict[str, Any] = None
    ) -> RegimeResult:
        """
        计算完整的牛熊状态

        Args:
            close: 收盘价列表（时间从早到晚）
            high: 最高价列表
            low: 最低价列表
            volume: 成交量列表
            turnover_rate: 换手率列表（可选）
            config: 配置参数，包含：
                - ma_short, ma_medium, ma_long, ma_anchor
                - roc_period, roc_dynamic_multiplier
                - volume_lookback
                - turnover_lookback_short, turnover_lookback_long
                - atr_period
                - volatility_percentile_low, volatility_percentile_high
                - bull_threshold, bear_threshold

        Returns:
            RegimeResult 对象
        """
        # 默认配置（中周期）
        default_config = {
            'ma_short': 5,
            'ma_medium': 10,
            'ma_long': 20,
            'ma_anchor': 60,
            'roc_period': 10,
            'roc_dynamic_multiplier': 1.5,
            'volume_lookback': 20,
            'turnover_lookback_short': 5,
            'turnover_lookback_long': 60,
            'atr_period': 5,
            'volatility_percentile_low': 40,
            'volatility_percentile_high': 80,
            'bull_threshold': 70,
            'bear_threshold': 40,
        }
        if config:
            default_config.update(config)
        cfg = default_config

        # 计算各维度得分
        trend_score, trend_details = RegimeCalculator.calculate_trend_score(
            close, cfg['ma_short'], cfg['ma_medium'], cfg['ma_long'], cfg['ma_anchor']
        )
        momentum_score, momentum_details = RegimeCalculator.calculate_momentum_score(
            close, high, low, cfg['roc_period'], cfg['roc_dynamic_multiplier']
        )
        volume_score, volume_details = RegimeCalculator.calculate_volume_score(
            close, volume, cfg['volume_lookback']
        )
        turnover_score, turnover_details = RegimeCalculator.calculate_turnover_score(
            turnover_rate, cfg['turnover_lookback_short'], cfg['turnover_lookback_long']
        )
        volatility_score, volatility_details = RegimeCalculator.calculate_volatility_score(
            close, high, low, trend_score, cfg['atr_period'],
            cfg['volatility_percentile_low'], cfg['volatility_percentile_high']
        )

        # 总分
        total_score = trend_score + momentum_score + volume_score + turnover_score + volatility_score

        # 判断状态
        if total_score >= cfg['bull_threshold']:
            regime = 'bull'
            confidence = min(1.0, (total_score - cfg['bull_threshold']) / 30)
        elif total_score <= cfg['bear_threshold']:
            regime = 'bear'
            confidence = min(1.0, (cfg['bear_threshold'] - total_score) / cfg['bear_threshold'])
        else:
            regime = 'neutral'
            confidence = 0.5

        details = {
            'trend': trend_details,
            'momentum': momentum_details,
            'volume': volume_details,
            'turnover': turnover_details,
            'volatility': volatility_details,
        }

        return RegimeResult(
            total_score=total_score,
            regime=regime,
            trend_score=trend_score,
            momentum_score=momentum_score,
            volume_score=volume_score,
            turnover_score=turnover_score,
            volatility_score=volatility_score,
            confidence=confidence,
            details=details
        )
