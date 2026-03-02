"""
Market Regime Service - 牛熊市判断服务 (v2.0)

基于五个正交维度对单只A股进行牛熊状态评分：
1. 趋势分 (30分): EMA多空排列 + SMA方向锚
2. 动能分 (25分): 两期ROC加速度（与趋势分正交）
3. 量价分 (20分): 20日涨跌日量比 + 量价背离检测
4. 换手率分 (15分): 5日换手率 vs 历史均值（仅正分）
5. 波动分 (10分): ATR百分位，与趋势方向联动

支持多周期配置：短周期(EMA3/5/10+SMA20)、中周期(EMA5/10/20+SMA60)、长周期(EMA10/20/40+SMA120)

设计文档参见 src/market/MARKET_REGIME_RULES.md
"""

import pandas as pd
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


# 多周期配置预设（与 src/market/market_regime.py 保持一致）
CYCLE_CONFIGS = {
    'short': {
        'name': '短周期',
        'description': 'EMA 3/5/10 + SMA20锚, 适合短线交易',
        'lookback_days': 30,
        'ma_short': 3,
        'ma_medium': 5,
        'ma_long': 10,
        'ma_anchor': 20,
        'roc_period': 5,
        'roc_dynamic_multiplier': 1.5,
        'bull_threshold': 70,
        'bear_threshold': 40,
        'volatility_percentile_low': 40,
        'volatility_percentile_high': 80,
        'volume_lookback': 10,
        'atr_period': 3,
        'turnover_lookback_short': 3,
        'turnover_lookback_long': 20,
    },
    'medium': {
        'name': '中周期',
        'description': 'EMA 5/10/20 + SMA60锚, 适合波段交易',
        'lookback_days': 60,
        'ma_short': 5,
        'ma_medium': 10,
        'ma_long': 20,
        'ma_anchor': 60,
        'roc_period': 10,
        'roc_dynamic_multiplier': 1.5,
        'bull_threshold': 70,
        'bear_threshold': 40,
        'volatility_percentile_low': 40,
        'volatility_percentile_high': 80,
        'volume_lookback': 20,
        'atr_period': 5,
        'turnover_lookback_short': 5,
        'turnover_lookback_long': 60,
    },
    'long': {
        'name': '长周期',
        'description': 'EMA 10/20/40 + SMA120锚, 适合中长线投资',
        'lookback_days': 120,
        'ma_short': 10,
        'ma_medium': 20,
        'ma_long': 40,
        'ma_anchor': 120,
        'roc_period': 20,
        'roc_dynamic_multiplier': 1.5,
        'bull_threshold': 70,
        'bear_threshold': 40,
        'volatility_percentile_low': 40,
        'volatility_percentile_high': 80,
        'volume_lookback': 40,
        'atr_period': 10,
        'turnover_lookback_short': 10,
        'turnover_lookback_long': 90,
    },
}


def get_cycle_config(cycle: str = 'medium') -> dict:
    """获取指定周期的配置"""
    if cycle not in CYCLE_CONFIGS:
        logger.warning(f"未知的周期类型 '{cycle}'，使用默认 'medium'")
        cycle = 'medium'
    config = CYCLE_CONFIGS[cycle].copy()
    config.pop('name', None)
    config.pop('description', None)
    return config


def get_all_cycle_configs() -> dict:
    """获取所有周期的配置（包含 name 和 description）"""
    return CYCLE_CONFIGS.copy()


class MarketRegimeService:
    """牛熊市判断服务 (v2.0)"""

    def __init__(self, config: Dict = None, cycle: str = None):
        """
        初始化服务

        Args:
            config: 自定义配置字典（最高优先级）
            cycle: 周期类型 ('short', 'medium', 'long')
        """
        if config is not None:
            self.config = config
        elif cycle is not None:
            self.config = get_cycle_config(cycle)
            self.cycle = cycle
        else:
            self.config = get_cycle_config('medium')
            self.cycle = 'medium'

    def calculate_regime_history(self, data: List[Dict]) -> List[Dict]:
        """
        计算历史牛熊市状态

        Args:
            data: K线数据列表，包含字段：
                  必须: open, high, low, close, volume
                  可选: turnover_rate（有则计算换手率分，无则该维度得0分）

        Returns:
            每个交易日的牛熊市状态和各维度得分列表
        """
        if len(data) < self.config['lookback_days']:
            logger.warning(f"数据不足: {len(data)} < {self.config['lookback_days']}")
            return []

        df = pd.DataFrame(data)
        df = df.sort_values('datetime' if 'datetime' in df.columns else 'date')
        has_turnover = 'turnover_rate' in df.columns

        results = []
        lookback = self.config['lookback_days']

        for i in range(lookback, len(df)):
            try:
                date = df.iloc[i]['datetime'] if 'datetime' in df.columns else df.iloc[i]['date']
                close_price = df.iloc[i]['close']

                window_df = df.iloc[i - lookback + 1:i + 1]
                close_list = window_df['close'].tolist()
                high_list = window_df['high'].tolist()
                low_list = window_df['low'].tolist()
                volume_list = window_df['volume'].tolist()
                turnover_list = window_df['turnover_rate'].tolist() if has_turnover else None

                trend_score, _ = self._calculate_trend_score(close_list)
                momentum_score, _ = self._calculate_momentum_score(close_list, high_list, low_list)
                volume_score, _ = self._calculate_volume_score(close_list, volume_list)
                turnover_score, _ = self._calculate_turnover_score(turnover_list)
                volatility_score, _ = self._calculate_volatility_score(
                    close_list, high_list, low_list, trend_score
                )

                total_score = trend_score + momentum_score + volume_score + turnover_score + volatility_score

                if total_score >= self.config['bull_threshold']:
                    regime = 'bull'
                elif total_score <= self.config['bear_threshold']:
                    regime = 'bear'
                else:
                    regime = 'neutral'

                results.append({
                    'date': str(date),
                    'close': close_price,
                    'regime': regime,
                    'total_score': total_score,
                    'trend_score': trend_score,
                    'momentum_score': momentum_score,
                    'volume_score': volume_score,
                    'turnover_score': turnover_score,
                    'volatility_score': volatility_score,
                })

            except Exception as e:
                logger.error(f"计算牛熊市状态失败 (index={i}): {e}")
                continue

        return results

    def calculate_multi_cycle_regimes(self, data: List[Dict], cycles: List[str] = None) -> Dict[str, List[Dict]]:
        """
        计算多个周期的牛熊市状态

        Args:
            data: K线数据列表
            cycles: 周期列表，默认 ['short', 'medium', 'long']

        Returns:
            每个周期的牛熊市状态历史，key为周期名称
        """
        if cycles is None:
            cycles = ['short', 'medium', 'long']

        results = {}
        for cycle in cycles:
            if cycle not in CYCLE_CONFIGS:
                logger.warning(f"未知的周期类型: {cycle}")
                continue
            service = MarketRegimeService(cycle=cycle)
            cycle_results = service.calculate_regime_history(data)
            if cycle_results:
                results[cycle] = cycle_results

        return results

    # -------------------------------------------------------------------------
    # EMA 计算工具
    # -------------------------------------------------------------------------

    def _calculate_ema(self, prices: List[float], period: int) -> float:
        """计算指数移动平均（EMA）"""
        if len(prices) < period:
            return sum(prices) / len(prices) if prices else 0
        k = 2.0 / (period + 1)
        ema = sum(prices[:period]) / period
        for price in prices[period:]:
            ema = price * k + ema * (1 - k)
        return ema

    # -------------------------------------------------------------------------
    # 维度1：趋势分（±30分）
    # -------------------------------------------------------------------------

    def _calculate_trend_score(self, close: List[float]) -> tuple:
        """趋势分（EMA多空排列 + SMA方向锚）"""
        ma_short = self.config['ma_short']
        ma_medium = self.config['ma_medium']
        ma_long = self.config['ma_long']
        ma_anchor = self.config.get('ma_anchor', 60)

        ema_s = self._calculate_ema(close, ma_short)
        ema_m = self._calculate_ema(close, ma_medium)
        ema_l = self._calculate_ema(close, ma_long)

        anchor_window = close[-ma_anchor:] if len(close) >= ma_anchor else close
        sma_anchor = sum(anchor_window) / len(anchor_window)
        above_anchor = close[-1] > sma_anchor

        if ema_s > ema_m > ema_l:
            score = 30 if above_anchor else 15
        elif ema_s > ema_m:
            score = 10 if above_anchor else 5
        elif ema_s < ema_m < ema_l:
            score = -30 if not above_anchor else -15
        elif ema_s < ema_m:
            score = -10 if not above_anchor else -5
        else:
            score = 0

        return score, {'ema_short': ema_s, 'ema_medium': ema_m, 'ema_long': ema_l, 'sma_anchor': sma_anchor}

    # -------------------------------------------------------------------------
    # 维度2：动能分（±25分）
    # -------------------------------------------------------------------------

    def _calculate_momentum_score(self, close: List[float], high: List[float], low: List[float]) -> tuple:
        """动能分（四象限加速度评分，区分涨跌方向）"""
        roc_period = self.config['roc_period']
        atr_period = self.config.get('atr_period', roc_period)

        min_required = 2 * roc_period + 1
        if len(close) < min_required:
            return 0, {'acceleration': 0, 'momentum_type': '数据不足'}

        current_roc = (close[-1] - close[-roc_period - 1]) / close[-roc_period - 1] * 100
        prev_roc = (close[-roc_period - 1] - close[-2 * roc_period - 1]) / close[-2 * roc_period - 1] * 100
        acceleration = current_roc - prev_roc

        atr_window = max(atr_period, 2)
        atr = self._calculate_atr(high[-atr_window:], low[-atr_window:], close[-atr_window:])
        atr_pct = atr / close[-1] * 100 if close[-1] > 0 else 0
        dynamic_threshold = self.config['roc_dynamic_multiplier'] * atr_pct

        # 四象限：当前方向（current_roc）为主，加速度为修正
        if current_roc > 0:
            if acceleration > dynamic_threshold:      score = 25
            elif acceleration >= 0:                   score = 15
            elif acceleration >= -dynamic_threshold:  score = 10
            else:                                     score = -5
        else:
            if acceleration > dynamic_threshold:      score = 5
            elif acceleration >= 0:                   score = -5
            elif acceleration >= -dynamic_threshold:  score = -15
            else:                                     score = -25

        return score, {'current_roc': current_roc, 'prev_roc': prev_roc, 'acceleration': acceleration}

    # -------------------------------------------------------------------------
    # 维度3：量价分（0~+20分）
    # -------------------------------------------------------------------------

    def _calculate_volume_score(self, close: List[float], volume: List[float]) -> tuple:
        """量价分（20日涨跌日量比 + 量价背离检测）"""
        lookback = self.config.get('volume_lookback', 20)
        actual_lookback = min(lookback, len(close) - 1)

        up_days_volume = []
        down_days_volume = []
        for i in range(-actual_lookback, 0):
            if close[i] > close[i - 1]:
                up_days_volume.append(volume[i])
            elif close[i] < close[i - 1]:
                down_days_volume.append(volume[i])

        if up_days_volume and down_days_volume:
            avg_up = sum(up_days_volume) / len(up_days_volume)
            avg_down = sum(down_days_volume) / len(down_days_volume)
            volume_ratio = avg_up / avg_down if avg_down > 0 else 2.0
        elif up_days_volume:
            avg_up = sum(up_days_volume) / len(up_days_volume)
            avg_down = 0
            volume_ratio = 2.0
        else:
            avg_up = 0
            avg_down = sum(down_days_volume) / len(down_days_volume) if down_days_volume else 0
            volume_ratio = 0.3

        if volume_ratio > 2.0:
            base_score = 18
        elif volume_ratio > 1.5:
            base_score = 14
        elif volume_ratio > 1.0:
            base_score = 10
        elif volume_ratio > 0.8:
            base_score = 5
        else:
            base_score = 0

        # 量价背离检测
        if len(close) >= 5 and len(volume) >= actual_lookback > 0:
            price_at_5d_high = close[-1] >= max(close[-5:])
            recent_avg_vol = sum(volume[-5:]) / 5
            baseline_avg_vol = sum(volume[-actual_lookback:]) / actual_lookback
            volume_shrinking = recent_avg_vol < baseline_avg_vol * 0.8
            if price_at_5d_high and volume_shrinking:
                base_score = max(0, base_score - 5)

        obv_new_high = len(close) >= 4 and close[-1] > close[-4]
        obv_bonus = 2 if (obv_new_high and base_score > 0) else 0
        final_score = min(base_score + obv_bonus, 20)

        return final_score, {'volume_ratio': volume_ratio}

    # -------------------------------------------------------------------------
    # 维度4：换手率分（0~+15分）
    # -------------------------------------------------------------------------

    def _calculate_turnover_score(self, turnover_rate: Optional[List[float]]) -> tuple:
        """换手率/情绪分（仅正分，捕捉散户参与热度）"""
        if not turnover_rate:
            return 0, {'turnover_ratio': None}

        short_period = self.config.get('turnover_lookback_short', 5)
        long_period = self.config.get('turnover_lookback_long', 60)

        valid = [t for t in turnover_rate if t is not None and t > 0]
        if len(valid) < short_period:
            return 0, {'turnover_ratio': None}

        short_avg = sum(valid[-short_period:]) / short_period
        actual_long = min(long_period, len(valid))
        long_avg = sum(valid[-actual_long:]) / actual_long

        if long_avg <= 0:
            return 0, {'turnover_ratio': None}

        ratio = short_avg / long_avg

        if 1.5 <= ratio <= 3.0:
            score = 15
        elif ratio > 3.0:
            score = 8
        elif 1.0 <= ratio < 1.5:
            score = 10
        elif 0.5 <= ratio < 1.0:
            score = 5
        elif 0.3 <= ratio < 0.5:
            score = 3
        else:
            score = 0

        return score, {'turnover_ratio': ratio}

    # -------------------------------------------------------------------------
    # 维度5：波动分（±10分，与趋势联动）
    # -------------------------------------------------------------------------

    def _calculate_volatility_score(
        self, close: List[float], high: List[float], low: List[float], trend_score: int = 0
    ) -> tuple:
        """波动分（ATR百分位，与趋势方向联动）"""
        atr_period = self.config.get('atr_period', 5)
        history_length = min(len(close), 40)

        atr_history = []
        for i in range(history_length, atr_period, -1):
            start_idx = len(high) - i
            end_idx = len(high) - i + atr_period
            if start_idx >= 0 and end_idx <= len(high):
                atr = self._calculate_atr(high[start_idx:end_idx], low[start_idx:end_idx], close[start_idx:end_idx])
                if atr > 0:
                    atr_history.append(atr)

        if not atr_history:
            return 0, {'percentile': 50}

        current_atr = self._calculate_atr(high[-atr_period:], low[-atr_period:], close[-atr_period:])
        percentile = (sum(x < current_atr for x in atr_history) / len(atr_history)) * 100

        low_thr = self.config['volatility_percentile_low']
        high_thr = self.config['volatility_percentile_high']

        if trend_score > 0:
            if percentile < low_thr:
                score = 10
            elif percentile < 70:
                score = 5
            else:
                score = -5
        elif trend_score < 0:
            if percentile > high_thr:
                score = 5
            elif percentile > low_thr:
                score = -5
            else:
                score = -10
        else:
            score = 0

        return score, {'percentile': percentile}

    # -------------------------------------------------------------------------
    # ATR 计算工具
    # -------------------------------------------------------------------------

    def _calculate_atr(self, high: List[float], low: List[float], close: List[float]) -> float:
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


# 单例
_market_regime_service: Optional[MarketRegimeService] = None


def get_market_regime_service() -> MarketRegimeService:
    """获取牛熊市判断服务实例"""
    global _market_regime_service
    if _market_regime_service is None:
        _market_regime_service = MarketRegimeService()
    return _market_regime_service
