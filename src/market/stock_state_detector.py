"""
Stock State Detector (v3.0)

A股单只股票牛熊状态判断 - 回测专用

本模块是 RegimeCalculator 的薄封装层，专门用于处理 backtrader 数据格式。
所有计算逻辑统一在 RegimeCalculator 中实现，确保回测和 API 结果一致。

设计文档参见 MARKET_REGIME_RULES.md
"""

import os
import logging
from typing import Optional
from .market_regime import (
    MarketState,
    MarketRegime,
    get_market_config,
    get_cycle_config
)
from .regime_calculator import RegimeCalculator, RegimeResult

logger = logging.getLogger(__name__)

# 状态枚举映射
REGIME_MAP = {
    'bull': MarketRegime.BULL,
    'bear': MarketRegime.BEAR,
    'neutral': MarketRegime.NEUTRAL,
}


class StockStateDetector:
    """
    A股单只股票牛熊状态检测器（基于个股自身历史数据，自适应不同类型股票）

    本类是 RegimeCalculator 的 backtrader 数据适配器。
    所有计算逻辑委托给 RegimeCalculator，确保与 API 计算结果一致。

    周期配置（通过 cycle 参数选择）:
    - short:  EMA3/5/10 + SMA20锚，适合短线/题材股
    - medium: EMA5/10/20 + SMA60锚，适合波段/成长股（默认）
    - long:   EMA10/20/40 + SMA120锚，适合长线/蓝筹股
    """

    def __init__(self, lookback_days: int = None, cycle: str = None, config: dict = None):
        """
        初始化检测器

        Args:
            lookback_days: 回看天数（已废弃，请使用 cycle）
            cycle: 周期类型 ('short', 'medium', 'long')
            config: 自定义配置字典（最高优先级）

        优先级: config > cycle > lookback_days > default
        """
        if config is not None:
            self.config = config
        elif cycle is not None:
            self.config = get_cycle_config(cycle)
        else:
            self.config = get_market_config()
            if lookback_days is not None:
                self.config['lookback_days'] = lookback_days

        self.lookback_days = self.config['lookback_days']

    def detect_state(self, data, stock_code: str = "", verbose: bool = True) -> MarketState:
        """
        检测股票市场状态

        Args:
            data: backtrader data source（需包含 close/high/low/volume 字段）
            stock_code: 股票代码（用于日志）
            verbose: 是否输出详细日志

        Returns:
            MarketState 对象
        """
        if len(data) < self.lookback_days:
            if verbose:
                logger.warning(f"数据不足 {stock_code}: {len(data)} < {self.lookback_days}")
            return MarketState(MarketRegime.NEUTRAL, 0.5, 0, 0, 0, 0, 0)

        # 提取数据（倒序转正序列表，包含当天数据）
        close = [data.close[-i] for i in range(self.lookback_days - 1, -1, -1)]
        high = [data.high[-i] for i in range(self.lookback_days - 1, -1, -1)]
        low = [data.low[-i] for i in range(self.lookback_days - 1, -1, -1)]
        volume = [data.volume[-i] for i in range(self.lookback_days - 1, -1, -1)]

        # 尝试获取换手率数据（backtrader data可能不含此字段）
        try:
            turnover_rate = [data.turnover_rate[-i] for i in range(self.lookback_days - 1, -1, -1)]
        except AttributeError:
            turnover_rate = None

        current_date = data.datetime.date(0)

        # 调用统一的计算器
        result = RegimeCalculator.calculate_regime(
            close=close,
            high=high,
            low=low,
            volume=volume,
            turnover_rate=turnover_rate,
            config=self.config
        )

        # 转换为 MarketRegime 枚举
        regime = REGIME_MAP.get(result.regime, MarketRegime.NEUTRAL)

        if verbose:
            quiet_mode = os.environ.get('BACKTEST_QUIET_MODE', '0') == '1'
            if not quiet_mode:
                self._log_detection_details(
                    stock_code, current_date,
                    result.trend_score, result.details['trend'],
                    result.momentum_score, result.details['momentum'],
                    result.volume_score, result.details['volume'],
                    result.turnover_score, result.details['turnover'],
                    result.volatility_score, result.details['volatility'],
                    result.total_score, regime, result.confidence
                )

        return MarketState(
            regime=regime,
            confidence=result.confidence,
            ma_trend=result.trend_score,
            momentum=result.momentum_score,
            position_ratio=0,  # deprecated
            volume_confirm=result.volume_score,
            volatility=result.volatility_score,
            turnover_score=result.turnover_score,
        )

    def _log_detection_details(
        self,
        stock_code: str,
        current_date,
        trend_score: int,
        trend_details: dict,
        momentum_score: int,
        momentum_details: dict,
        volume_score: int,
        volume_details: dict,
        turnover_score: int,
        turnover_details: dict,
        volatility_score: int,
        volatility_details: dict,
        total_score: int,
        regime: MarketRegime,
        confidence: float
    ):
        """输出详细日志"""
        logger.info(
            f"[{stock_code}] 总分: {total_score}/100 → {regime.value} "
            f"(置信度:{confidence:.2f}) "
            f"趋势:{trend_score} 动能:{momentum_score} "
            f"量价:{volume_score} 换手:{turnover_score} 波动:{volatility_score}"
        )
