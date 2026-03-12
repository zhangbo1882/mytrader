"""
Market Regime Service - 牛熊市判断服务 (v3.0)

本模块是 RegimeCalculator 的 API 封装层，专门用于处理 REST API 请求。
所有计算逻辑统一在 RegimeCalculator 中实现，确保 API 和回测结果一致。

设计文档参见 src/market/MARKET_REGIME_RULES.md
"""

import pandas as pd
from typing import List, Dict, Any, Optional
import logging
import sys
import os

# 添加项目根目录到路径，确保能导入 src 模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.market.regime_calculator import RegimeCalculator

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
    """
    牛熊市判断服务 (v3.0)

    本类是 RegimeCalculator 的 API 适配器。
    所有计算逻辑委托给 RegimeCalculator，确保与回测计算结果一致。
    """

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

                # 调用统一的计算器
                result = RegimeCalculator.calculate_regime(
                    close=close_list,
                    high=high_list,
                    low=low_list,
                    volume=volume_list,
                    turnover_rate=turnover_list,
                    config=self.config
                )

                results.append({
                    'date': str(date),
                    'close': close_price,
                    'regime': result.regime,
                    'total_score': result.total_score,
                    'trend_score': result.trend_score,
                    'momentum_score': result.momentum_score,
                    'volume_score': result.volume_score,
                    'turnover_score': result.turnover_score,
                    'volatility_score': result.volatility_score,
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
