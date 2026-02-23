#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
交易策略模块

包含各种Backtrader策略实现、分析器和策略评估指标
"""
from src.strategies.base.registry import (
    STRATEGY_CLASSES,
    STRATEGY_PARAMS_SCHEMA,
    STRATEGY_DESCRIPTIONS,
    get_strategy_params_schema,
    get_strategy_class,
    get_strategy_description,
    get_supported_strategies,
    validate_strategy_params,
    get_default_params
)
from src.strategies.base.analyzers import PortfolioValueAnalyzer, TradeAnalyzer, calculate_strategy_metrics
from src.strategies.base.metrics import (
    strategy_health_check,
    log_strategy_health_report,
    calculate_backtest_returns,
    StrategyHealthAnalyzer
)
from src.strategies.sma_cross.strategy import SMACrossStrategy
from src.strategies.price_breakout.strategy import PriceBreakoutStrategyV2 as PriceBreakoutStrategy

__all__ = [
    'SMACrossStrategy',
    'PriceBreakoutStrategy',
    'PortfolioValueAnalyzer',
    'TradeAnalyzer',
    'calculate_strategy_metrics',
    'strategy_health_check',
    'log_strategy_health_report',
    'calculate_backtest_returns',
    'StrategyHealthAnalyzer',
    # Registry exports
    'STRATEGY_CLASSES',
    'STRATEGY_PARAMS_SCHEMA',
    'STRATEGY_DESCRIPTIONS',
    'get_strategy_params_schema',
    'get_strategy_class',
    'get_strategy_description',
    'get_supported_strategies',
    'validate_strategy_params',
    'get_default_params'
]
