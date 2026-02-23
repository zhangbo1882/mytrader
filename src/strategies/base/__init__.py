#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
策略基础组件

包含共享的注册表、分析器和指标
"""
from .registry import (
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
from .analyzers import PortfolioValueAnalyzer, TradeAnalyzer, calculate_strategy_metrics
from .metrics import (
    strategy_health_check,
    log_strategy_health_report,
    calculate_backtest_returns,
    StrategyHealthAnalyzer
)

__all__ = [
    # Registry
    'STRATEGY_CLASSES',
    'STRATEGY_PARAMS_SCHEMA',
    'STRATEGY_DESCRIPTIONS',
    'get_strategy_params_schema',
    'get_strategy_class',
    'get_strategy_description',
    'get_supported_strategies',
    'validate_strategy_params',
    'get_default_params',
    # Analyzers
    'PortfolioValueAnalyzer',
    'TradeAnalyzer',
    'calculate_strategy_metrics',
    # Metrics
    'strategy_health_check',
    'log_strategy_health_report',
    'calculate_backtest_returns',
    'StrategyHealthAnalyzer',
]
