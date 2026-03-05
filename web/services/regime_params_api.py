#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Regime Params API - 提供最优参数查询服务
"""

from flask import request
from flask_restx import Resource, fields
import logging

from src.strategies.price_breakout.optimizer.params_db import RegimeParamsDB
from src.strategies.base.registry import get_default_params

logger = logging.getLogger(__name__)


def get_optimized_params(stock_code: str) -> dict:
    """
    获取股票的最优参数

    Args:
        stock_code: 股票代码

    Returns:
        包含最优参数的字典，如果不存在返回None
    """
    db = RegimeParamsDB()
    result = db.get_best_params(stock_code)
    db.close()

    if result:
        # 提取策略参数
        strategy_params = {}
        if 'metrics' in result and 'strategy_info' in result['metrics']:
            strategy_params = result['metrics']['strategy_info']['strategy_params']

        # 获取默认参数并合并（确保缺失的参数使用默认值）
        default_params = get_default_params('price_breakout_v2')
        merged_params = {**default_params, **strategy_params}

        return {
            'stock_code': result['stock_code'],
            'strategy': 'price_breakout',
            'strategy_params': merged_params,
            'regime_params': result['regime_params'],
            'market_config': result['market_config'],
            'score': result['score'],
            'performance': {
                'total_return': result['metrics']['basic_info']['total_return'],
                'sharpe_ratio': result['metrics']['health_metrics']['sharpe_ratio'],
                'max_drawdown': result['metrics']['health_metrics']['max_drawdown'],
                'win_rate': result['metrics']['trade_stats']['win_rate'],
                'total_trades': result['metrics']['trade_stats']['total_trades'],
            },
            'data_period': {
                'start_date': result['start_date'],
                'end_date': result['end_date'],
            },
            'updated_at': result['updated_at']
        }
    return None


def has_optimized_params(stock_code: str) -> bool:
    """
    检查股票是否有已优化的参数
    
    Args:
        stock_code: 股票代码
        
    Returns:
        True if exists, False otherwise
    """
    db = RegimeParamsDB()
    result = db.get_best_params(stock_code)
    db.close()
    return result is not None
