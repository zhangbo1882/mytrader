#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
策略注册表 - 管理所有可用的回测策略

该模块提供：
1. 策略类映射 (STRATEGY_CLASSES)
2. 策略参数模型 (STRATEGY_PARAMS_SCHEMA)
3. 策略描述信息 (STRATEGY_DESCRIPTIONS)
4. 策略验证和查询函数
"""
from typing import Optional, Tuple, Dict, Any, Type
from .sma_cross_strategy import SMACrossStrategy
from .price_breakout_strategy import PriceBreakoutStrategy


# ============================================================================
# 策略类映射
# ============================================================================

STRATEGY_CLASSES: Dict[str, Type] = {
    'sma_cross': SMACrossStrategy,
    'price_breakout': PriceBreakoutStrategy,
    # 未来扩展：
    # 'dual_ma': DualMAStrategy,
    # 'bollinger_bands': BollingerBandsStrategy,
}


# ============================================================================
# 策略参数模型 (JSON Schema 格式)
# ============================================================================

STRATEGY_PARAMS_SCHEMA: Dict[str, Dict[str, Any]] = {
    'sma_cross': {
        'type': 'object',
        'properties': {
            'maperiod': {
                'type': 'integer',
                'minimum': 1,
                'maximum': 100,
                'default': 10,
                'description': '移动平均线周期'
            }
        },
        'required': []
    },
    'price_breakout': {
        'type': 'object',
        'properties': {
            'buy_threshold': {
                'type': 'number',
                'minimum': 0.1,
                'maximum': 20.0,
                'default': 1.0,
                'description': '买入阈值（百分比），当日最低价低于开盘价此比例时触发买入'
            },
            'sell_threshold': {
                'type': 'number',
                'minimum': 0.1,
                'maximum': 50.0,
                'default': 5.0,
                'description': '止盈阈值（百分比），当日最高价高于买入价此比例时触发止盈卖出'
            },
            'stop_loss_threshold': {
                'type': 'number',
                'minimum': 1.0,
                'maximum': 50.0,
                'default': 10.0,
                'description': '止损阈值（百分比），当日最低价低于买入价此比例时触发止损卖出'
            }
        },
        'required': []
    },
    # 未来扩展示例：
    # 'dual_ma': {
    #     'type': 'object',
    #     'properties': {
    #         'fast_period': {
    #             'type': 'integer',
    #             'minimum': 1,
    #             'maximum': 50,
    #             'default': 5,
    #             'description': '快速均线周期'
    #         },
    #         'slow_period': {
    #             'type': 'integer',
    #             'minimum': 2,
    #             'maximum': 200,
    #             'default': 20,
    #             'description': '慢速均线周期'
    #         }
    #     },
    #     'required': []
    # },
    # 'bollinger_bands': {
    #     'type': 'object',
    #     'properties': {
    #         'period': {
    #             'type': 'integer',
    #             'minimum': 1,
    #             'maximum': 100,
    #             'default': 20,
    #             'description': '周期'
    #         },
    #         'devfactor': {
    #             'type': 'number',
    #             'minimum': 0.1,
    #             'maximum': 5.0,
    #             'default': 2.0,
    #             'description': '标准差倍数'
    #         }
    #     },
    #     'required': []
    # },
}


# ============================================================================
# 策略描述信息
# ============================================================================

STRATEGY_DESCRIPTIONS: Dict[str, Dict[str, Any]] = {
    'sma_cross': {
        'name': '简单移动平均线交叉策略',
        'description': '当收盘价向上突破MA时买入，向下跌破MA时卖出',
        'params': ['maperiod - MA周期'],
        'category': '趋势跟踪'
    },
    'price_breakout': {
        'name': '价格突破策略',
        'description': '当日最低价跌破开盘价指定阈值时以限价买入，持仓期间检查止损（当日最低价跌破买入价止损阈值）和止盈（当日最高价突破买入价止盈阈值），止损优先级高于止盈',
        'params': ['buy_threshold - 买入阈值(%)', 'sell_threshold - 止盈阈值(%)', 'stop_loss_threshold - 止损阈值(%)', 'commission - 手续费率'],
        'category': '突破策略'
    },
    # 未来扩展示例：
    # 'dual_ma': {
    #     'name': '双均线交叉策略',
    #     'description': '快线上穿慢线买入，快线下穿慢线卖出',
    #     'params': ['fast_period - 快线周期', 'slow_period - 慢线周期'],
    #     'category': '趋势跟踪'
    # },
    # 'bollinger_bands': {
    #     'name': '布林带策略',
    #     'description': '价格触及下轨买入，触及上轨卖出',
    #     'params': ['period - 周期', 'devfactor - 标准差倍数'],
    #     'category': '均值回归'
    # },
}


# ============================================================================
# 策略查询和验证函数
# ============================================================================

def get_strategy_params_schema(strategy_type: str) -> Optional[Dict[str, Any]]:
    """
    获取策略参数模型

    Args:
        strategy_type: 策略类型 (如 'sma_cross')

    Returns:
        策略参数模型 (JSON Schema 格式)，如果策略不存在则返回 None
    """
    return STRATEGY_PARAMS_SCHEMA.get(strategy_type)


def get_strategy_class(strategy_type: str) -> Optional[Type]:
    """
    获取策略类

    Args:
        strategy_type: 策略类型 (如 'sma_cross')

    Returns:
        策略类，如果策略不存在则返回 None
    """
    return STRATEGY_CLASSES.get(strategy_type)


def get_strategy_description(strategy_type: str) -> Optional[Dict[str, Any]]:
    """
    获取策略描述信息

    Args:
        strategy_type: 策略类型

    Returns:
        策略描述字典，如果策略不存在则返回 None
    """
    return STRATEGY_DESCRIPTIONS.get(strategy_type)


def get_supported_strategies() -> Dict[str, Dict[str, Any]]:
    """
    获取所有支持的策略列表

    Returns:
        字典，键为策略类型，值为包含 schema 和 description 的信息
    """
    result = {}
    for strategy_type in STRATEGY_CLASSES.keys():
        result[strategy_type] = {
            'type': strategy_type,
            'schema': get_strategy_params_schema(strategy_type),
            'description': get_strategy_description(strategy_type)
        }
    return result


def validate_strategy_params(strategy_type: str, params: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    验证策略参数

    Args:
        strategy_type: 策略类型
        params: 策略参数字典

    Returns:
        (is_valid, error_message) 元组
        - is_valid: 是否验证通过
        - error_message: 错误信息，验证通过时为 None
    """
    # 1. 检查策略类型是否存在
    schema = get_strategy_params_schema(strategy_type)
    if not schema:
        return False, f"不支持的策略类型: {strategy_type}"

    # 2. 如果没有提供参数，使用默认值
    if not params:
        return True, None

    # 3. 根据 schema 验证参数
    properties = schema.get('properties', {})

    for param_name, param_value in params.items():
        # 检查参数是否在 schema 中定义
        if param_name not in properties:
            return False, f"未知的参数: {param_name}"

        param_schema = properties[param_name]
        param_type = param_schema.get('type')

        # 类型验证
        if param_type == 'integer':
            if not isinstance(param_value, int):
                return False, f"参数 {param_name} 必须是整数"
        elif param_type == 'number':
            if not isinstance(param_value, (int, float)):
                return False, f"参数 {param_name} 必须是数字"

        # 范围验证
        if 'minimum' in param_schema:
            if param_value < param_schema['minimum']:
                return False, f"参数 {param_name} 必须大于等于 {param_schema['minimum']}"

        if 'maximum' in param_schema:
            if param_value > param_schema['maximum']:
                return False, f"参数 {param_name} 必须小于等于 {param_schema['maximum']}"

    return True, None


def get_default_params(strategy_type: str) -> Dict[str, Any]:
    """
    获取策略的默认参数

    Args:
        strategy_type: 策略类型

    Returns:
        默认参数字典
    """
    schema = get_strategy_params_schema(strategy_type)
    if not schema:
        return {}

    properties = schema.get('properties', {})
    default_params = {}

    for param_name, param_schema in properties.items():
        if 'default' in param_schema:
            default_params[param_name] = param_schema['default']

    return default_params
