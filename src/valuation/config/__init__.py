"""
估值配置模块

包含行业参数配置和估值配置管理
"""

from .industry_params import (
    INDUSTRY_PARAMS,
    MARKET_ADJUSTMENTS,
    get_industry_params,
    get_industry_params_by_name,
    get_market_cap_premium,
    get_growth_premium,
    get_liquidity_premium,
    calculate_industry_adjustment,
    get_primary_valuation_method
)

__all__ = [
    'INDUSTRY_PARAMS',
    'MARKET_ADJUSTMENTS',
    'get_industry_params',
    'get_industry_params_by_name',
    'get_market_cap_premium',
    'get_growth_premium',
    'get_liquidity_premium',
    'calculate_industry_adjustment',
    'get_primary_valuation_method'
]
