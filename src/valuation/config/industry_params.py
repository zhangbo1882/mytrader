"""
行业参数配置

针对申万行业配置估值参数，包括：
- 主要估值方法
- 典型估值倍数范围
- ROE/增长率基准
- 行业特性
- 行业权重配置（用于分层加权估值）
"""

from typing import Dict, List, Optional, Any


# 行业参数配置（使用正确的申万一级行业代码）
INDUSTRY_PARAMS = {
    # 银行 801780
    '801780': {
        'name': '银行',
        'primary_method': 'pb',  # 银行主要用PB估值
        'secondary_methods': ['pe'],
        'roe_baseline': 12.0,  # ROE基准 12%
        'growth_baseline': 5.0,  # 增长率基准 5%
        'pb_range': (0.5, 1.5),
        'pe_range': (4, 12),
        'characteristics': ['周期性', '高杠杆', '价值型'],
        'method_weights': {
            'pe': 0.30, 'pb': 0.60, 'ps': 0.00, 'peg': 0.00, 'dcf': 0.10
        },
        'adjustments': {
            'state_owned': 0.8,  # 国有银行折价
            'joint_stock': 1.0,  # 股份制正常
            'city_commercial': 1.2  # 城商行溢价
        }
    },

    # 医药生物 801150
    '801150': {
        'name': '医药生物',
        'primary_method': 'pe',
        'secondary_methods': ['ps'],
        'roe_baseline': 18.0,
        'growth_baseline': 15.0,
        'pe_range': (20, 60),
        'ps_range': (3, 10),
        'characteristics': ['成长性', '防御性', '创新驱动'],
        'method_weights': {
            'pe': 0.50, 'pb': 0.10, 'ps': 0.20, 'peg': 0.00, 'dcf': 0.20
        },
        'adjustments': {
            'innovation_drug': 1.5,  # 创新药溢价
            'generic_drug': 0.8,  # 仿制药折价
            'medical_equipment': 1.2,  # 医疗器械溢价
            'traditional_chinese': 1.0  # 中性
        }
    },

    # 电子 801080
    '801080': {
        'name': '电子',
        'primary_method': 'ps',  # 电子行业常用PS（尤其半导体）
        'secondary_methods': ['pe'],
        'roe_baseline': 12.0,
        'growth_baseline': 20.0,
        'ps_range': (2, 8),
        'pe_range': (20, 50),
        'characteristics': ['高成长', '高波动', '周期性'],
        'method_weights': {
            'pe': 0.30, 'pb': 0.10, 'ps': 0.40, 'peg': 0.00, 'dcf': 0.20
        },
        'adjustments': {
            'semiconductor': 1.3,  # 半导体溢价
            'consumer_electronics': 1.0,  # 消费电子中性
            'components': 0.9,  # 元器件折价
            'led_display': 0.8  # LED折价
        }
    },

    # 房地产 801180
    '801180': {
        'name': '房地产',
        'primary_method': 'nav',  # NAV估值
        'secondary_methods': ['pe'],
        'roe_baseline': 10.0,
        'growth_baseline': 0.0,
        'pe_range': (5, 15),
        'pb_range': (0.5, 1.2),
        'characteristics': ['强周期', '高杠杆', '政策敏感'],
        'method_weights': {
            'pe': 0.20, 'pb': 0.50, 'ps': 0.10, 'peg': 0.00, 'dcf': 0.20
        },
        'adjustments': {
            'tier1_city': 1.3,  # 一线城市溢价
            'commercial': 1.1,  # 商业地产溢价
            'industrial': 0.8  # 工业地产折价
        }
    },

    # 食品饮料 801120
    '801120': {
        'name': '食品饮料',
        'primary_method': 'pe',
        'secondary_methods': ['ps'],
        'roe_baseline': 20.0,
        'growth_baseline': 10.0,
        'pe_range': (20, 50),
        'ps_range': (2, 8),
        'characteristics': ['消费', '品牌', '防御性'],
        'method_weights': {
            'pe': 0.50, 'pb': 0.10, 'ps': 0.10, 'peg': 0.00, 'dcf': 0.30
        },
        'adjustments': {
            'liquor': 1.5,  # 白酒溢价
            'dairy': 1.0,  # 乳制品中性
            'seasoning': 1.2,  # 调味品溢价
            'soft_drink': 0.9  # 软饮料折价
        }
    },

    # 汽车 801880
    '801880': {
        'name': '汽车',
        'primary_method': 'pe',
        'secondary_methods': ['pb'],
        'roe_baseline': 8.0,
        'growth_baseline': 10.0,
        'pe_range': (8, 25),
        'pb_range': (0.8, 2.5),
        'characteristics': ['周期性', '竞争激烈'],
        'method_weights': {
            'pe': 0.40, 'pb': 0.30, 'ps': 0.10, 'peg': 0.00, 'dcf': 0.20
        },
        'adjustments': {
            'nev': 1.5,  # 新能源汽车溢价
            'traditional': 0.8,  # 传统汽车折价
            'parts': 0.9  # 汽车零部件折价
        }
    },

    # 基础化工 801030
    '801030': {
        'name': '基础化工',
        'primary_method': 'pe',
        'secondary_methods': ['pb'],
        'roe_baseline': 10.0,
        'growth_baseline': 8.0,
        'pe_range': (10, 30),
        'pb_range': (1.0, 3.0),
        'characteristics': ['周期性', '原材料敏感'],
        'method_weights': {
            'pe': 0.20, 'pb': 0.50, 'ps': 0.10, 'peg': 0.00, 'dcf': 0.20
        },
        'adjustments': {
            'new_materials': 1.3,  # 新材料溢价
            'chemical_fiber': 0.9,  # 化纤折价
            'pesticide': 1.0  # 农药中性
        }
    },

    # 计算机 801750
    '801750': {
        'name': '计算机',
        'primary_method': 'ps',
        'secondary_methods': ['pe'],
        'roe_baseline': 12.0,
        'growth_baseline': 20.0,
        'ps_range': (3, 12),
        'pe_range': (25, 60),
        'characteristics': ['高成长', '人才驱动'],
        'method_weights': {
            'pe': 0.30, 'pb': 0.10, 'ps': 0.40, 'peg': 0.00, 'dcf': 0.20
        },
        'adjustments': {
            'software': 1.3,  # 软件溢价
            'hardware': 0.9,  # 硬件折价
            'cloud_services': 1.5  # 云服务溢价
        }
    },

    # 传媒 801760
    '801760': {
        'name': '传媒',
        'primary_method': 'pe',
        'secondary_methods': ['ps'],
        'roe_baseline': 10.0,
        'growth_baseline': 15.0,
        'pe_range': (15, 40),
        'ps_range': (2, 6),
        'characteristics': ['内容', 'IP', '政策敏感'],
        'method_weights': {
            'pe': 0.40, 'pb': 0.10, 'ps': 0.30, 'peg': 0.00, 'dcf': 0.20
        },
        'adjustments': {
            'gaming': 1.2,  # 游戏溢价
            'advertising': 0.9,  # 广告折价
            'film_tv': 1.0  # 影视中性
        }
    },

    # 电力设备 801730
    '801730': {
        'name': '电力设备',
        'primary_method': 'pe',
        'secondary_methods': ['pb'],
        'roe_baseline': 12.0,
        'growth_baseline': 15.0,
        'pe_range': (15, 40),
        'pb_range': (1.5, 4.0),
        'characteristics': ['政策驱动', '周期性'],
        'method_weights': {
            'pe': 0.40, 'pb': 0.20, 'ps': 0.10, 'peg': 0.00, 'dcf': 0.30
        },
        'adjustments': {
            'renewable_energy': 1.4,  # 新能源发电溢价
            'grid_equipment': 1.0,  # 电网设备中性
            'traditional_power': 0.7  # 传统火电折价
        }
    },

    # 钢铁 801040
    '801040': {
        'name': '钢铁',
        'primary_method': 'pb',
        'secondary_methods': ['pe'],
        'roe_baseline': 8.0,
        'growth_baseline': 3.0,
        'pb_range': (0.5, 1.5),
        'pe_range': (5, 15),
        'characteristics': ['强周期', '资产重'],
        'method_weights': {
            'pe': 0.20, 'pb': 0.50, 'ps': 0.10, 'peg': 0.00, 'dcf': 0.20
        },
    },

    # 非银金融 801190
    '801190': {
        'name': '非银金融',
        'primary_method': 'pb',
        'secondary_methods': ['pe'],
        'roe_baseline': 12.0,
        'growth_baseline': 10.0,
        'pb_range': (1.0, 3.0),
        'pe_range': (10, 25),
        'characteristics': ['金融', '监管敏感'],
        'method_weights': {
            'pe': 0.35, 'pb': 0.45, 'ps': 0.00, 'peg': 0.00, 'dcf': 0.20
        },
    },

    # 通信 801770
    '801770': {
        'name': '通信',
        'primary_method': 'pe',
        'secondary_methods': ['ps'],
        'roe_baseline': 10.0,
        'growth_baseline': 10.0,
        'pe_range': (15, 35),
        'ps_range': (2, 6),
        'characteristics': ['基础设施', '稳定性'],
        'method_weights': {
            'pe': 0.40, 'pb': 0.10, 'ps': 0.30, 'peg': 0.00, 'dcf': 0.20
        },
    },
}

# 默认行业权重（未匹配到具体行业时使用）
DEFAULT_METHOD_WEIGHTS = {
    'pe': 0.40, 'pb': 0.20, 'ps': 0.20, 'peg': 0.00, 'dcf': 0.20
}

# A股市场特色调整参数
MARKET_ADJUSTMENTS = {
    # 行业轮动调整
    'cycle_stage': {
        'recovery': 1.3,  # 复苏期溢价
        'expansion': 1.1,  # 扩张期小溢价
        'slowdown': 0.9,  # 减速期折价
        'recession': 0.6  # 衰退期大折价
    },

    # 大小盘溢价（修正后）
    'market_cap': {
        'small_cap': 50,  # 小于50亿
        'mid_cap': 500,  # 50-500亿
        'large_cap': 1000  # 大于500亿
    },
    'market_cap_premium': {
        'small': 1.1,  # 小盘股溢价（修正：原1.2过高）
        'mid': 1.0,  # 中盘股中性
        'large': 1.0  # 大盘股中性（修正：原0.9，核心资产时代不一定折价）
    },

    # 成长性溢价
    'growth_premium': {
        'high_growth': 1.5,  # >30%增长率
        'growth': 1.3,  # >20%增长率
        'stable': 1.0,  # >10%增长率
        'low_growth': 0.8,  # >0%增长率
        'negative': 0.5  # 负增长
    },

    # 流动性溢价
    'liquidity_premium': {
        'high': 1.1,  # 日均成交额>10亿
        'medium': 1.0,  # 日均成交额1-10亿
        'low': 0.9  # 日均成交额<1亿
    }
}


def get_industry_params(industry_code: str) -> Optional[Dict[str, Any]]:
    """
    获取行业参数

    Args:
        industry_code: 申万行业代码

    Returns:
        行业参数字典，如果不存在返回None
    """
    return INDUSTRY_PARAMS.get(industry_code)


def get_industry_params_by_name(industry_name: str) -> Optional[Dict[str, Any]]:
    """
    根据行业名称获取参数（模糊匹配）

    Args:
        industry_name: 申万行业名称

    Returns:
        行业参数字典，如果不存在返回None
    """
    if not industry_name:
        return None
    for code, params in INDUSTRY_PARAMS.items():
        if params['name'] == industry_name or params['name'] in industry_name:
            return params
    return None


def get_industry_method_weights(industry_code: str = None, industry_name: str = None) -> Dict[str, float]:
    """
    获取行业估值方法权重

    Args:
        industry_code: 申万行业代码
        industry_name: 行业名称（如果代码未匹配，用名称查找）

    Returns:
        各方法的权重字典 {method: weight}
    """
    params = None
    if industry_code:
        params = get_industry_params(industry_code)
    if params is None and industry_name:
        params = get_industry_params_by_name(industry_name)

    if params and 'method_weights' in params:
        return params['method_weights']
    return DEFAULT_METHOD_WEIGHTS.copy()


def get_market_cap_premium(market_cap: float) -> float:
    """
    获取市值溢价系数

    Args:
        market_cap: 总市值（亿元）

    Returns:
        溢价系数
    """
    caps = MARKET_ADJUSTMENTS['market_cap']
    premiums = MARKET_ADJUSTMENTS['market_cap_premium']

    if market_cap < caps['small_cap']:
        return premiums['small']
    elif market_cap < caps['mid_cap']:
        return premiums['mid']
    else:
        return premiums['large']


def get_growth_premium(growth_rate: float) -> float:
    """
    获取成长性溢价系数

    Args:
        growth_rate: 增长率（%）

    Returns:
        溢价系数
    """
    premiums = MARKET_ADJUSTMENTS['growth_premium']

    if growth_rate > 30:
        return premiums['high_growth']
    elif growth_rate > 20:
        return premiums['growth']
    elif growth_rate > 10:
        return premiums['stable']
    elif growth_rate > 0:
        return premiums['low_growth']
    else:
        return premiums['negative']


def get_liquidity_premium(avg_daily_turnover: float) -> float:
    """
    获取流动性溢价系数

    Args:
        avg_daily_turnover: 日均成交额（亿元）

    Returns:
        溢价系数
    """
    premiums = MARKET_ADJUSTMENTS['liquidity_premium']

    if avg_daily_turnover > 10:
        return premiums['high']
    elif avg_daily_turnover > 1:
        return premiums['medium']
    else:
        return premiums['low']


def calculate_industry_adjustment(
    industry_code: str,
    market_cap: float,
    growth_rate: float,
    sub_industry: Optional[str] = None
) -> float:
    """
    计算综合行业调整系数

    Args:
        industry_code: 申万行业代码
        market_cap: 总市值（亿元）
        growth_rate: 增长率（%）
        sub_industry: 细分行业（可选）

    Returns:
        综合调整系数
    """
    # 获取行业基础参数
    params = get_industry_params(industry_code)
    if not params:
        return 1.0

    adjustment = 1.0

    # 行业细分调整
    if sub_industry and 'adjustments' in params:
        sub_adjustment = params['adjustments'].get(sub_industry, 1.0)
        adjustment *= sub_adjustment

    # 市值调整
    cap_premium = get_market_cap_premium(market_cap)
    adjustment *= cap_premium

    # 成长性调整
    growth_premium = get_growth_premium(growth_rate)
    adjustment *= growth_premium

    return adjustment


def get_primary_valuation_method(industry_code: str) -> str:
    """
    获取行业主要估值方法

    Args:
        industry_code: 申万行业代码

    Returns:
        估值方法名称
    """
    params = get_industry_params(industry_code)
    if params:
        return params['primary_method']
    return 'pe'  # 默认PE估值
