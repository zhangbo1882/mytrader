"""
AI Stock Screening Service
Provides REST API endpoints for AI-powered stock screening
"""
from flask import request
import re
import logging

logger = logging.getLogger(__name__)


def ai_screen():
    """
    AI 股票筛选（REST API 端点）
    复用 WebSocket 的 MockClaudeProcessor 逻辑

    Request body:
        {
            "query": "查找最近5天涨幅超过5%的股票"
        }

    Returns:
        {
            "success": true,
            "explanation": "根据你的条件...",
            "stocks": [...],
            "params": {...}
        }
    """
    data = request.get_json()
    query = data.get('query', '').strip()

    if not query:
        return {'error': '查询不能为空'}, 400

    try:
        logger.info(f"[AI Screen REST] Processing query: {query}")

        # 解析查询参数
        params = _parse_query(query)

        # 检查是否有有效参数
        has_params = any(v is not None and v != 5 for k, v in params.items() if k != 'days')

        if not has_params:
            return {'error': '未能提取到明确的筛选参数，请尝试更具体的描述'}, 400

        # 执行筛选
        from web.services.stock_service import stock_screen
        screen_data, status_code = stock_screen_with_params(params)

        if status_code == 200 and screen_data.get('success'):
            # 构建解释
            explanation = _build_explanation(query, params)

            return {
                'success': True,
                'explanation': explanation,
                'stocks': screen_data.get('symbols', []),
                'params': params,
                'count': screen_data.get('count', 0)
            }
        else:
            return {'error': screen_data.get('error', '筛选失败')}, 500

    except Exception as e:
        logger.error(f"[AI Screen REST] Processing failed: {e}")
        return {'error': f'处理失败: {str(e)}'}, 500


def ai_screen_chat():
    """
    AI 对话式股票筛选（REST API 端点）

    Request body:
        {
            "query": "帮我找一些科技股",
            "history": [...]
        }

    Returns:
        {
            "success": true,
            "response": "好的，我来帮您...",
            "params": {...}
        }
    """
    data = request.get_json()
    query = data.get('query', '').strip()
    history = data.get('history', [])

    if not query:
        return {'error': '查询不能为空'}, 400

    try:
        logger.info(f"[AI Screen Chat REST] Processing query: {query}")

        # 解析查询参数
        params = _parse_query(query)

        # 构建自然语言响应
        response = _build_response(query, params, history)

        return {
            'success': True,
            'response': response,
            'params': params if any(v is not None and v != 5 for k, v in params.items() if k != 'days') else None,
            'should_screen': any(v is not None and v != 5 for k, v in params.items() if k != 'days')
        }

    except Exception as e:
        logger.error(f"[AI Screen Chat REST] Processing failed: {e}")
        return {'error': f'处理失败: {str(e)}'}, 500


def _parse_query(query):
    """
    解析自然语言查询以提取筛选参数

    Args:
        query: 自然语言查询

    Returns:
        dict: 提取的参数
    """
    params = {}
    params['days'] = 5  # 默认值

    # 价格范围
    price_patterns = [
        (r'(?:价格|股价|低于|小于|<)\s*(\d+(?:\.\d+)?)\s*[元块钱]', 'price_max'),
        (r'(?:价格|股价|高于|大于|>)\s*(\d+(?:\.\d+)?)\s*[元块钱]', 'price_min'),
        (r'(\d+(?:\.\d+)?)\s*[元块钱]以下', 'price_max'),
        (r'(\d+(?:\.\d+)?)\s*[元块钱]以上', 'price_min'),
    ]

    # 换手率
    turnover_patterns = [
        (r'换手率\s*(?:大于|>|超过|高于)\s*(\d+(?:\.\d+)?)\s*%?', 'turnover_min'),
        (r'换手率\s*(?:小于|<|低于)\s*(\d+(?:\.\d+)?)\s*%?', 'turnover_max'),
        (r'(?:高换手|换手率高)', 'turnover_min'),
    ]

    # 涨跌幅
    pct_chg_patterns = [
        (r'(?:涨幅|涨跌幅)\s*(?:大于|>|超过)\s*(\d+(?:\.\d+)?)\s*%?', 'pct_chg_min'),
        (r'(?:涨幅|涨跌幅)\s*(?:小于|<)\s*(\d+(?:\.\d+)?)\s*%?', 'pct_chg_max'),
        (r'上涨\s*(\d+(?:\.\d+)?)\s*%?', 'pct_chg_min'),
        (r'连续上涨', 'pct_chg_min'),
    ]

    # 成交量
    volume_patterns = [
        (r'成交量\s*(?:放大|大于|>|超过)', 'volume_min'),
        (r'放量', 'volume_min'),
    ]

    # 天数
    days_patterns = [
        (r'最近\s*(\d+)\s*天', 'days'),
        (r'(\d+)\s*天\s*内', 'days'),
    ]

    # 应用所有模式
    all_patterns = [
        (price_patterns, float),
        (turnover_patterns, lambda x: float(x)),
        (pct_chg_patterns, float),
        (volume_patterns, lambda x: None),
        (days_patterns, int),
    ]

    for patterns, parser in all_patterns:
        for pattern, param_key in patterns:
            match = re.search(pattern, query)
            if match:
                value = match.group(1) if match.groups() else None
                if value is not None:
                    try:
                        params[param_key] = parser(value)
                    except (ValueError, IndexError):
                        params[param_key] = _get_default_value(param_key)
                else:
                    params[param_key] = _get_default_value(param_key)

    return params


def _get_default_value(param_key):
    """获取参数的默认值"""
    defaults = {
        'price_min': None,
        'price_max': None,
        'turnover_min': None,
        'turnover_max': None,
        'pct_chg_min': None,
        'pct_chg_max': None,
        'volume_min': None,
        'volume_max': None,
        'days': 5,
    }
    return defaults.get(param_key)


def _build_response(query, params, history):
    """构建自然语言响应"""
    response_parts = ["好的，我来帮您筛选股票。"]

    if params.get('price_min'):
        response_parts.append(f"价格高于{params['price_min']}元")
    if params.get('price_max'):
        response_parts.append(f"价格低于{params['price_max']}元")
    if params.get('turnover_min'):
        response_parts.append(f"换手率大于{params['turnover_min']}%")
    if params.get('pct_chg_min'):
        response_parts.append(f"涨幅超过{params['pct_chg_min']}%")

    if len(response_parts) > 1:
        return "，".join(response_parts) + "的股票。"
    else:
        return "好的，我来帮您分析。请稍等..."


def _build_explanation(query, params):
    """构建解释说明"""
    parts = [f"根据你的条件\"{query}\"，我为你找到了符合条件的股票："]

    if params.get('days'):
        parts.append(f"- 筛选周期：最近{params['days']}天")
    if params.get('turnover_min'):
        parts.append(f"- 换手率：大于{params['turnover_min']}%")
    if params.get('pct_chg_min'):
        parts.append(f"- 涨幅：大于{params['pct_chg_min']}%")
    if params.get('price_min') or params.get('price_max'):
        parts.append(f"- 价格：{params.get('price_min', '0')}-{params.get('price_max', '∞')}元")

    return "\n".join(parts)


def stock_screen_with_params(params):
    """
    使用给定参数执行股票筛选

    Args:
        params: 筛选参数字典

    Returns:
        tuple: (data, status_code)
    """
    from web.services.stock_service import stock_screen

    # 模拟 request.get_json() 返回的数据
    # 因为 stock_screen() 从 request 读取数据
    # 我们需要临时修改 Flask 的 request 对象

    # 保存原始请求数据
    from flask import has_request_context
    if not has_request_context():
        # 如果没有请求上下文，直接调用
        return stock_screen()

    # 在请求上下文中，我们需要模拟请求体
    # 这里使用一个临时方案：直接构造筛选逻辑
    try:
        db, query = get_db_for_screening()

        if not db or not query:
            return {'error': '数据库连接失败'}, 500

        # 构建筛选条件
        days = params.get('days', 5)

        # 调用数据库查询
        symbols_data = query.screen_stocks(
            days=days,
            turnover_min=params.get('turnover_min'),
            turnover_max=params.get('turnover_max'),
            pct_chg_min=params.get('pct_chg_min'),
            pct_chg_max=params.get('pct_chg_max'),
            price_min=params.get('price_min'),
            price_max=params.get('price_max'),
            volume_min=params.get('volume_min'),
            volume_max=params.get('volume_max'),
            limit=500
        )

        return {
            'success': True,
            'count': len(symbols_data),
            'symbols': symbols_data
        }, 200

    except Exception as e:
        logger.error(f"Screening failed: {e}")
        return {'error': str(e)}, 500


def get_db_for_screening():
    """获取数据库连接用于筛选"""
    from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH
    from src.data_sources.tushare import TushareDB

    try:
        db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))
        query = db.query()
        return db, query
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None, None
