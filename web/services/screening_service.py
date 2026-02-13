"""股票筛选业务逻辑服务"""
import logging
from flask import request
from src.screening.strategies.predefined_strategies import PredefinedStrategies
from src.screening.rule_engine import RuleEngine
from src.screening.screening_engine import ScreeningEngine
from config.settings import TUSHARE_DB_PATH
import pandas as pd

logger = logging.getLogger(__name__)

# 全局数据库连接
_db = None
_query = None


def get_db():
    """获取数据库连接"""
    global _db, _query
    if _db is None:
        try:
            from src.data_sources.tushare import TushareDB
            from config.settings import TUSHARE_TOKEN
            _db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))
            _query = _db.query()
        except Exception as e:
            print(f"Warning: Failed to initialize database: {e}")
            _db = False
            _query = False
    return _db, _query


def list_strategies():
    """
    列出所有可用的预设策略

    返回数据:
    {
        "success": true,
        "strategies": [
            {
                "name": "liquidity",
                "description": "流动性策略",
                "criteria_config": {  # 新增：筛选条件配置
                    "type": "AND",
                    "criteria": [...]
                }
            }
        ]
    }
    """
    strategies = PredefinedStrategies.list_strategies()
    strategy_list = []

    for key, description in strategies.items():
        try:
            # 获取策略的筛选条件配置
            strategy_method = getattr(PredefinedStrategies, f'{key}_strategy')
            criteria = strategy_method()
            criteria_config = criteria.to_config() if hasattr(criteria, 'to_config') else None
        except Exception as e:
            logger.warning(f"[list_strategies] Failed to get config for strategy {key}: {e}")
            criteria_config = None

        strategy_list.append({
            'name': key,
            'description': description,
            'criteria_config': criteria_config
        })

    return {
        'success': True,
        'strategies': strategy_list
    }


def apply_preset_strategy(strategy_name):
    """
    应用预设策略进行筛选

    路径参数:
        strategy_name: 策略名称

    查询参数:
        limit: 返回结果数量限制 (默认100)

    返回数据:
    {
        "success": true,
        "strategy": "value",
        "count": 50,
        "stocks": [...]
    }
    """
    logger.info(f"[PresetStrategy] Starting strategy: {strategy_name}")

    db, query = get_db()
    if not db or not query:
        logger.error(f"[PresetStrategy] Database connection failed")
        return {'error': '数据库连接失败'}, 500

    # 获取策略
    strategies_map = PredefinedStrategies.list_strategies()

    if strategy_name not in strategies_map:
        logger.error(f"[PresetStrategy] Unknown strategy: {strategy_name}")
        return {
            'error': f'未知策略: {strategy_name}',
            'available_strategies': list(strategies_map.keys())
        }, 400

    try:
        # 获取策略条件
        strategy_method = getattr(PredefinedStrategies, f'{strategy_name}_strategy')
        criteria = strategy_method()
        logger.info(f"[PresetStrategy] Criteria config: {criteria.to_config() if hasattr(criteria, 'to_config') else type(criteria).__name__}")

        # 使用筛选引擎执行筛选
        logger.info(f"[PresetStrategy] Starting screening engine...")
        engine = ScreeningEngine(str(TUSHARE_DB_PATH))
        df = engine.screen(criteria)
        logger.info(f"[PresetStrategy] Screening completed, initial result count: {len(df)}")

        # 获取结果限制
        limit = int(request.args.get('limit', 100))
        df = df.head(limit)

        # 转换结果
        results = []
        for _, row in df.iterrows():
            results.append({
                'code': row.get('symbol', ''),
                'name': row.get('stock_name', ''),
                'latest_close': round(float(row.get('close', 0)), 2) if pd.notna(row.get('close')) else None,
                'pe_ttm': round(float(row.get('pe_ttm', 0)), 2) if pd.notna(row.get('pe_ttm')) else None,
                'pb': round(float(row.get('pb', 0)), 2) if pd.notna(row.get('pb')) else None,
                'total_mv_yi': round(float(row.get('total_mv', 0)) / 10000, 2) if pd.notna(row.get('total_mv')) else None,
            })

        logger.info(f"[PresetStrategy] Returning {len(results)} results, strategy: {strategy_name}")

        return {
            'success': True,
            'strategy': strategy_name,
            'strategy_description': strategies_map[strategy_name],
            'count': len(results),
            'stocks': results
        }

    except Exception as e:
        import traceback
        logger.error(f"[PresetStrategy] Screening failed: {str(e)}")
        logger.error(traceback.format_exc())
        return {'error': f'筛选失败: {str(e)}'}, 500


def apply_custom_strategy():
    """
    应用自定义筛选策略

    请求体:
    {
        "config": {
            "type": "AND",
            "criteria": [
                {"type": "Range", "column": "pe_ttm", "min_val": 0, "max_val": 30},
                {"type": "GreaterThan", "column": "latest_roe", "threshold": 10}
            ]
        },
        "limit": 100
    }

    返回数据:
    {
        "success": true,
        "count": 50,
        "stocks": [...]
    }
    """
    logger.info(f"[CustomStrategy] Starting custom screening")

    db, query = get_db()
    if not db or not query:
        logger.error(f"[CustomStrategy] Database connection failed")
        return {'error': '数据库连接失败'}, 500

    try:
        data = request.json
        if not data:
            logger.error(f"[CustomStrategy] Request data is empty")
            return {'error': '请求数据为空'}, 400

        config = data.get('config')
        if not config:
            logger.error(f"[CustomStrategy] Missing config parameter")
            return {'error': '缺少config参数'}, 400

        limit = data.get('limit', 100)
        logger.info(f"[CustomStrategy] Screening config: {config}, limit: {limit}")

        # 转换配置中的字段名和值
        # total_mv_yi/circulation_mv_yi (亿) -> total_mv/circ_mv (元)
        config = _convert_config_units(config)

        # 使用规则引擎构建筛选条件（传递 db_path）
        logger.info(f"[CustomStrategy] Building criteria from config...")
        criteria = RuleEngine.build_from_config(config, db_path=str(TUSHARE_DB_PATH))
        logger.info(f"[CustomStrategy] Criteria type: {type(criteria).__name__}")

        # 使用筛选引擎执行筛选
        logger.info(f"[CustomStrategy] Starting screening engine...")
        engine = ScreeningEngine(str(TUSHARE_DB_PATH))
        df = engine.screen(criteria)
        logger.info(f"[CustomStrategy] Screening completed, initial result count: {len(df)}")

        # 获取实际符合条件的股票总数
        total_count = len(df)

        # 限制结果数量
        df = df.head(limit)

        # 转换结果
        results = []
        for _, row in df.iterrows():
            results.append({
                'code': row.get('symbol', ''),
                'name': row.get('stock_name', ''),
                'latest_close': round(float(row.get('close', 0)), 2) if pd.notna(row.get('close')) else None,
                'pe_ttm': round(float(row.get('pe_ttm', 0)), 2) if pd.notna(row.get('pe_ttm')) else None,
                'pb': round(float(row.get('pb', 0)), 2) if pd.notna(row.get('pb')) else None,
                'total_mv_yi': round(float(row.get('total_mv', 0)) / 10000, 2) if pd.notna(row.get('total_mv')) else None,
            })

        logger.info(f"[CustomStrategy] Returning {len(results)} results, total matched: {total_count}")

        return {
            'success': True,
            'count': total_count,  # 返回实际符合条件的股票总数
            'stocks': results
        }

    except ValueError as e:
        logger.error(f"[CustomStrategy] Config error: {str(e)}")
        return {'error': f'配置错误: {str(e)}'}, 400
    except Exception as e:
        import traceback
        logger.error(f"[CustomStrategy] Screening failed: {str(e)}")
        logger.error(traceback.format_exc())
        return {'error': f'筛选失败: {str(e)}'}, 500


def list_criteria_types():
    """
    列出支持的筛选条件类型

    返回数据:
    {
        "success": true,
        "types": ["Range", "GreaterThan", "LessThan", ...],
        "combinations": ["AND", "OR", "NOT"]
    }
    """
    return {
        'success': True,
        'types': RuleEngine.list_supported_types(),
        'criteria_details': {
            'Range': '范围条件 {type: "Range", column: "pe_ttm", min_val: 0, max_val: 30}',
            'GreaterThan': '大于条件 {type: "GreaterThan", column: "latest_roe", threshold: 10}',
            'LessThan': '小于条件 {type: "LessThan", column: "debt_to_assets", threshold: 60}',
            'Percentile': '百分位条件 {type: "Percentile", column: "pe_ttm", percentile: 0.25}',
            'TopN': '前N个 {type: "TopN", column: "latest_roe", n: 50}',
            'IndustryFilter': '行业过滤 {type: "IndustryFilter", industries: ["银行", "非银金融"], mode: "blacklist"}',
            'IndustryRelative': '行业相对 {type: "IndustryRelative", column: "latest_roe", percentile: 0.3}',
            'AverageAmplitude': '平均振幅 {type: "AverageAmplitude", period: 20, threshold: 4.0}',
            'PositiveDays': '涨幅天数 {type: "PositiveDays", period: 20, threshold: 2.0, min_positive_ratio: 0.5}',
            'MarketFilter': '市场过滤 {type: "MarketFilter", markets: ["主板", "创业板"], mode: "whitelist"}'
        },
        'market_types': {
            '主板': '沪深主板股票',
            '创业板': '创业板股票',
            '科创板': '科创板股票',
            '北交所': '北交所股票'
        }
    }


def list_industries(level=None):
    """
    获取申万行业分类列表

    查询参数:
        level: 行业级别 (1=一级, 2=二级, 3=三级，默认返回一级)

    返回数据:
    {
        "success": true,
        "level": 1,
        "industries": [
            {"code": "801080.SI", "name": "电子", "parent_code": "0"},
            {"code": "801010.SI", "name": "农林牧渔", "parent_code": "0"}
        ]
    }
    """
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(
            f'sqlite:///{TUSHARE_DB_PATH}',
            connect_args={
                'check_same_thread': False,
                'timeout': 30  # 30秒超时
            }
        )

        with engine.connect() as conn:
            if level is None:
                level = 1

            level_map = {1: 'L1', 2: 'L2', 3: 'L3'}
            level_str = level_map.get(int(level), 'L1')

            query = text('''
                SELECT index_code, industry_name, parent_code
                FROM sw_classify
                WHERE level = :level
                ORDER BY index_code
            ''')

            results = conn.execute(query, {'level': level_str}).fetchall()

            industries = [
                {
                    'code': r[0],
                    'name': r[1],
                    'parent_code': r[2]
                }
                for r in results
            ]

            return {
                'success': True,
                'level': int(level),
                'industries': industries
            }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'获取行业列表失败: {str(e)}'}, 500


def _convert_config_units(config: dict) -> dict:
    """
    转换配置中的字段名和单位

    将前端使用的字段名（单位：亿）转换为数据库字段名（单位：元）
    - total_mv_yi -> total_mv (乘以 10000)
    - circulation_mv_yi -> circ_mv (乘以 10000)

    递归处理嵌套条件（AND/OR/NOT）
    """
    # 递归处理嵌套条件
    if 'criteria' in config:
        for i, criteria in enumerate(config['criteria']):
            # 递归处理子条件
            config['criteria'][i] = _convert_config_units(criteria)

    # 转换当前层的字段名和单位（如果是条件节点）
    if 'column' in config:
        column = config['column']
        if column == 'total_mv_yi':
            config['column'] = 'total_mv'
            # 转换阈值（亿 -> 元，乘以 10000）
            if 'min_val' in config and config['min_val'] is not None:
                config['min_val'] = config['min_val'] * 10000
            if 'max_val' in config and config['max_val'] is not None:
                config['max_val'] = config['max_val'] * 10000
            if 'threshold' in config and config['threshold'] is not None:
                config['threshold'] = config['threshold'] * 10000
        elif column == 'circulation_mv_yi':
            config['column'] = 'circ_mv'
            # 转换阈值（亿 -> 元，乘以 10000）
            if 'min_val' in config and config['min_val'] is not None:
                config['min_val'] = config['min_val'] * 10000
            if 'max_val' in config and config['max_val'] is not None:
                config['max_val'] = config['max_val'] * 10000
            if 'threshold' in config and config['threshold'] is not None:
                config['threshold'] = config['threshold'] * 10000

    return config
