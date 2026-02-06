"""
Industry Statistics Service

提供行业统计数据查询API
"""
from flask import request
from config.settings import TUSHARE_DB_PATH
from src.screening.calculators.industry_statistics_calculator import IndustryStatisticsCalculator
from sqlalchemy import create_engine
import pandas as pd


def get_industry_name_from_code(industry_code: str) -> str:
    """
    根据行业代码获取行业名称
    
    Args:
        industry_code: 行业代码，如 "801010" 或 "801010.SI"
    
    Returns:
        行业名称，如 "银行"，如果未找到则返回None
    """
    # 标准化行业代码（移除.SI后缀）
    code = industry_code.replace('.SI', '')
    
    # 尝试匹配index_code（带.SI）和industry_code（不带）
    possible_patterns = [f'{code}.SI', code]
    
    engine = create_engine(f'sqlite:///{TUSHARE_DB_PATH}', echo=False)
    
    for pattern in possible_patterns:
        query = """
        SELECT industry_name FROM sw_classify 
        WHERE index_code = :code OR industry_code = :code
        LIMIT 1
        """
        try:
            df = pd.read_sql_query(query, engine, params={'code': pattern})
            if not df.empty:
                return df.iloc[0]['industry_name']
        except Exception:
            pass
    
    return None


def get_industry_percentile():
    """
    获取行业指标分位值（使用行业代码）

    Query params:
        industry_code: 申万行业代码 (必需) - 如 "801010" 或 "801010.SI"
        metric: 指标名称 (必需) - 如 "pe_ttm", "pb", "total_mv", "circ_mv"
        percentile: 百分位 (可选，默认0.75) - 支持0.1-0.9

    Returns:
        {
            "success": true,
            "data": {
                "industry_code": "801010",
                "industry_name": "银行",
                "metric": "pe_ttm",
                "percentile": 0.75,
                "percentile_str": "p75",
                "value": 6.5,
                "calculated_at": "2026-02-06 19:25:00"
            }
        }
    """
    industry_code = request.args.get('industry_code')
    metric = request.args.get('metric')
    percentile = request.args.get('percentile', 0.75, type=float)

    if not industry_code or not metric:
        return {'error': '参数缺失: industry_code 和 metric 都是必需参数'}, 400

    try:
        calculator = IndustryStatisticsCalculator(str(TUSHARE_DB_PATH))

        # 获取行业名称
        industry_name = get_industry_name_from_code(industry_code)
        
        if not industry_name:
            return {
                'error': f'未找到行业代码 {industry_code} 对应的行业名称'
            }, 404

        # 查询行业统计
        percentile_column = f"p{int(percentile * 100)}"

        # 使用行业名称查询（优先匹配最细粒度的行业）
        query = f"""
        SELECT
            sw_l3,
            sw_l2,
            sw_l1,
            {percentile_column} as value,
            calculated_at
        FROM industry_statistics
        WHERE metric_name = :metric
          AND (sw_l3 = :industry_name OR sw_l2 = :industry_name OR sw_l1 = :industry_name)
        ORDER BY 
            CASE 
                WHEN sw_l3 = :industry_name THEN 1
                WHEN sw_l2 = :industry_name THEN 2
                WHEN sw_l1 = :industry_name THEN 3
            END
        LIMIT 1
        """

        df = pd.read_sql_query(
            query,
            calculator.engine,
            params={'industry_name': industry_name, 'metric': metric}
        )

        if df.empty:
            return {
                'error': f'未找到行业 {industry_name} ({industry_code}) 的 {metric} 指标分位数据'
            }, 404

        value = float(df.iloc[0]['value'])
        calculated_at = df.iloc[0]['calculated_at']

        return {
            'success': True,
            'data': {
                'industry_code': industry_code,
                'industry_name': industry_name,
                'metric': metric,
                'percentile': percentile,
                'percentile_str': f'p{int(percentile * 100)}',
                'value': value,
                'calculated_at': calculated_at
            }
        }, 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'查询失败: {str(e)}'}, 500


def get_available_metrics():
    """
    获取可查询的指标列表

    Returns:
        {
            "success": true,
            "data": {
                "metrics": ["pe_ttm", "pb", "ps_ttm", "total_mv", "circ_mv"],
                "descriptions": {...}
            }
        }
    """
    try:
        calculator = IndustryStatisticsCalculator(str(TUSHARE_DB_PATH))

        # 查询数据库中已计算的指标
        query = """
        SELECT DISTINCT metric_name
        FROM industry_statistics
        ORDER BY metric_name
        """
        df = pd.read_sql_query(query, calculator.engine)

        metrics_list = df['metric_name'].tolist() if not df.empty else []

        # 指标描述映射
        descriptions = {
            'pe_ttm': '市盈率TTM (滚动12个月)',
            'pb': '市净率',
            'ps_ttm': '市销率TTM',
            'total_mv': '总市值 (万元)',
            'circ_mv': '流通市值 (万元)',
            'turnover': '换手率 (%)',
            'amount': '成交额 (万元)',
            'close': '收盘价'
        }

        return {
            'success': True,
            'data': {
                'metrics': metrics_list,
                'descriptions': {k: descriptions.get(k, '') for k in metrics_list},
                'available_percentiles': [10, 25, 50, 75, 90],
                'total_metrics': len(metrics_list)
            }
        }, 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'查询失败: {str(e)}'}, 500


def get_industries_with_stats():
    """
    获取有统计数据的行业列表

    Query params:
        level: 行业级别 (1/2/3)，默认1
        metric: 指标名称 (可选)，过滤有该指标数据的行业

    Returns:
        {
            "success": true,
            "data": {
                "industries": [
                    {
                        "industry_code": "801010",
                        "industry_name": "银行",
                        "level": 1,
                        "metrics": ["pe_ttm", "pb", "total_mv"]
                    }
                ],
                "total": 31
            }
        }
    """
    level = request.args.get('level', 1, type=int)
    metric = request.args.get('metric')

    try:
        calculator = IndustryStatisticsCalculator(str(TUSHARE_DB_PATH))

        # 根据级别确定列名
        if level == 1:
            col = 'sw_l1'
        elif level == 2:
            col = 'sw_l2'
        else:
            col = 'sw_l3'

        # 构建查询
        where_clause = f"{col} IS NOT NULL"
        params = {}
        
        if metric:
            where_clause += " AND metric_name = :metric"
            params['metric'] = metric

        query = f"""
        SELECT DISTINCT
            {col} as industry_name,
            GROUP_CONCAT(DISTINCT metric_name) as metrics
        FROM industry_statistics
        WHERE {where_clause}
        GROUP BY {col}
        ORDER BY industry_name
        """

        df = pd.read_sql_query(query, calculator.engine, params=params)

        if df.empty:
            return {
                'success': True,
                'data': {
                    'industries': [],
                    'total': 0
                }
            }, 200

        # 获取行业代码
        industries = []
        for _, row in df.iterrows():
            industry_name = row['industry_name']
            metrics = row['metrics'].split(',') if row['metrics'] else []
            
            # 查询行业代码
            code_query = """
            SELECT index_code, level FROM sw_classify 
            WHERE industry_name = :name
            LIMIT 1
            """
            code_df = pd.read_sql_query(code_query, calculator.engine, params={'name': industry_name})
            
            industry_code = code_df.iloc[0]['index_code'] if not code_df.empty else None
            
            industries.append({
                'industry_code': industry_code,
                'industry_name': industry_name,
                'level': level,
                'metrics': metrics
            })

        return {
            'success': True,
            'data': {
                'industries': industries,
                'total': len(industries)
            }
        }, 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'查询失败: {str(e)}'}, 500
