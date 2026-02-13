"""
Dragon List Service

龙虎榜数据查询服务
"""
import pandas as pd
from sqlalchemy import text
from flask import request
import logging

from config.settings import TUSHARE_DB_PATH
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def get_db_engine():
    """获取数据库引擎"""
    return create_engine(
        f'sqlite:///{TUSHARE_DB_PATH}',
        connect_args={
            'check_same_thread': False,
            'timeout': 30  # 30秒超时
        }
    )


def query_dragon_list(trade_date: str = None, start_date: str = None,
                      end_date: str = None, ts_code: str = None,
                      reason: str = None, limit: int = 100):
    """
    查询龙虎榜数据

    Args:
        trade_date: 交易日期 YYYY-MM-DD
        start_date: 开始日期 YYYY-MM-DD
        end_date: 结束日期 YYYY-MM-DD
        ts_code: 股票代码
        reason: 上榜理由（支持模糊匹配）
        limit: 返回记录数限制

    Returns:
        JSON response
    """
    try:
        engine = get_db_engine()

        # 构建查询
        query_sql = """
        SELECT * FROM dragon_list
        WHERE 1=1
        """
        params = {}

        if trade_date:
            query_sql += " AND trade_date = :trade_date"
            params['trade_date'] = trade_date.replace('-', '')

        if start_date:
            query_sql += " AND trade_date >= :start_date"
            params['start_date'] = start_date.replace('-', '')

        if end_date:
            query_sql += " AND trade_date <= :end_date"
            params['end_date'] = end_date.replace('-', '')

        if ts_code:
            query_sql += " AND ts_code = :ts_code"
            params['ts_code'] = ts_code.upper()

        if reason:
            query_sql += " AND reason LIKE :reason"
            params['reason'] = f'%{reason}%'

        query_sql += " ORDER BY trade_date DESC, net_amount DESC"
        query_sql += " LIMIT :limit"
        params['limit'] = limit

        with engine.connect() as conn:
            df = pd.read_sql_query(query_sql, conn, params=params)

        if df.empty:
            return {
                'success': True,
                'data': [],
                'count': 0
            }, 200

        # 转换为字典列表，确保 NaN 被替换为 None
        df_clean = df.where(pd.notnull(df), None)
        # 使用 to_dict('records') 然后处理 NaN 值
        data = df_clean.to_dict('records')
        # 额外处理：将任何 NaN 值替换为 None
        import math
        for record in data:
            for key, value in record.items():
                if isinstance(value, float) and math.isnan(value):
                    record[key] = None

        return {
            'success': True,
            'data': data,
            'count': len(data)
        }, 200

    except Exception as e:
        logger.error(f"查询龙虎榜数据失败: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }, 500


def get_dragon_list_by_stock(ts_code: str, limit: int = 50):
    """
    查询指定股票的龙虎榜历史

    Args:
        ts_code: 股票代码
        limit: 返回记录数限制

    Returns:
        JSON response
    """
    return query_dragon_list(ts_code=ts_code, limit=limit)


def get_top_dragon_list(trade_date: str = None, top_n: int = 10,
                        by: str = 'net_amount'):
    """
    获取龙虎榜排名

    Args:
        trade_date: 交易日期 YYYY-MM-DD，默认为最近
        top_n: 返回前N名
        by: 排序字段 (net_amount, l_amount, amount, net_rate)

    Returns:
        JSON response
    """
    try:
        engine = get_db_engine()

        if not trade_date:
            # 获取最新日期
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT MAX(trade_date) as max_date FROM dragon_list")
                ).fetchone()
                if result and result[0]:
                    trade_date = result[0]

        # 验证排序字段
        valid_fields = ['net_amount', 'l_amount', 'amount', 'net_rate']
        if by not in valid_fields:
            by = 'net_amount'

        query_sql = f"""
        SELECT * FROM dragon_list
        WHERE trade_date = :trade_date
        ORDER BY {by} DESC
        LIMIT :top_n
        """

        with engine.connect() as conn:
            df = pd.read_sql_query(query_sql, conn, params={
                'trade_date': trade_date.replace('-', '') if trade_date else trade_date,
                'top_n': top_n
            })

        if df.empty:
            return {
                'success': True,
                'data': [],
                'count': 0
            }, 200

        df_clean = df.where(pd.notnull(df), None)
        data = df_clean.to_dict('records')

        return {
            'success': True,
            'data': data,
            'count': len(data),
            'trade_date': trade_date
        }, 200

    except Exception as e:
        logger.error(f"查询龙虎榜排名失败: {e}")
        return {
            'success': False,
            'error': str(e)
        }, 500


def get_dragon_list_stats(trade_date: str = None):
    """
    获取龙虎榜统计数据

    Args:
        trade_date: 交易日期 YYYY-MM-DD，默认为最近

    Returns:
        JSON response with statistics
    """
    try:
        engine = get_db_engine()

        if not trade_date:
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT MAX(trade_date) as max_date FROM dragon_list")
                ).fetchone()
                if result and result[0]:
                    trade_date = result[0]

        query = """
        SELECT
            COUNT(*) as total_count,
            COUNT(DISTINCT reason) as reason_count,
            SUM(CASE WHEN net_amount > 0 THEN 1 ELSE 0 END) as net_buy_count,
            SUM(CASE WHEN net_amount < 0 THEN 1 ELSE 0 END) as net_sell_count,
            SUM(net_amount) as total_net_amount,
            SUM(l_amount) as total_l_amount,
            AVG(net_rate) as avg_net_rate
        FROM dragon_list
        WHERE trade_date = :trade_date
        """

        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params={
                'trade_date': trade_date.replace('-', '') if trade_date else trade_date
            })

        if df.empty:
            return {
                'success': True,
                'data': None
            }, 200

        stats = df.iloc[0].to_dict()

        # 按上榜理由统计
        reason_query = """
        SELECT
            reason,
            COUNT(*) as count,
            SUM(net_amount) as net_amount
        FROM dragon_list
        WHERE trade_date = :trade_date
        GROUP BY reason
        ORDER BY count DESC
        """

        with engine.connect() as conn:
            reason_df = pd.read_sql_query(reason_query, conn, params={
                'trade_date': trade_date.replace('-', '') if trade_date else trade_date
            })

        reason_stats = reason_df.to_dict('records') if not reason_df.empty else []

        return {
            'success': True,
            'data': {
                'summary': stats,
                'by_reason': reason_stats,
                'trade_date': trade_date
            }
        }, 200

    except Exception as e:
        logger.error(f"查询龙虎榜统计失败: {e}")
        return {
            'success': False,
            'error': str(e)
        }, 500
