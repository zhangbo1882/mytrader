"""
Moneyflow Service

Business logic service for handling money flow data queries.
"""
import pandas as pd
from sqlalchemy import text
from flask import jsonify, request
import logging

from config.settings import TUSHARE_DB_PATH
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def get_db_engine():
    """Get database engine."""
    return create_engine(
        f'sqlite:///{TUSHARE_DB_PATH}',
        connect_args={
            'check_same_thread': False,
            'timeout': 30  # 30秒超时
        }
    )


def normalize_ts_code(ts_code: str) -> str:
    """
    Normalize stock code to include suffix if missing.

    Args:
        ts_code: Stock code (with or without suffix)

    Returns:
        Normalized stock code with suffix
    """
    if not ts_code:
        return ts_code

    # If already has suffix (.), return as is
    if '.' in ts_code:
        return ts_code

    # Add suffix based on code pattern
    # 6xxxxx -> Shanghai (.SH)
    # 0xxxxx, 3xxxxx -> Shenzhen (.SZ)
    # 8xxxxx, 4xxxxx -> Beijing (.BJ)
    if ts_code.startswith('6'):
        return f'{ts_code}.SH'
    elif ts_code.startswith('0') or ts_code.startswith('3'):
        return f'{ts_code}.SZ'
    elif ts_code.startswith('8') or ts_code.startswith('4'):
        return f'{ts_code}.BJ'
    else:
        # Unknown pattern, try to find in database
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT ts_code FROM stock_moneyflow WHERE ts_code LIKE :code LIMIT 1"),
                {'code': f'{ts_code}.%'}
            ).fetchone()
            if result:
                return result[0]
        return ts_code


def get_stock_moneyflow(ts_code: str, start_date: str = None, end_date: str = None, limit: int = 100):
    """
    Get stock money flow data.

    Args:
        ts_code: Stock code
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD
        limit: Max number of records to return

    Returns:
        JSON response with money flow records
    """
    try:
        engine = get_db_engine()

        # Normalize stock code (add suffix if missing)
        ts_code = normalize_ts_code(ts_code)

        # Build query
        query = """
        SELECT * FROM stock_moneyflow
        WHERE ts_code = :ts_code
        """
        params = {'ts_code': ts_code}

        if start_date:
            query += " AND trade_date >= :start_date"
            params['start_date'] = start_date.replace('-', '')

        if end_date:
            query += " AND trade_date <= :end_date"
            params['end_date'] = end_date.replace('-', '')

        query += " ORDER BY trade_date DESC LIMIT :limit"
        params['limit'] = limit

        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return {
                'success': True,
                'data': [],
                'count': 0
            }, 200

        # Convert to list of dicts
        df_clean = df.where(pd.notnull(df), None)
        data = df_clean.to_dict('records')

        return {
            'success': True,
            'data': data,
            'count': len(data)
        }, 200

    except Exception as e:
        logger.error(f"Error getting stock moneyflow: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }, 500


def get_industry_moneyflow(level: str = 'L1', industry_name: str = None,
                          start_date: str = None, end_date: str = None, limit: int = 100):
    """
    Get industry money flow summary data.

    Args:
        level: Industry level (L1/L2/L3)
        industry_name: Industry name (optional)
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD
        limit: Max number of records to return

    Returns:
        JSON response with industry money flow records
    """
    try:
        engine = get_db_engine()

        # Build query
        query = """
        SELECT * FROM industry_moneyflow
        WHERE level = :level
        """
        params = {'level': level}

        if industry_name:
            # Filter by industry name based on level
            if level == 'L1':
                query += " AND sw_l1 = :industry_name"
            elif level == 'L2':
                query += " AND sw_l2 = :industry_name"
            elif level == 'L3':
                query += " AND sw_l3 = :industry_name"
            params['industry_name'] = industry_name

        if start_date:
            query += " AND trade_date >= :start_date"
            params['start_date'] = start_date.replace('-', '')

        if end_date:
            query += " AND trade_date <= :end_date"
            params['end_date'] = end_date.replace('-', '')

        query += " ORDER BY trade_date DESC LIMIT :limit"
        params['limit'] = limit

        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return {
                'success': True,
                'data': [],
                'count': 0
            }, 200

        # Convert to list of dicts
        df_clean = df.where(pd.notnull(df), None)
        data = df_clean.to_dict('records')

        return {
            'success': True,
            'data': data,
            'count': len(data)
        }, 200

    except Exception as e:
        logger.error(f"Error getting industry moneyflow: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }, 500


def get_top_industries_by_netflow(level: str = 'L1', trade_date: str = None, top_n: int = None, accumulate_days: int = 1):
    """
    Get top N industries by net money flow.

    Args:
        level: Industry level (L1/L2/L3)
        trade_date: Trade date YYYY-MM-DD (default: latest)
        top_n: Number of top industries to return (None = return all)
        accumulate_days: Number of trading days to accumulate (default: 1)
                          - 1 = single day (default)
                          - >1 = accumulate over multiple trading days

    Returns:
        JSON response with top industries by net flow
    """
    try:
        engine = get_db_engine()

        # Determine the end date
        if trade_date is None:
            query_latest = "SELECT MAX(trade_date) as max_date FROM industry_moneyflow WHERE level = :level"
            with engine.connect() as conn:
                result = conn.execute(text(query_latest), {'level': level}).fetchone()
                if result and result[0]:
                    trade_date = result[0]
                else:
                    return {
                        'success': True,
                        'data': [],
                        'count': 0
                    }, 200

        if accumulate_days == 1:
            # Single day query (original behavior)
            query = """
            SELECT * FROM industry_moneyflow
            WHERE level = :level AND trade_date = :trade_date
            ORDER BY net_mf_amount DESC
            """
            params = {
                'level': level,
                'trade_date': trade_date.replace('-', '') if '-' in trade_date else trade_date
            }
            if top_n:
                query += " LIMIT :top_n"
                params['top_n'] = top_n

            with engine.connect() as conn:
                df = pd.read_sql_query(query, conn, params=params)
        else:
            # Multiple days accumulation
            # First, get the last N trading dates
            dates_query = f"""
            SELECT DISTINCT trade_date
            FROM industry_moneyflow
            WHERE level = :level AND trade_date <= :trade_date
            ORDER BY trade_date DESC
            LIMIT :accumulate_days
            """

            with engine.connect() as conn:
                dates_df = pd.read_sql_query(dates_query, conn, params={
                    'level': level,
                    'trade_date': trade_date.replace('-', '') if '-' in trade_date else trade_date,
                    'accumulate_days': accumulate_days
                })

            if dates_df.empty:
                return {
                    'success': True,
                    'data': [],
                    'count': 0,
                    'trade_date': trade_date
                }, 200

            trade_dates = dates_df['trade_date'].tolist()

            # Sum by industry over the specified dates
            date_list = "', '".join(trade_dates)
            query = f"""
            SELECT
                level,
                MAX(sw_l1) as sw_l1,
                MAX(sw_l2) as sw_l2,
                MAX(sw_l3) as sw_l3,
                index_code,
                MAX(stock_count) as stock_count,
                SUM(up_count) as up_count,
                SUM(down_count) as down_count,
                SUM(limit_up_count) as limit_up_count,
                SUM(limit_down_count) as limit_down_count,
                SUM(net_mf_amount) as net_mf_amount,
                SUM(net_lg_amount) as net_lg_amount,
                SUM(net_elg_amount) as net_elg_amount,
                SUM(buy_elg_amount) as buy_elg_amount,
                SUM(sell_elg_amount) as sell_elg_amount,
                SUM(buy_lg_amount) as buy_lg_amount,
                SUM(sell_lg_amount) as sell_lg_amount,
                AVG(avg_net_amount) as avg_net_amount,
                AVG(avg_net_lg_amount) as avg_net_lg_amount,
                AVG(avg_net_elg_amount) as avg_net_elg_amount
            FROM industry_moneyflow
            WHERE level = :level
              AND trade_date IN ('{date_list}')
            GROUP BY level, index_code
            ORDER BY net_mf_amount DESC
            """
            params = {'level': level}
            if top_n:
                query += " LIMIT :top_n"
                params['top_n'] = top_n

            with engine.connect() as conn:
                df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return {
                'success': True,
                'data': [],
                'count': 0,
                'trade_date': trade_date
            }, 200

        # Convert to list of dicts
        df_clean = df.where(pd.notnull(df), None)
        data = df_clean.to_dict('records')

        return {
            'success': True,
            'data': data,
            'count': len(data),
            'trade_date': trade_date
        }, 200

    except Exception as e:
        logger.error(f"Error getting top industries by netflow: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }, 500


def get_industry_stocks_moneyflow(industry_name: str, level: str = 'L1',
                                  trade_date: str = None, accumulate_days: int = 1):
    """
    Get money flow data for all stocks in a specific industry.

    Args:
        industry_name: Industry name
        level: Industry level (L1/L2/L3)
        trade_date: Trade date YYYY-MM-DD (default: latest)
        accumulate_days: Number of trading days to accumulate (default: 1)

    Returns:
        JSON response with stock money flow data
    """
    try:
        engine = get_db_engine()

        # If no date specified, get the latest date
        if trade_date is None:
            query_latest = "SELECT MAX(trade_date) as max_date FROM stock_moneyflow"
            with engine.connect() as conn:
                result = conn.execute(text(query_latest)).fetchone()
                if result and result[0]:
                    trade_date = result[0]
                else:
                    return {
                        'success': True,
                        'data': [],
                        'count': 0
                    }, 200

        if accumulate_days == 1:
            # Single day query (original behavior)
            query = """
            SELECT m.*, c.industry_name, sb.name as stock_name
            FROM stock_moneyflow m
            JOIN sw_members mem ON m.ts_code = mem.ts_code AND mem.is_new = 'Y'
            JOIN sw_classify c ON mem.index_code = c.index_code
            LEFT JOIN stock_basic_info sb ON m.ts_code = sb.ts_code
            WHERE c.industry_name = :industry_name
            AND c.level = :level
            AND m.trade_date = :trade_date
            ORDER BY m.net_mf_amount DESC
            """

            with engine.connect() as conn:
                df = pd.read_sql_query(query, conn, params={
                    'industry_name': industry_name,
                    'level': level,
                    'trade_date': trade_date.replace('-', '') if '-' in trade_date else trade_date
                })
        else:
            # Multiple days accumulation
            # First, get the last N trading dates
            dates_query = """
            SELECT DISTINCT trade_date
            FROM stock_moneyflow
            WHERE trade_date <= :trade_date
            ORDER BY trade_date DESC
            LIMIT :accumulate_days
            """

            with engine.connect() as conn:
                dates_df = pd.read_sql_query(dates_query, conn, params={
                    'trade_date': trade_date.replace('-', '') if '-' in trade_date else trade_date,
                    'accumulate_days': accumulate_days
                })

            if dates_df.empty:
                return {
                    'success': True,
                    'data': [],
                    'count': 0,
                    'trade_date': trade_date,
                    'industry_name': industry_name
                }, 200

            trade_dates = dates_df['trade_date'].tolist()

            # Sum by stock over the specified dates
            date_list = "', '".join(trade_dates)
            query = f"""
            SELECT
                m.ts_code,
                MAX(sb.name) as stock_name,
                MAX(c.industry_name) as industry_name,
                SUM(m.buy_sm_vol) as buy_sm_vol,
                SUM(m.buy_sm_amount) as buy_sm_amount,
                SUM(m.sell_sm_vol) as sell_sm_vol,
                SUM(m.sell_sm_amount) as sell_sm_amount,
                SUM(m.buy_md_vol) as buy_md_vol,
                SUM(m.buy_md_amount) as buy_md_amount,
                SUM(m.sell_md_vol) as sell_md_vol,
                SUM(m.sell_md_amount) as sell_md_amount,
                SUM(m.buy_lg_vol) as buy_lg_vol,
                SUM(m.buy_lg_amount) as buy_lg_amount,
                SUM(m.sell_lg_vol) as sell_lg_vol,
                SUM(m.sell_lg_amount) as sell_lg_amount,
                SUM(m.buy_elg_vol) as buy_elg_vol,
                SUM(m.buy_elg_amount) as buy_elg_amount,
                SUM(m.sell_elg_vol) as sell_elg_vol,
                SUM(m.sell_elg_amount) as sell_elg_amount,
                SUM(m.net_mf_vol) as net_mf_vol,
                SUM(m.net_mf_amount) as net_mf_amount,
                SUM(m.net_lg_amount) as net_lg_amount,
                SUM(m.net_elg_amount) as net_elg_amount,
                MAX(m.trade_date) as trade_date
            FROM stock_moneyflow m
            JOIN sw_members mem ON m.ts_code = mem.ts_code AND mem.is_new = 'Y'
            JOIN sw_classify c ON mem.index_code = c.index_code
            LEFT JOIN stock_basic_info sb ON m.ts_code = sb.ts_code
            WHERE c.industry_name = :industry_name
              AND c.level = :level
              AND m.trade_date IN ('{date_list}')
            GROUP BY m.ts_code
            ORDER BY net_mf_amount DESC
            """

            with engine.connect() as conn:
                df = pd.read_sql_query(query, conn, params={
                    'industry_name': industry_name,
                    'level': level
                })

        if df.empty:
            return {
                'success': True,
                'data': [],
                'count': 0,
                'trade_date': trade_date,
                'industry_name': industry_name
            }, 200

        # Convert to list of dicts
        df_clean = df.where(pd.notnull(df), None)
        data = df_clean.to_dict('records')

        return {
            'success': True,
            'data': data,
            'count': len(data),
            'trade_date': trade_date,
            'industry_name': industry_name
        }, 200

    except Exception as e:
        logger.error(f"Error getting industry stocks moneyflow: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }, 500
