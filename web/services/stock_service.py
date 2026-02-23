"""
Stock-related business logic services
"""
from flask import request, session, jsonify
from src.utils.stock_lookup import search_stocks, get_stock_name_from_code
from web.utils.export import export_to_csv, export_to_excel
from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH
from src.data_sources.tushare import TushareDB
import pandas as pd
import threading
import time

# 内存缓存用于存储查询结果
_query_cache = {}
_query_cache_lock = threading.Lock()

# 数据库连接（懒加载）
_db = None
_query = None


def get_db():
    """获取数据库连接（懒加载）"""
    global _db, _query
    if _db is None:
        try:
            _db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))
            _query = _db.query()
        except Exception as e:
            print(f"Warning: Failed to initialize database: {e}")
            _db = False
            _query = False
    return _db, _query


def stock_search():
    """
    股票搜索（自动补全，支持股票和指数）

    Query params:
        q: 搜索关键词（代码或名称）
        limit: 返回结果数量（默认10）
        type: 资产类型 ('stock', 'index', 'all')，默认 'all'
    """
    q = request.args.get('q', '')
    limit = int(request.args.get('limit', 10))
    asset_type = request.args.get('type', 'all')

    results = search_stocks(q, limit=limit, asset_type=asset_type)
    return {'stocks': results}


def stock_query():
    """
    股票查询

    Body (JSON):
        symbols: 股票代码列表
        start_date: 开始日期 (YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS)
        end_date: 结束日期 (YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS)
        interval: 时间周期 ('1d', '5m', '15m', '30m', '60m'), 默认 '1d'
        price_type: 价格类型 ('qfq'=前复权, 'hfq'=后复权, 'bfq'=不复权)
    """
    try:
        data = request.json
        if not data:
            return {'error': '请求数据为空'}, 400

        symbols = data.get('symbols', [])
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        interval = data.get('interval', '1d')
        price_type = data.get('price_type', 'qfq')

        if not symbols:
            return {'error': '请选择至少一只股票'}, 400

        if not start_date or not end_date:
            return {'error': '请指定日期范围'}, 400

        # 优先使用 DuckDB 查询（支持所有时间周期）
        try:
            from web.services.duckdb_query_service import get_duckdb_query_service
            duckdb_service = get_duckdb_query_service()

            # 尝试从 DuckDB 查询
            results = duckdb_service.query_bars(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
                price_type=price_type
            )

            # 检查是否有数据返回
            has_data = any(len(data_list) > 0 for data_list in results.values())

            if has_data:
                # DuckDB 有数据，保存到缓存并返回
                cache_key = f"query_{int(time.time())}_{id(results)}"
                with _query_cache_lock:
                    if len(_query_cache) > 10:
                        old_keys = sorted(_query_cache.keys())[:len(_query_cache) - 10]
                        for key in old_keys:
                            del _query_cache[key]
                    _query_cache[cache_key] = {
                        'data': results,
                        'timestamp': time.time(),
                        'interval': interval,
                        'symbols': symbols
                    }
                session['last_query_cache_key'] = cache_key

                return results

        except Exception as e:
            print(f"DuckDB query failed: {e}, falling back to SQLite")

        # DuckDB 没有数据或查询失败，对于日线数据回退到 SQLite
        if interval == '1d':
            db, query = get_db()
            if not db or not query:
                return {'error': '数据库连接失败，且 DuckDB 中无可用数据'}, 500

            # 查询每只股票的数据
            results = {}
            for symbol in symbols:
                try:
                    df = query.query_bars(symbol, start_date, end_date, price_type=price_type)

                    # 将 DataFrame 转换为字典列表
                    if not df.empty:
                        df = df.copy()
                        if 'datetime' in df.columns:
                            df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d')

                        # 只保留需要的列
                        columns_to_keep = [
                            'datetime', 'open', 'high', 'low', 'close',
                            'volume', 'turnover', 'pct_chg', 'amount'
                        ]
                        df = df[[col for col in columns_to_keep if col in df.columns]]

                        # 转换为字典列表，并处理NaN值
                        records = []
                        for _, row in df.iterrows():
                            record = {}
                            for col in df.columns:
                                val = row[col]
                                if pd.isna(val):
                                    record[col] = None
                                else:
                                    record[col] = val
                            records.append(record)

                        results[symbol] = records
                    else:
                        results[symbol] = []

                except Exception as e:
                    results[symbol] = []
                    print(f"Error querying {symbol}: {e}")

            # 保存到内存缓存用于导出
            cache_key = f"query_{int(time.time())}_{id(results)}"
            with _query_cache_lock:
                # 清理旧缓存（保留最近10个）
                if len(_query_cache) > 10:
                    old_keys = sorted(_query_cache.keys())[:len(_query_cache) - 10]
                    for key in old_keys:
                        del _query_cache[key]
                _query_cache[cache_key] = {
                    'data': results,
                    'timestamp': time.time(),
                    'interval': interval,
                    'symbols': symbols
                }

            # 在session中只保存缓存key
            session['last_query_cache_key'] = cache_key

            return results
        else:
            # 非日线周期且 DuckDB 无数据
            return {'error': f'DuckDB 中没有 {interval} 周期的数据，请先导入数据'}, 404

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'查询失败: {str(e)}'}, 500


def stock_export(format):
    """
    数据导出

    Args:
        format: 导出格式 (csv 或 excel)
    """
    if format not in ['csv', 'excel']:
        return {'error': '不支持的导出格式'}, 400

    # 从session获取缓存key
    cache_key = session.get('last_query_cache_key')
    if not cache_key:
        return {'error': '没有可导出的数据，请先执行查询'}, 400

    # 从内存缓存获取数据
    with _query_cache_lock:
        cached = _query_cache.get(cache_key)
        if not cached:
            return {'error': '查询数据已过期，请重新执行查询'}, 400

        data = cached['data']

    try:
        if format == 'csv':
            return export_to_csv(data)
        else:
            return export_to_excel(data)

    except Exception as e:
        return {'error': f'导出失败: {str(e)}'}, 500


def get_stock_name(code):
    """
    获取股票名称

    Args:
        code: 股票代码
    """
    name = get_stock_name_from_code(code)
    return {'code': code, 'name': name}


def get_min_date():
    """
    获取数据库中最早的交易日期

    Returns:
        最早的日期 (YYYY-MM-DD)
    """
    # 优先从 DuckDB 获取（同时查询A股和港股表）
    try:
        from src.db.duckdb_manager import get_duckdb_manager

        duckdb_manager = get_duckdb_manager()
        with duckdb_manager.get_connection() as conn:
            # 查询A股表
            a_min = None
            try:
                if duckdb_manager.table_exists('bars_a_1d'):
                    result = conn.execute("SELECT MIN(datetime) as min_date FROM bars_a_1d").fetchone()
                    if result and result[0]:
                        a_min = str(result[0]).split(' ')[0]
            except:
                pass

            # 查询港股表
            hk_min = None
            try:
                if duckdb_manager.table_exists('bars_1d'):
                    result = conn.execute("SELECT MIN(datetime) as min_date FROM bars_1d WHERE exchange = 'HK'").fetchone()
                    if result and result[0]:
                        hk_min = str(result[0]).split(' ')[0]
            except:
                pass

            # 返回最早的日期
            if a_min and hk_min:
                return {'date': min(a_min, hk_min)}
            elif a_min:
                return {'date': a_min}
            elif hk_min:
                return {'date': hk_min}
    except Exception as e:
        print(f"Error getting min date from DuckDB: {e}")

    # DuckDB 失败，回退到 SQLite（已废弃，仅用于兼容）
    return {'date': '2020-01-01'}


def stock_screen():
    """股票筛选 - 根据条件筛选股票列表"""
    db, query = get_db()
    if not db or not query:
        return {'error': '数据库连接失败'}, 500

    try:
        data = request.json

        # 调用StockQuery的筛选方法
        df = query.screen_stocks(
            days=data.get('days', 5),
            turnover_min=data.get('turnover_min'),
            turnover_max=data.get('turnover_max'),
            pct_chg_min=data.get('pct_chg_min'),
            pct_chg_max=data.get('pct_chg_max'),
            price_min=data.get('price_min'),
            price_max=data.get('price_max'),
            volume_min=data.get('volume_min'),
            volume_max=data.get('volume_max')
        )

        # 转换为JSON格式
        results = []
        for _, row in df.iterrows():
            results.append({
                'code': row['symbol'],
                'name': row['name'],
                'board': row.get('board', '未知'),
                'latest_date': str(row['latest_date']),
                'latest_close': round(float(row['latest_close']), 2) if pd.notna(row['latest_close']) else None,
                'avg_turnover': round(float(row['avg_turnover']), 2) if pd.notna(row['avg_turnover']) else None,
                'avg_pct_chg': round(float(row['avg_pct_chg']), 2) if pd.notna(row['avg_pct_chg']) else None
            })

        return {
            'success': True,
            'count': len(results),
            'symbols': results[:500],  # 最多返回500只
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'筛选失败: {str(e)}'}, 500


def get_available_intervals():
    """
    获取可用的时间周期列表及数据统计（合并SQLite和DuckDB）

    Returns:
        各时间周期的可用性信息
    """
    try:
        from web.services.duckdb_query_service import get_duckdb_query_service
        import sqlite3

        # 支持的所有周期
        all_intervals = ['1d', '5m', '15m', '30m', '60m']

        # 存储每个周期的数据来源信息
        intervals_data = {}

        # 1. 从DuckDB获取数据
        duckdb_service = get_duckdb_query_service()
        duckdb_tables_info = duckdb_service.get_all_tables_info()

        for info in duckdb_tables_info:
            interval = info['interval']
            if info['exists']:
                intervals_data[interval] = {
                    'interval': interval,
                    'available': True,
                    'sources': ['duckdb'],
                    'table_name': info['table_name'],
                    'row_count': info['row_count'],
                    'symbol_count': info.get('symbol_count', 0)
                }
                if info.get('date_range'):
                    intervals_data[interval]['date_range'] = info['date_range']

        # 2. 从SQLite获取数据
        try:
            conn = sqlite3.connect(str(TUSHARE_DB_PATH))
            cursor = conn.cursor()

            # 查询每个周期的数据
            for interval in all_intervals:
                cursor.execute("""
                    SELECT
                        COUNT(*) as count,
                        COUNT(DISTINCT substr(symbol, 1, 6)) as symbol_count,
                        MIN(datetime) as min_date,
                        MAX(datetime) as max_date
                    FROM bars
                    WHERE interval = ?
                """, (interval,))

                row = cursor.fetchone()
                if row and row[0] > 0:
                    # SQLite有这个周期的数据
                    if interval in intervals_data:
                        # 已有DuckDB数据，合并
                        intervals_data[interval]['sources'].append('sqlite')
                        intervals_data[interval]['row_count'] += row[0]
                        intervals_data[interval]['symbol_count'] += row[1]
                    else:
                        # 只有SQLite数据
                        intervals_data[interval] = {
                            'interval': interval,
                            'available': True,
                            'sources': ['sqlite'],
                            'table_name': 'bars',
                            'row_count': row[0],
                            'symbol_count': row[1],
                            'date_range': {
                                'start': row[2],
                                'end': row[3]
                            }
                        }

            conn.close()
        except Exception as e:
            print(f"获取SQLite周期信息失败: {e}")

        # 3. 格式化返回数据
        intervals = []
        for interval in all_intervals:
            if interval in intervals_data:
                intervals.append(intervals_data[interval])
            else:
                # 没有数据的周期
                intervals.append({
                    'interval': interval,
                    'available': False,
                    'sources': []
                })

        return {
            'success': True,
            'intervals': intervals
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e),
            'intervals': []
        }
