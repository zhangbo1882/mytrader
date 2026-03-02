# web/services/duckdb_query_service.py
"""
DuckDB-based query service for multi-timeframe stock data
"""
import pandas as pd
from typing import List, Dict, Any, Optional
import logging

from src.db.duckdb_manager import DuckDBManager, get_duckdb_manager
from config.settings import SUPPORTED_INTERVALS, INTERVAL_TABLE_MAP, A_SHARE_TABLE_MAP

logger = logging.getLogger(__name__)


class DuckDBQueryService:
    """DuckDB 查询服务，支持多时间周期"""

    def __init__(self, db_manager: Optional[DuckDBManager] = None):
        self.db_manager = db_manager or get_duckdb_manager()

    def query_bars(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        interval: str = '1d',
        price_type: str = 'bfq'
    ) -> Dict[str, List[Dict]]:
        """
        查询K线数据

        Args:
            symbols: 股票代码列表（支持 000001 或 000001.SZ 格式）
            start_date: 开始日期 (YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS for minute data)
            end_date: 结束日期
            interval: 时间周期 ('1d', '5m', '15m', '30m', '60m')
            price_type: 价格类型 ('qfq'=前复权, 'hfq'=后复权, 'bfq'=不复权)

        Returns:
            字典，key为股票代码（带后缀，如 000001.SZ），value为数据列表
        """
        if interval not in SUPPORTED_INTERVALS:
            raise ValueError(f"Unsupported interval: {interval}. Must be one of {SUPPORTED_INTERVALS}")

        # 根据价格类型选择列
        if interval == '1d' and price_type == 'qfq':
            # 日线数据且有前复权字段
            price_columns = ['open_qfq', 'high_qfq', 'low_qfq', 'close_qfq']
        else:
            # 使用原始价格
            price_columns = ['open', 'high', 'low', 'close']

        # 使用 context manager 避免持久连接占用锁
        with self.db_manager.get_connection() as conn:
            results = {}
            table_cache = {}  # 缓存表信息，避免重复查询

            for symbol in symbols:
                try:
                    # 解析股票代码（支持 000001.SZ 或 000001 格式）
                    stock_code, exchange = self._parse_symbol(symbol)

                    # 根据股票类型选择表名
                    # A股: 6位数字开头(600xxx, 000xxx, 002xxx, 300xxx) 或包含.SH/.SZ后缀
                    # 港股: 包含.HK后缀或5位数字(00xxx, 09xxx)
                    is_a_share = (
                        (exchange and exchange in ['SH', 'SZ']) or  # 有A股交易所后缀
                        (not exchange and len(stock_code) == 6 and stock_code[0] in '01236')  # 6位A股代码
                    )

                    if is_a_share:
                        table_name = A_SHARE_TABLE_MAP.get(interval, INTERVAL_TABLE_MAP[interval])
                    else:
                        table_name = INTERVAL_TABLE_MAP[interval]

                    # 检查表是否存在（使用缓存）
                    if table_name not in table_cache:
                        if not self.db_manager.table_exists(table_name):
                            logger.warning(f"Table {table_name} does not exist for symbol {symbol}")
                            table_cache[table_name] = None
                        else:
                            # 获取表的列信息
                            table_info = self.db_manager.get_table_info(table_name)
                            existing_columns = [col['column_name'] for col in table_info]
                            # 基础列 + 可选列（换手率、股本、市值等）
                            # 包含pre_close以便在pct_chg为NULL时计算涨跌幅
                            optional_columns = ['volume', 'amount', 'pct_chg', 'pre_close', 'turnover', 'turnover_rate_f',
                                              'total_share', 'free_share', 'total_mv', 'circ_mv']
                            columns = ['datetime'] + price_columns + optional_columns
                            columns = [col for col in columns if col in existing_columns]
                            table_cache[table_name] = columns
                    else:
                        columns = table_cache[table_name]

                    # 表不存在，跳过
                    if columns is None:
                        results[symbol] = []
                        continue

                    # 构建查询条件
                    if exchange:
                        # 有交易所后缀，精确匹配
                        where_clause = "WHERE stock_code = ? AND exchange = ?"
                        params = [stock_code, exchange]
                    else:
                        # 无交易所后缀，只匹配 stock_code
                        where_clause = "WHERE stock_code = ?"
                        params = [stock_code]

                    # 构建查询SQL
                    columns_str = ', '.join(['stock_code', 'exchange'] + columns)

                    query = f"""
                        SELECT {columns_str}
                        FROM {table_name}
                        {where_clause}
                        AND datetime >= ? AND datetime <= ?
                        ORDER BY datetime
                    """
                    params.extend([start_date, end_date])

                    df = conn.execute(query, params).fetchdf()

                    # 转换为字典列表
                    if not df.empty:
                        df = df.copy()

                        # 添加 ts_code 计算字段
                        df['ts_code'] = df.apply(
                            lambda row: f"{row['stock_code']}.{row['exchange']}"
                            if pd.notna(row['exchange']) else row['stock_code'],
                            axis=1
                        )

                        # 标准化列名（如果使用了前复权列）
                        if price_type == 'qfq' and interval == '1d':
                            rename_map = {
                                'open_qfq': 'open',
                                'high_qfq': 'high',
                                'low_qfq': 'low',
                                'close_qfq': 'close'
                            }
                            df = df.rename(columns=rename_map)

                            # 计算前复权涨跌幅（使用前复权价格）
                            # 因为pre_close是未复权的，所以需要用close相对于前一日的close计算
                            if 'pct_chg' in df.columns and 'close' in df.columns:
                                # 按日期排序
                                df = df.sort_values('datetime')
                                # 计算前一日的收盘价
                                df['prev_close'] = df['close'].shift(1)
                                # 对于pct_chg为NULL的行，使用 (close - prev_close) / prev_close * 100 计算
                                mask_null_pct = df['pct_chg'].isna()
                                mask_valid_prev = df['prev_close'].notna() & (df['prev_close'] > 0)
                                df.loc[mask_null_pct & mask_valid_prev, 'pct_chg'] = (
                                    (df.loc[mask_null_pct & mask_valid_prev, 'close'] -
                                     df.loc[mask_null_pct & mask_valid_prev, 'prev_close']) /
                                    df.loc[mask_null_pct & mask_valid_prev, 'prev_close'] * 100
                                )
                                # 删除临时列
                                df = df.drop(columns=['prev_close'])
                        else:
                            # 非前复权时，使用pre_close计算涨跌幅
                            if 'pct_chg' in df.columns and 'pre_close' in df.columns and 'close' in df.columns:
                                # 对于pct_chg为NULL的行，使用 (close - pre_close) / pre_close * 100 计算
                                mask_null_pct = df['pct_chg'].isna()
                                mask_valid_pre = df['pre_close'].notna() & (df['pre_close'] > 0)
                                df.loc[mask_null_pct & mask_valid_pre, 'pct_chg'] = (
                                    (df.loc[mask_null_pct & mask_valid_pre, 'close'] -
                                     df.loc[mask_null_pct & mask_valid_pre, 'pre_close']) /
                                    df.loc[mask_null_pct & mask_valid_pre, 'pre_close'] * 100
                                )

                        # 转换datetime为字符串
                        if 'datetime' in df.columns:
                            if interval == '1d':
                                df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d')
                            else:
                                df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')

                        # 单位转换：根据股票类型不同处理
                        # A股：Tushare 原始单位是"手"和"千元"，保存时未转换
                        # 港股：Tushare 原始单位是"股"和"千元"，保存时未转换
                        is_hk = exchange == 'HK' or (exchange is None and len(stock_code) == 5 and stock_code.startswith('0'))

                        if not is_hk:
                            # A股：volume是"手"需要转"股"（乘以100）
                            if 'volume' in df.columns:
                                df['volume'] = df['volume'] * 100
                            # A股：amount是"千元"需要转"元"（乘以1000）
                            if 'amount' in df.columns:
                                df['amount'] = df['amount'] * 1000
                        # 港股：volume和amount在数据库中已经是正确单位，不需要转换
                        # 注意：turnover 字段现在存储的是换手率（百分比），不需要单位转换

                        # 处理NaN值 - 将所有NaN转为None（JSON null）
                        # 必须在 to_dict 之前处理，否则 NaN 会被序列化为无效的 JSON
                        for col in df.columns:
                            df[col] = df[col].where(pd.notna(df[col]), None)

                        # 转换为字典列表
                        records = []
                        for _, row in df.iterrows():
                            record = {}
                            for col in df.columns:
                                val = row[col]
                                # 额外检查，确保没有 NaN 漏网
                                if pd.isna(val):
                                    record[col] = None
                                else:
                                    record[col] = val
                            records.append(record)

                        results[symbol] = records
                    else:
                        results[symbol] = []

                except Exception as e:
                    logger.error(f"Error querying {symbol}: {e}")
                    results[symbol] = []

        return results

    def _parse_symbol(self, symbol: str) -> tuple:
        """
        解析股票代码，分离 stock_code 和 exchange

        支持格式：
        - 000001.SZ -> ('000001', 'SZ')
        - 600000.SH -> ('600000', 'SH')
        - 00941.HK -> ('00941', 'HK')
        - 000001 -> ('000001', None)

        Args:
            symbol: 股票代码

        Returns:
            (stock_code, exchange) 元组
        """
        exchange_suffix_map = {
            'SH': 'SH', 'SZ': 'SZ', 'HK': 'HK',
            'sh': 'SH', 'sz': 'SZ', 'hk': 'HK',
        }

        for suffix, exchange in exchange_suffix_map.items():
            if symbol.endswith(f'.{suffix}'):
                stock_code = symbol[:-len(f'.{suffix}')]
                return stock_code, exchange

        return symbol, None

    def get_available_intervals(self) -> List[Dict[str, str]]:
        """
        获取可用的时间周期列表

        Returns:
            时间周期列表，包含value和label
        """
        return [
            {'value': '5m', 'label': '5分钟'},
            {'value': '15m', 'label': '15分钟'},
            {'value': '30m', 'label': '30分钟'},
            {'value': '60m', 'label': '60分钟'},
            {'value': '1d', 'label': '日线'},
        ]

    def get_table_info(self, interval: str) -> Dict[str, Any]:
        """
        获取指定周期表的信息

        Args:
            interval: 时间周期

        Returns:
            表信息
        """
        if interval not in SUPPORTED_INTERVALS:
            raise ValueError(f"Unsupported interval: {interval}")

        table_name = INTERVAL_TABLE_MAP[interval]

        # 使用 context manager 避免持久连接占用锁
        with self.db_manager.get_connection() as conn:
            if not self.db_manager.table_exists(table_name):
                return {
                    'exists': False,
                    'interval': interval,
                    'table_name': table_name
                }

            # 获取基本信息
            row_count = self.db_manager.get_row_count(table_name)

            # 获取日期范围
            date_range = conn.execute(
                f"SELECT MIN(datetime) as min_date, MAX(datetime) as max_date FROM {table_name}"
            ).fetchdf()

            # 获取股票数量（使用 stock_code）
            symbol_count_df = conn.execute(
                f"SELECT COUNT(DISTINCT stock_code) as count FROM {table_name}"
            ).fetchdf()

            result = {
                'exists': True,
                'interval': interval,
                'table_name': table_name,
                'row_count': int(row_count) if row_count else 0,
                'symbol_count': int(symbol_count_df.iloc[0]['count']) if not symbol_count_df.empty else 0,
            }

            if not date_range.empty:
                min_date = date_range.iloc[0]['min_date']
                max_date = date_range.iloc[0]['max_date']
                result['date_range'] = {
                    'start': str(min_date) if pd.notna(min_date) else None,
                    'end': str(max_date) if pd.notna(max_date) else None
                }

        return result

    def get_all_tables_info(self) -> List[Dict[str, Any]]:
        """
        获取所有时间周期表的信息

        Returns:
            表信息列表
        """
        results = []
        for interval in SUPPORTED_INTERVALS:
            info = self.get_table_info(interval)
            results.append(info)
        return results

    def get_symbols_for_interval(self, interval: str, limit: Optional[int] = None) -> List[str]:
        """
        获取指定周期下的股票列表

        Args:
            interval: 时间周期
            limit: 最多返回多少只股票

        Returns:
            股票代码列表
        """
        if interval not in SUPPORTED_INTERVALS:
            raise ValueError(f"Unsupported interval: {interval}")

        table_name = INTERVAL_TABLE_MAP[interval]

        # 使用 context manager 避免持久连接占用锁
        with self.db_manager.get_connection() as conn:
            if not self.db_manager.table_exists(table_name):
                return []

            if limit:
                query = f"SELECT DISTINCT stock_code, exchange FROM {table_name} ORDER BY stock_code LIMIT ?"
                result = conn.execute(query, [limit]).fetchdf()
            else:
                query = f"SELECT DISTINCT stock_code, exchange FROM {table_name} ORDER BY stock_code"
                result = conn.execute(query).fetchdf()

            # 组合成 ts_code 格式
            if not result.empty:
                symbols = []
                for _, row in result.iterrows():
                    stock_code = row['stock_code']
                    exchange = row['exchange']
                    if pd.notna(exchange):
                        symbols.append(f"{stock_code}.{exchange}")
                    else:
                        symbols.append(stock_code)
                return symbols

        return []


# 全局单例
_query_service: Optional[DuckDBQueryService] = None


def get_duckdb_query_service() -> DuckDBQueryService:
    """
    获取全局查询服务实例

    Returns:
        DuckDBQueryService 实例
    """
    global _query_service
    if _query_service is None:
        _query_service = DuckDBQueryService()
    return _query_service
