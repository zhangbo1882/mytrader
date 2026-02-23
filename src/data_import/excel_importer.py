# src/data_import/excel_importer.py
"""
Excel 数据导入服务
支持自动识别时间周期并导入到 DuckDB
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime, timedelta

from src.db.duckdb_manager import DuckDBManager
from config.settings import SUPPORTED_INTERVALS

logger = logging.getLogger(__name__)


class ExcelDataImporter:
    """Excel 数据导入器"""

    # 必需的列
    REQUIRED_COLUMNS = ['stock_code', 'datetime', 'close']

    # 可选的数值列
    OPTIONAL_NUMERIC_COLUMNS = [
        'open', 'high', 'low', 'close', 'volume', 'amount',
        'open_qfq', 'high_qfq', 'low_qfq', 'close_qfq',
        'pre_close', 'change', 'pct_chg', 'turnover',
        'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm',
        'total_mv', 'circ_mv', 'total_share', 'float_share', 'free_share',
        'volume_ratio', 'turnover_rate_f', 'dv_ratio', 'dv_ttm'
    ]

    # 交易所后缀映射
    EXCHANGE_SUFFIX_MAP = {
        'SH': 'SH',  # 上海证券交易所
        'SZ': 'SZ',  # 深圳证券交易所
        'HK': 'HK',  # 香港交易所
        'sh': 'SH',
        'sz': 'SZ',
        'hk': 'HK',
    }

    # 交易所代码推断规则（基于股票代码前缀）
    EXCHANGE_INFERENCE_RULES = {
        '6': 'SH',   # 600xxx, 601xxx, 603xxx, 605xxx = 上海
        '900': 'SH', # B股 900xxx = 上海
        '0': 'SZ',   # 000xxx, 001xxx, 002xxx, 003xxx = 深圳
        '300': 'SZ', # 300xxx = 创业板
        '301': 'SZ', # 301xxx = 创业板
    }

    def _infer_exchange_from_code(self, stock_code: str) -> str:
        """
        根据股票代码推断交易所

        Args:
            stock_code: 纯数字股票代码

        Returns:
            交易所代码 ('SH', 'SZ', 'HK') 或 None
        """
        if not stock_code or len(stock_code) < 4:
            return None

        # 港股通常是 4-5 位数字，且以 0 开头但不是 000xxx, 001xxx 等 A股格式
        # 简单判断：4-5位且不在 A股 范围内
        if len(stock_code) in [4, 5]:
            # 检查是否是 A股 格式
            if stock_code.startswith('000') or stock_code.startswith('001') or stock_code.startswith('002') or stock_code.startswith('003'):
                return 'SZ'
            elif stock_code.startswith('300') or stock_code.startswith('301'):
                return 'SZ'
            elif stock_code.startswith('600') or stock_code.startswith('601') or stock_code.startswith('603') or stock_code.startswith('605') or stock_code.startswith('688'):
                return 'SH'
            else:
                # 默认认为是港股
                return 'HK'

        # 6位 A股代码
        if len(stock_code) == 6:
            for prefix, exchange in self.EXCHANGE_INFERENCE_RULES.items():
                if stock_code.startswith(prefix):
                    return exchange

        return None

    # 支持的文件格式
    SUPPORTED_FORMATS = ['.xlsx', '.xls', '.csv']

    def __init__(self, db_manager: Optional[DuckDBManager] = None):
        """
        初始化导入器

        Args:
            db_manager: DuckDB 管理器实例，如果为 None 则创建新实例
        """
        self.db_manager = db_manager or DuckDBManager()

    def detect_interval(self, df: pd.DataFrame) -> str:
        """
        自动检测数据的时间周期

        检测规则：
        1. 检查 datetime 列是否包含时间部分（非 00:00:00）
           - 如果所有时间都是 00:00:00，则为日线
           - 否则计算时间间隔判断分钟线周期
        2. 根据时间间隔判断：5分钟、15分钟、30分钟、60分钟

        Args:
            df: 包含 datetime 列的 DataFrame

        Returns:
            时间周期字符串 ('1d', '5m', '15m', '30m', '60m')
        """
        if 'datetime' not in df.columns:
            raise ValueError("DataFrame must contain 'datetime' column")

        # 确保 datetime 是 datetime 类型
        if not pd.api.types.is_datetime64_any_dtype(df['datetime']):
            df['datetime'] = pd.to_datetime(df['datetime'])

        # 检查是否有非零时间
        has_time = (df['datetime'].dt.time != pd.Timestamp('00:00:00').time()).any()

        if not has_time:
            return '1d'  # 日线

        # 计算时间间隔
        df_sorted = df.sort_values('datetime').copy()
        time_diffs = df_sorted['datetime'].diff().dropna()

        if len(time_diffs) == 0:
            return '1d'  # 默认日线

        # 使用中位数而不是平均值，避免异常值影响
        median_diff = time_diffs.median()

        # 根据时间间隔判断周期（允许一定误差范围）
        if pd.Timedelta(minutes=3) <= median_diff <= pd.Timedelta(minutes=7):
            return '5m'
        elif pd.Timedelta(minutes=12) <= median_diff <= pd.Timedelta(minutes=18):
            return '15m'
        elif pd.Timedelta(minutes=25) <= median_diff <= pd.Timedelta(minutes=35):
            return '30m'
        elif pd.Timedelta(minutes=50) <= median_diff <= pd.Timedelta(minutes=70):
            return '60m'
        else:
            logger.warning(f"Unable to determine interval from median diff {median_diff}, defaulting to 1d")
            return '1d'  # 默认日线

    def _validate_columns(self, df: pd.DataFrame) -> None:
        """
        验证 DataFrame 是否包含必需的列

        注意：在列名标准化后验证，此时股票代码列可能是 'symbol'（临时名称）
         会在 _clean_data 中被拆分为 'stock_code' + 'exchange'

        Args:
            df: 要验证的 DataFrame

        Raises:
            ValueError: 如果缺少必需的列
        """
        # 检查必需列，允许 'symbol' 作为 'stock_code' 的临时替代
        required = list(self.REQUIRED_COLUMNS)
        if 'stock_code' in required and 'symbol' in df.columns:
            required.remove('stock_code')
            required.append('symbol')

        missing_columns = set(required) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

    def _parse_symbol(self, symbol: Any) -> tuple:
        """
        解析股票代码，分离 stock_code 和 exchange

        支持格式：
        - 000001.SZ -> ('000001', 'SZ')
        - 600000.SH -> ('600000', 'SH')
        - 00941.HK -> ('00941', 'HK')
        - 000001 -> ('000001', None)

        Args:
            symbol: 股票代码（可能包含交易所后缀）

        Returns:
            (stock_code, exchange) 元组
        """
        if pd.isna(symbol):
            return symbol, None

        symbol = str(symbol).strip()

        # 检查是否有后缀
        for suffix, exchange in self.EXCHANGE_SUFFIX_MAP.items():
            if symbol.endswith(f'.{suffix}'):
                stock_code = symbol[:-len(f'.{suffix}')]
                return stock_code, exchange

        # 没有后缀，返回原代码和 None
        return symbol, None

    def _clean_symbol(self, symbol: Any) -> str:
        """
        清洗股票代码（保留以兼容旧代码）

        Args:
            symbol: 股票代码

        Returns:
            清洗后的纯数字股票代码
        """
        stock_code, _ = self._parse_symbol(symbol)
        return stock_code

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        数据清洗

        - 将 symbol 拆分为 stock_code 和 exchange
        - 处理空值
        - 类型转换
        - 删除无效行

        Args:
            df: 原始数据

        Returns:
            清洗后的数据
        """
        df = df.copy()

        # 处理 symbol/stock_code 列（第一步：拆分）
        symbol_col = None
        if 'symbol' in df.columns:
            symbol_col = 'symbol'
        elif 'stock_code' in df.columns:
            symbol_col = 'stock_code'

        if symbol_col:
            # 解析 stock_code 和 exchange
            parsed = df[symbol_col].apply(self._parse_symbol)
            df['stock_code'] = parsed.apply(lambda x: x[0] if pd.notna(x[0]) else None)
            df['exchange'] = parsed.apply(lambda x: x[1] if pd.notna(x[1]) else None)

            # 对于没有 exchange 的记录，尝试推断
            def infer_exchange_if_missing(row):
                if pd.notna(row['exchange']) and row['exchange']:
                    return row['exchange']
                # exchange 为空，尝试推断
                return self._infer_exchange_from_code(row['stock_code'])

            df['exchange'] = df.apply(infer_exchange_if_missing, axis=1)

            # 删除旧的 symbol 列（如果与 stock_code 不同）
            if symbol_col == 'symbol':
                df = df.drop(columns=[symbol_col])

        # 转换 datetime
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])

        # 数值列转换（第二步：转换数值）
        for col in df.columns:
            if col in self.OPTIONAL_NUMERIC_COLUMNS or col in self.REQUIRED_COLUMNS:
                if col not in ['stock_code', 'exchange', 'datetime']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

        # 删除必需列为空的行（第三步：删除无效行）
        # 此时 stock_code 列已经存在了
        required = [col for col in self.REQUIRED_COLUMNS if col in df.columns]
        if required:
            df = df.dropna(subset=required)

        # 删除重复的 (stock_code, datetime) 组合
        if 'stock_code' in df.columns and 'datetime' in df.columns:
            df = df.drop_duplicates(subset=['stock_code', 'datetime'], keep='last')

        return df

    def _read_file(self, file_path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """
        读取数据文件（支持 Excel 和 CSV）

        Args:
            file_path: 文件路径
            sheet_name: Excel 标签页名称（仅对 Excel 文件有效）

        Returns:
            DataFrame

        Raises:
            ValueError: 如果文件格式不支持
        """
        file_path_obj = Path(file_path)
        suffix = file_path_obj.suffix.lower()

        if suffix not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported file format: {suffix}. Supported formats: {self.SUPPORTED_FORMATS}")

        try:
            if suffix == '.csv':
                # 尝试不同的编码
                for encoding in ['utf-8', 'gbk', 'gb2312']:
                    try:
                        # 使用 dtype 参数确保股票代码列作为字符串读取，保留前导零
                        possible_symbol_columns = ['symbol', '代码', 'code', '股票代码', '证券代码', 'ts_code']
                        dtype_dict = {col: str for col in possible_symbol_columns}
                        df = pd.read_csv(file_path, encoding=encoding, dtype=dtype_dict)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    raise ValueError("Unable to read CSV with common encodings (utf-8, gbk, gb2312)")
            else:
                # Excel 文件，支持指定标签页
                # 使用 dtype 参数确保股票代码列作为字符串读取，保留前导零
                # 尝试多种可能的列名
                possible_symbol_columns = ['symbol', '代码', 'code', '股票代码', '证券代码', 'ts_code']
                dtype_dict = {col: str for col in possible_symbol_columns}

                result = pd.read_excel(file_path, sheet_name=sheet_name, dtype=dtype_dict)

                # 如果返回的是字典（多标签页），取第一个标签页
                if isinstance(result, dict):
                    sheet_keys = list(result.keys())
                    if sheet_keys:
                        df = result[sheet_keys[0]]
                        logger.info(f"Excel file has multiple sheets, using first sheet: {sheet_keys[0]}")
                    else:
                        raise ValueError("Excel file has no sheets")
                else:
                    df = result

            logger.info(f"Successfully read {len(df)} rows from {file_path}")
            return df

        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            raise

    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        标准化列名

        支持常见的列名变体：
        - 代码/code/symbol -> symbol
        - 日期/date/time/时间 -> datetime
        - 开/开仓价/open -> open
        - 高/最高价/high -> high
        - 低/最低价/low -> low
        - 收/收盘价/close -> close
        - 量/vol/volume/成交量 -> volume
        - 额/amt/amount/成交额 -> amount

        Args:
            df: 原始 DataFrame

        Returns:
            列名标准化后的 DataFrame
        """
        column_mapping = {
            # 股票代码（会进一步拆分为 stock_code 和 exchange）
            '代码': 'symbol',
            'code': 'symbol',
            '股票代码': 'symbol',
            '证券代码': 'symbol',
            'ts_code': 'symbol',
            'stock_code': 'symbol',  # 兼容已有列名

            # 股票名称（会被忽略，不需要存储）
            '名称': '_name',
            '证券名称': '_name',
            '股票名称': '_name',

            # 日期时间
            '日期': 'datetime',
            'date': 'datetime',
            '时间': 'datetime',
            'time': 'datetime',
            'trade_date': 'datetime',
            '交易日期': 'datetime',
            '交易时间': 'datetime',

            # OHLC
            '开': 'open',
            '开仓价': 'open',
            '开盘价': 'open',
            'open_price': 'open',
            '开盘': 'open',

            '高': 'high',
            '最高价': 'high',
            'high_price': 'high',

            '低': 'low',
            '最低价': 'low',
            'low_price': 'low',

            '收': 'close',
            '收盘价': 'close',
            'close_price': 'close',

            # 成交量
            '量': 'volume',
            'vol': 'volume',
            '成交量': 'volume',

            # 成交额
            '额': 'amount',
            'amt': 'amount',
            '成交额': 'amount',

            # 其他常用列
            '涨跌幅': 'pct_chg',
            '涨跌': 'change',
            '涨跌额': 'change',
            '涨跌幅%': 'pct_chg',
            '换手率': 'turnover_rate_f',
        }

        # 转换为小写进行匹配
        rename_dict = {}
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if col_lower in column_mapping:
                rename_dict[col] = column_mapping[col_lower]
            elif col in column_mapping:
                rename_dict[col] = column_mapping[col]

        if rename_dict:
            df = df.rename(columns=rename_dict)
            logger.info(f"Renamed columns: {rename_dict}")

        return df

    def import_from_excel(
        self,
        file_path: str,
        table_name: Optional[str] = None,
        interval: Optional[str] = None,
        skip_validation: bool = False,
        sheet_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        从 Excel/CSV 文件导入数据

        Args:
            file_path: 文件路径
            table_name: 目标表名，如果为 None 则自动根据 interval 生成
            interval: 时间周期，如果为 None 则自动检测
            skip_validation: 是否跳过数据验证
            sheet_name: Excel 标签页名称，如果为 None 则使用第一个标签页

        Returns:
            导入结果字典，包含：
                - table_name: 目标表名
                - interval: 时间周期
                - rows_imported: 导入的行数
                - rows_skipped: 跳过的行数
                - symbols: 导入的股票代码列表
                - date_range: 日期范围
        """
        # 读取文件
        df = self._read_file(file_path, sheet_name=sheet_name)

        # 标准化列名
        df = self._normalize_column_names(df)

        # 验证必需列
        if not skip_validation:
            self._validate_columns(df)

        # 数据清洗
        df = self._clean_data(df)

        if len(df) == 0:
            raise ValueError("No valid data after cleaning")

        # 自动检测 interval
        detected_interval = self.detect_interval(df)

        # 使用指定的 interval 或检测到的 interval
        target_interval = interval or detected_interval

        # 确定表名
        if not table_name:
            table_name = f"bars_{target_interval}"

        # 创建表（如果不存在）
        self.db_manager.create_table(table_name, target_interval)

        # 导入数据
        rows_imported = self.db_manager.insert_dataframe(df, table_name)

        # 获取统计信息（使用 stock_code + exchange 组合成 ts_code）
        if 'stock_code' in df.columns:
            # 组合 ts_code 用于显示
            df['_ts_code'] = df.apply(
                lambda row: f"{row['stock_code']}.{row['exchange']}" if pd.notna(row['exchange']) else row['stock_code'],
                axis=1
            )
            symbols = df['_ts_code'].unique().tolist()
        else:
            symbols = []

        date_range = {
            'start': df['datetime'].min().isoformat(),
            'end': df['datetime'].max().isoformat()
        }

        result = {
            'table_name': table_name,
            'interval': target_interval,
            'detected_interval': detected_interval,
            'rows_imported': rows_imported,
            'rows_total': len(df),
            'symbols': symbols,
            'symbol_count': len(symbols),
            'date_range': date_range
        }

        logger.info(f"Import completed: {result}")
        return result

    def validate_file(self, file_path: str) -> Dict[str, Any]:
        """
        验证文件格式和数据

        Args:
            file_path: 文件路径

        Returns:
            验证结果字典
        """
        try:
            df = self._read_file(file_path)
            df = self._normalize_column_names(df)

            # 检查必需列
            missing_columns = set(self.REQUIRED_COLUMNS) - set(df.columns)

            # 尝试检测 interval
            try:
                interval = self.detect_interval(df)
            except Exception as e:
                interval = None

            return {
                'valid': len(missing_columns) == 0,
                'missing_columns': list(missing_columns),
                'rows': len(df),
                'columns': list(df.columns),
                'detected_interval': interval,
                'symbols': df['symbol'].unique().tolist() if 'symbol' in df.columns else [],
                'error': None
            }

        except Exception as e:
            return {
                'valid': False,
                'error': str(e),
                'missing_columns': [],
                'rows': 0,
                'columns': [],
                'detected_interval': None,
                'symbols': []
            }

    def get_import_summary(self, table_name: str) -> Dict[str, Any]:
        """
        获取导入数据的统计摘要

        Args:
            table_name: 表名

        Returns:
            统计摘要
        """
        # 使用 context manager 避免持久连接占用锁
        with self.db_manager.get_connection() as conn:
            try:
                # 获取行数
                row_count = self.db_manager.get_row_count(table_name)

                # 获取股票列表（更新查询以使用新的列名）
                symbols_df = conn.execute(
                    f"SELECT DISTINCT stock_code, exchange FROM {table_name} ORDER BY stock_code"
                ).fetchdf()

                # 组合成 ts_code 格式
                symbols = []
                if not symbols_df.empty:
                    for _, row in symbols_df.iterrows():
                        stock_code = row['stock_code']
                        exchange = row['exchange']
                        if pd.notna(exchange):
                            symbols.append(f"{stock_code}.{exchange}")
                        else:
                            symbols.append(stock_code)

                # 获取日期范围
                date_range = conn.execute(
                    f"SELECT MIN(datetime) as min_date, MAX(datetime) as max_date FROM {table_name}"
                ).fetchdf()
                date_range = date_range.iloc[0].to_dict()

                return {
                    'table_name': table_name,
                    'row_count': row_count,
                    'symbol_count': len(symbols),
                    'symbols': symbols[:20],  # 返回前20个
                    'date_range': date_range
                }

            except Exception as e:
                logger.error(f"Error getting import summary for {table_name}: {e}")
                return {
                    'table_name': table_name,
                    'error': str(e)
                }
