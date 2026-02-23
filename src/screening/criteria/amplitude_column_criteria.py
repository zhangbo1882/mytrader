"""
振幅列筛选条件

支持将平均振幅作为可筛选的列字段，可以与其他条件类型（Range/GreaterThan/LessThan）结合使用
"""
import logging
import time
import pandas as pd
import duckdb
from sqlalchemy import create_engine, text
from typing import Dict, Union, Optional
from datetime import datetime
from src.screening.base_criteria import BaseCriteria

logger = logging.getLogger(__name__)


class AmplitudeColumnCriteria(BaseCriteria):
    """
    平均振幅列筛选条件

    计算股票在指定时间范围内的平均振幅，然后应用指定的筛选条件

    支持的条件类型：
    - Range: 振幅在指定范围内 (min_val <= avg_amplitude <= max_val)
    - GreaterThan: 振幅大于指定阈值 (avg_amplitude > threshold)
    - LessThan: 振幅小于指定阈值 (avg_amplitude < threshold)

    振幅计算: (high - low) / pre_close * 100

    支持两种时间范围模式：
    - period: 最近N个交易日
    - start_date + end_date: 指定日期范围 (YYYYMMDD格式)
    """

    def __init__(
        self,
        period: int = None,
        condition_type: str = 'Range',
        db_path: str = None,
        min_val: float = None,
        max_val: float = None,
        threshold: float = None,
        start_date: str = None,
        end_date: str = None
    ):
        """
        Args:
            period: 交易日数量 (如 5, 10, 20)，与 start_date/end_date 二选一
            condition_type: 条件类型 ('Range', 'GreaterThan', 'LessThan')
            db_path: 数据库路径（SQLite，保留兼容）
            min_val: 最小值（Range 条件使用）
            max_val: 最大值（Range 条件使用）
            threshold: 阈值（GreaterThan/LessThan 条件使用）
            start_date: 开始日期 (YYYYMMDD格式)，与 period 二选一
            end_date: 结束日期 (YYYYMMDD格式)，与 period 二选一
        """
        if period is None and (start_date is None or end_date is None):
            raise ValueError("Must specify either 'period' or both 'start_date' and 'end_date'")

        self.period = period
        self.start_date = start_date
        self.end_date = end_date
        self.condition_type = condition_type
        self.db_path = db_path
        self.min_val = min_val
        self.max_val = max_val
        self.threshold = threshold

        # DuckDB 路径
        try:
            from config.settings import DUCKDB_PATH
            self.duckdb_path = str(DUCKDB_PATH)
        except Exception:
            self.duckdb_path = None

        # 验证参数
        valid_condition_types = ['Range', 'GreaterThan', 'LessThan']
        if condition_type not in valid_condition_types:
            raise ValueError(f"Invalid condition_type: {condition_type}. Must be one of {valid_condition_types}")

        if condition_type == 'Range':
            if min_val is None or max_val is None:
                raise ValueError("Range condition requires both min_val and max_val")
        elif condition_type in ['GreaterThan', 'LessThan']:
            if threshold is None:
                raise ValueError(f"{condition_type} condition requires threshold")


    @property
    def cost(self) -> int:
        # 成本估算: 15 (中等偏高)
        # - 需要加载历史数据: 5
        # - 需要分组聚合计算: 5
        # - 需要计算振幅: 5
        return 15

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        应用筛选条件

        策略：从当前 df 提取股票列表，查询 DuckDB 历史数据计算平均振幅，然后应用筛选条件
        """
        logger.debug(f"[AmplitudeColumnCriteria] Starting filter, condition_type: {self.condition_type}, period: {self.period}, start_date: {self.start_date}, end_date: {self.end_date}, input size: {len(df)}")

        if df.empty:
            logger.warning(f"[AmplitudeColumnCriteria] Input DataFrame is empty")
            return df

        if 'symbol' not in df.columns:
            logger.warning(f"[AmplitudeColumnCriteria] 'symbol' column not found in DataFrame")
            return df

        # 获取股票列表
        symbols = df['symbol'].unique().tolist()
        logger.debug(f"[AmplitudeColumnCriteria] Extracted {len(symbols)} unique symbols from input")

        if not symbols:
            logger.warning(f"[AmplitudeColumnCriteria] No symbols found in DataFrame")
            return pd.DataFrame()

        # 区分A股和港股
        data_sources = df.groupby('symbol')['data_source'].first().to_dict() if 'data_source' in df.columns else {}
        a_symbols = [s for s in symbols if data_sources.get(s) == 'A股']
        hk_symbols = [s for s in symbols if data_sources.get(s) == '港股']

        # 使用 DuckDB 查询
        if not self.duckdb_path:
            logger.warning(f"[AmplitudeColumnCriteria] DuckDB path not configured")
            return df

        # 带重试的连接
        conn = None
        max_retries = 5
        retry_delay = 0.3
        last_error = None

        for attempt in range(max_retries):
            try:
                conn = duckdb.connect(self.duckdb_path, read_only=True)
                break
            except Exception as e:
                last_error = e
                if "lock" in str(e).lower() or "IO Error" in str(e):
                    if attempt < max_retries - 1:
                        logger.debug(f"[AmplitudeColumnCriteria] DuckDB locked, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(retry_delay)
                else:
                    logger.error(f"[AmplitudeColumnCriteria] Failed to connect to DuckDB: {e}")
                    break

        if conn is None:
            logger.error(f"[AmplitudeColumnCriteria] Failed to connect to DuckDB after {max_retries} attempts: {last_error}")
            return df

        try:
            all_results = []

            # 查询A股数据
            if a_symbols:
                result_df = self._query_duckdb(conn, a_symbols, 'bars_a_1d')
                if not result_df.empty:
                    all_results.append(result_df)

            # 查询港股数据
            if hk_symbols:
                result_df = self._query_duckdb(conn, hk_symbols, 'bars_1d')
                if not result_df.empty:
                    all_results.append(result_df)

            if not all_results:
                logger.info(f"[AmplitudeColumnCriteria] No symbols with amplitude data found")
                return pd.DataFrame()

            result_df = pd.concat(all_results, ignore_index=True)

        finally:
            conn.close()

        logger.debug(f"[AmplitudeColumnCriteria] Query returned {len(result_df)} symbols with amplitude data")

        if result_df.empty:
            logger.info(f"[AmplitudeColumnCriteria] No symbols with amplitude data found")
            return pd.DataFrame()

        # 应用筛选条件
        before_filter = len(result_df)
        if self.condition_type == 'Range':
            result_df = result_df[
                (result_df['avg_amplitude'] >= self.min_val) &
                (result_df['avg_amplitude'] <= self.max_val)
            ]
            logger.debug(f"[AmplitudeColumnCriteria] Applied Range filter: {self.min_val} <= amplitude <= {self.max_val}, filtered from {before_filter} to {len(result_df)}")
        elif self.condition_type == 'GreaterThan':
            result_df = result_df[result_df['avg_amplitude'] > self.threshold]
            logger.debug(f"[AmplitudeColumnCriteria] Applied GreaterThan filter: amplitude > {self.threshold}, filtered from {before_filter} to {len(result_df)}")
        elif self.condition_type == 'LessThan':
            result_df = result_df[result_df['avg_amplitude'] < self.threshold]
            logger.debug(f"[AmplitudeColumnCriteria] Applied LessThan filter: amplitude < {self.threshold}, filtered from {before_filter} to {len(result_df)}")

        if result_df.empty:
            logger.info(f"[AmplitudeColumnCriteria] No symbols meet the amplitude condition")
            return pd.DataFrame()

        # 筛选符合条件的股票
        qualified_symbols = result_df['symbol'].tolist()
        result = df[df['symbol'].isin(qualified_symbols)].copy()
        logger.info(f"[AmplitudeColumnCriteria] Filter completed, input: {len(df)}, with amplitude data: {before_filter}, qualified: {len(qualified_symbols)}, output: {len(result)}")
        return result

    def _query_duckdb(self, conn, symbols: list, table_name: str) -> pd.DataFrame:
        """从 DuckDB 查询指定表的振幅数据"""
        if not symbols:
            return pd.DataFrame()

        symbols_placeholder = ', '.join([f"'{s}'" for s in symbols])

        if self.period:
            # 使用 period 模式：获取最近N个交易日
            latest = conn.execute(f"SELECT MAX(datetime) FROM {table_name}").fetchone()
            trade_date = latest[0] if latest and latest[0] else None

            if not trade_date:
                logger.warning(f"[AmplitudeColumnCriteria] No trade_date found in {table_name}")
                return pd.DataFrame()

            trade_date_str = self.format_date_for_db(trade_date)
            logger.debug(f"[AmplitudeColumnCriteria] Using period mode: last {self.period} days from {trade_date_str}")

            query = f"""
            WITH recent_bars AS (
                SELECT
                    stock_code as symbol,
                    datetime,
                    high,
                    low,
                    pre_close,
                    ROW_NUMBER() OVER (
                        PARTITION BY stock_code
                        ORDER BY datetime DESC
                    ) as rn
                FROM {table_name}
                WHERE stock_code IN ({symbols_placeholder})
                  AND datetime <= '{trade_date_str}'
                  AND pre_close > 0
            ),
            amplitude_calc AS (
                SELECT
                    symbol,
                    AVG((high - low) / pre_close * 100) as avg_amplitude,
                    COUNT(*) as actual_days
                FROM recent_bars
                WHERE rn <= {self.period}
                GROUP BY symbol
                HAVING COUNT(*) >= 1
            )
            SELECT symbol, avg_amplitude
            FROM amplitude_calc
            """
        else:
            # 使用日期范围模式
            start_date = self.format_date_for_db(self.start_date)
            end_date = self.format_date_for_db(self.end_date)
            logger.debug(f"[AmplitudeColumnCriteria] Using date range mode: {start_date} to {end_date}")

            query = f"""
            WITH bars_in_range AS (
                SELECT
                    stock_code as symbol,
                    datetime,
                    high,
                    low,
                    pre_close
                FROM {table_name}
                WHERE datetime >= '{start_date}'
                  AND datetime <= '{end_date}'
                  AND stock_code IN ({symbols_placeholder})
                  AND pre_close > 0
            ),
            amplitude_calc AS (
                SELECT
                    symbol,
                    AVG((high - low) / pre_close * 100) as avg_amplitude,
                    COUNT(*) as actual_days
                FROM bars_in_range
                GROUP BY symbol
                HAVING COUNT(*) >= 1
            )
            SELECT symbol, avg_amplitude
            FROM amplitude_calc
            """

        try:
            result_df = conn.execute(query).fetchdf()
            return result_df
        except Exception as e:
            logger.error(f"[AmplitudeColumnCriteria] DuckDB query error: {e}")
            return pd.DataFrame()

    def to_config(self) -> Dict:
        """导出为配置字典（不包含 db_path）"""
        config = {
            'type': 'AmplitudeColumn',
            'column': 'avg_amplitude',
            'condition_type': self.condition_type
        }

        # 添加时间范围参数
        if self.period:
            config['period'] = self.period
        if self.start_date:
            config['start_date'] = self.start_date
        if self.end_date:
            config['end_date'] = self.end_date

        # 添加条件参数
        if self.condition_type == 'Range':
            config['min_val'] = self.min_val
            config['max_val'] = self.max_val
        elif self.condition_type in ['GreaterThan', 'LessThan']:
            config['threshold'] = self.threshold

        return config

    @classmethod
    def from_config(cls, config: Dict, db_path: str):
        """从配置创建实例"""
        # 自动检测条件类型
        if 'min_val' in config and 'max_val' in config:
            condition_type = 'Range'
        elif 'threshold' in config:
            # 需要进一步判断是 GreaterThan 还是 LessThan
            # 这里默认使用 GreaterThan，因为前端发送的 threshold 一般用于大于条件
            condition_type = 'GreaterThan'
        else:
            condition_type = config.get('condition_type', 'Range')

        return cls(
            period=config.get('period'),
            condition_type=condition_type,
            db_path=db_path,
            min_val=config.get('min_val'),
            max_val=config.get('max_val'),
            threshold=config.get('threshold'),
            start_date=config.get('start_date'),
            end_date=config.get('end_date')
        )
