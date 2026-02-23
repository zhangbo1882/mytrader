"""
换手率筛选条件
"""
import logging
import time
import pandas as pd
import duckdb
from typing import Dict, Optional
from src.screening.base_criteria import BaseCriteria

logger = logging.getLogger(__name__)


class AverageTurnoverCriteria(BaseCriteria):
    """
    平均换手率筛选条件

    计算股票在指定时间范围内的平均换手率，然后应用指定的筛选条件

    支持的条件类型：
    - Range: 换手率在指定范围内 (min_val <= avg_turnover <= max_val)
    - GreaterThan: 换手率大于指定阈值 (avg_turnover > threshold)
    - LessThan: 换手率小于指定阈值 (avg_turnover < max_threshold)

    换手率计算: 直接使用 turnover_rate_f 字段
    """

    def __init__(
        self,
        period: int = None,
        condition_type: str = 'GreaterThan',
        db_path: str = None,
        min_val: float = None,
        max_val: float = None,
        threshold: float = None,
        max_threshold: float = None,
        start_date: str = None,
        end_date: str = None
    ):
        """
        Args:
            period: 交易日数量 (如 5, 10, 20) - 与 start_date/end_date 二选一
            condition_type: 条件类型 ('Range', 'GreaterThan', 'LessThan')
            db_path: 数据库路径
            min_val: 最小值（Range 条件使用）
            max_val: 最大值（Range 条件使用）
            threshold: 阈值（GreaterThan 条件使用）
            max_threshold: 最大阈值（LessThan 条件使用）
            start_date: 开始日期 YYYYMMDD (可选，与 period 二选一)
            end_date: 结束日期 YYYYMMDD (可选，与 period 二选一)
        """
        self.period = period
        self.start_date = start_date
        self.end_date = end_date
        self.condition_type = condition_type
        self.db_path = db_path
        self.min_val = min_val
        self.max_val = max_val
        self.threshold = threshold
        self.max_threshold = max_threshold

        # DuckDB 路径
        try:
            from config.settings import DUCKDB_PATH
            self.duckdb_path = str(DUCKDB_PATH)
        except Exception:
            self.duckdb_path = None

    @property
    def cost(self) -> int:
        # 成本估算: 15 (中等偏高)
        # - 需要加载历史数据: 5
        # - 需要分组聚合计算: 5
        # - 需要计算换手率: 5
        return 15

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        应用筛选条件

        策略：从当前 df 提取股票列表，查询 DuckDB 历史数据计算平均换手率，然后应用筛选条件
        """
        logger.debug(f"[AverageTurnoverCriteria] Starting filter, condition_type: {self.condition_type}, period: {self.period}, start_date: {self.start_date}, end_date: {self.end_date}, input size: {len(df)}")

        if df.empty:
            logger.warning(f"[AverageTurnoverCriteria] Input DataFrame is empty")
            return df

        if 'symbol' not in df.columns:
            logger.warning(f"[AverageTurnoverCriteria] 'symbol' column not found in DataFrame")
            return df

        # 获取股票列表
        symbols = df['symbol'].unique().tolist()
        logger.debug(f"[AverageTurnoverCriteria] Extracted {len(symbols)} unique symbols from input")

        if not symbols:
            logger.warning(f"[AverageTurnoverCriteria] No symbols found in DataFrame")
            return pd.DataFrame()

        # 区分A股和港股
        data_sources = df.groupby('symbol')['data_source'].first().to_dict() if 'data_source' in df.columns else {}
        a_symbols = [s for s in symbols if data_sources.get(s) == 'A股']
        hk_symbols = [s for s in symbols if data_sources.get(s) == '港股']

        # 使用 DuckDB 查询
        if not self.duckdb_path:
            logger.warning(f"[AverageTurnoverCriteria] DuckDB path not configured")
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
                        logger.debug(f"[AverageTurnoverCriteria] DuckDB locked, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(retry_delay)
                else:
                    logger.error(f"[AverageTurnoverCriteria] Failed to connect to DuckDB: {e}")
                    break

        if conn is None:
            logger.error(f"[AverageTurnoverCriteria] Failed to connect to DuckDB after {max_retries} attempts: {last_error}")
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
                logger.info(f"[AverageTurnoverCriteria] No symbols with turnover data found")
                return pd.DataFrame()

            result_df = pd.concat(all_results, ignore_index=True)

        finally:
            conn.close()

        logger.debug(f"[AverageTurnoverCriteria] Query returned {len(result_df)} symbols with turnover data")

        if result_df.empty:
            logger.info(f"[AverageTurnoverCriteria] No symbols with turnover data found")
            return pd.DataFrame()

        # 应用筛选条件
        before_filter = len(result_df)
        if self.condition_type == 'Range':
            result_df = result_df[
                (result_df['avg_turnover'] >= self.min_val) &
                (result_df['avg_turnover'] <= self.max_val)
            ]
            logger.debug(f"[AverageTurnoverCriteria] Applied Range filter: {self.min_val} <= turnover <= {self.max_val}, filtered from {before_filter} to {len(result_df)}")
        elif self.condition_type == 'GreaterThan':
            result_df = result_df[result_df['avg_turnover'] > self.threshold]
            logger.debug(f"[AverageTurnoverCriteria] Applied GreaterThan filter: turnover > {self.threshold}, filtered from {before_filter} to {len(result_df)}")
        elif self.condition_type == 'LessThan':
            result_df = result_df[result_df['avg_turnover'] < self.max_threshold]
            logger.debug(f"[AverageTurnoverCriteria] Applied LessThan filter: turnover < {self.max_threshold}, filtered from {before_filter} to {len(result_df)}")

        if result_df.empty:
            logger.info(f"[AverageTurnoverCriteria] No symbols meet the turnover condition")
            return pd.DataFrame()

        # 筛选符合条件的股票
        qualified_symbols = result_df['symbol'].tolist()
        result = df[df['symbol'].isin(qualified_symbols)].copy()
        logger.info(f"[AverageTurnoverCriteria] Filter completed, input: {len(df)}, with turnover data: {before_filter}, qualified: {len(qualified_symbols)}, output: {len(result)}")
        return result

    def _query_duckdb(self, conn, symbols: list, table_name: str) -> pd.DataFrame:
        """从 DuckDB 查询指定表的换手率数据，返回带有平均换手率的DataFrame"""
        if not symbols:
            return pd.DataFrame()

        symbols_placeholder = ', '.join([f"'{s}'" for s in symbols])

        if self.start_date and self.end_date:
            # 时间范围模式
            start_date = self.format_date_for_db(self.start_date)
            end_date = self.format_date_for_db(self.end_date)
            logger.debug(f"[AverageTurnoverCriteria] Using date range mode: {start_date} to {end_date}")

            query = f"""
            SELECT
                stock_code as symbol,
                AVG(turnover_rate_f) as avg_turnover,
                COUNT(*) as actual_days
            FROM {table_name}
            WHERE datetime >= '{start_date}'
              AND datetime <= '{end_date}'
              AND stock_code IN ({symbols_placeholder})
              AND turnover_rate_f IS NOT NULL
            GROUP BY stock_code
            HAVING COUNT(*) >= 1
            """
        else:
            # period 模式
            if not self.period:
                logger.warning(f"[AverageTurnoverCriteria] Neither period nor date range specified")
                return pd.DataFrame()

            latest = conn.execute(f"SELECT MAX(datetime) FROM {table_name}").fetchone()
            trade_date = latest[0] if latest and latest[0] else None

            if not trade_date:
                logger.warning(f"[AverageTurnoverCriteria] No trade_date found in {table_name}")
                return pd.DataFrame()

            trade_date_str = self.format_date_for_db(trade_date)
            logger.debug(f"[AverageTurnoverCriteria] Using period mode: last {self.period} days from {trade_date_str}")

            query = f"""
            WITH recent_bars AS (
                SELECT
                    stock_code as symbol,
                    turnover_rate_f,
                    ROW_NUMBER() OVER (
                        PARTITION BY stock_code
                        ORDER BY datetime DESC
                    ) as rn
                FROM {table_name}
                WHERE stock_code IN ({symbols_placeholder})
                  AND datetime <= '{trade_date_str}'
                  AND turnover_rate_f IS NOT NULL
            )
            SELECT
                symbol,
                AVG(turnover_rate_f) as avg_turnover,
                COUNT(*) as actual_days
            FROM recent_bars
            WHERE rn <= {self.period}
            GROUP BY symbol
            HAVING COUNT(*) >= 1
            """

        try:
            result_df = conn.execute(query).fetchdf()
            return result_df
        except Exception as e:
            logger.error(f"[AverageTurnoverCriteria] DuckDB query error: {e}")
            return pd.DataFrame()

    def to_config(self) -> Dict:
        """导出为配置字典（不包含 db_path）"""
        config = {
            'type': 'TurnoverColumn',
            'condition_type': self.condition_type
        }
        if self.period:
            config['period'] = self.period
        if self.start_date:
            config['start_date'] = self.start_date
        if self.end_date:
            config['end_date'] = self.end_date
        if self.min_val is not None:
            config['min_val'] = self.min_val
        if self.max_val is not None:
            config['max_val'] = self.max_val
        if self.threshold is not None:
            config['threshold'] = self.threshold
        if self.max_threshold is not None:
            config['max_threshold'] = self.max_threshold
        return config

    @classmethod
    def from_config(cls, config: Dict, db_path: str):
        """从配置创建实例"""
        # 推断条件类型
        condition_type = config.get('condition_type')
        if not condition_type:
            if config.get('min_val') is not None and config.get('max_val') is not None:
                condition_type = 'Range'
            elif config.get('max_threshold') is not None:
                condition_type = 'LessThan'
            else:
                condition_type = 'GreaterThan'

        return cls(
            period=config.get('period'),
            condition_type=condition_type,
            db_path=db_path,
            min_val=config.get('min_val'),
            max_val=config.get('max_val'),
            threshold=config.get('threshold'),
            max_threshold=config.get('max_threshold'),
            start_date=config.get('start_date'),
            end_date=config.get('end_date')
        )
