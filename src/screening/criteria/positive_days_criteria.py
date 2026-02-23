"""
涨幅天数筛选条件

筛选过去N个交易日中涨幅超过阈值的交易日天数超过一定比例的股票
"""
import logging
import time
import pandas as pd
import duckdb
from typing import Dict, Optional
from src.screening.base_criteria import BaseCriteria

logger = logging.getLogger(__name__)


class PositiveDaysCriteria(BaseCriteria):
    """
    涨幅天数筛选条件

    筛选指定时间段内涨幅超过阈值的交易日天数超过一定比例的股票

    涨幅计算: pct_chg > threshold
    """

    def __init__(self, period: int = None, threshold: float = None, min_positive_ratio: float = None,
                 db_path: str = None, start_date: str = None, end_date: str = None):
        """
        Args:
            period: 交易日数量 (如 20) - 与 start_date/end_date 二选一
            threshold: 涨幅阈值百分比 (如 2.0 表示 2.0%)
            min_positive_ratio: 最小正涨幅天数比例 (如 0.5 表示至少50%的交易日涨幅超过阈值)
            db_path: 数据库路径（SQLite，保留兼容）
            start_date: 开始日期 YYYYMMDD (可选，与 period 二选一)
            end_date: 结束日期 YYYYMMDD (可选，与 period 二选一)
        """
        self.period = period
        self.threshold = threshold
        self.min_positive_ratio = min_positive_ratio
        self.start_date = start_date
        self.end_date = end_date
        self.db_path = db_path

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
        # - 需要计数和比例计算: 5
        return 15

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        应用筛选条件

        策略：从当前 df 提取股票列表，查询 DuckDB 历史数据计算涨幅天数比例
        """
        logger.debug(f"[PositiveDaysCriteria] Starting filter, period: {self.period}, threshold: {self.threshold}, "
                    f"min_positive_ratio: {self.min_positive_ratio}, input size: {len(df)}")

        if df.empty:
            return df

        if 'symbol' not in df.columns:
            logger.warning(f"[PositiveDaysCriteria] 'symbol' column not found in DataFrame")
            return df

        # 获取股票列表
        symbols = df['symbol'].unique().tolist()
        logger.debug(f"[PositiveDaysCriteria] Extracted {len(symbols)} unique symbols from input")

        if not symbols:
            return pd.DataFrame()

        # 使用 DuckDB 查询
        if not self.duckdb_path:
            logger.warning(f"[PositiveDaysCriteria] DuckDB path not configured")
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
                        logger.debug(f"[PositiveDaysCriteria] DuckDB locked, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(retry_delay)
                else:
                    logger.error(f"[PositiveDaysCriteria] Failed to connect to DuckDB: {e}")
                    break

        if conn is None:
            logger.error(f"[PositiveDaysCriteria] Failed to connect to DuckDB after {max_retries} attempts: {last_error}")
            return df

        try:
            # 区分A股和港股
            data_sources = df.groupby('symbol')['data_source'].first().to_dict() if 'data_source' in df.columns else {}
            a_symbols = [s for s in symbols if data_sources.get(s) == 'A股']
            hk_symbols = [s for s in symbols if data_sources.get(s) == '港股']

            all_qualified = []

            # 查询A股数据
            if a_symbols:
                qualified = self._query_duckdb(conn, a_symbols, 'bars_a_1d')
                all_qualified.extend(qualified)
                logger.debug(f"[PositiveDaysCriteria] A股: {len(a_symbols)} symbols, {len(qualified)} qualified")

            # 查询港股数据
            if hk_symbols:
                qualified = self._query_duckdb(conn, hk_symbols, 'bars_1d')
                all_qualified.extend(qualified)
                logger.debug(f"[PositiveDaysCriteria] 港股: {len(hk_symbols)} symbols, {len(qualified)} qualified")

            if not all_qualified:
                logger.info(f"[PositiveDaysCriteria] No symbols meet the positive days condition")
                return pd.DataFrame()

            # 返回原始 df 中符合条件的数据
            result = df[df['symbol'].isin(all_qualified)].copy()
            logger.info(f"[PositiveDaysCriteria] Filter completed, input: {len(df)}, qualified: {len(all_qualified)}, output: {len(result)}")
            return result

        finally:
            conn.close()

    def _query_duckdb(self, conn, symbols: list, table_name: str) -> list:
        """从 DuckDB 查询指定表的涨幅天数数据，返回符合条件的股票代码列表"""
        if not symbols:
            return []

        symbols_placeholder = ', '.join([f"'{s}'" for s in symbols])

        if self.start_date and self.end_date:
            # 时间范围模式
            start_date = self.format_date_for_db(self.start_date)
            end_date = self.format_date_for_db(self.end_date)

            query = f"""
            WITH bar_count AS (
                SELECT
                    stock_code as symbol,
                    COUNT(*) as total_days,
                    SUM(CASE WHEN pct_chg > {self.threshold} THEN 1 ELSE 0 END) as positive_days
                FROM {table_name}
                WHERE datetime >= '{start_date}'
                  AND datetime <= '{end_date}'
                  AND stock_code IN ({symbols_placeholder})
                  AND pct_chg IS NOT NULL
                GROUP BY stock_code
                HAVING COUNT(*) >= 1
            )
            SELECT symbol
            FROM bar_count
            WHERE CAST(positive_days AS FLOAT) / total_days >= {self.min_positive_ratio}
            """
        else:
            # period 模式
            if not self.period:
                logger.warning(f"[PositiveDaysCriteria] Neither period nor date range specified")
                return []

            latest = conn.execute(f"SELECT MAX(datetime) FROM {table_name}").fetchone()
            trade_date = latest[0] if latest and latest[0] else None

            if not trade_date:
                logger.warning(f"[PositiveDaysCriteria] No trade_date found in {table_name}")
                return []

            trade_date_str = self.format_date_for_db(trade_date)
            min_positive_days = int(self.period * self.min_positive_ratio) + 1

            query = f"""
            WITH recent_bars AS (
                SELECT
                    stock_code as symbol,
                    datetime,
                    pct_chg,
                    ROW_NUMBER() OVER (
                        PARTITION BY stock_code
                        ORDER BY datetime DESC
                    ) as rn
                FROM {table_name}
                WHERE stock_code IN ({symbols_placeholder})
                  AND datetime <= '{trade_date_str}'
                  AND pct_chg IS NOT NULL
            ),
            positive_count AS (
                SELECT
                    symbol,
                    SUM(CASE WHEN pct_chg > {self.threshold} THEN 1 ELSE 0 END) as positive_days,
                    COUNT(*) as actual_days
                FROM recent_bars
                WHERE rn <= {self.period}
                GROUP BY symbol
                HAVING COUNT(*) >= {self.period}
            )
            SELECT symbol
            FROM positive_count
            WHERE positive_days >= {min_positive_days}
            """

        try:
            result_df = conn.execute(query).fetchdf()
            return result_df['symbol'].tolist() if not result_df.empty else []
        except Exception as e:
            logger.error(f"[PositiveDaysCriteria] DuckDB query error: {e}")
            return []

    def to_config(self) -> Dict:
        """导出为配置字典（不包含 db_path）"""
        config = {
            'type': 'PositiveDays',
            'threshold': self.threshold,
            'min_positive_ratio': self.min_positive_ratio
        }
        if self.period:
            config['period'] = self.period
        if self.start_date:
            config['start_date'] = self.start_date
        if self.end_date:
            config['end_date'] = self.end_date
        return config

    @classmethod
    def from_config(cls, config: Dict, db_path: str):
        """从配置创建实例"""
        return cls(
            period=config.get('period'),
            threshold=config['threshold'],
            min_positive_ratio=config['min_positive_ratio'],
            db_path=db_path,
            start_date=config.get('start_date'),
            end_date=config.get('end_date')
        )