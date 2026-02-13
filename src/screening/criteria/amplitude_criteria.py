"""
振幅筛选条件
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict
from src.screening.base_criteria import BaseCriteria

logger = logging.getLogger(__name__)


class AverageAmplitudeCriteria(BaseCriteria):
    """
    平均振幅筛选条件

    筛选指定时间段内平均振幅超过阈值的股票

    振幅计算: (high - low) / pre_close * 100
    """

    def __init__(self, period: int = None, threshold: float = None, db_path: str = None,
                 start_date: str = None, end_date: str = None):
        """
        Args:
            period: 交易日数量 (如 5, 10, 20) - 与 start_date/end_date 二选一
            threshold: 平均振幅阈值百分比 (如 3.5 表示 3.5%)
            db_path: 数据库路径
            start_date: 开始日期 YYYYMMDD (可选，与 period 二选一)
            end_date: 结束日期 YYYYMMDD (可选，与 period 二选一)
        """
        self.period = period
        self.threshold = threshold
        self.start_date = start_date
        self.end_date = end_date
        self.db_path = db_path
        self.engine = create_engine(f'sqlite:///{db_path}', echo=False)

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

        策略：从当前 df 提取股票列表，查询历史数据计算平均振幅
        支持两种模式：
        1. period 模式：取最近 N 个交易日
        2. 时间范围模式：指定 start_date 和 end_date
        """
        logger.debug(f"[AverageAmplitudeCriteria] Starting filter, period: {self.period}, threshold: {self.threshold}, start_date: {self.start_date}, end_date: {self.end_date}, input size: {len(df)}")

        if df.empty:
            logger.warning(f"[AverageAmplitudeCriteria] Input DataFrame is empty")
            return df

        if 'symbol' not in df.columns:
            logger.warning(f"[AverageAmplitudeCriteria] 'symbol' column not found in DataFrame")
            return df

        # 获取股票列表
        symbols = df['symbol'].unique().tolist()
        logger.debug(f"[AverageAmplitudeCriteria] Extracted {len(symbols)} unique symbols from input")

        if not symbols:
            logger.warning(f"[AverageAmplitudeCriteria] No symbols found in DataFrame")
            return pd.DataFrame()

        # 构建 IN 子句的占位符字符串 (用于 SQLite)
        symbols_placeholder = ', '.join([f':symbol_{i}' for i in range(len(symbols))])

        # 根据模式构建不同的查询
        if self.start_date and self.end_date:
            logger.debug(f"[AverageAmplitudeCriteria] Using date range mode: {self.start_date} to {self.end_date}")
            # 时间范围模式：查询符合条件的所有股票，不限于传入 df 中的股票
            query = text(f"""
            WITH amplitude_calc AS (
                SELECT
                    symbol,
                    AVG((high - low) / pre_close * 100) as avg_amplitude,
                    COUNT(*) as actual_days
                FROM bars
                WHERE datetime >= :start_date
                  AND datetime <= :end_date
                  AND interval = '1d'
                  AND pre_close > 0
                GROUP BY symbol
                HAVING COUNT(*) >= 1
            )
            SELECT DISTINCT symbol
            FROM amplitude_calc
            WHERE avg_amplitude > :threshold
            """)
            params = {
                'start_date': self.format_date_for_db(self.start_date),
                'end_date': self.format_date_for_db(self.end_date),
                'threshold': self.threshold
            }

            logger.debug(f"[AverageAmplitudeCriteria] Executing date range query")
            result_df = pd.read_sql_query(query, self.engine, params=params)
            logger.debug(f"[AverageAmplitudeCriteria] Date range query returned {len(result_df)} symbols meeting amplitude threshold")

            if result_df.empty:
                logger.info(f"[AverageAmplitudeCriteria] No symbols found with amplitude > {self.threshold}% in date range")
                return pd.DataFrame()

            # 返回包含所有符合条件股票的 DataFrame（从原始 df 中查找）
            qualified_symbols = result_df['symbol'].tolist()
            available_symbols = df['symbol'].unique().tolist()
            final_symbols = [s for s in qualified_symbols if s in available_symbols]
            logger.debug(f"[AverageAmplitudeCriteria] {len(qualified_symbols)} symbols meet amplitude threshold, {len(final_symbols)} are in input DataFrame")

            if not final_symbols:
                logger.info(f"[AverageAmplitudeCriteria] No matching symbols found in input DataFrame")
                return pd.DataFrame()

            # 返回原始 df 中符合条件的数据
            result = df[df['symbol'].isin(final_symbols)].copy()
            logger.info(f"[AverageAmplitudeCriteria] Date range filter completed, input: {len(df)}, qualified: {len(qualified_symbols)}, in input: {len(final_symbols)}, output: {len(result)}")
            return result
        else:
            # period 模式（默认）
            if not self.period:
                logger.warning(f"[AverageAmplitudeCriteria] Neither period nor date range specified")
                return df

            if 'trade_date' not in df.columns:
                logger.warning(f"[AverageAmplitudeCriteria] 'trade_date' column not found in DataFrame")
                return df

            trade_date = df.iloc[0].get('trade_date')
            logger.debug(f"[AverageAmplitudeCriteria] Using period mode: last {self.period} days from {trade_date}")

            query = text(f"""
            WITH recent_bars AS (
                SELECT
                    symbol,
                    datetime,
                    high,
                    low,
                    pre_close,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol
                        ORDER BY datetime DESC
                    ) as rn
                FROM bars
                WHERE symbol IN ({symbols_placeholder})
                  AND datetime <= :trade_date
                  AND interval = '1d'
                  AND pre_close > 0
            ),
            amplitude_calc AS (
                SELECT
                    symbol,
                    AVG((high - low) / pre_close * 100) as avg_amplitude,
                    COUNT(*) as actual_days
                FROM recent_bars
                WHERE rn <= :period
                GROUP BY symbol
                HAVING COUNT(*) >= :min_days
            )
            SELECT symbol
            FROM amplitude_calc
            WHERE avg_amplitude > :threshold
            """)
            params = {
                'trade_date': self.format_date_for_db(trade_date),
                'period': self.period,
                'min_days': self.period,
                'threshold': self.threshold
            }

            # 添加每个股票代码作为独立参数
            for i, symbol in enumerate(symbols):
                params[f'symbol_{i}'] = symbol

            logger.debug(f"[AverageAmplitudeCriteria] Executing period mode query for {len(symbols)} symbols")
            result_df = pd.read_sql_query(query, self.engine, params=params)
            logger.debug(f"[AverageAmplitudeCriteria] Period mode query returned {len(result_df)} symbols meeting amplitude threshold")

            if result_df.empty:
                logger.info(f"[AverageAmplitudeCriteria] No symbols found with amplitude > {self.threshold}% in last {self.period} days")
                return pd.DataFrame()

            # 筛选符合条件的股票
            qualified_symbols = result_df['symbol'].tolist()
            result = df[df['symbol'].isin(qualified_symbols)].copy()
            logger.info(f"[AverageAmplitudeCriteria] Period mode filter completed, input: {len(df)}, qualified: {len(qualified_symbols)}, output: {len(result)}")
            return result

    def to_config(self) -> Dict:
        """导出为配置字典（不包含 db_path）"""
        config = {
            'type': 'AverageAmplitude',
            'threshold': self.threshold
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
            db_path=db_path,
            start_date=config.get('start_date'),
            end_date=config.get('end_date')
        )
