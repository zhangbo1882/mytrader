"""
涨幅天数筛选条件

筛选过去N个交易日中涨幅超过阈值的交易日天数超过一定比例的股票
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict
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
            db_path: 数据库路径
            start_date: 开始日期 YYYYMMDD (可选，与 period 二选一)
            end_date: 结束日期 YYYYMMDD (可选，与 period 二选一)
        """
        self.period = period
        self.threshold = threshold
        self.min_positive_ratio = min_positive_ratio
        self.start_date = start_date
        self.end_date = end_date
        self.db_path = db_path
        self.engine = create_engine(f'sqlite:///{db_path}', echo=False)


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

        策略：从当前 df 提取股票列表，查询历史数据计算涨幅天数比例
        支持两种模式：
        1. period 模式：取最近 N 个交易日
        2. 时间范围模式：指定 start_date 和 end_date
        """
        if df.empty:
            return df

        # 获取股票列表
        symbols = df['symbol'].unique().tolist()

        if not symbols:
            return pd.DataFrame()

        # 构建 IN 子句的占位符字符串 (用于 SQLite)
        symbols_placeholder = ', '.join([f':symbol_{i}' for i in range(len(symbols))])

        # 根据模式构建不同的查询
        if self.start_date and self.end_date:
            # 时间范围模式：查询符合条件的所有股票
            query = text(f"""
            WITH bar_count AS (
                SELECT
                    symbol,
                    COUNT(*) as total_days,
                    SUM(CASE WHEN pct_chg > :threshold THEN 1 ELSE 0 END) as positive_days
                FROM bars
                WHERE datetime >= :start_date
                  AND datetime <= :end_date
                  AND interval = '1d'
                  AND pct_chg IS NOT NULL
                GROUP BY symbol
                HAVING COUNT(*) >= 1
            )
            SELECT DISTINCT symbol
            FROM bar_count
            WHERE CAST(positive_days AS FLOAT) / total_days >= :min_ratio
            """)
            params = {
                'start_date': self.format_date_for_db(self.start_date),
                'end_date': self.format_date_for_db(self.end_date),
                'threshold': self.threshold,
                'min_ratio': self.min_positive_ratio
            }

            result_df = pd.read_sql_query(query, self.engine, params=params)

            if result_df.empty:
                return pd.DataFrame()

            # 返回包含所有符合条件股票的 DataFrame（从原始 df 中查找）
            qualified_symbols = result_df['symbol'].tolist()
            available_symbols = df['symbol'].unique().tolist()
            final_symbols = [s for s in qualified_symbols if s in available_symbols]

            if not final_symbols:
                return pd.DataFrame()

            # 返回原始 df 中符合条件的数据
            return df[df['symbol'].isin(final_symbols)].copy()
        else:
            # period 模式（默认）
            trade_date = df.iloc[0].get('trade_date')
            query = text(f"""
            WITH recent_bars AS (
                SELECT
                    symbol,
                    datetime,
                    pct_chg,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol
                        ORDER BY datetime DESC
                    ) as rn
                FROM bars
                WHERE symbol IN ({symbols_placeholder})
                  AND datetime <= :trade_date
                  AND interval = '1d'
                  AND pct_chg IS NOT NULL
            ),
            positive_count AS (
                SELECT
                    symbol,
                    SUM(CASE WHEN pct_chg > :threshold THEN 1 ELSE 0 END) as positive_days,
                    COUNT(*) as actual_days
                FROM recent_bars
                WHERE rn <= :period
                GROUP BY symbol
                HAVING COUNT(*) >= :min_days
            )
            SELECT symbol
            FROM positive_count
            WHERE positive_days >= :min_positive_days
            """)
            # 计算最小正涨幅天数（向上取整）
            min_positive_days = int(self.period * self.min_positive_ratio) + 1
            params = {
                'trade_date': self.format_date_for_db(trade_date),
                'period': self.period,
                'threshold': self.threshold,
                'min_days': self.period,  # 要求有足够的历史数据
                'min_positive_days': min_positive_days
            }

            # 添加每个股票代码作为独立参数
            for i, symbol in enumerate(symbols):
                params[f'symbol_{i}'] = symbol

            result_df = pd.read_sql_query(query, self.engine, params=params)

            if result_df.empty:
                return pd.DataFrame()

            # 筛选符合条件的股票
            qualified_symbols = result_df['symbol'].tolist()
            return df[df['symbol'].isin(qualified_symbols)].copy()

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