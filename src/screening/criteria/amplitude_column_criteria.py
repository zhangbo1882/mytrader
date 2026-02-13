"""
振幅列筛选条件

支持将平均振幅作为可筛选的列字段，可以与其他条件类型（Range/GreaterThan/LessThan）结合使用
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, Union
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
            db_path: 数据库路径
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
        self.engine = create_engine(f'sqlite:///{db_path}', echo=False)

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

        策略：从当前 df 提取股票列表，查询历史数据计算平均振幅，然后应用筛选条件
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

        # 构建 IN 子句的占位符字符串 (用于 SQLite)
        symbols_placeholder = ', '.join([f':symbol_{i}' for i in range(len(symbols))])

        # 批量查询历史数据（使用窗口函数优化性能）
        if self.period:
            # 使用 period 模式：获取最近N个交易日
            if 'trade_date' not in df.columns:
                logger.warning(f"[AmplitudeColumnCriteria] 'trade_date' column not found in DataFrame for period mode")
                return df

            trade_date = df.iloc[0].get('trade_date')
            logger.debug(f"[AmplitudeColumnCriteria] Using period mode: last {self.period} days from {trade_date}")

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
                HAVING COUNT(*) >= 1
            )
            SELECT symbol, avg_amplitude
            FROM amplitude_calc
            """)

            # 构建参数字典（转换日期格式）
            params = {
                'trade_date': self.format_date_for_db(trade_date),
                'period': self.period
            }
        else:
            # 使用日期范围模式：获取指定日期范围内的数据
            logger.debug(f"[AmplitudeColumnCriteria] Using date range mode: {self.start_date} to {self.end_date}")
            # 日期范围模式下查询所有股票（不限于输入DataFrame中的股票）
            # 这样可以找到更多符合条件的股票，然后再与输入DataFrame取交集
            query = text(f"""
            WITH bars_in_range AS (
                SELECT
                    symbol,
                    datetime,
                    high,
                    low,
                    pre_close
                FROM bars
                WHERE datetime >= :start_date
                  AND datetime <= :end_date
                  AND interval = '1d'
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
            """)

            # 构建参数字典（转换日期格式）
            params = {
                'start_date': self.format_date_for_db(self.start_date),
                'end_date': self.format_date_for_db(self.end_date)
            }

        # 添加每个股票代码作为独立参数（仅period模式需要）
        if self.period:
            for i, symbol in enumerate(symbols):
                params[f'symbol_{i}'] = symbol

        if self.period:
            logger.debug(f"[AmplitudeColumnCriteria] Executing period mode query for {len(symbols)} symbols")
            logger.debug(f"[AmplitudeColumnCriteria] Query params: { {k: v for k, v in params.items() if not k.startswith('symbol_')} }")
        else:
            logger.debug(f"[AmplitudeColumnCriteria] Executing date range mode query (all stocks)")
            logger.debug(f"[AmplitudeColumnCriteria] Query params: start_date={params.get('start_date')}, end_date={params.get('end_date')}")
        result_df = pd.read_sql_query(query, self.engine, params=params)
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
