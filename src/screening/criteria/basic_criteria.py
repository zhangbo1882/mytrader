"""
基础筛选条件

提供基本的筛选条件：范围、大于、百分位
"""
import pandas as pd
from typing import Optional, Dict
from src.screening.base_criteria import BaseCriteria


class RangeCriteria(BaseCriteria):
    """范围筛选：min_val <= value <= max_val"""

    def __init__(self, column: str, min_val: Optional[float] = None,
                 max_val: Optional[float] = None):
        """
        Args:
            column: 列名
            min_val: 最小值（None表示无下限）
            max_val: 最大值（None表示无上限）
        """
        self.column = column
        self.min_val = min_val
        self.max_val = max_val

    @property
    def cost(self) -> int:
        return 1  # 极低成本：直接列比较

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or self.column not in df.columns:
            return df

        result = df.copy()

        if self.min_val is not None:
            result = result[result[self.column] >= self.min_val]

        if self.max_val is not None:
            result = result[result[self.column] <= self.max_val]

        return result

    def to_config(self) -> Dict:
        return {
            'type': 'Range',
            'column': self.column,
            'min_val': self.min_val,
            'max_val': self.max_val
        }

    @classmethod
    def from_config(cls, config: Dict):
        return cls(
            column=config['column'],
            min_val=config.get('min_val'),
            max_val=config.get('max_val')
        )


class GreaterThanCriteria(BaseCriteria):
    """大于筛选：value > threshold"""

    def __init__(self, column: str, threshold: float):
        self.column = column
        self.threshold = threshold

    @property
    def cost(self) -> int:
        return 1

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or self.column not in df.columns:
            return df
        return df[df[self.column] > self.threshold].copy()

    def to_config(self) -> Dict:
        return {
            'type': 'GreaterThan',
            'column': self.column,
            'threshold': self.threshold
        }

    @classmethod
    def from_config(cls, config: Dict):
        return cls(
            column=config['column'],
            threshold=config['threshold']
        )


class LessThanCriteria(BaseCriteria):
    """小于筛选：value < threshold"""

    def __init__(self, column: str, threshold: float):
        self.column = column
        self.threshold = threshold

    @property
    def cost(self) -> int:
        return 1

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or self.column not in df.columns:
            return df
        return df[df[self.column] < self.threshold].copy()

    def to_config(self) -> Dict:
        return {
            'type': 'LessThan',
            'column': self.column,
            'threshold': self.threshold
        }

    @classmethod
    def from_config(cls, config: Dict):
        return cls(
            column=config['column'],
            threshold=config['threshold']
        )


class PercentileCriteria(BaseCriteria):
    """百分位筛选：value > threshold_percentile"""

    def __init__(self, column: str, percentile: float = 0.75):
        """
        Args:
            column: 列名
            percentile: 百分位（0.75 = 75th percentile，筛选前25%）
        """
        self.column = column
        self.percentile = percentile

    @property
    def cost(self) -> int:
        return 5  # 需要计算百分位

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or self.column not in df.columns:
            return df

        threshold = df[self.column].quantile(1 - self.percentile)
        return df[df[self.column] >= threshold].copy()

    def to_config(self) -> Dict:
        return {
            'type': 'Percentile',
            'column': self.column,
            'percentile': self.percentile
        }

    @classmethod
    def from_config(cls, config: Dict):
        return cls(
            column=config['column'],
            percentile=config.get('percentile', 0.75)
        )


class TopNCriteria(BaseCriteria):
    """取前N个筛选：按指定列排序，取前N个"""

    def __init__(self, column: str, n: int = 10, ascending: bool = False):
        """
        Args:
            column: 排序列名
            n: 取前N个
            ascending: 是否升序（默认False，降序取最大的）
        """
        self.column = column
        self.n = n
        self.ascending = ascending

    @property
    def cost(self) -> int:
        return 5  # 需要排序

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or self.column not in df.columns:
            return df

        return df.nlargest(self.n, self.column) if not self.ascending else df.nsmallest(self.n, self.column)

    def to_config(self) -> Dict:
        return {
            'type': 'TopN',
            'column': self.column,
            'n': self.n,
            'ascending': self.ascending
        }

    @classmethod
    def from_config(cls, config: Dict):
        return cls(
            column=config['column'],
            n=config.get('n', 10),
            ascending=config.get('ascending', False)
        )
