"""
筛选条件抽象基类

提供筛选条件的基本接口和逻辑组合功能（& | ~）
支持短路筛选优化（低成本条件优先执行）
"""
from abc import ABC, abstractmethod
from typing import Dict
import pandas as pd


class BaseCriteria(ABC):
    """筛选条件抽象基类（支持逐步排除优化）"""

    @abstractmethod
    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """应用筛选条件，返回符合条件的DataFrame"""
        pass

    @property
    @abstractmethod
    def cost(self) -> int:
        """
        返回该条件的计算成本（用于优化执行顺序）

        成本等级：
        - 1: 极低成本（直接从数据库读取，如价格范围）
        - 5: 低成本（简单计算，如PE比较）
        - 10: 中等成本（需要聚合计算，如20日平均）
        - 20: 高成本（复杂计算，如Amihud指标）
        """
        pass

    @abstractmethod
    def to_config(self) -> Dict:
        """导出为配置字典"""
        pass

    @classmethod
    @abstractmethod
    def from_config(cls, config: Dict):
        """从配置创建实例"""
        pass

    def __and__(self, other):  # crit1 & crit2
        """AND组合：自动优化执行顺序（低成本条件优先）"""
        return AndCriteria(self, other)

    def __or__(self, other):   # crit1 | crit2
        return OrCriteria(self, other)

    def __invert__(self):       # ~crit
        return NotCriteria(self)


class AndCriteria(BaseCriteria):
    """AND组合：短路筛选实现"""

    def __init__(self, *criteria: BaseCriteria):
        # 关键优化：按成本排序，低成本条件先执行
        self.criteria = sorted(criteria, key=lambda c: c.cost)

    @property
    def cost(self) -> int:
        return sum(c.cost for c in self.criteria)

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """逐步排除，短路优化"""
        result = df
        for crit in self.criteria:
            if result.empty:  # 短路：已经没有数据，直接返回
                return result
            result = crit.filter(result)
        return result

    def to_config(self) -> Dict:
        return {
            'type': 'AND',
            'criteria': [c.to_config() for c in self.criteria]
        }

    @classmethod
    def from_config(cls, config: Dict):
        # 延迟导入避免循环依赖
        from src.screening.rule_engine import RuleEngine
        sub_criteria = [RuleEngine.build_from_config(c) for c in config['criteria']]
        return cls(*sub_criteria)


class OrCriteria(BaseCriteria):
    """OR组合"""

    def __init__(self, *criteria: BaseCriteria):
        self.criteria = criteria

    @property
    def cost(self) -> int:
        return max(c.cost for c in self.criteria)

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """满足任一条件即可"""
        results = []
        for crit in self.criteria:
            results.append(crit.filter(df))

        # 合并结果，去重
        if results:
            return pd.concat(results).drop_duplicates()
        return pd.DataFrame()

    def to_config(self) -> Dict:
        return {
            'type': 'OR',
            'criteria': [c.to_config() for c in self.criteria]
        }

    @classmethod
    def from_config(cls, config: Dict):
        from src.screening.rule_engine import RuleEngine
        sub_criteria = [RuleEngine.build_from_config(c) for c in config['criteria']]
        return cls(*sub_criteria)


class NotCriteria(BaseCriteria):
    """NOT取反"""

    def __init__(self, criteria: BaseCriteria):
        self.criteria = criteria

    @property
    def cost(self) -> int:
        return self.criteria.cost

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """返回不满足条件的行"""
        passed_symbols = self.criteria.filter(df)['symbol'].unique()
        return df[~df['symbol'].isin(passed_symbols)]

    def to_config(self) -> Dict:
        return {
            'type': 'NOT',
            'criteria': self.criteria.to_config()
        }

    @classmethod
    def from_config(cls, config: Dict):
        from src.screening.rule_engine import RuleEngine
        sub_criteria = RuleEngine.build_from_config(config['criteria'])
        return cls(sub_criteria)
