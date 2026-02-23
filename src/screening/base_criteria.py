"""
筛选条件抽象基类

提供筛选条件的基本接口和逻辑组合功能（& | ~）
支持短路筛选优化（低成本条件优先执行）
"""
import logging
from abc import ABC, abstractmethod
from typing import Dict
import pandas as pd

logger = logging.getLogger(__name__)


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

    @staticmethod
    def format_date_for_db(date_val) -> str:
        """
        将日期格式化为数据库格式 (YYYY-MM-DD)

        支持输入格式:
        - pandas.Timestamp 对象
        - YYYYMMDD (如: 20251101)
        - YYYY-MM-DD (如: 2025-11-01)
        """
        if not date_val:
            return date_val

        # 处理 pandas Timestamp 对象
        if isinstance(date_val, pd.Timestamp):
            return date_val.strftime('%Y-%m-%d')

        # 转为字符串处理
        date_str = str(date_val)

        # 如果已经是 YYYY-MM-DD 格式，直接返回
        if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
            return date_str

        # 尝试解析 YYYYMMDD 格式
        if len(date_str) == 8 and date_str.isdigit():
            try:
                return f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"
            except Exception:
                pass

        # 无法解析，返回原字符串
        logger.warning(f"[BaseCriteria] Unable to parse date format: {date_val}")
        return date_str

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
        logger.debug(f"[AndCriteria] Starting filter with {len(self.criteria)} criteria, input size: {len(df)}")
        logger.debug(f"[AndCriteria] Criteria execution order (by cost): {[type(c).__name__ for c in self.criteria]}")

        if df.empty:
            logger.warning(f"[AndCriteria] Input DataFrame is empty")
            return df

        result = df
        for i, crit in enumerate(self.criteria):
            input_size = len(result)
            if result.empty:  # 短路：已经没有数据，直接返回
                logger.debug(f"[AndCriteria] Short-circuit: result is empty after {i} criteria")
                return result
            logger.debug(f"[AndCriteria] Applying criterion {i+1}/{len(self.criteria)}: {type(crit).__name__} (cost: {crit.cost})")
            result = crit.filter(result)
            logger.debug(f"[AndCriteria] Criterion {i+1}/{len(self.criteria)} applied, input: {input_size}, output: {len(result)}")

        logger.info(f"[AndCriteria] Filter completed, total input: {len(df)}, final output: {len(result)}")
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
        logger.debug(f"[OrCriteria] Starting filter with {len(self.criteria)} criteria, input size: {len(df)}")

        if df.empty:
            logger.warning(f"[OrCriteria] Input DataFrame is empty")
            return df

        results = []
        for i, crit in enumerate(self.criteria):
            logger.debug(f"[OrCriteria] Applying criterion {i+1}/{len(self.criteria)}: {type(crit).__name__}")
            result = crit.filter(df)
            logger.debug(f"[OrCriteria] Criterion {i+1}/{len(self.criteria)} returned {len(result)} results")
            results.append(result)

        # 合并结果，去重
        if results:
            combined = pd.concat(results).drop_duplicates()
            logger.info(f"[OrCriteria] Filter completed, input: {len(df)}, combined results before dedup: {sum(len(r) for r in results)}, final output: {len(combined)}")
            return combined

        logger.info(f"[OrCriteria] Filter completed, no criteria matched, returning empty DataFrame")
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
        logger.debug(f"[NotCriteria] Starting filter, input size: {len(df)}")

        if df.empty:
            logger.warning(f"[NotCriteria] Input DataFrame is empty")
            return df

        if 'symbol' not in df.columns:
            logger.warning(f"[NotCriteria] 'symbol' column not found in DataFrame")
            return df

        logger.debug(f"[NotCriteria] Applying inner criteria: {type(self.criteria).__name__}")
        passed_symbols = self.criteria.filter(df)['symbol'].unique()
        logger.debug(f"[NotCriteria] Inner criteria matched {len(passed_symbols)} symbols")

        result = df[~df['symbol'].isin(passed_symbols)]
        logger.info(f"[NotCriteria] Filter completed, input: {len(df)}, excluded symbols: {len(passed_symbols)}, output: {len(result)}")
        return result

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
