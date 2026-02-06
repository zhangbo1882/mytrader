"""
规则引擎

从JSON配置动态构建筛选条件
"""
from typing import Dict
from src.screening.base_criteria import BaseCriteria, AndCriteria, OrCriteria, NotCriteria
from src.screening.criteria.basic_criteria import RangeCriteria, GreaterThanCriteria, LessThanCriteria, PercentileCriteria, TopNCriteria
from src.screening.criteria.industry_criteria import (
    IndustryFilter, IndustryRelativeCriteria
)


class RuleEngine:
    """规则引擎：从JSON配置构建筛选条件"""

    CRITERIA_MAP = {
        'Range': RangeCriteria,
        'GreaterThan': GreaterThanCriteria,
        'LessThan': LessThanCriteria,
        'Percentile': PercentileCriteria,
        'TopN': TopNCriteria,
        'IndustryFilter': IndustryFilter,
        'IndustryRelative': IndustryRelativeCriteria,
    }

    @classmethod
    def build_from_config(cls, config: Dict) -> BaseCriteria:
        """
        从JSON配置构建筛选条件

        Args:
            config: 配置字典

        Returns:
            BaseCriteria实例

        Raises:
            ValueError: 未知的条件类型

        Examples:
            >>> config = {
            ...     'type': 'AND',
            ...     'criteria': [
            ...         {'type': 'Range', 'column': 'pe_ttm', 'min_val': 0, 'max_val': 30},
            ...         {'type': 'GreaterThan', 'column': 'latest_roe', 'threshold': 10}
            ...     ]
            ... }
            >>> criteria = RuleEngine.build_from_config(config)
        """
        crit_type = config['type']

        if crit_type == 'AND':
            sub_criteria = [cls.build_from_config(c) for c in config['criteria']]
            return AndCriteria(*sub_criteria)

        elif crit_type == 'OR':
            sub_criteria = [cls.build_from_config(c) for c in config['criteria']]
            return OrCriteria(*sub_criteria)

        elif crit_type == 'NOT':
            sub_criteria = cls.build_from_config(config['criteria'])
            return NotCriteria(sub_criteria)

        elif crit_type in cls.CRITERIA_MAP:
            return cls.CRITERIA_MAP[crit_type].from_config(config)

        else:
            raise ValueError(f"Unknown criteria type: {crit_type}")

    @classmethod
    def register_criteria(cls, name: str, criteria_class):
        """
        注册自定义筛选条件

        Args:
            name: 条件类型名称
            criteria_class: 筛选条件类（需实现from_config方法）
        """
        cls.CRITERIA_MAP[name] = criteria_class

    @classmethod
    def list_supported_types(cls) -> list:
        """
        列出所有支持的筛选条件类型

        Returns:
            条件类型列表
        """
        return list(cls.CRITERIA_MAP.keys()) + ['AND', 'OR', 'NOT']
