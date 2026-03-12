"""
规则引擎

从JSON配置动态构建筛选条件
"""
import logging
from typing import Dict
from src.screening.base_criteria import BaseCriteria, AndCriteria, OrCriteria, NotCriteria
from src.screening.criteria.basic_criteria import RangeCriteria, GreaterThanCriteria, LessThanCriteria, PercentileCriteria, TopNCriteria
from src.screening.criteria.industry_criteria import (
    IndustryFilter, IndustryRelativeCriteria
)
from src.screening.criteria.amplitude_criteria import AverageAmplitudeCriteria
from src.screening.criteria.amplitude_column_criteria import AmplitudeColumnCriteria
from src.screening.criteria.positive_days_criteria import PositiveDaysCriteria
from src.screening.criteria.turnover_criteria import AverageTurnoverCriteria
from src.screening.criteria.market_criteria import MarketFilter
from src.screening.criteria.field_criteria import FieldFilterCriteria
from src.screening.criteria.bear_to_bull_criteria import BearToBullTransitionCriteria
from src.screening.criteria.valuation_criteria import ValuationUpsideCriteria

logger = logging.getLogger(__name__)


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
        'AverageAmplitude': AverageAmplitudeCriteria,
        'AmplitudeColumn': AmplitudeColumnCriteria,
        'TurnoverColumn': AverageTurnoverCriteria,
        'PositiveDays': PositiveDaysCriteria,
        'MarketFilter': MarketFilter,
        'FieldFilter': FieldFilterCriteria,
        'BearToBull': BearToBullTransitionCriteria,
        'ValuationUpside': ValuationUpsideCriteria,
    }

    @classmethod
    def build_from_config(cls, config: Dict, db_path: str = None) -> BaseCriteria:
        """
        从JSON配置构建筛选条件

        Args:
            config: 配置字典
            db_path: 数据库路径（AverageAmplitude需要）

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
        logger.info(f"[RuleEngine] Building criteria from config: {config}")
        crit_type = config['type']
        logger.debug(f"[RuleEngine] Criteria type: {crit_type}")

        if crit_type == 'AND':
            logger.debug(f"[RuleEngine] Building AND criteria with {len(config['criteria'])} sub-criteria")
            sub_criteria = [cls.build_from_config(c, db_path) for c in config['criteria']]
            logger.debug(f"[RuleEngine] AND criteria built successfully")
            return AndCriteria(*sub_criteria)

        elif crit_type == 'OR':
            logger.debug(f"[RuleEngine] Building OR criteria with {len(config['criteria'])} sub-criteria")
            sub_criteria = [cls.build_from_config(c, db_path) for c in config['criteria']]
            logger.debug(f"[RuleEngine] OR criteria built successfully")
            return OrCriteria(*sub_criteria)

        elif crit_type == 'NOT':
            logger.debug(f"[RuleEngine] Building NOT criteria")
            sub_criteria = cls.build_from_config(config['criteria'], db_path)
            logger.debug(f"[RuleEngine] NOT criteria built successfully")
            return NotCriteria(sub_criteria)

        elif crit_type in cls.CRITERIA_MAP:
            logger.debug(f"[RuleEngine] Building {crit_type} criteria from config")
            # AverageAmplitude、AmplitudeColumn、TurnoverColumn、PositiveDays 和 ValuationUpside 需要传递 db_path
            if crit_type in ['AverageAmplitude', 'AmplitudeColumn', 'TurnoverColumn', 'PositiveDays', 'ValuationUpside']:
                if db_path is None:
                    logger.warning(f"[RuleEngine] {crit_type} criteria requires db_path parameter")
                    raise ValueError(f"{crit_type} criteria requires db_path parameter")
                logger.debug(f"[RuleEngine] Using db_path for {crit_type} criteria")
                result = cls.CRITERIA_MAP[crit_type].from_config(config, db_path)
            else:
                result = cls.CRITERIA_MAP[crit_type].from_config(config)
            logger.debug(f"[RuleEngine] {crit_type} criteria built successfully")
            return result

        else:
            logger.error(f"[RuleEngine] Unknown criteria type: {crit_type}")
            raise ValueError(f"Unknown criteria type: {crit_type}")

    @classmethod
    def register_criteria(cls, name: str, criteria_class):
        """
        注册自定义筛选条件

        Args:
            name: 条件类型名称
            criteria_class: 筛选条件类（需实现from_config方法）
        """
        logger.info(f"[RuleEngine] Registering custom criteria: {name}")
        cls.CRITERIA_MAP[name] = criteria_class

    @classmethod
    def list_supported_types(cls) -> list:
        """
        列出所有支持的筛选条件类型

        Returns:
            条件类型列表
        """
        supported_types = list(cls.CRITERIA_MAP.keys()) + ['AND', 'OR', 'NOT']
        logger.debug(f"[RuleEngine] Listing supported criteria types: {len(supported_types)} types")
        return supported_types
