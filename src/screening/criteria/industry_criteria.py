"""
行业相关筛选条件

提供行业相关的筛选条件：
- IndustryAwareCriteria: 行业适配筛选（动态参数）
- IndustryFilter: 行业白名单/黑名单
- IndustryRelativeCriteria: 行业内相对筛选
"""
import pandas as pd
from typing import Dict, List, Set
from src.screening.base_criteria import BaseCriteria, AndCriteria
from src.screening.criteria.basic_criteria import RangeCriteria


class IndustryFilter(BaseCriteria):
    """行业过滤：白名单/黑名单模式"""

    def __init__(self, industries: List[str], mode: str = 'whitelist', level: int = 1):
        """
        Args:
            industries: 行业列表（申万行业名称）
            mode: 'whitelist' (白名单) 或 'blacklist' (黑名单)
            level: 行业级别（1=一级，2=二级，3=三级）
        """
        self.industries = industries
        self.mode = mode
        self.level = level

        # 根据级别确定列名
        if level == 1:
            self.col_name = 'sw_l1'
        elif level == 2:
            self.col_name = 'sw_l2'
        else:
            self.col_name = 'sw_l3'

    @property
    def cost(self) -> int:
        return 1  # 极低成本，直接过滤

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or self.col_name not in df.columns:
            return df

        if self.mode == 'whitelist':
            return df[df[self.col_name].isin(self.industries)].copy()
        else:
            return df[~df[self.col_name].isin(self.industries)].copy()

    def to_config(self) -> Dict:
        return {
            'type': 'IndustryFilter',
            'industries': self.industries,
            'mode': self.mode,
            'level': self.level
        }

    @classmethod
    def from_config(cls, config: Dict):
        return cls(
            industries=config['industries'],
            mode=config.get('mode', 'whitelist'),
            level=config.get('level', 1)
        )


class IndustryRelativeCriteria(BaseCriteria):
    """
    行业内相对筛选：先行业内筛选，再跨行业比较

    示例：筛选每个行业内ROE排名前30%的股票
    """

    def __init__(self, column: str, percentile: float = 0.3,
                 min_stocks: int = 5, level: int = 1):
        """
        Args:
            column: 排序指标（如'latest_roe'）
            percentile: 行业内保留比例（0.3 = 前30%）
            min_stocks: 每个行业最少保留股票数
            level: 行业级别（1=一级，2=二级，3=三级）
        """
        self.column = column
        self.percentile = percentile
        self.min_stocks = min_stocks
        self.level = level

        # 根据级别确定列名
        if level == 1:
            self.group_col = 'sw_l1'
        elif level == 2:
            self.group_col = 'sw_l2'
        else:
            self.group_col = 'sw_l3'

    @property
    def cost(self) -> int:
        return 10  # 需要分组计算

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or self.column not in df.columns:
            return df

        if self.group_col not in df.columns:
            return df

        result_dfs = []

        # 按行业分组筛选
        for industry, group in df.groupby(self.group_col):
            # 计算百分位数阈值
            threshold = group[self.column].quantile(1 - self.percentile)

            # 筛选：高于阈值
            mask = group[self.column] >= threshold

            # 如果行业股票少，至少保留min_stocks只
            if len(group) <= self.min_stocks:
                result_dfs.append(group)
            elif mask.sum() >= self.min_stocks:
                result_dfs.append(group[mask])
            else:
                # 取排名前min_stocks的股票
                top_stocks = group.nlargest(self.min_stocks, self.column)
                result_dfs.append(top_stocks)

        if result_dfs:
            return pd.concat(result_dfs, ignore_index=True)
        return pd.DataFrame()

    def to_config(self) -> Dict:
        return {
            'type': 'IndustryRelative',
            'column': self.column,
            'percentile': self.percentile,
            'min_stocks': self.min_stocks,
            'level': self.level
        }

    @classmethod
    def from_config(cls, config: Dict):
        return cls(
            column=config['column'],
            percentile=config.get('percentile', 0.3),
            min_stocks=config.get('min_stocks', 5),
            level=config.get('level', 1)
        )


class IndustryAwareCriteria(BaseCriteria):
    """
    行业适配筛选条件（动态参数版本）

    根据股票所属行业，从industry_statistics表读取动态百分位参数
    支持到三级行业的精细化参数
    """

    # 参数规则：定义如何使用百分位
    PARAM_RULES = {
        # 示例：PE使用行业75th百分位作为上限
        'pe_ttm': {
            'percentile': 0.75,  # 使用p75
            'comparison': 'max',  # 作为上限
            'default': 30         # 默认值（如果统计不可用）
        },
        'pb': {
            'percentile': 0.75,
            'comparison': 'max',
            'default': 5
        },
        'latest_roe': {
            'percentile': 0.25,  # 使用p25（排除最差的25%）
            'comparison': 'min',  # 作为下限
            'default': 10
        },
    }

    def __init__(self, base_criteria: BaseCriteria,
                 statistics_calculator,
                 param_rules: Dict = None,
                 industry_level: int = 1):
        """
        Args:
            base_criteria: 基础筛选条件（会根据行业调整参数）
            statistics_calculator: IndustryStatisticsCalculator实例
            param_rules: 自定义参数规则（可选，默认使用类变量PARAM_RULES）
            industry_level: 行业级别（1=一级，2=二级，3=三级）
        """
        self.base_criteria = base_criteria
        self.stats_calc = statistics_calculator
        self.param_rules = param_rules or self.PARAM_RULES
        self.industry_level = industry_level

        # 根据级别确定列名
        if industry_level == 1:
            self.group_col = 'sw_l1'
        elif industry_level == 2:
            self.group_col = 'sw_l2'
        else:
            self.group_col = 'sw_l3'

    @property
    def cost(self) -> int:
        return 5  # 需要查询行业统计表

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """分行业应用动态参数筛选条件"""
        if df.empty:
            return df

        if self.group_col not in df.columns:
            return df

        # 按指定级别行业分组
        result_dfs = []
        for industry_key, group in df.groupby(self.group_col):
            # 获取该行业的动态参数
            params = self._get_industry_params(group, industry_key)

            # 应用动态参数的条件
            industry_criteria = self._apply_industry_params(
                self.base_criteria,
                params
            )

            # 筛选该行业股票
            filtered_group = industry_criteria.filter(group)
            result_dfs.append(filtered_group)

        # 合并所有行业的结果
        if result_dfs:
            return pd.concat(result_dfs, ignore_index=True)
        return pd.DataFrame()

    def _get_industry_params(self, group: pd.DataFrame, industry_key) -> Dict:
        """获取行业动态参数"""
        params = {}

        # 提取行业信息
        sw_l1 = group.iloc[0].get('sw_l1') if 'sw_l1' in group.columns else None
        sw_l2 = group.iloc[0].get('sw_l2') if 'sw_l2' in group.columns else None
        sw_l3 = group.iloc[0].get('sw_l3') if 'sw_l3' in group.columns else None

        for metric, rule in self.param_rules.items():
            percentile = rule['percentile']
            comparison = rule['comparison']
            default = rule['default']

            # 根据行业级别查询百分位值
            if self.industry_level == 1:
                value = self.stats_calc.get_industry_percentile(
                    sw_l1=sw_l1, metric_name=metric, percentile=percentile
                )
            elif self.industry_level == 2:
                value = self.stats_calc.get_industry_percentile(
                    sw_l1=sw_l1, sw_l2=sw_l2, metric_name=metric, percentile=percentile
                )
            else:  # level 3
                value = self.stats_calc.get_industry_percentile(
                    sw_l1=sw_l1, sw_l2=sw_l2, sw_l3=sw_l3,
                    metric_name=metric, percentile=percentile
                )

            if value is None:
                # 统计不可用，使用默认值
                value = default

            # 根据比较类型设置参数
            if comparison == 'max':
                params[f'max_{metric}'] = value
            elif comparison == 'min':
                params[f'min_{metric}'] = value

        return params

    def _apply_industry_params(self, criteria: BaseCriteria, params: Dict):
        """将行业参数应用到筛选条件"""
        # 递归处理AND组合
        if isinstance(criteria, AndCriteria):
            sub_criteria = [
                self._apply_industry_params(c, params)
                for c in criteria.criteria
            ]
            return AndCriteria(*sub_criteria)

        # 修改RangeCriteria的参数
        if isinstance(criteria, RangeCriteria):
            column = criteria.column
            if f'max_{column}' in params:
                max_val = min(params[f'max_{column}'], criteria.max_val or float('inf'))
                min_val = max(params.get(f'min_{column}', 0), criteria.min_val or 0)
                return RangeCriteria(column, min_val, max_val)

        return criteria

    def to_config(self) -> Dict:
        return {
            'type': 'IndustryAware',
            'base_criteria': self.base_criteria.to_config(),
            'industry_level': self.industry_level
        }

    @classmethod
    def from_config(cls, config: Dict):
        from src.screening.rule_engine import RuleEngine
        base_criteria = RuleEngine.build_from_config(config['base_criteria'])
        return cls(
            base_criteria=base_criteria,
            statistics_calculator=None,  # 使用时注入
            industry_level=config.get('industry_level', 1)
        )
