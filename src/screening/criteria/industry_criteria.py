"""
行业相关筛选条件

提供行业相关的筛选条件：
- IndustryAwareCriteria: 行业适配筛选（动态参数）
- IndustryFilter: 行业白名单/黑名单
- IndustryRelativeCriteria: 行业内相对筛选
"""
import logging
import pandas as pd
from typing import Dict, List, Set
from src.screening.base_criteria import BaseCriteria, AndCriteria
from src.screening.criteria.basic_criteria import RangeCriteria

logger = logging.getLogger(__name__)


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
        logger.debug(f"[IndustryFilter] Starting filter, mode: {self.mode}, level: {self.level}, industries: {self.industries}, input size: {len(df)}")

        if df.empty:
            logger.warning(f"[IndustryFilter] Input DataFrame is empty")
            return df

        if self.col_name not in df.columns:
            logger.warning(f"[IndustryFilter] Column '{self.col_name}' not found in DataFrame")
            return df

        if self.mode == 'whitelist':
            result = df[df[self.col_name].isin(self.industries)].copy()
            logger.info(f"[IndustryFilter] Whitelist filter applied, level: {self.level}, industries: {len(self.industries)}, input: {len(df)}, output: {len(result)}")
        else:
            result = df[~df[self.col_name].isin(self.industries)].copy()
            logger.info(f"[IndustryFilter] Blacklist filter applied, level: {self.level}, industries: {len(self.industries)}, input: {len(df)}, output: {len(result)}")

        return result

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
    行业内相对筛选：在指定行业内筛选指标值在指定百分位的股票

    示例：
    - 筛选电子行业内ROE排名前30%的股票（越大越优）
    - 筛选半导体行业内PE最低30%的股票（越小越优）
    """

    # 越小越优的指标列表（估值类、负债类等）
    LOWER_IS_BETTER_METRICS = {
        'pe_ttm', 'pb', 'ps_ttm',  # 估值指标
        'debt_to_assets', 'debt_to_eqity',  # 负债指标
    }

    def __init__(self, column: str, percentile: float = 0.3,
                 min_stocks: int = 5, level: int = 1, industry: str = None):
        """
        Args:
            column: 排序指标（如'latest_roe', 'pe_ttm'）
            percentile: 行业内保留比例（0.3 = 前30%或最低30%）
            min_stocks: 每个行业最少保留股票数
            level: 行业级别（1=一级，2=二级，3=三级）
            industry: 指定行业名称（如果指定，只在该行业内筛选）
        """
        self.column = column
        self.percentile = percentile
        self.min_stocks = min_stocks
        self.level = level
        self.industry = industry

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
        logger.debug(f"[IndustryRelativeCriteria] Starting filter, column: {self.column}, percentile: {self.percentile}, level: {self.level}, industry: {self.industry}, input size: {len(df)}")

        if df.empty:
            logger.warning(f"[IndustryRelativeCriteria] Input DataFrame is empty")
            return df

        if self.column not in df.columns:
            logger.warning(f"[IndustryRelativeCriteria] Column '{self.column}' not found in DataFrame")
            return df

        if self.group_col not in df.columns:
            logger.warning(f"[IndustryRelativeCriteria] Group column '{self.group_col}' not found in DataFrame")
            return df

        # 如果指定了行业，先过滤出该行业的股票
        original_size = len(df)
        if self.industry:
            logger.debug(f"[IndustryRelativeCriteria] Filtering for specific industry: {self.industry}")
            df = df[df[self.group_col] == self.industry].copy()
            if df.empty:
                logger.warning(f"[IndustryRelativeCriteria] No stocks found for industry: {self.industry}")
                return df
            logger.debug(f"[IndustryRelativeCriteria] Filtered to industry {self.industry}, size: {len(df)}")

        result_dfs = []

        # 判断是否为"越小越优"的指标
        is_lower_better = self.column in self.LOWER_IS_BETTER_METRICS
        logger.debug(f"[IndustryRelativeCriteria] Column '{self.column}' is_lower_better: {is_lower_better}")

        # 按行业分组筛选
        industries = df[self.group_col].unique()
        logger.debug(f"[IndustryRelativeCriteria] Processing {len(industries)} industries")

        for industry_name, group in df.groupby(self.group_col):
            industry_size = len(group)
            if is_lower_better:
                # 越小越优：选择最低的percentile比例
                threshold = group[self.column].quantile(self.percentile)
                mask = group[self.column] <= threshold
                selected_count = mask.sum()

                # 如果筛选出的股票太少，取最小的min_stocks只
                if selected_count >= self.min_stocks:
                    result_dfs.append(group[mask])
                    logger.debug(f"[IndustryRelativeCriteria] Industry '{industry_name}': percentile filter selected {selected_count}/{industry_size} stocks")
                elif industry_size <= self.min_stocks:
                    result_dfs.append(group)
                    logger.debug(f"[IndustryRelativeCriteria] Industry '{industry_name}': industry size ({industry_size}) <= min_stocks ({self.min_stocks}), selected all")
                else:
                    # 取最小的min_stocks只股票
                    top_stocks = group.nsmallest(self.min_stocks, self.column)
                    result_dfs.append(top_stocks)
                    logger.debug(f"[IndustryRelativeCriteria] Industry '{industry_name}': selected min_stocks ({self.min_stocks}) lowest stocks")
            else:
                # 越大越优：选择最高的percentile比例
                threshold = group[self.column].quantile(1 - self.percentile)
                mask = group[self.column] >= threshold
                selected_count = mask.sum()

                # 如果筛选出的股票太少，取最大的min_stocks只
                if selected_count >= self.min_stocks:
                    result_dfs.append(group[mask])
                    logger.debug(f"[IndustryRelativeCriteria] Industry '{industry_name}': percentile filter selected {selected_count}/{industry_size} stocks")
                elif industry_size <= self.min_stocks:
                    result_dfs.append(group)
                    logger.debug(f"[IndustryRelativeCriteria] Industry '{industry_name}': industry size ({industry_size}) <= min_stocks ({self.min_stocks}), selected all")
                else:
                    # 取最大的min_stocks只股票
                    top_stocks = group.nlargest(self.min_stocks, self.column)
                    result_dfs.append(top_stocks)
                    logger.debug(f"[IndustryRelativeCriteria] Industry '{industry_name}': selected min_stocks ({self.min_stocks}) highest stocks")

        if result_dfs:
            result = pd.concat(result_dfs, ignore_index=True)
            logger.info(f"[IndustryRelativeCriteria] Filter completed, input: {original_size}, industries: {len(industries)}, output: {len(result)}")
            return result

        logger.info(f"[IndustryRelativeCriteria] Filter completed, no stocks selected")
        return pd.DataFrame()

    def to_config(self) -> Dict:
        return {
            'type': 'IndustryRelative',
            'column': self.column,
            'percentile': self.percentile,
            'min_stocks': self.min_stocks,
            'level': self.level,
            'industry': self.industry
        }

    @classmethod
    def from_config(cls, config: Dict):
        return cls(
            column=config['column'],
            percentile=config.get('percentile', 0.3),
            min_stocks=config.get('min_stocks', 5),
            level=config.get('level', 1),
            industry=config.get('industry')
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
        logger.debug(f"[IndustryAwareCriteria] Starting filter, industry_level: {self.industry_level}, param_rules: {len(self.param_rules)}, input size: {len(df)}")

        if df.empty:
            logger.warning(f"[IndustryAwareCriteria] Input DataFrame is empty")
            return df

        if self.group_col not in df.columns:
            logger.warning(f"[IndustryAwareCriteria] Group column '{self.group_col}' not found in DataFrame")
            return df

        # 按指定级别行业分组
        result_dfs = []
        industries = df[self.group_col].unique()
        logger.debug(f"[IndustryAwareCriteria] Processing {len(industries)} industries")

        for industry_key, group in df.groupby(self.group_col):
            industry_size = len(group)
            logger.debug(f"[IndustryAwareCriteria] Processing industry '{industry_key}', size: {industry_size}")

            # 获取该行业的动态参数
            params = self._get_industry_params(group, industry_key)
            logger.debug(f"[IndustryAwareCriteria] Industry '{industry_key}': generated {len(params)} dynamic parameters")

            # 应用动态参数的条件
            industry_criteria = self._apply_industry_params(
                self.base_criteria,
                params
            )

            # 筛选该行业股票
            filtered_group = industry_criteria.filter(group)
            filtered_size = len(filtered_group)
            result_dfs.append(filtered_group)
            logger.debug(f"[IndustryAwareCriteria] Industry '{industry_key}': filtered from {industry_size} to {filtered_size} stocks")

        # 合并所有行业的结果
        if result_dfs:
            result = pd.concat(result_dfs, ignore_index=True)
            logger.info(f"[IndustryAwareCriteria] Filter completed, input: {len(df)}, industries: {len(industries)}, output: {len(result)}")
            return result

        logger.info(f"[IndustryAwareCriteria] Filter completed, no stocks selected")
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
