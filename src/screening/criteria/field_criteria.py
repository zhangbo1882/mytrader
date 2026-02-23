"""
字段筛选基类

支持基于字段的筛选（市场、行业等）
与现有的 MarketFilter、IndustryFilter 区分开，不冲突
"""
import logging
import pandas as pd
from typing import Dict, List, Any
from src.screening.base_criteria import BaseCriteria

logger = logging.getLogger(__name__)


class FieldFilterCriteria(BaseCriteria):
    """
    通用字段筛选基类

    支持的字段类型：
    - market: 市场筛选（主板、创业板、科创板、北交所）
    - industry: 行业筛选
    - pe_ttm: 市盈率TTM
    - pb: 市净率
    - circ_mv_yi: 流通市值（亿）
    - turnover_rate: 换手率
    - avg_amplitude: 平均振幅
    - positive_days: 正收益天数占比

    支持的运算符：
    - in: 包含（用于列表字段，如市场、行业）
    - eq: 等于
    - gt: 大于
    - lt: 小于
    - gte: 大于等于
    - lte: 小于等于
    """

    # 字段类型映射
    FIELD_TYPE_MARKET = 'market'      # 市场：列表值
    FIELD_TYPE_INDUSTRY = 'industry'  # 行业：列表值
    FIELD_TYPE_NUMERIC = 'numeric'    # 数值：需要范围或阈值

    def __init__(self, field: str, operator: str = 'eq', value: Any = None):
        """
        Args:
            field: 字段名称 ('market', 'industry', 'pe_ttm', 'pb', etc.)
            operator: 运算符 ('in', 'eq', 'gt', 'lt', 'gte', 'lte')
            value: 字段值（市场列表、行业名称、数值等）
        """
        self.field = field
        self.operator = operator
        self.value = value

        # 验证字段类型
        if field == 'market':
            if operator not in ['in', 'eq']:
                raise ValueError(f"市场字段只支持 'in' 或 'eq' 运算符")
            if not isinstance(value, list):
                raise ValueError(f"市场字段值必须是列表，如：['主板', '创业板']")
        elif field == 'industry':
            if operator == 'in':
                if not isinstance(value, list):
                    raise ValueError(f"行业字段值必须是列表，如：['银行', '非银金融']")
            elif operator not in ['in', 'eq']:
                raise ValueError(f"行业字段只支持 'eq' 运算符")
        elif field in ['pe_ttm', 'pb', 'circ_mv_yi', 'turnover_rate', 'avg_amplitude', 'positive_days']:
            if operator not in ['range', 'eq', 'gt', 'lt', 'gte', 'lte']:
                raise ValueError(f"{field} 字段只支持 'range', 'eq', 'gt', 'lt', 'gte', 'lte' 运算符")
            # 数值字段需要范围或单个值
            if operator == 'range':
                if not isinstance(value, (list, tuple)) or len(value) != 2:
                    raise ValueError(f"{field} 字段范围条件需要2个值 [min, max]")
            elif operator in ['gt', 'lt', 'gte', 'lte']:
                if not isinstance(value, (int, float)):
                    raise ValueError(f"{field} 字段比较条件需要单个数值")
            elif operator == 'eq':
                if not isinstance(value, (int, float, str)):
                    raise ValueError(f"{field} 字段等于条件需要单个值")
        else:
            raise ValueError(f"不支持的字段类型: {field}")

    @property
    def cost(self) -> int:
        """返回计算成本"""
        if self.field in ['market', 'industry']:
            return 1  # 低成本，直接过滤
        elif self.field in ['pe_ttm', 'pb', 'circ_mv_yi']:
            return 2  # 中等成本，数值比较
        elif self.field in ['turnover_rate', 'avg_amplitude', 'positive_days']:
            return 10  # 技术指标，需要计算
        return 5

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        应用字段筛选

        Args:
            df: 待筛选的股票数据 DataFrame

        Returns:
            符合条件的股票 DataFrame
        """
        logger.debug(f"[FieldFilterCriteria] Starting filter, field: {self.field}, operator: {self.operator}, value: {self.value}, input size: {len(df)}")

        if df.empty:
            logger.warning(f"[FieldFilterCriteria] Input DataFrame is empty")
            return df

        # 确保字段列存在
        if self.field not in df.columns:
            # 市场字段在 screening_engine 中已添加，但如果没有就跳过
            if self.field == 'market':
                logger.warning(f"[FieldFilterCriteria] 'market' column not found in DataFrame, skipping filter")
                return df  # 没有market列就不筛选（返回全部）
            else:
                # 对于其他字段，如果列不存在就返回全部
                logger.warning(f"[FieldFilterCriteria] Column '{self.field}' not found in DataFrame, skipping filter")
                return df

        # 执行筛选
        if self.field == 'market':
            # 市场筛选（支持 in 运算符）
            if isinstance(self.value, list):
                if self.operator == 'in':
                    result = df[df['market'].isin(self.value)]
                    logger.info(f"[FieldFilterCriteria] Market filter applied (in operator), markets: {self.value}, input: {len(df)}, output: {len(result)}")
                    return result
                else:
                    result = df[df['market'] == self.value]
                    logger.info(f"[FieldFilterCriteria] Market filter applied (eq operator), market: {self.value}, input: {len(df)}, output: {len(result)}")
                    return result
            elif self.field == 'industry':
                # 行业筛选
                if self.operator == 'in':
                    if not isinstance(self.value, list):
                        raise ValueError(f"行业值必须是列表")
                    result = df[df['sw_l1'].isin(self.value)]
                    logger.info(f"[FieldFilterCriteria] Industry filter applied (in operator), industries: {self.value}, input: {len(df)}, output: {len(result)}")
                    return result
                else:
                    # 暂不支持单行业筛选
                    logger.info(f"[FieldFilterCriteria] Industry filter not supported for operator '{self.operator}', returning all")
                    return df
        elif self.field in ['pe_ttm', 'pb', 'circ_mv_yi']:
            # 数值字段筛选
            if self.operator == 'range':
                if isinstance(self.value, (list, tuple)) and len(self.value) == 2:
                    min_val, max_val = self.value
                    result = df[(df[self.field] >= min_val) & (df[self.field] <= max_val)]
                    logger.info(f"[FieldFilterCriteria] Numeric range filter applied, field: {self.field}, range: [{min_val}, {max_val}], input: {len(df)}, output: {len(result)}")
                    return result
            elif self.operator in ['gt', 'lt', 'gte', 'lte', 'eq']:
                if not isinstance(self.value, (int, float)):
                    raise ValueError(f"{self.field} 字段比较条件需要单个数值")
                threshold = self.value
                if self.operator == 'gt':
                    result = df[df[self.field] > threshold]
                    logger.info(f"[FieldFilterCriteria] Numeric filter applied, field: {self.field}, operator: {self.operator}, threshold: {threshold}, input: {len(df)}, output: {len(result)}")
                    return result
                elif self.operator == 'lt':
                    result = df[df[self.field] < threshold]
                    logger.info(f"[FieldFilterCriteria] Numeric filter applied, field: {self.field}, operator: {self.operator}, threshold: {threshold}, input: {len(df)}, output: {len(result)}")
                    return result
                elif self.operator == 'gte':
                    result = df[df[self.field] >= threshold]
                    logger.info(f"[FieldFilterCriteria] Numeric filter applied, field: {self.field}, operator: {self.operator}, threshold: {threshold}, input: {len(df)}, output: {len(result)}")
                    return result
                elif self.operator == 'lte':
                    result = df[df[self.field] <= threshold]
                    logger.info(f"[FieldFilterCriteria] Numeric filter applied, field: {self.field}, operator: {self.operator}, threshold: {threshold}, input: {len(df)}, output: {len(result)}")
                    return result
                elif self.operator == 'eq':
                    result = df[df[self.field] == threshold]
                    logger.info(f"[FieldFilterCriteria] Numeric filter applied, field: {self.field}, operator: {self.operator}, value: {threshold}, input: {len(df)}, output: {len(result)}")
                    return result
            else:
                # 无效操作符
                logger.warning(f"[FieldFilterCriteria] Invalid operator '{self.operator}' for numeric field '{self.field}', returning all")
                return df

        # 技术指标字段（turnover_rate, avg_amplitude, positive_days）
        # 这些需要历史数据计算，暂时返回空DataFrame
        # TODO: 实现这些指标的计算逻辑
        logger.info(f"[FieldFilterCriteria] Technical indicator field '{self.field}' not implemented yet, returning empty DataFrame")
        return pd.DataFrame()

    def to_config(self) -> Dict:
        """
        导出为配置字典

        格式：
        {
            "type": "FieldFilter",
            "field": "market" | "industry" | "pe_ttm" | ...,
            "operator": "in" | "eq" | "gt" | "lt" | ...,
            "value": ...
        }
        """
        return {
            'type': 'FieldFilter',
            'field': self.field,
            'operator': self.operator,
            'value': self.value
        }

    @classmethod
    def from_config(cls, config: Dict) -> 'FieldFilterCriteria':
        """
        从配置创建实例

        Args:
            config: 配置字典，必须包含：
            - type: "FieldFilter"
            - field: 字段名称
            - operator: 运算符
            - value: 字段值
        """
        if config.get('type') != 'FieldFilter':
            raise ValueError(f"配置类型错误: {config.get('type')}, 期望 FieldFilter")

        field = config.get('field')
        operator = config.get('operator', 'eq')
        value = config.get('value')

        return cls(field=field, operator=operator, value=value)
