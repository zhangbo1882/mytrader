"""
市场/交易所筛选条件

提供基于市场（交易所）的筛选：
- MarketFilter: 市场白名单/黑名单
支持：主板、创业板、科创板、北交所、港股
"""
import logging
import pandas as pd
from typing import Dict, List
from src.screening.base_criteria import BaseCriteria

logger = logging.getLogger(__name__)


class MarketFilter(BaseCriteria):
    """市场过滤：白名单/黑名单模式

    支持的市场类型：
    - 主板：沪深主板
    - 创业板：创业板
    - 科创板：科创板
    - 北交所：北京证券交易所
    - 港股：香港交易所
    """

    # 所有支持的市场类型
    ALL_MARKETS = ['主板', '创业板', '科创板', '北交所', '港股']

    # A股市场类型
    A_STOCK_MARKETS = ['主板', '创业板', '科创板', '北交所']

    def __init__(self, markets: List[str], mode: str = 'whitelist'):
        """
        Args:
            markets: 市场列表（可选值：主板、创业板、科创板、北交所、港股）
            mode: 'whitelist' (白名单) 或 'blacklist' (黑名单)
        """
        # 验证市场类型
        invalid_markets = [m for m in markets if m not in self.ALL_MARKETS]
        if invalid_markets:
            raise ValueError(f"无效的市场类型: {invalid_markets}。有效值: {self.ALL_MARKETS}")

        self.markets = markets
        self.mode = mode

    @property
    def cost(self) -> int:
        return 1  # 极低成本，直接过滤

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.debug(f"[MarketFilter] Starting filter, mode: {self.mode}, markets: {self.markets}, input size: {len(df)}")

        if df.empty:
            logger.warning(f"[MarketFilter] Input DataFrame is empty")
            return df

        if 'market' not in df.columns and 'data_source' not in df.columns:
            logger.warning(f"[MarketFilter] 'market' or 'data_source' column not found in DataFrame")
            return df

        # 处理港股筛选（通过 data_source 字段）
        has_hk_filter = '港股' in self.markets

        # 分离港股和A股数据
        if 'data_source' in df.columns:
            hk_df = df[df['data_source'] == '港股'].copy()
            a_stock_df = df[df['data_source'] == 'A股'].copy()
        else:
            # 没有 data_source 字段，假设都是A股
            hk_df = pd.DataFrame()
            a_stock_df = df.copy()

        # 根据模式处理港股
        if has_hk_filter:
            if self.mode == 'whitelist':
                # 白名单：保留港股
                hk_result = hk_df
            else:
                # 黑名单：排除港股
                hk_result = pd.DataFrame()
        else:
            # 没有选择港股
            if self.mode == 'whitelist':
                # 白名单且没有港股：排除港股
                hk_result = pd.DataFrame()
            else:
                # 黑名单且没有港股：保留港股
                hk_result = hk_df

        # 处理A股市场筛选
        a_markets = [m for m in self.markets if m in self.A_STOCK_MARKETS]
        if a_markets:
            if self.mode == 'whitelist':
                # 白名单：只保留选中的A股市场
                if 'market' in a_stock_df.columns:
                    a_stock_result = a_stock_df[a_stock_df['market'].isin(a_markets)].copy()
                else:
                    a_stock_result = a_stock_df
            else:
                # 黑名单：排除选中的A股市场
                if 'market' in a_stock_df.columns:
                    a_stock_result = a_stock_df[~a_stock_df['market'].isin(a_markets)].copy()
                else:
                    a_stock_result = a_stock_df
        else:
            # 没有选择A股市场
            if self.mode == 'whitelist':
                # 白名单且没有选择A股市场：排除所有A股
                a_stock_result = pd.DataFrame()
            else:
                # 黑名单且没有选择A股市场：保留所有A股
                a_stock_result = a_stock_df

        # 合并结果
        result = pd.concat([hk_result, a_stock_result], ignore_index=True)
        logger.info(f"[MarketFilter] Filter applied, mode: {self.mode}, markets: {self.markets}, input: {len(df)}, output: {len(result)}")
        return result

    def to_config(self) -> Dict:
        return {
            'type': 'MarketFilter',
            'markets': self.markets,
            'mode': self.mode
        }

    @classmethod
    def from_config(cls, config: Dict):
        mode = config.get('mode', 'whitelist')
        # 前端发送 'include'/'exclude'，转换为 'whitelist'/'blacklist'
        if mode == 'include':
            mode = 'whitelist'
        elif mode == 'exclude':
            mode = 'blacklist'
        return cls(
            markets=config['markets'],
            mode=mode
        )
