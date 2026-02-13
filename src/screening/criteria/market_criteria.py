"""
市场/交易所筛选条件

提供基于市场（交易所）的筛选：
- MarketFilter: 市场白名单/黑名单
支持：主板、创业板、科创板、北交所
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
    """

    # 所有支持的市场类型
    ALL_MARKETS = ['主板', '创业板', '科创板', '北交所']

    def __init__(self, markets: List[str], mode: str = 'whitelist'):
        """
        Args:
            markets: 市场列表（可选值：主板、创业板、科创板、北交所）
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

        if 'market' not in df.columns:
            logger.warning(f"[MarketFilter] 'market' column not found in DataFrame")
            return df

        if self.mode == 'whitelist':
            result = df[df['market'].isin(self.markets)].copy()
            logger.info(f"[MarketFilter] Whitelist filter applied, markets: {len(self.markets)}, input: {len(df)}, output: {len(result)}")
            return result.reset_index(drop=True)
        else:
            result = df[~df['market'].isin(self.markets)].copy()
            logger.info(f"[MarketFilter] Blacklist filter applied, markets: {len(self.markets)}, input: {len(df)}, output: {len(result)}")
            return result.reset_index(drop=True)

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
