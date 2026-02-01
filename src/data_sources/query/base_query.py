"""
查询抽象基类
定义所有查询方法的抽象接口
"""
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Tuple
import pandas as pd


class BaseQuery(ABC):
    """
    股票数据查询抽象基类
    定义所有查询方法的接口规范
    """

    @abstractmethod
    def query_bars(self, symbol: str, start: str, end: str,
                   interval: str = "1d", price_type: str = "") -> pd.DataFrame:
        """
        查询K线数据

        Args:
            symbol: 股票代码
            start: 开始日期 YYYY-MM-DD
            end: 结束日期 YYYY-MM-DD
            interval: 时间周期，默认 1d
            price_type: 价格类型，''=不复权, 'qfq'=前复权

        Returns:
            包含OHLCV数据的DataFrame
        """
        pass

    @abstractmethod
    def query_multiple_symbols(self, symbols: List[str], start: str, end: str,
                               interval: str = "1d", price_type: str = "") -> Dict[str, pd.DataFrame]:
        """
        批量查询多只股票

        Args:
            symbols: 股票代码列表
            start: 开始日期 YYYY-MM-DD
            end: 结束日期 YYYY-MM-DD
            interval: 时间周期
            price_type: 价格类型

        Returns:
            字典，key为股票代码，value为对应的DataFrame
        """
        pass

    @abstractmethod
    def query_by_price_range(self, symbol: str, start: str, end: str,
                             min_price: Optional[float] = None,
                             max_price: Optional[float] = None,
                             price_column: str = "close") -> pd.DataFrame:
        """
        按价格范围查询

        Args:
            symbol: 股票代码
            start: 开始日期
            end: 结束日期
            min_price: 最低价格，None表示无限制
            max_price: 最高价格，None表示无限制
            price_column: 价格列，默认close

        Returns:
            符合价格范围的DataFrame
        """
        pass

    @abstractmethod
    def query_by_volume(self, symbol: str, start: str, end: str,
                        min_volume: Optional[float] = None,
                        max_volume: Optional[float] = None) -> pd.DataFrame:
        """
        按成交量范围查询

        Args:
            symbol: 股票代码
            start: 开始日期
            end: 结束日期
            min_volume: 最小成交量
            max_volume: 最大成交量

        Returns:
            符合成交量范围的DataFrame
        """
        pass

    @abstractmethod
    def query_by_turnover(self, symbol: str, start: str, end: str,
                          min_turnover: Optional[float] = None,
                          max_turnover: Optional[float] = None) -> pd.DataFrame:
        """
        按换手率范围查询

        Args:
            symbol: 股票代码
            start: 开始日期
            end: 结束日期
            min_turnover: 最小换手率（百分比）
            max_turnover: 最大换手率（百分比）

        Returns:
            符合换手率范围的DataFrame
        """
        pass

    @abstractmethod
    def query_by_change(self, symbol: str, start: str, end: str,
                        min_change: Optional[float] = None,
                        max_change: Optional[float] = None) -> pd.DataFrame:
        """
        按涨跌幅范围查询

        Args:
            symbol: 股票代码
            start: 开始日期
            end: 结束日期
            min_change: 最小涨跌幅（百分比）
            max_change: 最大涨跌幅（百分比）

        Returns:
            符合涨跌幅范围的DataFrame
        """
        pass

    @abstractmethod
    def query_with_filters(self, symbol: str, start: str, end: str,
                           filters: Dict[str, Tuple[Optional[float], Optional[float]]]) -> pd.DataFrame:
        """
        复合条件查询

        Args:
            symbol: 股票代码
            start: 开始日期
            end: 结束日期
            filters: 过滤条件字典，格式：
                {
                    'close': (10, 50),          # 价格 10-50
                    'volume': (1000000, None),  # 成交量 > 100万
                    'pct_chg': (-5, 5)          # 涨跌幅 -5% 到 5%
                }
                元组格式：(最小值, 最大值)，None表示无限制

        Returns:
            符合所有条件的DataFrame
        """
        pass

    @abstractmethod
    def check_data_completeness(self, symbol: str, start: str, end: str,
                                interval: str = "1d") -> Dict:
        """
        检查数据完整性

        Args:
            symbol: 股票代码
            start: 开始日期 YYYY-MM-DD
            end: 结束日期 YYYY-MM-DD
            interval: 时间周期

        Returns:
            包含完整性信息的字典:
            {
                'total_days': int,              # 应有的交易日数量
                'actual_days': int,             # 实际数据天数
                'missing_count': int,           # 缺失天数
                'completeness_rate': float,     # 完整率 0-1
                'missing_days': list,           # 缺失日期列表
                'first_date': str,              # 首个数据日期
                'last_date': str                # 最后数据日期
            }
        """
        pass

    @abstractmethod
    def find_missing_dates(self, symbol: str, start: str, end: str,
                           interval: str = "1d") -> List[str]:
        """
        查找缺失的交易日

        Args:
            symbol: 股票代码
            start: 开始日期
            end: 结束日期
            interval: 时间周期

        Returns:
            缺失日期列表
        """
        pass

    @abstractmethod
    def calculate_returns(self, symbol: str, start: str, end: str,
                          period: int = 1) -> pd.DataFrame:
        """
        计算收益率

        Args:
            symbol: 股票代码
            start: 开始日期
            end: 结束日期
            period: 周期，1=日收益，5=周收益，20=月收益

        Returns:
            包含收益率列的DataFrame
        """
        pass

    @abstractmethod
    def calculate_volatility(self, symbol: str, start: str, end: str,
                             window: int = 20) -> pd.DataFrame:
        """
        计算波动率

        Args:
            symbol: 股票代码
            start: 开始日期
            end: 结束日期
            window: 窗口大小

        Returns:
            包含波动率列的DataFrame
        """
        pass

    @abstractmethod
    def get_summary_stats(self, symbol: str, start: str, end: str) -> Dict:
        """
        获取汇总统计信息

        Args:
            symbol: 股票代码
            start: 开始日期
            end: 结束日期

        Returns:
            统计信息字典:
            {
                'total_days': int,
                'avg_volume': float,
                'avg_turnover': float,
                'total_return': float,
                'annual_return': float,
                'volatility': float,
                'max_drawdown': float,
                'max_price': float,
                'min_price': float
            }
        """
        pass
