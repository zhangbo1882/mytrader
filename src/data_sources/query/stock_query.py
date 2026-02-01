"""
通用查询实现类
提供所有查询方法的具体实现
"""
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional, List, Dict, Tuple
from datetime import datetime, timedelta
import numpy as np

from .base_query import BaseQuery


class StockQuery(BaseQuery):
    """
    股票数据查询实现类
    提供丰富的查询功能
    """

    def __init__(self, db_path: str):
        """
        初始化查询器

        Args:
            db_path: 数据库文件路径
        """
        self.engine = create_engine(f"sqlite:///{db_path}", echo=False)
        self._cache = {}  # 简单的查询缓存

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
        query = """
        SELECT * FROM bars
        WHERE symbol = :symbol
          AND interval = :interval
          AND datetime >= :start
          AND datetime <= :end
        ORDER BY datetime
        """

        df = pd.read_sql_query(
            query,
            self.engine,
            params={"symbol": symbol, "interval": interval, "start": start, "end": end}
        )

        if df.empty:
            return df

        df["datetime"] = pd.to_datetime(df["datetime"])

        # 根据价格类型选择对应的列
        if price_type == 'qfq' and 'close_qfq' in df.columns:
            df['open'] = df['open_qfq']
            df['high'] = df['high_qfq']
            df['low'] = df['low_qfq']
            df['close'] = df['close_qfq']

        return df

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
        results = {}
        for symbol in symbols:
            try:
                df = self.query_bars(symbol, start, end, interval, price_type)
                results[symbol] = df
            except Exception as e:
                print(f"⚠️  查询 {symbol} 失败: {e}")
                results[symbol] = pd.DataFrame()
        return results

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
        df = self.query_bars(symbol, start, end)

        if df.empty:
            return df

        # 应用价格过滤
        mask = pd.Series([True] * len(df))
        if min_price is not None:
            mask &= (df[price_column] >= min_price)
        if max_price is not None:
            mask &= (df[price_column] <= max_price)

        return df[mask].copy()

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
        df = self.query_bars(symbol, start, end)

        if df.empty:
            return df

        mask = pd.Series([True] * len(df))
        if min_volume is not None:
            mask &= (df['volume'] >= min_volume)
        if max_volume is not None:
            mask &= (df['volume'] <= max_volume)

        return df[mask].copy()

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
        df = self.query_bars(symbol, start, end)

        if df.empty or 'turnover' not in df.columns:
            return pd.DataFrame()

        mask = pd.Series([True] * len(df))
        if min_turnover is not None:
            mask &= (df['turnover'] >= min_turnover)
        if max_turnover is not None:
            mask &= (df['turnover'] <= max_turnover)

        return df[mask].copy()

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
        df = self.query_bars(symbol, start, end)

        if df.empty or 'pct_chg' not in df.columns:
            return pd.DataFrame()

        mask = pd.Series([True] * len(df))
        if min_change is not None:
            mask &= (df['pct_chg'] >= min_change)
        if max_change is not None:
            mask &= (df['pct_chg'] <= max_change)

        return df[mask].copy()

    def query_with_filters(self, symbol: str, start: str, end: str,
                           filters: Dict[str, Tuple[Optional[float], Optional[float]]]) -> pd.DataFrame:
        """
        复合条件查询

        Args:
            symbol: 股票代码
            start: 开始日期
            end: 结束日期
            filters: 过滤条件字典

        Returns:
            符合所有条件的DataFrame
        """
        df = self.query_bars(symbol, start, end)

        if df.empty:
            return df

        mask = pd.Series([True] * len(df))

        for column, (min_val, max_val) in filters.items():
            if column not in df.columns:
                continue

            if min_val is not None:
                mask &= (df[column] >= min_val)
            if max_val is not None:
                mask &= (df[column] <= max_val)

        return df[mask].copy()

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
            包含完整性信息的字典
        """
        df = self.query_bars(symbol, start, end, interval)

        result = {
            'total_days': 0,
            'actual_days': 0,
            'missing_count': 0,
            'completeness_rate': 0.0,
            'missing_days': [],
            'first_date': None,
            'last_date': None
        }

        if df.empty:
            return result

        # 生成期望的交易日序列（排除周末）
        start_dt = pd.to_datetime(start)
        end_dt = pd.to_datetime(end)
        expected_dates = pd.date_range(start=start_dt, end=end_dt, freq='B')  # B = 工作日

        result['total_days'] = len(expected_dates)
        result['actual_days'] = len(df)

        actual_dates = set(df['datetime'].dt.date)
        expected_dates_set = set(expected_dates.date)

        missing_dates = expected_dates_set - actual_dates
        result['missing_count'] = len(missing_dates)
        result['missing_days'] = sorted([d.strftime('%Y-%m-%d') for d in missing_dates])

        if result['total_days'] > 0:
            result['completeness_rate'] = result['actual_days'] / result['total_days']

        result['first_date'] = df['datetime'].min().strftime('%Y-%m-%d')
        result['last_date'] = df['datetime'].max().strftime('%Y-%m-%d')

        return result

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
        info = self.check_data_completeness(symbol, start, end, interval)
        return info['missing_days']

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
        df = self.query_bars(symbol, start, end)

        if df.empty:
            return df

        df = df.sort_values('datetime').copy()
        df['return'] = df['close'].pct_change(period) * 100

        return df

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
        df = self.calculate_returns(symbol, start, end)

        if df.empty:
            return df

        # 计算滚动标准差作为波动率
        df['volatility'] = df['return'].rolling(window=window).std()

        return df

    def get_summary_stats(self, symbol: str, start: str, end: str) -> Dict:
        """
        获取汇总统计信息

        Args:
            symbol: 股票代码
            end: 结束日期

        Returns:
            统计信息字典
        """
        df = self.query_bars(symbol, start, end)

        stats = {
            'total_days': 0,
            'avg_volume': 0.0,
            'avg_turnover': 0.0,
            'total_return': 0.0,
            'annual_return': 0.0,
            'volatility': 0.0,
            'max_drawdown': 0.0,
            'max_price': 0.0,
            'min_price': 0.0
        }

        if df.empty:
            return stats

        stats['total_days'] = len(df)
        stats['avg_volume'] = df['volume'].mean()
        stats['avg_turnover'] = df['turnover'].mean() if 'turnover' in df.columns else 0.0
        stats['max_price'] = df['high'].max()
        stats['min_price'] = df['low'].min()

        # 计算总收益率
        if len(df) > 0:
            first_price = df.iloc[0]['close']
            last_price = df.iloc[-1]['close']
            stats['total_return'] = ((last_price - first_price) / first_price) * 100

        # 计算年化收益率（假设252个交易日）
        days = len(df)
        if days > 0:
            stats['annual_return'] = ((1 + stats['total_return'] / 100) ** (252 / days) - 1) * 100

        # 计算最大回撤
        df['cummax'] = df['close'].cummax()
        df['drawdown'] = (df['close'] - df['cummax']) / df['cummax'] * 100
        stats['max_drawdown'] = df['drawdown'].min()

        # 计算波动率（日收益率标准差 * sqrt(252)）
        df['daily_return'] = df['close'].pct_change()
        stats['volatility'] = df['daily_return'].std() * np.sqrt(252) * 100 if len(df) > 1 else 0.0

        return stats

    def screen_stocks(self, days: int = 5,
                      turnover_min: Optional[float] = None,
                      turnover_max: Optional[float] = None,
                      pct_chg_min: Optional[float] = None,
                      pct_chg_max: Optional[float] = None,
                      price_min: Optional[float] = None,
                      price_max: Optional[float] = None,
                      volume_min: Optional[float] = None,
                      volume_max: Optional[float] = None) -> pd.DataFrame:
        """
        股票筛选器 - 根据多维度条件筛选股票

        Args:
            days: 筛选天数（过去N个交易日）
            turnover_min/max: 换手率范围（%），要求每天换手率都在此范围内
            pct_chg_min/max: 涨跌幅范围（%）
            price_min/max: 价格区间（元）
            volume_min/max: 成交量区间（股）

        Returns:
            筛选结果DataFrame
        """
        # 获取实际的交易日日期（通过查询bars表获取交易日期）
        end_date = datetime.now().strftime('%Y-%m-%d')

        # 查询过去N个交易日的实际日期范围
        trading_dates_sql = """
        SELECT DISTINCT datetime
        FROM bars
        WHERE interval = '1d'
          AND datetime <= :end_date
        ORDER BY datetime DESC
        LIMIT :days_limit
        """

        with self.engine.connect() as conn:
            # 先查询交易日历
            trading_dates_df = pd.read_sql_query(
                trading_dates_sql,
                conn,
                params={'end_date': end_date, 'days_limit': days}
            )

            if trading_dates_df.empty:
                # 如果没有数据，返回空DataFrame
                return pd.DataFrame()

            # 获取最早和最晚的交易日
            start_date = trading_dates_df['datetime'].min()
            end_date = trading_dates_df['datetime'].max()

            # 构建HAVING子句 - 只有设置了换手率范围才添加
            having_clauses = [f"COUNT(*) >= :required_trading_days"]

            # 换手率使用MIN()判断，确保每天换手率都满足条件
            if turnover_min is not None:
                having_clauses.append(f"MIN(b.turnover) >= :turnover_min")
            if turnover_max is not None:
                having_clauses.append(f"MIN(b.turnover) <= :turnover_max")

            having_clause = " AND ".join(having_clauses)

            # 构建SQL查询
            query_sql = f"""
                SELECT
                    b.symbol,
                    sn.name,
                    COUNT(*) as trading_days,
                    AVG(b.turnover) as avg_turnover,
                    MIN(b.turnover) as min_turnover,
                    MAX(b.turnover) as max_turnover,
                    AVG(b.pct_chg) as avg_pct_chg,
                    MIN(b.pct_chg) as min_pct_chg,
                    MAX(b.pct_chg) as max_pct_chg,
                    AVG(b.close) as avg_close,
                    MIN(b.low) as min_low,
                    MAX(b.high) as max_high,
                    AVG(b.volume) as avg_volume,
                    MAX(b.datetime) as latest_date,
                    (SELECT close FROM bars b2
                     WHERE b2.symbol = b.symbol
                     AND b2.interval = '1d'
                     ORDER BY b2.datetime DESC LIMIT 1) as latest_close
                FROM bars b
                JOIN stock_names sn ON b.symbol = sn.code
                WHERE b.interval = '1d'
                  AND b.datetime >= :start_date
                  AND b.datetime <= :end_date
                  AND (:pct_chg_min IS NULL OR b.pct_chg >= :pct_chg_min)
                  AND (:pct_chg_max IS NULL OR b.pct_chg <= :pct_chg_max)
                  AND (:price_min IS NULL OR b.close >= :price_min)
                  AND (:price_max IS NULL OR b.close <= :price_max)
                  AND (:volume_min IS NULL OR b.volume >= :volume_min)
                  AND (:volume_max IS NULL OR b.volume <= :volume_max)
                GROUP BY b.symbol, sn.name
                HAVING
                    {having_clause}
                ORDER BY avg_turnover DESC
                LIMIT 500
            """

            # 执行查询
            df = pd.read_sql_query(
                query_sql,
                conn,
                params={
                    'start_date': start_date,
                    'end_date': end_date,
                    'turnover_min': turnover_min,
                    'turnover_max': turnover_max,
                    'pct_chg_min': pct_chg_min,
                    'pct_chg_max': pct_chg_max,
                    'price_min': price_min,
                    'price_max': price_max,
                    'volume_min': volume_min,
                    'volume_max': volume_max,
                    'required_trading_days': int(days * 0.8)  # 允许20%停牌
                }
            )

        return df
