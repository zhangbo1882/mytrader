"""
ML Data Loader Module

Loads and prepares data for machine learning training.
Merges price data (daily) with financial data (quarterly, forward-filled).
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timedelta
import logging

from src.data_sources.query.stock_query import StockQuery
from src.data_sources.query.financial_query import FinancialQuery

logger = logging.getLogger(__name__)


class MlDataLoader:
    """
    ML数据加载器

    负责从数据库提取和准备机器学习训练数据：
    - 加载OHLCV数据（日线）
    - 加载财务数据（季报，前向填充到日线）
    - 合并为统一的时间序列数据集
    """

    def __init__(self, db_path: str = "data/tushare_data.db"):
        """
        初始化数据加载器

        Args:
            db_path: 数据库路径
        """
        self.db_path = db_path
        self.stock_query = StockQuery(db_path)
        self.financial_query = FinancialQuery(db_path)

    def load_training_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        price_type: str = "",
        include_financial: bool = True
    ) -> pd.DataFrame:
        """
        加载训练数据（价格 + 财务）

        Args:
            symbol: 股票代码
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            price_type: 价格类型 ''=不复权, 'qfq'=前复权
            include_financial: 是否包含财务数据

        Returns:
            包含价格和财务特征的DataFrame
        """
        # 1. 加载价格数据
        logger.info(f"Loading price data for {symbol} from {start_date} to {end_date}")
        price_df = self.stock_query.query_bars(symbol, start_date, end_date, price_type=price_type)

        if price_df.empty:
            logger.warning(f"No price data found for {symbol}")
            return pd.DataFrame()

        # 2. 添加基本价格特征
        price_df = self._add_basic_price_features(price_df)

        # 3. 加载并合并财务数据
        if include_financial:
            financial_df = self._load_financial_features(symbol, start_date, end_date)
            if not financial_df.empty:
                price_df = self._merge_financial_data(price_df, financial_df)

        # 4. 添加时间特征
        price_df = self._add_time_features(price_df)

        # 5. 删除包含NaN的行
        price_df = price_df.dropna()

        logger.info(f"Loaded {len(price_df)} rows of training data")
        return price_df

    def load_multiple_symbols(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        price_type: str = ""
    ) -> Dict[str, pd.DataFrame]:
        """
        批量加载多只股票的训练数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            price_type: 价格类型

        Returns:
            字典 {symbol: DataFrame}
        """
        results = {}
        for symbol in symbols:
            try:
                df = self.load_training_data(symbol, start_date, end_date, price_type)
                if not df.empty:
                    results[symbol] = df
            except Exception as e:
                logger.error(f"Failed to load data for {symbol}: {e}")
                results[symbol] = pd.DataFrame()
        return results

    def _add_basic_price_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加基本价格特征

        Args:
            df: 价格数据DataFrame

        Returns:
            添加特征后的DataFrame
        """
        df = df.copy()

        # 确保按时间排序
        df = df.sort_values('datetime').reset_index(drop=True)

        # 收益率
        df['returns_1d'] = df['close'].pct_change()
        df['returns_5d'] = df['close'].pct_change(5)
        df['returns_20d'] = df['close'].pct_change(20)

        # 对数收益率
        df['log_returns_1d'] = np.log(df['close'] / df['close'].shift(1))

        # 价格位置（相对于60日高低点）
        df['high_60'] = df['high'].rolling(60).max()
        df['low_60'] = df['low'].rolling(60).min()
        df['price_position'] = (df['close'] - df['low_60']) / (df['high_60'] - df['low_60'])

        # 成交量特征
        df['volume_ma_5'] = df['volume'].rolling(5).mean()
        df['volume_ma_20'] = df['volume'].rolling(20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_ma_20']

        # 换手率特征
        if 'turnover' in df.columns:
            df['turnover_ma_5'] = df['turnover'].rolling(5).mean()
            df['turnover_ma_20'] = df['turnover'].rolling(20).mean()

        # 波动率（20日收益率标准差）
        df['volatility_20'] = df['returns_1d'].rolling(20).std()

        return df

    def _load_financial_features(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        加载财务数据并计算特征

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            财务特征DataFrame
        """
        # 转换日期格式 YYYY-MM-DD -> YYYYMMDD
        start_yyyymmdd = start_date.replace('-', '')
        end_yyyymmdd = end_date.replace('-', '')

        # 加载三张报表
        income_df = self.financial_query.query_income(symbol, start_yyyymmdd, end_yyyymmdd)
        balance_df = self.financial_query.query_balancesheet(symbol, start_yyyymmdd, end_yyyymmdd)
        cashflow_df = self.financial_query.query_cashflow(symbol, start_yyyymmdd, end_yyyymmdd)

        if income_df.empty and balance_df.empty:
            return pd.DataFrame()

        # 选择合并报表 (report_type = '1')
        if not income_df.empty:
            income_df = income_df[income_df['report_type'] == '1'].copy()
        if not balance_df.empty:
            balance_df = balance_df[balance_df['report_type'] == '1'].copy()
        if not cashflow_df.empty:
            cashflow_df = cashflow_df[cashflow_df['report_type'] == '1'].copy()

        # 提取关键财务指标并计算特征
        financial_features = pd.DataFrame()

        if not income_df.empty:
            # 使用 end_date 作为报告期
            income_df['report_date'] = pd.to_datetime(income_df['end_date'], format='%Y%m%d')

            # 营业收入增长率（同比）
            income_df = income_df.sort_values('report_date')
            income_df['revenue'] = income_df['total_revenue'].combine_first(income_df['revenue'])
            income_df['revenue_growth_yoy'] = income_df['revenue'].pct_change(4)  # 4个季度前

            # 净利润增长率
            income_df['net_income_growth_yoy'] = income_df['n_income_attr_p'].pct_change(4)

            # 毛利率
            income_df['gross_margin'] = (income_df['revenue'] - income_df['oper_cost']) / income_df['revenue'] * 100

            # 净利率
            income_df['net_margin'] = income_df['n_income_attr_p'] / income_df['revenue'] * 100

            financial_features['report_date'] = income_df['report_date']
            financial_features['revenue'] = income_df['revenue']
            financial_features['revenue_growth_yoy'] = income_df['revenue_growth_yoy']
            financial_features['net_income'] = income_df['n_income_attr_p']
            financial_features['net_income_growth_yoy'] = income_df['net_income_growth_yoy']
            financial_features['gross_margin'] = income_df['gross_margin']
            financial_features['net_margin'] = income_df['net_margin']
            financial_features['eps'] = income_df['basic_eps']

        if not balance_df.empty:
            balance_df['report_date'] = pd.to_datetime(balance_df['end_date'], format='%Y%m%d')
            balance_df = balance_df.sort_values('report_date')

            # ROE
            balance_df['roe'] = balance_df['n_income_attr_p'] / balance_df['total_owner_equities'] * 100

            # ROA
            balance_df['roa'] = balance_df['n_income_attr_p'] / balance_df['total_assets'] * 100

            # 资产负债率
            balance_df['debt_ratio'] = balance_df['total_liability'] / balance_df['total_assets'] * 100

            # 流动比率
            balance_df['current_ratio'] = balance_df['total_current_assets'] / balance_df['total_current_liability']

            if financial_features.empty:
                financial_features['report_date'] = balance_df['report_date']

            financial_features = financial_features.merge(
                balance_df[['report_date', 'total_assets', 'total_liability', 'roe', 'roa', 'debt_ratio', 'current_ratio']],
                on='report_date',
                how='outer'
            )

        if financial_features.empty:
            return pd.DataFrame()

        # 计算估值指标（需要价格数据）
        # 这里先返回基础财务特征，在合并时计算PE/PB

        return financial_features.sort_values('report_date')

    def _merge_financial_data(self, price_df: pd.DataFrame, financial_df: pd.DataFrame) -> pd.DataFrame:
        """
        合并价格数据和财务数据

        财务数据是季度的，需要前向填充到每个交易日

        Args:
            price_df: 日线价格数据
            financial_df: 季度财务数据

        Returns:
            合并后的DataFrame
        """
        # 使用 datetime 列进行合并
        price_df = price_df.sort_values('datetime')
        financial_df = financial_df.sort_values('report_date')

        # 对每个交易日，找到最近的财务报告期
        merged = pd.merge_asof(
            price_df,
            financial_df,
            left_on='datetime',
            right_on='report_date',
            direction='forward'  # 使用未来的财报数据（已公告的）
        )

        # 计算估值指标
        if 'close' in merged.columns and 'net_income' in merged.columns:
            # PE = 市值 / 净利润 = 股价 * 总股本 / 净利润
            # 这里简化为: PE = 股价 / EPS (如果EPS可用)
            if 'eps' in merged.columns and merged['eps'].notna().any():
                merged['pe'] = merged['close'] / merged['eps']

        if 'close' in merged.columns and 'total_owner_equities' in merged.columns:
            # PB = 市值 / 净资产
            # 简化计算（需要股本数据）
            merged['pb'] = merged['close'] / (merged['total_owner_equities'] / 1e8)  # 粗略估算

        return merged

    def _add_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加时间特征

        Args:
            df: 数据DataFrame

        Returns:
            添加时间特征后的DataFrame
        """
        df = df.copy()

        df['day_of_week'] = df['datetime'].dt.dayofweek
        df['day_of_month'] = df['datetime'].dt.day
        df['month'] = df['datetime'].dt.month
        df['quarter'] = df['datetime'].dt.quarter

        return df

    def prepare_train_val_test(
        self,
        df: pd.DataFrame,
        train_ratio: float = 0.7,
        val_ratio: float = 0.15,
        test_ratio: float = 0.15
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        按时间划分训练集、验证集、测试集

        Args:
            df: 完整数据集
            train_ratio: 训练集比例
            val_ratio: 验证集比例
            test_ratio: 测试集比例

        Returns:
            (train_df, val_df, test_df)
        """
        assert abs(train_ratio + val_ratio + test_ratio - 1.0) < 1e-6

        n = len(df)
        train_end = int(n * train_ratio)
        val_end = int(n * (train_ratio + val_ratio))

        train_df = df.iloc[:train_end].copy()
        val_df = df.iloc[train_end:val_end].copy()
        test_df = df.iloc[val_end:].copy()

        logger.info(f"Train: {len(train_df)}, Val: {len(val_df)}, Test: {len(test_df)}")

        return train_df, val_df, test_df

    def get_feature_columns(self, df: pd.DataFrame, exclude_cols: List[str] = None) -> List[str]:
        """
        获取特征列名（排除目标变量和非特征列）

        Args:
            df: 数据DataFrame
            exclude_cols: 要排除的列名列表

        Returns:
            特征列名列表
        """
        exclude = exclude_cols or [
            'datetime', 'symbol', 'interval', 'report_date', 'ann_date', 'end_date',
            'ts_code'
        ]

        # 排除非数值列和指定的排除列
        feature_cols = [c for c in df.columns
                       if c not in exclude
                       and pd.api.types.is_numeric_dtype(df[c])]

        return feature_cols
