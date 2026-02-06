"""
Quarterly Financial Data Loader Module

Loads and prepares quarterly financial data for ML training.
This data loader specializes in:
- Loading quarterly financial statements (income, balance sheet, cash flow)
- Loading financial indicators (48 metrics)
- Calculating next quarter returns as target variable
- Supporting multiple feature modes (financial_only, with_reports, with_valuation)
"""
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Tuple
from datetime import datetime
import logging

from src.data_sources.query.stock_query import StockQuery
from src.data_sources.query.financial_query import FinancialQuery
from .quarterly_utils import (
    align_quarterly_data_with_prices,
    parse_quarter,
    generate_quarter_range,
    get_quarterly_financial_columns,
    get_income_statement_columns,
    get_balance_sheet_columns,
    get_cashflow_columns,
    get_valuation_columns
)

logger = logging.getLogger(__name__)


class QuarterlyFinancialDataLoader:
    """
    季度财务数据加载器

    负责从数据库提取季度财务数据并计算下一季度收益率作为目标变量

    支持三种特征模式：
    - financial_only: 仅使用fina_indicator表的48个指标
    - with_reports: 财务指标 + 三张报表核心字段
    - with_valuation: 财务指标 + 估值指标（PE、PB、市值等）
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

    def load_quarterly_data(
        self,
        symbols: List[str],
        start_quarter: str,
        end_quarter: str,
        feature_mode: str = "financial_only",
        price_type: str = "qfq",
        window_days: int = 60,
        skip_days: int = 5
    ) -> pd.DataFrame:
        """
        加载多只股票的季度财务数据

        Args:
            symbols: 股票代码列表
            start_quarter: 开始季度（如"2020Q1"）
            end_quarter: 结束季度（如"2024Q4"）
            feature_mode: 特征模式
                - "financial_only": 仅财务指标
                - "with_reports": 财务指标 + 报表数据
                - "with_valuation": 财务指标 + 估值指标
            price_type: 价格类型（""=不复权, "qfq"=前复权）
            window_days: 计算收益率的交易天数窗口
            skip_days: 公告日后跳过的交易天数

        Returns:
            包含特征和目标变量的DataFrame
        """
        all_data = []

        for symbol in symbols:
            try:
                symbol_data = self.load_single_symbol(
                    symbol=symbol,
                    start_quarter=start_quarter,
                    end_quarter=end_quarter,
                    feature_mode=feature_mode,
                    price_type=price_type,
                    window_days=window_days,
                    skip_days=skip_days
                )

                if not symbol_data.empty:
                    symbol_data['symbol'] = symbol
                    all_data.append(symbol_data)
                    logger.info(f"Loaded {len(symbol_data)} quarters for {symbol}")
                else:
                    logger.warning(f"No data loaded for {symbol}")

            except Exception as e:
                logger.error(f"Failed to load data for {symbol}: {e}")
                continue

        if not all_data:
            logger.error("No data loaded for any symbols")
            return pd.DataFrame()

        # 合并所有股票数据
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total samples loaded: {len(combined_df)}")

        return combined_df

    def load_single_symbol(
        self,
        symbol: str,
        start_quarter: str,
        end_quarter: str,
        feature_mode: str = "financial_only",
        price_type: str = "qfq",
        window_days: int = 60,
        skip_days: int = 5
    ) -> pd.DataFrame:
        """
        加载单只股票的季度财务数据

        Args:
            symbol: 股票代码
            start_quarter: 开始季度
            end_quarter: 结束季度
            feature_mode: 特征模式
            price_type: 价格类型
            window_days: 收益率计算窗口
            skip_days: 跳过天数

        Returns:
            单只股票的季度数据DataFrame
        """
        # 1. 加载财务指标数据（fina_indicator表）
        fina_indicator_df = self._load_fina_indicator(
            symbol, start_quarter, end_quarter
        )

        if fina_indicator_df.empty:
            logger.warning(f"No fina_indicator data for {symbol}")
            return pd.DataFrame()

        # 2. 根据模式加载额外数据
        if feature_mode == "with_reports":
            report_df = self._load_financial_reports(
                symbol, start_quarter, end_quarter
            )
            if not report_df.empty:
                fina_indicator_df = self._merge_reports(
                    fina_indicator_df, report_df
                )
        elif feature_mode == "with_valuation":
            report_df = self._load_financial_reports(
                symbol, start_quarter, end_quarter
            )
            if not report_df.empty:
                fina_indicator_df = self._merge_reports(
                    fina_indicator_df, report_df
                )

            # 添加估值指标（需要价格数据）
            valuation_df = self._load_valuation_metrics(
                symbol, start_quarter, end_quarter, price_type
            )
            if not valuation_df.empty:
                fina_indicator_df = self._merge_valuation(
                    fina_indicator_df, valuation_df
                )

        # 3. 加载价格数据并计算目标变量（下一季度收益率）
        price_df = self._load_price_data(
            symbol, start_quarter, end_quarter, price_type
        )

        if price_df.empty:
            logger.warning(f"No price data for {symbol}")
            return pd.DataFrame()

        # 4. 对齐财务数据与价格数据，计算下一季度收益率
        result_df = align_quarterly_data_with_prices(
            fina_indicator_df,
            price_df,
            window_days=window_days,
            skip_days=skip_days
        )

        if result_df.empty:
            logger.warning(f"No aligned data for {symbol}")
            return pd.DataFrame()

        # 5. 添加时间特征
        result_df = self._add_quarter_features(result_df)

        return result_df

    def _load_fina_indicator(
        self,
        symbol: str,
        start_quarter: str,
        end_quarter: str
    ) -> pd.DataFrame:
        """
        加载财务指标数据

        Args:
            symbol: 股票代码
            start_quarter: 开始季度
            end_quarter: 结束季度

        Returns:
            财务指标DataFrame
        """
        # 将季度转换为日期（使用季度结束日期）
        start_date = parse_quarter(start_quarter)
        end_year, end_q = parse_quarter(end_quarter)

        # 结束日期往后推一个季度，确保包含结束季度的数据
        from .utils.quarterly_utils import quarter_to_date, add_quarters
        end_date = add_quarters(end_quarter, 1)

        # 转换为YYYYMMDD格式
        start_yyyymmdd = f"{start_date[0]}{start_date[1] * 3:02d}01"
        end_yyyymmdd = f"{end_date[:4]}{int(end_date[5:]) * 3:02d}31"

        # 查询财务指标
        df = self.financial_query.query_fina_indicator(
            symbol,
            start_date=start_yyyymmdd,
            end_date=end_yyyymmdd,
            report_type='1'  # 合并报表
        )

        if df.empty:
            return pd.DataFrame()

        # 过滤并排序
        df = df[df['end_date'].notna()].copy()
        df = df.sort_values('end_date').reset_index(drop=True)

        # 只保留需要的财务指标列
        financial_columns = get_quarterly_financial_columns()
        all_indicator_cols = []
        for category_cols in financial_columns.values():
            all_indicator_cols.extend(category_cols)

        # 保留基础列和指标列
        base_cols = ['ts_code', 'ann_date', 'end_date', 'report_type']
        keep_cols = [c for c in base_cols if c in df.columns]
        keep_cols.extend([c for c in all_indicator_cols if c in df.columns])

        df = df[keep_cols].copy()

        return df

    def _load_financial_reports(
        self,
        symbol: str,
        start_quarter: str,
        end_quarter: str
    ) -> pd.DataFrame:
        """
        加载三张财务报表的核心字段

        Args:
            symbol: 股票代码
            start_quarter: 开始季度
            end_quarter: 结束季度

        Returns:
            合并后的报表数据DataFrame
        """
        # 将季度转换为日期
        start_date = parse_quarter(start_quarter)

        from .utils.quarterly_utils import add_quarters
        end_date = add_quarters(end_quarter, 1)

        # 转换为YYYYMMDD格式
        start_yyyymmdd = f"{start_date[0]}{start_date[1] * 3:02d}01"
        end_yyyymmdd = f"{end_date[:4]}{int(end_date[5:]) * 3:02d}31"

        # 加载三张报表
        income_df = self.financial_query.query_income(
            symbol,
            start_date=start_yyyymmdd,
            end_date=end_yyyymmdd,
            report_type='1'
        )

        balance_df = self.financial_query.query_balancesheet(
            symbol,
            start_date=start_yyyymmdd,
            end_date=end_yyyymmdd,
            report_type='1'
        )

        cashflow_df = self.financial_query.query_cashflow(
            symbol,
            start_date=start_yyyymmdd,
            end_date=end_yyyymmdd,
            report_type='1'
        )

        # 合并报表
        merged = pd.DataFrame()

        if not income_df.empty:
            income_cols = ['ann_date', 'end_date'] + get_income_statement_columns()
            income_cols = [c for c in income_cols if c in income_df.columns]
            merged = income_df[income_cols].copy()

        if not balance_df.empty and not merged.empty:
            balance_cols = get_balance_sheet_columns()
            balance_cols = [c for c in balance_cols if c in balance_df.columns]
            balance_to_merge = balance_df[['ann_date', 'end_date'] + balance_cols].copy()
            merged = pd.merge(merged, balance_to_merge, on=['ann_date', 'end_date'], how='outer')
        elif not balance_df.empty and merged.empty:
            balance_cols = get_balance_sheet_columns()
            balance_cols = [c for c in balance_cols if c in balance_df.columns]
            merged = balance_df[['ann_date', 'end_date'] + balance_cols].copy()

        if not cashflow_df.empty and not merged.empty:
            cashflow_cols = get_cashflow_columns()
            cashflow_cols = [c for c in cashflow_cols if c in cashflow_df.columns]
            cashflow_to_merge = cashflow_df[['ann_date', 'end_date'] + cashflow_cols].copy()
            merged = pd.merge(merged, cashflow_to_merge, on=['ann_date', 'end_date'], how='outer')
        elif not cashflow_df.empty and merged.empty:
            cashflow_cols = get_cashflow_columns()
            cashflow_cols = [c for c in cashflow_cols if c in cashflow_df.columns]
            merged = cashflow_df[['ann_date', 'end_date'] + cashflow_cols].copy()

        return merged

    def _load_valuation_metrics(
        self,
        symbol: str,
        start_quarter: str,
        end_quarter: str,
        price_type: str = "qfq"
    ) -> pd.DataFrame:
        """
        加载估值指标（从bars表获取）

        Args:
            symbol: 股票代码
            start_quarter: 开始季度
            end_quarter: 结束季度
            price_type: 价格类型

        Returns:
            估值指标DataFrame
        """
        # 将季度转换为日期范围
        from .utils.quarterly_utils import quarter_to_date

        start_dt = quarter_to_date(start_quarter)
        end_dt = quarter_to_date(add_quarters(end_quarter, 1))

        start_date_str = start_dt.strftime('%Y-%m-%d')
        end_date_str = end_dt.strftime('%Y-%m-%d')

        # 加载日线数据
        price_df = self.stock_query.query_bars(
            symbol,
            start_date_str,
            end_date_str,
            price_type=price_type
        )

        if price_df.empty:
            return pd.DataFrame()

        # 提取估值指标列
        valuation_cols = get_valuation_columns()
        valuation_cols = [c for c in valuation_cols if c in price_df.columns]

        if not valuation_cols:
            logger.warning(f"No valuation columns found in price data for {symbol}")
            return pd.DataFrame()

        # 只保留估值指标和日期
        result = price_df[['datetime'] + valuation_cols].copy()

        # 添加ann_date列用于合并（使用最接近的交易日）
        result['ann_date_temp'] = result['datetime'].dt.strftime('%Y%m%d')

        return result

    def _load_price_data(
        self,
        symbol: str,
        start_quarter: str,
        end_quarter: str,
        price_type: str = "qfq"
    ) -> pd.DataFrame:
        """
        加载日线价格数据

        Args:
            symbol: 股票代码
            start_quarter: 开始季度
            end_quarter: 结束季度
            price_type: 价格类型

        Returns:
            价格数据DataFrame
        """
        from .utils.quarterly_utils import quarter_to_date, add_quarters

        # 扩展日期范围以包含下一季度的数据
        start_dt = quarter_to_date(start_quarter)
        end_dt = quarter_to_date(add_quarters(end_quarter, 2))  # 往后推2季度

        start_date_str = start_dt.strftime('%Y-%m-%d')
        end_date_str = end_dt.strftime('%Y-%m-%d')

        # 加载日线数据
        price_df = self.stock_query.query_bars(
            symbol,
            start_date_str,
            end_date_str,
            price_type=price_type
        )

        if price_df.empty:
            return pd.DataFrame()

        # 确保日期格式正确
        price_df['datetime'] = pd.to_datetime(price_df['datetime'])

        return price_df

    def _merge_reports(
        self,
        fina_df: pd.DataFrame,
        report_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        合并财务指标和报表数据

        Args:
            fina_df: 财务指标DataFrame
            report_df: 报表数据DataFrame

        Returns:
            合并后的DataFrame
        """
        # 使用end_date进行合并
        merged = pd.merge(
            fina_df,
            report_df,
            on=['ann_date', 'end_date'],
            how='left'
        )

        return merged

    def _merge_valuation(
        self,
        fina_df: pd.DataFrame,
        valuation_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        合并财务数据和估值指标

        使用最接近公告日的估值指标

        Args:
            fina_df: 财务数据DataFrame
            valuation_df: 估值指标DataFrame

        Returns:
            合并后的DataFrame
        """
        # 确保日期格式正确
        fina_df['ann_date_dt'] = pd.to_datetime(fina_df['ann_date'], format='%Y%m%d')
        valuation_df = valuation_df.copy()
        valuation_df['datetime'] = pd.to_datetime(valuation_df['datetime'])

        # 对每个财务数据日期，找到最接近的估值指标
        merged = pd.merge_asof(
            fina_df.sort_values('ann_date_dt'),
            valuation_df.sort_values('datetime'),
            left_on='ann_date_dt',
            right_on='datetime',
            direction='backward'  # 使用公告日之前或当天的估值数据
        )

        # 删除临时列
        merged = merged.drop(columns=['ann_date_dt'])

        return merged

    def _add_quarter_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加季度相关特征

        Args:
            df: 数据DataFrame

        Returns:
            添加特征后的DataFrame
        """
        df = df.copy()

        # 解析季度
        df['end_date_dt'] = pd.to_datetime(df['end_date'], format='%Y%m%d')

        # 提取年份和季度
        df['year'] = df['end_date_dt'].dt.year
        df['quarter'] = df['end_date_dt'].dt.quarter

        # 季度哑变量
        df['is_q1'] = (df['quarter'] == 1).astype(int)
        df['is_q2'] = (df['quarter'] == 2).astype(int)
        df['is_q3'] = (df['quarter'] == 3).astype(int)
        df['is_q4'] = (df['quarter'] == 4).astype(int)

        # 删除临时列
        df = df.drop(columns=['end_date_dt'])

        return df

    def get_feature_columns(
        self,
        df: pd.DataFrame,
        exclude_cols: Optional[List[str]] = None
    ) -> List[str]:
        """
        获取特征列名

        Args:
            df: 数据DataFrame
            exclude_cols: 要排除的列名列表

        Returns:
            特征列名列表
        """
        exclude = exclude_cols or [
            'symbol', 'ts_code', 'ann_date', 'end_date', 'report_type',
            'next_quarter_return', 'datetime', 'ann_date_temp'
        ]

        feature_cols = [
            c for c in df.columns
            if c not in exclude
            and pd.api.types.is_numeric_dtype(df[c])
        ]

        return feature_cols
