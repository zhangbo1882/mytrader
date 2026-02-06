"""
Financial Feature Engineer Module

Performs feature engineering on quarterly financial data:
- YoY (Year-over-Year) and QoQ (Quarter-over-Quarter) growth rates
- Financial ratios and combinations
- Trend features (moving average, slope, volatility)
- Seasonal adjustments
"""
import pandas as pd
import numpy as np
from typing import List, Optional, Dict
from scipy import stats
import logging

logger = logging.getLogger(__name__)


class FinancialFeatureEngineer:
    """
    财务特征工程器

    对季度财务数据进行特征工程，生成用于机器学习的特征

    主要功能：
    1. 同比/环比增长率
    2. 财务比率组合
    3. 趋势特征
    4. 季节性特征
    """

    def __init__(self):
        """初始化特征工程器"""
        self.feature_cols: List[str] = []

    def engineer_features(
        self,
        df: pd.DataFrame,
        group_col: str = 'symbol'
    ) -> pd.DataFrame:
        """
        执行完整的特征工程流程

        Args:
            df: 原始财务数据DataFrame
            group_col: 分组列名（用于多股票模式）

        Returns:
            添加特征后的DataFrame
        """
        df = df.copy()

        # 按股票分组处理
        if group_col in df.columns:
            grouped = df.groupby(group_col, group_keys=False)
            df = grouped.apply(self._engineer_single_stock, include_groups=False)
        else:
            df = self._engineer_single_stock(df)

        logger.info(f"Engineered features. Total features: {len(df.columns)}")

        return df

    def _engineer_single_stock(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        对单只股票进行特征工程

        Args:
            df: 单只股票的财务数据

        Returns:
            添加特征后的DataFrame
        """
        df = df.copy()

        # 确保按季度排序
        if 'end_date' in df.columns:
            df['end_date_dt'] = pd.to_datetime(df['end_date'], format='%Y%m%d')
            df = df.sort_values('end_date_dt').reset_index(drop=True)
        elif 'year' in df.columns and 'quarter' in df.columns:
            df = df.sort_values(['year', 'quarter']).reset_index(drop=True)

        # 1. 同比增长率 (Year-over-Year)
        df = self._add_yoy_growth(df)

        # 2. 环比增长率 (Quarter-over-Quarter)
        df = self._add_qoq_growth(df)

        # 3. 财务比率组合
        df = self._add_financial_ratios(df)

        # 4. 趋势特征
        df = self._add_trend_features(df)

        # 5. 综合特征
        df = self._add_composite_features(df)

        return df

    def _add_yoy_growth(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加同比增长率

        同比 = (本期 - 4期前) / 4期前

        Args:
            df: 财务数据DataFrame

        Returns:
            添加同比特征后的DataFrame
        """
        # 定义可以计算同比的指标
        growth_metrics = [
            'eps', 'roe', 'roa', 'netprofit_margin',
            'or_yoy', 'netprofit_yoy', 'assets_yoy',
            'ocfps', 'bps'
        ]

        for metric in growth_metrics:
            if metric in df.columns:
                # 4期前的数据（同比）
                yoy_col = f'{metric}_yoy_calc'
                df[yoy_col] = df[metric].pct_change(4)

                # 处理无穷值和异常值
                df[yoy_col] = df[yoy_col].replace([np.inf, -np.inf], np.nan)

        return df

    def _add_qoq_growth(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加环比增长率

        环比 = (本期 - 1期前) / 1期前

        Args:
            df: 财务数据DataFrame

        Returns:
            添加环比特征后的DataFrame
        """
        # 定义可以计算环比的指标
        growth_metrics = [
            'eps', 'roe', 'roa', 'netprofit_margin',
            'ocfps', 'bps'
        ]

        for metric in growth_metrics:
            if metric in df.columns:
                # 1期前的数据（环比）
                qoq_col = f'{metric}_qoq'
                df[qoq_col] = df[metric].pct_change(1)

                # 处理无穷值和异常值
                df[qoq_col] = df[qoq_col].replace([np.inf, -np.inf], np.nan)

        return df

    def _add_financial_ratios(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加财务比率组合特征

        Args:
            df: 财务数据DataFrame

        Returns:
            添加财务比率后的DataFrame
        """
        # 盈利质量：OCF/债务 / ROE
        if 'ocf_to_debt' in df.columns and 'roe' in df.columns:
            df['profit_quality'] = df['ocf_to_debt'] / (df['roe'] + 1e-6)

        # 杜邦分析相关
        # 净利率 × 总资产周转率 × 权益乘数 ≈ ROE
        if 'netprofit_margin' in df.columns and 'assets_turn' in df.columns:
            if 'debt_to_assets' in df.columns:
                # 权益乘数 = 1 / (1 - 资产负债率)
                equity_multiplier = 1 / (1 - df['debt_to_assets'] / 100 + 1e-6)
                df['dupont_roe'] = (df['netprofit_margin'] / 100) * df['assets_turn'] * equity_multiplier

        # 现金含量：经营现金流 / 净利润
        if 'ocf_to_debt' in df.columns and 'n_income' in df.columns:
            # 简化的现金含量指标
            df['cash_content'] = df['ocf_to_debt'] / (df['n_income'].abs() + 1e-6)
            df['cash_content'] = df['cash_content'].replace([np.inf, -np.inf], np.nan)

        # 市净率相对PE：PB / ROE
        if 'pb' in df.columns and 'roe' in df.columns:
            df['pb_to_roe'] = df['pb'] / (df['roe'] + 1e-6)

        # 营业利润率稳定性：营业利润率标准差（4季度）
        if 'operateprofit_margin' in df.columns:
            df['operateprofit_margin_std_4q'] = df['operateprofit_margin'].rolling(4).std()

        return df

    def _add_trend_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加趋势特征

        包括：
        - 移动平均（4季度）
        - 线性趋势斜率
        - 波动率

        Args:
            df: 财务数据DataFrame

        Returns:
            添加趋势特征后的DataFrame
        """
        # 定义要计算趋势的指标
        trend_metrics = ['eps', 'roe', 'or_yoy', 'netprofit_yoy', 'ocfps']

        for metric in trend_metrics:
            if metric in df.columns:
                # 4季度移动平均
                df[f'{metric}_ma_4q'] = df[metric].rolling(4, min_periods=2).mean()

                # 4季度标准差（波动率）
                df[f'{metric}_std_4q'] = df[metric].rolling(4, min_periods=2).std()

                # 线性趋势斜率（使用最近4个数据点）
                df[f'{metric}_trend_slope'] = df[metric].rolling(4, min_periods=3).apply(
                    lambda x: self._calculate_slope(x),
                    raw=False
                )

        return df

    def _add_composite_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加综合特征

        将多个指标组合成新的特征

        Args:
            df: 财务数据DataFrame

        Returns:
            添加综合特征后的DataFrame
        """
        # 综合成长性（收入、利润、资产增长的加权平均）
        growth_cols = ['or_yoy', 'netprofit_yoy', 'assets_yoy']
        if all(col in df.columns for col in growth_cols):
            df['composite_growth'] = (
                df['or_yoy'] * 0.4 +
                df['netprofit_yoy'] * 0.4 +
                df['assets_yoy'] * 0.2
            )

        # 综合盈利能力（ROE、ROA、净利率的加权平均）
        profitability_cols = ['roe', 'roa', 'netprofit_margin']
        if all(col in df.columns for col in profitability_cols):
            # 归一化后加权
            df['composite_profitability'] = (
                df['roe'] * 0.5 +
                df['roa'] * 0.2 +
                df['netprofit_margin'] * 0.3
            )

        # 财务健康度（流动比率、现金流、债务比的组合）
        if 'current_ratio' in df.columns:
            health_score = df['current_ratio'].copy()
            if 'ocf_to_debt' in df.columns:
                # 归一化OCF/债务
                ocf_normalized = (df['ocf_to_debt'] - df['ocf_to_debt'].min()) / (df['ocf_to_debt'].max() - df['ocf_to_debt'].min() + 1e-6)
                health_score = health_score + ocf_normalized
            if 'debt_to_assets' in df.columns:
                # 债务比率越低越好（反转）
                debt_score = 100 - df['debt_to_assets']
                health_score = health_score + debt_score / 100

            df['financial_health'] = health_score / 3  # 三项平均

        # 规模特征（对数市值）
        if 'total_mv' in df.columns:
            df['log_market_cap'] = np.log(df['total_mv'] + 1)

        return df

    def _calculate_slope(self, series: pd.Series) -> float:
        """
        计算序列的线性回归斜率

        Args:
            series: 数值序列

        Returns:
            斜率值
        """
        if len(series) < 2:
            return np.nan

        # 移除NaN值
        series = series.dropna()

        if len(series) < 2:
            return np.nan

        x = np.arange(len(series))
        y = series.values

        # 线性回归
        slope, _, _, _, _ = stats.linregress(x, y)

        return slope

    def add_lag_features(
        self,
        df: pd.DataFrame,
        lag_periods: List[int] = [1, 2, 3, 4],
        feature_cols: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        添加滞后特征

        Args:
            df: 财务数据DataFrame
            lag_periods: 滞后周期列表
            feature_cols: 要添加滞后的特征列

        Returns:
            添加滞后特征后的DataFrame
        """
        df = df.copy()

        if feature_cols is None:
            # 默认对关键指标添加滞后
            feature_cols = ['eps', 'roe', 'or_yoy', 'netprofit_yoy']

        for col in feature_cols:
            if col in df.columns:
                for lag in lag_periods:
                    df[f'{col}_lag_{lag}'] = df[col].shift(lag)

        return df

    def add_ratio_features(
        self,
        df: pd.DataFrame,
        numerator_cols: List[str],
        denominator_cols: List[str]
    ) -> pd.DataFrame:
        """
        添加比率特征（两列相除）

        Args:
            df: 财务数据DataFrame
            numerator_cols: 分子列列表
            denominator_cols: 分母列列表

        Returns:
            添加比率特征后的DataFrame
        """
        df = df.copy()

        for num_col in numerator_cols:
            for den_col in denominator_cols:
                if num_col in df.columns and den_col in df.columns:
                    ratio_name = f'{num_col}_to_{den_col}'
                    df[ratio_name] = df[num_col] / (df[den_col].abs() + 1e-6)
                    df[ratio_name] = df[ratio_name].replace([np.inf, -np.inf], np.nan)

        return df

    def get_feature_importance_groups(self) -> Dict[str, List[str]]:
        """
        获取特征分组（用于特征重要性分析）

        Returns:
            特征分组字典
        """
        return {
            'growth': [
                'or_yoy', 'netprofit_yoy', 'assets_yoy', 'ocf_yoy',
                'eps_yoy_calc', 'roe_yoy_calc'
            ],
            'profitability': [
                'roe', 'roa', 'netprofit_margin', 'grossprofit_margin',
                'operateprofit_margin'
            ],
            'valuation': [
                'pe', 'pb', 'ps', 'total_mv'
            ],
            'cashflow': [
                'ocfps', 'ocf_to_debt', 'free_cf'
            ],
            'trend': [
                'eps_ma_4q', 'roe_ma_4q', 'eps_trend_slope',
                'roe_trend_slope'
            ],
            'composite': [
                'composite_growth', 'composite_profitability',
                'financial_health', 'profit_quality'
            ]
        }

    def reduce_features(
        self,
        df: pd.DataFrame,
        importance_threshold: float = 0.01,
        feature_importance: Optional[Dict[str, float]] = None
    ) -> pd.DataFrame:
        """
        基于特征重要性减少特征数量

        Args:
            df: 数据DataFrame
            importance_threshold: 重要性阈值
            feature_importance: 特征重要性字典

        Returns:
            减少特征后的DataFrame
        """
        if feature_importance is None:
            logger.warning("No feature importance provided, skipping feature reduction")
            return df

        # 选择重要性高于阈值的特征
        important_features = [
            feat for feat, imp in feature_importance.items()
            if imp >= importance_threshold
        ]

        # 保留所有非特征列（目标变量、标识符等）
        non_feature_cols = [
            'symbol', 'ts_code', 'ann_date', 'end_date', 'report_type',
            'next_quarter_return', 'year', 'quarter',
            'is_q1', 'is_q2', 'is_q3', 'is_q4'
        ]

        keep_cols = non_feature_cols + important_features

        # 只保留存在的列
        keep_cols = [c for c in keep_cols if c in df.columns]

        result_df = df[keep_cols].copy()

        logger.info(f"Reduced features from {len(df.columns)} to {len(result_df.columns)}")

        return result_df
