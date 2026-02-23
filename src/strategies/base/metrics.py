"""
Strategy Health Metrics Module

Provides comprehensive health metrics for quantitative trading strategies.
"""
import numpy as np
import pandas as pd
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


def strategy_health_check(
    returns: pd.Series,
    risk_free_rate: float = 0.02,
    trading_days_per_year: int = 252
) -> Dict[str, float]:
    """
    计算量化策略的健康指标

    Args:
        returns: 收益率序列 (pd.Series)
        risk_free_rate: 无风险利率 (默认 2%)
        trading_days_per_year: 每年交易日数量 (默认 252)

    Returns:
        包含所有健康指标的字典

    Metrics:
        - annual_return: 年化收益率
        - sharpe_ratio: 夏普比率 (风险调整后收益)
        - sortino_ratio: 索提诺比率 (下行风险调整后收益)
        - calmar_ratio: 卡玛比率 (回撤调整后收益)
        - max_drawdown: 最大回撤
        - profit_factor: 盈亏比 (总盈利/总亏损)
        - monthly_win_rate: 月度胜率
        - volatility: 年化波动率
        - total_return: 总收益率
        - avg_monthly_return: 平均月度收益率
    """
    if len(returns) == 0:
        logger.warning("Empty returns series provided")
        return {
            'annual_return': 0.0,
            'sharpe_ratio': 0.0,
            'sortino_ratio': 0.0,
            'calmar_ratio': 0.0,
            'max_drawdown': 0.0,
            'profit_factor': 0.0,
            'monthly_win_rate': 0.0,
            'volatility': 0.0,
            'total_return': 0.0,
            'avg_monthly_return': 0.0
        }

    # 确保是 pd.Series 且有 datetime index
    if not isinstance(returns, pd.Series):
        returns = pd.Series(returns)

    # 如果没有 index，创建默认的日期 index
    if returns.index is None or not isinstance(returns.index, pd.DatetimeIndex):
        returns.index = pd.date_range(start='2020-01-01', periods=len(returns), freq='D')

    # 1. 计算累计收益率
    cum_returns = (1 + returns).cumprod()

    # 2. 计算回撤
    drawdown = 1 - cum_returns / cum_returns.cummax()
    max_drawdown = drawdown.max()

    # 3. 计算年化收益率
    total_return = cum_returns.iloc[-1] - 1

    # 优先使用保存的实际天数，否则使用收益率序列长度
    if hasattr(returns, 'attrs') and 'n_days' in returns.attrs:
        n_days = returns.attrs['n_days']
    else:
        n_days = len(returns)

    annual_return = (1 + total_return) ** (trading_days_per_year / n_days) - 1

    # 4. 计算年化波动率
    volatility = returns.std() * np.sqrt(trading_days_per_year)

    # 5. 夏普比率 (年化)
    excess_return = annual_return - risk_free_rate
    sharpe_ratio = excess_return / volatility if volatility > 0 else 0.0

    # 6. 索提诺比率 (只考虑下行波动率)
    downside_returns = returns[returns < 0]
    downside_volatility = downside_returns.std() * np.sqrt(trading_days_per_year)
    sortino_ratio = excess_return / downside_volatility if downside_volatility > 0 else 0.0

    # 7. 卡玛比率 (年化收益 / 最大回撤)
    calmar_ratio = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0.0

    # 8. 盈亏比 (总盈利 / 总亏损)
    positive_returns = returns[returns > 0].sum()
    negative_returns = abs(returns[returns < 0].sum())
    profit_factor = positive_returns / negative_returns if negative_returns > 0 else (999999.99 if positive_returns > 0 else 0.0)

    # 9. 月度胜率
    monthly_returns = returns.resample('ME').sum()
    monthly_win_rate = (monthly_returns > 0).mean()

    # 10. 平均月度收益率
    avg_monthly_return = monthly_returns.mean()

    return {
        'annual_return': float(annual_return),
        'sharpe_ratio': float(sharpe_ratio),
        'sortino_ratio': float(sortino_ratio),
        'calmar_ratio': float(calmar_ratio),
        'max_drawdown': float(max_drawdown),
        'profit_factor': float(profit_factor),
        'monthly_win_rate': float(monthly_win_rate),
        'volatility': float(volatility),
        'total_return': float(total_return),
        'avg_monthly_return': float(avg_monthly_return)
    }


def log_strategy_health_report(
    returns: pd.Series,
    risk_free_rate: float = 0.02,
    trading_days_per_year: int = 252
) -> None:
    """
    记录策略健康指标报告到日志

    Args:
        returns: 收益率序列
        risk_free_rate: 无风险利率
        trading_days_per_year: 每年交易日数量
    """
    metrics = strategy_health_check(returns, risk_free_rate, trading_days_per_year)

    logger.info("=" * 70)
    logger.info("策略健康指标报告")
    logger.info("=" * 70)
    logger.info(f"年化收益率:  {metrics['annual_return']:.2%}")
    logger.info(f"夏普比率:    {metrics['sharpe_ratio']:.2f}")
    logger.info(f"索提诺比率:  {metrics['sortino_ratio']:.2f}")
    logger.info(f"卡玛比率:    {metrics['calmar_ratio']:.2f}")
    logger.info(f"最大回撤:    {metrics['max_drawdown']:.2%}")
    logger.info(f"盈亏比:      {metrics['profit_factor']:.2f}")
    logger.info(f"月度胜率:    {metrics['monthly_win_rate']:.2%}")
    logger.info(f"年化波动率:  {metrics['volatility']:.2%}")
    logger.info(f"总收益率:    {metrics['total_return']:.2%}")
    logger.info(f"平均月收益:  {metrics['avg_monthly_return']:.2%}")
    logger.info("=" * 70)


def calculate_backtest_returns(
    initial_cash: float,
    final_cash: float,
    portfolio_values: list,
    start_date: str = None,
    end_date: str = None
) -> pd.Series:
    """
    从回测的组合价值计算收益率序列

    Args:
        initial_cash: 初始资金
        final_cash: 最终资金
        portfolio_values: 组合价值序列
        start_date: 回测开始日期 (YYYY-MM-DD)
        end_date: 回测结束日期 (YYYY-MM-DD)

    Returns:
        收益率序列，带有日期索引
    """
    portfolio_values = np.array(portfolio_values)

    # 计算日收益率
    returns = np.diff(portfolio_values) / portfolio_values[:-1]

    # 如果提供了日期，创建日期索引
    if start_date and end_date:
        from datetime import datetime
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        n_days = (end - start).days + 1  # 实际回测天数

        # 创建日期索引（跳过第一天，因为diff会减少一个值）
        dates = pd.date_range(start=start, periods=len(returns), freq='D')
        returns_series = pd.Series(returns, index=dates)
        returns_series.attrs['n_days'] = n_days  # 保存实际天数
        return returns_series

    return pd.Series(returns)


class StrategyHealthAnalyzer:
    """
    策略健康分析器

    提供策略健康指标的深度分析和可视化
    """

    def __init__(self, risk_free_rate: float = 0.02):
        """
        初始化分析器

        Args:
            risk_free_rate: 无风险利率
        """
        self.risk_free_rate = risk_free_rate

    def analyze(
        self,
        returns: pd.Series,
        benchmark_returns: Optional[pd.Series] = None
    ) -> Dict[str, Any]:
        """
        完整分析策略健康状况

        Args:
            returns: 策略收益率序列
            benchmark_returns: 基准收益率序列（可选）

        Returns:
            完整的分析结果
        """
        basic_metrics = strategy_health_check(returns, self.risk_free_rate)

        result = {
            'basic_metrics': basic_metrics,
            'performance_rating': self._calculate_performance_rating(basic_metrics)
        }

        # 如果提供了基准，计算相对指标
        if benchmark_returns is not None:
            benchmark_metrics = strategy_health_check(benchmark_returns, self.risk_free_rate)
            result['relative_metrics'] = self._calculate_relative_metrics(
                returns, benchmark_returns
            )
            result['benchmark_comparison'] = self._compare_with_benchmark(
                basic_metrics, benchmark_metrics
            )

        return result

    def _calculate_performance_rating(
        self,
        metrics: Dict[str, float]
    ) -> str:
        """
        根据指标计算策略评级

        Args:
            metrics: 健康指标字典

        Returns:
            评级 (A/B/C/D/F)
        """
        sharpe = metrics['sharpe_ratio']
        max_dd = metrics['max_drawdown']
        annual_ret = metrics['annual_return']

        # 评级标准
        if sharpe > 2 and max_dd < -0.1 and annual_ret > 0.1:
            return "A"
        elif sharpe > 1.5 and max_dd < -0.15 and annual_ret > 0.05:
            return "B"
        elif sharpe > 1 and max_dd < -0.25 and annual_ret > 0:
            return "C"
        elif sharpe > 0.5:
            return "D"
        else:
            return "F"

    def _calculate_relative_metrics(
        self,
        returns: pd.Series,
        benchmark_returns: pd.Series
    ) -> Dict[str, float]:
        """
        计算相对基准的指标

        Args:
            returns: 策略收益率
            benchmark_returns: 基准收益率

        Returns:
            相对指标字典
        """
        # 对齐时间序列
        aligned_returns, aligned_benchmark = returns.align(benchmark_returns, join='inner')

        # 超额收益
        excess_returns = aligned_returns - aligned_benchmark

        # 信息比率 (Information Ratio)
        tracking_error = excess_returns.std() * np.sqrt(252)
        information_ratio = excess_returns.mean() * 252 / tracking_error if tracking_error > 0 else 0

        # 超额收益相关统计
        avg_excess_return = excess_returns.mean() * 252

        return {
            'information_ratio': float(information_ratio),
            'tracking_error': float(tracking_error),
            'avg_annual_excess_return': float(avg_excess_return)
        }

    def _compare_with_benchmark(
        self,
        strategy_metrics: Dict[str, float],
        benchmark_metrics: Dict[str, float]
    ) -> Dict[str, float]:
        """
        与基准进行比较

        Args:
            strategy_metrics: 策略指标
            benchmark_metrics: 基准指标

        Returns:
            比较结果
        """
        return {
            'excess_annual_return': strategy_metrics['annual_return'] - benchmark_metrics['annual_return'],
            'excess_sharpe': strategy_metrics['sharpe_ratio'] - benchmark_metrics['sharpe_ratio'],
            'excess_sortino': strategy_metrics['sortino_ratio'] - benchmark_metrics['sortino_ratio'],
            'drawdown_improvement': benchmark_metrics['max_drawdown'] - strategy_metrics['max_drawdown']
        }

    def generate_report(
        self,
        analysis_result: Dict[str, Any],
        strategy_name: str = "策略"
    ) -> str:
        """
        生成格式化的分析报告

        Args:
            analysis_result: 分析结果
            strategy_name: 策略名称

        Returns:
            格式化的报告字符串
        """
        metrics = analysis_result['basic_metrics']
        rating = analysis_result['performance_rating']

        report = f"""
{'='*70}
{strategy_name} 健康分析报告
{'='*70}

【评级】: {rating}

核心指标:
--------
年化收益率:    {metrics['annual_return']:>10.2%}
夏普比率:      {metrics['sharpe_ratio']:>10.2f}
索提诺比率:    {metrics['sortino_ratio']:>10.2f}
卡玛比率:      {metrics['calmar_ratio']:>10.2f}

风险指标:
--------
最大回撤:      {metrics['max_drawdown']:>10.2%}
年化波动率:    {metrics['volatility']:>10.2%}

交易指标:
--------
盈亏比:        {metrics['profit_factor']:>10.2f}
月度胜率:      {metrics['monthly_win_rate']:>10.2%}
总收益率:      {metrics['total_return']:>10.2%}
平均月收益:    {metrics['avg_monthly_return']:>10.2%}
"""

        # 如果有相对指标，添加基准比较部分
        if 'relative_metrics' in analysis_result:
            rel = analysis_result['relative_metrics']
            comp = analysis_result['benchmark_comparison']

            report += f"""

基准对比:
--------
信息比率:      {rel['information_ratio']:>10.2f}
跟踪误差:      {rel['tracking_error']:>10.2%}
年化超额收益:  {rel['avg_annual_excess_return']:>10.2%}

超额表现:
--------
超额年化收益:  {comp['excess_annual_return']:>10.2%}
超额夏普:      {comp['excess_sharpe']:>10.2f}
超额索提诺:    {comp['excess_sortino']:>10.2f}
回撤改善:      {comp['drawdown_improvement']:>10.2%}
"""

        report += f"""
{'='*70}
"""

        return report
