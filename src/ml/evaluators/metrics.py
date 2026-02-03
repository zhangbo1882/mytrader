"""
Model Evaluation Metrics Module

Provides comprehensive evaluation metrics for stock price prediction models.
"""
import numpy as np
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple
import logging

logger = logging.getLogger(__name__)


class ModelEvaluator:
    """
    模型评估器

    提供多种评估指标和可视化功能
    """

    def __init__(self):
        """初始化评估器"""
        pass

    def evaluate_regression(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray
    ) -> Dict[str, float]:
        """
        评估回归模型性能

        Args:
            y_true: 真实值
            y_pred: 预测值

        Returns:
            评估指标字典
        """
        from sklearn.metrics import (
            mean_absolute_error,
            mean_squared_error,
            r2_score,
            mean_absolute_percentage_error
        )

        # 基本指标
        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        r2 = r2_score(y_true, y_pred)

        # MAPE（避免除零）
        mask = np.abs(y_true) > 1e-6
        if mask.sum() > 0:
            mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
        else:
            mape = float('inf')

        # 方向准确率
        direction_acc = self._direction_accuracy(y_true, y_pred)

        # 平均绝对误差（百分比）
        mae_pct = np.mean(np.abs(y_true - y_pred) / (np.abs(y_true) + 1e-6)) * 100

        return {
            'mae': float(mae),
            'rmse': float(rmse),
            'mape': float(mape),
            'r2': float(r2),
            'direction_accuracy': float(direction_acc),
            'mae_pct': float(mae_pct)
        }

    def _direction_accuracy(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray
    ) -> float:
        """
        计算方向预测准确率

        Args:
            y_true: 真实值序列
            y_pred: 预测值序列

        Returns:
            方向准确率 (0-1)
        """
        if len(y_true) < 2:
            return 0.0

        # 计算方向
        true_direction = np.sign(y_true[1:] - y_true[:-1])
        pred_direction = np.sign(y_pred[1:] - y_pred[:-1])

        # 准确率
        accuracy = np.mean(true_direction == pred_direction)

        return float(accuracy)

    def evaluate_trading_performance(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        initial_capital: float = 100000,
        threshold: float = 0.01,
        transaction_cost: float = 0.002
    ) -> Dict[str, float]:
        """
        评估基于预测的交易策略性能

        Args:
            y_true: 真实收益率序列
            y_pred: 预测收益率序列
            initial_capital: 初始资金
            threshold: 交易阈值
            transaction_cost: 交易成本

        Returns:
            交易性能指标
        """
        capital = initial_capital
        position = 0  # 0=空仓, 1=满仓
        holdings = 0

        trades = []
        portfolio_values = []

        for i in range(len(y_pred) - 1):
            # 当前价格（假设y_true是价格变化）
            current_price = 1 + y_true[i]

            # 交易信号：预测上涨则买入
            if y_pred[i] > threshold and position == 0:
                # 买入
                holdings = capital / current_price
                capital = 0
                position = 1
                trades.append(('buy', i, current_price))

            elif y_pred[i] < -threshold and position == 1:
                # 卖出
                capital = holdings * current_price * (1 - transaction_cost)
                holdings = 0
                position = 0
                trades.append(('sell', i, current_price))

            # 计算当前组合价值
            if position == 1:
                portfolio_value = holdings * current_price
            else:
                portfolio_value = capital

            portfolio_values.append(portfolio_value)

        # 计算指标
        portfolio_values = np.array(portfolio_values)
        returns = np.diff(portfolio_values) / portfolio_values[:-1]

        # 总收益率
        total_return = (portfolio_values[-1] - initial_capital) / initial_capital

        # 年化收益率（假设252个交易日）
        n_days = len(returns)
        if n_days > 0:
            annual_return = (1 + total_return) ** (252 / n_days) - 1
        else:
            annual_return = 0

        # 夏普比率
        if returns.std() > 0:
            sharpe_ratio = returns.mean() / returns.std() * np.sqrt(252)
        else:
            sharpe_ratio = 0

        # 最大回撤
        cummax = np.maximum.accumulate(portfolio_values)
        drawdowns = (portfolio_values - cummax) / cummax
        max_drawdown = drawdowns.min()

        # 胜率
        winning_trades = sum(1 for i in range(1, len(trades))
                            if trades[i][0] == 'sell' and trades[i][2] > trades[i-1][2])
        total_trades = len([t for t in trades if t[0] == 'sell'])
        win_rate = winning_trades / total_trades if total_trades > 0 else 0

        # 买入持有基准
        buy_hold_return = (y_true.sum() + 1) - 1

        return {
            'total_return': float(total_return),
            'annual_return': float(annual_return),
            'sharpe_ratio': float(sharpe_ratio),
            'max_drawdown': float(max_drawdown),
            'win_rate': float(win_rate),
            'total_trades': total_trades,
            'buy_hold_return': float(buy_hold_return),
            'final_capital': float(portfolio_values[-1])
        }

    def calculate_information_coefficient(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray
    ) -> Dict[str, float]:
        """
        计算信息系数（IC）和相关指标

        Args:
            y_true: 真实值
            y_pred: 预测值

        Returns:
            IC相关指标
        """
        from scipy.stats import spearmanr, pearsonr

        # Pearson IC
        pearson_ic, pearson_p = pearsonr(y_pred, y_true)

        # Spearman IC (秩相关)
        spearman_ic, spearman_p = spearmanr(y_pred, y_true)

        return {
            'pearson_ic': float(pearson_ic),
            'pearson_p_value': float(pearson_p),
            'spearman_ic': float(spearman_ic),
            'spearman_p_value': float(spearman_p)
        }

    def calculate_feature_importance_metrics(
        self,
        importance: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        分析特征重要性

        Args:
            importance: 特征重要性字典

        Returns:
            特征重要性分析结果
        """
        sorted_features = sorted(importance.items(), key=lambda x: x[1], reverse=True)

        # Top 10特征
        top_10 = sorted_features[:10]

        # 特征重要性分布
        values = [v for _, v in sorted_features]
        total = sum(values)

        # 累积重要性
        cumulative = []
        cumsum = 0
        for _, v in sorted_features:
            cumsum += v
            cumulative.append(cumsum / total)

        # 找出达到80%累积重要性的特征数
        n_features_80 = next((i + 1 for i, c in enumerate(cumulative) if c >= 0.8), len(cumulative))

        return {
            'top_features': top_10,
            'n_features': len(importance),
            'n_features_80pct': n_features_80,
            'total_importance': float(total),
            'mean_importance': float(np.mean(values)),
            'std_importance': float(np.std(values))
        }

    def generate_evaluation_report(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        model_id: str = "unknown"
    ) -> str:
        """
        生成评估报告

        Args:
            y_true: 真实值
            y_pred: 预测值
            model_id: 模型ID

        Returns:
            格式化的评估报告
        """
        regression_metrics = self.evaluate_regression(y_true, y_pred)
        ic_metrics = self.calculate_information_coefficient(y_true, y_pred)

        report = f"""
{'='*60}
Model Evaluation Report
{'='*60}
Model ID: {model_id}
{'='*60}

Regression Metrics:
-------------------
MAE:      {regression_metrics['mae']:.6f}
RMSE:     {regression_metrics['rmse']:.6f}
MAPE:     {regression_metrics['mape']:.2f}%
R²:       {regression_metrics['r2']:.4f}
MAE %:    {regression_metrics['mae_pct']:.2f}%

Direction Analysis:
-------------------
Direction Accuracy: {regression_metrics['direction_accuracy']:.2%}

Information Coefficient:
------------------------
Pearson IC:  {ic_metrics['pearson_ic']:.4f} (p={ic_metrics['pearson_p_value']:.4f})
Spearman IC: {ic_metrics['spearman_ic']:.4f} (p={ic_metrics['spearman_p_value']:.4f})

{'='*60}
"""
        return report

    def compare_models(
        self,
        results: Dict[str, Dict[str, float]]
    ) -> pd.DataFrame:
        """
        比较多个模型的性能

        Args:
            results: {model_name: metrics} 字典

        Returns:
            比较结果DataFrame
        """
        df = pd.DataFrame.from_dict(results, orient='index')

        # 按MAE排序
        if 'mae' in df.columns:
            df = df.sort_values('mae')

        return df


def calculate_all_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    return_metrics: Optional[List[str]] = None
) -> Dict[str, float]:
    """
    便捷函数：计算所有指标

    Args:
        y_true: 真实值
        y_pred: 预测值
        return_metrics: 返回的指标列表（None则返回全部）

    Returns:
        指标字典
    """
    evaluator = ModelEvaluator()

    all_metrics = {
        **evaluator.evaluate_regression(y_true, y_pred),
        **evaluator.calculate_information_coefficient(y_true, y_pred)
    }

    if return_metrics is not None:
        return {k: v for k, v in all_metrics.items() if k in return_metrics}

    return all_metrics
