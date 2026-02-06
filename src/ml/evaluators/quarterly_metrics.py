"""
Quarterly Model Evaluator Module

Provides evaluation metrics for quarterly financial prediction models.
Extends the base ModelEvaluator with quarterly-specific metrics.
"""
import numpy as np
import pandas as pd
from typing import Dict, Any, Optional, List
from scipy import stats
import logging

logger = logging.getLogger(__name__)


class QuarterlyEvaluator:
    """
    季度模型评估器

    专门评估季度财务数据预测模型的性能

    主要功能：
    1. 回归指标评估（MAE、RMSE、R²等）
    2. 方向预测准确率
    3. 信息系数（IC）
    4. 分组性能评估（按年份、季度、股票）
    5. 回测性能分析
    """

    def __init__(self):
        """初始化评估器"""
        pass

    def evaluate(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        X: Optional[np.ndarray] = None,
        feature_names: Optional[List[str]] = None,
        feature_importance: Optional[Dict[str, float]] = None
    ) -> Dict[str, Any]:
        """
        完整评估季度模型

        Args:
            y_true: 真实的下一季度收益率
            y_pred: 预测的下一季度收益率
            X: 特征矩阵（可选，用于计算IC等）
            feature_names: 特征名称列表
            feature_importance: 特征重要性字典

        Returns:
            评估指标字典
        """
        from sklearn.metrics import (
            mean_absolute_error,
            mean_squared_error,
            r2_score,
            mean_absolute_percentage_error
        )

        # 1. 基础回归指标
        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        r2 = r2_score(y_true, y_pred)

        # MAPE（避免除零）
        mask = np.abs(y_true) > 1e-6
        if mask.sum() > 0:
            mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
        else:
            mape = float('inf')

        # 2. 方向预测准确率
        direction_acc = self._calculate_direction_accuracy(y_true, y_pred)

        # 3. 信息系数（IC）
        ic_metrics = self._calculate_information_coefficient(y_true, y_pred)

        # 4. 预测准确度分级
        accuracy_grades = self._calculate_accuracy_grades(y_true, y_pred)

        # 5. 置信区间统计
        confidence_stats = self._calculate_confidence_statistics(y_true, y_pred)

        metrics = {
            'mae': float(mae),
            'rmse': float(rmse),
            'r2': float(r2),
            'mape': float(mape),
            'direction_accuracy': float(direction_acc),
            'pearson_ic': float(ic_metrics['pearson_ic']),
            'spearman_ic': float(ic_metrics['spearman_ic']),
            'ic_p_value': float(ic_metrics['p_value']),
            **accuracy_grades,
            **confidence_stats
        }

        # 6. 特征重要性分析（如果提供）
        if feature_importance:
            metrics['feature_importance'] = self._analyze_feature_importance(
                feature_importance, feature_names
            )

        return metrics

    def _calculate_direction_accuracy(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray
    ) -> float:
        """
        计算方向预测准确率

        预测下一季度收益率的方向（涨/跌）与实际方向一致的比例

        Args:
            y_true: 真实收益率
            y_pred: 预测收益率

        Returns:
            方向准确率（0-1）
        """
        # 计算方向（正为涨，负为跌）
        true_direction = np.sign(y_true)
        pred_direction = np.sign(y_pred)

        # 计算准确率
        accuracy = np.mean(true_direction == pred_direction)

        return float(accuracy)

    def _calculate_information_coefficient(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray
    ) -> Dict[str, float]:
        """
        计算信息系数（IC）

        IC是预测值与真实值的Spearman相关系数，
        衡量模型的排序能力

        Args:
            y_true: 真实收益率
            y_pred: 预测收益率

        Returns:
            IC指标字典
        """
        # Spearman相关系数
        spearman_ic, spearman_p = stats.spearmanr(y_pred, y_true)

        # Pearson相关系数
        pearson_ic, pearson_p = stats.pearsonr(y_pred, y_true)

        return {
            'spearman_ic': float(spearman_ic if not np.isnan(spearman_ic) else 0),
            'p_value': float(spearman_p if not np.isnan(spearman_p) else 1),
            'pearson_ic': float(pearson_ic if not np.isnan(pearson_ic) else 0)
        }

    def _calculate_accuracy_grades(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        thresholds: List[float] = [0.05, 0.10, 0.15]
    ) -> Dict[str, float]:
        """
        计算不同误差阈值下的准确率

        Args:
            y_true: 真实收益率
            y_pred: 预测收益率
            thresholds: 误差阈值列表

        Returns:
            各阈值下的准确率
        """
        absolute_errors = np.abs(y_true - y_pred)

        grades = {}
        for threshold in thresholds:
            accuracy = np.mean(absolute_errors <= threshold)
            grades[f'accuracy_within_{int(threshold*100)}pct'] = float(accuracy)

        return grades

    def _calculate_confidence_statistics(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray
    ) -> Dict[str, float]:
        """
        计算预测置信度统计

        Args:
            y_true: 真实收益率
            y_pred: 预测收益率

        Returns:
            置信度统计
        """
        errors = y_pred - y_true

        return {
            'mean_error': float(np.mean(errors)),
            'std_error': float(np.std(errors)),
            'median_error': float(np.median(errors)),
            'mean_abs_error': float(np.mean(np.abs(errors)))
        }

    def _analyze_feature_importance(
        self,
        importance: Dict[str, float],
        feature_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        分析特征重要性

        Args:
            importance: 特征重要性字典
            feature_names: 特征名称列表

        Returns:
            特征重要性分析结果
        """
        sorted_features = sorted(importance.items(), key=lambda x: x[1], reverse=True)

        # Top 10特征
        top_10 = dict(sorted_features[:10])

        # 特征重要性分布
        values = list(importance.values())
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
            'top_10': top_10,
            'n_features': len(importance),
            'n_features_80pct': n_features_80,
            'total_importance': float(total),
            'mean_importance': float(np.mean(values)),
            'std_importance': float(np.std(values))
        }

    def evaluate_by_group(
        self,
        df: pd.DataFrame,
        group_col: str,
        y_true_col: str = 'next_quarter_return',
        y_pred_col: str = 'prediction'
    ) -> pd.DataFrame:
        """
        按分组评估模型性能

        Args:
            df: 包含真实值、预测值和分组列的DataFrame
            group_col: 分组列名（如'year', 'quarter', 'symbol'）
            y_true_col: 真实值列名
            y_pred_col: 预测值列名

        Returns:
            分组评估结果DataFrame
        """
        results = []

        for group_value, group_df in df.groupby(group_col):
            y_true = group_df[y_true_col].values
            y_pred = group_df[y_pred_col].values

            # 跳过样本数太少的组
            if len(y_true) < 3:
                continue

            metrics = self.evaluate(y_true, y_pred)

            result = {
                group_col: group_value,
                'n_samples': len(y_true),
                **metrics
            }

            # 移除嵌套的feature_importance
            if 'feature_importance' in result:
                del result['feature_importance']

            results.append(result)

        return pd.DataFrame(results)

    def backtest(
        self,
        df: pd.DataFrame,
        prediction_col: str = 'prediction',
        target_col: str = 'next_quarter_return',
        initial_capital: float = 100000,
        top_n: int = 10,
        rebalance_freq: str = 'quarterly'
    ) -> Dict[str, Any]:
        """
        回测基于预测的交易策略

        策略：每个季度选择预测收益率最高的top_n只股票

        Args:
            df: 包含预测和真实收益率的DataFrame
            prediction_col: 预测列名
            target_col: 目标列名
            initial_capital: 初始资金
            top_n: 每期选择的股票数量
            rebalance_freq: 再平衡频率

        Returns:
            回测结果
        """
        # 按年份和季度分组
        if 'year' in df.columns and 'quarter' in df.columns:
            df['period'] = df['year'].astype(str) + 'Q' + df['quarter'].astype(str)
            group_col = 'period'
        else:
            group_col = 'end_date'

        capital = initial_capital
        capital_history = []

        for period, period_df in df.groupby(group_col):
            # 选择预测收益率最高的top_n只股票
            top_stocks = period_df.nlargest(top_n, prediction_col)

            # 计算实际收益率
            actual_return = top_stocks[target_col].mean()

            # 更新资金
            capital = capital * (1 + actual_return)
            capital_history.append({
                'period': period,
                'capital': capital,
                'return': actual_return,
                'n_stocks': len(top_stocks)
            })

        capital_history_df = pd.DataFrame(capital_history)

        # 计算回测指标
        total_return = (capital - initial_capital) / initial_capital
        n_periods = len(capital_history_df)

        # 年化收益率（假设每期为一个季度）
        annual_return = (1 + total_return) ** (4 / n_periods) - 1 if n_periods > 0 else 0

        # 最大回撤
        capital_series = capital_history_df['capital'].values
        cummax = np.maximum.accumulate(capital_series)
        drawdowns = (capital_series - cummax) / cummax
        max_drawdown = drawdowns.min()

        # 胜率
        winning_periods = (capital_history_df['return'] > 0).sum()
        win_rate = winning_periods / n_periods if n_periods > 0 else 0

        # 买入持有基准（所有股票平均）
        buy_hold_return = df.groupby(group_col)[target_col].mean().mean()

        return {
            'total_return': float(total_return),
            'annual_return': float(annual_return),
            'max_drawdown': float(max_drawdown),
            'win_rate': float(win_rate),
            'final_capital': float(capital),
            'n_periods': int(n_periods),
            'buy_hold_return': float(buy_hold_return),
            'capital_history': capital_history_df.to_dict('records')
        }

    def generate_report(
        self,
        metrics: Dict[str, Any],
        model_id: str = "unknown",
        training_info: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        生成格式化的评估报告

        Args:
            metrics: 评估指标字典
            model_id: 模型ID
            training_info: 训练信息字典

        Returns:
            格式化的评估报告
        """
        report = f"""
{'='*70}
Quarterly Model Evaluation Report
{'='*70}
Model ID: {model_id}
{'='*70}

Performance Metrics:
--------------------
MAE (Mean Absolute Error):      {metrics.get('mae', 'N/A'):.4f}
RMSE (Root Mean Squared Error): {metrics.get('rmse', 'N/A'):.4f}
R² (R-squared):                 {metrics.get('r2', 'N/A'):.4f}
MAPE (Mean Absolute % Error):   {metrics.get('mape', 'N/A'):.2f}%

Direction Prediction:
--------------------
Direction Accuracy:             {metrics.get('direction_accuracy', 'N/A'):.2%}

Information Coefficient:
-----------------------
Spearman IC:                    {metrics.get('spearman_ic', 'N/A'):.4f}
IC P-Value:                     {metrics.get('ic_p_value', 'N/A'):.4f}

Accuracy Within Thresholds:
---------------------------
Within 5%:                      {metrics.get('accuracy_within_5pct', 'N/A'):.2%}
Within 10%:                     {metrics.get('accuracy_within_10pct', 'N/A'):.2%}
Within 15%:                     {metrics.get('accuracy_within_15pct', 'N/A'):.2%}

Error Statistics:
-----------------
Mean Error:                     {metrics.get('mean_error', 'N/A'):.4f}
Std Error:                      {metrics.get('std_error', 'N/A'):.4f}
Median Error:                   {metrics.get('median_error', 'N/A'):.4f}

"""

        # 添加特征重要性
        if 'feature_importance' in metrics:
            fi = metrics['feature_importance']
            report += f"""
Top 10 Features:
----------------
"""
            for i, (feat, imp) in enumerate(fi.get('top_10', {}).items(), 1):
                report += f"{i:2d}. {feat:30s}: {imp:.4f}\n"

            report += f"""
Feature Importance Summary:
---------------------------
Total Features:                {fi.get('n_features', 'N/A')}
Features for 80% Importance:   {fi.get('n_features_80pct', 'N/A')}
"""

        # 添加训练信息
        if training_info:
            report += f"""
{'='*70}
Training Information:
---------------------
Symbols:                        {training_info.get('symbols', 'N/A')}
Feature Mode:                   {training_info.get('feature_mode', 'N/A')}
Train Mode:                     {training_info.get('train_mode', 'N/A')}
Train Samples:                  {training_info.get('train_samples', 'N/A')}
Validation Samples:             {training_info.get('val_samples', 'N/A')}
Test Samples:                   {training_info.get('test_samples', 'N/A')}
"""
            if 'target_type' in training_info:
                report += f"Target Type:                    {training_info['target_mode']}\n"

        report += f"""
{'='*70}
"""

        return report

    def compare_models(
        self,
        results: Dict[str, Dict[str, float]]
    ) -> pd.DataFrame:
        """
        比较多个模型的性能

        Args:
            results: {model_id: metrics} 字典

        Returns:
            比较结果DataFrame
        """
        df = pd.DataFrame.from_dict(results, orient='index')

        # 按MAE排序
        if 'mae' in df.columns:
            df = df.sort_values('mae')

        return df
