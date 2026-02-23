"""
ML Prediction Visualization Module

Provides tools to visualize prediction vs actual comparison,
feature importance, and evaluation metrics.
"""
import numpy as np
import pandas as pd
from typing import Optional, List, Dict, Tuple
import logging

logger = logging.getLogger(__name__)


def plot_prediction_vs_actual(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    dates: Optional[pd.DatetimeIndex] = None,
    symbol: str = "未知",
    target_type: str = "return_1d",
    save_path: Optional[str] = None
):
    """
    生成预测 vs 实际收益率对比图（3子图）

    Args:
        y_true: 真实收益率
        y_pred: 预测收益率
        dates: 对应日期索引
        symbol: 股票代码
        target_type: 预测目标类型
        save_path: 图表保存路径（None则不保存）

    Returns:
        matplotlib Figure 对象，或 None（若matplotlib不可用）
    """
    try:
        import matplotlib
        matplotlib.use('Agg')  # 无GUI模式
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        logger.warning("matplotlib not installed, skipping visualization")
        return None

    if dates is None:
        x_axis = np.arange(len(y_true))
        x_label = "样本索引"
        use_dates = False
    else:
        x_axis = dates
        x_label = "日期"
        use_dates = True

    fig, axes = plt.subplots(3, 1, figsize=(14, 11))
    fig.suptitle(f'{symbol} 机器学习预测对比报告（{target_type}）', fontsize=14)

    # ---------- 子图1：时间序列对比 ----------
    ax1 = axes[0]
    ax1.plot(x_axis, y_true, 'o-', label='实际收益率', alpha=0.8, linewidth=1.5,
             markersize=4, color='#2196F3')
    ax1.plot(x_axis, y_pred, 'x-', label='预测收益率', alpha=0.8, linewidth=1.5,
             markersize=4, color='#FF5722')
    ax1.axhline(y=0, color='gray', linestyle='--', alpha=0.5, linewidth=1)
    ax1.fill_between(x_axis, y_true, y_pred, alpha=0.15, color='orange', label='预测误差')

    # 标注方向正确的点
    correct_direction = (np.sign(y_true) == np.sign(y_pred))
    ax1.scatter(
        np.array(x_axis)[correct_direction],
        y_true[correct_direction],
        marker='^', color='green', s=30, alpha=0.6, zorder=5, label='方向正确'
    )

    ax1.set_title('预测 vs 实际收益率时间序列')
    ax1.set_ylabel('收益率')
    ax1.legend(loc='upper left', fontsize=8)
    ax1.grid(True, alpha=0.3)

    if use_dates:
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

    # ---------- 子图2：散点图（预测相关性）----------
    ax2 = axes[1]
    direction_accuracy = (np.sign(y_true) == np.sign(y_pred)).mean()

    # 按实际涨跌着色
    up_mask = y_true > 0
    down_mask = y_true <= 0
    ax2.scatter(y_pred[up_mask], y_true[up_mask], alpha=0.6, s=40,
                color='#F44336', label=f'实际上涨 ({up_mask.sum()})')
    ax2.scatter(y_pred[down_mask], y_true[down_mask], alpha=0.6, s=40,
                color='#4CAF50', label=f'实际下跌 ({down_mask.sum()})')

    # 完美预测线
    min_val = min(y_true.min(), y_pred.min())
    max_val = max(y_true.max(), y_pred.max())
    ax2.plot([min_val, max_val], [min_val, max_val], 'k--', alpha=0.4, label='完美预测')

    # 零线（四象限分割）
    ax2.axhline(y=0, color='gray', linestyle='-', alpha=0.3)
    ax2.axvline(x=0, color='gray', linestyle='-', alpha=0.3)

    # 计算IC
    try:
        from scipy.stats import pearsonr
        ic, _ = pearsonr(y_pred, y_true)
        ic_str = f'IC={ic:.4f}'
    except Exception:
        ic_str = ''

    ax2.set_title(f'预测散点图（方向准确率={direction_accuracy:.1%}, {ic_str}）')
    ax2.set_xlabel('预测收益率')
    ax2.set_ylabel('实际收益率')
    ax2.legend(fontsize=8)
    ax2.grid(True, alpha=0.3)

    # ---------- 子图3：残差和累计方向正确率 ----------
    ax3 = axes[2]
    residuals = y_true - y_pred

    # 柱状图：残差
    colors_bar = ['#F44336' if r < 0 else '#4CAF50' for r in residuals]
    ax3.bar(x_axis, residuals, color=colors_bar, alpha=0.6, width=0.8)
    ax3.axhline(y=0, color='black', linestyle='-', linewidth=1)

    # 右轴：累计方向准确率
    ax3_twin = ax3.twinx()
    cumulative_dir_acc = np.cumsum(np.sign(y_true) == np.sign(y_pred)) / np.arange(1, len(y_true) + 1)
    ax3_twin.plot(x_axis, cumulative_dir_acc, 'b-', alpha=0.8, linewidth=2, label='累计方向准确率')
    ax3_twin.axhline(y=0.5, color='blue', linestyle='--', alpha=0.4)
    ax3_twin.set_ylim(0, 1)
    ax3_twin.set_ylabel('累计方向准确率', color='blue')
    ax3_twin.tick_params(axis='y', labelcolor='blue')

    mae = np.abs(residuals).mean()
    ax3.set_title(f'预测残差（MAE={mae:.4f}）')
    ax3.set_ylabel('残差')
    if use_dates:
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        ax3.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
        plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
    ax3.grid(True, alpha=0.3)

    plt.tight_layout()

    if save_path:
        try:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            logger.info(f"Chart saved to {save_path}")
        except Exception as e:
            logger.warning(f"Failed to save chart: {e}")

    return fig


def plot_feature_importance(
    feature_importance: Dict[str, float],
    top_k: int = 20,
    title: str = "特征重要性",
    save_path: Optional[str] = None
):
    """
    绘制特征重要性条形图

    Args:
        feature_importance: 特征重要性字典 {feature: importance}
        top_k: 显示前K个特征
        title: 图表标题
        save_path: 保存路径

    Returns:
        matplotlib Figure 对象
    """
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        logger.warning("matplotlib not installed")
        return None

    # 排序并取top_k
    sorted_items = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
    sorted_items = sorted_items[:top_k]

    features = [item[0] for item in sorted_items]
    importance = [item[1] for item in sorted_items]

    fig, ax = plt.subplots(figsize=(10, max(6, len(features) * 0.4)))

    colors = ['#FF6B6B' if i == 0 else '#4ECDC4' if i < 5 else '#45B7D1'
              for i in range(len(features))]

    bars = ax.barh(range(len(features)), importance, color=colors, alpha=0.8)
    ax.set_yticks(range(len(features)))
    ax.set_yticklabels(features)
    ax.invert_yaxis()

    # 在柱子上添加数值
    for bar, val in zip(bars, importance):
        ax.text(bar.get_width() + max(importance) * 0.01, bar.get_y() + bar.get_height() / 2,
                f'{val:.1f}', va='center', ha='left', fontsize=9)

    ax.set_title(title)
    ax.set_xlabel('重要性（Gain）')
    ax.grid(True, axis='x', alpha=0.3)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')

    return fig


def generate_prediction_report(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    dates: Optional[pd.DatetimeIndex] = None,
    symbol: str = "未知",
    target_type: str = "return_1d",
    feature_importance: Optional[Dict[str, float]] = None,
    save_dir: str = "data/ml_reports"
) -> Dict[str, str]:
    """
    生成完整预测报告（图表 + 文字）

    Args:
        y_true: 真实值
        y_pred: 预测值
        dates: 日期
        symbol: 股票代码
        target_type: 预测类型
        feature_importance: 特征重要性
        save_dir: 报告保存目录

    Returns:
        {'comparison_chart': 路径, 'importance_chart': 路径}
    """
    import os
    os.makedirs(save_dir, exist_ok=True)

    report_paths = {}

    # 1. 预测对比图
    comparison_path = os.path.join(
        save_dir, f"{symbol}_{target_type}_comparison.png"
    )
    fig1 = plot_prediction_vs_actual(
        y_true, y_pred, dates, symbol, target_type, save_path=comparison_path
    )
    if fig1 is not None:
        report_paths['comparison_chart'] = comparison_path
        try:
            import matplotlib.pyplot as plt
            plt.close(fig1)
        except Exception:
            pass

    # 2. 特征重要性图
    if feature_importance:
        importance_path = os.path.join(
            save_dir, f"{symbol}_{target_type}_importance.png"
        )
        fig2 = plot_feature_importance(
            feature_importance,
            title=f"{symbol} 特征重要性（{target_type}）",
            save_path=importance_path
        )
        if fig2 is not None:
            report_paths['importance_chart'] = importance_path
            try:
                import matplotlib.pyplot as plt
                plt.close(fig2)
            except Exception:
                pass

    return report_paths
