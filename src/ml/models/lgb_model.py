"""
LightGBM Model Implementation

Fast, efficient gradient boosting model for stock price prediction.

=== 特征向量说明 ===

本模型实际使用的特征向量（共59个）分为以下几类：

1. 原始OHLCV数据 (6个)
   - open: 开盘价
   - high: 最高价
   - low: 最低价
   - close: 收盘价
   - volume: 成交量
   - returns_1d: 1日收益率

2. 移动平均线类 (4个)
   - sma_5: 5日简单移动平均线
   - sma_10: 10日简单移动平均线
   - sma_20: 20日简单移动平均线（短期趋势基准）
   - sma_60: 60日简单移动平均线（中期趋势基准）

3. 指数移动平均线类 (2个)
   - ema_12: 12日指数移动平均线
   - ema_26: 26日指数移动平均线

4. 相对强弱指标类 (2个)
   - rsi_14: 14日相对强弱指标（超买超卖信号）
   - rsi_6: 6日相对强弱指标（短期超买超卖）

5. MACD指标类 (3个)
   - macd: MACD快线（动量趋势）
   - macd_signal: MACD信号线（平滑后的MACD）
   - macd_hist: MACD柱状图（快线-信号线，反映趋势强度）

6. 布林带指标类 (5个)
   - bb_upper: 布林带上轨（压力位）
   - bb_middle: 布林带中轨（20日均线）
   - bb_lower: 布林带下轨（支撑位）
   - bb_width: 布林带宽度（上轨-下轨，波动率指标）
   - bb_pct: 价格在布林带中的位置百分比

7. 波动率指标类 (2个)
   - atr_14: 14日平均真实波幅（衡量价格波动幅度）
   - atr_ratio: ATR比率（atr_14/收盘价，标准化波动率）

8. 随机指标类 (2个)
   - stoch_k: 随机指标K值（快线，超买超卖信号）
   - stoch_d: 随机指标D值（慢线，K值的3日均线）

9. 威廉指标 (1个)
   - williams_r: 威廉指标（14日，超买超卖信号，范围-100~0）

10. 商品通道指标 (1个)
    - cci: 20日商品通道指标（趋势和超买超卖）

11. 动量指标类 (2个)
    - momentum_10: 10日动量（当前价-10日前收盘价）
    - roc_12: 12日变化率（收益率）

12. 成交量指标类 (3个)
    - obv: 能量潮指标（成交量累积）
    - obv_ma_5: OBV的5日均线
    - volume_change: 成交量变化率
    - volume_ma_ratio: 成交量比率（当前量/20日平均量）

13. 价格位置类 (2个)
    - close_to_sma20: 收盘价相对SMA20的位置（close/sma20 - 1）
    - close_to_sma60: 收盘价相对SMA60的位置（close/sma60 - 1）

14. 多周期动量类 (3个)
    - momentum_1w: 1周动量（5日收益率）
    - momentum_1m: 1月动量（20日收益率）
    - momentum_3m: 3月动量（60日收益率）

15. 时间序列统计特征类 (3个)
    - realized_vol_20: 20日已实现波动率（年化）
    - rolling_skew: 20日收益率偏度（分布不对称性）
    - rolling_kurt: 20日收益率峰度（分布尖峰程度）

    注意：这些特征来自 add_cross_sectional_features() 方法，
    虽然函数名包含"cross_sectional"，但实际是单股票的时间序列
    统计特征，并非跨股票的横截面比较特征。

16. 精选核心特征类 (17个，专为小样本设计，提升模型表现的关键)
    - momentum_5d/20d/60d: 多周期动量（短中长期）
    - close_position: 收盘价在当日高低区间的位置（0~1）
    - high_low_range: 当日高低价幅度（波动率信号）
    - close_vs_sma20/60: 收盘价相对均线的偏离度
    - volume_price_corr: 20日量价相关系数（量价配合信号）
    - volatility_20: 20日年化波动率（市场情绪）
    - trend_strength: 趋势强度（20日价格变化/ATR）
    - up_rate_20d/40d: 20日/40日上涨天数占比（多空胜率，核心指标）
    - drawdown_from_60d_high: 相对60日高点的回撤幅度
    - sma60_slope: SMA60的20日斜率（趋势方向）
    - sma20_below_sma60: SMA20是否低于SMA60（死叉信号，1=熊市，0=牛市）
    - bull_regime_uprate: 牛市体制多空胜率（均值回归信号，★核心特征）
    - bear_regime_momentum: 熊市体制动量延续（趋势跟踪信号，★核心特征）

特征总计：59个特征
实际训练：59个特征全部参与训练
特征选择：根据重要性和相关性筛选，通常保留20-30个最重要特征

模型选择逻辑：
- 回归任务：使用MAE/RMSE评估，关注IC（信息系数）
- 分类任务：使用准确率、精确率、召回率、F1分数评估
- 特征选择：基于LightGBM特征重要性（gain）+ 相关性去重（threshold=0.85）

★关键特征说明：
  - bull_regime_uprate: 牛市中低up_rate预示反弹（均值回归）
  - bear_regime_momentum: 熊市中负动量预示延续（趋势跟踪）
  - up_rate_20d/40d: 直接反映市场多空力量对比
  - 这两个体制交互特征解决了"均值回归vs趋势跟踪"的困境
"""
import numpy as np
import pandas as pd
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path
import logging
import joblib
from datetime import datetime

try:
    import lightgbm as lgb
    LIGHTGBM_AVAILABLE = True
except ImportError:
    LIGHTGBM_AVAILABLE = False

from .base_model import BaseModel

logger = logging.getLogger(__name__)


class LGBModel(BaseModel):
    """
    LightGBM模型

    用于股票价格预测的梯度提升模型
    """

    # 默认参数
    DEFAULT_PARAMS = {
        'objective': 'regression',
        'metric': 'mae',
        'boosting_type': 'gbdt',
        'num_leaves': 31,
        'learning_rate': 0.05,
        'feature_fraction': 0.9,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'min_child_samples': 20,
        'verbosity': -1,
        'random_state': 42,
    }

    # 训练参数
    DEFAULT_TRAIN_PARAMS = {
        'num_boost_round': 1000,
        'early_stopping_rounds': 100,
        'verbose_eval': 100
    }

    def __init__(
        self,
        model_id: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        train_params: Optional[Dict[str, Any]] = None
    ):
        """
        初始化LightGBM模型

        Args:
            model_id: 模型ID
            params: LightGBM超参数
            train_params: 训练参数
        """
        if not LIGHTGBM_AVAILABLE:
            raise ImportError("LightGBM is not installed. Install with: pip install lightgbm")

        super().__init__(model_id)

        # 合并参数
        self.params = {**self.DEFAULT_PARAMS, **(params or {})}
        self.train_params = {**self.DEFAULT_TRAIN_PARAMS, **(train_params or {})}

        # 特征名称（用于特征重要性）
        self.feature_names: Optional[List[str]] = None

        self.metadata['params'] = self.params
        self.metadata['train_params'] = self.train_params
        self.metadata['model_type'] = 'lightgbm'

    def train(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
        feature_names: Optional[List[str]] = None,
        categorical_features: Optional[List[str]] = None,
        **kwargs
    ) -> 'LGBModel':
        """
        训练模型

        Args:
            X_train: 训练特征
            y_train: 训练目标
            X_val: 验证特征
            y_val: 验证目标
            feature_names: 特征名称列表
            categorical_features: 分类特征名称列表
            **kwargs: 额外参数

        Returns:
            self
        """
        # 保存特征名称
        self.feature_names = feature_names

        # 创建数据集
        train_data = lgb.Dataset(X_train, label=y_train, categorical_feature=categorical_features)

        valid_sets = [train_data]
        valid_names = ['train']

        if X_val is not None and y_val is not None:
            val_data = lgb.Dataset(X_val, label=y_val, reference=train_data, categorical_feature=categorical_features)
            valid_sets.append(val_data)
            valid_names.append('valid')

        # 训练回调
        callbacks = [
            lgb.log_evaluation(self.train_params['verbose_eval'])
        ]

        if 'early_stopping_rounds' in self.train_params and X_val is not None:
            callbacks.append(lgb.early_stopping(self.train_params['early_stopping_rounds']))

        # 训练模型
        logger.info(f"Training LightGBM model with params: {self.params}")
        self.model = lgb.train(
            self.params,
            train_data,
            num_boost_round=self.train_params['num_boost_round'],
            valid_sets=valid_sets,
            valid_names=valid_names,
            callbacks=callbacks
        )

        self.is_fitted = True

        # 获取训练历史
        self._extract_training_history()

        # 计算特征重要性
        self._compute_feature_importance()

        # 保存元数据
        self.metadata['trained_at'] = datetime.now().isoformat()
        self.metadata['n_train_samples'] = len(X_train)
        self.metadata['n_features'] = X_train.shape[1]
        if X_val is not None:
            self.metadata['n_val_samples'] = len(X_val)

        # 保存特征名称到元数据
        if feature_names is not None:
            self.metadata['feature_names'] = feature_names

        logger.info(f"Model trained successfully. Best iteration: {self.model.best_iteration}")

        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        """
        生成预测

        Args:
            X: 特征矩阵

        Returns:
            预测值数组
        """
        if not self.is_fitted:
            raise RuntimeError("Model must be trained before prediction")

        predictions = self.model.predict(X, num_iteration=self.model.best_iteration)

        return predictions

    def predict_with_confidence(
        self,
        X: np.ndarray,
        n_bootstrap: int = 100,
        quantiles: Tuple[float, float] = (0.05, 0.95)
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        使用Bootstrap生成带置信区间的预测

        Args:
            X: 特征矩阵
            n_bootstrap: Bootstrap采样次数
            quantiles: 分位数 (lower, upper)

        Returns:
            (predictions, confidence_intervals)
        """
        # 点预测
        predictions = self.predict(X)

        # Bootstrap预测（使用不同迭代次数）
        all_preds = np.zeros((n_bootstrap, len(X)))
        num_trees = self.model.num_trees()

        # 对于树少的模型，使用更小的步长
        if num_trees < 5:
            # 树太少，在1到num_trees之间随机选择
            for i in range(n_bootstrap):
                n_trees_use = np.random.randint(1, num_trees + 1)
                all_preds[i] = self.model.predict(
                    X,
                    num_iteration=n_trees_use,
                    predict_disable_shape_check=True
                )
        else:
            # 正常情况：使用60%到100%之间的树
            min_trees = max(5, int(num_trees * 0.6))
            max_trees = num_trees
            for i in range(n_bootstrap):
                n_trees_use = np.random.randint(min_trees, max_trees + 1)
                all_preds[i] = self.model.predict(
                    X,
                    num_iteration=n_trees_use,
                    predict_disable_shape_check=True
                )

        # 计算分位数
        lower = np.quantile(all_preds, quantiles[0], axis=0)
        upper = np.quantile(all_preds, quantiles[1], axis=0)

        confidence_intervals = np.column_stack([lower, upper])

        logger.info(f"Bootstrap CI: {num_trees} trees, avg width={np.mean(upper - lower):.6f}")

        return predictions, confidence_intervals

    def save(self, path: str) -> None:
        """
        保存模型

        Args:
            path: 保存路径
        """
        if not self.is_fitted:
            raise RuntimeError("Cannot save unfitted model")

        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)

        # 保存模型
        model_path = f'{path}.pkl'
        joblib.dump(self.model, model_path)

        # 保存元数据
        self.save_metadata(path)

        logger.info(f"Model saved to {model_path}")

    def load(self, path: str) -> 'LGBModel':
        """
        加载模型

        Args:
            path: 模型文件路径

        Returns:
            self
        """
        # 加载模型
        model_path = f'{path}.pkl'
        self.model = joblib.load(model_path)

        # 加载元数据
        self.load_metadata(path)

        # 从元数据恢复特征名称
        self.feature_names = self.metadata.get('feature_names')

        self.is_fitted = True

        logger.info(f"Model loaded from {model_path}")

        return self

    def _extract_training_history(self) -> None:
        """从模型中提取训练历史"""
        if self.model is not None:
            try:
                evals_result = self.model.evals_result()

                if 'train' in evals_result:
                    metric_key = list(evals_result['train'].keys())[0]
                    self.training_history['train_loss'] = evals_result['train'][metric_key]

                if 'valid' in evals_result:
                    metric_key = list(evals_result['valid'].keys())[0]
                    self.training_history['val_loss'] = evals_result['valid'][metric_key]
            except (AttributeError, KeyError):
                # evals_result may not be available in all LightGBM versions
                logger.debug("Could not extract evals_result from model")
                pass

    def _compute_feature_importance(self) -> None:
        """计算特征重要性"""
        if self.model is not None:
            importance = self.model.feature_importance(importance_type='gain')

            if self.feature_names is not None:
                self.feature_importance = dict(zip(self.feature_names, importance))
            else:
                self.feature_importance = {f'feature_{i}': imp for i, imp in enumerate(importance)}

    def get_num_trees(self) -> int:
        """获取树的数量"""
        if self.model is not None:
            return self.model.num_trees()
        return 0

    def get_best_iteration(self) -> int:
        """获取最佳迭代次数"""
        if self.model is not None:
            return self.model.best_iteration
        return 0


def create_lgb_model(
    model_id: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None
) -> LGBModel:
    """
    便捷函数：创建LightGBM模型

    Args:
        model_id: 模型ID
        params: 模型参数

    Returns:
        LGBModel实例
    """
    return LGBModel(model_id=model_id, params=params)


def quick_train(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: Optional[np.ndarray] = None,
    y_val: Optional[np.ndarray] = None,
    feature_names: Optional[List[str]] = None
) -> LGBModel:
    """
    便捷函数：快速训练模型

    Args:
        X_train: 训练特征
        y_train: 训练目标
        X_val: 验证特征
        y_val: 验证目标
        feature_names: 特征名称

    Returns:
        训练好的模型
    """
    model = create_lgb_model()
    model.train(
        X_train, y_train,
        X_val, y_val,
        feature_names=feature_names
    )
    return model
