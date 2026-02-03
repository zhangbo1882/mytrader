"""
LightGBM Model Implementation

Fast, efficient gradient boosting model for stock price prediction.
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

        # Bootstrap预测（使用每棵树的预测分布）
        # 获取所有boosting轮次的预测
        all_preds = np.zeros((n_bootstrap, len(X)))

        for i in range(n_bootstrap):
            # 随机采样树的预测
            all_preds[i] = self.model.predict(
                X,
                num_iteration=self.model.num_trees() - 1,
                predict_disable_shape_check=True
            )

        # 计算分位数
        lower = np.quantile(all_preds, quantiles[0], axis=0)
        upper = np.quantile(all_preds, quantiles[1], axis=0)

        confidence_intervals = np.column_stack([lower, upper])

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
