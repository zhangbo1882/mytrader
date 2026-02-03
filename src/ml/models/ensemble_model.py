"""
Ensemble Model Implementation

Combines multiple models for robust predictions.
"""
import numpy as np
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
import logging
from datetime import datetime
import json

from .base_model import BaseModel
from .lgb_model import LGBModel
from .lstm_model import LSTMModel

logger = logging.getLogger(__name__)


class EnsembleModel(BaseModel):
    """
    集成模型

    组合多个基模型的预测结果，提高预测鲁棒性
    支持简单平均、加权平均、stacking等方法
    """

    # 默认参数
    DEFAULT_PARAMS = {
        'method': 'weighted_average',  # 'simple_average', 'weighted_average', 'stacking'
        'weights': {'lgb': 0.6, 'lstm': 0.4},  # 默认权重
        'meta_model': 'ridge',  # stacking元模型: 'ridge', 'lasso', 'elasticnet'
    }

    def __init__(
        self,
        model_id: Optional[str] = None,
        models: Optional[Dict[str, BaseModel]] = None,
        params: Optional[Dict[str, Any]] = None
    ):
        """
        初始化集成模型

        Args:
            model_id: 模型ID
            models: 基模型字典 {'lgb': LGBModel, 'lstm': LSTMModel}
            params: 集成参数
        """
        super().__init__(model_id)

        self.params = {**self.DEFAULT_PARAMS, **(params or {})}
        self.models = models or {}
        self.meta_model = None  # 用于stacking的元模型

        self.metadata['params'] = self.params
        self.metadata['model_type'] = 'ensemble'

    def add_model(self, name: str, model: BaseModel) -> None:
        """
        添加基模型

        Args:
            name: 模型名称
            model: 模型实例
        """
        self.models[name] = model
        logger.info(f"Added model '{name}' to ensemble")

    def remove_model(self, name: str) -> None:
        """
        移除基模型

        Args:
            name: 模型名称
        """
        if name in self.models:
            del self.models[name]
            logger.info(f"Removed model '{name}' from ensemble")

    def train(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
        feature_names: Optional[list] = None,
        **kwargs
    ) -> 'EnsembleModel':
        """
        训练集成模型

        Args:
            X_train: 训练特征
            y_train: 训练目标
            X_val: 验证特征
            y_val: 验证目标
            feature_names: 特征名称列表
            **kwargs: 额外参数

        Returns:
            self
        """
        self.feature_names = feature_names

        # 训练所有基模型
        for name, model in self.models.items():
            logger.info(f"Training base model '{name}'...")
            model.train(
                X_train, y_train,
                X_val, y_val,
                feature_names=feature_names
            )

        # 如果使用stacking，训练元模型
        if self.params['method'] == 'stacking' and X_val is not None:
            self._train_meta_model(X_val, y_val)

        self.is_fitted = True

        # 计算集成特征重要性
        self._compute_ensemble_importance()

        # 保存元数据
        self.metadata['trained_at'] = datetime.now().isoformat()
        self.metadata['n_train_samples'] = len(X_train)
        self.metadata['n_features'] = X_train.shape[1]
        self.metadata['base_models'] = list(self.models.keys())

        logger.info(f"Ensemble training complete with {len(self.models)} base models")

        return self

    def _train_meta_model(self, X_val: np.ndarray, y_val: np.ndarray) -> None:
        """
        训练元模型（用于stacking）

        Args:
            X_val: 验证特征
            y_val: 验证目标
        """
        from sklearn.linear_model import Ridge, Lasso, ElasticNet

        # 获取各基模型的预测结果
        predictions = []
        for name, model in self.models.items():
            pred = model.predict(X_val)
            predictions.append(pred)

        # 堆叠预测结果作为元特征
        meta_features = np.column_stack(predictions)

        # 选择元模型
        meta_type = self.params['meta_model']
        if meta_type == 'ridge':
            self.meta_model = Ridge(alpha=1.0)
        elif meta_type == 'lasso':
            self.meta_model = Lasso(alpha=0.1)
        elif meta_type == 'elasticnet':
            self.meta_model = ElasticNet(alpha=0.1, l1_ratio=0.5)
        else:
            self.meta_model = Ridge(alpha=1.0)

        # 训练元模型
        self.meta_model.fit(meta_features, y_val)

        logger.info(f"Meta-model ({meta_type}) trained")

    def predict(self, X: np.ndarray) -> np.ndarray:
        """
        生成集成预测

        Args:
            X: 特征矩阵

        Returns:
            预测值数组
        """
        if not self.is_fitted:
            raise RuntimeError("Model must be trained before prediction")

        # 获取各基模型预测
        predictions = {}
        for name, model in self.models.items():
            try:
                pred = model.predict(X)
                predictions[name] = pred
            except Exception as e:
                logger.warning(f"Failed to get prediction from '{name}': {e}")
                predictions[name] = np.zeros(len(X))

        # 根据方法组合预测
        if self.params['method'] == 'simple_average':
            return self._simple_average(predictions)

        elif self.params['method'] == 'weighted_average':
            return self._weighted_average(predictions)

        elif self.params['method'] == 'stacking':
            return self._stacking_predict(predictions)

        else:
            raise ValueError(f"Unknown ensemble method: {self.params['method']}")

    def _simple_average(self, predictions: Dict[str, np.ndarray]) -> np.ndarray:
        """简单平均"""
        pred_matrix = np.column_stack([predictions[name] for name in self.models.keys()])
        return np.mean(pred_matrix, axis=1)

    def _weighted_average(self, predictions: Dict[str, np.ndarray]) -> np.ndarray:
        """加权平均"""
        weights = self.params['weights']

        # 归一化权重
        total_weight = sum(weights.get(name, 0) for name in self.models.keys())
        if total_weight == 0:
            return self._simple_average(predictions)

        weighted_sum = np.zeros(len(next(iter(predictions.values()))))
        for name, model_name in enumerate(self.models.keys()):
            weight = weights.get(model_name, 1.0 / len(self.models))
            weighted_sum += weight * predictions[model_name]

        return weighted_sum / total_weight

    def _stacking_predict(self, predictions: Dict[str, np.ndarray]) -> np.ndarray:
        """使用元模型预测"""
        if self.meta_model is None:
            # 元模型未训练，回退到加权平均
            logger.warning("Meta-model not trained, falling back to weighted average")
            return self._weighted_average(predictions)

        # 堆叠预测
        meta_features = np.column_stack([predictions[name] for name in self.models.keys()])
        return self.meta_model.predict(meta_features)

    def predict_with_confidence(
        self,
        X: np.ndarray,
        n_bootstrap: int = 100,
        quantiles: Tuple[float, float] = (0.05, 0.95)
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        生成带置信区间的预测

        Args:
            X: 特征矩阵
            n_bootstrap: Bootstrap采样次数
            quantiles: 分位数

        Returns:
            (predictions, confidence_intervals)
        """
        if not self.is_fitted:
            raise RuntimeError("Model must be trained before prediction")

        # 获取各基模型的置信区间预测
        all_predictions = []
        all_intervals = []

        for name, model in self.models.items():
            try:
                pred, interval = model.predict_with_confidence(X, n_bootstrap, quantiles)
                all_predictions.append(pred)
                all_intervals.append(interval)
            except Exception as e:
                logger.warning(f"Failed to get confidence prediction from '{name}': {e}")

        if not all_predictions:
            return np.zeros(len(X)), np.column_stack([np.zeros(len(X)), np.zeros(len(X))])

        # 平均预测
        mean_pred = np.mean(all_predictions, axis=0)

        # 平均置信区间
        mean_lower = np.mean([interv[:, 0] for interv in all_intervals], axis=0)
        mean_upper = np.mean([interv[:, 1] for interv in all_intervals], axis=0)

        confidence_intervals = np.column_stack([mean_lower, mean_upper])

        return mean_pred, confidence_intervals

    def _compute_ensemble_importance(self) -> None:
        """
        计算集成特征重要性

        综合各基模型的特征重要性
        """
        all_importance = {}

        # 收集所有模型的特征重要性
        for name, model in self.models.items():
            if hasattr(model, 'feature_importance') and model.feature_importance:
                weight = self.params['weights'].get(name, 1.0 / len(self.models))
                for feature, importance in model.feature_importance.items():
                    if feature not in all_importance:
                        all_importance[feature] = 0
                    all_importance[feature] += weight * importance

        self.feature_importance = all_importance

    def evaluate_ensemble_performance(
        self,
        X: np.ndarray,
        y: np.ndarray
    ) -> Dict[str, Any]:
        """
        评估集成模型性能（包括各基模型）

        Args:
            X: 特征矩阵
            y: 真实目标

        Returns:
            性能指标字典
        """
        results = {}

        # 评估各基模型
        for name, model in self.models.items():
            try:
                metrics = model.evaluate(X, y)
                results[f'base_{name}'] = metrics
            except Exception as e:
                logger.warning(f"Failed to evaluate '{name}': {e}")

        # 评估集成模型
        ensemble_metrics = self.evaluate(X, y)
        results['ensemble'] = ensemble_metrics

        # 计算提升
        if len(self.models) > 0:
            base_name = list(self.models.keys())[0]
            base_mae = results.get(f'base_{base_name}', {}).get('mae', ensemble_metrics['mae'])
            ensemble_mae = ensemble_metrics['mae']

            improvement = (base_mae - ensemble_mae) / base_mae * 100 if base_mae > 0 else 0
            results['improvement_over_base'] = improvement

        return results

    def save(self, path: str) -> None:
        """
        保存集成模型

        Args:
            path: 保存路径
        """
        if not self.is_fitted:
            raise RuntimeError("Cannot save unfitted model")

        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)

        # 保存各基模型
        model_dir = path_obj.parent / path_obj.name
        model_dir.mkdir(exist_ok=True)

        for name, model in self.models.items():
            model_path = model_dir / f"{name}_model"
            model.save(str(model_path))

        # 保存元模型
        if self.meta_model is not None:
            import joblib
            meta_path = model_dir / "meta_model.pkl"
            joblib.dump(self.meta_model, str(meta_path))

        # 保存元数据
        self.save_metadata(path)

        logger.info(f"Ensemble model saved to {model_dir}")

    def load(self, path: str) -> 'EnsembleModel':
        """
        加载集成模型

        Args:
            path: 模型文件路径

        Returns:
            self
        """
        path_obj = Path(path)
        model_dir = path_obj.parent / path_obj.name

        # 加载各基模型
        self.models = {}
        for model_file in model_dir.glob("*_model.pkl"):
            name = model_file.stem.replace("_model", "")

            # 根据名称确定模型类型
            if 'lgb' in name:
                model = LGBModel(model_id=name)
            elif 'lstm' in name:
                model = LSTMModel(model_id=name)
            else:
                continue

            model.load(str(model_file.with_suffix("")))
            self.models[name] = model

        # 加载元模型
        meta_path = model_dir / "meta_model.pkl"
        if meta_path.exists():
            import joblib
            self.meta_model = joblib.load(str(meta_path))

        # 加载元数据
        self.load_metadata(path)

        self.is_fitted = True

        logger.info(f"Ensemble model loaded from {model_dir}")

        return self


def create_ensemble_model(
    model_id: Optional[str] = None,
    models: Optional[Dict[str, BaseModel]] = None,
    method: str = 'weighted_average',
    weights: Optional[Dict[str, float]] = None
) -> EnsembleModel:
    """
    便捷函数：创建集成模型

    Args:
        model_id: 模型ID
        models: 基模型字典
        method: 集成方法
        weights: 模型权重

    Returns:
        EnsembleModel实例
    """
    params = {'method': method}
    if weights:
        params['weights'] = weights

    return EnsembleModel(model_id=model_id, models=models, params=params)


def create_default_ensemble(model_id: Optional[str] = None) -> EnsembleModel:
    """
    创建默认的LGB+LSTM集成模型

    Args:
        model_id: 模型ID

    Returns:
        EnsembleModel实例
    """
    models = {
        'lgb': LGBModel(model_id=f'{model_id}_lgb' if model_id else None),
        'lstm': LSTMModel(model_id=f'{model_id}_lstm' if model_id else None)
    }

    return EnsembleModel(
        model_id=model_id,
        models=models,
        params={
            'method': 'weighted_average',
            'weights': {'lgb': 0.6, 'lstm': 0.4}
        }
    )
