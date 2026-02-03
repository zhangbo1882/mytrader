"""
Base Model Abstract Class

Defines the interface that all ML prediction models must implement.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Tuple
import pandas as pd
import numpy as np
import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class BaseModel(ABC):
    """
    ML模型基类

    所有预测模型必须继承此类并实现抽象方法
    """

    def __init__(self, model_id: Optional[str] = None):
        """
        初始化模型

        Args:
            model_id: 模型唯一标识符
        """
        self.model_id = model_id
        self.model = None
        self.is_fitted = False
        self.feature_importance: Dict[str, float] = {}
        self.metadata: Dict[str, Any] = {}
        self.training_history: Dict[str, list] = {
            'train_loss': [],
            'val_loss': []
        }

    @abstractmethod
    def train(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
        **kwargs
    ) -> 'BaseModel':
        """
        训练模型

        Args:
            X_train: 训练特征
            y_train: 训练目标
            X_val: 验证特征（可选）
            y_val: 验证目标（可选）
            **kwargs: 其他训练参数

        Returns:
            self
        """
        pass

    @abstractmethod
    def predict(self, X: np.ndarray) -> np.ndarray:
        """
        生成预测

        Args:
            X: 特征矩阵

        Returns:
            预测值数组
        """
        pass

    def predict_with_confidence(
        self,
        X: np.ndarray,
        n_bootstrap: int = 100
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        生成带置信区间的预测

        默认实现：返回预测值和零置信区间
        子类可以覆盖此方法提供真实的置信区间

        Args:
            X: 特征矩阵
            n_bootstrap: Bootstrap采样次数

        Returns:
            (predictions, confidence_intervals)
            confidence_intervals shape: (n_samples, 2) -> [lower, upper]
        """
        predictions = self.predict(X)

        # 默认返回零置信区间
        confidence_intervals = np.column_stack([
            predictions,  # lower = prediction
            predictions   # upper = prediction
        ])

        return predictions, confidence_intervals

    @abstractmethod
    def save(self, path: str) -> None:
        """
        保存模型到文件

        Args:
            path: 保存路径
        """
        pass

    @abstractmethod
    def load(self, path: str) -> 'BaseModel':
        """
        从文件加载模型

        Args:
            path: 模型文件路径

        Returns:
            self
        """
        pass

    def evaluate(
        self,
        X: np.ndarray,
        y: np.ndarray,
        metrics: Optional[list] = None
    ) -> Dict[str, float]:
        """
        评估模型性能

        Args:
            X: 特征矩阵
            y: 真实目标值
            metrics: 评估指标列表 ['mae', 'rmse', 'mape', 'r2']

        Returns:
            评估指标字典
        """
        from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

        if metrics is None:
            metrics = ['mae', 'rmse', 'mape', 'r2']

        predictions = self.predict(X)

        results = {}

        if 'mae' in metrics:
            results['mae'] = float(mean_absolute_error(y, predictions))

        if 'rmse' in metrics:
            results['rmse'] = float(np.sqrt(mean_squared_error(y, predictions)))

        if 'mape' in metrics:
            # 避免除零
            mask = np.abs(y) > 1e-6
            if mask.sum() > 0:
                results['mape'] = float(np.mean(np.abs((y[mask] - predictions[mask]) / y[mask])) * 100)
            else:
                results['mape'] = float('inf')

        if 'r2' in metrics:
            results['r2'] = float(r2_score(y, predictions))

        # 方向准确率（对于回归任务）
        if 'direction_accuracy' in metrics:
            if len(y) > 1:
                actual_direction = np.sign(y[1:] - y[:-1])
                pred_direction = np.sign(predictions[1:] - predictions[:-1])
                results['direction_accuracy'] = float(np.mean(actual_direction == pred_direction))
            else:
                results['direction_accuracy'] = 0.0

        return results

    def get_feature_importance(self) -> Dict[str, float]:
        """
        获取特征重要性

        Returns:
            特征重要性字典
        """
        return self.feature_importance

    def get_params(self) -> Dict[str, Any]:
        """
        获取模型参数

        Returns:
            参数字典
        """
        return self.metadata.get('params', {})

    def set_params(self, **params) -> 'BaseModel':
        """
        设置模型参数

        Args:
            **params: 参数键值对

        Returns:
            self
        """
        self.metadata['params'] = params
        return self

    def get_metadata(self) -> Dict[str, Any]:
        """
        获取模型元数据

        Returns:
            元数据字典
        """
        return {
            'model_id': self.model_id,
            'is_fitted': self.is_fitted,
            'feature_importance': self.feature_importance,
            'training_history': self.training_history,
            **self.metadata
        }

    def save_metadata(self, path: str) -> None:
        """
        保存模型元数据到JSON文件

        Args:
            path: 保存路径（不含扩展名）
        """
        metadata = self.get_metadata()

        # 转换numpy类型为Python原生类型
        def convert(obj):
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, dict):
                return {k: convert(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert(v) for v in obj]
            return obj

        metadata = convert(metadata)

        with open(f'{path}.json', 'w') as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"Saved metadata to {path}.json")

    def load_metadata(self, path: str) -> Dict[str, Any]:
        """
        从JSON文件加载模型元数据

        Args:
            path: 文件路径（不含扩展名）

        Returns:
            元数据字典
        """
        with open(f'{path}.json', 'r') as f:
            metadata = json.load(f)

        self.model_id = metadata.get('model_id')
        self.is_fitted = metadata.get('is_fitted', False)
        self.feature_importance = metadata.get('feature_importance', {})
        self.training_history = metadata.get('training_history', {'train_loss': [], 'val_loss': []})

        # 保留其他元数据
        for key, value in metadata.items():
            if key not in ['model_id', 'is_fitted', 'feature_importance', 'training_history']:
                self.metadata[key] = value

        logger.info(f"Loaded metadata from {path}.json")
        return metadata

    def __repr__(self) -> str:
        """字符串表示"""
        return f"{self.__class__.__name__}(model_id='{self.model_id}', fitted={self.is_fitted})"
