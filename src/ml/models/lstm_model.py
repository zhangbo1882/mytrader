"""
LSTM Model Implementation for Time Series Stock Price Prediction
"""
import numpy as np
import pandas as pd
from typing import Dict, Any, Optional, Tuple
from pathlib import Path
import logging
from datetime import datetime

try:
    import tensorflow as tf
    from tensorflow import keras
    from tensorflow.keras import layers, callbacks
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False

from .base_model import BaseModel

logger = logging.getLogger(__name__)


class LSTMModel(BaseModel):
    """
    LSTM模型用于时间序列股价预测

    使用多层LSTM网络捕获时间序列中的长期依赖关系
    """

    # 默认参数
    DEFAULT_PARAMS = {
        'lookback': 60,           # 输入窗口大小（交易日）
        'lstm_units': 64,         # LSTM单元数
        'num_layers': 2,          # LSTM层数
        'dropout': 0.2,           # Dropout比率
        'dense_units': 32,        # Dense层单元数
        'learning_rate': 0.001,
        'batch_size': 32,
        'epochs': 100,
        'early_stopping_patience': 10,
        'validation_split': 0.2
    }

    def __init__(
        self,
        model_id: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None
    ):
        """
        初始化LSTM模型

        Args:
            model_id: 模型ID
            params: 模型参数
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is not installed. Install with: pip install tensorflow")

        super().__init__(model_id)

        self.params = {**self.DEFAULT_PARAMS, **(params or {})}
        self.lookback = self.params['lookback']
        self.scaler = None  # 用于数据标准化

        self.metadata['params'] = self.params
        self.metadata['model_type'] = 'lstm'

    def _build_model(self, input_shape: Tuple[int, int]) -> 'keras.Model':
        """
        构建LSTM模型

        Args:
            input_shape: (timesteps, features)

        Returns:
            Keras Model
        """
        model = keras.Sequential(name='LSTM_Stock_Predictor')

        # 第一层LSTM
        model.add(layers.LSTM(
            self.params['lstm_units'],
            return_sequences=True,
            input_shape=input_shape,
            name='lstm_1'
        ))
        model.add(layers.Dropout(self.params['dropout'], name='dropout_1'))

        # 中间LSTM层
        for i in range(1, self.params['num_layers'] - 1):
            model.add(layers.LSTM(
                self.params['lstm_units'],
                return_sequences=True,
                name=f'lstm_{i+1}'
            ))
            model.add(layers.Dropout(self.params['dropout'], name=f'dropout_{i+1}'))

        # 最后一层LSTM
        if self.params['num_layers'] > 1:
            model.add(layers.LSTM(
                self.params['lstm_units'],
                return_sequences=False,
                name='lstm_last'
            ))
            model.add(layers.Dropout(self.params['dropout'], name='dropout_last'))

        # Dense层
        model.add(layers.Dense(
            self.params['dense_units'],
            activation='relu',
            name='dense_1'
        ))
        model.add(layers.Dropout(self.params['dropout'], name='dropout_dense'))

        # 输出层
        model.add(layers.Dense(1, name='output'))

        # 编译模型
        model.compile(
            optimizer=keras.optimizers.Adam(learning_rate=self.params['learning_rate']),
            loss='mse',
            metrics=['mae']
        )

        return model

    def _create_sequences(
        self,
        X: np.ndarray,
        y: np.ndarray
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        创建时间序列样本

        Args:
            X: 特征矩阵 (n_samples, n_features)
            y: 目标向量 (n_samples,)

        Returns:
            (X_seq, y_seq) 序列化数据
        """
        X_seq, y_seq = [], []

        for i in range(self.lookback, len(X)):
            X_seq.append(X[i-self.lookback:i])
            y_seq.append(y[i])

        return np.array(X_seq), np.array(y_seq)

    def train(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
        feature_names: Optional[list] = None,
        **kwargs
    ) -> 'LSTMModel':
        """
        训练LSTM模型

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
        from sklearn.preprocessing import StandardScaler

        # 保存特征名称
        self.feature_names = feature_names

        # 标准化数据
        self.scaler = StandardScaler()
        X_train_scaled = self.scaler.fit_transform(X_train)

        # 创建序列
        X_train_seq, y_train_seq = self._create_sequences(X_train_scaled, y_train)

        logger.info(f"Training LSTM with {len(X_train_seq)} sequences")

        # 准备验证数据
        validation_data = None
        if X_val is not None and y_val is not None:
            X_val_scaled = self.scaler.transform(X_val)
            X_val_seq, y_val_seq = self._create_sequences(X_val_scaled, y_val)
            validation_data = (X_val_seq, y_val_seq)
            logger.info(f"Validation with {len(X_val_seq)} sequences")

        # 构建模型
        input_shape = (X_train_seq.shape[1], X_train_seq.shape[2])
        self.model = self._build_model(input_shape)

        # 回调函数
        callback_list = [
            callbacks.EarlyStopping(
                monitor='val_loss',
                patience=self.params['early_stopping_patience'],
                restore_best_weights=True,
                verbose=1
            ),
            callbacks.ReduceLROnPlateau(
                monitor='val_loss',
                factor=0.5,
                patience=5,
                min_lr=1e-7,
                verbose=1
            )
        ]

        # 训练模型
        logger.info(f"Starting LSTM training...")
        history = self.model.fit(
            X_train_seq,
            y_train_seq,
            batch_size=self.params['batch_size'],
            epochs=self.params['epochs'],
            validation_data=validation_data,
            callbacks=callback_list,
            verbose=1
        )

        self.is_fitted = True

        # 保存训练历史
        self.training_history['train_loss'] = history.history['loss']
        if 'val_loss' in history.history:
            self.training_history['val_loss'] = history.history['val_loss']

        # 计算特征重要性（使用简单的排列重要性）
        self._compute_feature_importance(X_train_seq, y_train_seq)

        # 保存元数据
        self.metadata['trained_at'] = datetime.now().isoformat()
        self.metadata['n_train_samples'] = len(X_train)
        self.metadata['n_features'] = X_train.shape[1]
        if X_val is not None:
            self.metadata['n_val_samples'] = len(X_val)

        logger.info(f"LSTM training complete. Best epoch: {len(history.history['loss'])}")

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

        # 标准化
        X_scaled = self.scaler.transform(X)

        # 创建序列（只预测最后一个序列）
        if len(X) >= self.lookback:
            X_seq = X_scaled[-self.lookback:].reshape(1, self.lookback, -1)
            predictions = self.model.predict(X_seq, verbose=0)
            return predictions.flatten()
        else:
            # 数据不足，返回零
            return np.zeros(len(X))

    def predict_with_confidence(
        self,
        X: np.ndarray,
        n_bootstrap: int = 100,
        quantiles: Tuple[float, float] = (0.05, 0.95)
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        使用MC Dropout生成带置信区间的预测

        Args:
            X: 特征矩阵
            n_bootstrap: Bootstrap采样次数
            quantiles: 分位数

        Returns:
            (predictions, confidence_intervals)
        """
        if not self.is_fitted:
            raise RuntimeError("Model must be trained before prediction")

        # 标准化
        X_scaled = self.scaler.transform(X)

        if len(X) < self.lookback:
            return np.zeros(len(X)), np.column_stack([np.zeros(len(X)), np.zeros(len(X))])

        X_seq = X_scaled[-self.lookback:].reshape(1, self.lookback, -1)

        # MC Dropout预测
        predictions = []
        for _ in range(n_bootstrap):
            # 启用dropout进行预测
            pred = self.model(X_seq, training=True)
            predictions.append(pred.numpy().flatten())

        predictions = np.array(predictions)

        # 计算统计量
        mean_pred = np.mean(predictions, axis=0)
        lower = np.quantile(predictions, quantiles[0], axis=0)
        upper = np.quantile(predictions, quantiles[1], axis=0)

        confidence_intervals = np.column_stack([lower, upper])

        return mean_pred, confidence_intervals

    def _compute_feature_importance(
        self,
        X_seq: np.ndarray,
        y_seq: np.ndarray
    ) -> None:
        """
        计算特征重要性（使用基于排列的方法）

        Args:
            X_seq: 序列特征
            y_seq: 序列目标
        """
        if self.feature_names is None:
            self.feature_importance = {}
            return

        # 基准误差
        baseline_loss = self.model.evaluate(X_seq, y_seq, verbose=0)[0]

        importance = {}
        n_features = X_seq.shape[2]

        for i, feature_name in enumerate(self.feature_names):
            # 打乱该特征
            X_permuted = X_seq.copy()
            X_permuted[:, :, i] = np.random.permutation(X_permuted[:, :, i])

            # 计算新的误差
            permuted_loss = self.model.evaluate(X_permuted, y_seq, verbose=0)[0]

            # 重要性 = 误差增加
            importance[feature_name] = max(0, permuted_loss - baseline_loss)

        self.feature_importance = importance

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

        # 保存Keras模型
        model_path = f'{path}.keras'
        self.model.save(model_path)

        # 保存标准化器
        import joblib
        scaler_path = f'{path}_scaler.pkl'
        joblib.dump(self.scaler, scaler_path)

        # 保存元数据
        self.save_metadata(path)

        logger.info(f"LSTM model saved to {model_path}")

    def load(self, path: str) -> 'LSTMModel':
        """
        加载模型

        Args:
            path: 模型文件路径

        Returns:
            self
        """
        # 加载Keras模型
        model_path = f'{path}.keras'
        self.model = keras.models.load_model(model_path)

        # 加载标准化器
        import joblib
        scaler_path = f'{path}_scaler.pkl'
        self.scaler = joblib.load(scaler_path)

        # 加载元数据
        self.load_metadata(path)

        self.is_fitted = True

        logger.info(f"LSTM model loaded from {model_path}")

        return self


def create_lstm_model(
    model_id: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None
) -> LSTMModel:
    """
    便捷函数：创建LSTM模型

    Args:
        model_id: 模型ID
        params: 模型参数

    Returns:
        LSTMModel实例
    """
    return LSTMModel(model_id=model_id, params=params)
