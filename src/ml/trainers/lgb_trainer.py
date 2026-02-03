"""
LightGBM Trainer Module

Handles the training workflow for LightGBM models including:
- Data preparation
- Model training
- Hyperparameter tuning
- Model evaluation
"""
import numpy as np
import pandas as pd
from typing import Dict, Any, Optional, Tuple, List
from pathlib import Path
import logging
from datetime import datetime
import json

from ..models.lgb_model import LGBModel
from ..preprocessor import FeatureEngineer
from ..evaluators.metrics import ModelEvaluator

logger = logging.getLogger(__name__)


class LGBTrainer:
    """
    LightGBM训练器

    管理完整的训练流程
    """

    def __init__(
        self,
        model_save_dir: str = "data/ml_models",
        params: Optional[Dict[str, Any]] = None
    ):
        """
        初始化训练器

        Args:
            model_save_dir: 模型保存目录
            params: LightGBM参数
        """
        self.model_save_dir = Path(model_save_dir)
        self.model_save_dir.mkdir(parents=True, exist_ok=True)

        self.params = params or {}
        self.model: Optional[LGBModel] = None
        self.evaluator = ModelEvaluator()

    def train(
        self,
        df: pd.DataFrame,
        target_type: str = "return_1d",
        feature_cols: Optional[List[str]] = None,
        train_ratio: float = 0.7,
        val_ratio: float = 0.15,
        symbol: str = "unknown",
        add_technical: bool = True,
        optimize_hyperparams: bool = False,
        n_trials: int = 50
    ) -> Dict[str, Any]:
        """
        完整训练流程

        Args:
            df: 原始数据DataFrame
            target_type: 目标类型
            feature_cols: 特征列名列表
            train_ratio: 训练集比例
            val_ratio: 验证集比例
            symbol: 股票代码
            add_technical: 是否添加技术指标
            optimize_hyperparams: 是否优化超参数
            n_trials: 超参数优化试验次数

        Returns:
            训练结果字典
        """
        logger.info(f"Starting training for {symbol}")

        # 1. 特征工程
        engineer = FeatureEngineer(scaler_type='standard')

        if add_technical:
            df = engineer.add_technical_indicators(df)
            df = engineer.add_cross_sectional_features(df)

        # 2. 创建目标
        df = engineer.create_target(df, target_type=target_type)

        # 3. 准备数据
        X, y = engineer.prepare_training_data(
            df,
            feature_cols=feature_cols,
            handle_missing='drop'
        )

        # 4. 划分数据集
        n = len(X)
        train_end = int(n * train_ratio)
        val_end = int(n * (train_ratio + val_ratio))

        X_train, y_train = X[:train_end], y[:train_end]
        X_val, y_val = X[train_end:val_end], y[train_end:val_end]
        X_test, y_test = X[val_end:], y[val_end:]

        # 5. 标准化
        X_train = engineer.fit_transform(X_train)
        X_val = engineer.transform(X_val)
        X_test = engineer.transform(X_test)

        feature_names = engineer.feature_cols

        logger.info(f"Data split: train={len(X_train)}, val={len(X_val)}, test={len(X_test)}")

        # 6. 超参数优化（可选）
        if optimize_hyperparams:
            best_params = self._optimize_hyperparams(
                X_train, y_train, X_val, y_val, n_trials
            )
            self.params.update(best_params)
            logger.info(f"Optimized params: {best_params}")

        # 7. 创建并训练模型
        model_id = f"lgb_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        self.model = LGBModel(model_id=model_id, params=self.params)
        self.model.train(
            X_train, y_train,
            X_val, y_val,
            feature_names=feature_names
        )

        # 8. 评估模型
        train_metrics = self.model.evaluate(X_train, y_train)
        val_metrics = self.model.evaluate(X_val, y_val)
        test_metrics = self.model.evaluate(X_test, y_test)

        # 9. 保存模型
        model_path = self.model_save_dir / model_id
        self.model.save(str(model_path))

        # 10. 返回结果
        result = {
            'model_id': model_id,
            'symbol': symbol,
            'target_type': target_type,
            'train_samples': len(X_train),
            'val_samples': len(X_val),
            'test_samples': len(X_test),
            'n_features': X_train.shape[1],
            'train_metrics': train_metrics,
            'val_metrics': val_metrics,
            'test_metrics': test_metrics,
            'feature_importance': self.model.get_feature_importance(),
            'model_path': str(model_path),
            'trained_at': datetime.now().isoformat()
        }

        logger.info(f"Training complete. Test MAE: {test_metrics.get('mae', 'N/A')}")

        return result

    def _optimize_hyperparams(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
        n_trials: int = 50
    ) -> Dict[str, Any]:
        """
        使用Optuna优化超参数

        Args:
            X_train: 训练特征
            y_train: 训练目标
            X_val: 验证特征
            y_val: 验证目标
            n_trials: 试验次数

        Returns:
            最佳参数字典
        """
        try:
            import optuna
        except ImportError:
            logger.warning("Optuna not installed. Skipping hyperparameter optimization.")
            return {}

        def objective(trial):
            params = {
                'num_leaves': trial.suggest_int('num_leaves', 20, 100),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.2, log=True),
                'feature_fraction': trial.suggest_float('feature_fraction', 0.7, 1.0),
                'bagging_fraction': trial.suggest_float('bagging_fraction', 0.7, 1.0),
                'bagging_freq': trial.suggest_int('bagging_freq', 1, 10),
                'min_child_samples': trial.suggest_int('min_child_samples', 10, 50),
            }

            model = LGBModel(params={**self.params, **params})
            model.train(X_train, y_train, X_val, y_val)

            val_metrics = model.evaluate(X_val, y_val)
            return val_metrics['mae']

        study = optuna.create_study(direction='minimize')
        study.optimize(objective, n_trials=n_trials)

        logger.info(f"Best trial: {study.best_trial.value}")
        logger.info(f"Best params: {study.best_params}")

        return study.best_params

    def train_multiple_symbols(
        self,
        data_dict: Dict[str, pd.DataFrame],
        target_type: str = "return_1d",
        **kwargs
    ) -> Dict[str, Dict[str, Any]]:
        """
        批量训练多只股票的模型

        Args:
            data_dict: {symbol: DataFrame} 字典
            target_type: 目标类型
            **kwargs: 其他训练参数

        Returns:
            {symbol: training_result} 字典
        """
        results = {}

        for symbol, df in data_dict.items():
            try:
                result = self.train(
                    df=df,
                    target_type=target_type,
                    symbol=symbol,
                    **kwargs
                )
                results[symbol] = result
            except Exception as e:
                logger.error(f"Failed to train model for {symbol}: {e}")
                results[symbol] = {'error': str(e)}

        return results

    def load_model(self, model_id: str) -> LGBModel:
        """
        加载已保存的模型

        Args:
            model_id: 模型ID

        Returns:
            加载的模型
        """
        model_path = self.model_save_dir / model_id

        self.model = LGBModel(model_id=model_id)
        self.model.load(str(model_path))

        logger.info(f"Loaded model {model_id}")

        return self.model

    def predict(
        self,
        X: np.ndarray,
        model_id: Optional[str] = None
    ) -> np.ndarray:
        """
        生成预测

        Args:
            X: 特征矩阵
            model_id: 模型ID（可选，使用当前模型）

        Returns:
            预测值
        """
        if model_id is not None:
            self.load_model(model_id)

        if self.model is None or not self.model.is_fitted:
            raise RuntimeError("No trained model available")

        return self.model.predict(X)

    def get_trained_models(self) -> List[str]:
        """
        获取所有已训练的模型列表

        Returns:
            模型ID列表
        """
        models = []

        for path in self.model_save_dir.glob("lgb_*.pkl"):
            model_id = path.stem
            models.append(model_id)

        return sorted(models)

    def delete_model(self, model_id: str) -> None:
        """
        删除模型文件

        Args:
            model_id: 模型ID
        """
        model_path = self.model_save_dir / model_id

        # 删除模型文件
        if model_path.with_suffix('.pkl').exists():
            model_path.with_suffix('.pkl').unlink()

        # 删除元数据文件
        if model_path.with_suffix('.json').exists():
            model_path.with_suffix('.json').unlink()

        logger.info(f"Deleted model {model_id}")
