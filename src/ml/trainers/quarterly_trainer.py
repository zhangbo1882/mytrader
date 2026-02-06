"""
Quarterly Model Trainer Module

Handles training workflow for quarterly financial prediction models:
- Data loading and preparation
- Feature engineering
- Model training with time-series cross-validation
- Model evaluation
- Model persistence
"""
import numpy as np
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
import logging
from datetime import datetime
import json
import joblib

from ..quarterly_data_loader import QuarterlyFinancialDataLoader
from ..financial_feature_engineer import FinancialFeatureEngineer
from ..evaluators.quarterly_metrics import QuarterlyEvaluator
from ..utils import get_ml_db
from ..models.lgb_model import LGBModel

logger = logging.getLogger(__name__)


class QuarterlyTrainer:
    """
    季度财务预测模型训练器

    管理完整的训练流程：
    1. 数据加载
    2. 特征工程
    3. 数据集划分（时间序列交叉验证）
    4. 模型训练
    5. 模型评估
    6. 模型保存
    """

    def __init__(
        self,
        db_path: str = "data/tushare_data.db",
        model_save_dir: str = "data/ml_models",
        lgb_params: Optional[Dict[str, Any]] = None
    ):
        """
        初始化训练器

        Args:
            db_path: 数据库路径
            model_save_dir: 模型保存目录
            lgb_params: LightGBM参数
        """
        self.db_path = db_path
        self.model_save_dir = Path(model_save_dir)
        self.model_save_dir.mkdir(parents=True, exist_ok=True)

        # 默认LightGBM参数
        self.lgb_params = lgb_params or {
            'objective': 'regression',
            'metric': 'mae',
            'boosting_type': 'gbdt',
            'num_leaves': 31,
            'learning_rate': 0.05,
            'feature_fraction': 0.9,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'min_child_samples': 20,
            'verbose': -1,
            'n_estimators': 100
        }

        self.data_loader = QuarterlyFinancialDataLoader(db_path)
        self.feature_engineer = FinancialFeatureEngineer()
        self.evaluator = QuarterlyEvaluator()
        self.model: Optional[LGBModel] = None
        self.ml_db = get_ml_db(db_path)

    def train(
        self,
        symbols: List[str],
        start_quarter: str,
        end_quarter: str,
        feature_mode: str = "financial_only",
        train_mode: str = "multi",
        train_ratio: float = 0.7,
        val_ratio: float = 0.15,
        optimize_hyperparams: bool = False,
        model_id: Optional[str] = None,
        window_days: int = 60,
        skip_days: int = 5
    ) -> Dict[str, Any]:
        """
        完整训练流程

        Args:
            symbols: 股票代码列表
            start_quarter: 开始季度（如"2020Q1"）
            end_quarter: 结束季度（如"2024Q4"）
            feature_mode: 特征模式
                - "financial_only": 仅财务指标
                - "with_reports": 财务指标 + 报表数据
                - "with_valuation": 财务指标 + 估值指标
            train_mode: 训练模式
                - "single": 单股票模式（需要只提供1只股票）
                - "multi": 多股票模式
            train_ratio: 训练集比例
            val_ratio: 验证集比例
            optimize_hyperparams: 是否优化超参数
            model_id: 指定的模型ID（可选）
            window_days: 收益率计算窗口
            skip_days: 跳过交易日数

        Returns:
            训练结果字典
        """
        logger.info(f"Starting quarterly model training: {train_mode} mode for {len(symbols)} symbols")

        # 1. 加载数据
        logger.info("Loading quarterly financial data...")
        df = self.data_loader.load_quarterly_data(
            symbols=symbols,
            start_quarter=start_quarter,
            end_quarter=end_quarter,
            feature_mode=feature_mode,
            window_days=window_days,
            skip_days=skip_days
        )

        if df.empty:
            raise ValueError("No data loaded. Check your parameters and database.")

        # 2. 特征工程
        logger.info("Performing feature engineering...")
        df = self.feature_engineer.engineer_features(
            df,
            group_col='symbol' if train_mode == 'multi' else None
        )

        # 3. 准备训练数据
        feature_cols = self.data_loader.get_feature_columns(df)
        target_col = 'next_quarter_return'

        # 移除包含NaN的行
        df_clean = df[feature_cols + [target_col]].dropna()

        if len(df_clean) < 50:
            logger.warning(f"Insufficient samples after cleaning: {len(df_clean)}")

        logger.info(f"Prepared {len(df_clean)} samples with {len(feature_cols)} features")

        # 4. 划分数据集（按时间顺序）
        if train_mode == 'multi' and 'symbol' in df.columns:
            # 多股票模式：按时间划分
            df_clean = df_clean.sort_values(['year', 'quarter']).reset_index(drop=True)
        else:
            # 单股票模式：按时间划分
            df_clean = df_clean.sort_values(['year', 'quarter']).reset_index(drop=True)

        n = len(df_clean)
        train_end = int(n * train_ratio)
        val_end = int(n * (train_ratio + val_ratio))

        train_df = df_clean.iloc[:train_end].copy()
        val_df = df_clean.iloc[train_end:val_end].copy()
        test_df = df_clean.iloc[val_end:].copy()

        logger.info(f"Data split: train={len(train_df)}, val={len(val_df)}, test={len(test_df)}")

        # 5. 准备特征和目标
        X_train = train_df[feature_cols].values
        y_train = train_df[target_col].values

        X_val = val_df[feature_cols].values
        y_val = val_df[target_col].values

        X_test = test_df[feature_cols].values
        y_test = test_df[target_col].values

        # 6. 超参数优化（可选）
        if optimize_hyperparams:
            logger.info("Optimizing hyperparameters...")
            best_params = self._optimize_hyperparams(X_train, y_train, X_val, y_val)
            self.lgb_params.update(best_params)
            logger.info(f"Optimized params: {best_params}")

        # 7. 训练模型
        logger.info("Training LightGBM model...")
        if model_id is None:
            model_id = f"quarterly_lgb_{train_mode}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        self.model = LGBModel(model_id=model_id, params=self.lgb_params)
        self.model.train(
            X_train, y_train,
            X_val, y_val,
            feature_names=feature_cols
        )

        # 8. 评估模型
        logger.info("Evaluating model...")
        train_pred = self.model.predict(X_train)
        val_pred = self.model.predict(X_val)
        test_pred = self.model.predict(X_test)

        train_metrics = self.evaluator.evaluate(y_train, train_pred)
        val_metrics = self.evaluator.evaluate(y_val, val_pred)
        test_metrics = self.evaluator.evaluate(y_test, test_pred)

        # 获取特征重要性
        feature_importance = self.model.get_feature_importance()

        # 9. 保存模型
        logger.info("Saving model...")
        model_path = self.model_save_dir / model_id
        self.model.save(str(model_path))

        # 保存元数据
        metadata = {
            'symbols': symbols,
            'feature_mode': feature_mode,
            'train_mode': train_mode,
            'start_quarter': start_quarter,
            'end_quarter': end_quarter,
            'window_days': window_days,
            'skip_days': skip_days
        }

        # 保存到数据库
        self.ml_db.save_model_metadata(
            model_id=model_id,
            model_type='quarterly_lgb',
            symbol=','.join(symbols) if train_mode == 'multi' else symbols[0],
            hyperparameters=self.lgb_params,
            metrics=test_metrics,
            feature_importance=feature_importance,
            model_path=str(model_path),
            training_start=start_quarter,
            training_end=end_quarter,
            n_features=len(feature_cols),
            train_samples=len(X_train),
            val_samples=len(X_val),
            test_samples=len(X_test),
            target_type='next_quarter_return',
            metadata=metadata
        )

        # 10. 返回结果
        result = {
            'model_id': model_id,
            'train_mode': train_mode,
            'symbols': symbols,
            'feature_mode': feature_mode,
            'train_samples': len(X_train),
            'val_samples': len(X_val),
            'test_samples': len(X_test),
            'n_features': len(feature_cols),
            'train_metrics': train_metrics,
            'val_metrics': val_metrics,
            'test_metrics': test_metrics,
            'feature_importance': feature_importance,
            'model_path': str(model_path),
            'trained_at': datetime.now().isoformat(),
            'metadata': metadata
        }

        logger.info(f"Training complete. Test MAE: {test_metrics['mae']:.4f}, R²: {test_metrics['r2']:.4f}")

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
                'n_estimators': trial.suggest_int('n_estimators', 50, 200),
            }

            model = LGBModel(params={**self.lgb_params, **params})
            model.train(X_train, y_train, X_val, y_val)

            val_pred = model.predict(X_val)
            from sklearn.metrics import mean_absolute_error
            mae = mean_absolute_error(y_val, val_pred)

            return mae

        study = optuna.create_study(direction='minimize')
        study.optimize(objective, n_trials=n_trials)

        logger.info(f"Best trial: {study.best_trial.value}")
        logger.info(f"Best params: {study.best_params}")

        return study.best_params

    def predict(
        self,
        df: pd.DataFrame,
        model_id: Optional[str] = None
    ) -> np.ndarray:
        """
        使用训练好的模型进行预测

        Args:
            df: 包含特征的数据
            model_id: 模型ID（可选，使用当前模型）

        Returns:
            预测值数组
        """
        if model_id is not None:
            self.load_model(model_id)

        if self.model is None:
            raise RuntimeError("No trained model available. Please train or load a model first.")

        # 获取特征列
        feature_cols = self.data_loader.get_feature_columns(df)

        # 准备特征
        X = df[feature_cols].values

        # 预测
        predictions = self.model.predict(X)

        return predictions

    def load_model(self, model_id: str) -> None:
        """
        加载已保存的模型

        Args:
            model_id: 模型ID
        """
        model_path = self.model_save_dir / model_id

        self.model = LGBModel(model_id=model_id)
        self.model.load(str(model_path))

        logger.info(f"Loaded model {model_id}")

    def get_model_list(self) -> List[Dict[str, Any]]:
        """
        获取所有季度模型列表

        Returns:
            模型列表
        """
        return self.ml_db.list_models(model_type='quarterly_lgb')

    def evaluate_model(
        self,
        model_id: str,
        test_data: Optional[pd.DataFrame] = None
    ) -> Dict[str, Any]:
        """
        评估已训练的模型

        Args:
            model_id: 模型ID
            test_data: 测试数据（可选）

        Returns:
            评估结果
        """
        # 加载模型
        self.load_model(model_id)

        # 获取模型元数据
        model_metadata = self.ml_db.get_model(model_id)

        if model_metadata is None:
            raise ValueError(f"Model {model_id} not found in database")

        if test_data is not None:
            # 使用提供的测试数据
            predictions = self.predict(test_data)
            actual = test_data['next_quarter_return'].values
        else:
            # 从数据库获取测试结果
            # 这里简化处理，实际应该保存测试数据
            return {
                'model_id': model_id,
                'message': 'Please provide test_data for evaluation',
                'metadata': model_metadata
            }

        # 评估
        metrics = self.evaluator.evaluate(actual, predictions)

        return {
            'model_id': model_id,
            'metrics': metrics,
            'metadata': model_metadata
        }

    def generate_report(
        self,
        training_result: Dict[str, Any]
    ) -> str:
        """
        生成训练报告

        Args:
            training_result: train()方法返回的结果

        Returns:
            格式化的训练报告
        """
        return self.evaluator.generate_report(
            training_result['test_metrics'],
            training_result['model_id'],
            training_info=training_result.get('metadata')
        )

    def time_series_cross_validation(
        self,
        df: pd.DataFrame,
        feature_cols: List[str],
        target_col: str = 'next_quarter_return',
        n_splits: int = 5
    ) -> Dict[str, Any]:
        """
        时间序列交叉验证

        Args:
            df: 完整数据集
            feature_cols: 特征列
            target_col: 目标列
            n_splits: 折数

        Returns:
            交叉验证结果
        """
        from sklearn.model_selection import TimeSeriesSplit

        # 准备数据
        df_clean = df[feature_cols + [target_col]].dropna()
        df_clean = df_clean.sort_values(['year', 'quarter']).reset_index(drop=True)

        X = df_clean[feature_cols].values
        y = df_clean[target_col].values

        # 时间序列交叉验证
        tscv = TimeSeriesSplit(n_splits=n_splits)

        cv_results = []
        fold = 1

        for train_idx, val_idx in tscv.split(X):
            X_train_fold, X_val_fold = X[train_idx], X[val_idx]
            y_train_fold, y_val_fold = y[train_idx], y[val_idx]

            # 训练模型
            model = LGBModel(params=self.lgb_params)
            model.train(X_train_fold, y_train_fold, X_val_fold, y_val_fold)

            # 预测
            y_pred = model.predict(X_val_fold)

            # 评估
            metrics = self.evaluator.evaluate(y_val_fold, y_pred)

            cv_results.append({
                'fold': fold,
                'train_size': len(X_train_fold),
                'val_size': len(X_val_fold),
                **metrics
            })

            fold += 1

        # 计算平均指标
        avg_metrics = {}
        for key in cv_results[0].keys():
            if key not in ['fold', 'train_size', 'val_size']:
                values = [r[key] for r in cv_results]
                avg_metrics[f'{key}_mean'] = np.mean(values)
                avg_metrics[f'{key}_std'] = np.std(values)

        return {
            'cv_results': cv_results,
            'average_metrics': avg_metrics
        }
