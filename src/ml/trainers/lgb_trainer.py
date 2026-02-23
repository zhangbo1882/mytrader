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
        n_trials: int = 50,
        use_feature_selection: bool = True,
        top_k_features: int = 30
    ) -> Dict[str, Any]:
        """
        完整训练流程（含精选特征工程和两阶段特征选择）

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
            use_feature_selection: 是否启用两阶段特征选择（推荐小样本开启）
            top_k_features: 最终保留的特征数量（use_feature_selection=True时生效）

        Returns:
            训练结果字典
        """
        logger.info(f"Starting training for {symbol}")

        # 1. 特征工程
        engineer = FeatureEngineer(scaler_type='standard')

        if add_technical:
            df = engineer.add_technical_indicators(df)
            df = engineer.add_cross_sectional_features(df)
            df = engineer.add_essential_features(df)  # 精选核心特征

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

        # 保存训练目标统计（用于预测时去偏）
        y_train_mean = float(y_train.mean())
        y_train_std  = float(y_train.std())
        logger.info(
            f"Training target: mean={y_train_mean*100:.3f}%, std={y_train_std*100:.3f}%, "
            f"up_rate={(y_train > (0.5 if 'direction' in target_type else 0)).mean()*100:.1f}%"
        )

        # 5. 标准化
        X_train = engineer.fit_transform(X_train)
        X_val = engineer.transform(X_val)
        X_test = engineer.transform(X_test)

        feature_names = engineer.feature_cols

        logger.info(f"Data split: train={len(X_train)}, val={len(X_val)}, test={len(X_test)}")

        # 小样本策略：训练集 < 300 时不使用验证集早停，避免过早终止
        small_sample_mode = len(X_train) < 300
        if small_sample_mode:
            logger.info(
                f"Small dataset detected ({len(X_train)} samples < 300), "
                "disabling early stopping to prevent premature termination"
            )

        # 6. 根据目标类型调整模型参数
        if 'direction' in target_type:
            # 分类任务：使用二分类参数

            # 计算类别分布
            unique, counts = np.unique(y_train, return_counts=True)
            class_dist = dict(zip(unique, counts))
            logger.info(f"Training data class distribution: {class_dist}")

            # 计算类别权重（处理不平衡）
            if len(unique) == 2:
                # scale_pos_weight = 负类样本数 / 正类样本数
                # LightGBM的scale_pos_weight用于平衡正负样本
                neg_count = counts[0] if unique[0] == 0 else counts[1]
                pos_count = counts[1] if unique[1] == 1 else counts[0]
                scale_pos_weight = neg_count / pos_count if pos_count > 0 else 1.0

                logger.info(f"Class balance: scale_pos_weight={scale_pos_weight:.2f} (neg={neg_count}, pos={pos_count})")
            else:
                scale_pos_weight = 1.0

            default_params = {
                'objective': 'binary',
                'metric': ['binary_logloss', 'auc'],
                'boosting_type': 'gbdt',
                'num_leaves': 10,           # 31 → 10，防过拟合
                'max_depth': 4,             # 限制树深
                'learning_rate': 0.01,      # 0.05 → 0.01，更稳定
                'feature_fraction': 0.75,   # 0.9 → 0.75，增加随机性
                'bagging_fraction': 0.7,    # 0.8 → 0.7
                'bagging_freq': 3,
                'min_child_samples': 50,    # 20 → 50，每叶最少样本
                'min_child_weight': 0.01,
                'reg_alpha': 0.5,           # L1正则化
                'reg_lambda': 2.0,          # L2正则化
                'scale_pos_weight': scale_pos_weight,
                'verbosity': -1,
                'random_state': 42,
            }
            default_train_params = {
                'num_boost_round': 500,     # 1000 → 500
                'early_stopping_rounds': 50, # 100 → 50
                'verbose_eval': 50
            }
            # 小样本禁止早停
            if small_sample_mode:
                default_train_params = {
                    'num_boost_round': 300,
                    'early_stopping_rounds': None,
                    'verbose_eval': 100
                }
            # 合并用户参数（用户参数覆盖默认值）
            final_params = {**default_params, **self.params}
            final_train_params = {**default_train_params}
            logger.info(f"Using classification params for {target_type}: objective={final_params.get('objective')}")
        else:
            # 回归任务：使用回归参数
            default_params = {
                'objective': 'regression',
                'metric': ['mae', 'rmse'],  # 同时监控两个指标
                'boosting_type': 'gbdt',
                'num_leaves': 15,           # 31 → 15，防过拟合
                'max_depth': 5,             # 限制树深
                'learning_rate': 0.01,      # 0.05 → 0.01，更稳定
                'feature_fraction': 0.8,    # 0.9 → 0.8
                'bagging_fraction': 0.7,    # 0.8 → 0.7
                'bagging_freq': 3,
                'min_child_samples': 40,    # 20 → 40，每叶最少样本
                'min_child_weight': 0.01,
                'reg_alpha': 0.1,           # L1正则化
                'reg_lambda': 1.0,          # L2正则化
                'verbosity': -1,
                'random_state': 42,
                'deterministic': True,      # 确保可复现
            }
            default_train_params = {
                'num_boost_round': 500,     # 1000 → 500
                'early_stopping_rounds': 50, # 100 → 50
                'verbose_eval': 50
            }
            # 小样本禁止早停
            if small_sample_mode:
                default_train_params = {
                    'num_boost_round': 300,
                    'early_stopping_rounds': None,
                    'verbose_eval': 100
                }
            # 合并用户参数
            final_params = {**default_params, **self.params}
            final_train_params = {**default_train_params}
            logger.info(f"Using regression params for {target_type}: objective={final_params.get('objective')}")

        # 超参数优化（可选）
        if optimize_hyperparams:
            best_params = self._optimize_hyperparams(
                X_train, y_train, X_val, y_val, n_trials
            )
            final_params.update(best_params)
            logger.info(f"Optimized params: {best_params}")

        # 7. 创建并训练初始模型
        model_id = f"lgb_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        self.model = LGBModel(model_id=model_id, params=final_params, train_params=final_train_params)
        # 小样本模式：不使用验证集，避免早停在第1轮触发
        train_val_X = None if small_sample_mode else X_val
        train_val_y = None if small_sample_mode else y_val
        self.model.train(
            X_train, y_train,
            train_val_X, train_val_y,
            feature_names=feature_names
        )

        # 7.5 两阶段特征选择（针对小样本数据集）
        if use_feature_selection and feature_names and len(feature_names) > top_k_features:
            logger.info(
                f"Feature selection: {len(feature_names)} → {top_k_features} features"
            )

            # [新增] 预筛选：移除与目标变量相关性过低的特征
            # 这可以加速训练并减少噪声
            feature_target_corr = {}
            for i, feat in enumerate(feature_names):
                # 计算特征与目标的相关性
                feat_col = X_train[:, i]
                corr = np.abs(np.corrcoef(feat_col, y_train)[0, 1])
                if not np.isnan(corr):
                    feature_target_corr[feat] = corr

            # 过滤掉相关性 < 0.01 的特征（几乎无预测能力）
            # 但强制包含关键资金流向特征（只保留最重要的）
            corr_threshold = 0.01
            key_moneyflow_features = ['net_elg_amount', 'main_net_amount']

            valid_features = []
            for f in feature_names:
                # 关键资金流向特征强制包含（不管相关性）
                if f in key_moneyflow_features:
                    valid_features.append(f)
                # 其他特征按相关性筛选
                elif feature_target_corr.get(f, 0) >= corr_threshold:
                    valid_features.append(f)

            if len(valid_features) < len(feature_names):
                filtered_count = len(feature_names) - len(valid_features)
                logger.info(
                    f"Pre-filtered {filtered_count} low-correlation features "
                    f"(corr < {corr_threshold}), but kept all moneyflow features"
                )
                # 更新特征列表和矩阵
                valid_indices = [i for i, f in enumerate(feature_names) if f in valid_features]
                X_train = X_train[:, valid_indices]
                X_val = X_val[:, valid_indices]
                X_test = X_test[:, valid_indices]
                feature_names = valid_features

            initial_importance = self.model.get_feature_importance()

            # 强制包含牛熊体制交互特征（分类任务和回归任务都需要）
            # 分类任务：避免单纯均值回归偏差
            # 回归任务：识别熊市体制，避免在熊市中继续使用牛市逻辑
            mandatory = None
            excluded = None
            is_classification = 'direction' in target_type
            is_return_task = 'return' in target_type

            if is_classification or is_return_task:
                mandatory = [
                    f for f in ['sma20_below_sma60', 'bull_regime_uprate', 'bear_regime_momentum']
                    if f in feature_names
                ]

                # 对于回归任务，额外强制包含熊市关键特征
                # 这些特征在熊市中与收益负相关，但对整体数据重要性不够
                if is_return_task:
                    bear_market_keys = [
                        f for f in ['sma60_slope', 'up_rate_40d', 'momentum_3m']
                        if f in feature_names
                    ]
                    mandatory.extend(bear_market_keys)

                if mandatory:
                    # 对于分类任务，排除被体制交互特征替代的原始特征
                    # 对于回归任务，保留所有原始特征以提供更多信息
                    if is_classification:
                        excluded = [
                            f for f in ['up_rate_20d', 'up_rate_40d']
                            if f in feature_names and 'bull_regime_uprate' in mandatory
                        ]
                    logger.info(f"Mandatory features: {mandatory}, excluded (replaced): {excluded}")

            selected_features = engineer.select_top_features(
                X_train, feature_names, initial_importance,
                top_k=top_k_features, corr_threshold=0.85,
                mandatory_features=mandatory,
                excluded_features=excluded
            )

            if len(selected_features) >= 5:
                # 获取选中特征的索引
                feat_idx = [feature_names.index(f) for f in selected_features
                            if f in feature_names]

                X_train_sel = X_train[:, feat_idx]
                X_val_sel = X_val[:, feat_idx]
                X_test_sel = X_test[:, feat_idx]

                # 用精选特征重新训练
                model_id = f"lgb_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                self.model = LGBModel(
                    model_id=model_id, params=final_params, train_params=final_train_params
                )
                # 小样本模式不使用验证集
                sel_val_X = None if small_sample_mode else X_val_sel
                sel_val_y = None if small_sample_mode else y_val
                self.model.train(
                    X_train_sel, y_train,
                    sel_val_X, sel_val_y,
                    feature_names=selected_features
                )

                # 替换为精选特征的矩阵
                X_train, X_val, X_test = X_train_sel, X_val_sel, X_test_sel
                feature_names = selected_features
                logger.info(f"Retrained with {len(selected_features)} selected features")
                logger.info(f"Final features: {', '.join(selected_features)}")

        # 8. 评估模型
        train_metrics = self.model.evaluate(X_train, y_train)
        val_metrics = self.model.evaluate(X_val, y_val)
        test_metrics = self.model.evaluate(X_test, y_test)

        # 如果是分类目标，计算分类指标
        if 'direction' in target_type:
            from src.ml.evaluators.metrics import ModelEvaluator
            evaluator = ModelEvaluator()

            # 获取测试集的预测值
            test_predictions = self.model.predict(X_test)

            # 计算分类指标
            classification_metrics = evaluator.evaluate_classification(y_test, test_predictions)

            # 将分类指标添加到test_metrics中
            test_metrics.update({
                'accuracy': classification_metrics['accuracy'],
                'precision': classification_metrics['precision'],
                'recall': classification_metrics['recall'],
                'f1_score': classification_metrics['f1_score']
            })

            logger.info(f"Classification metrics - Accuracy: {classification_metrics['accuracy']:.4f}, "
                       f"F1: {classification_metrics['f1_score']:.4f}")
        else:
            # 回归任务：计算IC（信息系数）
            test_predictions = self.model.predict(X_test)
            ic_metrics = self.evaluator.calculate_information_coefficient(y_test, test_predictions)
            test_metrics.update({
                'ic_pearson': ic_metrics['pearson_ic'],
                'ic_spearman': ic_metrics['spearman_ic'],
                'ic_p_value': ic_metrics['pearson_p_value'],
            })
            logger.info(
                f"IC metrics - Pearson IC: {ic_metrics['pearson_ic']:.4f}, "
                f"Spearman IC: {ic_metrics['spearman_ic']:.4f}"
            )

        # 9. 保存模型（含训练目标均值，用于预测去偏）
        self.model.metadata['y_train_mean'] = y_train_mean
        self.model.metadata['y_train_std']  = y_train_std
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
            'feature_names': feature_names,
            'train_metrics': train_metrics,
            'val_metrics': val_metrics,
            'test_metrics': test_metrics,
            'feature_importance': self.model.get_feature_importance(),
            'model_path': str(model_path),
            'y_train_mean': y_train_mean,
            'trained_at': datetime.now().isoformat()
        }

        logger.info(f"Training complete. Test MAE: {test_metrics.get('mae', 'N/A')}")

        return result

    def _get_lgb_params(
        self,
        target_type: str,
        y_train: np.ndarray,
        small_sample: bool = False
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        根据目标类型和训练数据返回 LightGBM 参数

        Returns:
            (model_params, train_params)
        """
        if 'direction' in target_type:
            unique, counts = np.unique(y_train, return_counts=True)
            if len(unique) == 2:
                neg_count = counts[0] if unique[0] == 0 else counts[1]
                pos_count = counts[1] if unique[1] == 1 else counts[0]
                scale_pos_weight = neg_count / pos_count if pos_count > 0 else 1.0
            else:
                scale_pos_weight = 1.0

            params = {
                'objective': 'binary',
                'metric': ['binary_logloss', 'auc'],
                'boosting_type': 'gbdt',
                'num_leaves': 10,
                'max_depth': 4,
                'learning_rate': 0.01,
                'feature_fraction': 0.75,
                'bagging_fraction': 0.7,
                'bagging_freq': 3,
                'min_child_samples': 50,
                'min_child_weight': 0.01,
                'reg_alpha': 0.5,
                'reg_lambda': 2.0,
                'scale_pos_weight': scale_pos_weight,
                'verbosity': -1,
                'random_state': 42,
            }
            train_params = {
                'num_boost_round': 300 if small_sample else 500,
                'early_stopping_rounds': None if small_sample else 50,
                'verbose_eval': 100,
            }
        else:
            params = {
                'objective': 'regression',
                'metric': ['mae', 'rmse'],
                'boosting_type': 'gbdt',
                'num_leaves': 15,
                'max_depth': 5,
                'learning_rate': 0.01,
                'feature_fraction': 0.8,
                'bagging_fraction': 0.7,
                'bagging_freq': 3,
                'min_child_samples': 40,
                'min_child_weight': 0.01,
                'reg_alpha': 0.1,
                'reg_lambda': 1.0,
                'verbosity': -1,
                'random_state': 42,
                'deterministic': True,
            }
            train_params = {
                'num_boost_round': 300 if small_sample else 500,
                'early_stopping_rounds': None if small_sample else 50,
                'verbose_eval': 100,
            }

        params.update(self.params)
        return params, train_params

    def train_walk_forward(
        self,
        df: pd.DataFrame,
        target_type: str = "return_1d",
        symbol: str = "unknown",
        n_splits: int = 5,
        add_technical: bool = True,
        use_feature_selection: bool = True,
        top_k_features: int = 30,
    ) -> Dict[str, Any]:
        """
        Walk-Forward 滚动窗口训练（Expanding Window）

        策略：
        - 特征工程做一次，时间序列切分成 n_splits 个测试窗口
        - 每折使用截至当前时间点的全部历史数据训练，下一个窗口测试
        - Walk-forward 仅用于评估泛化能力，最终模型在全量数据上训练

        Args:
            df:              原始数据
            target_type:     目标类型（return_1d/direction_1d 等）
            symbol:          股票代码
            n_splits:        折数（默认 5）
            add_technical:   是否添加技术指标
            use_feature_selection: 是否做特征选择
            top_k_features:  保留特征数

        Returns:
            包含各折指标、CV 均值/标准差、最终模型信息的字典
        """
        logger.info(f"Walk-forward training: symbol={symbol}, target={target_type}, n_splits={n_splits}")

        # ── 1. 特征工程（全量数据做一次） ──────────────────────────────────
        engineer = FeatureEngineer(scaler_type='standard')
        if add_technical:
            df = engineer.add_technical_indicators(df)
            df = engineer.add_cross_sectional_features(df)
            df = engineer.add_essential_features(df)
        df = engineer.create_target(df, target_type=target_type)

        X_all, y_all = engineer.prepare_training_data(df, handle_missing='drop')
        all_feature_names = list(engineer.feature_cols)

        # 提取日期列（用于 fold 结果的可读性）
        date_col = None
        for col in ['datetime', 'date', 'trade_date']:
            if col in df.columns:
                date_col = col
                break

        df_clean = df.dropna(subset=['target'])
        df_clean = df_clean.reset_index(drop=True)

        n = len(X_all)
        logger.info(f"Walk-forward: total samples={n}, features={len(all_feature_names)}")

        # ── 2. 计算折叠边界 ────────────────────────────────────────────────
        # 前 50% 作为最小初始训练集，后 50% 分成 n_splits 个测试窗口
        min_train = int(n * 0.5)
        available_for_cv = n - min_train
        test_window = max(10, available_for_cv // n_splits)
        val_ratio = 0.15

        actual_splits = min(n_splits, available_for_cv // max(10, test_window))
        if actual_splits < 2:
            raise ValueError(
                f"数据量不足以进行 {n_splits} 折 walk-forward 验证（总样本={n}）。"
                "请增加数据量或减少折数。"
            )
        logger.info(
            f"Walk-forward config: min_train={min_train}, test_window={test_window}, "
            f"actual_splits={actual_splits}"
        )

        # ── 3. 第 0 折：做一次特征选择，后续折复用 ─────────────────────────
        selected_features = all_feature_names  # 默认用全部

        if use_feature_selection and len(all_feature_names) > top_k_features:
            fold0_full_train_end = min_train
            fold0_val_size = max(5, int(fold0_full_train_end * val_ratio))
            fold0_train_end = fold0_full_train_end - fold0_val_size

            from sklearn.preprocessing import StandardScaler as _SS
            _sc0 = _SS()
            X0_train = _sc0.fit_transform(X_all[:fold0_train_end])

            # 先训练一个粗模型用于重要性排名
            y0_train = y_all[:fold0_train_end]
            small0 = len(y0_train) < 300
            p0, tp0 = self._get_lgb_params(target_type, y0_train, small_sample=small0)
            _m0 = LGBModel(model_id='_sel_tmp', params=p0, train_params=tp0)
            _m0.train(X0_train, y0_train, feature_names=all_feature_names)

            imp0 = _m0.get_feature_importance()
            mandatory = None
            excluded = None
            is_classification = 'direction' in target_type
            is_return_task = 'return' in target_type

            # 强制包含关键特征（无论重要性高低）
            if is_classification or is_return_task:
                mandatory = []

                # 1. 牛熊体制交互特征
                regime_features = [f for f in ['sma20_below_sma60', 'bull_regime_uprate', 'bear_regime_momentum']
                                   if f in all_feature_names]
                mandatory.extend(regime_features)

                # 2. 对于回归任务，额外强制包含熊市关键特征
                if is_return_task:
                    bear_market_keys = [f for f in ['sma60_slope', 'up_rate_40d', 'momentum_3m']
                                        if f in all_feature_names]
                    mandatory.extend(bear_market_keys)

                # 3. 资金流向特征（只保留最关键的：特大单净流入和主力净流入）
                moneyflow_features = ['net_elg_amount', 'main_net_amount']
                mandatory.extend([f for f in moneyflow_features if f in all_feature_names])

                if mandatory:
                    logger.info(f"Mandatory features: {len(mandatory)} (regime, bear market, moneyflow)")

                if mandatory and is_classification:
                    # 对于分类任务，排除被体制交互特征替代的原始特征
                    excluded = [f for f in ['up_rate_20d', 'up_rate_40d']
                                if f in all_feature_names and 'bull_regime_uprate' in mandatory]

            selected_features = engineer.select_top_features(
                X0_train, all_feature_names, imp0,
                top_k=top_k_features, corr_threshold=0.85,
                mandatory_features=mandatory,
                excluded_features=excluded,
            )
            logger.info(f"Feature selection (fold 0): {len(all_feature_names)} → {len(selected_features)}")

        # 选中特征的列索引
        feat_idx = [all_feature_names.index(f) for f in selected_features if f in all_feature_names]
        X_sel = X_all[:, feat_idx]

        # ── 4. Walk-forward 循环 ───────────────────────────────────────────
        fold_results = []
        is_classification = 'direction' in target_type

        for fold in range(actual_splits):
            test_start = min_train + fold * test_window
            test_end = min(test_start + test_window, n)
            if test_start >= n:
                break

            full_train_end = test_start
            val_size = max(5, int(full_train_end * val_ratio))
            train_end = full_train_end - val_size

            if train_end < 20:
                logger.warning(f"Fold {fold}: not enough training data ({train_end}), skipping")
                continue

            # 各折独立标准化（fit on train only）
            from sklearn.preprocessing import StandardScaler as _SS2
            fold_scaler = _SS2()
            X_tr = fold_scaler.fit_transform(X_sel[:train_end])
            X_va = fold_scaler.transform(X_sel[train_end:full_train_end])
            X_te = fold_scaler.transform(X_sel[test_start:test_end])
            y_tr = y_all[:train_end]
            y_va = y_all[train_end:full_train_end]
            y_te = y_all[test_start:test_end]

            small = len(y_tr) < 300
            fold_params, fold_train_params = self._get_lgb_params(target_type, y_tr, small_sample=small)

            fold_model_id = f"lgb_{symbol}_wf_fold{fold}_{datetime.now().strftime('%H%M%S')}"
            fold_lgb = LGBModel(model_id=fold_model_id, params=fold_params, train_params=fold_train_params)

            # 小样本不使用验证集
            _va_X = None if small else X_va
            _va_y = None if small else y_va
            fold_lgb.train(X_tr, y_tr, _va_X, _va_y, feature_names=selected_features)
            fold_metrics = fold_lgb.evaluate(X_te, y_te)

            # 分类任务补充准确率
            if is_classification:
                from src.ml.evaluators.metrics import ModelEvaluator as _ME
                _ev = _ME()
                preds = fold_lgb.predict(X_te)
                cls_m = _ev.evaluate_classification(y_te, preds)
                fold_metrics.update({
                    'accuracy': cls_m['accuracy'],
                    'f1_score': cls_m['f1_score'],
                })
            else:
                preds = fold_lgb.predict(X_te)
                ic = self.evaluator.calculate_information_coefficient(y_te, preds)
                fold_metrics['ic_pearson'] = ic['pearson_ic']

            # 获取该折测试期日期范围
            test_date_range = None
            if date_col and date_col in df_clean.columns and test_end <= len(df_clean):
                d_start = df_clean[date_col].iloc[test_start]
                d_end   = df_clean[date_col].iloc[min(test_end - 1, len(df_clean) - 1)]
                if isinstance(d_start, pd.Timestamp):
                    test_date_range = (d_start.strftime('%Y-%m-%d'), d_end.strftime('%Y-%m-%d'))

            fold_results.append({
                'fold':           fold + 1,
                'train_samples':  train_end,
                'val_samples':    full_train_end - train_end,
                'test_samples':   test_end - test_start,
                'test_date_range': test_date_range,
                'metrics':        fold_metrics,
            })
            logger.info(
                f"Fold {fold + 1}/{actual_splits}: train={train_end}, test={test_end - test_start}, "
                f"{'accuracy=' + str(round(fold_metrics.get('accuracy', 0), 4)) if is_classification else 'mae=' + str(round(fold_metrics.get('mae', 0), 4))}"
            )

        if not fold_results:
            raise RuntimeError("Walk-forward 未能完成任何折叠训练，请检查数据量")

        # ── 5. CV 汇总统计 ──────────────────────────────────────────────────
        cv_metric = 'accuracy' if is_classification else 'mae'
        cv_scores = [f['metrics'].get(cv_metric, 0) for f in fold_results]
        cv_mean = float(np.mean(cv_scores))
        cv_std  = float(np.std(cv_scores))
        logger.info(
            f"Walk-forward CV {cv_metric}: {cv_mean:.4f} ± {cv_std:.4f} "
            f"({actual_splits} folds)"
        )

        # ── 6. 最终模型：在全量数据上训练 ─────────────────────────────────
        final_val_size = max(5, int(n * val_ratio))
        final_train_end = n - final_val_size

        from sklearn.preprocessing import StandardScaler as _SS3
        final_scaler = _SS3()
        X_final_train = final_scaler.fit_transform(X_sel[:final_train_end])
        X_final_val   = final_scaler.transform(X_sel[final_train_end:])
        y_final_train = y_all[:final_train_end]
        y_final_val   = y_all[final_train_end:]

        y_train_mean = float(y_final_train.mean())
        y_train_std  = float(y_final_train.std())

        small_final = len(y_final_train) < 300
        final_params, final_train_params = self._get_lgb_params(
            target_type, y_final_train, small_sample=small_final
        )

        model_id = f"lgb_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.model = LGBModel(model_id=model_id, params=final_params, train_params=final_train_params)
        _fva_X = None if small_final else X_final_val
        _fva_y = None if small_final else y_final_val
        self.model.train(
            X_final_train, y_final_train, _fva_X, _fva_y,
            feature_names=selected_features
        )

        # 用最后一折的测试指标作为 test_metrics
        test_metrics = fold_results[-1]['metrics']
        train_metrics = self.model.evaluate(X_final_train, y_final_train)

        self.model.metadata['y_train_mean'] = y_train_mean
        self.model.metadata['y_train_std']  = y_train_std
        model_path = self.model_save_dir / model_id
        self.model.save(str(model_path))

        logger.info(f"Walk-forward final model saved: {model_id}")

        return {
            'model_id':            model_id,
            'symbol':              symbol,
            'target_type':         target_type,
            'walk_forward':        True,
            'n_splits':            len(fold_results),
            'cv_score':            cv_mean,
            'cv_std':              cv_std,
            'cv_metric':           cv_metric,
            'fold_results':        fold_results,
            'train_samples':       final_train_end,
            'val_samples':         final_val_size,
            'test_samples':        fold_results[-1]['test_samples'],
            'n_features':          len(selected_features),
            'feature_names':       selected_features,
            'train_metrics':       train_metrics,
            'val_metrics':         {},
            'test_metrics':        test_metrics,
            'feature_importance':  self.model.get_feature_importance(),
            'model_path':          str(model_path),
            'y_train_mean':        y_train_mean,
            'trained_at':          datetime.now().isoformat(),
        }


    def optimize_hyperparams(
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
