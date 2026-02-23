"""
ML Model Routes for Flask

Provides API endpoints for machine learning model training, prediction,
and management.
"""
from flask import Blueprint, request, jsonify, Response
import logging
import threading
import traceback
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd
from pathlib import Path
import json

from config.settings import TUSHARE_DB_PATH
from src.ml.data_loader import MlDataLoader
from src.ml.preprocessor import FeatureEngineer
from src.ml.trainers.lgb_trainer import LGBTrainer
from src.ml.models.lgb_model import LGBModel
from src.ml.utils import MLDatabase
from web.tasks import init_task_manager

logger = logging.getLogger(__name__)

# 辅助函数：确保所有数据都是JSON可序列化的
def make_json_serializable(obj):
    """递归转换numpy类型为Python原生类型"""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_serializable(item) for item in obj]
    elif pd.isna(obj) or obj is None or (isinstance(obj, float) and np.isnan(obj)):
        return None
    return obj

# 创建 ML Blueprint
ml_bp = Blueprint('ml_routes', __name__)

# 全局变量
_trainer = None
_ml_db = None
_ml_lock = threading.Lock()


def get_trainer():
    """获取ML训练器实例"""
    global _trainer
    with _ml_lock:
        if _trainer is None:
            _trainer = LGBTrainer(model_save_dir="data/ml_models")
        return _trainer


def get_ml_db():
    """获取ML数据库实例"""
    global _ml_db
    with _ml_lock:
        if _ml_db is None:
            _ml_db = MLDatabase(db_path=str(TUSHARE_DB_PATH))
        return _ml_db


def run_training_task(task_id, params):
    """
    在后台线程运行模型训练任务

    Args:
        task_id: 任务ID
        params: 训练参数
    """
    task_manager = init_task_manager()

    try:
        # 更新任务状态
        task_manager.update_task(task_id, status='running', message='开始训练模型...')

        # 获取参数
        symbol = params.get('symbol', '600382')
        start_date = params.get('start_date', '2020-01-01')
        end_date = params.get('end_date')
        target_type = params.get('target_type', 'return_1d')
        model_type = params.get('model_type', 'lgb')
        add_technical = params.get('add_technical', True)
        train_ratio = params.get('train_ratio', 0.7)
        val_ratio = params.get('val_ratio', 0.15)
        use_feature_selection = params.get('use_feature_selection', True)
        top_k_features = params.get('top_k_features', 30)  # 默认30，包含多空胜率+趋势方向特征
        walk_forward = params.get('walk_forward', True)  # 默认开启滚动窗口
        n_splits = params.get('n_splits', 5)

        logger.info(f"Training params: symbol={symbol}, target_type={target_type}, model_type={model_type}")

        # 获取结束日期
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')

        # 加载数据
        task_manager.update_task(task_id, progress=10, message='正在加载数据...')

        data_loader = MlDataLoader(db_path=str(TUSHARE_DB_PATH))
        df = data_loader.load_training_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            include_financial=True
        )

        if df.empty:
            raise ValueError(f"无法加载 {symbol} 的数据")

        task_manager.update_task(task_id, progress=20, message=f'已加载 {len(df)} 条数据，正在训练模型...')

        # 训练模型
        trainer = get_trainer()
        if walk_forward:
            result = trainer.train_walk_forward(
                df=df,
                target_type=target_type,
                symbol=symbol,
                n_splits=n_splits,
                add_technical=add_technical,
                use_feature_selection=use_feature_selection,
                top_k_features=top_k_features,
            )
        else:
            result = trainer.train(
                df=df,
                target_type=target_type,
                symbol=symbol,
                add_technical=add_technical,
                train_ratio=train_ratio,
                val_ratio=val_ratio,
                use_feature_selection=use_feature_selection,
                top_k_features=top_k_features
            )

        # 保存到数据库
        ml_db = get_ml_db()
        ml_db.save_model_metadata(
            model_id=result['model_id'],
            model_type=model_type,
            symbol=symbol,
            hyperparameters=trainer.params,
            metrics=result['test_metrics'],
            feature_importance=result['feature_importance'],
            model_path=result['model_path'],
            training_start=start_date,
            training_end=end_date,
            n_features=result['n_features'],
            train_samples=result['train_samples'],
            val_samples=result['val_samples'],
            test_samples=result['test_samples'],
            target_type=target_type,
            metadata={
                'trained_by': 'api',
                'walk_forward': result.get('walk_forward', False),
                'n_splits': result.get('n_splits'),
                'cv_score': result.get('cv_score'),
                'cv_std': result.get('cv_std'),
                'cv_metric': result.get('cv_metric'),
            }
        )

        # 更新任务状态
        ic_info = ""
        if 'ic_pearson' in result.get('test_metrics', {}):
            ic_val = result['test_metrics']['ic_pearson']
            ic_info = f", IC={ic_val:.4f}"
        dir_acc = result['test_metrics'].get('direction_accuracy') or result['test_metrics'].get('accuracy')
        dir_info = f", 方向准确率={dir_acc:.2%}" if dir_acc is not None else ""
        mae_val = result['test_metrics'].get('mae')
        mae_info = f"{mae_val:.6f}" if mae_val is not None else "N/A"

        if result.get('walk_forward'):
            cv_metric = result.get('cv_metric', 'score')
            cv_mean = result.get('cv_score', 0)
            cv_std = result.get('cv_std', 0)
            n_sp = result.get('n_splits', 0)
            wf_info = f"（滚动窗口 {n_sp} 折，CV {cv_metric}={cv_mean:.4f}±{cv_std:.4f}）"
        else:
            wf_info = f"（使用{result['n_features']}个特征）"

        task_manager.update_task(
            task_id,
            status='completed',
            progress=100,
            message=f'训练完成！测试集 MAE: {mae_info}{ic_info}{dir_info}{wf_info}',
            result=result
        )

        logger.info(f"Training task {task_id} completed successfully")

    except Exception as e:
        error_msg = f"训练失败: {str(e)}"
        logger.error(f"Training task {task_id} failed: {error_msg}\n{traceback.format_exc()}")

        task_manager.update_task(
            task_id,
            status='failed',
            message=error_msg,
            error=error_msg
        )


@ml_bp.route('/api/ml/train', methods=['POST'])
def api_train_model():
    """
    训练ML模型 API

    Body (JSON):
        symbol: 股票代码
        start_date: 开始日期
        end_date: 结束日期 (可选)
        target_type: 目标类型 ('return_1d', 'direction_1d')
        model_type: 模型类型 ('lgb')
        add_technical: 是否添加技术指标 (true/false)
    """
    try:
        data = request.json
        if not data:
            return jsonify({'error': '请求数据为空'}), 400

        symbol = data.get('symbol')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        target_type = data.get('target_type', 'return_1d')
        model_type = data.get('model_type', 'lgb')
        add_technical = data.get('add_technical', True)

        logger.info(f"API received: symbol={symbol}, target_type={target_type}, full_data={data}")

        if not symbol:
            return jsonify({'error': '请指定股票代码'}), 400

        if not start_date:
            return jsonify({'error': '请指定开始日期'}), 400

        # 创建任务
        task_manager = init_task_manager()

        try:
            task_id = task_manager.create_task(
                'ml_train',
                {
                    'symbol': symbol,
                    'start_date': start_date,
                    'end_date': end_date,
                    'target_type': target_type,
                    'model_type': model_type,
                    'add_technical': add_technical
                }
            )
        except Exception as e:
            return jsonify({'error': f'创建任务失败: {str(e)}'}), 400

        # 在后台线程运行训练
        thread = threading.Thread(
            target=run_training_task,
            args=(task_id, {
                'symbol': symbol,
                'start_date': start_date,
                'end_date': end_date,
                'target_type': target_type,
                'model_type': model_type,
                'add_technical': add_technical
            })
        )
        thread.daemon = True
        thread.start()

        return jsonify({
            'success': True,
            'task_id': task_id,
            'message': '训练任务已创建，正在后台执行...'
        })

    except Exception as e:
        logger.error(f"Error in api_train_model: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'error': f'请求失败: {str(e)}'}), 500


@ml_bp.route('/api/ml/models')
def api_list_models():
    """
    列出所有训练好的模型

    Query params:
        symbol: 股票代码过滤 (可选)
        model_type: 模型类型过滤 (可选)
        limit: 返回数量限制 (默认50)
    """
    try:
        ml_db = get_ml_db()

        symbol = request.args.get('symbol')
        model_type = request.args.get('model_type')
        limit = int(request.args.get('limit', 50))

        models = ml_db.list_models(
            symbol=symbol,
            model_type=model_type,
            limit=limit
        )

        return jsonify({
            'success': True,
            'models': models,
            'count': len(models)
        })

    except Exception as e:
        logger.error(f"Error listing models: {str(e)}")
        return jsonify({'error': f'查询失败: {str(e)}'}), 500


@ml_bp.route('/api/ml/models/<model_id>')
def api_get_model(model_id):
    """
    获取模型详细信息

    Path params:
        model_id: 模型ID
    """
    try:
        ml_db = get_ml_db()

        model = ml_db.get_model(model_id)

        if not model:
            return jsonify({'error': '模型不存在'}), 404

        return jsonify({
            'success': True,
            'model': model
        })

    except Exception as e:
        logger.error(f"Error getting model: {str(e)}")
        return jsonify({'error': f'查询失败: {str(e)}'}), 500


@ml_bp.route('/api/ml/predict', methods=['POST'])
def api_predict():
    """
    生成预测

    Body (JSON):
        model_id: 模型ID
        symbol: 股票代码
        date: 预测日期 (可选，默认最新)
        days: 返回历史预测天数 (可选，用于绘制对比图，默认0只返回最新预测)
    """
    try:
        data = request.json
        if not data:
            return jsonify({'error': '请求数据为空'}), 400

        model_id = data.get('model_id')
        symbol = data.get('symbol')
        history_days = data.get('days', 0)  # 新增：返回历史预测天数

        if not model_id:
            return jsonify({'error': '请指定模型ID'}), 400

        # 加载模型
        trainer = get_trainer()
        model = trainer.load_model(model_id)

        # 获取模型训练时使用的特征名称
        if model.feature_names is None:
            return jsonify({'error': '模型没有保存特征名称，无法进行预测'}), 400

        feature_names = model.feature_names
        logger.info(f"Model expects {len(feature_names)} features")

        # 加载最新数据
        data_loader = MlDataLoader(db_path=str(TUSHARE_DB_PATH))

        # 获取最新的日期范围 - 需要足够的数据来计算技术指标
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = datetime.now().replace(year=datetime.now().year - 2).strftime('%Y-%m-%d')

        df = data_loader.load_training_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )

        if df.empty:
            return jsonify({'error': '无法加载股票数据'}), 400

        # 准备特征 - 使用与训练时相同的特征工程流程
        engineer = FeatureEngineer(scaler_type='standard')

        # 添加技术指标
        df = engineer.add_technical_indicators(df)

        # 添加横截面特征
        df = engineer.add_cross_sectional_features(df)

        # 添加精选核心特征（与训练时保持一致）
        df = engineer.add_essential_features(df)

        # 创建目标（用于删除最后一行没有目标值的数据）
        # 注意：需要先获取模型的 target_type，然后创建对应的目标
        ml_db = get_ml_db()
        model_info = ml_db.get_model(model_id)
        model_target_type = model_info.get('target_type', 'return_1d') if model_info else 'return_1d'

        logger.info(f"Model target_type: {model_target_type}")

        df = engineer.create_target(df, target_type=model_target_type)

        # 只保留模型需要的特征列
        available_features = [f for f in feature_names if f in df.columns]

        if len(available_features) != len(feature_names):
            missing_features = set(feature_names) - set(available_features)
            logger.warning(f"Missing {len(missing_features)} features: {missing_features}")

        if not available_features:
            return jsonify({'error': f'没有可用的特征列。模型需要: {feature_names[:10]}...'}), 400

        # 确保所有特征都是数值型
        for col in available_features:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # 保留日期信息用于历史预测
        date_col = None
        if 'datetime' in df.columns:
            date_col = 'datetime'
        elif 'date' in df.columns:
            date_col = 'date'
        elif isinstance(df.index, pd.DatetimeIndex):
            # 索引是日期，创建一个日期列
            df = df.reset_index()
            date_col = df.columns[0]  # 通常是 'index' 或 'datetime'

        # 删除包含 NaN 的行，保留日期列
        cols_to_keep = available_features + ['target']
        if date_col:
            cols_to_keep.append(date_col)
        df_clean = df[cols_to_keep].dropna()

        if len(df_clean) == 0:
            return jsonify({'error': '清洗后无有效数据'}), 400

        # 提取特征 - 确保列顺序与训练时一致
        X = df_clean[available_features].values

        logger.info(f"Prepared prediction data: X shape={X.shape}, features={len(available_features)}, date_col={date_col}")

        # 获取最后一行作为预测输入
        X_latest = X[-1:, :]

        # 标准化
        X_scaled = engineer.fit_transform(X)
        X_latest_scaled = X_scaled[-1:, :]

        # 生成预测
        try:
            # 确定是否为分类任务（用于后续去偏处理）
            is_classification = 'direction' in model_target_type

            prediction = model.predict(X_latest_scaled)[0]

            # 对回归任务进行预测去偏处理
            # 模型训练时保存了 y_train_mean，预测时需要减去这个均值
            # 因为模型在训练数据上学习的是围绕均值的偏差
            y_train_mean = model.metadata.get('y_train_mean', 0.0)
            if not is_classification and y_train_mean != 0:
                original_prediction = prediction
                prediction = prediction - y_train_mean
                logger.info(
                    f"Prediction debias: {original_prediction:.6f} - {y_train_mean:.6f} = {prediction:.6f}"
                )

            # 检查预测值是否有效
            if not np.isfinite(prediction):
                return jsonify({'error': f'预测结果无效: {prediction}'}), 500

            # 生成带置信区间的预测
            try:
                pred_with_conf, confidence = model.predict_with_confidence(X_latest_scaled)
                # 对回归任务的置信区间也需要去偏
                if not is_classification and y_train_mean != 0:
                    conf_lower = float(confidence[0][0]) - y_train_mean if len(confidence) > 0 else prediction - 0.01
                    conf_upper = float(confidence[0][1]) - y_train_mean if len(confidence) > 0 else prediction + 0.01
                else:
                    conf_lower = float(confidence[0][0]) if len(confidence) > 0 else prediction - 0.01
                    conf_upper = float(confidence[0][1]) if len(confidence) > 0 else prediction + 0.01

                # 确保置信区间有效
                if not np.isfinite(conf_lower):
                    conf_lower = prediction - 0.01
                if not np.isfinite(conf_upper):
                    conf_upper = prediction + 0.01

                interval_width = abs(conf_upper - conf_lower)
                logger.info(f"Prediction confidence interval: [{conf_lower:.6f}, {conf_upper:.6f}], width={interval_width:.6f}")
            except Exception as conf_err:
                logger.warning(f"Could not generate confidence interval: {conf_err}")
                # 使用默认置信区间
                conf_lower = prediction - 0.01
                conf_upper = prediction + 0.01

            # 记录模型信息用于调试（使用前面获取的 model_target_type）
            logger.info(f"Model {model_id}: target_type={model_target_type}, is_classification={is_classification}, prediction={prediction:.6f}")

            # 构建响应
            response = {
                'success': True,
                'prediction': float(prediction),
                'confidence_lower': float(conf_lower),
                'confidence_upper': float(conf_upper),
                'symbol': symbol,
                'model_id': model_id,
                'date': str(end_date),
                'target_type': model_target_type
            }

            # 转换所有numpy类型
            response = make_json_serializable(response)

            # 如果请求了历史预测数据
            if history_days > 0:
                history_predictions = []

                # 确定起始索引：从训练结束日期之后开始
                training_end_date = model_info.get('training_end') if model_info else None
                post_train_start_idx = 0

                if training_end_date and date_col and date_col in df_clean.columns:
                    training_end_dt = pd.to_datetime(training_end_date)
                    after_mask = df_clean[date_col] > training_end_dt
                    if after_mask.any():
                        post_train_start_idx = int(after_mask.values.argmax())
                    else:
                        # 没有训练后数据，回退到最近 history_days 天
                        post_train_start_idx = max(0, len(X_scaled) - history_days)
                    logger.info(f"Post-training start index: {post_train_start_idx} (training_end={training_end_date})")
                else:
                    # 无日期信息，回退到最近 history_days 个点
                    post_train_start_idx = max(0, len(X_scaled) - history_days)

                # 最多取 history_days 个点，且不包含最后一行（已用于当前预测）
                start_idx = post_train_start_idx
                end_idx = len(X_scaled)  # 包含最后一行（当天）

                logger.info(f"Generating historical predictions from index {start_idx} to {end_idx}, date_col={date_col}")

                for i in range(start_idx, end_idx):
                    hist_pred = model.predict(X_scaled[i:i+1])[0]

                    # 对回归任务进行预测去偏处理
                    if not is_classification and y_train_mean != 0:
                        hist_pred = hist_pred - y_train_mean

                    # 获取实际值和日期
                    hist_actual = None
                    hist_date = end_date  # 默认使用当前日期

                    if i < len(df_clean):
                        actual_val = df_clean['target'].iloc[i]
                        if pd.notna(actual_val) and np.isfinite(actual_val):
                            hist_actual = float(actual_val)

                        # 确定日期偏移：
                        # - 1日模型：target[i] 在 day i+1 实现，显示 i+1 的日期
                        # - 5日模型：target[i] 跨越 i 到 i+5，显示特征日期 i
                        is_5d = '5d' in model_target_type
                        date_offset_i = i if is_5d else (i + 1)

                        if date_col and date_col in df_clean.columns:
                            if date_offset_i < len(df_clean):
                                date_val = df_clean[date_col].iloc[date_offset_i]
                            else:
                                date_val = df_clean[date_col].iloc[i]
                            if isinstance(date_val, pd.Timestamp):
                                hist_date = date_val.strftime('%Y-%m-%d')
                            else:
                                hist_date = str(date_val)
                        elif date_offset_i < len(df_clean) and isinstance(df_clean.index[date_offset_i], pd.Timestamp):
                            hist_date = df_clean.index[date_offset_i].strftime('%Y-%m-%d')
                        elif isinstance(df_clean.index[i], pd.Timestamp):
                            hist_date = df_clean.index[i].strftime('%Y-%m-%d')

                    history_predictions.append({
                        'date': hist_date,
                        'prediction': float(hist_pred),
                        'actual': hist_actual
                    })

                # 统计分析
                if history_predictions:
                    preds = [h['prediction'] for h in history_predictions]
                    actuals = [h['actual'] for h in history_predictions if h['actual'] is not None]
                    up_count = sum(1 for p in preds if p > (0.5 if 'direction' in model_target_type else 0))
                    down_count = len(preds) - up_count

                    # 计算IC和方向准确率（有实际值时）
                    ic_pearson = None
                    ic_spearman = None
                    direction_accuracy = None
                    if len(actuals) >= 5:
                        try:
                            from src.ml.evaluators.metrics import ModelEvaluator
                            _evaluator = ModelEvaluator()
                            pred_arr = np.array([h['prediction'] for h in history_predictions
                                                 if h['actual'] is not None])
                            actual_arr = np.array(actuals)
                            ic_metrics = _evaluator.calculate_information_coefficient(actual_arr, pred_arr)
                            ic_pearson = round(float(ic_metrics.get('pearson_ic', 0)), 4)
                            ic_spearman = round(float(ic_metrics.get('spearman_ic', 0)), 4)
                            stock_metrics = _evaluator.evaluate_stock_prediction(actual_arr, pred_arr)
                            direction_accuracy = round(float(stock_metrics.get('direction_accuracy', 0)), 4)
                        except Exception as metric_err:
                            logger.warning(f"Could not compute IC: {metric_err}")

                    response['history_stats'] = {
                        'pred_up': up_count,
                        'pred_down': down_count,
                        'total': len(preds),
                        'actual_count': len(actuals),
                        'ic_pearson': ic_pearson,
                        'ic_spearman': ic_spearman,
                        'direction_accuracy': direction_accuracy,
                    }

                    logger.info(f"History stats: pred_up={up_count}/{len(preds)}, "
                               f"IC={ic_pearson}, dir_acc={direction_accuracy}")

                response['history'] = history_predictions
                logger.info(f"Generated {len(history_predictions)} historical predictions, first date: {history_predictions[0]['date'] if history_predictions else 'N/A'}")

            return jsonify(response)
        except Exception as pred_err:
            logger.error(f"Prediction failed: {pred_err}")
            return jsonify({'error': f'预测失败: {str(pred_err)}'}), 500

    except Exception as e:
        logger.error(f"Error in prediction: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'error': f'预测失败: {str(e)}'}), 500


@ml_bp.route('/api/ml/models/<model_id>', methods=['DELETE'])
def api_delete_model(model_id):
    """
    删除模型

    Path params:
        model_id: 模型ID
    """
    try:
        ml_db = get_ml_db()

        success = ml_db.delete_model(model_id)

        if success:
            # 删除文件系统中的模型文件
            trainer = get_trainer()
            trainer.delete_model(model_id)

            return jsonify({
                'success': True,
                'message': f'模型 {model_id} 已删除'
            })
        else:
            return jsonify({'error': '删除失败'}), 500

    except Exception as e:
        logger.error(f"Error deleting model: {str(e)}")
        return jsonify({'error': f'删除失败: {str(e)}'}), 500


@ml_bp.route('/api/ml/models/<model_id>/predictions')
def api_get_model_predictions(model_id):
    """
    获取模型的预测记录

    Path params:
        model_id: 模型ID

    Query params:
        start_date: 开始日期 (可选)
        end_date: 结束日期 (可选)
    """
    try:
        ml_db = get_ml_db()

        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        predictions = ml_db.get_predictions(
            model_id=model_id,
            start_date=start_date,
            end_date=end_date
        )

        return jsonify({
            'success': True,
            'predictions': predictions,
            'count': len(predictions)
        })

    except Exception as e:
        logger.error(f"Error getting predictions: {str(e)}")
        return jsonify({'error': f'查询失败: {str(e)}'}), 500


@ml_bp.route('/api/ml/models/<model_id>/performance')
def api_get_model_performance(model_id):
    """
    获取模型性能记录

    Path params:
        model_id: 模型ID
    """
    try:
        ml_db = get_ml_db()

        performance = ml_db.get_model_performance(model_id)

        return jsonify({
            'success': True,
            'performance': performance,
            'count': len(performance)
        })

    except Exception as e:
        logger.error(f"Error getting performance: {str(e)}")
        return jsonify({'error': f'查询失败: {str(e)}'}), 500


# ==================== Quarterly Model Routes ====================

def register_ml_routes(app):
    """
    注册ML路由到Flask应用

    Args:
        app: Flask应用实例
    """
    app.register_blueprint(ml_bp)
