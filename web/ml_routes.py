"""
ML Model Routes for Flask

Provides API endpoints for machine learning model training, prediction,
and management.
"""
from flask import Blueprint, request, jsonify
import logging
import threading
import traceback
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd

from config.settings import TUSHARE_DB_PATH
from src.ml.data_loader import MlDataLoader
from src.ml.preprocessor import FeatureEngineer
from src.ml.trainers.lgb_trainer import LGBTrainer
from src.ml.models.lgb_model import LGBModel
from src.ml.utils import MLDatabase
from web.tasks import init_task_manager

logger = logging.getLogger(__name__)

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
        result = trainer.train(
            df=df,
            target_type=target_type,
            symbol=symbol,
            add_technical=add_technical
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
            metadata={'trained_by': 'api'}
        )

        # 更新任务状态
        task_manager.update_task(
            task_id,
            status='completed',
            progress=100,
            message=f'训练完成！测试集 MAE: {result["test_metrics"].get("mae", "N/A"):.6f}',
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
    """
    try:
        data = request.json
        if not data:
            return jsonify({'error': '请求数据为空'}), 400

        model_id = data.get('model_id')
        symbol = data.get('symbol')

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

        # 创建目标（用于删除最后一行没有目标值的数据）
        df = engineer.create_target(df, target_type='return_1d')

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

        # 删除包含 NaN 的行
        df_clean = df[available_features + ['target']].dropna()

        if len(df_clean) == 0:
            return jsonify({'error': '清洗后无有效数据'}), 400

        # 提取特征 - 确保列顺序与训练时一致
        X = df_clean[available_features].values

        logger.info(f"Prepared prediction data: X shape={X.shape}, features={len(available_features)}")

        # 获取最后一行作为预测输入
        X_latest = X[-1:, :]

        # 标准化
        X_scaled = engineer.fit_transform(X)
        X_latest_scaled = X_scaled[-1:, :]

        # 生成预测
        try:
            prediction = model.predict(X_latest_scaled)[0]

            # 检查预测值是否有效
            if not np.isfinite(prediction):
                return jsonify({'error': f'预测结果无效: {prediction}'}), 500

            # 生成带置信区间的预测
            try:
                pred_with_conf, confidence = model.predict_with_confidence(X_latest_scaled)
                conf_lower = float(confidence[0][0]) if len(confidence) > 0 else prediction - 0.01
                conf_upper = float(confidence[0][1]) if len(confidence) > 0 else prediction + 0.01

                # 确保置信区间有效
                if not np.isfinite(conf_lower):
                    conf_lower = prediction - 0.01
                if not np.isfinite(conf_upper):
                    conf_upper = prediction + 0.01
            except Exception as conf_err:
                logger.warning(f"Could not generate confidence interval: {conf_err}")
                # 使用默认置信区间
                conf_lower = prediction - 0.01
                conf_upper = prediction + 0.01

            # 获取模型的目标类型
            ml_db = get_ml_db()
            model_info = ml_db.get_model(model_id)
            target_type = model_info.get('target_type', 'return_1d') if model_info else 'return_1d'

            return jsonify({
                'success': True,
                'prediction': float(prediction),
                'confidence_lower': conf_lower,
                'confidence_upper': conf_upper,
                'symbol': symbol,
                'model_id': model_id,
                'date': end_date,
                'target_type': target_type
            })
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

@ml_bp.route('/api/ml/quarterly/train', methods=['POST'])
def api_train_quarterly_model():
    """
    训练季度财务预测模型 API

    Body (JSON):
        symbols: 股票代码列表
        start_quarter: 开始季度 (e.g., "2020Q1")
        end_quarter: 结束季度 (e.g., "2024Q4")
        feature_mode: 特征模式 ("financial_only", "with_reports", "with_valuation")
        train_mode: 训练模式 ("single", "multi")
        optimize_hyperparams: 是否优化超参数 (default: false)
        train_ratio: 训练集比例 (default: 0.7)
        val_ratio: 验证集比例 (default: 0.15)
    """
    try:
        from web.services.quarterly_ml_service import create_quarterly_training_task

        data = request.json
        if not data:
            return jsonify({'error': '请求数据为空'}), 400

        # Extract parameters
        symbols = data.get('symbols', [])
        start_quarter = data.get('start_quarter', '2020Q1')
        end_quarter = data.get('end_quarter', '2024Q4')
        feature_mode = data.get('feature_mode', 'financial_only')
        train_mode = data.get('train_mode', 'multi')
        optimize_hyperparams = data.get('optimize_hyperparams', False)
        train_ratio = data.get('train_ratio', 0.7)
        val_ratio = data.get('val_ratio', 0.15)

        # Validate parameters
        if not symbols:
            return jsonify({'error': '必须提供股票代码列表 (symbols)'}), 400

        if train_mode == 'single' and len(symbols) > 1:
            return jsonify({'error': '单股票模式 (single) 只能提供一只股票代码'}), 400

        # Create training task
        result = create_quarterly_training_task({
            'symbols': symbols,
            'start_quarter': start_quarter,
            'end_quarter': end_quarter,
            'feature_mode': feature_mode,
            'train_mode': train_mode,
            'optimize_hyperparams': optimize_hyperparams,
            'train_ratio': train_ratio,
            'val_ratio': val_ratio
        })

        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify(result), 400

    except Exception as e:
        logger.error(f"Error in api_train_quarterly_model: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'error': f'请求失败: {str(e)}'}), 500


@ml_bp.route('/api/ml/quarterly/models')
def api_list_quarterly_models():
    """
    列出所有季度模型

    Query params:
        symbol: 股票代码过滤 (可选)
        limit: 返回数量限制 (默认50)
    """
    try:
        from web.services.quarterly_ml_service import get_quarterly_models

        symbol = request.args.get('symbol')
        limit = int(request.args.get('limit', 50))

        result = get_quarterly_models(symbol=symbol, limit=limit)

        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify(result), 500

    except Exception as e:
        logger.error(f"Error listing quarterly models: {str(e)}")
        return jsonify({'error': f'查询失败: {str(e)}'}), 500


@ml_bp.route('/api/ml/quarterly/models/<model_id>')
def api_get_quarterly_model(model_id):
    """
    获取季度模型详细信息

    Path params:
        model_id: 模型ID
    """
    try:
        from web.services.quarterly_ml_service import get_quarterly_model

        result = get_quarterly_model(model_id)

        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify(result), 404 if 'not found' in result.get('error', '') else 500

    except Exception as e:
        logger.error(f"Error getting quarterly model: {str(e)}")
        return jsonify({'error': f'查询失败: {str(e)}'}), 500


@ml_bp.route('/api/ml/quarterly/predict', methods=['POST'])
def api_predict_quarterly():
    """
    使用季度模型进行预测

    Body (JSON):
        model_id: 模型ID
        symbols: 股票代码列表
    """
    try:
        from web.services.quarterly_ml_service import predict_quarterly_return

        data = request.json
        if not data:
            return jsonify({'error': '请求数据为空'}), 400

        model_id = data.get('model_id')
        symbols = data.get('symbols', [])

        if not model_id:
            return jsonify({'error': '请指定模型ID'}), 400

        if not symbols:
            return jsonify({'error': '请提供股票代码列表'}), 400

        result = predict_quarterly_return(model_id, symbols)

        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify(result), 500

    except Exception as e:
        logger.error(f"Error in quarterly prediction: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'error': f'预测失败: {str(e)}'}), 500


@ml_bp.route('/api/ml/quarterly/models/<model_id>/evaluation')
def api_evaluate_quarterly_model(model_id):
    """
    获取季度模型评估报告

    Path params:
        model_id: 模型ID

    Query params:
        test_start_quarter: 测试期开始季度 (可选)
        test_end_quarter: 测试期结束季度 (可选)
    """
    try:
        from web.services.quarterly_ml_service import evaluate_quarterly_model

        test_start_quarter = request.args.get('test_start_quarter')
        test_end_quarter = request.args.get('test_end_quarter')

        result = evaluate_quarterly_model(
            model_id,
            test_start_quarter=test_start_quarter,
            test_end_quarter=test_end_quarter
        )

        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify(result), 404 if 'not found' in result.get('error', '') else 500

    except Exception as e:
        logger.error(f"Error evaluating quarterly model: {str(e)}")
        return jsonify({'error': f'评估失败: {str(e)}'}), 500


@ml_bp.route('/api/ml/quarterly/models/<model_id>/importance')
def api_get_quarterly_feature_importance(model_id):
    """
    获取季度模型特征重要性

    Path params:
        model_id: 模型ID

    Query params:
        top_n: 返回前N个特征 (默认20)
    """
    try:
        from web.services.quarterly_ml_service import get_feature_importance

        top_n = int(request.args.get('top_n', 20))

        result = get_feature_importance(model_id, top_n=top_n)

        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify(result), 404 if 'not found' in result.get('error', '') else 500

    except Exception as e:
        logger.error(f"Error getting feature importance: {str(e)}")
        return jsonify({'error': f'查询失败: {str(e)}'}), 500


@ml_bp.route('/api/ml/quarterly/models/<model_id>', methods=['DELETE'])
def api_delete_quarterly_model(model_id):
    """
    删除季度模型

    Path params:
        model_id: 模型ID
    """
    try:
        from web.services.quarterly_ml_service import delete_quarterly_model

        result = delete_quarterly_model(model_id)

        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify(result), 500

    except Exception as e:
        logger.error(f"Error deleting quarterly model: {str(e)}")
        return jsonify({'error': f'删除失败: {str(e)}'}), 500


def register_ml_routes(app):
    """
    注册ML路由到Flask应用

    Args:
        app: Flask应用实例
    """
    app.register_blueprint(ml_bp)
