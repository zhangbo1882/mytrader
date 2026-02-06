"""
Quarterly ML Model Service

Business logic for quarterly financial prediction models.
"""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from src.ml.trainers.quarterly_trainer import QuarterlyTrainer
from src.ml.quarterly_data_loader import QuarterlyFinancialDataLoader
from src.ml.financial_feature_engineer import FinancialFeatureEngineer
from src.ml.evaluators.quarterly_metrics import QuarterlyEvaluator
from src.ml.utils import get_ml_db
from config.settings import TUSHARE_DB_PATH

logger = logging.getLogger(__name__)


def get_quarterly_models(
    symbol: Optional[str] = None,
    limit: int = 50
) -> Dict[str, Any]:
    """
    Get list of quarterly models

    Args:
        symbol: Filter by stock symbol
        limit: Maximum number of models to return

    Returns:
        Dictionary with success status and models list
    """
    try:
        ml_db = get_ml_db(str(TUSHARE_DB_PATH))
        models = ml_db.list_models(
            symbol=symbol,
            model_type='quarterly_lgb',
            limit=limit
        )

        return {
            'success': True,
            'models': models,
            'count': len(models)
        }
    except Exception as e:
        logger.error(f"Error getting quarterly models: {e}")
        return {
            'success': False,
            'error': str(e)
        }


def get_quarterly_model(model_id: str) -> Dict[str, Any]:
    """
    Get a specific quarterly model details

    Args:
        model_id: Model ID

    Returns:
        Dictionary with success status and model details
    """
    try:
        ml_db = get_ml_db(str(TUSHARE_DB_PATH))
        model = ml_db.get_model(model_id)

        if model is None:
            return {
                'success': False,
                'error': f'Model {model_id} not found'
            }

        # Parse JSON fields if needed
        if model.get('hyperparameters'):
            import json
            if isinstance(model['hyperparameters'], str):
                model['hyperparameters'] = json.loads(model['hyperparameters'])
        if model.get('metrics'):
            import json
            if isinstance(model['metrics'], str):
                model['metrics'] = json.loads(model['metrics'])
        if model.get('feature_importance'):
            import json
            if isinstance(model['feature_importance'], str):
                model['feature_importance'] = json.loads(model['feature_importance'])

        return {
            'success': True,
            'model': model
        }
    except Exception as e:
        logger.error(f"Error getting model {model_id}: {e}")
        return {
            'success': False,
            'error': str(e)
        }


def create_quarterly_training_task(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a quarterly model training task

    Args:
        params: Training parameters
            - symbols: List of stock codes
            - start_quarter: Start quarter (e.g., "2020Q1")
            - end_quarter: End quarter (e.g., "2024Q4")
            - feature_mode: Feature mode
            - train_mode: Training mode
            - optimize_hyperparams: Whether to optimize hyperparameters

    Returns:
        Dictionary with success status and task_id
    """
    try:
        from worker.task_manager import TaskManager

        # Validate required parameters
        symbols = params.get('symbols', [])
        if not symbols:
            return {
                'success': False,
                'error': '必须提供股票代码列表 (symbols)'
            }

        # Create task
        tm = TaskManager()
        task_id = tm.create_task(
            task_type='train_quarterly_model',
            params=params,
            description=f"训练季度模型: {params.get('train_mode', 'multi')} 模式, {len(symbols)} 只股票"
        )

        return {
            'success': True,
            'task_id': task_id,
            'message': '训练任务已创建'
        }
    except Exception as e:
        logger.error(f"Error creating training task: {e}")
        return {
            'success': False,
            'error': str(e)
        }


def predict_quarterly_return(
    model_id: str,
    symbols: List[str]
) -> Dict[str, Any]:
    """
    Predict next quarter returns using a trained model

    Args:
        model_id: Model ID
        symbols: Stock codes to predict

    Returns:
        Dictionary with success status and predictions
    """
    try:
        # Load model
        trainer = QuarterlyTrainer(db_path=str(TUSHARE_DB_PATH))
        trainer.load_model(model_id)

        # Get model metadata to understand feature requirements
        ml_db = get_ml_db(str(TUSHARE_DB_PATH))
        model_metadata = ml_db.get_model(model_id)

        if model_metadata is None:
            return {
                'success': False,
                'error': f'Model {model_id} not found'
            }

        # Load data for prediction
        metadata = model_metadata.get('metadata', {})
        if isinstance(metadata, str):
            import json
            metadata = json.loads(metadata)

        feature_mode = metadata.get('feature_mode', 'financial_only')
        end_quarter = metadata.get('end_quarter', '2024Q4')

        # Load latest quarterly data for prediction
        data_loader = QuarterlyFinancialDataLoader(str(TUSHARE_DB_PATH))
        df = data_loader.load_quarterly_data(
            symbols=symbols,
            start_quarter='2020Q1',
            end_quarter=end_quarter,
            feature_mode=feature_mode
        )

        if df.empty:
            return {
                'success': False,
                'error': 'No data available for prediction'
            }

        # Perform feature engineering
        feature_engineer = FinancialFeatureEngineer()
        df = feature_engineer.engineer_features(
            df,
            group_col='symbol' if len(symbols) > 1 else None
        )

        # Get predictions for the most recent quarter
        predictions = trainer.predict(df)

        # Format results
        results = []
        for i, symbol in enumerate(symbols):
            # Find the most recent prediction for each symbol
            symbol_mask = df['symbol'] == symbol if 'symbol' in df.columns else slice(None)
            symbol_df = df[symbol_mask]

            if not symbol_df.empty:
                most_recent_idx = symbol_df.index[-1]
                prediction = float(predictions[most_recent_idx])

                results.append({
                    'symbol': symbol,
                    'prediction': prediction,
                    'prediction_pct': prediction * 100
                })

        return {
            'success': True,
            'model_id': model_id,
            'predictions': results,
            'generated_at': datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error predicting with model {model_id}: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }


def evaluate_quarterly_model(
    model_id: str,
    test_start_quarter: Optional[str] = None,
    test_end_quarter: Optional[str] = None
) -> Dict[str, Any]:
    """
    Evaluate a quarterly model performance

    Args:
        model_id: Model ID
        test_start_quarter: Test period start quarter (optional)
        test_end_quarter: Test period end quarter (optional)

    Returns:
        Dictionary with success status and evaluation results
    """
    try:
        ml_db = get_ml_db(str(TUSHARE_DB_PATH))
        model_metadata = ml_db.get_model(model_id)

        if model_metadata is None:
            return {
                'success': False,
                'error': f'Model {model_id} not found'
            }

        evaluator = QuarterlyEvaluator()

        # Get performance records from database
        performance_records = ml_db.get_model_performance(model_id)

        results = {
            'model_id': model_id,
            'metadata': model_metadata,
            'performance_records': performance_records
        }

        # If metrics are stored in model metadata, include them
        if model_metadata.get('metrics'):
            import json
            metrics = model_metadata['metrics']
            if isinstance(metrics, str):
                metrics = json.loads(metrics)
            results['test_metrics'] = metrics

        return {
            'success': True,
            'evaluation': results
        }

    except Exception as e:
        logger.error(f"Error evaluating model {model_id}: {e}")
        return {
            'success': False,
            'error': str(e)
        }


def delete_quarterly_model(model_id: str) -> Dict[str, Any]:
    """
    Delete a quarterly model

    Args:
        model_id: Model ID

    Returns:
        Dictionary with success status
    """
    try:
        ml_db = get_ml_db(str(TUSHARE_DB_PATH))

        # Delete from database
        success = ml_db.delete_model(model_id)

        if not success:
            return {
                'success': False,
                'error': f'Failed to delete model {model_id}'
            }

        # Delete model files
        from pathlib import Path
        model_dir = Path('data/ml_models')
        model_path = model_dir / model_id

        if model_path.with_suffix('.pkl').exists():
            model_path.with_suffix('.pkl').unlink()
        if model_path.with_suffix('.json').exists():
            model_path.with_suffix('.json').unlink()

        return {
            'success': True,
            'message': f'Model {model_id} deleted successfully'
        }

    except Exception as e:
        logger.error(f"Error deleting model {model_id}: {e}")
        return {
            'success': False,
            'error': str(e)
        }


def get_feature_importance(
    model_id: str,
    top_n: int = 20
) -> Dict[str, Any]:
    """
    Get feature importance for a model

    Args:
        model_id: Model ID
        top_n: Number of top features to return

    Returns:
        Dictionary with success status and feature importance
    """
    try:
        ml_db = get_ml_db(str(TUSHARE_DB_PATH))
        model = ml_db.get_model(model_id)

        if model is None:
            return {
                'success': False,
                'error': f'Model {model_id} not found'
            }

        feature_importance = model.get('feature_importance', {})

        if isinstance(feature_importance, str):
            import json
            feature_importance = json.loads(feature_importance)

        # Sort by importance
        sorted_features = sorted(
            feature_importance.items(),
            key=lambda x: x[1],
            reverse=True
        )[:top_n]

        return {
            'success': True,
            'model_id': model_id,
            'top_features': dict(sorted_features),
            'total_features': len(feature_importance)
        }

    except Exception as e:
        logger.error(f"Error getting feature importance for {model_id}: {e}")
        return {
            'success': False,
            'error': str(e)
        }
