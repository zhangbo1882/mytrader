"""
Time-Series Cross-Validation for REGIME_PARAMS optimization.

Implements time-series cross-validation to prevent overfitting.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def time_series_cv(
    stock_code: str,
    start_date: str,
    end_date: str,
    n_folds: int = 3,
    n_workers: int = 4
) -> Optional[Dict[str, Any]]:
    """
    Time-series cross-validation.

    Splits data into n_folds for cross-validation while maintaining
    temporal order (no look-ahead bias).

    Args:
        stock_code: Stock symbol
        start_date: Start date
        end_date: End date
        n_folds: Number of CV folds
        n_workers: Number of parallel workers

    Returns:
        Dictionary with averaged results and best params
    """
    from src.strategies.price_breakout.optimizer.parallel_optimizer import ParallelRegimeOptimizer
    from src.strategies.price_breakout.optimizer.objectives import calculate_score, check_constraints
    from web.services.backtest_service import run_single_backtest
    from src.market.market_regime import update_regime_params, reset_regime_params

    # Calculate fold boundaries
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    total_days = (end_dt - start_dt).days

    fold_size = total_days // n_folds

    fold_results = []

    for fold in range(n_folds):
        # Calculate train/val split for this fold
        fold_start_dt = start_dt + timedelta(days=fold * fold_size)
        fold_end_dt = fold_start_dt + timedelta(days=fold_size)

        # Use 80% for training, 20% for validation within each fold
        train_days = int(fold_size * 0.8)
        train_end_dt = fold_start_dt + timedelta(days=train_days)
        val_start_dt = train_end_dt + timedelta(days=1)

        train_start = fold_start_dt.strftime('%Y-%m-%d')
        train_end = train_end_dt.strftime('%Y-%m-%d')
        val_start = val_start_dt.strftime('%Y-%m-%d')
        val_end = fold_end_dt.strftime('%Y-%m-%d')

        print(f"\n[Fold {fold + 1}/{n_folds}] Train: {train_start}~{train_end}, Val: {val_start}~{val_end}")
        logger.info(f"Fold {fold + 1}: Training on {train_start}~{train_end}")

        # Optimize on training data
        optimizer = ParallelRegimeOptimizer(stock_code, train_start, train_end, n_workers)
        train_best = optimizer.optimize()

        if train_best is None:
            logger.warning(f"Fold {fold + 1}: Training failed")
            continue

        # Validate on validation data
        val_result = _validate_fold(
            stock_code, train_best['params'], val_start, val_end
        )

        if val_result and val_result.get('valid', False):
            fold_results.append({
                'fold': fold + 1,
                'params': train_best['params'],
                'train_score': train_best['score'],
                'val_score': val_result['score'],
                'degradation': (train_best['score'] - val_result['score']) / train_best['score']
                             if train_best['score'] > 0 else 0
            })

    if not fold_results:
        logger.error("All folds failed")
        return None

    # Calculate average scores
    avg_train_score = sum(r['train_score'] for r in fold_results) / len(fold_results)
    avg_val_score = sum(r['val_score'] for r in fold_results) / len(fold_results)
    avg_degradation = sum(r['degradation'] for r in fold_results) / len(fold_results)

    # Select best params based on validation score
    best_fold = max(fold_results, key=lambda x: x['val_score'])

    logger.info(f"CV Results: {len(fold_results)}/{n_folds} folds successful")
    logger.info(f"Avg Train Score: {avg_train_score:.4f}")
    logger.info(f"Avg Val Score: {avg_val_score:.4f}")
    logger.info(f"Avg Degradation: {avg_degradation:.1%}")

    print(f"\n[CV Results] {len(fold_results)}/{n_folds} folds successful")
    print(f"[CV Results] Avg Train: {avg_train_score:.4f}, Avg Val: {avg_val_score:.4f}")
    print(f"[CV Results] Avg Degradation: {avg_degradation:.1%}")

    # Run final backtest with best params on full period
    os.environ['BACKTEST_QUIET_MODE'] = '0'
    final_result = run_single_backtest({
        'stock': stock_code,
        'start_date': start_date,
        'end_date': end_date,
        'strategy': 'price_breakout',
        'strategy_params': best_fold['params'].get('strategy_params', {})
    })

    return {
        'params': best_fold['params'],
        'score': avg_val_score,  # Use average validation score
        'train_score': avg_train_score,
        'val_score': avg_val_score,
        'degradation': avg_degradation,
        'metrics': final_result if final_result else {},
        'cv_results': fold_results
    }


def _validate_fold(
    stock_code: str,
    params: Dict[str, Any],
    val_start: str,
    val_end: str
) -> Optional[Dict[str, Any]]:
    """Validate parameters on a fold."""
    from web.services.backtest_service import run_single_backtest
    from src.market.market_regime import update_regime_params, reset_regime_params
    from src.strategies.price_breakout.optimizer.objectives import calculate_score, check_constraints
    import os

    os.environ['BACKTEST_QUIET_MODE'] = '1'
    os.environ['STRATEGY_QUIET_MODE'] = '1'

    try:
        reset_regime_params()
        update_regime_params(params)

        result = run_single_backtest({
            'stock': stock_code,
            'start_date': val_start,
            'end_date': val_end,
            'strategy': 'price_breakout',
            'strategy_params': params.get('strategy_params', {})
        })

        if result is None or not check_constraints(result):
            return None

        score = calculate_score(result)
        return {'score': score, 'valid': True, 'metrics': result}

    except Exception as e:
        logger.error(f"Fold validation failed: {e}")
        return None
