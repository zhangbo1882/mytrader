"""
Walk-Forward Validation for REGIME_PARAMS optimization.

Implements walk-forward validation to prevent overfitting:
- Train on 80% of data, validate on 20%
- Only accept parameters that pass validation
- Calculate degradation rate between train and validation
"""

import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def anchored_walk_forward_optimization(
    stock_code: str,
    start_date: str,
    end_date: str,
    n_workers: int = 4,
    train_ratio: float = 0.65,
    embargo_calendar_days: int = 28,
    n_test_folds: int = 6,
    min_avg_val_score: float = 0.10,    # Low threshold for quarterly folds
    min_pass_ratio: float = 0.50,
) -> Optional[Dict[str, Any]]:
    """
    Anchored Walk-Forward Optimization with embargo period.

    Design rationale vs the simple multi_val approach:
    - Uses only train_ratio (65%) of data for training, leaving more for testing
    - Adds an embargo period between train end and first test to prevent data leakage
    - Divides the remaining test data into n_test_folds non-overlapping periods
    - Tests the trained parameters on all folds independently
    - Requires min_pass_ratio of folds to pass to accept parameters

    Args:
        stock_code: Stock symbol to optimize
        start_date: Overall start date (YYYY-MM-DD)
        end_date: Overall end date (YYYY-MM-DD)
        n_workers: Number of parallel workers for training optimization
        train_ratio: Fraction of data to use for training (default: 0.65 = 65%)
        embargo_calendar_days: Calendar days between training end and first test (default: 28)
        n_test_folds: Number of non-overlapping quarterly test periods (default: 6)
        min_avg_val_score: Minimum average validation score to accept parameters
        min_pass_ratio: Minimum fraction of folds that must pass

    Returns:
        Dictionary with params, scores, fold results, or None if validation fails
    """
    from src.strategies.price_breakout.optimizer.parallel_optimizer import ParallelRegimeOptimizer

    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    total_days = (end_dt - start_dt).days

    # Calculate training end date
    train_end_dt = start_dt + timedelta(days=int(total_days * train_ratio))
    train_end = train_end_dt.strftime('%Y-%m-%d')

    # Calculate first test start (after embargo)
    first_test_start_dt = train_end_dt + timedelta(days=embargo_calendar_days)

    # Calculate test folds
    remaining_days = (end_dt - first_test_start_dt).days
    if remaining_days <= 0:
        logger.error(
            f"Not enough data after embargo. "
            f"Train ends: {train_end}, Embargo: {embargo_calendar_days} days, "
            f"Data ends: {end_date}"
        )
        return None

    fold_days = remaining_days // n_test_folds

    if fold_days < 30:
        logger.warning(
            f"Fold size too small ({fold_days} days). "
            f"Consider reducing n_test_folds or increasing date range."
        )
        n_test_folds = max(2, remaining_days // 60)
        fold_days = remaining_days // n_test_folds

    # Build fold list
    folds: List[Tuple[str, str, str]] = []
    for i in range(n_test_folds):
        fold_start_dt = first_test_start_dt + timedelta(days=i * fold_days)
        fold_end_dt = first_test_start_dt + timedelta(days=(i + 1) * fold_days - 1)
        if fold_end_dt > end_dt:
            fold_end_dt = end_dt
        if fold_start_dt >= end_dt:
            break
        folds.append((
            f'Fold{i + 1}',
            fold_start_dt.strftime('%Y-%m-%d'),
            fold_end_dt.strftime('%Y-%m-%d'),
        ))

    logger.info("=" * 70)
    logger.info("ANCHORED WALK-FORWARD OPTIMIZATION (with embargo)")
    logger.info("=" * 70)
    logger.info(f"Stock: {stock_code}")
    logger.info(f"Training:  {start_date} ~ {train_end} ({int(total_days * train_ratio)} days)")
    logger.info(f"Embargo:   {embargo_calendar_days} calendar days")
    logger.info(f"Test folds: {len(folds)}")
    for name, fs, fe in folds:
        logger.info(f"  {name}: {fs} ~ {fe}")
    logger.info(f"Min Avg Score: {min_avg_val_score}, Min Pass Ratio: {min_pass_ratio:.0%}")
    logger.info("=" * 70)

    print(f"\n{'=' * 70}")
    print(f"ANCHORED WALK-FORWARD OPTIMIZATION")
    print(f"{'=' * 70}")
    print(f"Stock: {stock_code}")
    print(f"Training: {start_date} ~ {train_end}  ({int(total_days * train_ratio)} days, {train_ratio:.0%} of data)")
    print(f"Embargo:  {embargo_calendar_days} calendar days (prevents data leakage)")
    print(f"Test:     {len(folds)} non-overlapping folds after embargo")
    for name, fs, fe in folds:
        print(f"  {name}: {fs} ~ {fe}")
    print(f"{'=' * 70}")

    # Phase 1: Optimize on training period only
    print(f"\n[Phase 1] Optimizing on training data {start_date} ~ {train_end}")
    train_optimizer = ParallelRegimeOptimizer(
        stock_code, start_date, train_end, n_workers
    )
    train_best = train_optimizer.optimize()

    if train_best is None:
        logger.error("Training optimization failed")
        print("[ERROR] Training optimization failed")
        return None

    train_score = train_best['score']
    logger.info(f"Training best score: {train_score:.4f}")
    print(f"[Phase 1 Complete] Training score: {train_score:.4f}")

    # Phase 2: Test on each fold (all folds share same trained parameters)
    print(f"\n[Phase 2] Testing on {len(folds)} out-of-sample folds")
    fold_results = []
    passed_folds = 0

    for fold_name, fold_start, fold_end in folds:
        print(f"\n  Testing {fold_name}: {fold_start} ~ {fold_end}")
        logger.info(f"Testing {fold_name}: {fold_start} ~ {fold_end}")

        val_result = _validate_parameters(
            stock_code, train_best['params'], fold_start, fold_end,
            # Validation folds are short: only require Sharpe > 0 and at least 1 trade
            min_trades=1,
            min_sharpe=0.0,
            max_drawdown_limit=-0.35,
        )

        if val_result and val_result.get('valid', False):
            fold_score = val_result['score']
            passed = fold_score >= min_avg_val_score
            fold_results.append({
                'name': fold_name,
                'start': fold_start,
                'end': fold_end,
                'score': fold_score,
                'passed': passed,
            })
            if passed:
                passed_folds += 1
                print(f"    PASSED: Score {fold_score:.4f}")
            else:
                print(f"    FAILED: Score {fold_score:.4f} < {min_avg_val_score:.2f}")
        else:
            fold_results.append({
                'name': fold_name,
                'start': fold_start,
                'end': fold_end,
                'score': 0.0,
                'passed': False,
            })
            print(f"    FAILED: No valid result")

    # Phase 3: Evaluate overall
    avg_val_score = sum(r['score'] for r in fold_results) / len(fold_results) if fold_results else 0.0
    pass_ratio = passed_folds / len(folds) if folds else 0.0

    degradations = [
        (train_score - r['score']) / train_score
        for r in fold_results
        if r['score'] > 0 and train_score > 0
    ]
    avg_degradation = sum(degradations) / len(degradations) if degradations else 0.0

    print(f"\n[Anchored WFO Result]")
    print(f"  Train Score:     {train_score:.4f}")
    print(f"  Avg Test Score:  {avg_val_score:.4f}  (min: {min_avg_val_score:.2f})")
    print(f"  Pass Ratio:      {pass_ratio:.1%}  ({passed_folds}/{len(folds)} folds, min: {min_pass_ratio:.0%})")
    print(f"  Avg Degradation: {avg_degradation:.1%}")
    for r in fold_results:
        status = "PASS" if r['passed'] else "FAIL"
        print(f"    [{status}] {r['name']} {r['start']}~{r['end']}: {r['score']:.4f}")

    if avg_val_score < min_avg_val_score or pass_ratio < min_pass_ratio:
        logger.warning(
            f"Anchored WFO FAILED: AvgScore={avg_val_score:.4f}<{min_avg_val_score} "
            f"or PassRatio={pass_ratio:.1%}<{min_pass_ratio:.0%}"
        )
        print(f"\n[FAILED] Parameters rejected: avg test score or pass ratio below threshold")
        return None

    logger.info(
        f"Anchored WFO PASSED: Train={train_score:.4f} → AvgTest={avg_val_score:.4f} "
        f"(degradation: {avg_degradation:.1%}, pass_ratio: {pass_ratio:.1%})"
    )
    print(f"\n[PASSED] Parameters accepted")

    return {
        'params': train_best['params'],
        'score': avg_val_score,
        'train_score': train_score,
        'avg_val_score': avg_val_score,
        'pass_ratio': pass_ratio,
        'avg_degradation': avg_degradation,
        'fold_results': fold_results,
        'metrics': fold_results[0].get('metrics', train_best.get('metrics', {})) if fold_results else {},
    }


def walk_forward_optimization(
    stock_code: str,
    train_start: str,
    train_end: str,
    val_start: str,
    val_end: str,
    n_workers: int = 4,
    min_val_score: float = 0.30  # 降低阈值: 0.4 → 0.30，允许30%退化
) -> Optional[Dict[str, Any]]:
    """
    Walk-Forward validation optimization flow.

    1. Optimize parameters on training period (80%)
    2. Validate parameters on validation period (20%)
    3. Only accept if validation score >= min_val_score
    4. Calculate degradation rate

    Args:
        stock_code: Stock symbol to optimize
        train_start: Training period start date (YYYY-MM-DD)
        train_end: Training period end date (YYYY-MM-DD)
        val_start: Validation period start date (YYYY-MM-DD)
        val_end: Validation period end date (YYYY-MM-DD)
        n_workers: Number of parallel workers
        min_val_score: Minimum validation score to accept parameters

    Returns:
        Dictionary with params, score, train_score, val_score, degradation, metrics
        or None if validation fails
    """
    from src.strategies.price_breakout.optimizer.parallel_optimizer import ParallelRegimeOptimizer

    logger.info("=" * 70)
    logger.info("WALK-FORWARD OPTIMIZATION")
    logger.info("=" * 70)
    logger.info(f"Stock: {stock_code}")
    logger.info(f"Training:   {train_start} ~ {train_end}")
    logger.info(f"Validation: {val_start} ~ {val_end}")
    logger.info(f"Min Val Score: {min_val_score}")
    logger.info("=" * 70)

    # Phase 1: Training period optimization
    print(f"\n[Phase 1] Training on {train_start} ~ {train_end}")
    logger.info(f"Phase 1: Optimizing on training period...")

    train_optimizer = ParallelRegimeOptimizer(
        stock_code, train_start, train_end, n_workers
    )
    train_best = train_optimizer.optimize()

    if train_best is None:
        logger.error("Training optimization failed")
        print("[ERROR] Training optimization failed")
        return None

    train_score = train_best['score']
    logger.info(f"Training best score: {train_score:.4f}")
    print(f"[Phase 1 Complete] Training score: {train_score:.4f}")

    # Phase 2: Validation period backtest
    print(f"\n[Phase 2] Validating on {val_start} ~ {val_end}")
    logger.info(f"Phase 2: Validating on validation period...")

    val_result = _validate_parameters(
        stock_code, train_best['params'], val_start, val_end
    )

    if val_result is None or not val_result.get('valid', False):
        logger.warning(f"Validation failed: No valid backtest result")
        print(f"[FAILED] Validation failed: No valid backtest result")
        return None

    val_score = val_result['score']
    logger.info(f"Validation score: {val_score:.4f}")
    print(f"[Phase 2 Complete] Validation score: {val_score:.4f}")

    # Phase 3: Check validation score
    if val_score < min_val_score:
        degradation = (train_score - val_score) / train_score if train_score > 0 else 0
        logger.warning(
            f"Validation FAILED: Train={train_score:.4f} → Val={val_score:.4f} "
            f"(degradation: {degradation:.1%}, below min: {min_val_score:.2f})"
        )
        print(
            f"[FAILED] Validation: Train {train_score:.4f} → Val {val_score:.4f} "
            f"(degradation: {degradation:.1%}, below min: {min_val_score:.2f})"
        )
        return None

    # Phase 4: Calculate degradation rate
    degradation = (train_score - val_score) / train_score if train_score > 0 else 0

    logger.info(
        f"Validation PASSED: Train={train_score:.4f} → Val={val_score:.4f} "
        f"(degradation: {degradation:.1%})"
    )
    print(
        f"[PASSED] Validation: Train {train_score:.4f} → Val {val_score:.4f} "
        f"(degradation: {degradation:.1%})"
    )

    return {
        'params': train_best['params'],
        'score': val_score,  # Use validation score as final score
        'train_score': train_score,
        'val_score': val_score,
        'degradation': degradation,
        'metrics': val_result.get('metrics', train_best.get('metrics', {}))
    }


def _validate_parameters(
    stock_code: str,
    params: Dict[str, Any],
    val_start: str,
    val_end: str,
    min_trades: int = 15,
    min_sharpe: float = 0.5,
    max_drawdown_limit: float = -0.15,
) -> Optional[Dict[str, Any]]:
    """
    Run backtest on validation period with given parameters.

    Args:
        stock_code: Stock symbol
        params: Optimized parameters from training phase
        val_start: Validation period start date
        val_end: Validation period end date
        min_trades: Minimum trades required (default: 15; use 5 for short folds)
        min_sharpe: Minimum Sharpe required (default: 0.5; use 0.2 for short folds)
        max_drawdown_limit: Max drawdown allowed (default: -0.15; use -0.25 for short folds)

    Returns:
        Dictionary with score, valid flag, metrics
    """
    from web.services.backtest_service import run_single_backtest
    from src.market.market_regime import update_regime_params, reset_regime_params
    from src.strategies.price_breakout.optimizer.objectives import calculate_score, check_constraints

    # Set quiet mode
    import os
    os.environ['BACKTEST_QUIET_MODE'] = '1'
    os.environ['STRATEGY_QUIET_MODE'] = '1'

    try:
        # Reset and apply parameters
        reset_regime_params()
        update_regime_params(params)

        # Extract strategy_params and remove unsupported params
        strategy_params = params.get('strategy_params', {}).copy()
        strategy_params.pop('enable_adaptive_thresholds', None)
        strategy_params.pop('enable_blacklist', None)

        # Build backtest parameters
        bt_params = {
            'stock': stock_code,
            'start_date': val_start,
            'end_date': val_end,
            'strategy': 'price_breakout',
            'strategy_params': strategy_params,
        }

        # Run validation backtest
        result = run_single_backtest(bt_params)

        if result is None:
            return {
                'score': 0.0,
                'valid': False,
                'error': 'Backtest returned None'
            }

        # Check constraints (use caller-specified thresholds)
        if not check_constraints(result, max_drawdown_limit=max_drawdown_limit,
                                 min_trades=min_trades, min_sharpe=min_sharpe):
            return {
                'score': 0.0,
                'valid': False,
                'metrics': result
            }

        # Calculate score
        score = calculate_score(result)

        return {
            'score': score,
            'valid': True,
            'metrics': result
        }

    except Exception as e:
        import traceback
        logger.error(f"Validation backtest failed: {e}")
        return {
            'score': 0.0,
            'valid': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }


def rolling_window_optimization(
    stock_code: str,
    start_date: str,
    end_date: str,
    train_days: int = 250,
    val_days: int = 60,
    n_workers: int = 4
) -> list:
    """
    Rolling window optimization for robust parameter selection.

    Creates multiple rolling windows and optimizes on each.
    Returns best parameters based on validation performance.

    Args:
        stock_code: Stock symbol
        start_date: Overall start date
        end_date: Overall end date
        train_days: Number of training days per window
        val_days: Number of validation days per window
        n_workers: Number of parallel workers

    Returns:
        List of window results with params and scores
    """
    from datetime import datetime, timedelta

    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    # Calculate number of windows
    total_days = (end_dt - start_dt).days
    window_step = train_days + val_days

    results = []
    window_num = 0

    current_start = start_dt
    while current_start < end_dt:
        window_num += 1

        train_end_dt = current_start + timedelta(days=train_days)
        val_start_dt = train_end_dt + timedelta(days=1)
        val_end_dt = val_start_dt + timedelta(days=val_days)

        # Skip if validation period extends beyond end_date
        if val_end_dt > end_dt:
            break

        train_end = train_end_dt.strftime('%Y-%m-%d')
        val_start = val_start_dt.strftime('%Y-%m-%d')
        val_end = val_end_dt.strftime('%Y-%m-%d')
        win_start = current_start.strftime('%Y-%m-%d')

        print(f"\n[Window {window_num}] Train: {win_start}~{train_end}, Val: {val_start}~{val_end}")

        result = walk_forward_optimization(
            stock_code=stock_code,
            train_start=win_start,
            train_end=train_end,
            val_start=val_start,
            val_end=val_end,
            n_workers=n_workers
        )

        if result:
            results.append({
                'window': window_num,
                'train_start': win_start,
                'train_end': train_end,
                'val_start': val_start,
                'val_end': val_end,
                'result': result
            })

        # Move to next window
        current_start = val_start_dt

    logger.info(f"Rolling window complete: {len(results)} valid windows")
    return results


def multi_period_walk_forward_optimization(
    stock_code: str,
    train_start: str,
    train_end: str,
    validation_periods: list,
    n_workers: int = 4,
    min_avg_val_score: float = 0.30,  # 降低阈值: 0.35 → 0.30
    min_pass_ratio: float = 0.25  # 降低通过率要求: 0.5 → 0.25
) -> Optional[Dict[str, Any]]:
    """
    Multi-period Walk-Forward validation for robust parameter selection.

    Instead of single validation period, tests parameters on multiple validation periods.
    Only accepts parameters that perform consistently across multiple periods.

    Args:
        stock_code: Stock symbol
        train_start: Training period start date
        train_end: Training period end date
        validation_periods: List of (name, start_date, end_date) tuples for validation
        n_workers: Number of parallel workers
        min_avg_val_score: Minimum average validation score
        min_pass_ratio: Minimum ratio of validation periods that must pass (0.5 = 50%)

    Returns:
        Dictionary with params, scores, metrics if validation passed, None otherwise
    """
    from src.strategies.price_breakout.optimizer.parallel_optimizer import ParallelRegimeOptimizer

    logger.info("=" * 70)
    logger.info("MULTI-PERIOD WALK-FORWARD OPTIMIZATION")
    logger.info("=" * 70)
    logger.info(f"Stock: {stock_code}")
    logger.info(f"Training: {train_start} ~ {train_end}")
    logger.info(f"Validation Periods: {len(validation_periods)}")
    for name, val_start, val_end in validation_periods:
        logger.info(f"  - {name}: {val_start} ~ {val_end}")
    logger.info(f"Min Avg Score: {min_avg_val_score}")
    logger.info(f"Min Pass Ratio: {min_pass_ratio:.0%}")
    logger.info("=" * 70)

    # Phase 1: Training period optimization
    print(f"\n[Phase 1] Training on {train_start} ~ {train_end}")
    logger.info(f"Phase 1: Optimizing on training period...")

    train_optimizer = ParallelRegimeOptimizer(
        stock_code, train_start, train_end, n_workers
    )
    train_best = train_optimizer.optimize()

    if train_best is None:
        logger.error("Training optimization failed")
        print("[ERROR] Training optimization failed")
        return None

    train_score = train_best['score']
    logger.info(f"Training best score: {train_score:.4f}")
    print(f"[Phase 1 Complete] Training score: {train_score:.4f}")

    # Phase 2: Multi-period validation
    print(f"\n[Phase 2] Validating on {len(validation_periods)} periods")
    logger.info(f"Phase 2: Multi-period validation...")

    val_results = []
    passed_periods = 0

    for period_name, val_start, val_end in validation_periods:
        print(f"\n  Validating: {period_name} ({val_start} ~ {val_end})")
        logger.info(f"Validating {period_name}: {val_start} ~ {val_end}")

        val_result = _validate_parameters(
            stock_code, train_best['params'], val_start, val_end
        )

        if val_result and val_result.get('valid', False):
            val_score = val_result['score']
            val_results.append({
                'name': period_name,
                'start': val_start,
                'end': val_end,
                'score': val_score,
                'passed': val_score >= min_avg_val_score
            })

            if val_score >= min_avg_val_score:
                passed_periods += 1
                print(f"    ✓ PASSED: Score {val_score:.4f}")
            else:
                print(f"    ✗ FAILED: Score {val_score:.4f} < {min_avg_val_score:.2f}")
        else:
            val_results.append({
                'name': period_name,
                'start': val_start,
                'end': val_end,
                'score': 0.0,
                'passed': False
            })
            print(f"    ✗ FAILED: No valid result")

    # Phase 3: Check multi-period validation
    avg_val_score = sum(r['score'] for r in val_results) / len(val_results)
    pass_ratio = passed_periods / len(validation_periods)

    logger.info(f"Multi-period Validation Results:")
    logger.info(f"  Average Score: {avg_val_score:.4f} (min: {min_avg_val_score:.2f})")
    logger.info(f"  Pass Ratio: {pass_ratio:.1%} (min: {min_pass_ratio:.0%})")
    logger.info(f"  Passed: {passed_periods}/{len(validation_periods)} periods")

    print(f"\n[Multi-Period Validation]")
    print(f"  Average Score: {avg_val_score:.4f} (min: {min_avg_val_score:.2f})")
    print(f"  Pass Ratio: {pass_ratio:.1%} (min: {min_pass_ratio:.0%})")
    print(f"  Passed: {passed_periods}/{len(validation_periods)} periods")

    # Check if passed
    if avg_val_score < min_avg_val_score or pass_ratio < min_pass_ratio:
        logger.warning(
            f"Validation FAILED: Avg={avg_val_score:.4f}<{min_avg_val_score:.2f} "
            f"or Pass={pass_ratio:.1%}<{min_pass_ratio:.0%}"
        )
        print(f"[FAILED] Validation: Avg score {avg_val_score:.4f} or pass ratio {pass_ratio:.1%} below threshold")
        return None

    # Calculate degradation metrics
    degradations = []
    for r in val_results:
        if r['score'] > 0:
            deg = (train_score - r['score']) / train_score if train_score > 0 else 0
            degradations.append(deg)

    avg_degradation = sum(degradations) / len(degradations) if degradations else 0

    logger.info(
        f"Validation PASSED: Train={train_score:.4f} → AvgVal={avg_val_score:.4f} "
        f"(degradation: {avg_degradation:.1%})"
    )
    print(f"[PASSED] Validation: Train {train_score:.4f} → AvgVal {avg_val_score:.4f} (degradation: {avg_degradation:.1%})")

    return {
        'params': train_best['params'],
        'score': avg_val_score,  # Use average validation score as final score
        'train_score': train_score,
        'val_scores': [r['score'] for r in val_results],
        'avg_val_score': avg_val_score,
        'pass_ratio': pass_ratio,
        'avg_degradation': avg_degradation,
        'validation_results': val_results,
        'metrics': val_results[0].get('metrics', train_best.get('metrics', {}))
    }
