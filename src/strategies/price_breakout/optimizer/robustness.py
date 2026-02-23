"""
Parameter robustness checker for optimization results.

After finding optimal parameters, verify they are not "island solutions" —
isolated points that happen to perform well due to luck rather than genuine edge.

A robust parameter set should have neighbors (nearby parameter values) that
also perform well. If small perturbations cause a large performance drop,
the parameters are fragile and likely overfit to training data.
"""

import logging
import random
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

# Score threshold for a perturbed parameter set to "pass"
ROBUSTNESS_PASS_THRESHOLD = 0.25


def _perturb_params(params: Dict[str, Any], noise_pct: float = 0.15) -> Dict[str, Any]:
    """
    Add multiplicative noise to numeric parameters.

    Args:
        params: Original parameter dictionary
        noise_pct: Maximum relative perturbation (default: ±15%)

    Returns:
        New parameter dictionary with perturbed values
    """
    import copy
    perturbed = copy.deepcopy(params)

    def _jitter(value: float) -> float:
        if value is None or (isinstance(value, float) and (value != value)):  # None or NaN
            return value
        factor = 1.0 + random.uniform(-noise_pct, noise_pct)
        result = round(value * factor, 3)
        return result if result == result else value  # guard against NaN result

    # Perturb strategy_params (base thresholds)
    sp = perturbed.get('strategy_params', {})
    for key in ['base_buy_threshold', 'base_sell_threshold', 'base_stop_loss_threshold']:
        if key in sp and isinstance(sp[key], (int, float)):
            sp[key] = max(0.1, _jitter(sp[key]))

    # Perturb regime multipliers
    rp = perturbed.get('regime_params', {})
    for regime in ['bull', 'bear', 'neutral']:
        regime_dict = rp.get(regime, {})
        for key in ['buy_threshold_multiplier', 'sell_threshold_multiplier', 'stop_loss_multiplier']:
            if key in regime_dict and isinstance(regime_dict[key], (int, float)):
                regime_dict[key] = max(0.1, min(5.0, _jitter(regime_dict[key])))

    return perturbed


def check_parameter_robustness(
    best_params: Dict[str, Any],
    stock_code: str,
    start_date: str,
    end_date: str,
    n_perturbations: int = 20,
    noise_pct: float = 0.15,
    pass_threshold: float = ROBUSTNESS_PASS_THRESHOLD,
) -> Dict[str, Any]:
    """
    Test robustness of optimal parameters by testing perturbed neighbors.

    Generates n_perturbations random perturbations of best_params (±noise_pct)
    and runs backtests on each. If a high fraction of neighbors also perform
    well, the parameters are considered robust (not an isolated peak).

    Args:
        best_params: Optimal parameters to test
        stock_code: Stock symbol
        start_date: Backtest start date
        end_date: Backtest end date
        n_perturbations: Number of perturbed parameter sets to test (default: 20)
        noise_pct: Maximum relative perturbation per parameter (default: ±15%)
        pass_threshold: Minimum score for a neighbor to "pass" (default: 0.25)

    Returns:
        Dictionary with robustness_score, passed, failed, scores, recommendation
    """
    from web.services.backtest_service import run_single_backtest
    from src.market.market_regime import update_regime_params, reset_regime_params
    from src.strategies.price_breakout.optimizer.objectives import calculate_score, check_constraints
    import os

    os.environ['BACKTEST_QUIET_MODE'] = '1'
    os.environ['STRATEGY_QUIET_MODE'] = '1'

    logger.info("=" * 60)
    logger.info("PARAMETER ROBUSTNESS CHECK")
    logger.info("=" * 60)
    logger.info(f"Stock: {stock_code}, Period: {start_date} ~ {end_date}")
    logger.info(f"Perturbations: {n_perturbations}, Noise: ±{noise_pct:.0%}, Pass threshold: {pass_threshold}")

    print(f"\n{'=' * 60}")
    print(f"ROBUSTNESS CHECK: {n_perturbations} perturbed parameter sets (±{noise_pct:.0%} noise)")
    print(f"{'=' * 60}")

    scores: List[float] = []
    passed = 0
    failed = 0

    random.seed(42)  # Reproducible results

    for i in range(n_perturbations):
        perturbed = _perturb_params(best_params, noise_pct)

        try:
            reset_regime_params()
            update_regime_params(perturbed)

            strategy_params = perturbed.get('strategy_params', {}).copy()
            # Remove unsupported parameters
            strategy_params.pop('enable_adaptive_thresholds', None)
            strategy_params.pop('enable_blacklist', None)

            bt_params = {
                'stock': stock_code,
                'start_date': start_date,
                'end_date': end_date,
                'strategy': 'price_breakout',
                'strategy_params': strategy_params,
            }

            result = run_single_backtest(bt_params)

            if result and check_constraints(result):
                score = calculate_score(result)
                scores.append(score)
                if score >= pass_threshold:
                    passed += 1
                    status = "PASS"
                else:
                    failed += 1
                    status = "FAIL"
            else:
                scores.append(0.0)
                failed += 1
                status = "FAIL (constraints)"

        except Exception as e:
            logger.warning(f"Perturbation {i+1} failed: {e}")
            scores.append(0.0)
            failed += 1
            status = "ERROR"

        print(f"  [{i+1:2d}/{n_perturbations}] {status}: score={scores[-1]:.4f}")

    # Compute robustness score = fraction of neighbors that pass
    robustness_score = passed / n_perturbations if n_perturbations > 0 else 0.0
    avg_neighbor_score = sum(scores) / len(scores) if scores else 0.0

    is_robust = robustness_score >= 0.5  # At least 50% of neighbors must pass

    if is_robust:
        recommendation = "ROBUST: Parameters are stable. Safe to use for live trading."
    elif robustness_score >= 0.30:
        recommendation = "MARGINAL: Parameters show some robustness but may be fragile. Use with caution."
    else:
        recommendation = "FRAGILE: Parameters are likely overfit. Consider simpler parameters or more data."

    print(f"\n[Robustness Summary]")
    print(f"  Passed:            {passed}/{n_perturbations} ({robustness_score:.1%})")
    print(f"  Avg neighbor score:{avg_neighbor_score:.4f}")
    print(f"  Robustness score:  {robustness_score:.1%}  ({'ROBUST' if is_robust else 'FRAGILE'})")
    print(f"  Recommendation:    {recommendation}")

    logger.info(f"Robustness: {passed}/{n_perturbations} passed ({robustness_score:.1%}), avg_score={avg_neighbor_score:.4f}")

    return {
        'robustness_score': robustness_score,
        'passed': passed,
        'failed': failed,
        'n_perturbations': n_perturbations,
        'avg_neighbor_score': avg_neighbor_score,
        'all_scores': scores,
        'is_robust': is_robust,
        'recommendation': recommendation,
    }
