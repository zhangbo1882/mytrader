"""
Objective functions and constraints for optimization.

Module-level functions for pickle compatibility with multiprocessing.
"""

from typing import Dict, Any, Optional


def check_constraints(result: Optional[Dict[str, Any]], max_drawdown_limit: float = -0.20,
                      min_trades: int = 1, min_sharpe: float = 0.3,
                      verbose: bool = False) -> tuple[bool, list[str]]:
    """
    Check if backtest result meets hard constraints.

    Kept minimal: only reject truly degenerate results.
    - Drawdown and Sharpe are the meaningful hard gates.
    - Trade count minimum is 1 (just needs at least one trade).
    - Statistical quality is captured by Sharpe, not trade count.

    Args:
        result: Backtest result dictionary
        max_drawdown_limit: Maximum acceptable drawdown (default: -20%)
        min_trades: Minimum number of trades (default: 1)
        min_sharpe: Minimum Sharpe ratio (default: 0.3)
        verbose: If True, return failure reasons

    Returns:
        Tuple of (passed, failure_reasons)
    """
    if result is None:
        return False, ["No result"]

    try:
        max_dd = result.get('health_metrics', {}).get('max_drawdown', -999)
        trades = result.get('trade_stats', {}).get('total_trades', 0)
        sharpe = result.get('health_metrics', {}).get('sharpe_ratio', -999)

        failure_reasons = []

        # Check each constraint
        if max_dd < max_drawdown_limit:
            failure_reasons.append(f"MaxDD {max_dd*100:.2f}% < {max_drawdown_limit*100:.0f}%")

        if trades < min_trades:
            failure_reasons.append(f"Trades {trades} < {min_trades}")

        if sharpe <= min_sharpe:
            failure_reasons.append(f"Sharpe {sharpe:.2f} ≤ {min_sharpe}")

        passed = len(failure_reasons) == 0

        if verbose and not passed:
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"[Constraint Failed] {', '.join(failure_reasons)}")

        return passed, failure_reasons

    except (KeyError, TypeError) as e:
        return False, [f"Error: {e}"]


def calculate_score(result: Optional[Dict[str, Any]], mode: str = 'balanced') -> float:
    """
    Calculate composite score for backtest result.

    Args:
        result: Backtest result dictionary
        mode: Scoring mode
            - 'balanced': 35% return, 35% drawdown, 30% sharpe [default]
            - 'return_first': 50% return, 30% drawdown, 20% sharpe
            - 'robust': 25% return, 45% drawdown, 30% sharpe

    Returns:
        Composite score between 0 and 1
    """
    if result is None:
        return 0.0

    try:
        sharpe = result.get('health_metrics', {}).get('sharpe_ratio', 0)
        drawdown = abs(result.get('health_metrics', {}).get('max_drawdown', 0))
        total_return = result.get('basic_info', {}).get('total_return', 0)
        win_rate = result.get('trade_stats', {}).get('win_rate', 0)

        # Normalize each component to [0, 1]
        # No cap on return: higher is always better.
        # Use log scale so 200% isn't infinitely better than 100%.
        import math
        if total_return > 0:
            # log(1 + r) / log(1 + target), target = 100% annual equivalent
            return_score = min(math.log1p(total_return) / math.log1p(1.0), 1)
        else:
            return_score = 0.0

        drawdown_score = 1 - min(abs(drawdown) / 0.30, 1)   # 30% DD → 0 score
        sharpe_score   = min(max(sharpe / 2.0, 0), 1)        # Sharpe 2.0 → full score
        win_rate_score = win_rate                             # already in [0, 1]

        if mode == 'return_first':
            weights = {'return': 0.50, 'drawdown': 0.25, 'sharpe': 0.25}
        elif mode == 'balanced':
            weights = {'return': 0.35, 'drawdown': 0.35, 'sharpe': 0.30}
        elif mode == 'robust':
            weights = {'return': 0.20, 'drawdown': 0.50, 'sharpe': 0.30}
        else:
            weights = {'return': 0.35, 'drawdown': 0.35, 'sharpe': 0.30}

        base_score = (
            weights['return']   * return_score +
            weights['drawdown'] * drawdown_score +
            weights['sharpe']   * sharpe_score
        )

        # Mild win rate penalty only when win rate is very low (<30%)
        if win_rate < 0.3:
            base_score *= (0.7 + 0.3 * (win_rate / 0.3))

        return float(base_score)

    except (KeyError, TypeError, ZeroDivisionError):
        return 0.0


def format_result_summary(result: Dict[str, Any]) -> str:
    """Format backtest result for logging."""
    try:
        basic  = result.get('basic_info', {})
        health = result.get('health_metrics', {})
        trades = result.get('trade_stats', {})

        return (
            f"Return: {basic.get('total_return', 0):.2%} | "
            f"Sharpe: {health.get('sharpe_ratio', 0):.2f} | "
            f"MaxDD: {health.get('max_drawdown', 0):.2%} | "
            f"WinRate: {trades.get('win_rate', 0):.2%} | "
            f"Trades: {trades.get('total_trades', 0)}"
        )
    except (KeyError, TypeError):
        return "Invalid result format"
