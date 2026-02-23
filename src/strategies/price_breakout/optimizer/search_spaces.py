"""
Search space definitions for REGIME_PARAMS optimization.

Defines coarse and fine search grids for hierarchical optimization.
"""

import itertools
from typing import Dict, List, Any, Optional


# Load default parameters from database
_DEFAULT_PARAMS_CACHE = None


def get_default_regime_params(strategy_name: str = 'price_breakout_v2_default'):
    """
    Get default regime parameters from database.

    Args:
        strategy_name: Strategy identifier in database

    Returns:
        Dictionary with default multipliers or None if not found
    """
    global _DEFAULT_PARAMS_CACHE

    if _DEFAULT_PARAMS_CACHE is None:
        try:
            from src.strategies.price_breakout.optimizer.regime_params_db import RegimeParamsDB
            db = RegimeParamsDB()
            params = db.get_default_strategy_params(strategy_name)
            db.close()

            if params:
                _DEFAULT_PARAMS_CACHE = {
                    'bull_buy': params['bull_buy_multiplier'],
                    'bull_sell': params['bull_sell_multiplier'],
                    'bull_stop': params['bull_stop_multiplier'],
                    'bear_buy': params['bear_buy_multiplier'],
                    'bear_sell': params['bear_sell_multiplier'],
                    'bear_stop': params['bear_stop_multiplier'],
                    'bull_threshold': params['bull_threshold'],
                    'bear_threshold': params['bear_threshold'],
                }
                print(f"✅ Loaded default regime params from database for '{strategy_name}'")
            else:
                # Fallback to hardcoded defaults if database not initialized
                _DEFAULT_PARAMS_CACHE = {
                    'bull_buy': 0.7,
                    'bull_sell': 1.5,
                    'bull_stop': 0.9,
                    'bear_buy': 1.4,
                    'bear_sell': 0.7,
                    'bear_stop': 0.7,
                    'bull_threshold': 70,
                    'bear_threshold': 40,
                }
                print(f"⚠️  Using hardcoded default regime params (database not found)")
        except Exception as e:
            # Fallback on any error
            _DEFAULT_PARAMS_CACHE = {
                'bull_buy': 0.7,
                'bull_sell': 1.5,
                'bull_stop': 0.9,
                'bear_buy': 1.4,
                'bear_sell': 0.7,
                'bear_stop': 0.7,
                'bull_threshold': 70,
                'bear_threshold': 40,
            }
            print(f"⚠️  Error loading from database, using defaults: {e}")

    return _DEFAULT_PARAMS_CACHE


def generate_coarse_search_space() -> List[Dict[str, Any]]:
    """
    Generate coarse search space (Phase 1).

    Fast exploration with larger step sizes to find promising regions.
    Base thresholds are fixed to defaults during coarse search.
    Neutral market uses fixed multipliers (1.0) to reduce search space.

    Returns:
        List of parameter dictionaries with market_config, regime_params, and strategy_params
    """
    # Market state thresholds - REDUCED from 3 to 2 values each
    bull_thresholds = [65, 75]
    bear_thresholds = [35, 45]

    # Bull market multipliers - REDUCED from 3 to 2 values each
    bull_buy_multipliers = [0.4, 0.6]
    bull_sell_multipliers = [1.4, 1.8]
    bull_stop_multipliers = [0.7, 0.9]

    # Bear market multipliers - REDUCED from 3 to 2 values each
    bear_buy_multipliers = [1.5, 1.9]
    bear_sell_multipliers = [0.6, 0.8]
    bear_stop_multipliers = [0.6, 0.8]

    # Neutral market multipliers - FIXED to 1.0 (uses base thresholds directly)
    # This reduces search space significantly since neutral market should use base values
    neutral_buy_multiplier = 1.0
    neutral_sell_multiplier = 1.0
    neutral_stop_multiplier = 1.0

    # Base thresholds - NOW INCLUDED in coarse search (2 values each)
    # This allows strategy to adapt to different stock characteristics
    base_buy_thresholds = [1.0, 3.0]      # 1% or 3% buy threshold
    base_sell_thresholds = [5.0, 8.0]     # 5% or 8% sell threshold
    base_stop_loss_thresholds = [10.0]    # Fixed (less critical)

    # Generate all combinations
    combinations = list(itertools.product(
        bull_thresholds, bear_thresholds,
        bull_buy_multipliers, bull_sell_multipliers, bull_stop_multipliers,
        bear_buy_multipliers, bear_sell_multipliers, bear_stop_multipliers,
        base_buy_thresholds, base_sell_thresholds, base_stop_loss_thresholds
        # Note: neutral multipliers are NOT in product (fixed to 1.0)
    ))
    # Total: 2^8 × 2 × 2 × 1 = 1,024 combinations (still much less than 6,561)

    print(f"[Coarse Search] Total combinations: {len(combinations):,} (reduced for anti-overfitting)")

    # Convert to parameter dictionaries
    param_list = []
    for combo in combinations:
        (bull_th, bear_th,
         bull_buy, bull_sell, bull_stop,
         bear_buy, bear_sell, bear_stop,
         base_buy, base_sell, base_stop) = combo

        params = {
            'market_config': {
                'bull_threshold': bull_th,
                'bear_threshold': bear_th,
            },
            'regime_params': {
                'bull': {
                    'buy_threshold_multiplier': bull_buy,
                    'sell_threshold_multiplier': bull_sell,
                    'stop_loss_multiplier': bull_stop,
                },
                'bear': {
                    'buy_threshold_multiplier': bear_buy,
                    'sell_threshold_multiplier': bear_sell,
                    'stop_loss_multiplier': bear_stop,
                },
                'neutral': {
                    'buy_threshold_multiplier': neutral_buy_multiplier,    # Fixed 1.0
                    'sell_threshold_multiplier': neutral_sell_multiplier,  # Fixed 1.0
                    'stop_loss_multiplier': neutral_stop_multiplier,       # Fixed 1.0
                }
            },
            # Base thresholds - NOW varied in coarse search
            'strategy_params': {
                'base_buy_threshold': base_buy,
                'base_sell_threshold': base_sell,
                'base_stop_loss_threshold': base_stop,
            }
        }
        param_list.append(params)

    return param_list


def generate_fine_search_space(best_coarse_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Generate fine search space around best coarse parameters (Phase 2).

    Performs local optimization with smaller step sizes.
    Neutral market multipliers remain fixed at 1.0.

    Args:
        best_coarse_params: Best parameters from coarse search phase

    Returns:
        List of parameter dictionaries with market_config, regime_params, and strategy_params
    """
    # Keep market config fixed from coarse search
    market_config = best_coarse_params.get('market_config', {
        'bull_threshold': 70,
        'bear_threshold': 40,
    })

    # Extract coarse regime params
    coarse_regime = best_coarse_params.get('regime_params', {})
    coarse_strategy = best_coarse_params.get('strategy_params', {})

    # Helper to generate fine grid around a value (for multipliers)
    # INCREASED step from 0.1 to 0.2 to reduce search space
    def fine_grid(value: float, step: float = 0.2) -> List[float]:
        """Generate [value-step, value, value+step] with bounds checking"""
        values = [value - step, value, value + step]
        # Clamp to reasonable ranges
        return [max(0.1, min(3.0, v)) for v in values]

    # Helper to generate fine grid for base thresholds (NOW use 3 values)
    # Buy threshold: step 0.5%, Sell threshold: step 1.0%
    def fine_grid_base(value: float, step: float, min_val: float, max_val: float) -> List[float]:
        """Generate [value-step, value, value+step] with bounds checking for base thresholds"""
        values = [value - step, value, value + step]
        # Clamp to reasonable ranges
        return [max(min_val, min(max_val, v)) for v in values]

    # Bull market fine grid
    bull_buy = fine_grid(coarse_regime.get('bull', {}).get('buy_threshold_multiplier', 0.5))
    bull_sell = fine_grid(coarse_regime.get('bull', {}).get('sell_threshold_multiplier', 1.6))
    bull_stop = fine_grid(coarse_regime.get('bull', {}).get('stop_loss_multiplier', 0.8))

    # Bear market fine grid
    bear_buy = fine_grid(coarse_regime.get('bear', {}).get('buy_threshold_multiplier', 1.7))
    bear_sell = fine_grid(coarse_regime.get('bear', {}).get('sell_threshold_multiplier', 0.7))
    bear_stop = fine_grid(coarse_regime.get('bear', {}).get('stop_loss_multiplier', 0.7))

    # Neutral market - FIXED to 1.0 (uses base thresholds directly)
    neutral_buy = [1.0]
    neutral_sell = [1.0]
    neutral_stop = [1.0]

    # Base thresholds - NOW fine-tuned in fine search (3 values each)
    coarse_base_buy = coarse_strategy.get('base_buy_threshold', 1.0)
    coarse_base_sell = coarse_strategy.get('base_sell_threshold', 5.0)
    coarse_base_stop = coarse_strategy.get('base_stop_loss_threshold', 10.0)

    base_buy = fine_grid_base(coarse_base_buy, step=0.5, min_val=0.5, max_val=5.0)   # 0.5% - 5.0%
    base_sell = fine_grid_base(coarse_base_sell, step=1.0, min_val=3.0, max_val=15.0)  # 3.0% - 15.0%
    base_stop = [coarse_base_stop]  # Fixed (less critical)

    # Generate all combinations
    # Now 6 regime parameters (bull/bear: buy, sell, stop) + 2 base thresholds (buy, sell)
    # 3^6 × 3 × 3 × 1 = 2,187 combinations (increased but still reasonable)
    combinations = list(itertools.product(
        bull_buy, bull_sell, bull_stop,
        bear_buy, bear_sell, bear_stop,
        base_buy, base_sell, base_stop
    ))

    print(f"[Fine Search] Total combinations: {len(combinations):,} (base thresholds NOW included)")

    # Convert to parameter dictionaries
    param_list = []
    for combo in combinations:
        (bull_buy_v, bull_sell_v, bull_stop_v,
         bear_buy_v, bear_sell_v, bear_stop_v,
         base_buy_v, base_sell_v, base_stop_v) = combo

        params = {
            'market_config': market_config.copy(),
            'regime_params': {
                'bull': {
                    'buy_threshold_multiplier': bull_buy_v,
                    'sell_threshold_multiplier': bull_sell_v,
                    'stop_loss_multiplier': bull_stop_v,
                },
                'bear': {
                    'buy_threshold_multiplier': bear_buy_v,
                    'sell_threshold_multiplier': bear_sell_v,
                    'stop_loss_multiplier': bear_stop_v,
                },
                'neutral': {
                    'buy_threshold_multiplier': 1.0,    # Fixed
                    'sell_threshold_multiplier': 1.0,   # Fixed
                    'stop_loss_multiplier': 1.0,        # Fixed
                }
            },
            'strategy_params': {
                'base_buy_threshold': base_buy_v,
                'base_sell_threshold': base_sell_v,
                'base_stop_loss_threshold': base_stop_v,  # NOW fine-tuned (except stop)
            }
        }
        param_list.append(params)

    return param_list


def generate_hybrid_search_space(
    bull_buy_multiplier: float = None,
    bull_sell_multiplier: float = None,
    bull_stop_multiplier: float = None,
    bear_buy_multiplier: float = None,
    bear_sell_multiplier: float = None,
    bear_stop_multiplier: float = None,
) -> List[Dict[str, Any]]:
    """
    Generate hybrid search space: fixed regime multipliers + optimized base thresholds.

    This is the recommended approach to avoid overfitting:
    - Regime multipliers (bull/bear) are FIXED based on market logic, NOT optimized
    - Only 3 base parameters are searched: buy/sell/stop_loss threshold
    - enable_adaptive_thresholds=True: regime detection still runs at runtime

    Default multipliers are loaded from database (price_breakout_v2_default).
    Economic rationale for defaults:
      Bull market:
        buy_multiplier=0.7   → Enter on smaller dips (0.7x), trend is your friend
        sell_multiplier=1.5  → Hold for bigger gains (1.5x), ride the momentum
        stop_multiplier=0.9  → Slightly wider stop (0.9x), allow normal pullbacks

      Bear market:
        buy_multiplier=1.4   → Require larger dip (1.4x) before buying, avoid catching falling knives
        sell_multiplier=0.7  → Take profit quickly (0.7x), dead-cat-bounce territory
        stop_multiplier=0.7  → Tight stop (0.7x), bear rallies fail fast

    Total combinations: 6 × 5 × 4 = 120  (vs 1024 coarse / 2187 fine)
    Parameter/sample ratio: 3 params / ~90 trades ≈ 0.033  (healthy)

    Args:
        bull_*_multiplier: Fixed multipliers for bull market (None = use database default)
        bear_*_multiplier: Fixed multipliers for bear market (None = use database default)

    Returns:
        List of parameter dictionaries
    """
    # Load default parameters from database if not specified
    defaults = get_default_regime_params()

    bull_buy_multiplier = bull_buy_multiplier if bull_buy_multiplier is not None else defaults['bull_buy']
    bull_sell_multiplier = bull_sell_multiplier if bull_sell_multiplier is not None else defaults['bull_sell']
    bull_stop_multiplier = bull_stop_multiplier if bull_stop_multiplier is not None else defaults['bull_stop']
    bear_buy_multiplier = bear_buy_multiplier if bear_buy_multiplier is not None else defaults['bear_buy']
    bear_sell_multiplier = bear_sell_multiplier if bear_sell_multiplier is not None else defaults['bear_sell']
    bear_stop_multiplier = bear_stop_multiplier if bear_stop_multiplier is not None else defaults['bear_stop']
    bull_threshold = defaults['bull_threshold']
    bear_threshold = defaults['bear_threshold']
    """
    Generate hybrid search space: fixed regime multipliers + optimized base thresholds.

    This is the recommended approach to avoid overfitting:
    - Regime multipliers (bull/bear) are FIXED based on market logic, NOT optimized
    - Only 3 base parameters are searched: buy/sell/stop_loss threshold
    - enable_adaptive_thresholds=True: regime detection still runs at runtime

    Economic rationale for defaults:
      Bull market:
        buy_multiplier=0.7   → Enter on smaller dips (0.7x), trend is your friend
        sell_multiplier=1.5  → Hold for bigger gains (1.5x), ride the momentum
        stop_multiplier=0.9  → Slightly wider stop (0.9x), allow normal pullbacks

      Bear market:
        buy_multiplier=1.4   → Require larger dip (1.4x) before buying, avoid catching falling knives
        sell_multiplier=0.7  → Take profit quickly (0.7x), dead-cat-bounce territory
        stop_multiplier=0.7  → Tight stop (0.7x), bear rallies fail fast

    Total combinations: 6 × 5 × 4 = 120  (vs 1024 coarse / 2187 fine)
    Parameter/sample ratio: 3 params / ~90 trades ≈ 0.033  (healthy)

    Args:
        bull_*_multiplier: Fixed multipliers for bull market regime
        bear_*_multiplier: Fixed multipliers for bear market regime

    Returns:
        List of parameter dictionaries
    """
    base_buy_thresholds      = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]   # 6 values
    base_sell_thresholds     = [3.0, 5.0, 7.0, 9.0, 12.0, 15.0]         # 6 values
    base_stop_loss_thresholds = [10.0, 15.0, 20.0, 25]            # 4 values


    combinations = list(itertools.product(
        base_buy_thresholds,
        base_sell_thresholds,
        base_stop_loss_thresholds,
    ))

    print(
        f"[Hybrid Search] Total combinations: {len(combinations)} "
        f"(fixed regime multipliers, optimized base thresholds only)"
    )
    print(
        f"[Hybrid Search] Bull: buy×{bull_buy_multiplier} sell×{bull_sell_multiplier} stop×{bull_stop_multiplier} | "
        f"Bear: buy×{bear_buy_multiplier} sell×{bear_sell_multiplier} stop×{bear_stop_multiplier}"
    )

    param_list = []
    for base_buy, base_sell, base_stop in combinations:
        params = {
            'market_config': {
                'bull_threshold': 70,
                'bear_threshold': 40,
            },
            'regime_params': {
                'bull': {
                    'buy_threshold_multiplier':  bull_buy_multiplier,
                    'sell_threshold_multiplier': bull_sell_multiplier,
                    'stop_loss_multiplier':      bull_stop_multiplier,
                },
                'bear': {
                    'buy_threshold_multiplier':  bear_buy_multiplier,
                    'sell_threshold_multiplier': bear_sell_multiplier,
                    'stop_loss_multiplier':      bear_stop_multiplier,
                },
                'neutral': {
                    'buy_threshold_multiplier':  1.0,
                    'sell_threshold_multiplier': 1.0,
                    'stop_loss_multiplier':      1.0,
                },
            },
            'strategy_params': {
                'base_buy_threshold':       base_buy,
                'base_sell_threshold':      base_sell,
                'base_stop_loss_threshold': base_stop,
                'enable_adaptive_thresholds': True,   # Keep regime detection active
            }
        }
        param_list.append(params)

    return param_list


def generate_simple_search_space() -> List[Dict[str, Any]]:
    """
    Generate a simplified search space with only 3 base parameters (Phase 0).

    Designed to prevent overfitting by drastically reducing the parameter space.
    - Only 3 parameters: base_buy_threshold, base_sell_threshold, base_stop_loss_threshold
    - All regime multipliers are fixed to 1.0
    - enable_adaptive_thresholds is disabled

    Use this as the first optimization step before adding regime complexity.
    Total combinations: 5 × 4 × 3 = 60

    Returns:
        List of parameter dictionaries
    """
    base_buy_thresholds = [0.5, 1.0, 1.5, 2.0, 2.5]    # 5 values
    base_sell_thresholds = [3.0, 5.0, 7.0, 9.0]          # 4 values
    base_stop_loss_thresholds = [7.0, 10.0, 13.0]         # 3 values

    combinations = list(itertools.product(
        base_buy_thresholds,
        base_sell_thresholds,
        base_stop_loss_thresholds,
    ))

    print(f"[Simple Search] Total combinations: {len(combinations)} (anti-overfitting: base params only)")

    param_list = []
    for base_buy, base_sell, base_stop in combinations:
        params = {
            'market_config': {
                'bull_threshold': 70,
                'bear_threshold': 40,
            },
            'regime_params': {
                'bull':    {'buy_threshold_multiplier': 1.0, 'sell_threshold_multiplier': 1.0, 'stop_loss_multiplier': 1.0},
                'bear':    {'buy_threshold_multiplier': 1.0, 'sell_threshold_multiplier': 1.0, 'stop_loss_multiplier': 1.0},
                'neutral': {'buy_threshold_multiplier': 1.0, 'sell_threshold_multiplier': 1.0, 'stop_loss_multiplier': 1.0},
            },
            'strategy_params': {
                'base_buy_threshold': base_buy,
                'base_sell_threshold': base_sell,
                'base_stop_loss_threshold': base_stop,
                'enable_adaptive_thresholds': False,  # Disable regime adaptation
            }
        }
        param_list.append(params)

    return param_list


def estimate_search_time(n_combinations: int, n_workers: int = 6,
                         seconds_per_backtest: float = 4.0) -> Dict[str, float]:
    """
    Estimate optimization time for given search space.

    Args:
        n_combinations: Number of parameter combinations
        n_workers: Number of parallel workers
        seconds_per_backtest: Average time per backtest

    Returns:
        Dictionary with time estimates
    """
    total_seconds = n_combinations * seconds_per_backtest
    parallel_seconds = total_seconds / n_workers

    return {
        'combinations': n_combinations,
        'total_seconds': total_seconds,
        'parallel_seconds': parallel_seconds,
        'total_hours': total_seconds / 3600,
        'parallel_hours': parallel_seconds / 3600,
    }
