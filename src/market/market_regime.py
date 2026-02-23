"""
Market Regime Definitions

Defines the market state enumeration used by the stock state detector.
Provides thread/process-safe parameter management for optimization.
Default parameters are loaded from database (regime_params.db).
"""

from enum import Enum
import threading
import copy
from typing import Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


# Global lock for thread-safe parameter access
_param_lock = threading.Lock()

# Default configuration constants (immutable reference)
_DEFAULT_MARKET_STATE_CONFIG = {
    'lookback_days': 250,          # Lookback period (trading days)
    'ma_short': 20,                # Short-term MA
    'ma_medium': 60,               # Medium-term MA
    'ma_long': 120,                # Long-term MA
    'roc_period': 20,              # ROC period
    'roc_dynamic_multiplier': 1.5, # ROC dynamic threshold multiplier
    'bull_threshold': 70,          # Bull market total score threshold
    'bear_threshold': 40,          # Bear market total score threshold
    'volatility_percentile_low': 40,   # Low volatility percentile
    'volatility_percentile_high': 80,  # High volatility percentile
}

# Default regime params (immutable reference)
# These values match the database default_strategy_params for price_breakout_v2_default
_DEFAULT_REGIME_PARAMS = {
    'bull': {
        'buy_threshold_multiplier': 0.7,      # 0.7x base: trend following (buy on smaller dips)
        'sell_threshold_multiplier': 1.5,     # 1.5x base: hold for bigger gains
        'stop_loss_multiplier': 0.9,          # 0.9x base: wider stop (allow pullbacks)
    },
    'bear': {
        'buy_threshold_multiplier': 1.4,      # 1.4x base: defensive (require larger dips)
        'sell_threshold_multiplier': 0.7,     # 0.7x base: quick profits (dead cat bounce)
        'stop_loss_multiplier': 0.7,          # 0.7x base: tight stop (bear rallies fail)
    },
    'neutral': {
        'buy_threshold_multiplier': 1.0,      # 1.0x base: use base as-is
        'sell_threshold_multiplier': 1.0,     # 1.0x base: use base as-is
        'stop_loss_multiplier': 1.0,          # 1.0x base: use base as-is
    }
}

# Current active parameters (mutable copies)
_current_market_config = copy.deepcopy(_DEFAULT_MARKET_STATE_CONFIG)
_current_regime_params = copy.deepcopy(_DEFAULT_REGIME_PARAMS)


# Auto-load default parameters from database on module import
# This ensures the strategy uses the latest defaults from database
try:
    load_regime_params_from_db()
except Exception as e:
    # If loading fails, use code defaults (already set above)
    pass


class MarketRegime(Enum):
    """Market regime enumeration"""
    BULL = "bull"       # Strong upward trend
    BEAR = "bear"       # Strong downward trend
    NEUTRAL = "neutral" # Sideways/consolidation


class MarketState:
    """
    Market state data class

    Contains the detected market regime along with confidence scores
    and individual component scores.
    """

    def __init__(
        self,
        regime: MarketRegime,
        confidence: float,
        ma_trend: float,
        momentum: float,
        position_ratio: float,
        volume_confirm: float,
        volatility: float
    ):
        self.regime = regime
        self.confidence = confidence
        self.ma_trend = ma_trend
        self.momentum = momentum
        self.position_ratio = position_ratio
        self.volume_confirm = volume_confirm
        self.volatility = volatility

    def __repr__(self):
        return (
            f"MarketState(regime={self.regime.value}, "
            f"confidence={self.confidence:.2f}, "
            f"ma_trend={self.ma_trend}, "
            f"momentum={self.momentum}, "
            f"position_ratio={self.position_ratio:.2f}, "
            f"volume_confirm={self.volume_confirm}, "
            f"volatility={self.volatility})"
        )

    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            "regime": self.regime.value,
            "confidence": self.confidence,
            "ma_trend": self.ma_trend,
            "momentum": self.momentum,
            "position_ratio": self.position_ratio,
            "volume_confirm": self.volume_confirm,
            "volatility": self.volatility
        }


# Backwards compatibility exports
MARKET_STATE_CONFIG = _DEFAULT_MARKET_STATE_CONFIG
REGIME_PARAMS = _DEFAULT_REGIME_PARAMS


def get_market_config() -> dict:
    """
    Get current market configuration (thread-safe).

    Returns:
        Copy of current market configuration
    """
    with _param_lock:
        return copy.deepcopy(_current_market_config)


def get_regime_params() -> dict:
    """
    Get current regime parameters (thread-safe).

    Returns:
        Copy of current regime parameters
    """
    with _param_lock:
        return copy.deepcopy(_current_regime_params)


def update_regime_params(params: dict):
    """
    Update regime parameters and market config (thread-safe).

    Args:
        params: Dictionary with optional keys:
            - 'market_config': Dict with market state threshold config
            - 'regime_params': Dict with bull/bear/neutral multipliers
    """
    with _param_lock:
        global _current_market_config, _current_regime_params

        if 'market_config' in params:
            _current_market_config.update(params['market_config'])

        if 'regime_params' in params:
            for regime, config in params['regime_params'].items():
                if regime in _current_regime_params:
                    _current_regime_params[regime].update(config)


def reset_regime_params():
    """Reset all parameters to default values (thread-safe)."""
    with _param_lock:
        global _current_market_config, _current_regime_params
        _current_market_config = copy.deepcopy(_DEFAULT_MARKET_STATE_CONFIG)
        _current_regime_params = copy.deepcopy(_DEFAULT_REGIME_PARAMS)


def load_regime_params_from_db(strategy_name: str = 'price_breakout_v2_default',
                                db_path: str = 'data/regime_params.db') -> bool:
    """
    Load default regime parameters from database.

    This function updates the global regime parameters with values stored
    in the database. Useful for keeping strategy parameters in sync with
    optimized defaults.

    Args:
        strategy_name: Name of the strategy in database
        db_path: Path to regime_params.db

    Returns:
        True if parameters were loaded successfully, False otherwise
    """
    try:
        import sqlite3
        from pathlib import Path

        db_file = Path(db_path)
        if not db_file.exists():
            logger.warning(f"Database not found: {db_path}, using code defaults")
            return False

        conn = sqlite3.connect(db_path)
        cursor = conn.execute("""
            SELECT bull_buy_multiplier, bull_sell_multiplier, bull_stop_multiplier,
                   bear_buy_multiplier, bear_sell_multiplier, bear_stop_multiplier,
                   bull_threshold, bear_threshold
            FROM default_strategy_params
            WHERE strategy_name = ?
        """, (strategy_name,))

        row = cursor.fetchone()
        conn.close()

        if not row:
            logger.warning(f"Strategy '{strategy_name}' not found in database")
            return False

        # Update regime params with database values
        db_params = {
            'bull': {
                'buy_threshold_multiplier': row[0],
                'sell_threshold_multiplier': row[1],
                'stop_loss_multiplier': row[2],
            },
            'bear': {
                'buy_threshold_multiplier': row[3],
                'sell_threshold_multiplier': row[4],
                'stop_loss_multiplier': row[5],
            },
            'neutral': {
                'buy_threshold_multiplier': 1.0,
                'sell_threshold_multiplier': 1.0,
                'stop_loss_multiplier': 1.0,
            }
        }

        # Update market config thresholds
        market_config = {
            'bull_threshold': row[6],
            'bear_threshold': row[7],
        }

        # Apply updates
        update_regime_params({
            'regime_params': db_params,
            'market_config': market_config
        })

        logger.info(f"✅ Loaded regime params from database for '{strategy_name}'")
        logger.info(f"   Bull: buy×{row[0]:.1f} sell×{row[1]:.1f} stop×{row[2]:.1f}")
        logger.info(f"   Bear: buy×{row[3]:.1f} sell×{row[4]:.1f} stop×{row[5]:.1f}")
        logger.info(f"   Thresholds: Bull>={row[6]}, Bear<={row[7]}")

        return True

    except Exception as e:
        logger.error(f"Error loading regime params from database: {e}")
        return False


# Order execution configuration
ORDER_EXECUTION_CONFIG = {
    'buy_price_buffer': 0.003,      # Buy price buffer 0.3%
    'sell_price_buffer': 0.002,     # Sell price buffer 0.2%
    'use_market_for_stop': True,    # Use market orders for stop loss
    'slippage_buy': 0.002,          # Buy slippage 0.2%
    'slippage_sell': 0.002,         # Sell slippage 0.2%
}


# Fundamental filter configuration
FUNDAMENTAL_FILTER_CONFIG = {
    'enable_fundamental_filter': False,  # Default off (needs data support)
    'lookahead_days': 5,  # Check next 5 trading days
}
