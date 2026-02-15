"""
101 Formulaic Alphas - Operator Primitives

Time-series operators (rolling along time axis per stock) and
cross-sectional operators (across stocks on the same day).

All operators work on pd.DataFrame (index=dates, columns=symbols).
"""
import numpy as np
import pandas as pd
from typing import Dict


# ============================================================================
# Time-series operators (per-stock rolling)
# ============================================================================

def delay(x: pd.DataFrame, d: int) -> pd.DataFrame:
    """Shift x by d periods (lag)."""
    return x.shift(d)


def delta(x: pd.DataFrame, d: int) -> pd.DataFrame:
    """x - x.shift(d)"""
    return x - x.shift(d)


def ts_sum(x: pd.DataFrame, d: int) -> pd.DataFrame:
    """Rolling sum over d periods."""
    return x.rolling(window=d, min_periods=d).sum()


def ts_min(x: pd.DataFrame, d: int) -> pd.DataFrame:
    """Rolling minimum over d periods."""
    return x.rolling(window=d, min_periods=d).min()


def ts_max(x: pd.DataFrame, d: int) -> pd.DataFrame:
    """Rolling maximum over d periods."""
    return x.rolling(window=d, min_periods=d).max()


def ts_argmax(x: pd.DataFrame, d: int) -> pd.DataFrame:
    """Position of max value in rolling window of d periods.
    Returns days since the max (0 = today had max, d-1 = oldest day had max).
    """
    def _argmax(s):
        return s.rolling(window=d, min_periods=d).apply(
            lambda w: w.argmax(), raw=True
        )
    if isinstance(x, pd.DataFrame):
        return x.apply(_argmax)
    return _argmax(x)


def ts_argmin(x: pd.DataFrame, d: int) -> pd.DataFrame:
    """Position of min value in rolling window of d periods."""
    def _argmin(s):
        return s.rolling(window=d, min_periods=d).apply(
            lambda w: w.argmin(), raw=True
        )
    if isinstance(x, pd.DataFrame):
        return x.apply(_argmin)
    return _argmin(x)


def ts_rank(x: pd.DataFrame, d: int) -> pd.DataFrame:
    """Rolling percentile rank over d periods.
    Returns the fraction of values in the window <= current value.
    """
    def _ts_rank(s):
        return s.rolling(window=d, min_periods=d).apply(
            lambda w: pd.Series(w).rank(pct=True).iloc[-1], raw=True
        )
    if isinstance(x, pd.DataFrame):
        return x.apply(_ts_rank)
    return _ts_rank(x)


def product(x: pd.DataFrame, d: int) -> pd.DataFrame:
    """Rolling product over d periods."""
    return x.rolling(window=d, min_periods=d).apply(np.prod, raw=True)


def stddev(x: pd.DataFrame, d: int) -> pd.DataFrame:
    """Rolling standard deviation over d periods."""
    return x.rolling(window=d, min_periods=d).std()


def correlation(x: pd.DataFrame, y: pd.DataFrame, d: int) -> pd.DataFrame:
    """Rolling pairwise correlation between x and y over d periods."""
    return x.rolling(window=d, min_periods=d).corr(y)


def covariance(x: pd.DataFrame, y: pd.DataFrame, d: int) -> pd.DataFrame:
    """Rolling pairwise covariance between x and y over d periods."""
    return x.rolling(window=d, min_periods=d).cov(y)


def decay_linear(x: pd.DataFrame, d: int) -> pd.DataFrame:
    """Linearly weighted moving average.
    Weights: d, d-1, ..., 1 (most recent gets weight 1, oldest gets weight d).
    Actually in the paper, the most recent gets the highest weight.
    Weights: 1, 2, ..., d normalized so they sum to 1.
    """
    weights = np.arange(1, d + 1, dtype=float)
    weights = weights / weights.sum()

    def _decay(s):
        return s.rolling(window=d, min_periods=d).apply(
            lambda w: np.dot(w, weights), raw=True
        )
    if isinstance(x, pd.DataFrame):
        return x.apply(_decay)
    return _decay(x)


def sum_ts(x: pd.DataFrame, d: int) -> pd.DataFrame:
    """Alias for ts_sum."""
    return ts_sum(x, d)


def min_ts(x: pd.DataFrame, d: int) -> pd.DataFrame:
    """Alias for ts_min."""
    return ts_min(x, d)


def max_ts(x: pd.DataFrame, d: int) -> pd.DataFrame:
    """Alias for ts_max."""
    return ts_max(x, d)


# ============================================================================
# Cross-sectional operators (across stocks on the same day)
# ============================================================================

def rank(x: pd.DataFrame) -> pd.DataFrame:
    """Cross-sectional percentile rank (across all stocks on each day).
    Returns values in [0, 1].
    """
    if isinstance(x, pd.DataFrame):
        return x.rank(axis=1, pct=True)
    elif isinstance(x, pd.Series):
        return x.rank(pct=True)
    return x


def scale(x: pd.DataFrame, a: float = 1.0) -> pd.DataFrame:
    """Rescale so sum(abs(x)) = a per row."""
    if isinstance(x, pd.DataFrame):
        abs_sum = x.abs().sum(axis=1)
        abs_sum = abs_sum.replace(0, np.nan)
        return x.div(abs_sum, axis=0) * a
    elif isinstance(x, pd.Series):
        abs_sum = x.abs().sum()
        if abs_sum == 0:
            return x
        return x / abs_sum * a
    return x


def indneutralize(x: pd.DataFrame, industry_map: Dict[str, Dict[str, str]],
                   level: str = 'sector') -> pd.DataFrame:
    """Industry-neutralize: demean x within industry groups.

    Args:
        x: DataFrame (dates x symbols)
        industry_map: Dict mapping level -> {symbol: industry_name}
        level: 'sector' (L1), 'industry' (L2), or 'subindustry' (L3)
    """
    level_key = {
        'sector': 'L1',
        'industry': 'L2',
        'subindustry': 'L3'
    }.get(level, level)

    if level_key not in industry_map:
        return x

    group_map = industry_map[level_key]
    symbols = x.columns.tolist()

    # Build group series for symbols
    groups = pd.Series({s: group_map.get(s, 'unknown') for s in symbols})

    result = x.copy()
    for _, group_symbols in groups.groupby(groups):
        cols = [s for s in group_symbols.index if s in x.columns]
        if len(cols) > 1:
            group_mean = x[cols].mean(axis=1)
            for col in cols:
                result[col] = x[col] - group_mean

    return result


# ============================================================================
# Standard operators
# ============================================================================

def signedpower(x, a: float):
    """sign(x) * abs(x)^a"""
    if isinstance(x, (pd.DataFrame, pd.Series)):
        return np.sign(x) * np.abs(x) ** a
    return np.sign(x) * abs(x) ** a


def log(x):
    """Natural logarithm."""
    if isinstance(x, (pd.DataFrame, pd.Series)):
        return np.log(x.replace(0, np.nan))
    return np.log(x) if x > 0 else np.nan


def sign(x):
    """Sign function."""
    if isinstance(x, (pd.DataFrame, pd.Series)):
        return np.sign(x)
    return np.sign(x)


def abs_val(x):
    """Absolute value."""
    if isinstance(x, (pd.DataFrame, pd.Series)):
        return x.abs()
    return abs(x)


def adv(amount: pd.DataFrame, d: int) -> pd.DataFrame:
    """Average daily dollar volume over d periods."""
    return amount.rolling(window=d, min_periods=d).mean()
