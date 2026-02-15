"""
101 Formulaic Alphas - Alpha Engine

Orchestrator for computing alpha factors. Handles data loading, buffering,
caching, and result cleanup.
"""
import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Optional

from src.alphas.data_adapter import AlphaDataAdapter, AlphaDataPanel
from src.alphas.alpha101 import (
    ALPHA_REGISTRY, ALPHA_DESCRIPTIONS, get_alpha_func, list_all_alphas
)
from config.settings import TUSHARE_DB_PATH

logger = logging.getLogger(__name__)

# Extra lookback buffer for rolling windows (longest window ~250 days + margin)
LOOKBACK_BUFFER_DAYS = 450


class AlphaEngine:
    """Orchestrator for computing 101 Formulaic Alphas."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(TUSHARE_DB_PATH)
        self.adapter = AlphaDataAdapter(self.db_path)
        self._cached_panel: Optional[AlphaDataPanel] = None
        self._cache_key: Optional[str] = None

    def compute_alpha(self, alpha_id: int, symbols: List[str],
                      start_date: str, end_date: str,
                      price_type: str = '') -> pd.DataFrame:
        """Compute a single alpha for given symbols and date range.

        Args:
            alpha_id: Alpha number (1-101)
            symbols: List of stock codes
            start_date: Start date 'YYYY-MM-DD'
            end_date: End date 'YYYY-MM-DD'
            price_type: Price adjustment type

        Returns:
            DataFrame (dates x symbols) with alpha values
        """
        func = get_alpha_func(alpha_id)
        if func is None:
            raise ValueError(f"Alpha #{alpha_id} not found. Available: {list_all_alphas()}")

        # Load data with lookback buffer
        buffered_start = self._get_buffered_start(start_date)
        panel = self._get_panel(symbols, buffered_start, end_date, price_type)

        if panel.close.empty:
            logger.warning(f"No data available for alpha #{alpha_id}")
            return pd.DataFrame()

        # Compute alpha
        logger.info(f"Computing alpha #{alpha_id}...")
        try:
            result = func(panel)
        except Exception as e:
            logger.error(f"Error computing alpha #{alpha_id}: {e}")
            raise

        # Clean up result
        result = self._cleanup(result)

        # Trim to requested date range
        result = result.loc[result.index >= start_date]
        result = result.loc[result.index <= end_date]

        logger.info(f"Alpha #{alpha_id} computed: {result.shape}")
        return result

    def compute_alphas_batch(self, alpha_ids: List[int], symbols: List[str],
                             start_date: str, end_date: str,
                             price_type: str = '') -> Dict[int, pd.DataFrame]:
        """Compute multiple alphas for the same symbols and date range.

        Shares the loaded data panel across all alpha computations.

        Returns:
            Dict mapping alpha_id -> DataFrame
        """
        # Load data once with lookback buffer
        buffered_start = self._get_buffered_start(start_date)
        panel = self._get_panel(symbols, buffered_start, end_date, price_type)

        if panel.close.empty:
            logger.warning("No data available for batch computation")
            return {}

        results = {}
        for alpha_id in alpha_ids:
            func = get_alpha_func(alpha_id)
            if func is None:
                logger.warning(f"Alpha #{alpha_id} not found, skipping")
                continue

            try:
                result = func(panel)
                result = self._cleanup(result)
                result = result.loc[result.index >= start_date]
                result = result.loc[result.index <= end_date]
                results[alpha_id] = result
                logger.info(f"Alpha #{alpha_id} computed: {result.shape}")
            except Exception as e:
                logger.error(f"Error computing alpha #{alpha_id}: {e}")
                results[alpha_id] = pd.DataFrame()

        return results

    def get_alpha_snapshot(self, alpha_id: int, symbols: List[str],
                           trade_date: str, price_type: str = '') -> pd.Series:
        """Get cross-sectional alpha scores on a single date.

        Returns:
            Series (index=symbols) with alpha values for the given date
        """
        result = self.compute_alpha(alpha_id, symbols, trade_date, trade_date, price_type)
        if result.empty:
            return pd.Series(dtype=float)

        # Get the last row (should be the trade_date)
        if trade_date in result.index:
            return result.loc[trade_date]
        elif len(result) > 0:
            return result.iloc[-1]
        return pd.Series(dtype=float)

    def list_alphas(self) -> List[Dict]:
        """List all available alphas with metadata.

        Returns:
            List of dicts with alpha_id, name, description
        """
        alphas = []
        for alpha_id in list_all_alphas():
            alphas.append({
                'alpha_id': alpha_id,
                'name': f'Alpha#{alpha_id:03d}',
                'description': ALPHA_DESCRIPTIONS.get(alpha_id, ''),
            })
        return alphas

    def _get_panel(self, symbols: List[str], start_date: str, end_date: str,
                   price_type: str) -> AlphaDataPanel:
        """Get data panel, using cache if possible."""
        cache_key = f"{','.join(sorted(symbols))}|{start_date}|{end_date}|{price_type}"
        if self._cache_key == cache_key and self._cached_panel is not None:
            return self._cached_panel

        panel = self.adapter.load_panel(symbols, start_date, end_date, price_type)
        self._cached_panel = panel
        self._cache_key = cache_key
        return panel

    def _get_buffered_start(self, start_date: str) -> str:
        """Calculate buffered start date for rolling window lookback."""
        from datetime import datetime, timedelta
        dt = datetime.strptime(start_date, '%Y-%m-%d')
        # Add buffer for calendar days (trading days are fewer)
        buffered = dt - timedelta(days=int(LOOKBACK_BUFFER_DAYS * 1.5))
        return buffered.strftime('%Y-%m-%d')

    @staticmethod
    def _cleanup(result: pd.DataFrame) -> pd.DataFrame:
        """Replace inf/-inf with NaN in results."""
        if isinstance(result, pd.DataFrame):
            return result.replace([np.inf, -np.inf], np.nan)
        elif isinstance(result, pd.Series):
            return result.replace([np.inf, -np.inf], np.nan)
        return result
