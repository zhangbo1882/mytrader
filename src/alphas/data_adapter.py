"""
101 Formulaic Alphas - Data Adapter

Loads bars + industry classification data from the database
and structures it into panel DataFrames (dates x symbols) for alpha computation.
"""
import logging
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from sqlalchemy import create_engine
from config.settings import TUSHARE_DB_PATH

logger = logging.getLogger(__name__)


@dataclass
class AlphaDataPanel:
    """Container for panel data needed by alpha computations.

    All DataFrames have index=dates (sorted ascending), columns=symbols.
    """
    open: pd.DataFrame
    high: pd.DataFrame
    low: pd.DataFrame
    close: pd.DataFrame
    volume: pd.DataFrame
    amount: pd.DataFrame
    returns: pd.DataFrame
    vwap: pd.DataFrame
    cap: pd.DataFrame
    industry_map: Dict[str, Dict[str, str]] = field(default_factory=dict)
    _adv_cache: Dict[int, pd.DataFrame] = field(default_factory=dict, repr=False)

    def adv(self, d: int) -> pd.DataFrame:
        """Average daily dollar volume over d periods (cached)."""
        if d not in self._adv_cache:
            self._adv_cache[d] = self.amount.rolling(window=d, min_periods=d).mean()
        return self._adv_cache[d]


class AlphaDataAdapter:
    """Load data from the database and build AlphaDataPanel."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(TUSHARE_DB_PATH)
        self.engine = create_engine(f'sqlite:///{self.db_path}', echo=False)

    def load_panel(self, symbols: List[str], start_date: str, end_date: str,
                   price_type: str = '') -> AlphaDataPanel:
        """Load OHLCV data for given symbols and date range, pivot to wide format.

        Args:
            symbols: List of stock codes (e.g. ['600382', '000001'])
            start_date: Start date 'YYYY-MM-DD'
            end_date: End date 'YYYY-MM-DD'
            price_type: '' (no adjust), 'qfq' (forward), 'hfq' (backward)

        Returns:
            AlphaDataPanel with all fields populated
        """
        logger.info(f"Loading panel data: {len(symbols)} symbols, {start_date} to {end_date}")

        # Build SQL with symbol placeholders
        placeholders = ','.join([f"'{s}'" for s in symbols])
        query = f"""
        SELECT symbol, datetime, open, high, low, close,
               volume, amount, pct_chg, total_mv
        FROM bars
        WHERE symbol IN ({placeholders})
          AND interval = '1d'
          AND datetime >= :start_date
          AND datetime <= :end_date
        ORDER BY datetime, symbol
        """

        df = pd.read_sql_query(query, self.engine,
                               params={'start_date': start_date, 'end_date': end_date})

        if df.empty:
            logger.warning("No data returned from database")
            empty_df = pd.DataFrame()
            return AlphaDataPanel(
                open=empty_df, high=empty_df, low=empty_df, close=empty_df,
                volume=empty_df, amount=empty_df, returns=empty_df,
                vwap=empty_df, cap=empty_df
            )

        logger.info(f"Loaded {len(df)} rows for {df['symbol'].nunique()} symbols")

        # Pivot to wide format (dates x symbols)
        def pivot(col):
            return df.pivot(index='datetime', columns='symbol', values=col).sort_index()

        open_df = pivot('open')
        high_df = pivot('high')
        low_df = pivot('low')
        close_df = pivot('close')
        volume_df = pivot('volume')
        amount_df = pivot('amount')

        # Returns: pct_chg / 100
        pct_chg_df = pivot('pct_chg')
        returns_df = pct_chg_df / 100.0

        # VWAP: amount / volume (handle zero volume)
        vwap_df = amount_df / volume_df.replace(0, np.nan)

        # Market cap
        cap_df = pivot('total_mv')

        # Load industry classification
        industry_map = self._load_industry_map(symbols)

        panel = AlphaDataPanel(
            open=open_df,
            high=high_df,
            low=low_df,
            close=close_df,
            volume=volume_df,
            amount=amount_df,
            returns=returns_df,
            vwap=vwap_df,
            cap=cap_df,
            industry_map=industry_map,
        )

        logger.info(f"Panel built: {len(open_df)} dates x {len(open_df.columns)} symbols")
        return panel

    def _load_industry_map(self, symbols: List[str]) -> Dict[str, Dict[str, str]]:
        """Load Shenwan industry classification for symbols.

        Returns:
            Dict with keys 'L1', 'L2', 'L3', each mapping symbol -> industry_name
        """
        placeholders = ','.join([f"'{s}'" for s in symbols])
        query = f"""
        SELECT
            SUBSTR(swm.ts_code, 1, 6) as symbol,
            sc.industry_name,
            sc.level
        FROM sw_members swm
        JOIN sw_classify sc ON swm.index_code = sc.index_code
        WHERE SUBSTR(swm.ts_code, 1, 6) IN ({placeholders})
          AND (swm.out_date IS NULL OR swm.out_date = '')
        """

        try:
            df = pd.read_sql_query(query, self.engine)
        except Exception as e:
            logger.warning(f"Failed to load industry map: {e}")
            return {}

        if df.empty:
            return {}

        result = {}
        for level in ['L1', 'L2', 'L3']:
            level_df = df[df['level'] == level]
            if not level_df.empty:
                mapping = dict(zip(level_df['symbol'], level_df['industry_name']))
                result[level] = mapping

        logger.info(f"Industry map loaded: {', '.join(f'{k}={len(v)}' for k, v in result.items())}")
        return result

    def get_all_symbols(self, trade_date: str = None) -> List[str]:
        """Get all available stock symbols from the bars table.

        Args:
            trade_date: If specified, only return symbols that have data on this date.
                        If None, return symbols from the latest available date.
        """
        if trade_date is None:
            query = """
            SELECT DISTINCT symbol FROM bars
            WHERE interval = '1d'
              AND datetime = (SELECT MAX(datetime) FROM bars WHERE interval = '1d')
            ORDER BY symbol
            """
            df = pd.read_sql_query(query, self.engine)
        else:
            query = """
            SELECT DISTINCT symbol FROM bars
            WHERE interval = '1d' AND datetime = :trade_date
            ORDER BY symbol
            """
            df = pd.read_sql_query(query, self.engine, params={'trade_date': trade_date})

        return df['symbol'].tolist()
