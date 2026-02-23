"""
101 Formulaic Alphas - Data Adapter

Loads bars + industry classification data from the database
and structures it into panel DataFrames (dates x symbols) for alpha computation.

Bars data is loaded from DuckDB, industry classification from SQLite.
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
    """Load data from the database and build AlphaDataPanel.

    A-share bars data is loaded from DuckDB bars_a_1d table,
    industry classification from SQLite.
    """

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
        from src.db.duckdb_manager import get_duckdb_manager

        logger.info(f"Loading panel data: {len(symbols)} symbols, {start_date} to {end_date}")

        # A股专用表名
        a_share_table = 'bars_a_1d'

        # Query from DuckDB bars_a_1d table
        duckdb_manager = get_duckdb_manager()
        with duckdb_manager.get_connection() as conn:
            # Build symbol placeholders for DuckDB
            # Handle symbols with or without exchange suffix
            symbol_conditions = []
            params = []

            for symbol in symbols:
                if '.' in symbol:
                    # Symbol already has exchange suffix
                    stock_code, exchange = symbol.split('.')
                    symbol_conditions.append("(stock_code = ? AND exchange = ?)")
                    params.extend([stock_code, exchange])
                else:
                    # Symbol without exchange - match any exchange
                    symbol_conditions.append("stock_code = ?")
                    params.append(symbol)

            where_clause = "({})".format(" OR ".join(symbol_conditions))

            # Determine which price columns to use
            if price_type == 'qfq':
                price_cols = "open_qfq, high_qfq, low_qfq, close_qfq"
            else:
                price_cols = "open, high, low, close"

            query = f"""
            SELECT stock_code as symbol, datetime, {price_cols},
                   volume, amount, pct_chg, total_mv
            FROM {a_share_table}
            WHERE {where_clause}
              AND datetime >= ?::DATE
              AND datetime <= ?::DATE
            ORDER BY datetime, stock_code
            """
            params.extend([start_date, end_date])

            df = conn.execute(query, params).fetchdf()

        if df.empty:
            logger.warning("No data returned from DuckDB")
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

        # Rename price columns if using qfq
        if price_type == 'qfq':
            df = df.rename(columns={
                'open_qfq': 'open',
                'high_qfq': 'high',
                'low_qfq': 'low',
                'close_qfq': 'close'
            })

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
        """Get all available stock symbols from the bars_a_1d table in DuckDB.

        Args:
            trade_date: If specified, only return symbols that have data on this date.
                        If None, return symbols from the latest available date.
        """
        from src.db.duckdb_manager import get_duckdb_manager

        a_share_table = 'bars_a_1d'

        duckdb_manager = get_duckdb_manager()
        with duckdb_manager.get_connection() as conn:
            if trade_date is None:
                query = f"""
                SELECT DISTINCT stock_code as symbol FROM {a_share_table}
                WHERE datetime = (SELECT MAX(datetime) FROM {a_share_table})
                ORDER BY symbol
                """
                df = conn.execute(query).fetchdf()
            else:
                query = f"""
                SELECT DISTINCT stock_code as symbol FROM {a_share_table}
                WHERE datetime = ?::DATE
                ORDER BY symbol
                """
                df = conn.execute(query, [trade_date]).fetchdf()

        return df['symbol'].tolist()
