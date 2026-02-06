"""
Liquidity Query Module for A-Share Stocks

Provides three-tier liquidity screening:
1. Absolute liquidity floor (prevent extreme risks)
2. Relative activity (filter out "zombie stocks")
3. Liquidity quality (detect small-cap traps)
"""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class LiquidityQuery:
    """
    Liquidity query and screening class

    Calculates Amihud illiquidity indicator and implements three-tier
    liquidity filter to identify stocks with good liquidity characteristics.
    """

    # Default parameter values
    DEFAULT_LOOKBACK_DAYS = 20
    DEFAULT_MIN_AVG_AMOUNT_20D = 3000  # 30 million yuan (3000万元)
    DEFAULT_MIN_AVG_TURNOVER_20D = 0.3  # 0.3%
    DEFAULT_SMALL_CAP_THRESHOLD = 500000  # 5 billion yuan (50亿元)
    DEFAULT_HIGH_TURNOVER_THRESHOLD = 8.0  # 8%
    DEFAULT_MAX_AMIHUD_ILLIQUIDITY = 0.8

    def __init__(self, db_path: str):
        """
        Initialize liquidity query

        Args:
            db_path: Database file path
        """
        self.engine = create_engine(f"sqlite:///{db_path}", echo=False)

    def calculate_amihud_illiquidity(self, df: pd.DataFrame) -> Optional[float]:
        """
        Calculate Amihud illiquidity indicator

        Formula: ILLIQ = mean(|return| / (amount * circ_mv))

        Where:
        - return = Daily return rate
        - amount = Trading volume in yuan
        - circ_mv = Circulating market cap in yuan

        Args:
            df: DataFrame with columns ['close', 'amount', 'circ_mv']

        Returns:
            Amihud illiquidity value (higher = more illiquid)
            Returns None if calculation fails
        """
        if df.empty or len(df) < 2:
            return None

        # Check required columns
        required_cols = ['close', 'amount', 'circ_mv']
        if not all(col in df.columns for col in required_cols):
            logger.warning(f"Missing required columns for Amihud calculation: {df.columns}")
            return None

        try:
            # Calculate daily returns
            df = df.copy()
            df['return'] = df['close'].pct_change()

            # Unit conversion
            # amount is in 万元 (10k yuan), convert to yuan
            df['amount_yuan'] = df['amount'] * 10000
            # circ_mv is in 亿元 (100M yuan), convert to yuan
            df['circ_mv_yuan'] = df['circ_mv'] * 100000000

            # Drop first row (NaN return)
            df = df.dropna(subset=['return', 'amount_yuan', 'circ_mv_yuan'])

            if df.empty:
                return None

            # Filter out zero or negative values
            df = df[(df['amount_yuan'] > 0) & (df['circ_mv_yuan'] > 0)]

            if df.empty:
                return None

            # Calculate daily illiquidity: |return| / (amount * circ_mv)
            df['daily_illiq'] = np.abs(df['return']) / (df['amount_yuan'] * df['circ_mv_yuan'])

            # Handle infinite values
            df = df[~df['daily_illiq'].isin([np.inf, -np.inf])]

            if df.empty:
                return None

            # Return mean daily illiquidity
            amihud = df['daily_illiq'].mean()
            return float(amihud) if np.isfinite(amihud) else None

        except Exception as e:
            logger.error(f"Error calculating Amihud illiquidity: {e}")
            return None

    def calculate_liquidity_metrics(self, symbol: str, lookback_days: int = None) -> Dict[str, any]:
        """
        Calculate liquidity metrics for a single stock

        Args:
            symbol: Stock code
            lookback_days: Number of trading days to look back (default: DEFAULT_LOOKBACK_DAYS)

        Returns:
            Dictionary with keys:
            - symbol: Stock code
            - avg_amount_20d: Average daily trading amount (yuan)
            - avg_turnover_20d: Average daily turnover rate (%)
            - avg_circ_mv: Average circulating market cap (yuan)
            - amihud_illiquidity: Amihud illiquidity indicator
            - data_points: Number of trading days used
        """
        if lookback_days is None:
            lookback_days = self.DEFAULT_LOOKBACK_DAYS

        # Use a wider date range to ensure we get enough trading days
        # Assume ~60% of days are trading days (accounting for weekends and holidays)
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=lookback_days * 3)).strftime('%Y-%m-%d')

        query = """
        SELECT
            close, amount, turnover, circ_mv, datetime
        FROM bars
        WHERE symbol = :symbol
          AND interval = '1d'
          AND datetime >= :start_date
          AND datetime <= :end_date
        ORDER BY datetime DESC
        LIMIT :limit
        """

        try:
            df = pd.read_sql_query(
                query,
                self.engine,
                params={
                    'symbol': symbol,
                    'start_date': start_date,
                    'end_date': end_date,
                    'limit': lookback_days
                }
            )

            if df.empty:
                return {
                    'symbol': symbol,
                    'avg_amount_20d': None,
                    'avg_turnover_20d': None,
                    'avg_circ_mv': None,
                    'amihud_illiquidity': None,
                    'data_points': 0
                }

            # Calculate metrics
            avg_amount = df['amount'].mean() * 10000  # Convert 万元 to yuan
            avg_turnover = df['turnover'].mean()  # Already in percentage
            avg_circ_mv = df['circ_mv'].mean() * 100000000  # Convert 亿元 to yuan

            # Calculate Amihud illiquidity
            amihud = self.calculate_amihud_illiquidity(df)

            return {
                'symbol': symbol,
                'avg_amount_20d': float(avg_amount) if pd.notna(avg_amount) else None,
                'avg_turnover_20d': float(avg_turnover) if pd.notna(avg_turnover) else None,
                'avg_circ_mv': float(avg_circ_mv) if pd.notna(avg_circ_mv) else None,
                'amihud_illiquidity': amihud,
                'data_points': len(df)
            }

        except Exception as e:
            logger.error(f"Error calculating liquidity metrics for {symbol}: {e}")
            return {
                'symbol': symbol,
                'avg_amount_20d': None,
                'avg_turnover_20d': None,
                'avg_circ_mv': None,
                'amihud_illiquidity': None,
                'data_points': 0
            }

    def liquidity_filter(
        self,
        metrics: Dict[str, any],
        min_avg_amount_20d: Optional[float] = None,
        min_avg_turnover_20d: Optional[float] = None,
        small_cap_threshold: Optional[float] = None,
        high_turnover_threshold: Optional[float] = None,
        max_amihud_illiquidity: Optional[float] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Three-tier liquidity filter

        Level 1: Absolute liquidity floor (prevent extreme risks)
        Level 2: Relative activity (filter out "zombie stocks")
        Level 3: Liquidity quality (detect small-cap traps)

        Args:
            metrics: Liquidity metrics dictionary from calculate_liquidity_metrics
            min_avg_amount_20d: Minimum average daily amount (yuan)
            min_avg_turnover_20d: Minimum average turnover rate (%)
            small_cap_threshold: Small cap threshold (yuan)
            high_turnover_threshold: High turnover threshold (%)
            max_amihud_illiquidity: Maximum Amihud illiquidity value

        Returns:
            Tuple of (passed: bool, reason: Optional[str])
            If passed=False, reason contains the rejection reason
        """
        # Use default values if not specified
        if min_avg_amount_20d is None:
            min_avg_amount_20d = self.DEFAULT_MIN_AVG_AMOUNT_20D * 10000  # Convert to yuan
        if min_avg_turnover_20d is None:
            min_avg_turnover_20d = self.DEFAULT_MIN_AVG_TURNOVER_20D
        if small_cap_threshold is None:
            small_cap_threshold = self.DEFAULT_SMALL_CAP_THRESHOLD * 100000000  # Convert to yuan
        if high_turnover_threshold is None:
            high_turnover_threshold = self.DEFAULT_HIGH_TURNOVER_THRESHOLD
        if max_amihud_illiquidity is None:
            max_amihud_illiquidity = self.DEFAULT_MAX_AMIHUD_ILLIQUIDITY

        # Extract metrics
        avg_amount = metrics.get('avg_amount_20d')
        avg_turnover = metrics.get('avg_turnover_20d')
        avg_circ_mv = metrics.get('avg_circ_mv')
        amihud = metrics.get('amihud_illiquidity')

        # Validate required data
        if avg_amount is None or avg_turnover is None or avg_circ_mv is None:
            return False, "Missing required liquidity data"

        # Level 1: Absolute liquidity floor
        if avg_amount < min_avg_amount_20d:
            return False, f"Level 1: Average amount {avg_amount/10000:.2f}万 < {min_avg_amount_20d/10000:.2f}万"

        # Level 2: Relative activity
        if avg_turnover < min_avg_turnover_20d:
            return False, f"Level 2: Average turnover {avg_turnover:.2f}% < {min_avg_turnover_20d:.2f}%"

        # Level 3: Liquidity quality (only for small caps with high turnover)
        if avg_circ_mv < small_cap_threshold and avg_turnover > high_turnover_threshold:
            if amihud is None:
                return False, "Level 3: Cannot calculate Amihud illiquidity"
            if amihud > max_amihud_illiquidity:
                return False, f"Level 3: Amihud illiquidity {amihud:.4f} > {max_amihud_illiquidity:.4f} (small-cap trap)"

        return True, None

    def screen_by_liquidity(
        self,
        lookback_days: int = None,
        min_avg_amount_20d: Optional[float] = None,
        min_avg_turnover_20d: Optional[float] = None,
        small_cap_threshold: Optional[float] = None,
        high_turnover_threshold: Optional[float] = None,
        max_amihud_illiquidity: Optional[float] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Batch liquidity screening for all stocks

        Args:
            lookback_days: Lookback period for metrics calculation
            min_avg_amount_20d: Minimum average daily amount (万元)
            min_avg_turnover_20d: Minimum average turnover rate (%)
            small_cap_threshold: Small cap threshold (亿元)
            high_turnover_threshold: High turnover threshold (%)
            max_amihud_illiquidity: Maximum Amihud illiquidity value
            limit: Maximum number of results to return

        Returns:
            DataFrame with columns:
            - symbol: Stock code
            - name: Stock name
            - avg_amount_20d: Average daily amount (万元)
            - avg_turnover_20d: Average turnover rate (%)
            - avg_circ_mv: Average circulating market cap (亿元)
            - amihud_illiquidity: Amihud illiquidity indicator
            - filter_result: 'PASS' or rejection reason
        """
        if lookback_days is None:
            lookback_days = self.DEFAULT_LOOKBACK_DAYS

        # Convert units from input (万元/亿元) to yuan for calculation
        min_avg_amount_yuan = min_avg_amount_20d * 10000 if min_avg_amount_20d else None
        small_cap_yuan = small_cap_threshold * 100000000 if small_cap_threshold else None

        # Get date range
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=lookback_days * 2)).strftime('%Y-%m-%d')

        # First, get list of all stocks with recent data
        stocks_query = """
        SELECT DISTINCT b.symbol
        FROM bars b
        WHERE b.interval = '1d'
          AND b.datetime >= :start_date
          AND b.datetime <= :end_date
        """

        try:
            with self.engine.connect() as conn:
                stocks_df = pd.read_sql_query(
                    stocks_query,
                    conn,
                    params={'start_date': start_date, 'end_date': end_date}
                )

            if stocks_df.empty:
                logger.warning("No stocks found in database")
                return pd.DataFrame()

            symbols = stocks_df['symbol'].tolist()
            logger.info(f"Starting liquidity screening for {len(symbols)} stocks")

            # Calculate metrics for each stock
            results = []
            for i, symbol in enumerate(symbols):
                if (i + 1) % 100 == 0:
                    logger.info(f"Progress: {i + 1}/{len(symbols)} stocks processed")

                metrics = self.calculate_liquidity_metrics(symbol, lookback_days)

                # Apply filter
                passed, reason = self.liquidity_filter(
                    metrics,
                    min_avg_amount_20d=min_avg_amount_yuan,
                    min_avg_turnover_20d=min_avg_turnover_20d,
                    small_cap_threshold=small_cap_yuan,
                    high_turnover_threshold=high_turnover_threshold,
                    max_amihud_illiquidity=max_amihud_illiquidity
                )

                # Only include passed stocks
                if passed:
                    # Get stock name
                    name_query = "SELECT name FROM stock_names WHERE code = :symbol LIMIT 1"
                    try:
                        with self.engine.connect() as conn:
                            name_df = pd.read_sql_query(name_query, conn, params={'symbol': symbol})
                            name = name_df.iloc[0]['name'] if not name_df.empty else symbol
                    except:
                        name = symbol

                    results.append({
                        'symbol': symbol,
                        'name': name,
                        'avg_amount_20d': metrics['avg_amount_20d'] / 10000 if metrics['avg_amount_20d'] else None,  # Convert to 万元
                        'avg_turnover_20d': metrics['avg_turnover_20d'],
                        'avg_circ_mv': metrics['avg_circ_mv'] / 100000000 if metrics['avg_circ_mv'] else None,  # Convert to 亿元
                        'amihud_illiquidity': metrics['amihud_illiquidity'],
                        'filter_result': 'PASS'
                    })

            # Create result DataFrame
            result_df = pd.DataFrame(results)

            if not result_df.empty:
                # Sort by average amount (descending)
                result_df = result_df.sort_values('avg_amount_20d', ascending=False)

                # Apply limit
                if limit and len(result_df) > limit:
                    result_df = result_df.head(limit)

            logger.info(f"Liquidity screening completed: {len(result_df)} stocks passed")
            return result_df

        except Exception as e:
            logger.error(f"Error in liquidity screening: {e}")
            return pd.DataFrame()
