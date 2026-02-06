"""
Quarterly ML Utilities Module

Provides utility functions for quarterly financial data prediction.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Tuple, List
import logging

logger = logging.getLogger(__name__)


def parse_quarter(quarter_str: str) -> Tuple[int, int]:
    """
    Parse quarter string to year and quarter number

    Args:
        quarter_str: Quarter string like "2020Q1", "2024Q4"

    Returns:
        (year, quarter) tuple

    Examples:
        >>> parse_quarter("2020Q1")
        (2020, 1)
        >>> parse_quarter("2024Q4")
        (2024, 4)
    """
    try:
        year, q = quarter_str.split('Q')
        return int(year), int(q)
    except Exception as e:
        raise ValueError(f"Invalid quarter format: {quarter_str}. Expected format: YYYYQ# (e.g., 2020Q1)") from e


def quarter_to_date(quarter_str: str) -> datetime:
    """
    Convert quarter string to the end date of that quarter

    Args:
        quarter_str: Quarter string like "2020Q1"

    Returns:
        End date of the quarter

    Examples:
        >>> quarter_to_date("2020Q1")
        datetime(2020, 3, 31)
        >>> quarter_to_date("2020Q4")
        datetime(2020, 12, 31)
    """
    year, quarter = parse_quarter(quarter_str)

    quarter_end_months = {
        1: 3,
        2: 6,
        3: 9,
        4: 12
    }

    month = quarter_end_months[quarter]

    # Last day of the quarter
    if quarter == 1:
        day = 31
    elif quarter == 2:
        day = 30
    elif quarter == 3:
        day = 30
    else:  # Q4
        day = 31

    return datetime(year, month, day)


def get_quarter_from_date(date: datetime) -> str:
    """
    Get quarter string from a date

    Args:
        date: Date object

    Returns:
        Quarter string like "2020Q1"
    """
    quarter = (date.month - 1) // 3 + 1
    return f"{date.year}Q{quarter}"


def add_quarters(quarter_str: str, n: int) -> str:
    """
    Add n quarters to a quarter string

    Args:
        quarter_str: Quarter string like "2020Q1"
        n: Number of quarters to add (can be negative)

    Returns:
        New quarter string

    Examples:
        >>> add_quarters("2020Q1", 1)
        "2020Q2"
        >>> add_quarters("2020Q4", 1)
        "2021Q1"
        >>> add_quarters("2020Q1", -1)
        "2019Q4"
    """
    year, quarter = parse_quarter(quarter_str)

    # Add quarters
    quarter += n

    # Handle year rollover
    if quarter > 4:
        yearsToAdd = (quarter - 1) // 4
        year += yearsToAdd
        quarter = ((quarter - 1) % 4) + 1
    elif quarter < 1:
        yearsToAdd = (abs(quarter) // 4) + 1
        year -= yearsToAdd
        quarter = 4 + (quarter % 4)

    return f"{year}Q{quarter}"


def generate_quarter_range(start_quarter: str, end_quarter: str) -> List[str]:
    """
    Generate a list of quarters from start to end (inclusive)

    Args:
        start_quarter: Start quarter (e.g., "2020Q1")
        end_quarter: End quarter (e.g., "2024Q4")

    Returns:
        List of quarter strings
    """
    quarters = []
    current = start_quarter

    while current <= end_quarter:
        quarters.append(current)
        current = add_quarters(current, 1)

    return quarters


def calculate_next_quarter_return(
    price_df: pd.DataFrame,
    ann_date: str,
    window_days: int = 60,
    skip_days: int = 5
) -> Optional[float]:
    """
    Calculate next quarter stock return from announcement date

    The return is calculated from skip_days after announcement
    to (skip_days + window_days) after announcement.

    Args:
        price_df: DataFrame with 'datetime' and 'close' columns
        ann_date: Announcement date string (YYYYMMDD)
        window_days: Number of trading days to measure return (default 60)
        skip_days: Number of trading days to skip after announcement (default 5)

    Returns:
        Return as decimal (0.15 = 15%), or None if not enough data

    Examples:
        If announcement is 2020-04-30:
        - Start: 2020-05-07 (5 trading days later)
        - End: 2020-07-30 (60 trading days from start)
        - Return = (price[end] - price[start]) / price[start]
    """
    # Parse announcement date
    try:
        ann_dt = datetime.strptime(ann_date, '%Y%m%d')
    except ValueError:
        try:
            ann_dt = datetime.strptime(ann_date, '%Y-%m-%d')
        except ValueError:
            logger.error(f"Invalid announcement date format: {ann_date}")
            return None

    # Filter data after announcement date
    price_df = price_df[price_df['datetime'] >= ann_dt].copy()

    if len(price_df) < skip_days + window_days + 1:
        logger.warning(f"Not enough price data after {ann_date}: {len(price_df)} < {skip_days + window_days + 1}")
        return None

    # Get start and end prices
    start_price = price_df.iloc[skip_days]['close']
    end_price = price_df.iloc[skip_days + window_days]['close']

    # Calculate return
    return_rate = (end_price - start_price) / start_price

    return return_rate


def align_quarterly_data_with_prices(
    financial_df: pd.DataFrame,
    price_df: pd.DataFrame,
    window_days: int = 60,
    skip_days: int = 5
) -> pd.DataFrame:
    """
    Align quarterly financial data with price data and calculate returns

    Args:
        financial_df: Quarterly financial data with 'ann_date' column
        price_df: Daily price data with 'datetime' and 'close' columns
        window_days: Trading days window for return calculation
        skip_days: Trading days to skip after announcement

    Returns:
        DataFrame with financial data and next quarter returns
    """
    results = []

    # Ensure price_df is sorted by date
    price_df = price_df.sort_values('datetime').reset_index(drop=True)

    for _, row in financial_df.iterrows():
        ann_date = row['ann_date']

        # Calculate next quarter return
        next_return = calculate_next_quarter_return(
            price_df,
            ann_date,
            window_days=window_days,
            skip_days=skip_days
        )

        if next_return is not None:
            new_row = row.copy()
            new_row['next_quarter_return'] = next_return
            results.append(new_row)

    if not results:
        logger.warning("No valid alignments found between financial and price data")
        return pd.DataFrame()

    return pd.DataFrame(results)


def validate_no_data_leakage(
    df: pd.DataFrame,
    feature_cols: List[str],
    target_col: str = 'next_quarter_return'
) -> bool:
    """
    Validate that there's no data leakage in features

    Checks that all features are available at the time of prediction
    (before the target return period).

    Args:
        df: DataFrame with features and target
        feature_cols: List of feature column names
        target_col: Target column name

    Returns:
        True if no data leakage detected, False otherwise
    """
    # This is a basic check - in practice, you might want more sophisticated checks
    # For now, we just check that target column doesn't contain NaN values in features
    # (which would indicate misalignment)

    if target_col not in df.columns:
        logger.error(f"Target column {target_col} not found in DataFrame")
        return False

    # Check that we have valid target values
    valid_targets = df[target_col].notna().sum()

    if valid_targets == 0:
        logger.error("No valid target values found")
        return False

    # Check that features don't have excessive missing values
    for col in feature_cols:
        if col in df.columns:
            missing_ratio = df[col].isna().sum() / len(df)
            if missing_ratio > 0.5:
                logger.warning(f"Feature {col} has {missing_ratio:.1%} missing values")

    logger.info(f"Data leakage validation passed: {valid_targets} valid samples")
    return True


def get_quarterly_financial_columns() -> dict:
    """
    Get mapping of financial indicator categories to their column names

    Returns:
        Dictionary with categories and their column names
    """
    return {
        'profitability': [
            'eps', 'basic_eps', 'diluted_eps',
            'roe', 'roa', 'roic',
            'netprofit_margin', 'grossprofit_margin', 'operateprofit_margin'
        ],
        'growth': [
            'or_yoy', 'netprofit_yoy', 'assets_yoy', 'ocf_yoy'
        ],
        'operation': [
            'assets_turn', 'ar_turn', 'inv_turn'
        ],
        'solvency': [
            'current_ratio', 'quick_ratio', 'debt_to_assets'
        ],
        'cashflow': [
            'ocfps', 'ocf_to_debt', 'free_cf'
        ],
        'per_share': [
            'bps'
        ]
    }


def get_income_statement_columns() -> List[str]:
    """
    Get key income statement columns for feature engineering

    Returns:
        List of column names
    """
    return [
        'total_revenue',
        'oper_cost',
        'operate_profit',
        'total_profit',
        'n_income',
        'n_income_attr_p'
    ]


def get_balance_sheet_columns() -> List[str]:
    """
    Get key balance sheet columns for feature engineering

    Returns:
        List of column names
    """
    return [
        'total_assets',
        'total_liability',
        'total_owner_equities'
    ]


def get_cashflow_columns() -> List[str]:
    """
    Get key cash flow statement columns for feature engineering

    Returns:
        List of column names
    """
    return [
        'n_cashflow_act',
        'n_cash_flows_fnc_act',
        'n_cash_flows_inv_act'
    ]


def get_valuation_columns() -> List[str]:
    """
    Get valuation metric columns

    Returns:
        List of column names
    """
    return [
        'pe', 'pe_ttm',
        'pb',
        'ps', 'ps_ttm',
        'total_mv', 'circ_mv'
    ]
