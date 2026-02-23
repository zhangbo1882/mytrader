"""
Fundamental Filter

Filters stocks based on fundamental event risks to avoid
non-technical risks like earnings announcements, unlock dates, etc.
"""

import logging
from datetime import date, timedelta
from typing import Tuple, Optional

logger = logging.getLogger(__name__)


class FundamentalFilter:
    """
    Fundamental filter for event risk avoidance

    Checks for upcoming events that may cause abnormal price movements:
    - Earnings announcements
    - Stock unlock dates
    - Other significant corporate events
    """

    def __init__(self, duckdb_service=None):
        """
        Initialize the filter

        Args:
            duckdb_service: DuckDB query service (optional)
        """
        self.duckdb_service = duckdb_service
        self.event_cache = {}  # Cache for queried events

    def check_event_risk(self, stock_code: str, current_date: date) -> Tuple[bool, str]:
        """
        Check if stock has major events in next 5 trading days

        Args:
            stock_code: Stock code
            current_date: Current date

        Returns:
            (is_risky, reason) tuple
            is_risky: True means has risk
            reason: Risk reason
        """
        cache_key = f"{stock_code}_{current_date}"
        if cache_key in self.event_cache:
            return self.event_cache[cache_key]

        # 1. Check earnings announcement
        has_earnings, earnings_reason = self._check_upcoming_earnings(stock_code, current_date)
        if has_earnings:
            self.event_cache[cache_key] = (True, earnings_reason)
            return True, earnings_reason

        # 2. Check unlock dates
        has_unlock, unlock_reason = self._check_upcoming_unlock(stock_code, current_date)
        if has_unlock:
            self.event_cache[cache_key] = (True, unlock_reason)
            return True, unlock_reason

        # 3. Check other events (suspension, resumption, etc.)
        # TODO: Add more event types as needed

        self.event_cache[cache_key] = (False, "")
        return False, ""

    def _check_upcoming_earnings(self, stock_code: str, current_date: date) -> Tuple[bool, str]:
        """
        Check if there's earnings announcement in next 5 trading days

        Note: This requires earnings calendar data in database
        If not available, returns False (no filtering)
        """
        if self.duckdb_service is None:
            return False, ""

        # Check next 7 calendar days (covers ~5 trading days)
        end_date = current_date + timedelta(days=7)

        try:
            # TODO: Query earnings calendar table when available
            # Example query:
            # sql = """
            # SELECT announcement_date
            # FROM earnings_calendar
            # WHERE stock_code = ?
            #   AND announcement_date BETWEEN ? AND ?
            # LIMIT 1
            # """
            # result = self.duckdb_service.execute_query(sql, [stock_code, current_date, end_date])
            # if result:
            #     return True, f"即将发布财报: {result[0]['announcement_date']}"

            pass
        except Exception as e:
            logger.warning(f"Error checking earnings for {stock_code}: {e}")

        return False, ""

    def _check_upcoming_unlock(self, stock_code: str, current_date: date) -> Tuple[bool, str]:
        """
        Check if there's stock unlock in next 5 trading days

        Note: This requires unlock data in database
        If not available, returns False (no filtering)
        """
        if self.duckdb_service is None:
            return False, ""

        # Check next 7 calendar days (covers ~5 trading days)
        end_date = current_date + timedelta(days=7)

        try:
            # TODO: Query unlock table when available
            # Example query:
            # sql = """
            # SELECT unlock_date, unlock_volume
            # FROM stock_unlocks
            # WHERE stock_code = ?
            #   AND unlock_date BETWEEN ? AND ?
            # LIMIT 1
            # """
            # result = self.duckdb_service.execute_query(sql, [stock_code, current_date, end_date])
            # if result:
            #     return True, f"即将解禁: {result[0]['unlock_date']}, 解禁量: {result[0]['unlock_volume']}"

            pass
        except Exception as e:
            logger.warning(f"Error checking unlock for {stock_code}: {e}")

        return False, ""

    def clear_cache(self):
        """Clear event cache"""
        self.event_cache.clear()
