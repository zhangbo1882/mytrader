"""
Mock TushareDB for unit testing.

This module provides a mock implementation of TushareDB that doesn't
make external API calls or database connections.
"""
from unittest.mock import Mock, MagicMock
import pandas as pd


class MockTushareDB:
    """
    Mock TushareDB that returns simulated data.

    This mock simulates database operations without actually connecting
    to a database or making API calls, making tests fast and reliable.
    """

    def __init__(self):
        """Initialize the mock with default return values."""
        self.engine = Mock()

        # Mock method return values
        self.save_all_stocks_by_code_incremental = Mock(
            return_value={'success': 1, 'failed': 0, 'skipped': 0}
        )
        self.save_index_basic = Mock(return_value=10)
        self.save_index_daily = Mock(return_value=1)
        self.save_all_sw_industry = Mock(
            return_value={'success': 100, 'failed': 0}
        )
        self.save_sw_classify = Mock(return_value=511)  # Number of industry classifications
        self.save_sw_members = Mock(return_value=3000)  # Average members per industry

        # Track call counts for testing
        self._call_counts = {
            'save_all_stocks_by_code_incremental': 0,
            'save_index_basic': 0,
            'save_index_daily': 0,
            'save_all_sw_industry': 0,
            'save_sw_classify': 0,
            'save_sw_members': 0
        }

    def set_incremental_result(self, success=1, failed=0, skipped=0):
        """Set the return value for incremental stock updates."""
        self.save_all_stocks_by_code_incremental = Mock(
            return_value={'success': success, 'failed': failed, 'skipped': skipped}
        )

    def set_index_daily_result(self, count=1):
        """Set the return value for index daily updates."""
        self.save_index_daily = Mock(return_value=count)

    def set_sw_industry_result(self, success=100, failed=0):
        """Set the return value for SW industry updates."""
        self.save_all_sw_industry = Mock(
            return_value={'success': success, 'failed': failed}
        )

    def get_call_count(self, method_name):
        """Get the number of times a method was called."""
        return self._call_counts.get(method_name, 0)

    def reset_call_counts(self):
        """Reset all call counts."""
        for key in self._call_counts:
            self._call_counts[key] = 0

    def _track_call(self, method_name):
        """Track a method call."""
        self._call_counts[method_name] = self._call_counts.get(method_name, 0) + 1

    def save_all_stocks_by_code_incremental(self, *args, **kwargs):
        """Mock incremental stock update with call tracking."""
        self._track_call('save_all_stocks_by_code_incremental')
        return self.save_all_stocks_by_code_incremental(*args, **kwargs)

    def save_index_basic(self, *args, **kwargs):
        """Mock index basic update with call tracking."""
        self._track_call('save_index_basic')
        return self.save_index_basic(*args, **kwargs)

    def save_index_daily(self, *args, **kwargs):
        """Mock index daily update with call tracking."""
        self._track_call('save_index_daily')
        return self.save_index_daily(*args, **kwargs)

    def save_all_sw_industry(self, *args, **kwargs):
        """Mock SW industry update with call tracking."""
        self._track_call('save_all_sw_industry')
        return self.save_all_sw_industry(*args, **kwargs)

    def save_sw_classify(self, *args, **kwargs):
        """Mock SW classification save with call tracking."""
        self._track_call('save_sw_classify')
        return self.save_sw_classify(*args, **kwargs)

    def save_sw_members(self, *args, **kwargs):
        """Mock SW members save with call tracking."""
        self._track_call('save_sw_members')
        return self.save_sw_members(*args, **kwargs)

    def get_sw_industry_codes(self, src='SW2021'):
        """Mock get industry codes list. Returns mock SW industry codes."""
        # Return a small list for testing
        return [
            '801010.SI', '801020.SI', '801030.SI', '801040.SI', '801050.SI',
            '801080.SI', '801110.SI', '801120.SI', '801130.SI', '801140.SI'
        ]


class MockStockList:
    """
    Mock for stock list data.

    Provides predefined stock lists for testing different scenarios.
    """

    @staticmethod
    def get_all_stocks():
        """Get a list of all stock codes."""
        return [f"{i:06d}" for i in range(600000, 600500)]

    @staticmethod
    def get_favorites():
        """Get a list of favorite stock codes."""
        return ["600382", "600711", "000001"]

    @staticmethod
    def get_custom_stocks():
        """Get a list of custom stock codes."""
        return ["600382", "600711", "000001", "000002", "600519"]

    @staticmethod
    def get_index_list():
        """Get a list of index codes."""
        return [
            "000001.SH", "000002.SH", "000003.SH",  # Shanghai indices
            "399001.SZ", "399002.SZ", "399003.SZ"   # Shenzhen indices
        ]


class MockEngine:
    """
    Mock SQLAlchemy engine for database operations.
    """

    def __init__(self):
        """Initialize mock engine with mock connection."""
        self._connection = MockConnection()

    def connect(self):
        """Return a mock connection."""
        return self._connection


class MockConnection:
    """
    Mock SQLAlchemy connection.
    """

    def __init__(self):
        """Initialize mock connection."""
        self._cursor = MockCursor()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, *args):
        """Context manager exit."""
        pass

    def execute(self, query):
        """Execute a query and return mock cursor."""
        return self._cursor

    def close(self):
        """Close the connection (no-op)."""
        pass


class MockCursor:
    """
    Mock database cursor.
    """

    def __init__(self, stock_list=None):
        """Initialize cursor with optional stock list."""
        self._stock_list = stock_list or MockStockList.get_all_stocks()

    def fetchall(self):
        """Return mock stock list as tuples."""
        return [(stock,) for stock in self._stock_list]

    def close(self):
        """Close cursor (no-op)."""
        pass
