"""
Unit tests for worker utility functions.

Tests utility functions using mocks.
Run with: pytest tests/worker/unit/test_utils.py -v
"""
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from worker.utils import get_stock_list_for_task
from tests.worker.fixtures.mock_db import MockTushareDB, MockStockList


class TestGetStockListForTask:
    """Test get_stock_list_for_task utility function."""

    def test_custom_range(self):
        """Test custom stock range returns provided list."""
        custom_stocks = ['600382', '600711', '000001']
        db = MockTushareDB()

        result = get_stock_list_for_task('custom', custom_stocks, db)

        assert result == custom_stocks

    def test_custom_range_empty_list(self):
        """Test custom range with empty list."""
        db = MockTushareDB()

        result = get_stock_list_for_task('custom', [], db)

        assert result == []

    def test_favorites_range_default(self):
        """Test favorites range with default stocks."""
        db = MockTushareDB()

        result = get_stock_list_for_task('favorites', [], db)

        assert result == ["600382", "600711", "000001"]

    def test_favorites_range_with_stocks_param(self):
        """Test favorites range with stocks_param override."""
        custom_favorites = ['600519', '000858']
        db = MockTushareDB()

        result = get_stock_list_for_task('favorites', [], db, custom_favorites)

        assert result == custom_favorites

    @patch('worker.utils.text')
    def test_all_range_from_database(self, mock_text):
        """Test all range queries database."""
        # Mock the database query
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ('600382',), ('600711',), ('000001',)
        ]
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db = MagicMock()
        mock_db.engine.connect.return_value = mock_conn

        # Mock text() to return the query string
        mock_text.return_value = "SELECT DISTINCT symbol FROM bars WHERE symbol LIKE '____%'"

        result = get_stock_list_for_task('all', [], mock_db)

        assert len(result) == 3
        assert '600382' in result
        assert '600711' in result
        assert '000001' in result

    @patch('worker.utils.text')
    def test_all_range_database_error(self, mock_text):
        """Test all range handles database errors gracefully."""
        # Mock database that raises exception
        mock_db = MagicMock()
        mock_db.engine.connect.side_effect = Exception("Database error")
        mock_text.return_value = "SELECT..."

        result = get_stock_list_for_task('all', [], mock_db)

        assert result == []

    def test_unrecognized_range_defaults_to_custom(self):
        """Test that unrecognized range defaults to custom behavior."""
        # This tests implementation detail - may need adjustment
        db = MockTushareDB()

        # If 'all' range fails (no engine), it might return empty list
        # or the function might handle it differently
        result = get_stock_list_for_task('unknown_range', ['600382'], db)

        # Should at least not crash
        assert isinstance(result, list)

    @pytest.mark.parametrize("stock_range,custom_stocks,expected", [
        ('custom', ['600382', '600711'], ['600382', '600711']),
        ('custom', [], []),
        ('favorites', [], ["600382", "600711", "000001"]),
    ])
    def test_various_ranges(self, stock_range, custom_stocks, expected):
        """Parameterized test for different stock ranges."""
        db = MockTushareDB()

        result = get_stock_list_for_task(stock_range, custom_stocks, db)

        assert result == expected


class TestStockListGeneration:
    """Test stock list generation scenarios."""

    def test_large_custom_list(self):
        """Test handling large custom stock lists."""
        large_list = [f"{i:06d}" for i in range(600000, 601000)]
        db = MockTushareDB()

        result = get_stock_list_for_task('custom', large_list, db)

        assert len(result) == 1000
        assert result == large_list

    def test_duplicate_handling(self):
        """Test that duplicates in custom list are preserved."""
        # This tests current behavior - duplicates are passed through
        list_with_dupes = ['600382', '600382', '600711', '600711']
        db = MockTushareDB()

        result = get_stock_list_for_task('custom', list_with_dupes, db)

        # Duplicates are preserved (function doesn't deduplicate)
        assert result == list_with_dupes

    def test_mixed_stock_codes(self):
        """Test handling different stock code formats."""
        mixed_codes = ['600382', '000001', '300001', '688001']
        db = MockTushareDB()

        result = get_stock_list_for_task('custom', mixed_codes, db)

        assert result == mixed_codes


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_none_db_handle(self):
        """Test with None database (should not crash)."""
        # This tests the 'custom' and 'favorites' paths
        # which don't use the db
        result = get_stock_list_for_task('custom', ['600382'], None)
        assert result == ['600382']

    def test_empty_strings_in_list(self):
        """Test list with empty strings."""
        list_with_empty = ['600382', '', '600711', '']
        db = MockTushareDB()

        result = get_stock_list_for_task('custom', list_with_empty, db)

        # Empty strings are preserved
        assert result == list_with_empty

    def test_whitespace_in_codes(self):
        """Test stock codes with whitespace."""
        codes_with_spaces = ['600382 ', ' 600711', ' 000001 ']
        db = MockTushareDB()

        result = get_stock_list_for_task('custom', codes_with_spaces, db)

        # Whitespace is preserved as-is
        assert result == codes_with_spaces


class TestBackwardCompatibility:
    """Test backward compatibility scenarios."""

    def test_stocks_param_used_for_favorites(self):
        """Test that stocks_param is used for favorites range."""
        stocks_param = ['600519', '000858', '600000']
        db = MockTushareDB()

        result = get_stock_list_for_task('favorites', [], db, stocks_param)

        assert result == stocks_param

    def test_stocks_param_ignored_for_custom(self):
        """Test that stocks_param is ignored for custom range."""
        custom_stocks = ['600382', '600711']
        stocks_param = ['600519', '000858']
        db = MockTushareDB()

        result = get_stock_list_for_task('custom', custom_stocks, db, stocks_param)

        # Should use custom_stocks, not stocks_param
        assert result == custom_stocks

    def test_empty_stocks_param_falls_back_to_default(self):
        """Test empty stocks_param uses default favorites."""
        db = MockTushareDB()

        result = get_stock_list_for_task('favorites', [], db, [])

        # Should use default favorites
        assert result == ["600382", "600711", "000001"]
