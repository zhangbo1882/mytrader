# tests/test_excel_importer.py
"""
Unit tests for Excel data importer
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os

from src.data_import.excel_importer import ExcelDataImporter


@pytest.fixture
def mock_db_manager():
    """Mock DuckDBManager"""
    manager = Mock()
    manager.create_table = Mock(return_value=True)
    manager.insert_dataframe = Mock(return_value=2)  # Return 2 rows
    manager.connect = Mock()
    manager.table_exists = Mock(return_value=True)
    return manager


@pytest.fixture
def importer(mock_db_manager):
    """Create ExcelDataImporter instance with mocked DB manager"""
    return ExcelDataImporter(mock_db_manager)


class TestIntervalDetection:
    """Test interval detection logic"""

    def test_detect_interval_daily(self, importer):
        """Test detecting daily interval"""
        df = pd.DataFrame({
            'datetime': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'close': [10.0, 11.0, 12.0]
        })
        df['datetime'] = pd.to_datetime(df['datetime'])

        interval = importer.detect_interval(df)
        assert interval == '1d'

    def test_detect_interval_5m(self, importer):
        """Test detecting 5-minute interval"""
        df = pd.DataFrame({
            'datetime': pd.date_range('2024-01-01 09:30', periods=10, freq='5min'),
            'close': range(10)
        })

        interval = importer.detect_interval(df)
        assert interval == '5m'

    def test_detect_interval_15m(self, importer):
        """Test detecting 15-minute interval"""
        df = pd.DataFrame({
            'datetime': pd.date_range('2024-01-01 09:30', periods=10, freq='15min'),
            'close': range(10)
        })

        interval = importer.detect_interval(df)
        assert interval == '15m'

    def test_detect_interval_30m(self, importer):
        """Test detecting 30-minute interval"""
        df = pd.DataFrame({
            'datetime': pd.date_range('2024-01-01 09:30', periods=10, freq='30min'),
            'close': range(10)
        })

        interval = importer.detect_interval(df)
        assert interval == '30m'

    def test_detect_interval_60m(self, importer):
        """Test detecting 60-minute interval"""
        df = pd.DataFrame({
            'datetime': pd.date_range('2024-01-01 09:30', periods=10, freq='60min'),
            'close': range(10)
        })

        interval = importer.detect_interval(df)
        assert interval == '60m'

    def test_detect_interval_missing_datetime_column(self, importer):
        """Test error when datetime column is missing"""
        df = pd.DataFrame({
            'close': [10.0, 11.0, 12.0]
        })

        with pytest.raises(ValueError, match="must contain 'datetime' column"):
            importer.detect_interval(df)


class TestDataCleaning:
    """Test data cleaning logic"""

    def test_clean_symbol_removes_suffix(self, importer):
        """Test removing exchange suffix from symbol"""
        assert importer._clean_symbol('600000.SH') == '600000'
        assert importer._clean_symbol('000001.SZ') == '000001'
        assert importer._clean_symbol('600000.sh') == '600000'
        assert importer._clean_symbol('000001.sz') == '000001'
        assert importer._clean_symbol('600000') == '600000'

    def test_clean_symbol_handles_nan(self, importer):
        """Test handling NaN values in symbol"""
        result = importer._clean_symbol(np.nan)
        assert pd.isna(result)

    def test_clean_data(self, importer):
        """Test overall data cleaning"""
        df = pd.DataFrame({
            'symbol': ['600000.SH', '000001.SZ', '600000'],
            'datetime': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'open': ['10.0', '11.0', 'invalid'],
            'close': [10.0, 11.0, 12.0],
            'volume': [1000, 2000, 3000]
        })

        cleaned = importer._clean_data(df)

        # Check symbol suffix removal
        assert cleaned['symbol'].tolist() == ['600000', '000001', '600000']

        # Check datetime conversion
        assert pd.api.types.is_datetime64_any_dtype(cleaned['datetime'])

        # Check numeric conversion
        assert pd.api.types.is_numeric_dtype(cleaned['open'])

        # Check NaN handling for invalid values
        assert pd.isna(cleaned['open'].iloc[2])

    def test_clean_data_removes_duplicates(self, importer):
        """Test duplicate removal"""
        df = pd.DataFrame({
            'symbol': ['600000', '600000', '600000'],
            'datetime': ['2024-01-01', '2024-01-01', '2024-01-02'],
            'close': [10.0, 11.0, 12.0]
        })
        df['datetime'] = pd.to_datetime(df['datetime'])

        cleaned = importer._clean_data(df)

        assert len(cleaned) == 2  # Only 2 unique (symbol, datetime) pairs
        assert cleaned.iloc[0]['close'] == 11.0  # Last value kept


class TestColumnNormalization:
    """Test column name normalization"""

    def test_normalize_column_names_chinese(self, importer):
        """Test normalizing Chinese column names"""
        df = pd.DataFrame({
            '代码': ['600000'],
            '日期': ['2024-01-01'],
            '开': [10.0],
            '收': [11.0]
        })

        normalized = importer._normalize_column_names(df)

        assert 'symbol' in normalized.columns
        assert 'datetime' in normalized.columns
        assert 'open' in normalized.columns
        assert 'close' in normalized.columns

    def test_normalize_column_names_english_variants(self, importer):
        """Test normalizing English column name variants"""
        df = pd.DataFrame({
            'code': ['600000'],
            'time': ['2024-01-01'],
            'open_price': [10.0],
            'close_price': [11.0],
            'vol': [1000]
        })

        normalized = importer._normalize_column_names(df)

        assert 'symbol' in normalized.columns
        assert 'datetime' in normalized.columns
        assert 'open' in normalized.columns
        assert 'close' in normalized.columns
        assert 'volume' in normalized.columns


class TestValidation:
    """Test file validation"""

    def test_validate_columns_success(self, importer):
        """Test successful validation"""
        df = pd.DataFrame({
            'symbol': ['600000'],
            'datetime': ['2024-01-01'],
            'close': [10.0]
        })

        # Should not raise
        importer._validate_columns(df)

    def test_validate_columns_missing_required(self, importer):
        """Test validation fails with missing required columns"""
        df = pd.DataFrame({
            'symbol': ['600000'],
            'datetime': ['2024-01-01']
            # Missing 'close'
        })

        with pytest.raises(ValueError, match="Missing required columns"):
            importer._validate_columns(df)


class TestFileReading:
    """Test file reading functionality"""

    def test_read_csv_with_different_encodings(self, importer):
        """Test reading CSV with different encodings"""
        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write('symbol,datetime,close\n')
            f.write('600000,2024-01-01,10.0\n')
            temp_path = f.name

        try:
            df = importer._read_file(temp_path)
            assert len(df) == 1
            assert 'symbol' in df.columns
        finally:
            os.unlink(temp_path)

    def test_read_excel_file(self, importer):
        """Test reading Excel file"""
        # Create a temporary Excel file
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.xlsx', delete=False) as f:
            temp_path = f.name

        try:
            # Write test data to Excel
            df = pd.DataFrame({
                'symbol': ['600000'],
                'datetime': ['2024-01-01'],
                'close': [10.0]
            })
            df.to_excel(temp_path, index=False)

            # Read it back
            df_read = importer._read_file(temp_path)
            assert len(df_read) == 1
            assert 'symbol' in df_read.columns
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_read_unsupported_format(self, importer):
        """Test error on unsupported file format"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('test')
            temp_path = f.name

        try:
            with pytest.raises(ValueError, match="Unsupported file format"):
                importer._read_file(temp_path)
        finally:
            os.unlink(temp_path)


class TestImportWorkflow:
    """Test complete import workflow"""

    def test_import_from_excel(self, importer, mock_db_manager):
        """Test complete import process"""
        # Create temporary Excel file
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.xlsx', delete=False) as f:
            temp_path = f.name

        try:
            # Prepare test data
            df = pd.DataFrame({
                'symbol': ['600000', '600000'],
                'datetime': ['2024-01-01', '2024-01-02'],
                'open': [10.0, 11.0],
                'high': [10.5, 11.5],
                'low': [9.5, 10.5],
                'close': [10.2, 11.2],
                'volume': [1000, 2000]
            })
            df.to_excel(temp_path, index=False)

            # Import
            result = importer.import_from_excel(temp_path)

            # Verify
            assert result['interval'] == '1d'  # Daily data
            assert result['rows_imported'] == 2
            assert result['symbol_count'] == 1
            assert '600000' in result['symbols']

            # Verify table creation was called
            mock_db_manager.create_table.assert_called_once()

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_import_with_custom_interval(self, importer, mock_db_manager):
        """Test import with specified interval"""
        # Create temporary Excel file
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.xlsx', delete=False) as f:
            temp_path = f.name

        try:
            df = pd.DataFrame({
                'symbol': ['600000'],
                'datetime': ['2024-01-01'],
                'close': [10.0]
            })
            df.to_excel(temp_path, index=False)

            # Import with custom interval
            result = importer.import_from_excel(temp_path, interval='5m')

            # Verify table name
            assert result['interval'] == '5m'
            assert result['table_name'] == 'bars_5m'

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
