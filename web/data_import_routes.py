# web/data_import_routes.py
"""
Data import API routes
Support Excel/CSV file upload and import to DuckDB
"""
import os
import tempfile
from pathlib import Path
from flask import Blueprint, request, jsonify, current_app
from werkzeug.utils import secure_filename
from werkzeug.exceptions import BadRequest, UnsupportedMediaType
import logging
import pandas as pd

from src.data_import.excel_importer import ExcelDataImporter
from src.db.duckdb_manager import get_duckdb_manager

logger = logging.getLogger(__name__)

# Create Blueprint
data_import_bp = Blueprint('data_import', __name__, url_prefix='/api/data-import')

# Allowed file extensions
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB


def allowed_file(filename: str) -> bool:
    """Check if file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@data_import_bp.route('/upload', methods=['POST'])
def upload_file():
    """
    Upload and import Excel/CSV file

    Form data:
        - file: The file to upload
        - table_name: (optional) Target table name
        - interval: (optional) Time interval ('1d', '5m', '15m', '30m', '60m')
        - sheet_name: (optional) Excel sheet name to import

    Returns:
        JSON response with import result
    """
    # Check if file is in request
    if 'file' not in request.files:
        return jsonify({
            'success': False,
            'error': 'No file provided'
        }), 400

    file = request.files['file']

    # Check if filename is empty
    if file.filename == '':
        return jsonify({
            'success': False,
            'error': 'No file selected'
        }), 400

    # Check file extension
    if not allowed_file(file.filename):
        return jsonify({
            'success': False,
            'error': f'Unsupported file format. Allowed formats: {", ".join(ALLOWED_EXTENSIONS)}'
        }), 400

    # Get optional parameters
    table_name = request.form.get('table_name', '').strip() or None
    interval = request.form.get('interval', '').strip() or None
    sheet_name = request.form.get('sheet_name', '').strip() or None

    # Validate interval if provided
    if interval and interval not in ['1d', '5m', '15m', '30m', '60m']:
        return jsonify({
            'success': False,
            'error': f'Invalid interval. Must be one of: 1d, 5m, 15m, 30m, 60m'
        }), 400

    # Save file to temporary location
    filename = secure_filename(file.filename)
    filepath = None

    try:
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(filename).suffix) as tmp_file:
            file.save(tmp_file.name)
            filepath = tmp_file.name

        logger.info(f"File saved to temporary location: {filepath}")

        # Import data using writer connection
        from src.db.duckdb_manager import get_duckdb_writer
        db_manager = get_duckdb_writer()
        try:
            importer = ExcelDataImporter(db_manager)

            result = importer.import_from_excel(
                filepath,
                table_name=table_name,
                interval=interval,
                sheet_name=sheet_name
            )

            return jsonify({
                'success': True,
                'message': f'Successfully imported {result["rows_imported"]} rows to table {result["table_name"]}',
                'data': result
            }), 200
        finally:
            # 立即关闭写入连接
            db_manager.close()

    except ValueError as e:
        logger.error(f"Validation error during import: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

    except Exception as e:
        logger.error(f"Error during import: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Import failed: {str(e)}'
        }), 500

    finally:
        # Clean up temporary file
        if filepath and os.path.exists(filepath):
            try:
                os.remove(filepath)
                logger.info(f"Temporary file removed: {filepath}")
            except Exception as e:
                logger.warning(f"Failed to remove temporary file {filepath}: {e}")


@data_import_bp.route('/sheets', methods=['POST'])
def get_excel_sheets():
    """
    Get list of sheet names from an Excel file

    Form data:
        - file: The Excel file to analyze

    Returns:
        JSON response with sheet names list
    """
    if 'file' not in request.files:
        return jsonify({
            'success': False,
            'error': 'No file provided'
        }), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({
            'success': False,
            'error': 'No file selected'
        }), 400

    suffix = Path(file.filename).suffix.lower()
    if suffix not in ['.xlsx', '.xls']:
        return jsonify({
            'success': False,
            'error': 'File must be an Excel file (.xlsx or .xls)'
        }), 400

    filepath = None

    try:
        # Save to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
            file.save(tmp_file.name)
            filepath = tmp_file.name

        # Get sheet names using pandas
        import pandas as pd
        xl_file = pd.ExcelFile(filepath)
        sheet_names = xl_file.sheet_names

        return jsonify({
            'success': True,
            'data': {
                'sheets': sheet_names,
                'count': len(sheet_names)
            }
        }), 200

    except Exception as e:
        logger.error(f"Error getting sheet names: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

    finally:
        if filepath and os.path.exists(filepath):
            try:
                os.remove(filepath)
            except Exception:
                pass


@data_import_bp.route('/validate', methods=['POST'])
def validate_file():
    """
    Validate file format and data before import

    Form data:
        - file: The file to validate

    Returns:
        JSON response with validation result
    """
    if 'file' not in request.files:
        return jsonify({
            'success': False,
            'error': 'No file provided'
        }), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({
            'success': False,
            'error': 'No file selected'
        }), 400

    if not allowed_file(file.filename):
        return jsonify({
            'success': False,
            'error': f'Unsupported file format. Allowed formats: {", ".join(ALLOWED_EXTENSIONS)}'
        }), 400

    filepath = None

    try:
        # Save to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename).suffix) as tmp_file:
            file.save(tmp_file.name)
            filepath = tmp_file.name

        # Validate file
        db_manager = get_duckdb_manager()
        importer = ExcelDataImporter(db_manager)
        validation_result = importer.validate_file(filepath)

        return jsonify({
            'success': True,
            'data': validation_result
        }), 200

    except Exception as e:
        logger.error(f"Error during validation: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

    finally:
        if filepath and os.path.exists(filepath):
            try:
                os.remove(filepath)
            except Exception:
                pass


@data_import_bp.route('/tables', methods=['GET'])
def list_tables():
    """
    List all available time interval tables

    Returns:
        JSON response with table information
    """
    try:
        db_manager = get_duckdb_manager()
        intervals = db_manager.get_intervals()

        tables_info = []
        for interval in intervals:
            table_name = db_manager.get_table_name_for_interval(interval)
            row_count = db_manager.get_row_count(table_name)

            # Get date range and symbol count using context manager
            try:
                with db_manager.get_connection() as conn:
                    date_range = conn.execute(
                        f"SELECT MIN(datetime) as min_date, MAX(datetime) as max_date FROM {table_name}"
                    ).fetchdf()
                    if not date_range.empty:
                        date_range = date_range.iloc[0].to_dict()
                        # Convert to strings
                        date_range = {
                            'start': str(date_range.get('min_date', '')),
                            'end': str(date_range.get('max_date', ''))
                        }
                    else:
                        date_range = {'start': '', 'end': ''}

                    # Get symbol count (更新查询以使用新的列名 stock_code)
                    symbol_count = conn.execute(
                        f"SELECT COUNT(DISTINCT stock_code) FROM {table_name}"
                    ).fetchone()[0]
            except Exception:
                date_range = {'start': '', 'end': ''}
                symbol_count = 0

            tables_info.append({
                'interval': interval,
                'table_name': table_name,
                'row_count': row_count,
                'symbol_count': symbol_count,
                'date_range': date_range
            })

        return jsonify({
            'success': True,
            'data': tables_info
        }), 200

    except Exception as e:
        logger.error(f"Error listing tables: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@data_import_bp.route('/tables/<interval>/symbols', methods=['GET'])
def get_table_symbols(interval: str):
    """
    获取指定时间周期表中的股票列表

    Args:
        interval: 时间周期 ('1d', '5m', '15m', '30m', '60m')

    Returns:
        JSON response with stock symbols
    """
    try:
        if interval not in ['1d', '5m', '15m', '30m', '60m']:
            return jsonify({
                'success': False,
                'error': f'Invalid interval: {interval}'
            }), 400

        db_manager = get_duckdb_manager()
        table_name = db_manager.get_table_name_for_interval(interval)

        if not db_manager.table_exists(table_name):
            return jsonify({
                'success': False,
                'error': f'Table {table_name} does not exist'
            }), 404

        with db_manager.get_connection() as conn:
            # 更新查询以使用新的列名 stock_code 和 exchange
            result = conn.execute(
                f"SELECT DISTINCT stock_code, exchange FROM {table_name} ORDER BY stock_code"
            ).fetchdf()

            # 组合成 ts_code 格式返回
            symbols = []
            if not result.empty:
                for _, row in result.iterrows():
                    stock_code = row['stock_code']
                    exchange = row['exchange']
                    if pd.notna(exchange):
                        symbols.append(f"{stock_code}.{exchange}")
                    else:
                        symbols.append(stock_code)

        return jsonify({
            'success': True,
            'data': {
                'interval': interval,
                'table_name': table_name,
                'symbols': symbols
            }
        }), 200

    except Exception as e:
        logger.error(f"Error getting symbols for {interval}: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@data_import_bp.route('/tables/<interval>/summary', methods=['GET'])
def get_table_summary(interval: str):
    """
    Get detailed summary for a specific interval table

    Args:
        interval: Time interval ('1d', '5m', '15m', '30m', '60m')

    Returns:
        JSON response with table summary
    """
    try:
        if interval not in ['1d', '5m', '15m', '30m', '60m']:
            return jsonify({
                'success': False,
                'error': f'Invalid interval: {interval}'
            }), 400

        db_manager = get_duckdb_manager()
        table_name = db_manager.get_table_name_for_interval(interval)

        if not db_manager.table_exists(table_name):
            return jsonify({
                'success': False,
                'error': f'Table {table_name} does not exist'
            }), 404

        importer = ExcelDataImporter(db_manager)
        summary = importer.get_import_summary(table_name)

        return jsonify({
            'success': True,
            'data': summary
        }), 200

    except Exception as e:
        logger.error(f"Error getting table summary: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@data_import_bp.route('/intervals', methods=['GET'])
def get_supported_intervals():
    """
    Get list of supported time intervals

    Returns:
        JSON response with supported intervals
    """
    intervals = [
        {'value': '5m', 'label': '5分钟'},
        {'value': '15m', 'label': '15分钟'},
        {'value': '30m', 'label': '30分钟'},
        {'value': '60m', 'label': '60分钟'},
        {'value': '1d', 'label': '日线'},
    ]

    return jsonify({
        'success': True,
        'data': intervals
    }), 200


def register_data_import_routes(app):
    """
    Register data import routes with Flask app

    Args:
        app: Flask application instance
    """
    app.register_blueprint(data_import_bp)
    logger.info("Data import routes registered")
