"""
Alpha Factor Service

Service layer for 101 Formulaic Alphas API endpoints.
"""
import logging
from flask import request
from config.settings import TUSHARE_DB_PATH

logger = logging.getLogger(__name__)

_engine = None


def get_engine():
    """Lazy-load AlphaEngine."""
    global _engine
    if _engine is None:
        from src.alphas.engine import AlphaEngine
        _engine = AlphaEngine(str(TUSHARE_DB_PATH))
    return _engine


def list_alphas():
    """List all available alpha factors."""
    try:
        engine = get_engine()
        alphas = engine.list_alphas()
        return {
            'success': True,
            'count': len(alphas),
            'alphas': alphas
        }, 200
    except Exception as e:
        logger.error(f"Error listing alphas: {e}")
        return {'error': str(e)}, 500


def compute_alphas():
    """Compute alpha factor(s) for given symbols and date range.

    Request JSON:
        {
            "alpha_ids": [1, 101],         # Alpha numbers (1-101)
            "symbols": ["600382", "000001"],  # Stock codes
            "start_date": "2025-01-01",
            "end_date": "2025-06-30",
            "price_type": ""               # Optional: qfq/hfq/''
        }
    """
    try:
        data = request.get_json()
        if not data:
            return {'error': '请提供请求参数'}, 400

        alpha_ids = data.get('alpha_ids', [])
        symbols = data.get('symbols', [])
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        price_type = data.get('price_type', '')

        if not alpha_ids:
            return {'error': '请提供alpha_ids参数'}, 400
        if not symbols:
            return {'error': '请提供symbols参数'}, 400
        if not start_date or not end_date:
            return {'error': '请提供start_date和end_date参数'}, 400

        engine = get_engine()

        if len(alpha_ids) == 1:
            result = engine.compute_alpha(alpha_ids[0], symbols, start_date, end_date, price_type)
            results_dict = {alpha_ids[0]: result}
        else:
            results_dict = engine.compute_alphas_batch(alpha_ids, symbols, start_date, end_date, price_type)

        # Convert to JSON-serializable format
        response_data = {}
        for alpha_id, df in results_dict.items():
            if df.empty:
                response_data[str(alpha_id)] = []
                continue

            records = []
            for date_str in df.index:
                row = {'date': str(date_str)}
                for symbol in df.columns:
                    val = df.loc[date_str, symbol]
                    row[symbol] = None if (val != val) else round(float(val), 6)  # NaN check
                records.append(row)
            response_data[str(alpha_id)] = records

        return {
            'success': True,
            'data': response_data,
            'alpha_ids': alpha_ids,
            'symbols': symbols,
            'start_date': start_date,
            'end_date': end_date
        }, 200

    except ValueError as e:
        return {'error': str(e)}, 400
    except Exception as e:
        logger.error(f"Error computing alphas: {e}")
        return {'error': str(e)}, 500


def get_alpha_snapshot():
    """Get cross-sectional alpha scores on a single date.

    Request JSON:
        {
            "alpha_id": 101,
            "symbols": ["600382", "000001"],  # Optional: if empty, use all available
            "trade_date": "2025-06-30",
            "price_type": ""
        }
    """
    try:
        data = request.get_json()
        if not data:
            return {'error': '请提供请求参数'}, 400

        alpha_id = data.get('alpha_id')
        symbols = data.get('symbols', [])
        trade_date = data.get('trade_date')
        price_type = data.get('price_type', '')

        if not alpha_id:
            return {'error': '请提供alpha_id参数'}, 400
        if not trade_date:
            return {'error': '请提供trade_date参数'}, 400

        engine = get_engine()

        # If no symbols specified, get all available
        if not symbols:
            adapter = engine.adapter
            symbols = adapter.get_all_symbols(trade_date)
            if not symbols:
                return {'error': f'日期 {trade_date} 没有可用数据'}, 404

        result = engine.get_alpha_snapshot(alpha_id, symbols, trade_date, price_type)

        if result.empty:
            return {
                'success': True,
                'alpha_id': alpha_id,
                'trade_date': trade_date,
                'count': 0,
                'data': []
            }, 200

        # Convert to list of {symbol, value} sorted by value descending
        records = []
        for symbol, value in result.items():
            if value == value:  # NaN check
                records.append({
                    'symbol': symbol,
                    'value': round(float(value), 6)
                })
        records.sort(key=lambda x: x['value'], reverse=True)

        return {
            'success': True,
            'alpha_id': alpha_id,
            'trade_date': trade_date,
            'count': len(records),
            'data': records
        }, 200

    except ValueError as e:
        return {'error': str(e)}, 400
    except Exception as e:
        logger.error(f"Error getting alpha snapshot: {e}")
        return {'error': str(e)}, 500
