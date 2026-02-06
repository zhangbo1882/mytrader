"""
Liquidity screening service

Provides REST API endpoints for A-share liquidity screening
with three-tier filter logic.

Note: lookback_days parameter refers to TRADING DAYS, not calendar days.
The system automatically handles weekends and holidays to ensure the
correct number of trading days are used for calculations.
"""
from flask import jsonify, request
import logging
import traceback
import pandas as pd

from config.settings import TUSHARE_DB_PATH
from src.data_sources.query.stock_query import StockQuery

logger = logging.getLogger(__name__)


def liquidity_screen():
    """
    Liquidity screening API endpoint

    Three-tier liquidity filter:
    1. Absolute liquidity floor (prevent extreme risks)
    2. Relative activity (filter out "zombie stocks")
    3. Liquidity quality (detect small-cap traps)

    Request body (JSON):
        lookback_days: Number of TRADING DAYS for metrics calculation (default: 20)
        min_avg_amount_20d: Minimum average daily amount in 万元 (default: 3000)
        min_avg_turnover_20d: Minimum average turnover rate % (default: 0.3)
        small_cap_threshold: Small cap threshold in 亿元 (default: 50)
        high_turnover_threshold: High turnover threshold % (default: 8)
        max_amihud_illiquidity: Maximum Amihud illiquidity (default: 0.8)
        limit: Maximum number of results to return (default: None)

    Note: lookback_days is counted in TRADING DAYS (交易日), not calendar days.
    The system uses a wider date range to ensure sufficient trading days are captured.

    Returns:
        JSON response with:
            success: True/False
            count: Number of stocks that passed the filter
            stocks: List of stock records with liquidity metrics
            error: Error message (if success=False)
    """
    try:
        data = request.json if request.json else {}

        # Extract parameters with defaults
        lookback_days = data.get('lookback_days', 20)
        min_avg_amount_20d = data.get('min_avg_amount_20d', 3000)  # 3000万元
        min_avg_turnover_20d = data.get('min_avg_turnover_20d', 0.3)  # 0.3%
        small_cap_threshold = data.get('small_cap_threshold', 50)  # 50亿元
        high_turnover_threshold = data.get('high_turnover_threshold', 8.0)  # 8%
        max_amihud_illiquidity = data.get('max_amihud_illiquidity', 0.8)
        limit = data.get('limit', 100)  # Default limit to 100 results

        logger.info(f"Liquidity screening request: lookback_days={lookback_days}, "
                   f"min_avg_amount_20d={min_avg_amount_20d}, "
                   f"min_avg_turnover_20d={min_avg_turnover_20d}, "
                   f"small_cap_threshold={small_cap_threshold}, "
                   f"high_turnover_threshold={high_turnover_threshold}, "
                   f"max_amihud_illiquidity={max_amihud_illiquidity}, "
                   f"limit={limit}")

        # Create query instance
        query = StockQuery(str(TUSHARE_DB_PATH))

        # Perform liquidity screening
        df = query.liquidity_screen(
            lookback_days=lookback_days,
            min_avg_amount_20d=min_avg_amount_20d,
            min_avg_turnover_20d=min_avg_turnover_20d,
            small_cap_threshold=small_cap_threshold,
            high_turnover_threshold=high_turnover_threshold,
            max_amihud_illiquidity=max_amihud_illiquidity,
            limit=limit
        )

        if df.empty:
            return jsonify({
                'success': True,
                'count': 0,
                'stocks': [],
                'message': 'No stocks passed the liquidity filter'
            }), 200

        # Convert DataFrame to list of dictionaries
        # Replace NaN with None for JSON serialization
        df_clean = df.where(pd.notnull(df), None)

        stocks = []
        for _, row in df_clean.iterrows():
            stocks.append({
                'symbol': row.get('symbol'),
                'name': row.get('name'),
                'avg_amount_20d': round(row.get('avg_amount_20d', 0), 2) if row.get('avg_amount_20d') else None,
                'avg_turnover_20d': round(row.get('avg_turnover_20d', 0), 2) if row.get('avg_turnover_20d') else None,
                'avg_circ_mv': round(row.get('avg_circ_mv', 0), 2) if row.get('avg_circ_mv') else None,
                'amihud_illiquidity': round(row.get('amihud_illiquidity', 0), 6) if row.get('amihud_illiquidity') else None,
                'filter_result': row.get('filter_result')
            })

        return jsonify({
            'success': True,
            'count': len(stocks),
            'stocks': stocks
        }), 200

    except Exception as e:
        logger.error(f"Error in liquidity screening: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


def liquidity_metrics(symbol):
    """
    Get liquidity metrics for a single stock

    Path params:
        symbol: Stock code

    Query params:
        lookback_days: Number of TRADING DAYS (交易日) for calculation (default: 20)

    Returns:
        JSON response with liquidity metrics

    Note: lookback_days is counted in TRADING DAYS, not calendar days.
    """
    try:
        if not symbol:
            return jsonify({
                'success': False,
                'error': 'Stock code is required'
            }), 400

        lookback_days = request.args.get('lookback_days', 20)

        from src.data_sources.query.liquidity_query import LiquidityQuery

        liquidity_query = LiquidityQuery(str(TUSHARE_DB_PATH))

        metrics = liquidity_query.calculate_liquidity_metrics(symbol, lookback_days)

        # Get stock name
        from src.utils.stock_lookup import get_stock_name_from_code
        stock_name = get_stock_name_from_code(symbol)

        return jsonify({
            'success': True,
            'symbol': symbol,
            'name': stock_name or symbol,
            'metrics': {
                'avg_amount_20d': round(metrics['avg_amount_20d'] / 10000, 2) if metrics['avg_amount_20d'] else None,  # 万元
                'avg_turnover_20d': round(metrics['avg_turnover_20d'], 2) if metrics['avg_turnover_20d'] else None,
                'avg_circ_mv': round(metrics['avg_circ_mv'] / 100000000, 2) if metrics['avg_circ_mv'] else None,  # 亿元
                'amihud_illiquidity': round(metrics['amihud_illiquidity'], 6) if metrics['amihud_illiquidity'] else None,
                'data_points': metrics['data_points']
            }
        }), 200

    except Exception as e:
        logger.error(f"Error getting liquidity metrics for {symbol}: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
