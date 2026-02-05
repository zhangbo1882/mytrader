"""
Worker Utility Functions

Helper functions for task execution.
"""
from sqlalchemy import text


def get_stock_list_for_task(stock_range, custom_stocks, db, stocks_param=None):
    """
    Get stock list for task execution.

    Args:
        stock_range: Stock range type ('all', 'favorites', 'custom')
        custom_stocks: Custom stock list
        db: TushareDB instance
        stocks_param: Optional stocks list from params (for backward compatibility)

    Returns:
        List of stock codes
    """
    if stock_range == 'custom':
        return custom_stocks
    elif stock_range == 'favorites':
        # Use stocks_param if provided (backward compatibility), otherwise use defaults
        return stocks_param if stocks_param else ["600382", "600711", "000001"]
    else:  # all
        try:
            with db.engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT DISTINCT symbol FROM bars WHERE symbol LIKE '____%'"
                ))
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            print(f"[Task Worker] Error getting stock list: {e}")
            return []
