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
        List of stock codes (excludes indices)
    """
    if stock_range == 'custom':
        return custom_stocks
    elif stock_range == 'favorites':
        # Use stocks_param if provided (backward compatibility), otherwise use defaults
        return stocks_param if stocks_param else ["600382", "600711", "000001"]
    else:  # all
        try:
            with db.engine.connect() as conn:
                # 带后缀的symbol（如000001.SH）是指数，不带后缀的是股票
                # 排除ST股（名称包含ST或*ST）
                result = conn.execute(text("""
                    SELECT DISTINCT b.symbol
                    FROM bars b
                    LEFT JOIN stock_names sn ON b.symbol = sn.code
                    WHERE b.symbol LIKE '____%'
                    AND b.symbol NOT LIKE '%.%'
                    AND (sn.name IS NULL OR sn.name NOT LIKE '%ST%')
                """))
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            print(f"[Task Worker] Error getting stock list: {e}")
            return []
