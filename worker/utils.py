"""
Worker Utility Functions

Helper functions for task execution.
"""
from sqlalchemy import text


def get_hk_stock_list_for_task(stock_range, custom_stocks=None):
    """
    Get HK stock list for task execution.

    Args:
        stock_range: 'all' | 'favorites' | 'custom'
        custom_stocks: Custom stock list (required when stock_range='custom', optional when stock_range='favorites')

    Returns:
        List of HK stock codes (without .HK suffix, e.g., ['00700', '00941'])
    """
    if stock_range == 'custom':
        if not custom_stocks:
            raise ValueError("custom_stocks required when stock_range='custom'")
        return custom_stocks

    elif stock_range == 'favorites':
        # If custom_stocks is provided, use it (frontend has already filtered HK stocks from favorites)
        if custom_stocks:
            return custom_stocks

        # Otherwise, fetch HK stocks from favorites table in SQLite database
        import sqlite3
        from pathlib import Path

        # Use tushare_data.db where favorites table is located
        db_path = Path(__file__).parent.parent / "data" / "tushare_data.db"

        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            cursor.execute("""
                SELECT stock_code FROM favorites
                WHERE stock_code LIKE '%.HK'
                ORDER BY stock_code
            """)
            result = cursor.fetchall()
            conn.close()

            if not result:
                # Fallback to default if no favorites
                return ["00700", "00941", "02318"]

            # Extract stock codes (remove .HK suffix)
            return [row[0].split('.')[0] for row in result]
        except Exception as e:
            # Fallback to default on error
            return ["00700", "00941", "02318"]

    else:  # 'all'
        # Query from DuckDB hk_stock_list table
        from src.db.duckdb_manager import get_duckdb_manager

        db_manager = get_duckdb_manager(read_only=True)
        with db_manager.get_connection() as conn:
            try:
                df = conn.execute("""
                    SELECT ts_code FROM hk_stock_list
                    WHERE list_status = 'L'
                    ORDER BY ts_code
                """).fetchdf()

                if df.empty:
                    raise ValueError("No HK stocks found in hk_stock_list table. Run python scripts/fetch_hk_stocks.py first.")

                # Extract stock codes (remove .HK suffix)
                return [row['ts_code'].split('.')[0] for _, row in df.iterrows()]
            except Exception as e:
                if "Table" in str(e) and "does not exist" in str(e):
                    raise ValueError("hk_stock_list table does not exist. Run python scripts/fetch_hk_stocks.py first.")
                raise


def get_market_type(code: str) -> str:
    """
    根据股票代码判断市场类型

    Args:
        code: 6位股票代码

    Returns:
        市场类型: 'main', 'gem', 'star', 'bse', 'unknown'
    """
    code = str(code).strip()
    # 上海主板
    if code.startswith(('600', '601', '603', '604', '605')):
        return 'main'
    # 科创板
    elif code.startswith(('688', '689')):
        return 'star'
    # 深圳主板
    elif code.startswith(('000', '001')):
        return 'main'
    # 中小板（已合并到主板）
    elif code.startswith(('002', '003')):
        return 'main'
    # 创业板
    elif code.startswith(('300', '301')):
        return 'gem'
    # 北交所
    elif code.startswith(('8', '4')):
        return 'bse'
    else:
        return 'unknown'


def get_stock_list_from_tushare(markets=None, exclude_st=True):
    """
    从 tushare 获取完整股票列表

    Args:
        markets: List of market types ['main', 'gem', 'star', 'bse']
        exclude_st: Whether to exclude ST stocks

    Returns:
        List of stock codes
    """
    import tushare as ts
    from config.settings import TUSHARE_TOKEN

    pro = ts.pro_api(TUSHARE_TOKEN)

    # 获取全部A股列表
    df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')

    # 提取股票代码（去掉后缀）
    stocks = [code.split('.')[0] for code in df['ts_code'].tolist()]
    st_stocks = set()

    # 排除ST股票（从名称判断）
    if exclude_st:
        for _, row in df.iterrows():
            code = row['ts_code'].split('.')[0]
            name = row['name'] or ''
            if 'ST' in name.upper():
                st_stocks.add(code)
        stocks = [s for s in stocks if s not in st_stocks]

    # 按市场类型筛选
    if markets:
        stocks = [s for s in stocks if get_market_type(s) in markets]

    return stocks


def get_stock_list_for_task(stock_range, custom_stocks, db, stocks_param=None, markets=None, exclude_st=True):
    """
    Get stock list for task execution.

    Args:
        stock_range: Stock range type ('all', 'favorites', 'custom', 'market')
        custom_stocks: Custom stock list
        db: TushareDB instance (not used, kept for backward compatibility)
        stocks_param: Optional stocks list from params (for backward compatibility)
        markets: List of market types ['main', 'gem', 'star', 'bse'] (required when stock_range='market')
        exclude_st: Whether to exclude ST stocks (default: True)

    Returns:
        List of stock codes (excludes indices)
    """
    if stock_range == 'custom':
        return custom_stocks
    elif stock_range == 'favorites':
        # Use stocks_param if provided (backward compatibility), otherwise use defaults
        return stocks_param if stocks_param else ["600382", "600711", "000001"]
    else:
        # 从 tushare 获取完整股票列表
        if stock_range == 'market':
            if not markets:
                markets = ['main']  # 默认主板
        else:
            markets = None  # 全部市场

        return get_stock_list_from_tushare(markets=markets, exclude_st=exclude_st)
