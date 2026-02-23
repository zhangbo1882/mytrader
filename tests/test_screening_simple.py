"""
Simple stock screening test - using DuckDB
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.screening.screening_engine import ScreeningEngine
from src.screening.criteria.basic_criteria import RangeCriteria, GreaterThanCriteria
from src.screening.base_criteria import AndCriteria


def test_basic_screening():
    """Test basic screening using DuckDB"""
    print("=" * 50)
    print("Simple Screening Test (DuckDB - A-share)")
    print("=" * 50)

    from src.db.duckdb_manager import get_duckdb_manager
    from config.settings import TUSHARE_DB_PATH
    from sqlalchemy import create_engine
    import pandas as pd

    # Test 1: Simple PE filter from DuckDB
    print("\nTest 1: PE Filter (0 < PE < 30) from DuckDB")

    a_share_table = 'bars_a_1d'

    duckdb_manager = get_duckdb_manager()
    with duckdb_manager.get_connection() as conn:
        # Get available dates first
        dates_df = conn.execute(f"""
            SELECT DISTINCT datetime as trade_date
            FROM {a_share_table}
            ORDER BY datetime DESC
            LIMIT 5
        """).fetchdf()

        if dates_df.empty:
            print("No data found in DuckDB")
            return

        dates = dates_df['trade_date'].tolist()
        print(f"Available dates: {dates[:3]}")

        trade_date = dates[0]
        print(f"Using date: {trade_date}")

        # Load data from DuckDB
        query = f"""
        SELECT
            b.stock_code as symbol,
            b.datetime as trade_date,
            b.close, b.volume, b.amount, b.turnover_rate_f as turnover,
            b.pe_ttm, b.pb, b.total_mv, b.circ_mv
        FROM {a_share_table} b
        WHERE b.datetime = ?::DATE
          AND b.pe_ttm IS NOT NULL
          AND b.pe_ttm > 0
        LIMIT 20
        """

        df = conn.execute(query, [str(trade_date)]).fetchdf()

        if not df.empty:
            print(f"\nLoaded {len(df)} stocks")
            print("\nSample data:")
            print(df[['symbol', 'pe_ttm', 'pb']].head(10))

            # Test basic filter
            criteria = RangeCriteria('pe_ttm', 0, 30)
            result = criteria.filter(df)
            print(f"\nAfter PE filter (0 < PE < 30): {len(result)} stocks")
            print(result[['symbol', 'stock_name', 'pe_ttm']].head(10))

            # Test AND filter
            criteria2 = AndCriteria(
                RangeCriteria('pe_ttm', 0, 20),
                GreaterThanCriteria('amount', 5000)
            )
            result2 = criteria2.filter(df)
            print(f"\nAfter AND filter (PE < 20 AND amount > 5000): {len(result2)} stocks")
            if not result2.empty:
                print(result2[['symbol', 'stock_name', 'pe_ttm', 'amount']].head(10))
        else:
            print("No data found")


def test_industry_filter():
    """Test industry filter with simplified query"""
    print("\n" + "=" * 50)
    print("Industry Filter Test")
    print("=" * 50)

    db_path = 'data/tushare_data.db'
    engine = ScreeningEngine(db_path)

    # Test with industry info (limit to L1 to avoid complex joins)
    query = """
    SELECT
        b.symbol,
        b.close, b.pe_ttm, b.amount,
        sc.industry_name as sw_l1,
        sn.name as stock_name
    FROM bars b
    LEFT JOIN stock_names sn ON b.symbol = sn.code
    LEFT JOIN sw_members swm ON b.symbol = SUBSTR(swm.ts_code, 1, 6)
    LEFT JOIN sw_classify sc ON swm.index_code = sc.index_code AND sc.level = 'L1'
    WHERE b.datetime = (SELECT MAX(datetime) FROM bars WHERE interval = '1d')
      AND b.interval = '1d'
      AND b.pe_ttm IS NOT NULL
    LIMIT 50
    """

    import pandas as pd
    df = pd.read_sql_query(query, engine.engine)

    if not df.empty:
        print(f"\nLoaded {len(df)} stocks with industry info")
        print(f"\nIndustries found: {df['sw_l1'].dropna().unique()[:10]}")

        # Show sample
        print("\nSample data:")
        print(df[['symbol', 'stock_name', 'pe_ttm', 'sw_l1']].dropna(subset=['sw_l1']).head(10))

        # Count by industry
        print("\nStocks by industry:")
        print(df.groupby('sw_l1').size().head(10))
    else:
        print("No data found")


def main():
    print("Simple Stock Screening Test")
    print("=" * 50)

    test_basic_screening()
    test_industry_filter()

    print("\n" + "=" * 50)
    print("Test Complete!")
    print("=" * 50)


if __name__ == '__main__':
    main()
