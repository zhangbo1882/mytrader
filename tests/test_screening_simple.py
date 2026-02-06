"""
Simple stock screening test - without complex industry joins
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.screening.screening_engine import ScreeningEngine
from src.screening.criteria.basic_criteria import RangeCriteria, GreaterThanCriteria
from src.screening.base_criteria import AndCriteria


def test_basic_screening():
    """Test basic screening without complex joins"""
    print("=" * 50)
    print("Simple Screening Test (No Industry Joins)")
    print("=" * 50)

    db_path = 'data/tushare_data.db'
    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        return

    # Test 1: Simple PE filter
    print("\nTest 1: PE Filter (0 < PE < 30)")
    engine = ScreeningEngine(db_path)

    # Get available dates first
    dates = engine.get_available_dates(limit=5)
    print(f"Available dates: {dates[:3]}")

    if dates:
        trade_date = dates[0]
        print(f"Using date: {trade_date}")

        # Load data without industry joins
        query = """
        SELECT
            b.symbol,
            b.datetime as trade_date,
            b.close, b.volume, b.amount, b.turnover,
            b.pe_ttm, b.pb, b.total_mv, b.circ_mv,
            sn.name as stock_name
        FROM bars b
        LEFT JOIN stock_names sn ON b.symbol = sn.code
        WHERE b.datetime = :trade_date
          AND b.interval = '1d'
          AND b.pe_ttm IS NOT NULL
          AND b.pe_ttm > 0
        LIMIT 20
        """

        import pandas as pd
        df = pd.read_sql_query(query, engine.engine, params={'trade_date': trade_date})

        if not df.empty:
            print(f"\nLoaded {len(df)} stocks")
            print("\nSample data:")
            print(df[['symbol', 'stock_name', 'pe_ttm', 'pb']].head(10))

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
