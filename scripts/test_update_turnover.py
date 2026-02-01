#!/usr/bin/env python3
"""
测试换手率更新功能
"""
import sys
sys.path.insert(0, '/Users/zhangbo/Public/go/github.com/mytrader')

from src.data_sources.tushare import TushareDB
from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH
from datetime import datetime, timedelta

def main():
    print("=" * 60)
    print("测试换手率更新功能")
    print("=" * 60)

    # 初始化数据库连接
    db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

    # 1. 检查哪些股票的最新日期缺少换手率
    print("\n1. 检查缺少换手率的股票...")
    import pandas as pd

    query = """
    SELECT symbol, datetime, close, volume, turnover
    FROM bars
    WHERE interval = '1d'
      AND datetime IN (
        SELECT DISTINCT datetime FROM bars ORDER BY datetime DESC LIMIT 5
      )
      AND turnover IS NULL
    ORDER BY symbol, datetime DESC
    LIMIT 20
    """

    with db.engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    if df.empty:
        print("   ✓ 没有发现缺少换手率的记录")
    else:
        print(f"   发现 {len(df)} 条缺少换手率的记录:")
        for _, row in df.head(10).iterrows():
            print(f"      {row['symbol']} {row['datetime']}")

    # 2. 测试更新换手率（只更新最近3天）
    print("\n2. 更新最近3天的换手率数据...")
    from datetime import timedelta

    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')

    print(f"   日期范围: {start_date} - {end_date}")

    # 只测试几只股票
    test_symbols = ['600382', '600711', '000001']

    updated = db.update_turnover_only(
        symbols=test_symbols,
        start_date=start_date,
        end_date=end_date
    )

    print(f"\n   ✓ 更新了 {updated} 条记录")

    # 3. 验证更新结果
    print("\n3. 验证更新结果...")
    query = """
    SELECT symbol, datetime, close, turnover
    FROM bars
    WHERE symbol IN ('600382', '600711', '000001')
      AND datetime >= date('now', '-3 days')
    ORDER BY symbol, datetime DESC
    """

    with db.engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    print("\n最近3天的换手率数据:")
    for symbol in test_symbols:
        symbol_df = df[df['symbol'] == symbol].sort_values('datetime', ascending=False)
        if not symbol_df.empty:
            print(f"\n   {symbol}:")
            for _, row in symbol_df.head(3).iterrows():
                turnover = f"{row['turnover']:.2f}%" if row['turnover'] else "NULL"
                print(f"      {row['datetime']}: {turnover}")

    print("\n" + "=" * 60)
    print("测试完成!")
    print("=" * 60)

if __name__ == '__main__':
    main()
