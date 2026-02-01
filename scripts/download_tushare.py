#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
使用 Tushare 下载数据到数据库
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_sources.tushare import TushareDB
from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH, A_STOCKS, DEFAULT_STOCK

# 初始化数据库
db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

# 保存多只股票数据
print("=== 批量下载股票 ===")
print(f"股票列表: {A_STOCKS}")
print("-" * 50)
db.save_multiple_stocks(A_STOCKS, start_date="20200101")

# 从本地加载数据验证
print("\n=== 数据验证 ===")
symbol = DEFAULT_STOCK  # 从配置文件读取
stock_name = db.get_stock_name(symbol)
print(f"股票代码: {symbol}")
print(f"股票名称: {stock_name}")
print("-" * 50)

# 查询最近30天的数据
from datetime import datetime, timedelta
end_date = datetime.today().strftime("%Y-%m-%d")
start_date = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")

df = db.load_bars(symbol, start_date, end_date)
print(f"查询日期范围: {start_date} 至 {end_date}")
print(f"共 {len(df)} 条记录")
if not df.empty:
    print(f"实际数据范围: {df['datetime'].iloc[0]} 至 {df['datetime'].iloc[-1]}")

print("\n最新5条数据:")
if not df.empty:
    print(df[['datetime', 'open', 'high', 'low', 'close', 'close_qfq', 'volume']].tail())
else:
    print("无数据")

# 统计换手率
if not df.empty and 'turnover' in df.columns:
    high_turnover = df[df['turnover'] > 1]
    print(f"\n换手率统计:")
    print(f"  总天数: {len(df)}")
    print(f"  换手率 > 1% 的天数: {len(high_turnover)}")
    if len(df) > 0:
        print(f"  占比: {len(high_turnover)/len(df)*100:.1f}%")
