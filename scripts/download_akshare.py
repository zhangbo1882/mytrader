#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
使用 AKShare 下载数据到数据库
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_sources.akshare import AKShareDB
from config.settings import AKSHARE_DB_PATH, A_STOCKS, HK_STOCKS, DEFAULT_STOCK

# 初始化数据库
db = AKShareDB(db_path=str(AKSHARE_DB_PATH))

# 保存 A 股
print("=== 下载 A 股数据 ===")
print(f"A股列表: {A_STOCKS}")
print("-" * 50)
for code in A_STOCKS:
    db.save_a_stock(code, start_date="20200101")

# 保存港股
print("\n=== 下载港股数据 ===")
print(f"港股列表: {HK_STOCKS}")
print("-" * 50)
for code in HK_STOCKS:
    db.save_hk_stock(code, start_date="20200101")

# 从本地加载验证
print("\n=== 数据验证 ===")
symbol = DEFAULT_STOCK
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
