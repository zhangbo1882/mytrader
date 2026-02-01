#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
运行 Backtrader 回测
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, date

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import backtrader as bt
import matplotlib.pyplot as plt
import pandas as pd

from src.data_sources.tushare import TushareDB
from src.strategies import SMACrossStrategy
from config.settings import (
    TUSHARE_TOKEN, TUSHARE_DB_PATH, DEFAULT_STOCK,
    DEFAULT_INITIAL_CASH, DEFAULT_COMMISSION, DEFAULT_BACKTEST_START
)

plt.rcParams["font.sans-serif"] = ["Arial Unicode MS", "PingFang SC", "STHeiti"]
plt.rcParams["axes.unicode_minus"] = False

# 命令行参数解析
parser = argparse.ArgumentParser(description='运行 Backtrader 回测')
parser.add_argument('--stock', type=str, default=DEFAULT_STOCK,
                    help=f'股票代码 (默认: {DEFAULT_STOCK})')
parser.add_argument('--start-date', type=str, default=DEFAULT_BACKTEST_START,
                    help=f'回测开始日期，格式: YYYY-MM-DD (默认: {DEFAULT_BACKTEST_START})')
parser.add_argument('--end-date', type=str, default=None,
                    help='回测结束日期，格式: YYYY-MM-DD (默认: 今天)')
parser.add_argument('--cash', type=float, default=DEFAULT_INITIAL_CASH,
                    help=f'初始资金 (默认: {DEFAULT_INITIAL_CASH})')
parser.add_argument('--commission', type=float, default=DEFAULT_COMMISSION,
                    help=f'手续费率 (默认: {DEFAULT_COMMISSION})')
parser.add_argument('--maperiod', type=int, default=10,
                    help='移动平均线周期 (默认: 10)')
args = parser.parse_args()

# 股票代码
stock_code = args.stock

# 回测日期
start_date_str = args.start_date
end_date_str = args.end_date

# 如果没有指定结束日期，使用今天
if end_date_str is None:
    end_date_dt = date.today()
else:
    end_date_dt = datetime.strptime(end_date_str, '%Y-%m-%d').date()

# 转换开始日期
start_date_dt = datetime.strptime(start_date_str, '%Y-%m-%d').date()

# 计算数据加载开始日期（提前20天以便计算移动平均线指标）
from datetime import timedelta
data_start_date_dt = start_date_dt - timedelta(days=30)

# 从数据库加载数据（使用 Tushare 数据库）
db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))
stock_name = db.get_stock_name(stock_code)

print(f"股票代码: {stock_code}")
print(f"股票名称: {stock_name}")
print(f"价格类型: 前复权（适合计算收益率和技术指标）")
print("-" * 50)

# 从数据库获取数据
# 提前30天以便计算移动平均线指标
start_date_db = data_start_date_dt.strftime('%Y-%m-%d')
end_date_db = end_date_dt.strftime('%Y-%m-%d')
stock_df = db.load_bars(stock_code, start_date_db, end_date_db, price_type='qfq')  # 使用前复权价格

if stock_df.empty:
    print(f"❌ 数据库中没有 {stock_code} 的数据，请先运行下载脚本")
    sys.exit(1)

# 处理字段命名，以符合 Backtrader 的要求
stock_df = stock_df.rename(columns={'datetime': 'date'})
stock_df = stock_df.set_index('date')
stock_df = stock_df[['open', 'high', 'low', 'close', 'volume']]


# 初始化回测系统
cerebro = bt.Cerebro()
start_date = datetime.combine(start_date_dt, datetime.min.time())
end_date = datetime.combine(end_date_dt, datetime.min.time())
data = bt.feeds.PandasData(dataname=stock_df, fromdate=start_date, todate=end_date)  # 加载数据
cerebro.adddata(data)  # 将数据传入回测系统
cerebro.addstrategy(SMACrossStrategy, maperiod=args.maperiod, commission=args.commission)  # 将交易策略加载到回测系统中
cerebro.broker.setcash(args.cash)
cerebro.broker.setcommission(commission=args.commission)
cerebro.run()  # 运行回测系统

port_value = cerebro.broker.getvalue()  # 获取回测结束后的总资金
pnl = port_value - args.cash  # 盈亏统计

print(f"初始资金: {args.cash}\n回测期间：{start_date.strftime('%Y%m%d')}:{end_date.strftime('%Y%m%d')}")
print(f"总资金: {round(port_value, 2)}")
print(f"净收益: {round(pnl, 2)}")

# cerebro.plot(style='candlestick')  # 画图