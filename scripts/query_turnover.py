#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
查询换手率大于指定值的交易日
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_sources.tushare import TushareDB
from src.data_sources.akshare import AKShareDB
from config.settings import (
    TUSHARE_TOKEN, TUSHARE_DB_PATH, AKSHARE_DB_PATH,
    DEFAULT_STOCK, DEFAULT_START_DATE
)


def query_high_turnover(source: str = "tushare", symbol: str = None):
    """
    查询换手率大于指定值的交易日

    Args:
        source: 数据源类型，"tushare" 或 "akshare"
        symbol: 股票代码，如果为None则使用配置文件中的默认值
    """
    # 使用配置文件中的默认股票代码
    if symbol is None:
        symbol = DEFAULT_STOCK

    min_turnover = 1.0
    start_date = "2025-01-01"
    end_date = "2026-01-31"

    # 选择数据库
    if source == "tushare":
        db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))
    else:
        db = AKShareDB(db_path=str(AKSHARE_DB_PATH))

    df = db.load_bars(symbol, start_date, end_date)

    if df.empty:
        print(f"⚠️ 没有找到 {symbol} 的数据")
        return

    # 过滤换手率大于指定值的数据
    if 'turnover' in df.columns:
        high_turnover = df[df['turnover'] > min_turnover].copy()
    else:
        print(f"⚠️ 数据源 {source} 没有换手率数据")
        return

    print(f"{'='*60}")
    print(f"股票代码: {symbol}")
    print(f"数据来源: {source.upper()}")
    print(f"查询时间: {start_date} 至 {end_date}")
    print(f"换手率阈值: > {min_turnover}%")
    print(f"{'='*60}")
    print(f"总交易日: {len(df)} 天")
    print(f"换手率 > {min_turnover}% 的天数: {len(high_turnover)} 天")
    if len(df) > 0:
        print(f"占比: {len(high_turnover)/len(df)*100:.1f}%")

    if not high_turnover.empty:
        print(f"\n换手率统计:")
        print(f"  平均换手率: {high_turnover['turnover'].mean():.2f}%")
        print(f"  最大换手率: {high_turnover['turnover'].max():.2f}%")
        print(f"  最小换手率: {high_turnover['turnover'].min():.2f}%")

        print(f"\n最近10次换手率 > {min_turnover}% 的记录:")
        print(high_turnover[['datetime', 'close', 'volume', 'turnover']].tail(10).to_string(index=False))
    else:
        print(f"\n没有找到换手率 > {min_turnover}% 的交易日")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    # 查询 Tushare 数据
    print("查询 Tushare 数据:")
    query_high_turnover(source="tushare")

    # 查询 AKShare 数据
    # print("\n查询 AKShare 数据:")
    # query_high_turnover(source="akshare")

