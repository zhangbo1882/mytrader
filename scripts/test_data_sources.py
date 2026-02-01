#!/usr/bin/env python3
"""
测试 akshare 和 tushare 数据源的股票数据字段差异
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import tushare as ts
import akshare as ak
import pandas as pd
from config.settings import TUSHARE_TOKEN


def test_data_sources():
    """测试两个数据源的字段差异"""
    print("=" * 80)
    print("测试 akshare 和 tushare 数据源的字段差异")
    print("=" * 80)

    # 测试股票代码
    test_code = "000001"

    print(f"\n测试股票: {test_code}")
    print("-" * 80)

    # 1. 测试 akshare
    print("\n【1】AKShare 数据源")
    print("-" * 80)
    try:
        # 获取股票历史行情数据
        ak_stock = ak.stock_zh_a_hist(symbol=test_code, period="daily", start_date="20240101", end_date="20240105", adjust="qfq")
        print(f"✅ 成功获取数据，共 {len(ak_stock)} 条记录")
        print(f"\n字段列表 ({len(ak_stock.columns)} 个):")
        for i, col in enumerate(ak_stock.columns, 1):
            print(f"  {i:2d}. {col}")

        print(f"\n前2条数据示例:")
        print(ak_stock.head(2).to_string())

    except Exception as e:
        print(f"❌ akshare 获取数据失败: {e}")

    # 2. 测试 tushare
    print("\n" + "=" * 80)
    print("【2】Tushare 数据源")
    print("-" * 80)
    try:
        ts.set_token(TUSHARE_TOKEN)
        pro = ts.pro_api()

        # 获取股票日线数据
        ts_code = f"{test_code}.SZ"
        ts_stock = pro.daily(ts_code=ts_code, start_date='20240101', end_date='20240105')

        print(f"✅ 成功获取数据，共 {len(ts_stock)} 条记录")
        print(f"\n字段列表 ({len(ts_stock.columns)} 个):")
        for i, col in enumerate(ts_stock.columns, 1):
            print(f"  {i:2d}. {col}")

        print(f"\n前2条数据示例:")
        print(ts_stock.head(2).to_string())

    except Exception as e:
        print(f"❌ tushare 获取数据失败: {e}")

    # 3. 字段对比
    print("\n" + "=" * 80)
    print("【3】字段对比分析")
    print("-" * 80)

    try:
        ak_columns = set(ak_stock.columns)
        ts_columns = set(ts_stock.columns)

        print(f"\nAKShare 独有字段 ({len(ak_columns - ts_columns)} 个):")
        if ak_columns - ts_columns:
            for col in sorted(ak_columns - ts_columns):
                print(f"  - {col}")
        else:
            print("  无")

        print(f"\nTushare 独有字段 ({len(ts_columns - ak_columns)} 个):")
        if ts_columns - ak_columns:
            for col in sorted(ts_columns - ak_columns):
                print(f"  - {col}")
        else:
            print("  无")

        print(f"\n共同字段 ({len(ak_columns & ts_columns)} 个):")
        if ak_columns & ts_columns:
            for col in sorted(ak_columns & ts_columns):
                print(f"  - {col}")
        else:
            print("  无")

        # 4. 字段映射关系
        print("\n" + "-" * 80)
        print("【4】常见字段映射关系:")
        print("-" * 80)

        mappings = [
            ("日期", "日期", "日期", "trade_date"),
            ("开盘价", "开盘价", "开盘", "open"),
            ("最高价", "最高价", "最高", "high"),
            ("最低价", "最低价", "最低", "low"),
            ("收盘价", "收盘价", "收盘", "close"),
            ("成交量", "成交量", "成交量", "vol"),
            ("成交额", "成交额", "成交额", "amount"),
        ]

        for cn_name, ak_name, ts_name_cn, ts_name in mappings:
            ak_col = [c for c in ak_stock.columns if c in ak_name]
            ts_col = [c for c in ts_stock.columns if c in ts_name]

            ak_str = ak_col[0] if ak_col else "❌ 无对应"
            ts_str = ts_col[0] if ts_col else "❌ 无对应"

            print(f"  {cn_name:8s}: AKShare={ak_str:15s} Tushare={ts_str}")

    except Exception as e:
        print(f"❌ 字段对比失败: {e}")

    # 5. 数据质量检查
    print("\n" + "=" * 80)
    print("【5】数据质量检查")
    print("-" * 80)

    try:
        print("\nAKShare 数据类型:")
        print(ak_stock.dtypes.to_string())

        print("\nTushare 数据类型:")
        print(ts_stock.dtypes.to_string())

    except Exception as e:
        print(f"❌ 数据质量检查失败: {e}")

    print("\n" + "=" * 80)
    print("测试完成！")
    print("=" * 80)


if __name__ == "__main__":
    test_data_sources()
