#!/usr/bin/env python3
"""
测试财务数据下载和查询功能

使用方法:
    python scripts/test_financial.py
"""

import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_sources.tushare import TushareDB
from src.data_sources.query.financial_query import FinancialQuery


def test_financial_download():
    """测试财务数据下载功能"""
    print("\n" + "="*60)
    print("测试财务数据下载功能")
    print("="*60)

    # 加载 Token
    import os
    token = os.getenv('TUSHARE_TOKEN')
    if not token:
        try:
            from config.settings import TUSHARE_TOKEN
            token = TUSHARE_TOKEN
        except ImportError:
            print("❌ 找不到 Tushare Token")
            return

    print(f"Token: {token[:10]}...")

    # 初始化数据库
    db = TushareDB(token=token)

    # 测试股票代码
    test_symbol = "000001.SZ"  # 平安银行

    print(f"\n测试股票: {test_symbol}")
    print("-"*60)

    # 下载财务数据
    try:
        count = db.save_all_financial(test_symbol)
        print(f"\n✅ 下载成功，共 {count} 条记录")
    except Exception as e:
        print(f"\n❌ 下载失败: {e}")
        return


def test_financial_query():
    """测试财务数据查询功能"""
    print("\n" + "="*60)
    print("测试财务数据查询功能")
    print("="*60)

    # 初始化查询
    fq = FinancialQuery("data/tushare_data.db")

    # 测试股票代码
    test_symbol = "000001"

    print(f"\n测试股票: {test_symbol}")
    print("-"*60)

    # 1. 查询利润表
    print("\n1. 查询利润表数据")
    income_df = fq.query_income(test_symbol)
    if not income_df.empty:
        print(f"✅ 利润表记录数: {len(income_df)}")
        print(income_df[['end_date', 'total_revenue', 'n_income']].head(3))
    else:
        print("⚠️  无利润表数据")

    # 2. 查询资产负债表
    print("\n2. 查询资产负债表数据")
    balance_df = fq.query_balancesheet(test_symbol)
    if not balance_df.empty:
        print(f"✅ 资产负债表记录数: {len(balance_df)}")
        print(balance_df[['end_date', 'total_assets', 'total_liability']].head(3))
    else:
        print("⚠️  无资产负债表数据")

    # 3. 查询现金流量表
    print("\n3. 查询现金流量表数据")
    cashflow_df = fq.query_cashflow(test_symbol)
    if not cashflow_df.empty:
        print(f"✅ 现金流量表记录数: {len(cashflow_df)}")
        print(cashflow_df[['end_date', 'net_cash_flows_oper_act']].head(3))
    else:
        print("⚠️  无现金流量表数据")

    # 4. 查询最新财报日期
    print("\n4. 查询最新财报日期")
    latest = fq.get_latest_report_date(test_symbol)
    if latest:
        print(f"✅ 最新财报: 公告日期 {latest.get('ann_date')}, 报告期 {latest.get('end_date')}")
    else:
        print("⚠️  无最新财报数据")

    # 5. 财务摘要
    print("\n5. 获取财务摘要")
    summary = fq.get_financial_summary(test_symbol)
    if summary:
        print("✅ 财务摘要:")
        if 'income' in summary:
            print(f"  营业总收入: {summary['income'].get('total_revenue')}")
            print(f"  净利润: {summary['income'].get('n_income')}")
        if 'balance' in summary:
            print(f"  总资产: {summary['balance'].get('total_assets')}")
            print(f"  总负债: {summary['balance'].get('total_liability')}")
        if 'cashflow' in summary:
            print(f"  经营活动现金流: {summary['cashflow'].get('net_cash_flows_oper_act')}")
    else:
        print("⚠️  无财务摘要数据")

    # 6. 列出所有财务表
    print("\n6. 列出所有财务表")
    tables = fq.list_financial_tables()
    print(f"✅ 共有 {len(tables)} 张财务表")
    for table in tables[:10]:  # 只显示前10个
        print(f"  - {table}")

    # 7. 获取财务统计
    print("\n7. 获取财务数据统计")
    stats_df = fq.get_financial_stats()
    if not stats_df.empty:
        print(f"✅ 统计信息:")
        print(f"  总表数: {len(stats_df)}")
        print(f"  总记录数: {stats_df['record_count'].sum()}")
        print("\n按类型统计:")
        print(stats_df.groupby('table_type')['record_count'].sum())


def test_table_existence():
    """测试表是否正确创建"""
    print("\n" + "="*60)
    print("测试表结构")
    print("="*60)

    import sqlite3
    conn = sqlite3.connect("data/tushare_data.db")
    cursor = conn.cursor()

    # 检查表是否存在
    tables_to_check = [
        'income_000001',
        'balancesheet_000001',
        'cashflow_000001'
    ]

    for table in tables_to_check:
        cursor.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
        )
        result = cursor.fetchone()
        if result:
            print(f"✅ 表 {table} 存在")

            # 获取列信息
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            print(f"   列数: {len(columns)}")

            # 获取记录数
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"   记录数: {count}")
        else:
            print(f"⚠️  表 {table} 不存在")

    conn.close()


def main():
    """主测试函数"""
    print("\n" + "="*60)
    print("财务数据功能测试")
    print("="*60)

    # 选择测试类型
    print("\n请选择测试类型:")
    print("1. 测试数据下载（需要 Tushare Token 和 2000+ 积分）")
    print("2. 测试数据查询（需要先有数据）")
    print("3. 测试表结构")
    print("4. 全部测试")

    choice = input("\n请输入选项 (1-4): ").strip()

    if choice == '1':
        test_financial_download()
    elif choice == '2':
        test_financial_query()
    elif choice == '3':
        test_table_existence()
    elif choice == '4':
        test_financial_download()
        test_table_existence()
        test_financial_query()
    else:
        print("无效选项")


if __name__ == '__main__':
    main()
