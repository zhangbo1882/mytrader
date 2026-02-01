#!/usr/bin/env python3
"""
获取换手率数据 - 优化版本

支持分批更新、断点续传，避免API频率限制
"""
import sys
from pathlib import Path
import datetime
import time

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import akshare as ak
import pandas as pd
from sqlalchemy import create_engine, text
from config.settings import TUSHARE_DB_PATH


def update_recent_turnover(days=30):
    """
    只更新最近N天的换手率数据（推荐）

    Args:
        days: 更新最近多少天的数据，默认30天
    """
    print("=" * 80)
    print(f"更新最近 {days} 天的换手率数据")
    print("=" * 80)

    # 计算日期范围
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=days)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    print(f"📅 日期范围: {start_str} 至 {end_str}")

    # 获取需要更新的股票列表（从数据库中实际存在的股票）
    engine = create_engine(f"sqlite:///{TUSHARE_DB_PATH}", echo=False)

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT DISTINCT symbol
            FROM bars
            WHERE datetime >= :start_date
            ORDER BY symbol
        """), {"start_date": start_str})

        stock_list = [row[0] for row in result]

    print(f"✅ 需要更新 {len(stock_list)} 支股票")

    if not stock_list:
        print("❌ 没有需要更新的股票")
        return

    # 统计信息
    total_updated = 0
    failed_count = 0

    # 逐个更新
    print(f"\n开始更新...")
    for i, code in enumerate(stock_list, 1):
        try:
            # 使用正确的日期格式
            start_date_fmt = start_date.strftime("%Y%m%d")
            end_date_fmt = end_date.strftime("%Y%m%d")

            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date_fmt,
                end_date=end_date_fmt,
                adjust="qfq"
            )

            if df.empty:
                continue

            # 批量更新
            with engine.begin() as conn:
                for _, row_data in df.iterrows():
                    date = str(row_data['日期'])
                    turnover_rate = float(row_data['换手率']) if pd.notna(row_data['换手率']) else None

                    if turnover_rate is not None:
                        conn.execute(text("""
                            UPDATE bars
                            SET turnover = :turnover
                            WHERE symbol = :symbol AND datetime = :datetime
                        """), {
                            'turnover': turnover_rate,
                            'symbol': code,
                            'datetime': date
                        })

            total_updated += len(df)
            print(f"[{i}/{len(stock_list)}] ✅ {code}: 更新 {len(df)} 条记录")

            # 避免API频率限制，每10个股票暂停一下
            if i % 10 == 0:
                time.sleep(1)

        except Exception as e:
            failed_count += 1
            print(f"[{i}/{len(stock_list)}] ⚠️  {code}: 失败 - {str(e)[:50]}")
            # 遇到错误暂停一下
            time.sleep(2)

    # 验证结果
    print("\n" + "=" * 80)
    print("更新完成！")
    print("=" * 80)
    print(f"✅ 成功更新记录数: {total_updated:,}")
    print(f"❌ 失败股票数: {failed_count}")

    # 验证
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN turnover IS NOT NULL THEN 1 END) as has_turnover
            FROM bars
            WHERE datetime >= :start_date
        """), {"start_date": start_str})

        row = result.fetchone()
        print(f"\n最近{days}天数据验证:")
        print(f"  总记录数: {row[0]:,}")
        print(f"  有换手率: {row[1]:,}")
        print(f"  覆盖率: {row[1]/row[0]*100:.2f}%")


def show_update_status():
    """显示更新状态"""
    engine = create_engine(f"sqlite:///{TUSHARE_DB_PATH}", echo=False)

    with engine.connect() as conn:
        # 总体统计
        result = conn.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN turnover IS NOT NULL THEN 1 END) as has_turnover
            FROM bars
        """))
        row = result.fetchone()

        print("数据库换手率统计:")
        print(f"  总记录数: {row[0]:,}")
        print(f"  已有换手率: {row[1]:,}")
        print(f"  覆盖率: {row[1]/row[0]*100:.2f}%")

        # 按日期统计
        result = conn.execute(text("""
            SELECT
                datetime,
                COUNT(*) as total,
                COUNT(CASE WHEN turnover IS NOT NULL THEN 1 END) as has_turnover
            FROM bars
            GROUP BY datetime
            ORDER BY datetime DESC
            LIMIT 10
        """))

        print("\n最近10天的覆盖情况:")
        for row in result:
            date, total, has_turnover = row
            coverage = has_turnover / total * 100 if total > 0 else 0
            print(f"  {date}: {has_turnover:6d}/{total:6d} ({coverage:5.1f}%)")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="更新股票换手率（优化版）")
    parser.add_argument(
        '--days',
        type=int,
        default=30,
        help='更新最近多少天的数据（默认30天）'
    )
    parser.add_argument(
        '--status',
        action='store_true',
        help='只显示更新状态，不更新数据'
    )

    args = parser.parse_args()

    if args.status:
        show_update_status()
    else:
        update_recent_turnover(days=args.days)
