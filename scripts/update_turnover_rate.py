#!/usr/bin/env python3
"""
获取所有股票的换手率并更新到数据库

从 AKShare 获取历史数据的换手率字段，并更新到数据库的 turnover 字段
"""
import sys
from pathlib import Path
import datetime

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import akshare as ak
import pandas as pd
from sqlalchemy import create_engine, text
from config.settings import TUSHARE_DB_PATH
from tqdm import tqdm


def get_stock_list():
    """获取所有A股股票列表"""
    print("正在获取股票列表...")
    try:
        stock_info = ak.stock_info_a_code_name()
        return stock_info['code'].tolist()
    except Exception as e:
        print(f"❌ 获取股票列表失败: {e}")
        return []


def get_date_range():
    """获取需要更新的日期范围"""
    # 从数据库获取最早的和最晚的日期
    engine = create_engine(f"sqlite:///{TUSHARE_DB_PATH}", echo=False)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT MIN(datetime), MAX(datetime) FROM bars"))
        min_date, max_date = result.fetchone()

    if not min_date or not max_date:
        # 如果数据库为空，使用默认范围
        min_date = "2020-01-01"
        max_date = datetime.datetime.now().strftime("%Y-%m-%d")

    return min_date, max_date


def update_turnover_for_stock(code, start_date, end_date, engine):
    """
    更新单只股票的换手率

    Args:
        code: 股票代码
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        engine: 数据库引擎

    Returns:
        成功更新的记录数
    """
    try:
        # 从 AKShare 获取数据
        start_str = start_date.replace('-', '')
        end_str = end_date.replace('-', '')

        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_str,
            end_date=end_str,
            adjust="qfq"  # 使用前复权数据
        )

        if df.empty:
            return 0

        # 更新数据库
        updated_count = 0
        with engine.begin() as conn:
            for _, row in df.iterrows():
                date = str(row['日期'])
                turnover_rate = float(row['换手率']) if pd.notna(row['换手率']) else None

                if turnover_rate is not None:
                    # 更新换手率
                    update_sql = """
                    UPDATE bars
                    SET turnover = :turnover
                    WHERE symbol = :symbol
                      AND datetime = :datetime
                    """
                    result = conn.execute(text(update_sql), {
                        'turnover': turnover_rate,
                        'symbol': code,
                        'datetime': date
                    })

                    if result.rowcount > 0:
                        updated_count += 1

        return updated_count

    except Exception as e:
        print(f"  ⚠️  {code} 更新失败: {e}")
        return 0


def update_all_turnover_rates():
    """更新所有股票的换手率"""
    print("=" * 80)
    print("更新所有股票的换手率到数据库")
    print("=" * 80)

    # 获取股票列表
    stock_list = get_stock_list()
    if not stock_list:
        print("❌ 没有获取到股票列表")
        return

    print(f"✅ 获取到 {len(stock_list)} 支股票")

    # 获取日期范围
    start_date, end_date = get_date_range()
    print(f"📅 日期范围: {start_date} 至 {end_date}")

    # 创建数据库连接
    engine = create_engine(f"sqlite:///{TUSHARE_DB_PATH}", echo=False)

    # 统计信息
    total_updated = 0
    failed_stocks = []

    # 逐个更新股票的换手率
    print(f"\n开始更新...")
    for code in tqdm(stock_list, desc="更新进度"):
        updated = update_turnover_for_stock(code, start_date, end_date, engine)
        total_updated += updated
        if updated == 0:
            failed_stocks.append(code)

    # 输出结果
    print("\n" + "=" * 80)
    print("更新完成！")
    print("=" * 80)
    print(f"✅ 成功更新记录数: {total_updated:,}")
    print(f"❌ 失败或无数据的股票: {len(failed_stocks)}")

    if failed_stocks and len(failed_stocks) <= 10:
        print(f"\n失败股票列表: {', '.join(failed_stocks)}")
    elif failed_stocks:
        print(f"\n部分失败股票: {', '.join(failed_stocks[:10])} ...")

    # 验证更新结果
    print("\n验证更新结果...")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN turnover IS NOT NULL THEN 1 END) as has_turnover,
                AVG(CASE WHEN turnover IS NOT NULL THEN turnover END) as avg_turnover
            FROM bars
        """))
        row = result.fetchone()

        print(f"总记录数: {row[0]:,}")
        print(f"有换手率的记录: {row[1]:,}")
        print(f"换手率覆盖率: {row[1]/row[0]*100:.2f}%")
        if row[2]:
            print(f"平均换手率: {row[2]:.2f}%")

    print("\n" + "=" * 80)


def update_missing_turnover():
    """只更新缺失的换手率数据"""
    print("=" * 80)
    print("更新缺失的换手率数据")
    print("=" * 80)

    engine = create_engine(f"sqlite:///{TUSHARE_DB_PATH}", echo=False)

    # 查找缺失换手率的记录
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT DISTINCT symbol
            FROM bars
            WHERE turnover IS NULL OR turnover = 0
            ORDER BY symbol
        """))
        missing_stocks = [row[0] for row in result]

    if not missing_stocks:
        print("✅ 所有记录都有换手率数据！")
        return

    print(f"📊 发现 {len(missing_stocks)} 支股票缺失换手率数据")

    # 获取日期范围
    start_date, end_date = get_date_range()
    print(f"📅 日期范围: {start_date} 至 {end_date}")

    # 统计信息
    total_updated = 0

    # 逐个更新
    print(f"\n开始更新...")
    for code in tqdm(missing_stocks, desc="更新进度"):
        updated = update_turnover_for_stock(code, start_date, end_date, engine)
        total_updated += updated

    print("\n" + "=" * 80)
    print(f"✅ 成功更新记录数: {total_updated:,}")
    print("=" * 80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="更新股票换手率到数据库")
    parser.add_argument(
        '--mode',
        choices=['all', 'missing'],
        default='missing',
        help='更新模式: all=全部更新, missing=只更新缺失的（默认）'
    )

    args = parser.parse_args()

    if args.mode == 'all':
        update_all_turnover_rates()
    else:
        update_missing_turnover()
