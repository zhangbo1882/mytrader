#!/usr/bin/env python3
"""
测试下载功能的简单脚本

使用方法:
    # 测试按股票代码下载（只下载前5只股票的最近1个月数据）
    python scripts/test_download.py --method code --count 5

    # 测试按交易日下载（只下载最近2个交易日）
    python scripts/test_download.py --method date --days 2

    # 查看帮助
    python scripts/test_download.py --help
"""
import argparse
from datetime import datetime, timedelta
import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_sources.tushare import TushareDB


def test_download_by_code(db, test_count=5):
    """
    测试按股票代码下载

    Args:
        db: TushareDB 实例
        test_count: 测试下载的股票数量
    """
    print("=" * 60)
    print(f"测试模式：按股票代码下载（前 {test_count} 只股票）")
    print("=" * 60)

    # 获取股票列表
    print("\n1️⃣  获取股票列表...")
    try:
        stock_list = db._retry_api_call(
            db.pro.stock_basic,
            exchange='',
            list_status='L',
            fields='ts_code,name,area,industry,list_date'
        )
        if stock_list is None or stock_list.empty:
            print("❌ 获取股票列表失败")
            return False

        test_stocks = stock_list['ts_code'].tolist()[:test_count]
        print(f"✅ 获取到 {len(test_stocks)} 只测试股票")
        print(f"   股票列表: {', '.join(test_stocks)}")
    except Exception as e:
        print(f"❌ 获取股票列表失败: {e}")
        return False

    # 计算测试日期范围（最近1个月）
    end_date = datetime.today()
    start_date = end_date - timedelta(days=30)
    start_date_str = start_date.strftime("%Y%m%d")
    end_date_str = end_date.strftime("%Y%m%d")

    print(f"\n2️⃣  测试下载（日期范围: {start_date_str} - {end_date_str}）")

    # 下载测试数据
    for i, ts_code in enumerate(test_stocks, 1):
        print(f"\n[{i}/{len(test_stocks)}] 测试下载 {ts_code}...")
        try:
            db.save_daily(ts_code, start_date_str, end_date_str, adjust='')
            print(f"✅ {ts_code} 下载成功")
        except Exception as e:
            print(f"❌ {ts_code} 下载失败: {e}")

    print("\n" + "=" * 60)
    print("测试完成！")
    print("=" * 60)
    return True


def test_download_by_date(db, test_days=2):
    """
    测试按交易日下载

    Args:
        db: TushareDB 实例
        test_days: 测试下载的交易日数量
    """
    print("=" * 60)
    print(f"测试模式：按交易日下载（最近 {test_days} 个交易日）")
    print("=" * 60)

    # 计算日期范围
    end_date = datetime.today()
    start_date = end_date - timedelta(days=30)  # 往前取30天，确保有足够的交易日
    start_date_str = start_date.strftime("%Y%m%d")
    end_date_str = end_date.strftime("%Y%m%d")

    # 获取交易日历
    print(f"\n1️⃣  获取交易日历（{start_date_str} - {end_date_str}）...")
    try:
        df_cal = db._retry_api_call(
            db.pro.trade_cal,
            exchange='SSE',
            is_open='1',
            start_date=start_date_str,
            end_date=end_date_str,
            fields='cal_date'
        )
        if df_cal is None or df_cal.empty:
            print("❌ 获取交易日历失败")
            return False

        all_dates = df_cal['cal_date'].tolist()
        test_dates = all_dates[-test_days:] if len(all_dates) >= test_days else all_dates
        print(f"✅ 获取到 {len(test_dates)} 个测试交易日")
        print(f"   交易日期: {', '.join(test_dates)}")
    except Exception as e:
        print(f"❌ 获取交易日历失败: {e}")
        return False

    print(f"\n2️⃣  测试下载")

    # 下载测试数据
    for i, date in enumerate(test_dates, 1):
        print(f"\n[{i}/{len(test_dates)}] 测试下载 {date}...")
        try:
            df = db._retry_api_call(db.pro.daily, trade_date=date)
            if df is not None and not df.empty:
                saved_count = db._save_daily_batch(df, date)
                print(f"✅ {date} 下载成功，保存了 {saved_count} 条记录")
            else:
                print(f"⚠️ {date} 无数据")
        except Exception as e:
            print(f"❌ {date} 下载失败: {e}")

    print("\n" + "=" * 60)
    print("测试完成！")
    print("=" * 60)
    return True


def main():
    parser = argparse.ArgumentParser(
        description='测试 Tushare 数据下载功能',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s --method code --count 5    # 测试按股票代码下载（5只股票）
  %(prog)s --method date --days 2     # 测试按交易日下载（2个交易日）
        """
    )
    parser.add_argument('--method', choices=['code', 'date'], default='code',
                       help='测试方法: code=按股票代码, date=按交易日 (默认: code)')
    parser.add_argument('--count', type=int, default=5,
                       help='测试的股票数量 (仅method=code, 默认: 5)')
    parser.add_argument('--days', type=int, default=2,
                       help='测试的交易日数量 (仅method=date, 默认: 2)')

    args = parser.parse_args()

    # 从环境变量或配置文件获取token
    try:
        from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH
    except ImportError:
        print("❌ 无法导入配置文件，请确保 config/settings.py 存在并包含 TUSHARE_TOKEN")
        return 1

    if not TUSHARE_TOKEN:
        print("❌ TUSHARE_TOKEN 未设置，请在 config/settings.py 中配置")
        return 1

    # 初始化数据库连接
    db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

    # 显示测试信息
    print("\n" + "=" * 60)
    print("Tushare 数据下载功能测试")
    print("=" * 60)
    print(f"测试方法: {'按股票代码' if args.method == 'code' else '按交易日'}")
    if args.method == 'code':
        print(f"测试数量: {args.count} 只股票")
    else:
        print(f"测试数量: {args.days} 个交易日")
    print("=" * 60)

    # 执行测试
    success = False
    if args.method == 'code':
        success = test_download_by_code(db, test_count=args.count)
    else:
        success = test_download_by_date(db, test_days=args.days)

    if success:
        print("\n✅ 测试通过！可以开始完整下载。")
        print("\n提示:")
        print("  - 如需下载全部A股，请运行: python scripts/download_all_stocks.py")
        return 0
    else:
        print("\n❌ 测试失败，请检查配置和网络连接。")
        return 1


if __name__ == '__main__':
    exit(main())
