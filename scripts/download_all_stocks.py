#!/usr/bin/env python3
"""
下载全部A股历史数据脚本

使用方法:
    # 增量更新（只下载最新数据，推荐日常使用）
    python scripts/download_all_stocks.py --incremental

    # 下载最近2年数据（默认，从2024-01-01开始）
    python scripts/download_all_stocks.py

    # 指定日期范围
    python scripts/download_all_stocks.py --start-date 20230101 --end-date 20240101

    # 使用快速模式（按交易日，无复权数据）
    python scripts/download_all_stocks.py --method date

    # 查看帮助
    python scripts/download_all_stocks.py --help
"""
import argparse
from datetime import datetime, timedelta
import sys
import os
import pandas as pd

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_sources.tushare import TushareDB


def get_incremental_start_date(db, stock_list, end_date):
    """
    获取增量更新的开始日期

    对于每只股票，查询其最新数据的日期，然后从下一天开始下载
    如果某只股票没有数据，则使用默认的开始日期

    Args:
        db: TushareDB 实例
        stock_list: 股票代码列表
        end_date: 结束日期

    Returns:
        dict: {股票代码: 开始日期}
    """
    result = {}
    end_date_dt = pd.to_datetime(end_date)

    print("🔍 检查本地数据最新日期...")

    for ts_code in stock_list:
        try:
            code = ts_code.split('.')[0]

            # 查询该股票的最新数据日期
            query = """
            SELECT datetime FROM bars
            WHERE symbol = :symbol AND interval = '1d'
            ORDER BY datetime DESC LIMIT 1
            """
            with db.engine.connect() as conn:
                df = pd.read_sql_query(
                    query,
                    conn,
                    params={"symbol": code}
                )

            if not df.empty:
                latest_date = pd.to_datetime(df['datetime'].iloc[0])
                # 从最新日期的下一天开始
                start_date_dt = latest_date + timedelta(days=1)
                # 转换为 YYYYMMDD 格式
                start_date = start_date_dt.strftime("%Y%m%d")

                # 只在需要更新时才添加到结果中
                if start_date_dt <= end_date_dt:
                    result[ts_code] = start_date
            else:
                # 无数据，使用默认开始日期
                result[ts_code] = None

        except Exception as e:
            print(f"  ⚠️  {ts_code} 检查失败: {e}")
            result[ts_code] = None

    # 统计
    need_update = sum(1 for v in result.values() if v is not None)
    no_data = sum(1 for v in result.values() if v is None)

    print(f"✅ 检查完成: {need_update} 只需要更新, {no_data} 只无数据")

    return result


def main():
    parser = argparse.ArgumentParser(
        description='下载全部A股历史数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s --incremental                      # 增量更新（只下载最新数据）
  %(prog)s --incremental --stocks 000001,600000  # 更新指定股票
  %(prog)s --incremental --stocks stocks.txt  # 从文件读取股票列表
  %(prog)s                                    # 下载最近2年数据（默认）
  %(prog)s --method date                      # 快速下载（不含复权）
  %(prog)s --start-date 20230101             # 指定开始日期
  %(prog)s --start-date 20230101 --end-date 20240101  # 指定日期范围
        """
    )
    parser.add_argument('--method', choices=['code', 'date'], default='code',
                       help='下载方法: code=按股票代码(慢但完整,含复权), date=按交易日(快但无复权) (默认: code)')
    parser.add_argument('--start-date', default='20240101',
                       help='开始日期 (YYYYMMDD)，默认为20240101。使用--incremental时自动检测')
    parser.add_argument('--end-date', default=None,
                       help='结束日期 (YYYYMMDD)，默认为今天')
    parser.add_argument('--adjust', choices=['', 'qfq', 'hfq'], default='',
                       help='复权类型 (仅method=code时有效): qfq=前复权, hfq=后复权, 空=不复权 (默认: 空)')
    parser.add_argument('--incremental', action='store_true',
                       help='增量更新模式：只下载每只股票最新缺失的数据（推荐日常使用）')
    parser.add_argument('--stocks', default=None,
                       help='股票列表：逗号分隔的代码(如000001,600000)或文件路径(每行一个代码)')

    args = parser.parse_args()

    # 处理股票列表参数
    stock_list = None
    if args.stocks:
        if ',' in args.stocks:
            # 逗号分隔的股票代码
            stock_list = [s.strip() for s in args.stocks.split(',') if s.strip()]
        else:
            # 从文件读取
            try:
                with open(args.stocks, 'r') as f:
                    stock_list = [line.strip() for line in f if line.strip()]
                print(f"📋 从文件读取了 {len(stock_list)} 只股票")
            except FileNotFoundError:
                print(f"❌ 找不到文件: {args.stocks}")
                return 1
            except Exception as e:
                print(f"❌ 读取文件失败: {e}")
                return 1

    # 如果没有指定结束日期，使用今天
    if args.end_date is None:
        args.end_date = datetime.today().strftime("%Y%m%d")

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

    # 显示下载信息
    print("=" * 60)
    if args.incremental:
        print("全部A股数据增量更新")
    else:
        print("全部A股数据下载")
    print("=" * 60)

    if args.incremental:
        print(f"更新模式: 增量更新（只下载最新数据）")
        print(f"默认开始日期: {args.start_date}（用于无数据的股票）")
    else:
        print(f"开始日期: {args.start_date}")

    print(f"结束日期: {args.end_date}")
    print(f"下载方式: {'按股票代码（包含复权）' if args.method == 'code' else '按交易日（快速）'}")
    print(f"复权类型: {args.adjust if args.adjust else '不复权'}")
    print("=" * 60)

    # 根据方法调用不同的函数
    if args.method == 'code':
        if args.incremental:
            print("\n📊 使用增量更新模式（按股票代码）")
            if stock_list:
                print(f"📋 指定股票列表: {len(stock_list)} 只")
            print("⏱️  预计耗时较短（只下载缺失的数据）")
            print("⚠️  API频率限制：每分钟50次，每次间隔1.3秒")
            print()
            stats = db.save_all_stocks_by_code_incremental(
                default_start_date=args.start_date,
                end_date=args.end_date,
                adjust=args.adjust if args.adjust else None,
                stock_list=stock_list
            )
        else:
            print("\n📊 使用按股票代码方式下载（包含复权数据）")
            print("⏱️  预计耗时较长（约2-3小时）")
            print("⚠️  API频率限制：每分钟50次，每次间隔1.3秒")
            print()
            stats = db.save_all_stocks_by_code(
                start_date=args.start_date,
                end_date=args.end_date,
                adjust=args.adjust if args.adjust else None
            )
    else:
        print("\n📊 使用按交易日方式下载（不含复权数据）")
        print("⏱️  预计耗时较短（约10-20分钟）")
        print("⚠️  API频率限制：每分钟50次，每次间隔1.3秒")
        print()
        stats = db.save_all_stocks_by_date(
            start_date=args.start_date,
            end_date=args.end_date
        )

    if stats:
        print("\n✅ 下载完成！")
        return 0
    else:
        print("\n❌ 下载失败！")
        return 1


if __name__ == '__main__':
    exit(main())
