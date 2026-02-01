#!/usr/bin/env python3
"""
下载上市公司财务数据脚本

使用方法:
    # 下载单只股票
    python scripts/download_financial.py --stocks 000001

    # 下载多只股票
    python scripts/download_financial.py --stocks 000001,600382

    # 下载所有股票
    python scripts/download_financial.py --all

    # 增量更新（只下载最新数据）
    python scripts/download_financial.py --incremental

    # 查看帮助
    python scripts/download_financial.py --help
"""

import argparse
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_sources.tushare import TushareDB


def load_token():
    """从环境变量或配置文件加载 Tushare Token"""
    # 优先从环境变量读取
    token = os.getenv('TUSHARE_TOKEN')
    if token:
        return token

    # 从配置文件读取
    try:
        from config.settings import TUSHARE_TOKEN
        return TUSHARE_TOKEN
    except ImportError:
        print("❌ 找不到 Tushare Token！")
        print("请设置环境变量 TUSHARE_TOKEN 或在 config/settings.py 中配置 TUSHARE_TOKEN")
        sys.exit(1)


def download_stocks(stock_list, db, incremental=False):
    """
    下载指定股票列表的财务数据

    Args:
        stock_list: 股票代码列表
        db: TushareDB 实例
        incremental: 是否增量更新
    """
    # 计算最近2年的日期范围
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)  # 2年 = 365*2
    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = end_date.strftime('%Y%m%d')

    print(f"日期范围: {start_date_str} - {end_date_str}（最近2年）")

    success_count = 0
    failed_count = 0
    skipped_count = 0
    total = len(stock_list)

    print(f"\n{'='*60}")
    print(f"开始下载 {total} 只股票的财务数据")
    print(f"{'='*60}")

    for i, ts_code in enumerate(stock_list, 1):
        try:
            # 增量更新：检查最新日期
            if incremental:
                latest_date = db.get_latest_financial_date(ts_code, 'income')
                if latest_date:
                    print(f"\n[{i}/{total}] {ts_code} 已有数据（最新: {latest_date}），跳过")
                    skipped_count += 1
                    continue

            # 显示进度
            print(f"\n{'='*60}")
            print(f"[{i}/{total}] 正在下载 {ts_code}")
            print(f"{'='*60}")

            # 下载所有财务数据（传递日期范围，限制最近2年）
            count = db.save_all_financial(ts_code, start_date=start_date_str, end_date=end_date_str)
            if count > 0:
                success_count += 1
            else:
                failed_count += 1

        except KeyboardInterrupt:
            print("\n\n⚠️  用户中断下载")
            print(f"已下载: {success_count} | 失败: {failed_count} | 跳过: {skipped_count}")
            sys.exit(0)
        except Exception as e:
            print(f"❌ {ts_code} 处理失败: {e}")
            failed_count += 1

    # 输出统计
    print(f"\n{'='*60}")
    print(f"下载完成:")
    print(f"  总计: {total} 只股票")
    print(f"  成功: {success_count}")
    print(f"  失败: {failed_count}")
    print(f"  跳过: {skipped_count}")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(
        description='下载上市公司财务数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s --stocks 000001              # 下载单只股票
  %(prog)s --stocks 000001,600382       # 下载多只股票
  %(prog)s --all                        # 下载所有股票
  %(prog)s --incremental                # 增量更新
        """
    )

    parser.add_argument(
        '--stocks',
        type=str,
        help='股票列表（逗号分隔），如: 000001,600382'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='下载所有A股的财务数据'
    )
    parser.add_argument(
        '--incremental',
        action='store_true',
        help='增量更新（只下载没有数据的股票）'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='开始日期（格式 YYYYMMDD）'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='结束日期（格式 YYYYMMDD）'
    )

    args = parser.parse_args()

    # 检查参数
    if not args.stocks and not args.all:
        parser.print_help()
        print("\n❌ 错误: 必须指定 --stocks 或 --all 参数")
        sys.exit(1)

    # 加载 Token
    token = load_token()
    print(f"Token: {token[:10]}...")

    # 初始化数据库
    try:
        db = TushareDB(token=token)
    except Exception as e:
        print(f"❌ 初始化数据库失败: {e}")
        sys.exit(1)

    # 检查权限
    print("\n检查财务数据接口权限...")
    print("提示: 财务数据接口需要 2000+ 积分")
    print("访问 https://tushare.pro 查看积分规则\n")

    # 获取股票列表
    if args.all:
        print("正在获取股票列表...")
        try:
            stock_df = db.pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,name'
            )

            # 过滤ST股（过滤名称中包含ST、*ST、SST、S*ST的股票）
            total_before_filter = len(stock_df)
            stock_df = stock_df[~stock_df['name'].str.contains('ST', na=False)]
            stock_list = stock_df['ts_code'].tolist()

            # 统计过滤情况
            filtered_count = total_before_filter - len(stock_df)
            print(f"总共: {total_before_filter} 只股票")
            print(f"过滤ST股: {filtered_count} 只")
            print(f"实际下载: {len(stock_list)} 只股票\n")
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            sys.exit(1)
    else:
        # 解析股票列表
        stock_list = [s.strip() for s in args.stocks.split(',')]

    # 下载财务数据
    download_stocks(stock_list, db, incremental=args.incremental)


if __name__ == '__main__':
    main()
