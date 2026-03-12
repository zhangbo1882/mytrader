#!/usr/bin/env python3
"""
更新缺失end_bal_cash字段股票的财务数据

使用方法:
    python scripts/update_missing_cashflow_stocks.py
    python scripts/update_missing_cashflow_stocks.py --limit 10  # 限制更新数量
    python scripts/update_missing_cashflow_stocks.py --dry-run   # 仅查看要更新的股票
"""

import argparse
import sys
import os
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import sqlite3
from src.data_sources.tushare import TushareDB


def get_missing_stocks():
    """获取2020年后缺失end_bal_cash的股票列表"""
    conn = sqlite3.connect('data/tushare_data.db')

    import pandas as pd
    df = pd.read_sql("""
        SELECT DISTINCT ts_code
        FROM cashflow
        WHERE end_bal_cash IS NULL
          AND end_date >= '20200101'
        ORDER BY ts_code
    """, conn)

    conn.close()
    return df['ts_code'].tolist()


def load_token():
    """从环境变量或配置文件加载 Tushare Token"""
    token = os.getenv('TUSHARE_TOKEN')
    if token:
        return token

    try:
        from config.settings import TUSHARE_TOKEN
        return TUSHARE_TOKEN
    except ImportError:
        print("❌ 找不到 Tushare Token！")
        print("请设置环境变量 TUSHARE_TOKEN 或在 config/settings.py 中配置 TUSHARE_TOKEN")
        sys.exit(1)


def update_stocks(stock_list, db, limit=None):
    """
    更新指定股票列表的财务数据

    Args:
        stock_list: 股票代码列表
        db: TushareDB 实例
        limit: 限制更新数量（用于测试）
    """
    if limit:
        stock_list = stock_list[:limit]

    total = len(stock_list)
    success_count = 0
    failed_count = 0
    total_records = 0

    print(f"\n{'='*70}")
    print(f"开始更新 {total} 只股票的财务数据（全量更新）")
    print(f"{'='*70}\n")

    for i, ts_code in enumerate(stock_list, 1):
        try:
            print(f"\n[{i}/{total}] 正在更新 {ts_code}")

            # 全量更新（不限制日期范围）
            result = db.save_all_financial(ts_code)

            if result.get('total_records', 0) > 0:
                success_count += 1
                total_records += result['total_records']

                # 显示现金回填情况
                if result.get('cashflow_backfill'):
                    backfill = result['cashflow_backfill']
                    print(f"  end_bal_cash 回填: {backfill['filled']}/{backfill['missing_candidates']}")
            else:
                failed_count += 1
                print(f"  ⚠️ 无数据返回")

        except KeyboardInterrupt:
            print("\n\n⚠️ 用户中断更新")
            print(f"已更新: {success_count} | 失败: {failed_count} | 总记录: {total_records}")
            sys.exit(0)
        except Exception as e:
            print(f"  ❌ {ts_code} 更新失败: {e}")
            failed_count += 1

    # 输出统计
    print(f"\n{'='*70}")
    print(f"更新完成:")
    print(f"  总计: {total} 只股票")
    print(f"  成功: {success_count}")
    print(f"  失败: {failed_count}")
    print(f"  总记录数: {total_records}")
    print(f"{'='*70}")

    return {
        'total': total,
        'success': success_count,
        'failed': failed_count,
        'total_records': total_records
    }


def main():
    parser = argparse.ArgumentParser(
        description='更新缺失end_bal_cash字段股票的财务数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s                    # 更新所有缺失数据的股票
  %(prog)s --limit 10         # 仅更新前10只股票（测试用）
  %(prog)s --dry-run          # 仅查看要更新的股票列表
        """
    )

    parser.add_argument(
        '--limit',
        type=int,
        help='限制更新数量（用于测试）'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='仅查看要更新的股票列表，不执行更新'
    )

    args = parser.parse_args()

    # 获取缺失数据的股票列表
    print("正在查询缺失 end_bal_cash 的股票...")
    stock_list = get_missing_stocks()
    print(f"共找到 {len(stock_list)} 只股票需要更新")

    if args.dry_run:
        print("\n要更新的股票列表:")
        for i, code in enumerate(stock_list, 1):
            print(f"  {i:3d}. {code}")
        return

    # 加载 Token
    token = load_token()
    print(f"Token: {token[:10]}...")

    # 初始化数据库
    try:
        db = TushareDB(token=token)
    except Exception as e:
        print(f"❌ 初始化数据库失败: {e}")
        sys.exit(1)

    # 更新财务数据
    update_stocks(stock_list, db, limit=args.limit)


if __name__ == '__main__':
    main()
