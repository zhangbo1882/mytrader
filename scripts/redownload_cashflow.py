#!/usr/bin/env python3
"""
重新下载缺失 end_bal_cash 股票的现金流数据

使用方法:
    python scripts/redownload_cashflow.py
    python scripts/redownload_cashflow.py --since 2020-01-01
    python scripts/redownload_cashflow.py --limit 100  # 限制数量（测试用）
"""

import argparse
import sys
import os
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import sqlite3
from src.data_sources.tushare import TushareDB


def get_stocks_needing_redownload(
    db_path: str,
    since: str | None = None,
    stocks: list[str] | None = None,
) -> list[str]:
    """获取主板非ST且需要重新下载现金流数据的股票列表"""
    conn = sqlite3.connect(db_path)
    try:
        sql = """
            SELECT DISTINCT c.ts_code
            FROM cashflow c
            JOIN stock_basic_info s ON s.ts_code = c.ts_code
            WHERE c.end_bal_cash IS NULL
              AND COALESCE(s.name, '') NOT LIKE '%ST%'
              AND (
                c.ts_code GLOB '000*.SZ' OR
                c.ts_code GLOB '001*.SZ' OR
                c.ts_code GLOB '002*.SZ' OR
                c.ts_code GLOB '600*.SH' OR
                c.ts_code GLOB '601*.SH' OR
                c.ts_code GLOB '603*.SH' OR
                c.ts_code GLOB '605*.SH'
              )
        """
        params = []
        if since:
            sql += " AND c.end_date >= ?"
            params.append(since.replace('-', ''))
        if stocks:
            placeholders = ','.join('?' * len(stocks))
            sql += f" AND c.ts_code IN ({placeholders})"
            params.extend(stocks)
        sql += " ORDER BY c.ts_code"
        rows = conn.execute(sql, params).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def normalize_stock_codes(raw_codes: list[str]) -> list[str]:
    normalized = []
    for s in raw_codes:
        code = s.strip().upper()
        if not code:
            continue
        if '.' not in code:
            code = code.zfill(6)
            if code.startswith('6'):
                normalized.append(f'{code}.SH')
            else:
                normalized.append(f'{code}.SZ')
        else:
            normalized.append(code)
    return normalized


def load_token():
    """加载 Tushare Token"""
    token = os.getenv('TUSHARE_TOKEN')
    if token:
        return token
    try:
        from config.settings import TUSHARE_TOKEN
        return TUSHARE_TOKEN
    except ImportError:
        print("❌ 找不到 Tushare Token!")
        sys.exit(1)


def redownload_cashflow(stock_list: list[str], db: TushareDB, limit: int = None):
    """重新下载现金流数据"""
    if limit:
        stock_list = stock_list[:limit]

    total = len(stock_list)
    success = 0
    failed = 0
    total_records = 0

    print(f"\n{'='*70}")
    print(f"开始重新下载 {total} 只股票的现金流数据")
    print(f"{'='*70}\n")

    for i, ts_code in enumerate(stock_list, 1):
        try:
            print(f"[{i}/{total}] {ts_code}", end=" ")

            # 只下载现金流数据（不限制日期，全量下载）
            count = db.save_cashflow(ts_code)

            if count > 0:
                success += 1
                total_records += count
                print(f"✅ {count} 条")
            else:
                failed += 1
                print("⚠️ 无数据")

        except KeyboardInterrupt:
            print("\n\n用户中断")
            break
        except Exception as e:
            failed += 1
            print(f"❌ {e}")

    print(f"\n{'='*70}")
    print(f"下载完成:")
    print(f"  总计: {total}")
    print(f"  成功: {success}")
    print(f"  失败: {failed}")
    print(f"  总记录: {total_records}")
    print(f"{'='*70}")


def main():
    parser = argparse.ArgumentParser(description='重新下载现金流数据')
    parser.add_argument('--limit', type=int, help='限制下载数量')
    parser.add_argument('--since', default='2020-01-01', help='只扫描此日期之后缺失的记录，默认 2020-01-01')
    parser.add_argument('--stocks', help='指定股票代码，逗号分隔，如 601717,000001')
    parser.add_argument('--db', default='data/tushare_data.db', help='数据库路径')
    args = parser.parse_args()

    stock_list = None
    if args.stocks:
        stock_list = normalize_stock_codes([s for s in args.stocks.split(',') if s.strip()])

    print("扫描主板非ST且需要重新下载的股票...")
    stock_list = get_stocks_needing_redownload(args.db, args.since, stock_list)
    print(f"共 {len(stock_list)} 只股票需要重新下载")

    token = load_token()
    db = TushareDB(token=token, db_path=args.db)

    redownload_cashflow(stock_list, db, limit=args.limit)


if __name__ == '__main__':
    main()
