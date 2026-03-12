#!/usr/bin/env python3
"""
快速回填 cashflow.end_bal_cash 脚本

针对库里已有行但 end_bal_cash 仍为 NULL 的记录，
直接从 AKShare stock_cash_flow_sheet_by_report_em 补值，
不走 Tushare 重新拉全量财报。

使用方法:
    # 回填所有缺失股票
    python scripts/backfill_end_bal_cash.py

    # 只回填指定股票
    python scripts/backfill_end_bal_cash.py --stocks 601717,000001

    # 只回填 2024 年以来的缺失
    python scripts/backfill_end_bal_cash.py --since 2024-01-01

    # 并发数（默认 4）
    python scripts/backfill_end_bal_cash.py --workers 8

    # 跳过前 N 只（断点续跑）
    python scripts/backfill_end_bal_cash.py --offset 500
"""

import argparse
import sys
import sqlite3
import time
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH
from src.data_sources.tushare import TushareDB

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)


def get_stocks_with_missing(
    db_path: str,
    since: str | None,
    stocks: list[str] | None,
    mainboard_nonst: bool = False,
) -> list[str]:
    """返回库里有 end_bal_cash=NULL 记录的股票列表（A股）。"""
    conn = sqlite3.connect(db_path)
    try:
        base_sql = """
            SELECT DISTINCT c.ts_code
            FROM cashflow c
        """
        cond = [
            "c.report_type = 1",
            "c.end_bal_cash IS NULL",
            "(c.ts_code LIKE '%.SH' OR c.ts_code LIKE '%.SZ')",
        ]
        params: list[str] = []

        if mainboard_nonst:
            base_sql += "\n            JOIN stock_basic_info s ON s.ts_code = c.ts_code\n        "
            cond.append("COALESCE(s.name, '') NOT LIKE '%ST%'")
            cond.append("""
                (
                    c.ts_code GLOB '000*.SZ' OR
                    c.ts_code GLOB '001*.SZ' OR
                    c.ts_code GLOB '002*.SZ' OR
                    c.ts_code GLOB '600*.SH' OR
                    c.ts_code GLOB '601*.SH' OR
                    c.ts_code GLOB '603*.SH' OR
                    c.ts_code GLOB '605*.SH'
                )
            """)

        if since:
            cond.append("c.end_date >= ?")
            params.append(since.replace('-', ''))
        if stocks:
            placeholders = ','.join('?' * len(stocks))
            cond.append(f"c.ts_code IN ({placeholders})")
            params.extend(stocks)

        sql = base_sql + "\n WHERE " + " AND ".join(cond) + "\n ORDER BY c.ts_code"
        rows = conn.execute(sql, params).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def backfill_one(ts_code: str, db_path: str, token: str, since: str | None) -> dict:
    """对单只股票执行回填，直接 UPDATE 到 SQLite，返回统计。"""
    db = TushareDB(token=token, db_path=db_path)

    # 从 AKShare 拉数据
    ak_df = db._load_akshare_cashflow_end_bal_cash(ts_code)
    if ak_df.empty:
        return {'ts_code': ts_code, 'filled': 0, 'unmatched': 0, 'ak_empty': True}

    # 构建 (ann_date, end_date) -> end_bal_cash 查找表，去除重复 key
    dup_mask = ak_df.duplicated(subset=['ann_date', 'end_date'], keep=False)
    ak_clean = ak_df.loc[~dup_mask].dropna(subset=['ann_date', 'end_date', 'end_bal_cash'])
    ak_lookup = {
        (row['ann_date'], row['end_date']): float(row['end_bal_cash'])
        for _, row in ak_clean.iterrows()
    }

    # 读库里缺失行
    since_cond = ''
    since_params: list = []
    if since:
        since_cond = f" AND end_date >= ?"
        since_params = [since.replace('-', '')]

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            f"SELECT rowid, ann_date, end_date FROM cashflow "
            f"WHERE ts_code = ? AND report_type = 1 AND end_bal_cash IS NULL{since_cond}",
            [ts_code] + since_params,
        ).fetchall()

        filled = 0
        unmatched = 0
        for rowid, ann_date, end_date in rows:
            norm_ann = db._normalize_financial_date(ann_date)
            norm_end = db._normalize_financial_date(end_date)
            if not norm_ann or not norm_end:
                unmatched += 1
                continue
            val = ak_lookup.get((norm_ann, norm_end))
            if val is None:
                unmatched += 1
                continue
            conn.execute(
                "UPDATE cashflow SET end_bal_cash = ? WHERE rowid = ?",
                (val, rowid),
            )
            filled += 1

        conn.commit()
        return {'ts_code': ts_code, 'filled': filled, 'unmatched': unmatched, 'ak_empty': False}
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='快速回填 cashflow.end_bal_cash')
    parser.add_argument('--stocks', help='指定股票代码，逗号分隔，如 601717,000001')
    parser.add_argument('--since', default=None, help='只回填此日期之后的报告期，格式 YYYY-MM-DD，默认全量')
    parser.add_argument('--workers', type=int, default=4, help='并发线程数（默认 4）')
    parser.add_argument('--offset', type=int, default=0, help='跳过前 N 只股票（断点续跑）')
    parser.add_argument('--mainboard-nonst', action='store_true', help='只处理主板非ST股票')
    parser.add_argument('--db', default=str(TUSHARE_DB_PATH), help='数据库路径')
    parser.add_argument('--delay', type=float, default=0.3, help='每只股票请求间隔秒数（默认 0.3）')
    args = parser.parse_args()

    stock_list = None
    if args.stocks:
        raw = [s.strip() for s in args.stocks.split(',') if s.strip()]
        # 标准化：6位数字 -> 加后缀
        normalized = []
        for s in raw:
            if '.' not in s:
                code = s.zfill(6)
                if code.startswith('6'):
                    normalized.append(f'{code}.SH')
                else:
                    normalized.append(f'{code}.SZ')
            else:
                normalized.append(s.upper())
        stock_list = normalized

    logger.info('扫描缺失股票...')
    ts_codes = get_stocks_with_missing(args.db, args.since, stock_list, args.mainboard_nonst)
    total = len(ts_codes)
    logger.info('共 %d 只股票存在 end_bal_cash 缺失', total)

    if args.offset:
        ts_codes = ts_codes[args.offset:]
        logger.info('跳过前 %d 只，剩余 %d 只', args.offset, len(ts_codes))

    if not ts_codes:
        logger.info('无需回填，退出')
        return

    # 统计
    total_filled = 0
    total_unmatched = 0
    total_ak_empty = 0
    done = 0
    start_time = time.time()

    def process(ts_code: str) -> dict:
        result = backfill_one(ts_code, args.db, TUSHARE_TOKEN, args.since)
        if args.delay > 0:
            time.sleep(args.delay)
        return result

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process, code): code for code in ts_codes}
        for future in as_completed(futures):
            result = future.result()
            done += 1
            total_filled += result['filled']
            total_unmatched += result['unmatched']
            if result.get('ak_empty'):
                total_ak_empty += 1

            if done % 50 == 0 or done == len(ts_codes):
                elapsed = time.time() - start_time
                rate = done / elapsed if elapsed > 0 else 0
                eta = (len(ts_codes) - done) / rate if rate > 0 else 0
                logger.info(
                    '[%d/%d] 已补 %d 条 | 未命中 %d | AK无数据 %d | %.1f只/s | ETA %.0fs',
                    done + args.offset, total + args.offset,
                    total_filled, total_unmatched, total_ak_empty,
                    rate, eta,
                )
            elif result['filled'] > 0:
                logger.info('%s 补齐 %d 条', result['ts_code'], result['filled'])

    elapsed = time.time() - start_time
    logger.info(
        '完成！共处理 %d 只股票，补齐 %d 条，未命中 %d 条，AK无数据 %d 只，耗时 %.1f秒',
        done, total_filled, total_unmatched, total_ak_empty, elapsed,
    )


if __name__ == '__main__':
    main()
