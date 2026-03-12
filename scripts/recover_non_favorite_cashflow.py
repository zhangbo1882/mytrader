#!/usr/bin/env python3
"""
恢复非收藏股票的 cashflow.end_bal_cash。

流程：
1. 仅用 Tushare pro.cashflow 重建现金流表数据
2. 再用 AKShare END_CCE 补充剩余 end_bal_cash 缺口

范围：
- 主板
- 非 ST
- 非收藏股票
- 默认只处理 2020-01-01 之后仍有缺失的股票

特性：
- 断点续传（checkpoint）
- 每 30 秒输出一次进度

示例：
    python scripts/recover_non_favorite_cashflow.py
    python scripts/recover_non_favorite_cashflow.py --limit 50
    python scripts/recover_non_favorite_cashflow.py --checkpoint reports/non_favorite_cashflow_recovery.json
    python scripts/recover_non_favorite_cashflow.py --reset
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import TUSHARE_DB_PATH, TUSHARE_TOKEN
from src.data_sources.tushare import TushareDB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def load_token() -> str:
    token = os.getenv("TUSHARE_TOKEN")
    if token:
        return token
    return TUSHARE_TOKEN


def normalize_favorite_ts_codes(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT
            CASE
                WHEN stock_code GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]' THEN
                    CASE WHEN stock_code LIKE '6%' THEN stock_code || '.SH' ELSE stock_code || '.SZ' END
                ELSE NULL
            END AS ts_code
        FROM favorites
        """
    ).fetchall()
    return {row[0] for row in rows if row[0]}


def build_non_favorite_targets(db_path: str, since: str, limit: int | None = None) -> list[str]:
    conn = sqlite3.connect(db_path)
    try:
        favorite_ts_codes = normalize_favorite_ts_codes(conn)
        sql = """
            SELECT DISTINCT c.ts_code
            FROM cashflow c
            JOIN stock_basic_info s ON s.ts_code = c.ts_code
            WHERE c.end_bal_cash IS NULL
              AND c.end_date >= ?
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
            ORDER BY c.ts_code
        """
        rows = [row[0] for row in conn.execute(sql, (since.replace('-', ''),)).fetchall()]
        targets = [code for code in rows if code not in favorite_ts_codes]
        if limit:
            targets = targets[:limit]
        return targets
    finally:
        conn.close()


def remaining_missing_from_targets(db_path: str, since: str, targets: list[str]) -> list[str]:
    if not targets:
        return []
    conn = sqlite3.connect(db_path)
    try:
        placeholders = ",".join("?" * len(targets))
        sql = f"""
            SELECT DISTINCT ts_code
            FROM cashflow
            WHERE end_bal_cash IS NULL
              AND report_type = 1
              AND end_date >= ?
              AND ts_code IN ({placeholders})
            ORDER BY ts_code
        """
        rows = conn.execute(sql, [since.replace('-', '')] + targets).fetchall()
        return [row[0] for row in rows]
    finally:
        conn.close()


def save_cashflow_tushare_only(db: TushareDB, ts_code: str) -> int:
    ts_code_std = db._standardize_code(ts_code)
    table_name = "cashflow"

    df = db._retry_api_call(
        db.pro.cashflow,
        ts_code=ts_code_std,
        start_date=None,
        end_date=None,
    )
    if df is None or df.empty:
        return 0

    df = db._smart_dedup_financial_data(df, table_name)
    db._create_unified_financial_table(table_name)

    with db.engine.connect() as conn:
        conn.execute(text("DELETE FROM cashflow WHERE ts_code = :ts_code"), {"ts_code": ts_code_std})
        conn.commit()

    df.to_sql(table_name, db.engine, if_exists="append", index=False, method="multi")
    return len(df)


def backfill_one_with_fallback(db: TushareDB, db_path: str, ts_code: str, since: str) -> dict:
    ak_df = db._load_akshare_cashflow_end_bal_cash(ts_code)
    if ak_df.empty:
        return {
            "ts_code": ts_code,
            "filled": 0,
            "filled_exact": 0,
            "filled_end_date": 0,
            "unmatched": 0,
            "ak_empty": True,
        }

    dup_mask = ak_df.duplicated(subset=["ann_date", "end_date"], keep=False)
    ak_clean = ak_df.loc[~dup_mask].dropna(subset=["ann_date", "end_date", "end_bal_cash"]).copy()
    ak_exact = {
        (row["ann_date"], row["end_date"]): float(row["end_bal_cash"])
        for _, row in ak_clean.iterrows()
    }
    ak_by_end_date: dict[str, list[float]] = {}
    for _, row in ak_clean.iterrows():
        ak_by_end_date.setdefault(row["end_date"], []).append(float(row["end_bal_cash"]))

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT rowid, ann_date, end_date
            FROM cashflow
            WHERE ts_code = ?
              AND report_type = 1
              AND end_bal_cash IS NULL
              AND end_date >= ?
            ORDER BY end_date, ann_date
            """,
            (ts_code, since.replace('-', '')),
        ).fetchall()

        filled = 0
        filled_exact = 0
        filled_end_date = 0
        unmatched = 0

        for rowid, ann_date, end_date in rows:
            norm_ann = db._normalize_financial_date(ann_date)
            norm_end = db._normalize_financial_date(end_date)
            value = ak_exact.get((norm_ann, norm_end))
            if value is not None:
                conn.execute("UPDATE cashflow SET end_bal_cash = ? WHERE rowid = ?", (value, rowid))
                filled += 1
                filled_exact += 1
                continue

            fallback_values = ak_by_end_date.get(norm_end, [])
            if len(fallback_values) == 1:
                conn.execute(
                    "UPDATE cashflow SET end_bal_cash = ? WHERE rowid = ?",
                    (fallback_values[0], rowid),
                )
                filled += 1
                filled_end_date += 1
                continue

            unmatched += 1

        conn.commit()
        return {
            "ts_code": ts_code,
            "filled": filled,
            "filled_exact": filled_exact,
            "filled_end_date": filled_end_date,
            "unmatched": unmatched,
            "ak_empty": False,
        }
    finally:
        conn.close()


def load_checkpoint(checkpoint_path: Path) -> dict | None:
    if not checkpoint_path.exists():
        return None
    with checkpoint_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_checkpoint(checkpoint_path: Path, state: dict) -> None:
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = checkpoint_path.with_suffix(checkpoint_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    tmp_path.replace(checkpoint_path)


def init_state(db_path: str, since: str, limit: int | None) -> dict:
    download_targets = build_non_favorite_targets(db_path, since, limit)
    return {
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "since": since,
        "limit": limit,
        "phase": "download",
        "download_targets": download_targets,
        "download_index": 0,
        "download_stats": {
            "success": 0,
            "failed": 0,
            "records": 0,
        },
        "backfill_targets": [],
        "backfill_index": 0,
        "backfill_stats": {
            "filled": 0,
            "filled_exact": 0,
            "filled_end_date": 0,
            "unmatched": 0,
            "ak_empty": 0,
        },
    }


def maybe_log_progress(stage: str, done: int, total: int, started_at: float, last_log_at: float, interval: int, extra: str) -> float:
    now = time.time()
    if done == total or now - last_log_at >= interval:
        elapsed = max(now - started_at, 1e-6)
        rate = done / elapsed
        remaining = max(total - done, 0)
        eta = remaining / rate if rate > 0 else 0
        logger.info("[%s] %d/%d | %.2f只/s | ETA %.0fs | %s", stage, done, total, rate, eta, extra)
        return now
    return last_log_at


def run_download_phase(state: dict, db_path: str, token: str, checkpoint_path: Path, progress_interval: int) -> dict:
    db = TushareDB(token=token, db_path=db_path)
    targets = state["download_targets"]
    started_at = time.time()
    last_log_at = started_at

    while state["download_index"] < len(targets):
        ts_code = targets[state["download_index"]]
        try:
            count = save_cashflow_tushare_only(db, ts_code)
            if count > 0:
                state["download_stats"]["success"] += 1
                state["download_stats"]["records"] += count
            else:
                state["download_stats"]["failed"] += 1
        except Exception as e:
            logger.exception("download failed for %s: %s", ts_code, e)
            state["download_stats"]["failed"] += 1
        state["download_index"] += 1
        state["updated_at"] = now_iso()
        save_checkpoint(checkpoint_path, state)
        extra = (
            f"成功 {state['download_stats']['success']} | 失败 {state['download_stats']['failed']} | "
            f"记录 {state['download_stats']['records']}"
        )
        last_log_at = maybe_log_progress(
            "download",
            state["download_index"],
            len(targets),
            started_at,
            last_log_at,
            progress_interval,
            extra,
        )

    state["backfill_targets"] = remaining_missing_from_targets(db_path, state["since"], targets)
    state["phase"] = "backfill"
    state["updated_at"] = now_iso()
    save_checkpoint(checkpoint_path, state)
    logger.info("download 阶段完成，仍有 %d 只股票需要 AKShare 补充", len(state["backfill_targets"]))
    return state


def run_backfill_phase(state: dict, db_path: str, token: str, checkpoint_path: Path, progress_interval: int) -> dict:
    db = TushareDB(token=token, db_path=db_path)
    targets = state["backfill_targets"]
    started_at = time.time()
    last_log_at = started_at

    while state["backfill_index"] < len(targets):
        ts_code = targets[state["backfill_index"]]
        try:
            result = backfill_one_with_fallback(db, db_path, ts_code, state["since"])
            state["backfill_stats"]["filled"] += result["filled"]
            state["backfill_stats"]["filled_exact"] += result["filled_exact"]
            state["backfill_stats"]["filled_end_date"] += result["filled_end_date"]
            state["backfill_stats"]["unmatched"] += result["unmatched"]
            if result.get("ak_empty"):
                state["backfill_stats"]["ak_empty"] += 1
        except Exception as e:
            logger.exception("backfill failed for %s: %s", ts_code, e)
            state["backfill_stats"]["unmatched"] += 1
        state["backfill_index"] += 1
        state["updated_at"] = now_iso()
        save_checkpoint(checkpoint_path, state)
        extra = (
            f"补齐 {state['backfill_stats']['filled']} | 精确 {state['backfill_stats']['filled_exact']} | "
            f"按end_date {state['backfill_stats']['filled_end_date']} | 未命中 {state['backfill_stats']['unmatched']} | "
            f"AK无数据 {state['backfill_stats']['ak_empty']}"
        )
        last_log_at = maybe_log_progress(
            "backfill",
            state["backfill_index"],
            len(targets),
            started_at,
            last_log_at,
            progress_interval,
            extra,
        )

    state["phase"] = "done"
    state["updated_at"] = now_iso()
    save_checkpoint(checkpoint_path, state)
    logger.info("backfill 阶段完成")
    return state


def main() -> None:
    parser = argparse.ArgumentParser(description="恢复非收藏股票的 cashflow.end_bal_cash")
    parser.add_argument("--since", default="2020-01-01", help="只处理此日期之后的缺失，默认 2020-01-01")
    parser.add_argument("--db", default=str(TUSHARE_DB_PATH), help="数据库路径")
    parser.add_argument(
        "--checkpoint",
        default="reports/non_favorite_cashflow_recovery_checkpoint.json",
        help="checkpoint 文件路径",
    )
    parser.add_argument("--progress-interval", type=int, default=30, help="进度输出间隔秒数，默认 30")
    parser.add_argument("--limit", type=int, default=None, help="仅处理前 N 只股票，便于测试")
    parser.add_argument("--reset", action="store_true", help="忽略已有 checkpoint，从头开始")
    args = parser.parse_args()

    checkpoint_path = Path(args.checkpoint)
    token = load_token()

    if args.reset and checkpoint_path.exists():
        checkpoint_path.unlink()
        logger.info("已删除旧 checkpoint: %s", checkpoint_path)

    state = load_checkpoint(checkpoint_path)
    if state is None:
        state = init_state(args.db, args.since, args.limit)
        save_checkpoint(checkpoint_path, state)
        logger.info(
            "初始化完成：download 目标 %d 只股票",
            len(state["download_targets"]),
        )
    else:
        logger.info(
            "从 checkpoint 恢复：phase=%s, download=%d/%d, backfill=%d/%d",
            state["phase"],
            state["download_index"],
            len(state.get("download_targets", [])),
            state.get("backfill_index", 0),
            len(state.get("backfill_targets", [])),
        )

    if state["phase"] == "done":
        logger.info("任务已完成，无需继续")
        return

    if state["phase"] == "download":
        state = run_download_phase(state, args.db, token, checkpoint_path, args.progress_interval)

    if state["phase"] == "backfill":
        state = run_backfill_phase(state, args.db, token, checkpoint_path, args.progress_interval)

    logger.info(
        "完成：download 成功 %d / 失败 %d / 记录 %d；backfill 补齐 %d（精确 %d，按end_date %d），未命中 %d，AK无数据 %d",
        state["download_stats"]["success"],
        state["download_stats"]["failed"],
        state["download_stats"]["records"],
        state["backfill_stats"]["filled"],
        state["backfill_stats"]["filled_exact"],
        state["backfill_stats"]["filled_end_date"],
        state["backfill_stats"]["unmatched"],
        state["backfill_stats"]["ak_empty"],
    )


if __name__ == "__main__":
    main()
