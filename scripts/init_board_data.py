#!/usr/bin/env python3
"""
初始化板块数据到数据库

从 akshare 接口获取所有行业板块及其成分股并保存到数据库

Usage:
    python scripts/init_board_data.py           # 断点续传（跳过已存在）
    python scripts/init_board_data.py --force   # 强制更新所有板块
"""
import sys
import argparse
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_sources.board import BoardDB
from config.settings import BOARD_DB_PATH


def init_board_data(force_update: bool = False):
    """从 akshare 获取板块数据并保存到数据库"""
    print("=" * 60)
    print("初始化板块数据到数据库")
    print("=" * 60)

    db = BoardDB(db_path=str(BOARD_DB_PATH))
    stats = db.save_all_boards(force_update=force_update)

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='初始化板块数据')
    parser.add_argument('--force', action='store_true',
                        help='强制更新所有板块数据（忽略已存在的数据）')
    args = parser.parse_args()

    init_board_data(force_update=args.force)
