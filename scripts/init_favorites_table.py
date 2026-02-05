#!/usr/bin/env python3
"""初始化 favorites 表"""
import sqlite3
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import TUSHARE_DB_PATH


def init_favorites_table():
    """初始化 favorites 表"""
    db_path = TUSHARE_DB_PATH

    print(f"Database: {db_path}")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create favorites table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS favorites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL DEFAULT 'default',
            stock_code TEXT NOT NULL,
            stock_name TEXT NOT NULL,
            added_at TEXT NOT NULL,
            notes TEXT,
            UNIQUE(user_id, stock_code)
        )
    """)

    # Create indexes
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_favorites_user_id ON favorites(user_id)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_favorites_added_at ON favorites(added_at)
    """)

    conn.commit()
    conn.close()

    print("✓ favorites table created successfully")


if __name__ == '__main__':
    init_favorites_table()
