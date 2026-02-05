#!/usr/bin/env python3
"""
板块数据库管理类
"""
import os
import sys

# 禁用代理（必须在导入任何网络库之前）
os.environ['NO_PROXY'] = '*'
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)

# Monkey patch 禁用所有 requests session 的代理
import requests
original_init = requests.Session.__init__
def new_init(self, *args, **kwargs):
    original_init(self, *args, **kwargs)
    self.trust_env = False
    self.proxies = {'http': None, 'https': None, 'no_proxy': '*'}
requests.Session.__init__ = new_init

import akshare as ak
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime


# 配置
REQUEST_DELAY = 2  # 每次请求间隔秒数
MAX_RETRIES = 3  # 最大重试次数


class BoardDB:
    """板块数据库管理类"""

    def __init__(self, db_path: str = "data/board_data.db"):
        """初始化数据库连接"""
        self.engine = create_engine(f"sqlite:///{db_path}", echo=False)
        self._create_tables()

    def _create_tables(self):
        """创建板块相关表"""
        # 板块名称表
        board_names_sql = """
        CREATE TABLE IF NOT EXISTS board_names (
            board_code TEXT PRIMARY KEY,
            board_name TEXT NOT NULL,
            board_type TEXT DEFAULT 'industry',
            source TEXT DEFAULT 'eastmoney',
            description TEXT,
            updated_at TEXT,
            created_at TEXT
        );
        """

        # 板块成分股表
        board_cons_sql = """
        CREATE TABLE IF NOT EXISTS board_cons (
            board_code TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            added_date TEXT,
            weight REAL,
            PRIMARY KEY (board_code, stock_code),
            FOREIGN KEY (board_code) REFERENCES board_names(board_code) ON DELETE CASCADE
        );
        """

        # 创建索引
        board_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_board_names_name ON board_names(board_name);",
            "CREATE INDEX IF NOT EXISTS idx_board_names_type ON board_names(board_type);",
            "CREATE INDEX IF NOT EXISTS idx_board_cons_stock ON board_cons(stock_code);",
            "CREATE INDEX IF NOT EXISTS idx_board_cons_board ON board_cons(board_code);",
        ]

        with self.engine.connect() as conn:
            for sql in [board_names_sql, board_cons_sql] + board_indexes:
                conn.execute(text(sql))
                conn.commit()

    def get_board_names(self) -> pd.DataFrame:
        """获取所有板块名称（带重试）"""
        import time
        for attempt in range(MAX_RETRIES):
            try:
                df = ak.stock_board_industry_name_em()
                return df
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    wait_time = (attempt + 1) * 2
                    print(f"⚠️  获取板块名称失败（第{attempt+1}次），{wait_time}秒后重试...")
                    time.sleep(wait_time)
                else:
                    print(f"❌ 获取板块名称失败: {e}")
                    return pd.DataFrame()

    def save_board_names(self, df: pd.DataFrame = None) -> int:
        """保存板块名称到数据库"""
        if df is None:
            df = self.get_board_names()

        if df.empty:
            print("⚠️  无板块数据")
            return 0

        # 标准化数据
        df = df.rename(columns={'板块名称': 'board_name', '板块代码': 'board_code'})
        df['board_type'] = 'industry'
        df['source'] = 'eastmoney'
        df['description'] = None
        df['updated_at'] = datetime.now().isoformat()
        df['created_at'] = datetime.now().isoformat()

        df = df[['board_code', 'board_name', 'board_type', 'source',
                 'description', 'updated_at', 'created_at']]

        try:
            df.to_sql('board_names', self.engine, if_exists='replace',
                      index=False, method='multi')
            print(f"✅ 成功保存 {len(df)} 个板块")
            return len(df)
        except Exception as e:
            print(f"❌ 保存板块名称失败: {e}")
            return 0

    def get_board_constituents(self, board_name: str) -> pd.DataFrame:
        """获取指定板块的成分股（带重试）"""
        import time
        for attempt in range(MAX_RETRIES):
            try:
                df = ak.stock_board_industry_cons_em(symbol=board_name)
                return df
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    wait_time = (attempt + 1) * 2
                    print(f"⚠️  获取 {board_name} 失败（第{attempt+1}次），{wait_time}秒后重试...")
                    time.sleep(wait_time)
                else:
                    print(f"❌ 获取板块 {board_name} 成分股失败: {e}")
                    return pd.DataFrame()

    def has_board_constituents(self, board_code: str) -> bool:
        """检查指定板块的成分股是否已存在于数据库"""
        query = """
        SELECT COUNT(*) as cnt FROM board_cons WHERE board_code = :board_code
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"board_code": board_code}).fetchone()
            return result[0] > 0

    def save_board_constituents(self, board_name: str, board_code: str,
                               df: pd.DataFrame = None) -> int:
        """保存板块成分股到数据库"""
        if df is None:
            df = self.get_board_constituents(board_name)

        if df.empty:
            print(f"⚠️  板块 {board_name} 无成分股数据")
            return 0

        # 标准化数据
        df = df.rename(columns={'代码': 'stock_code', '名称': 'stock_name'})
        df['board_code'] = board_code
        df['added_date'] = datetime.now().isoformat()
        df['weight'] = None

        df = df[['board_code', 'stock_code', 'stock_name', 'added_date', 'weight']]

        # 删除该板块的旧数据
        delete_sql = "DELETE FROM board_cons WHERE board_code = :board_code"
        with self.engine.connect() as conn:
            conn.execute(text(delete_sql), {"board_code": board_code})
            conn.commit()

        try:
            df.to_sql('board_cons', self.engine, if_exists='append',
                      index=False, method='multi')
            print(f"✅ 成功保存板块 {board_name} 的 {len(df)} 只成分股")
            return len(df)
        except Exception as e:
            print(f"❌ 保存板块 {board_name} 成分股失败: {e}")
            return 0

    def save_all_boards(self, force_update: bool = False) -> dict:
        """获取并保存所有板块及其成分股"""
        mode = "强制更新" if force_update else "断点续传"
        print("=" * 60)
        print(f"开始初始化板块数据 ({mode})")
        print("=" * 60)

        stats = {
            'total_boards': 0,
            'successful_boards': 0,
            'skipped_boards': 0,
            'total_stocks': 0,
            'failed_boards': []
        }

        # 1. 获取并保存板块名称
        print("\n1. 获取板块名称...")
        board_names_df = self.get_board_names()
        if board_names_df.empty:
            print("❌ 获取板块名称失败")
            return stats

        stats['total_boards'] = len(board_names_df)
        board_count = self.save_board_names(board_names_df)
        if board_count == 0:
            return stats

        # 2. 获取每个板块的成分股
        print(f"\n2. 获取板块成分股（共 {board_count} 个板块）...")

        for idx, row in board_names_df.iterrows():
            board_name = row['板块名称']
            board_code = row['板块代码']

            print(f"[{idx+1}/{board_count}] {board_name}...", end=' ')

            # 检查是否已存在（断点续传）
            if not force_update and self.has_board_constituents(board_code):
                print(f"⊙ 跳过（已存在）")
                stats['skipped_boards'] += 1
                continue

            # 获取并保存成分股
            stock_count = self.save_board_constituents(
                board_name=board_name,
                board_code=board_code
            )

            if stock_count > 0:
                stats['successful_boards'] += 1
                stats['total_stocks'] += stock_count
            else:
                stats['failed_boards'].append(board_name)

            # 避免请求过快
            import time
            time.sleep(REQUEST_DELAY)

        # 3. 打印统计信息
        print("\n" + "=" * 60)
        print(f"总板块数: {stats['total_boards']}")
        print(f"成功处理: {stats['successful_boards']}")
        if stats['skipped_boards'] > 0:
            print(f"跳过（已存在）: {stats['skipped_boards']}")
        print(f"总成分股数: {stats['total_stocks']}")
        if stats['failed_boards']:
            print(f"失败板块: {', '.join(stats['failed_boards'])}")
        print("=" * 60)

        return stats

    def get_all_boards(self) -> pd.DataFrame:
        """从数据库获取所有板块"""
        query = """
        SELECT board_code, board_name, board_type, source, updated_at
        FROM board_names
        ORDER BY board_name
        """
        return pd.read_sql_query(query, self.engine)

    def get_board_constituents_from_db(self, board_code: str) -> pd.DataFrame:
        """从数据库获取指定板块的成分股"""
        query = """
        SELECT bc.stock_code, bc.stock_name, bn.board_name, bn.board_code
        FROM board_cons bc
        JOIN board_names bn ON bc.board_code = bn.board_code
        WHERE bc.board_code = :board_code
        ORDER BY bc.stock_code
        """
        return pd.read_sql_query(query, self.engine, params={"board_code": board_code})

    def get_stock_boards(self, stock_code: str) -> pd.DataFrame:
        """从数据库获取指定股票所属的所有板块"""
        query = """
        SELECT bn.board_code, bn.board_name, bc.stock_code, bc.stock_name
        FROM board_cons bc
        JOIN board_names bn ON bc.board_code = bn.board_code
        WHERE bc.stock_code = :stock_code
        ORDER BY bn.board_name
        """
        return pd.read_sql_query(query, self.engine, params={"stock_code": stock_code})
