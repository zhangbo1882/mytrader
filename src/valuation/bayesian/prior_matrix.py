"""
贝叶斯先验矩阵

管理行业×方法的历史准确率矩阵（SQLite存储）。
用于贝叶斯加权估值组合方法。

支持 L1/L2 分层回退：
- L2 级别优先（精度更高），无数据时回退到 L1 级别
"""

import sqlite3
import logging
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class PriorMatrix:
    """
    管理行业×方法的历史准确率矩阵（SQLite存储）

    表结构：
    - industry_code: 申万行业代码（L1如 801780，L2如 340500）
    - industry_name: 行业名称
    - level: 分类层级 ('L1' 或 'L2')
    - method: 估值方法 (pe/pb/ps/peg/dcf)
    - accuracy: 历史准确率 0.0-1.0
    - sample_count: 样本数量
    - stock_count: 涉及股票数量
    - updated_at: 更新时间
    """

    TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS valuation_prior_matrix (
        industry_code TEXT NOT NULL,
        industry_name TEXT,
        level TEXT DEFAULT 'L1',
        method TEXT NOT NULL,
        accuracy REAL NOT NULL,
        sample_count INTEGER DEFAULT 0,
        stock_count INTEGER DEFAULT 0,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (industry_code, method)
    )
    """

    def __init__(self, db_path: str = "data/tushare_data.db"):
        """
        初始化先验矩阵

        Args:
            db_path: SQLite 数据库路径
        """
        self.db_path = db_path
        self.ensure_table()

    def _get_conn(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _migrate(self) -> None:
        """执行数据库迁移（兼容旧版本表结构）"""
        try:
            with self._get_conn() as conn:
                cursor = conn.execute("PRAGMA table_info(valuation_prior_matrix)")
                cols = [row[1] for row in cursor.fetchall()]
                if 'level' not in cols:
                    conn.execute(
                        "ALTER TABLE valuation_prior_matrix "
                        "ADD COLUMN level TEXT DEFAULT 'L1'"
                    )
                    conn.commit()
                    logger.info("valuation_prior_matrix: 已添加 level 列")
        except Exception as e:
            logger.warning(f"迁移 valuation_prior_matrix 失败（可忽略）: {e}")

    def ensure_table(self) -> None:
        """创建表（如不存在），并执行迁移"""
        try:
            with self._get_conn() as conn:
                conn.execute(self.TABLE_DDL)
                conn.commit()
            # 迁移旧表（添加 level 列）
            self._migrate()
        except Exception as e:
            logger.error(f"创建 valuation_prior_matrix 表失败: {e}")
            raise

    def get_weights(
        self,
        industry_code: Optional[str],
        industry_name: Optional[str] = None
    ) -> Optional[Dict[str, float]]:
        """
        获取行业的历史准确率权重

        Args:
            industry_code: 申万行业代码
            industry_name: 行业名称（代码未匹配时使用）

        Returns:
            {method: accuracy} 字典，无数据时返回 None
        """
        try:
            with self._get_conn() as conn:
                rows = None

                # 先按 industry_code 查找
                if industry_code:
                    cursor = conn.execute(
                        "SELECT method, accuracy FROM valuation_prior_matrix "
                        "WHERE industry_code = ?",
                        (industry_code,)
                    )
                    rows = cursor.fetchall()

                # 如果没找到，按 industry_name 查找
                if not rows and industry_name:
                    cursor = conn.execute(
                        "SELECT method, accuracy FROM valuation_prior_matrix "
                        "WHERE industry_name = ?",
                        (industry_name,)
                    )
                    rows = cursor.fetchall()

                if not rows:
                    return None

                return {row['method']: row['accuracy'] for row in rows}

        except Exception as e:
            logger.error(f"获取先验权重失败 ({industry_code}): {e}")
            return None

    def get_weights_hierarchical(
        self,
        l2_code: Optional[str],
        l2_name: Optional[str],
        l1_code: Optional[str],
        l1_name: Optional[str]
    ) -> Optional[Dict[str, float]]:
        """
        分层获取历史准确率权重：L2 优先，无数据则回退到 L1

        Args:
            l2_code: 申万L2行业代码（如 '340500'）
            l2_name: 申万L2行业名称（如 '白酒Ⅱ'）
            l1_code: 申万L1行业代码（如 '480000'）
            l1_name: 申万L1行业名称（如 '食品饮料'）

        Returns:
            {method: accuracy} 字典；L2 有数据时返回L2数据，
            否则返回L1数据，均无时返回 None
        """
        # 优先尝试 L2
        if l2_code or l2_name:
            weights = self.get_weights(l2_code, l2_name)
            if weights:
                logger.debug(f"贝叶斯先验：使用L2数据 {l2_name}({l2_code})")
                return weights

        # 回退到 L1
        if l1_code or l1_name:
            weights = self.get_weights(l1_code, l1_name)
            if weights:
                logger.debug(f"贝叶斯先验：L2无数据，回退到L1 {l1_name}({l1_code})")
                return weights

        return None

    def upsert(
        self,
        industry_code: str,
        industry_name: str,
        method: str,
        accuracy: float,
        sample_count: int = 0,
        stock_count: int = 0,
        level: str = 'L1'
    ) -> None:
        """
        插入或更新行业×方法的准确率

        Args:
            industry_code: 申万行业代码
            industry_name: 行业名称
            method: 估值方法
            accuracy: 历史准确率
            sample_count: 样本数量
            stock_count: 涉及股票数量
            level: 行业层级 ('L1' 或 'L2')
        """
        try:
            updated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with self._get_conn() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO valuation_prior_matrix
                    (industry_code, industry_name, level, method, accuracy,
                     sample_count, stock_count, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (industry_code, industry_name, level, method, accuracy,
                     sample_count, stock_count, updated_at)
                )
                conn.commit()
        except Exception as e:
            logger.error(f"更新先验矩阵失败 ({industry_code}/{method}): {e}")
            raise

    def get_status(self) -> List[Dict]:
        """
        获取所有行业×方法的数据状态

        Returns:
            状态列表
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.execute(
                    """
                    SELECT industry_code, industry_name, level, method, accuracy,
                           sample_count, stock_count, updated_at
                    FROM valuation_prior_matrix
                    ORDER BY level, industry_code, method
                    """
                )
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"获取先验矩阵状态失败: {e}")
            return []

    def clear_industry(self, industry_code: str) -> None:
        """
        清除指定行业的所有数据

        Args:
            industry_code: 申万行业代码
        """
        try:
            with self._get_conn() as conn:
                conn.execute(
                    "DELETE FROM valuation_prior_matrix WHERE industry_code = ?",
                    (industry_code,)
                )
                conn.commit()
        except Exception as e:
            logger.error(f"清除行业数据失败 ({industry_code}): {e}")
            raise
