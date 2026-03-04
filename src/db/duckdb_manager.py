# src/db/duckdb_manager.py
"""
DuckDB 数据库管理器
支持多时间周期股票数据的存储和查询
"""
import duckdb
from pathlib import Path
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
import logging

from config.settings import DUCKDB_PATH, SUPPORTED_INTERVALS, INTERVAL_TABLE_MAP

logger = logging.getLogger(__name__)


class DuckDBManager:
    """DuckDB 数据库管理器"""

    def __init__(self, db_path: Optional[str] = None, read_only: bool = False):
        """
        初始化 DuckDB 管理器

        Args:
            db_path: 数据库文件路径，默认使用配置文件中的路径
            read_only: 是否以只读模式打开（允许多进程并发访问）
        """
        self.db_path = db_path or str(DUCKDB_PATH)
        self.read_only = read_only
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

        # 确保数据目录存在
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> duckdb.DuckDBPyConnection:
        """
        获取数据库连接

        注意：由于 DuckDB 的限制，不应该长期持有连接。
        每次使用后应该调用 close() 关闭连接。

        Returns:
            DuckDB 连接对象
        """
        # 不再缓存连接，每次都创建新连接以避免锁冲突
        if self.read_only:
            # 只读模式
            return duckdb.connect(self.db_path, read_only=True)
        else:
            # 读写模式
            return duckdb.connect(self.db_path)

    def close(self):
        """关闭数据库连接（如果已打开）"""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @contextmanager
    def get_connection(self, max_retries: int = 3, retry_delay: float = 2.0):
        """
        上下文管理器，支持锁冲突时自动重试

        Args:
            max_retries: 最大重试次数（默认 3）
            retry_delay: 重试间隔秒数（默认 2.0）

        Example:
            with db_manager.get_connection() as conn:
                conn.execute("SELECT * FROM bars_1d")
        """
        import time
        last_error = None
        total_wait_time = 0.0
        start_time = time.time()

        for attempt in range(max_retries):
            conn = None
            try:
                connect_start = time.time()
                conn = self.connect()
                connect_time = time.time() - connect_start
                if connect_time > 0.5:
                    logger.warning(f"[DuckDB] 连接耗时: {connect_time:.2f}s")
                yield conn
                total_elapsed = time.time() - start_time
                if total_elapsed > 1.0 or total_wait_time > 0:
                    logger.info(f"[DuckDB] get_connection总耗时: {total_elapsed:.2f}s (等待锁: {total_wait_time:.2f}s)")
                return
            except Exception as e:
                last_error = e
                # 检查是否是锁冲突错误
                error_str = str(e).lower()
                if 'lock' in error_str and attempt < max_retries - 1:
                    logger.warning(f"DuckDB locked, retrying in {retry_delay}s... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
                    total_wait_time += retry_delay
                    continue
                raise
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except:
                        pass

    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在

        Args:
            table_name: 表名

        Returns:
            表是否存在
        """
        conn = self.connect()
        try:
            # 使用 DuckDB 的方式检查表是否存在
            result = conn.execute(
                f"SELECT table_name FROM information_schema.tables "
                f"WHERE table_name = '{table_name}'"
            ).fetchdf()
            return len(result) > 0
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
        finally:
            conn.close()

    def create_table(self, table_name: str, interval: str) -> bool:
        """
        根据时间周期创建对应的表

        Args:
            table_name: 表名
            interval: 时间周期 ('1d', '5m', '15m', '30m', '60m')

        Returns:
            是否创建成功
        """
        if interval not in SUPPORTED_INTERVALS:
            raise ValueError(f"Unsupported interval: {interval}. Must be one of {SUPPORTED_INTERVALS}")

        # 如果表已存在，跳过创建
        if self.table_exists(table_name):
            logger.info(f"Table {table_name} already exists")
            return True

        schema = self._get_table_schema(interval)
        conn = self.connect()

        try:
            conn.execute(f"CREATE TABLE {table_name} {schema}")
            logger.info(f"Created table {table_name} with interval {interval}")
            return True
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            return False
        finally:
            conn.close()

    def _get_table_schema(self, interval: str) -> str:
        """
        根据时间周期获取表结构

        设计原则：
        - stock_code: 纯数字股票代码（如 000001, 600000, 00941）
        - exchange: 交易所代码（SH, SZ, HK 等）
        - ts_code: 计算字段，格式为 stock_code.exchange（如 000001.SZ）

        Args:
            interval: 时间周期

        Returns:
            SQL 表结构定义
        """
        if interval == '1d':
            # 日线表包含估值指标
            return """(
                stock_code VARCHAR NOT NULL,
                exchange VARCHAR,
                datetime DATE NOT NULL,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                open_qfq FLOAT,
                high_qfq FLOAT,
                low_qfq FLOAT,
                close_qfq FLOAT,
                pre_close FLOAT,
                change FLOAT,
                pct_chg FLOAT,
                volume DOUBLE,
                turnover FLOAT,
                amount DOUBLE,
                pe FLOAT,
                pe_ttm FLOAT,
                pb FLOAT,
                ps FLOAT,
                ps_ttm FLOAT,
                total_mv FLOAT,
                circ_mv FLOAT,
                total_share FLOAT,
                float_share FLOAT,
                free_share FLOAT,
                volume_ratio FLOAT,
                turnover_rate_f FLOAT,
                dv_ratio FLOAT,
                dv_ttm FLOAT,
                PRIMARY KEY (stock_code, datetime)
            )"""
        else:
            # 分钟线表（包含涨跌幅和前复权价格）
            return """(
                stock_code VARCHAR NOT NULL,
                exchange VARCHAR,
                datetime TIMESTAMP NOT NULL,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                open_qfq FLOAT,
                high_qfq FLOAT,
                low_qfq FLOAT,
                close_qfq FLOAT,
                pre_close FLOAT,
                change FLOAT,
                pct_chg FLOAT,
                volume DOUBLE,
                amount DOUBLE,
                PRIMARY KEY (stock_code, datetime)
            )"""

    def initialize_all_tables(self) -> Dict[str, bool]:
        """
        初始化所有时间周期的表

        Returns:
            各表创建结果的字典
        """
        results = {}
        for interval in SUPPORTED_INTERVALS:
            table_name = INTERVAL_TABLE_MAP[interval]
            results[table_name] = self.create_table(table_name, interval)
        return results

    def insert_dataframe(self, df, table_name: str,
                        on_conflict: str = 'DO NOTHING') -> int:
        """
        插入 DataFrame 到数据库

        Args:
            df: pandas DataFrame
            table_name: 目标表名
            on_conflict: 冲突处理策略 ('DO NOTHING', 'UPDATE', etc.)

        Returns:
            插入的行数
        """
        with self.get_connection() as conn:
            try:
                # 获取目标表的列
                table_columns = conn.execute(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_name = '{table_name}'"
                ).fetchdf()['column_name'].tolist()

                # 获取数据源表中存在的列（只插入两边的交集）
                available_columns = [col for col in table_columns if col in df.columns]

                if not available_columns:
                    raise ValueError(f"No matching columns between DataFrame and table {table_name}")

                columns_str = ', '.join(available_columns)

                # 注册临时表
                conn.register('temp_df', df)

                # 只插入两边的交集列
                insert_sql = (
                    f"INSERT INTO {table_name} ({columns_str}) "
                    f"SELECT {columns_str} FROM temp_df "
                    f"ON CONFLICT {on_conflict}"
                )

                conn.execute(insert_sql)
                rows_inserted = len(df)

                # 清理临时表
                conn.unregister('temp_df')

                logger.info(f"Inserted {rows_inserted} rows into {table_name}")
                return rows_inserted
            except Exception as e:
                logger.error(f"Error inserting dataframe into {table_name}: {e}")
                # 清理临时表（如果存在）
                try:
                    conn.unregister('temp_df')
                except:
                    pass
                raise

    def query(self, sql: str, params: Optional[List] = None) -> duckdb.DuckDBPyRelation:
        """
        执行查询

        Args:
            sql: SQL 查询语句
            params: 查询参数

        Returns:
            查询结果
        """
        with self.get_connection() as conn:
            if params:
                return conn.execute(sql, params)
            return conn.execute(sql)

    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """
        获取表的列信息

        Args:
            table_name: 表名

        Returns:
            列信息列表
        """
        with self.get_connection() as conn:
            try:
                result = conn.execute(f"DESCRIBE {table_name}").fetchdf()
                return result.to_dict('records')
            except Exception as e:
                logger.error(f"Error getting table info for {table_name}: {e}")
                return []

    def get_row_count(self, table_name: str) -> int:
        """
        获取表的行数

        Args:
            table_name: 表名

        Returns:
            行数
        """
        with self.get_connection() as conn:
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                return result[0] if result else 0
            except Exception as e:
                logger.error(f"Error getting row count for {table_name}: {e}")
                return 0

    def get_intervals(self) -> List[str]:
        """
        获取所有已创建的时间周期表

        Returns:
            时间周期列表
        """
        intervals = []
        for interval, table_name in INTERVAL_TABLE_MAP.items():
            if self.table_exists(table_name):
                intervals.append(interval)
        return intervals

    def get_table_name_for_interval(self, interval: str) -> str:
        """
        根据时间周期获取表名

        Args:
            interval: 时间周期

        Returns:
            表名
        """
        if interval not in INTERVAL_TABLE_MAP:
            raise ValueError(f"Unsupported interval: {interval}")
        return INTERVAL_TABLE_MAP[interval]

    # ============================================================================
    # 任务管理相关方法 (Task Management)
    # ============================================================================

    def create_tasks_tables(self):
        """创建任务管理相关表"""
        conn = self.connect()
        # Create tasks table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id VARCHAR PRIMARY KEY,
                task_type VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                progress INTEGER DEFAULT 0,
                message VARCHAR,
                result VARCHAR,
                error VARCHAR,
                current_stock_index INTEGER DEFAULT 0,
                total_stocks INTEGER DEFAULT 0,
                stats VARCHAR,
                params VARCHAR,
                metadata VARCHAR,
                checkpoint_path VARCHAR,
                stop_requested INTEGER DEFAULT 0,
                pause_requested INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)

        # Create indexes for tasks table
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks(task_type)")

        # Create task_checkpoints table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_checkpoints (
                task_id VARCHAR PRIMARY KEY,
                current_index INTEGER NOT NULL,
                stats VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                stage VARCHAR DEFAULT 'stock',
                FOREIGN KEY (task_id) REFERENCES tasks(task_id)
            )
        """)

        # Create apscheduler_jobs table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS apscheduler_jobs (
                id VARCHAR(191) PRIMARY KEY,
                next_run_time FLOAT,
                job_state BLOB NOT NULL
            )
        """)

        conn.execute("CREATE INDEX IF NOT EXISTS ix_apscheduler_jobs_next_run_time ON apscheduler_jobs (next_run_time)")

        # Create job_configs table (for storing scheduled job configurations)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS job_configs (
                job_id VARCHAR PRIMARY KEY,
                config VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        logger.info("Created/verified tasks management tables")

    def migrate_tasks_from_sqlite(self, sqlite_db_path: str, table_name: str = 'tasks'):
        """
        从 SQLite 迁移任务数据到 DuckDB

        Args:
            sqlite_db_path: SQLite 数据库文件路径
            table_name: 要迁移的表名 ('tasks', 'task_checkpoints', 'apscheduler_jobs', 'job_configs')
        """
        import sqlite3
        import pandas as pd

        if not Path(sqlite_db_path).exists():
            logger.warning(f"SQLite database not found: {sqlite_db_path}")
            return 0

        # 连接 SQLite
        sqlite_conn = sqlite3.connect(sqlite_db_path)

        try:
            # 迁移指定表
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", sqlite_conn)
            if len(df) == 0:
                logger.info(f"Table {table_name} is empty, skipping migration")
                return 0

            logger.info(f"Migrating {len(df)} rows from {table_name}")

            # 注册临时表
            temp_name = f"temp_{table_name}"
            conn = self.connect()
            conn.register(temp_name, df)

            try:
                # 插入数据
                conn.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM {temp_name}")
            except Exception as insert_err:
                # If foreign key constraint error, try filtering for task_checkpoints
                if 'foreign key constraint' in str(insert_err).lower() and table_name == 'task_checkpoints':
                    logger.warning(f"Foreign key constraint error, filtering orphaned checkpoints...")
                    conn.execute(f'''
                        INSERT OR REPLACE INTO {table_name}
                        SELECT t.* FROM {temp_name} t
                        WHERE EXISTS (SELECT 1 FROM tasks WHERE tasks.task_id = t.task_id)
                    ''')
                    logger.info(f"Migrated with orphan filtering applied")
                else:
                    raise insert_err

            # 清理临时表
            conn.unregister(temp_name)

            logger.info(f"Successfully migrated {len(df)} rows from {table_name}")
            return len(df)

        except Exception as e:
            logger.error(f"Error migrating {table_name}: {e}")
            # Don't raise - continue with other tables
            return 0
        finally:
            sqlite_conn.close()

    def migrate_all_from_sqlite(self, tasks_db_path: str, schedule_db_path: str):
        """
        迁移所有任务和调度相关表

        Args:
            tasks_db_path: tasks.db 路径
            schedule_db_path: schedule.db 路径
        """
        # 确保表已创建
        self.create_tasks_tables()

        total_migrated = 0

        # 迁移 tasks.db 中的表
        if Path(tasks_db_path).exists():
            total_migrated += self.migrate_tasks_from_sqlite(tasks_db_path, 'tasks')
            total_migrated += self.migrate_tasks_from_sqlite(tasks_db_path, 'task_checkpoints')

        # 迁移 schedule.db 中的表
        if Path(schedule_db_path).exists():
            total_migrated += self.migrate_tasks_from_sqlite(schedule_db_path, 'apscheduler_jobs')
            total_migrated += self.migrate_tasks_from_sqlite(schedule_db_path, 'job_configs')

        logger.info(f"Total migrated rows: {total_migrated}")
        return total_migrated


# 全局单例
_db_manager: Optional[DuckDBManager] = None
_db_manager_readonly: Optional[DuckDBManager] = None


def get_duckdb_manager(read_only: bool = True) -> DuckDBManager:
    """
    获取全局 DuckDB 管理器实例

    Args:
        read_only: 是否使用只读模式（默认True，允许多进程并发访问）

    Returns:
        DuckDBManager 实例
    """
    global _db_manager, _db_manager_readonly

    if read_only:
        if _db_manager_readonly is None:
            _db_manager_readonly = DuckDBManager(read_only=True)
        return _db_manager_readonly
    else:
        if _db_manager is None:
            _db_manager = DuckDBManager(read_only=False)
        return _db_manager


def get_duckdb_writer() -> DuckDBManager:
    """
    获取 DuckDB 写入管理器实例（独占访问）

    注意：此函数返回的实例会独占数据库，其他进程无法访问。
    使用完毕后应尽快关闭连接。

    Returns:
        DuckDBManager 实例（读写模式）
    """
    # 每次创建新实例以避免长连接持有锁
    return DuckDBManager(read_only=False)
