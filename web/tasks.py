"""
Background task management for async operations with SQLite/DuckDB persistence
"""
import threading
import uuid
import json
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from filelock import FileLock
import logging
import numpy as np

# Import settings to determine which backend to use
from config.settings import USE_DUCKDB_FOR_TASKS


def convert_numpy_to_native(obj):
    """Convert numpy types to native Python types for DuckDB compatibility"""
    if isinstance(obj, dict):
        return {k: convert_numpy_to_native(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_to_native(item) for item in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    else:
        return obj

# Setup backend selection
if USE_DUCKDB_FOR_TASKS:
    # Use DuckDB implementation (defined below in this file)
    TaskManager = None  # Will use TaskManagerDuckDB
else:
    # Use SQLite implementation (defined below in this file)
    TaskManager = None  # Will use TaskManager (SQLite version)


class TaskManager:
    """
    Database-backed task manager for tracking background async operations.
    Thread-safe implementation for Flask multi-threaded environment.
    Tasks persist across application restarts.
    """

    def __init__(self, db_path=None, checkpoint_dir=None, init_db=True):
        """
        Initialize TaskManager with database path

        Args:
            db_path: Path to SQLite database (default: ./data/tasks.db)
            checkpoint_dir: Path to checkpoint files directory
            init_db: Whether to initialize the database schema.
                     Should be False for web service, True for worker.
                     (default: True)
        """
        self.db_path = db_path or Path("./data/tasks.db")
        self.lock = threading.RLock()  # 可重入锁，防止死锁
        self.checkpoint_dir = checkpoint_dir or Path("./data/checkpoints")
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        # In-memory sets for fast, lock-free stop/pause requests
        self._stop_requested = set()
        self._pause_requested = set()
        self._memory_lock = threading.Lock()

        # Ensure parent directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        # Initialize database schema (only if requested)
        # Web service should NOT init_db to avoid lock conflicts with worker
        if init_db:
            self._init_db()

    def _get_db_connection(self, timeout=30):
        """
        Create a database connection with proper settings for concurrency.

        Args:
            timeout: Busy timeout in seconds (default: 30)

        Returns:
            SQLite connection object
        """
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=timeout)
        # Set longer busy timeout for this connection
        conn.execute('PRAGMA busy_timeout=30000')  # 30 seconds
        return conn

    def _init_db(self):
        """Create task tables if they don't exist"""
        import time
        max_retries = 5
        retry_delay = 0.5

        for attempt in range(max_retries):
            try:
                conn = self._get_db_connection(timeout=30)
                cursor = conn.cursor()

                # Create tasks table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS tasks (
                        task_id TEXT PRIMARY KEY,
                        task_type TEXT NOT NULL,
                        status TEXT NOT NULL,
                        progress INTEGER DEFAULT 0,
                        message TEXT,
                        result TEXT,
                        error TEXT,
                        current_stock_index INTEGER DEFAULT 0,
                        total_stocks INTEGER DEFAULT 0,
                        stats TEXT,
                        params TEXT,
                        metadata TEXT,
                        checkpoint_path TEXT,
                        stop_requested INTEGER DEFAULT 0,
                        pause_requested INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        completed_at TIMESTAMP
                    )
                ''')

                # Create checkpoints table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS task_checkpoints (
                        task_id TEXT PRIMARY KEY,
                        current_index INTEGER NOT NULL,
                        stats TEXT,
                        stage TEXT DEFAULT 'stock',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (task_id) REFERENCES tasks(task_id)
                    )
                ''')

                # Add stage column to existing table if not exists (for backward compatibility)
                try:
                    cursor.execute('ALTER TABLE task_checkpoints ADD COLUMN stage TEXT DEFAULT "stock"')
                except Exception:
                    pass  # Column already exists

                # Create indexes for performance
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks(task_type)')

                # Enable WAL mode for better concurrency (allows multiple readers + one writer)
                cursor.execute('PRAGMA journal_mode=WAL')
                cursor.execute('PRAGMA synchronous=NORMAL')  # Faster writes with good safety

                conn.commit()
                conn.close()
                return

            except sqlite3.OperationalError as e:
                if 'database is locked' in str(e).lower() and attempt < max_retries - 1:
                    print(f"[TaskManager._init_db] 数据库锁定，重试 {attempt + 1}/{max_retries}...")
                    time.sleep(retry_delay)
                    retry_delay *= 1.5
                else:
                    print(f"[TaskManager._init_db] 初始化数据库失败: {e}")
                    raise
            except Exception as e:
                print(f"[TaskManager._init_db] 初始化数据库时发生错误: {e}")
                raise

    def is_stop_requested(self, task_id):
        """Check if task stop has been requested (lock-free with DB fallback)"""
        # Check in-memory set first (fast)
        with self._memory_lock:
            if task_id in self._stop_requested:
                return True

        # Also check database (for worker process with separate memory)
        try:
            task = self.get_task(task_id)
            if task and task.get('stop_requested', False):
                # Also update memory set for faster future checks
                with self._memory_lock:
                    self._stop_requested.add(task_id)
                return True
        except Exception:
            pass

        return False

    def is_pause_requested(self, task_id):
        """Check if task pause has been requested (lock-free with DB fallback)"""
        # Also check database (for worker process with separate memory)
        try:
            task = self.get_task(task_id)
            if task:
                db_pause_requested = task.get('pause_requested', False)
                # Sync in-memory set with database state
                with self._memory_lock:
                    if db_pause_requested:
                        self._pause_requested.add(task_id)
                    else:
                        self._pause_requested.discard(task_id)
                if db_pause_requested:
                    return True
        except Exception:
            pass

        return False

    def request_stop(self, task_id):
        """Request task to stop (lock-free, also updates DB directly)"""
        # Check if task is pending and directly mark as stopped
        try:
            task = self.get_task(task_id)
            if task and task['status'] == 'pending':
                self.update_task(task_id, status='stopped', message='任务已取消')
                return True
        except:
            pass  # Fall through to normal stop request

        # Set memory flag (fast, lock-free)
        with self._memory_lock:
            self._stop_requested.add(task_id)

        # Update database directly WITHOUT using update_task (avoid lock contention)
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE tasks
                SET stop_requested = 1, updated_at = ?
                WHERE task_id = ?
            ''', (now, task_id))
            conn.commit()
            conn.close()
        except Exception as e:
            # If DB update fails, memory flag is still set, so stop will still work
            pass

        return True  # Always return success

    def request_pause(self, task_id):
        """Request task to pause (lock-free, also updates DB directly)"""
        # Set memory flag (fast, lock-free)
        with self._memory_lock:
            self._pause_requested.add(task_id)

        # Update database directly WITHOUT using update_task (avoid lock contention)
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE tasks
                SET pause_requested = 1, updated_at = ?
                WHERE task_id = ?
            ''', (now, task_id))
            conn.commit()
            conn.close()
        except Exception as e:
            # If DB update fails, memory flag is still set, so pause will still work
            pass

        return True  # Always return success

    def clear_stop_request(self, task_id):
        """Clear stop request (lock-free)"""
        with self._memory_lock:
            self._stop_requested.discard(task_id)

    def clear_pause_request(self, task_id):
        """Clear pause request (lock-free)"""
        with self._memory_lock:
            self._pause_requested.discard(task_id)

    def has_active_task(self):
        """
        检查是否存在活动任务（pending, running, 或 paused）

        Returns:
            tuple: (bool: has_active, dict: task_info or None)
        """
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT task_id, task_type, status, created_at,
                       current_stock_index, total_stocks, progress, message
                FROM tasks
                WHERE status IN ('pending', 'running', 'paused')
                ORDER BY created_at DESC
                LIMIT 1
            ''')

            row = cursor.fetchone()

            if row:
                return True, dict(row)
            return False, None

        finally:
            conn.close()

    def create_task(self, task_type, params, metadata=None):
        """
        创建新任务并保存到数据库

        Args:
            task_type: 任务类型（如 'update_stock_prices'）
            params: 任务参数
            metadata: 可选元数据（如 total_stocks）

        Returns:
            task_id: 任务唯一标识符
        """
        # 获取锁防止竞态条件
        with self.lock:
            task_id = str(uuid.uuid4())
            checkpoint_path = str(self.checkpoint_dir / f"{task_id}.json")
            created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            conn = self._get_db_connection(timeout=30)
            cursor = conn.cursor()

            try:
                cursor.execute('''
                    INSERT INTO tasks (
                        task_id, task_type, status, progress, message,
                        params, total_stocks, checkpoint_path, stats, created_at
                    ) VALUES (?, ?, 'pending', 0, '任务已创建', ?, ?, ?, ?, ?)
                ''', (
                    task_id,
                    task_type,
                    json.dumps(params) if params else None,
                    metadata.get('total_stocks') if metadata else 0,
                    checkpoint_path,
                    json.dumps({'success': 0, 'failed': 0, 'skipped': 0}),
                    created_at
                ))

                conn.commit()
            except Exception as e:
                conn.rollback()
                raise
            finally:
                conn.close()

            return task_id

    def update_task(self, task_id, **kwargs):
        """
        Update task information in database

        Args:
            task_id: Task identifier
            **kwargs: Fields to update (status, progress, message, result, stats, etc.)
        """
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"[update_task] Acquiring lock for task {task_id}...")
        with self.lock:
            logger.info(f"[update_task] Lock acquired for task {task_id}")
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()

            # Build update statement dynamically
            updates = []
            values = []
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Handle special fields that need JSON serialization
            if 'stats' in kwargs:
                updates.append('stats = ?')
                values.append(json.dumps(kwargs['stats']))
                kwargs.pop('stats')

            if 'result' in kwargs:
                updates.append('result = ?')
                values.append(json.dumps(kwargs['result']) if kwargs['result'] else None)
                kwargs.pop('result')

            # Handle boolean fields
            if 'stop_requested' in kwargs:
                updates.append('stop_requested = ?')
                values.append(1 if kwargs['stop_requested'] else 0)
                kwargs.pop('stop_requested')

            if 'pause_requested' in kwargs:
                updates.append('pause_requested = ?')
                values.append(1 if kwargs['pause_requested'] else 0)
                kwargs.pop('pause_requested')

            # Handle remaining fields
            for key, value in kwargs.items():
                updates.append(f'{key} = ?')
                values.append(value)

            # Always update updated_at timestamp with local time
            updates.append('updated_at = ?')
            values.append(now)

            # Set completed_at for terminal states
            status = kwargs.get('status')
            if status in ['completed', 'failed', 'stopped', 'cancelled']:
                updates.append('completed_at = ?')
                values.append(now)

            values.append(task_id)

            cursor.execute(f'''
                UPDATE tasks
                SET {', '.join(updates)}
                WHERE task_id = ?
            ''', values)

            conn.commit()
            conn.close()
            logger.info(f"[update_task] Lock released for task {task_id}")

    def get_task(self, task_id):
        """
        Get task information from database

        Args:
            task_id: Task identifier

        Returns:
            Task dictionary or None if not found
        """
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM tasks WHERE task_id = ?', (task_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        task = dict(row)

        # Parse JSON fields
        if task.get('stats'):
            try:
                task['stats'] = json.loads(task['stats'])
            except (json.JSONDecodeError, TypeError):
                task['stats'] = {'success': 0, 'failed': 0, 'skipped': 0}

        if task.get('params'):
            try:
                task['params'] = json.loads(task['params'])
            except (json.JSONDecodeError, TypeError):
                task['params'] = {}

        if task.get('metadata'):
            try:
                task['metadata'] = json.loads(task['metadata'])
            except (json.JSONDecodeError, TypeError):
                task['metadata'] = {}

        if task.get('result'):
            try:
                task['result'] = json.loads(task['result'])
            except (json.JSONDecodeError, TypeError):
                task['result'] = None

        # Convert boolean fields
        task['stop_requested'] = bool(task.get('stop_requested', 0))
        task['pause_requested'] = bool(task.get('pause_requested', 0))

        # Format datetime fields to ISO 8601 with timezone
        task['created_at'] = self._format_datetime(task.get('created_at'))
        task['updated_at'] = self._format_datetime(task.get('updated_at'))
        task['completed_at'] = self._format_datetime(task.get('completed_at'))

        return task

    def _format_datetime(self, dt_str):
        """
        Convert datetime string to ISO 8601 format with timezone

        Args:
            dt_str: Datetime string in format 'YYYY-MM-DD HH:MM:SS' or None

        Returns:
            ISO 8601 formatted string with timezone (e.g., '2024-01-01T10:00:00+08:00') or None
        """
        if not dt_str:
            return None
        try:
            # Parse the datetime string (assume it's in local timezone)
            dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
            # Format as ISO 8601 with timezone info
            # Get current timezone offset
            timestamp = dt.timestamp()
            tz_offset = datetime.fromtimestamp(timestamp) - datetime.utcfromtimestamp(timestamp)
            tz_offset_secs = int(tz_offset.total_seconds())
            tz_offset_hours = tz_offset_secs // 3600
            tz_offset_mins = abs(tz_offset_secs // 60) % 60
            tz_sign = '+' if tz_offset_hours >= 0 else '-'
            tz_str = f"{tz_sign}{abs(tz_offset_hours):02d}:{tz_offset_mins:02d}"

            return f"{dt.isoformat()}{tz_str}"
        except (ValueError, TypeError):
            return dt_str

    def get_all_tasks(self, status=None, limit=None):
        """
        Get all tasks from database

        Args:
            status: Optional status filter
            limit: Optional maximum number of tasks to return

        Returns:
            List of task dictionaries
        """
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query = 'SELECT * FROM tasks'
        params = []

        if status:
            query += ' WHERE status = ?'
            params.append(status)

        query += ' ORDER BY created_at DESC'

        if limit:
            query += ' LIMIT ?'
            params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        tasks = []
        for row in rows:
            task = dict(row)

            # Parse JSON fields
            if task.get('stats'):
                try:
                    task['stats'] = json.loads(task['stats'])
                except (json.JSONDecodeError, TypeError):
                    task['stats'] = {'success': 0, 'failed': 0, 'skipped': 0}

            if task.get('params'):
                try:
                    task['params'] = json.loads(task['params'])
                except (json.JSONDecodeError, TypeError):
                    task['params'] = {}

            if task.get('metadata'):
                try:
                    task['metadata'] = json.loads(task['metadata'])
                except (json.JSONDecodeError, TypeError):
                    task['metadata'] = {}

            if task.get('result'):
                try:
                    task['result'] = json.loads(task['result'])
                except (json.JSONDecodeError, TypeError):
                    task['result'] = None

            # Convert boolean fields
            task['stop_requested'] = bool(task.get('stop_requested', 0))
            task['pause_requested'] = bool(task.get('pause_requested', 0))

            # Format datetime fields to ISO 8601 with timezone
            task['created_at'] = self._format_datetime(task.get('created_at'))
            task['updated_at'] = self._format_datetime(task.get('updated_at'))
            task['completed_at'] = self._format_datetime(task.get('completed_at'))

            tasks.append(task)

        return tasks

    def claim_task(self):
        """
        Atomically claim a pending task for execution.

        This method uses an atomic UPDATE to ensure that only one worker
        can claim a specific task. Multiple workers calling this method
        will get different tasks (or None if no tasks available).

        Returns:
            Task dictionary if successfully claimed, None if no tasks available
        """
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            # Start transaction
            conn.execute('BEGIN TRANSACTION')

            # Get the oldest pending task
            cursor.execute('''
                SELECT * FROM tasks
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
            ''')

            row = cursor.fetchone()

            if not row:
                conn.rollback()
                conn.close()
                return None

            task = dict(row)

            # Parse JSON fields
            if task.get('stats'):
                try:
                    task['stats'] = json.loads(task['stats'])
                except (json.JSONDecodeError, TypeError):
                    task['stats'] = {'success': 0, 'failed': 0, 'skipped': 0}

            if task.get('params'):
                try:
                    task['params'] = json.loads(task['params'])
                except (json.JSONDecodeError, TypeError):
                    task['params'] = {}

            # Atomically claim the task by updating its status
            # Only one worker will succeed because of the WHERE status = 'pending' condition
            cursor.execute('''
                UPDATE tasks
                SET status = 'running',
                    updated_at = CURRENT_TIMESTAMP,
                    message = 'Worker正在执行任务...'
                WHERE task_id = ?
                  AND status = 'pending'
            ''', (task['task_id'],))

            # Check if the update was successful
            if cursor.rowcount == 0:
                # Another worker already claimed this task
                conn.rollback()
                conn.close()
                return None

            conn.commit()

            # Convert boolean fields
            task['stop_requested'] = bool(task.get('stop_requested', 0))
            task['pause_requested'] = bool(task.get('pause_requested', 0))

            # Format datetime fields
            task['created_at'] = self._format_datetime(task.get('created_at'))
            task['updated_at'] = self._format_datetime(task.get('updated_at'))
            task['completed_at'] = self._format_datetime(task.get('completed_at'))

            return task

        except Exception as e:
            conn.rollback()
            conn.close()
            raise e

    def delete_task(self, task_id):
        """
        Delete a task from database (cleanup)

        Args:
            task_id: Task identifier

        Raises:
            TimeoutError: If task is running and lock cannot be acquired
        """
        # First check task status to determine if we need the lock
        task = self.get_task(task_id)
        if not task:
            # Task doesn't exist, nothing to delete
            return

        # If task is stopped/completed/failed, we can delete without acquiring lock
        # (only running/paused tasks might be using the lock)
        if task['status'] in ['stopped', 'completed', 'failed', 'cancelled']:
            # Direct deletion without lock - task is not running
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            try:
                # Delete checkpoint first
                cursor.execute('DELETE FROM task_checkpoints WHERE task_id = ?', (task_id,))
                # Delete task
                cursor.execute('DELETE FROM tasks WHERE task_id = ?', (task_id,))
                conn.commit()
            finally:
                conn.close()

            # Also clear from memory sets
            with self._memory_lock:
                self._stop_requested.discard(task_id)
                self._pause_requested.discard(task_id)
            return

        # For running/paused tasks, request stop first and try to acquire lock
        with self._memory_lock:
            self._stop_requested.add(task_id)

        # Try to acquire lock with timeout
        acquired = False
        try:
            acquired = self.lock.acquire(blocking=True, timeout=2)
            if not acquired:
                # Task might be running, wait a bit for it to notice stop request
                import time
                time.sleep(0.5)
                acquired = self.lock.acquire(blocking=True, timeout=2)
                if not acquired:
                    raise TimeoutError(f"Could not acquire lock to delete task {task_id}. The task might still be running.")

            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()

            # Delete checkpoint first
            cursor.execute('DELETE FROM task_checkpoints WHERE task_id = ?', (task_id,))
            # Delete task
            cursor.execute('DELETE FROM tasks WHERE task_id = ?', (task_id,))

            conn.commit()
            conn.close()

        finally:
            if acquired:
                self.lock.release()

        # Also clear from memory sets
        with self._memory_lock:
            self._stop_requested.discard(task_id)
            self._pause_requested.discard(task_id)

    def cleanup_old_tasks(self, max_age_hours=24):
        """
        Cleanup old completed/failed tasks from database

        Args:
            max_age_hours: Maximum age in hours to keep tasks
        """
        with self.lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()

            cutoff = (datetime.now() - timedelta(hours=max_age_hours)).isoformat()

            # Delete checkpoints first
            cursor.execute('''
                DELETE FROM task_checkpoints
                WHERE task_id IN (
                    SELECT task_id FROM tasks
                    WHERE status IN ('completed', 'failed', 'cancelled')
                    AND completed_at < ?
                )
            ''', (cutoff,))

            # Delete tasks
            cursor.execute('''
                DELETE FROM tasks
                WHERE status IN ('completed', 'failed', 'cancelled')
                AND completed_at < ?
            ''', (cutoff,))

            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()

            return deleted_count

    def cleanup_stale_tasks(self, stale_threshold_hours=24):
        """
        将卡在 pending/running 状态过久的任务标记为 'failed'

        Args:
            stale_threshold_hours: 多少小时后认为任务陈旧

        Returns:
            int: 清理的任务数量
        """
        import time
        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                with self.lock:
                    conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=10)
                    cursor = conn.cursor()

                    cutoff = (datetime.now() - timedelta(hours=stale_threshold_hours)).strftime('%Y-%m-%d %H:%M:%S')

                    # 查找陈旧任务
                    cursor.execute('''
                        UPDATE tasks
                        SET status = 'failed',
                            message = '任务已超时',
                            completed_at = ?
                        WHERE status IN ('pending', 'running', 'paused')
                        AND created_at < ?
                    ''', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), cutoff))

                    cleaned_count = cursor.rowcount
                    conn.commit()
                    conn.close()

                    if cleaned_count > 0:
                        print(f"[TaskManager] 清理了 {cleaned_count} 个陈旧任务")

                    return cleaned_count

            except sqlite3.OperationalError as e:
                if 'database is locked' in str(e).lower() and attempt < max_retries - 1:
                    print(f"[TaskManager] 数据库锁定，重试 {attempt + 1}/{max_retries}...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
                else:
                    print(f"[TaskManager] 清理陈旧任务失败: {e}")
                    return 0
            except Exception as e:
                print(f"[TaskManager] 清理陈旧任务时发生错误: {e}")
                return 0

    def pause_task(self, task_id):
        """
        Request to pause a running task

        Args:
            task_id: Task identifier

        Returns:
            bool: True if task was paused, False otherwise
        """
        with self.lock:
            task = self.get_task(task_id)
            if task and task['status'] == 'running':
                self.update_task(task_id, pause_requested=True)
                return True
        return False

    def resume_task(self, task_id):
        """
        Resume a paused task

        Args:
            task_id: Task identifier

        Returns:
            bool: True if task was resumed, False otherwise
        """
        # Clear memory flag first (lock-free, ensures worker sees the change)
        with self._memory_lock:
            self._pause_requested.discard(task_id)

        with self.lock:
            task = self.get_task(task_id)
            if task and task['status'] == 'paused':
                self.update_task(task_id, pause_requested=False, status='running')
                return True
        return False

    # Note: request_stop method is defined earlier (line 103) as a lock-free version

    def get_unfinished_tasks(self):
        """
        Get all unfinished tasks (running or paused)

        Returns:
            List of unfinished task dictionaries
        """
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM tasks
            WHERE status IN ('running', 'paused')
            ORDER BY created_at ASC
        ''')

        rows = cursor.fetchall()
        conn.close()

        tasks = []
        for row in rows:
            task = dict(row)

            # Parse JSON fields
            if task.get('params'):
                try:
                    task['params'] = json.loads(task['params'])
                except (json.JSONDecodeError, TypeError):
                    task['params'] = {}

            if task.get('stats'):
                try:
                    task['stats'] = json.loads(task['stats'])
                except (json.JSONDecodeError, TypeError):
                    task['stats'] = {'success': 0, 'failed': 0, 'skipped': 0}

            # Convert boolean fields
            task['stop_requested'] = bool(task.get('stop_requested', 0))
            task['pause_requested'] = bool(task.get('pause_requested', 0))

            tasks.append(task)

        return tasks

    def save_checkpoint(self, task_id, current_index, stats=None, stage='stock'):
        """
        Save task checkpoint to database for resume capability

        Args:
            task_id: Task identifier
            current_index: Current stock index
            stats: Optional stats dict (success, failed, skipped counts)
            stage: Optional stage identifier for multi-stage tasks (e.g., 'stock', 'financial')
        """
        with self.lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()

            try:
                checkpoint_data = json.dumps(stats or {})
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute('''
                    INSERT OR REPLACE INTO task_checkpoints (task_id, current_index, stats, stage, created_at)
                    VALUES (?, ?, ?, ?, ?)
                ''', (task_id, current_index, checkpoint_data, stage, now))

                conn.commit()
                return True
            except Exception as e:
                print(f"Error saving checkpoint: {e}")
                return False
            finally:
                conn.close()

    def load_checkpoint(self, task_id):
        """
        Load task checkpoint from database

        Args:
            task_id: Task identifier

        Returns:
            dict with 'current_index', 'stats', and 'stage', or None if not found
        """
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT * FROM task_checkpoints WHERE task_id = ?', (task_id,))
            row = cursor.fetchone()

            if row:
                stats_json = row['stats']
                # Convert row to dict to use .get() method
                row_dict = dict(row)
                stage = row_dict.get('stage', 'stock')
                return {
                    'task_id': row['task_id'],
                    'current_index': row['current_index'],
                    'stats': json.loads(stats_json) if stats_json else {},
                    'stage': stage,
                    'timestamp': row['created_at']
                }
            return None
        except Exception as e:
            print(f"Error loading checkpoint: {e}")
            return None
        finally:
            conn.close()

    def delete_checkpoint(self, task_id):
        """
        Delete task checkpoint from database

        Args:
            task_id: Task identifier
        """
        with self.lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()

            try:
                cursor.execute('DELETE FROM task_checkpoints WHERE task_id = ?', (task_id,))
                conn.commit()
            except Exception as e:
                print(f"Error deleting checkpoint: {e}")
            finally:
                conn.close()

    def increment_stats(self, task_id, stat_type, count=1):
        """
        Increment task statistics

        Args:
            task_id: Task identifier
            stat_type: Type of stat ('success', 'failed', 'skipped')
            count: Amount to increment (default 1)
        """
        with self.lock:
            task = self.get_task(task_id)
            if task:
                stats = task.get('stats', {'success': 0, 'failed': 0, 'skipped': 0})
                stats[stat_type] = stats.get(stat_type, 0) + count
                self.update_task(task_id, stats=stats)


# Global instance
task_manager = None


def init_task_manager(db_path=None, checkpoint_dir=None, init_db=True):
    """
    Initialize the global task manager (SQLite-based)

    This function now always returns the SQLite version of TaskManager
    to resolve DuckDB lock conflicts between API and Worker processes.

    Args:
        db_path: Path to SQLite task database (default: TASKS_DB_PATH from settings)
        checkpoint_dir: Path to checkpoint directory (optional)
        init_db: Whether to initialize database schema (default: True)
                 Should be False for web service to avoid lock conflicts

    Returns:
        TaskManager instance (SQLite-based)
    """
    global task_manager
    if task_manager is None:
        if USE_DUCKDB_FOR_TASKS:
            task_manager = TaskManagerDuckDB(checkpoint_dir=checkpoint_dir, init_db=init_db)
        else:
            task_manager = TaskManager(db_path=db_path, checkpoint_dir=checkpoint_dir, init_db=init_db)
    return task_manager


# ============================================================================
# DuckDB Task Manager Implementation
# ============================================================================

class TaskManagerDuckDB:
    """
    DuckDB-backed task manager for tracking background async operations.
    Thread-safe implementation for Flask multi-threaded environment.
    Tasks persist across application restarts.
    """

    def __init__(self, checkpoint_dir=None, init_db=True):
        """
        Initialize TaskManager with DuckDB

        Args:
            checkpoint_dir: Path to checkpoint files directory
            init_db: Whether to initialize the database schema.
                     Should be False for web service, True for worker.
                     (default: True)
        """
        self.lock = threading.RLock()  # 可重入锁，防止死锁
        self.checkpoint_dir = checkpoint_dir or Path("./data/checkpoints")
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        # In-memory sets for fast, lock-free stop/pause requests
        self._stop_requested = set()
        self._pause_requested = set()
        self._memory_lock = threading.Lock()

        # Get DuckDB manager
        from src.db.duckdb_manager import get_duckdb_writer, get_duckdb_manager
        self._get_writer = get_duckdb_writer
        self._get_reader = get_duckdb_manager

        # Initialize database schema (only if requested)
        if init_db:
            self._init_db()

    def _init_db(self):
        """Create task tables if they don't exist"""
        manager = self._get_writer()
        try:
            manager.create_tasks_tables()
        finally:
            manager.close()

    def is_stop_requested(self, task_id):
        """Check if task stop has been requested (lock-free with DB fallback)"""
        # Check in-memory set first (fast)
        with self._memory_lock:
            if task_id in self._stop_requested:
                return True

        # Also check database (for worker process with separate memory)
        try:
            task = self.get_task(task_id)
            if task and task.get('stop_requested', False):
                # Also update memory set for faster future checks
                with self._memory_lock:
                    self._stop_requested.add(task_id)
                return True
        except Exception:
            pass

        return False

    def is_pause_requested(self, task_id):
        """Check if task pause has been requested (lock-free with DB fallback)"""
        # Also check database (for worker process with separate memory)
        try:
            task = self.get_task(task_id)
            if task:
                db_pause_requested = task.get('pause_requested', False)
                # Sync in-memory set with database state
                with self._memory_lock:
                    if db_pause_requested:
                        self._pause_requested.add(task_id)
                    else:
                        self._pause_requested.discard(task_id)
                if db_pause_requested:
                    return True
        except Exception:
            pass

        return False

    def request_stop(self, task_id):
        """Request task to stop (lock-free, also updates DB directly)"""
        # Check if task is pending and directly mark as stopped
        try:
            task = self.get_task(task_id)
            if task and task['status'] == 'pending':
                self.update_task(task_id, status='stopped', message='任务已取消')
                return True
        except:
            pass  # Fall through to normal stop request

        # Set memory flag (fast, lock-free)
        with self._memory_lock:
            self._stop_requested.add(task_id)

        # Create stop request flag file for Worker to check
        stop_flag_path = self.checkpoint_dir / f".stop_{task_id}"
        try:
            stop_flag_path.touch()
            logging.info(f"Created stop flag file: {stop_flag_path}")
        except Exception as e:
            logging.warning(f"Failed to create stop flag file: {e}")

        # Update database directly
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            manager = self._get_writer()
            try:
                manager.connect().execute('''
                    UPDATE tasks
                    SET stop_requested = 1, updated_at = ?
                    WHERE task_id = ?
                ''', [now, task_id])
            finally:
                manager.close()
        except Exception as e:
            logging.warning(f"Failed to update stop_requested in DB: {e}")

        return True

    def is_stop_requested(self, task_id):
        """Check if task stop has been requested (lock-free with DB fallback)"""
        # Check in-memory set first (fast)
        with self._memory_lock:
            if task_id in self._stop_requested:
                return True

        # For worker process, periodically check database to catch stop requests from API
        # Use a simple direct query to avoid connection overhead
        try:
            manager = self._get_writer()
            conn = manager.connect()
            result = conn.execute(
                "SELECT stop_requested FROM tasks WHERE task_id = ?",
                [task_id]
            ).fetchone()
            conn.close()

            if result and result[0]:
                # Update memory set
                with self._memory_lock:
                    self._stop_requested.add(task_id)
                return True
        except Exception:
            # If we can't connect (e.g., worker holding the lock), skip this check
            # The in-memory set will be updated by request_stop()
            pass

        return False

    def request_pause(self, task_id):
        """Request task to pause (lock-free, also updates DB directly)"""
        # Set memory flag (fast, lock-free)
        with self._memory_lock:
            self._pause_requested.add(task_id)

        # Update database directly
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            manager = self._get_writer()
            try:
                manager.connect().execute('''
                    UPDATE tasks
                    SET pause_requested = 1, updated_at = ?
                    WHERE task_id = ?
                ''', [now, task_id])
            finally:
                manager.close()
        except Exception as e:
            logging.warning(f"Failed to update pause_requested in DB: {e}")

        return True

    def clear_stop_request(self, task_id):
        """Clear stop request (lock-free)"""
        with self._memory_lock:
            self._stop_requested.discard(task_id)

    def clear_pause_request(self, task_id):
        """Clear pause request (lock-free)"""
        with self._memory_lock:
            self._pause_requested.discard(task_id)

    def has_active_task(self):
        """
        检查是否存在活动任务（pending, running, 或 paused）

        Returns:
            tuple: (bool: has_active, dict: task_info or None)
        """
        manager = self._get_reader()

        try:
            df = manager.connect().execute('''
                SELECT task_id, task_type, status, created_at,
                       current_stock_index, total_stocks, progress, message
                FROM tasks
                WHERE status IN ('pending', 'running', 'paused')
                ORDER BY created_at DESC
                LIMIT 1
            ''').fetchdf()

            if len(df) > 0:
                row = df.iloc[0]
                return True, row.to_dict()
            return False, None
        except Exception as e:
            logging.error(f"Error checking active task: {e}")
            return False, None

    def create_task(self, task_type, params, metadata=None):
        """
        创建新任务并保存到数据库

        Args:
            task_type: 任务类型（如 'update_stock_prices'）
            params: 任务参数
            metadata: 可选元数据（如 total_stocks）

        Returns:
            task_id: 任务唯一标识符
        """
        # 获取锁防止竞态条件
        with self.lock:
            manager = self._get_writer()

            try:
                task_id = str(uuid.uuid4())
                checkpoint_path = str(self.checkpoint_dir / f"{task_id}.json")
                created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                manager.connect().execute('''
                    INSERT INTO tasks (
                        task_id, task_type, status, progress, message,
                        params, total_stocks, checkpoint_path, stats, created_at
                    ) VALUES (?, ?, 'pending', 0, '任务已创建', ?, ?, ?, ?, ?)
                ''', [
                    task_id,
                    task_type,
                    json.dumps(params) if params else None,
                    metadata.get('total_stocks') if metadata else 0,
                    checkpoint_path,
                    json.dumps({'success': 0, 'failed': 0, 'skipped': 0}),
                    created_at
                ])

                return task_id

            finally:
                manager.close()

    def update_task(self, task_id, **kwargs):
        """
        Update task information in database

        Args:
            task_id: Task identifier
            **kwargs: Fields to update (status, progress, message, result, stats, etc.)
        """
        with self.lock:
            manager = self._get_writer()

            try:
                # Build update statement dynamically
                updates = []
                values = []
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # Handle special fields that need JSON serialization
                if 'stats' in kwargs:
                    # Convert numpy types to native Python types
                    stats_native = convert_numpy_to_native(kwargs['stats'])
                    updates.append('stats = ?')
                    values.append(json.dumps(stats_native))
                    kwargs.pop('stats')

                if 'result' in kwargs:
                    # Convert numpy types to native Python types
                    result_native = convert_numpy_to_native(kwargs['result']) if kwargs['result'] else None
                    updates.append('result = ?')
                    values.append(json.dumps(result_native) if result_native else None)
                    kwargs.pop('result')

                # Handle boolean fields
                if 'stop_requested' in kwargs:
                    updates.append('stop_requested = ?')
                    values.append(1 if kwargs['stop_requested'] else 0)
                    kwargs.pop('stop_requested')

                if 'pause_requested' in kwargs:
                    updates.append('pause_requested = ?')
                    values.append(1 if kwargs['pause_requested'] else 0)
                    kwargs.pop('pause_requested')

                # Always update updated_at timestamp
                updates.append('updated_at = ?')
                values.append(now)

                # Set completed_at for terminal states
                # Check status BEFORE removing it from kwargs
                status = kwargs.get('status')
                if status in ['completed', 'failed', 'stopped', 'cancelled']:
                    updates.append('completed_at = ?')
                    values.append(now)

                # Handle remaining fields (convert numpy types)
                for key, value in kwargs.items():
                    updates.append(f'{key} = ?')
                    # Convert numpy types to native Python types
                    values.append(convert_numpy_to_native(value))

                values.append(task_id)

                manager.connect().execute(f'''
                    UPDATE tasks
                    SET {', '.join(updates)}
                    WHERE task_id = ?
                ''', values)

            finally:
                manager.close()

    def get_task(self, task_id):
        """
        Get task information from database

        Args:
            task_id: Task identifier

        Returns:
            Task dictionary or None if not found
        """
        manager = self._get_reader()

        try:
            df = manager.connect().execute('SELECT * FROM tasks WHERE task_id = ?', [task_id]).fetchdf()

            if len(df) == 0:
                return None

            task = df.iloc[0].to_dict()

            # Parse JSON fields
            if task.get('stats'):
                try:
                    task['stats'] = json.loads(task['stats'])
                except (json.JSONDecodeError, TypeError):
                    task['stats'] = {'success': 0, 'failed': 0, 'skipped': 0}

            if task.get('params'):
                try:
                    task['params'] = json.loads(task['params'])
                except (json.JSONDecodeError, TypeError):
                    task['params'] = {}

            if task.get('metadata'):
                try:
                    task['metadata'] = json.loads(task['metadata'])
                except (json.JSONDecodeError, TypeError):
                    task['metadata'] = {}

            if task.get('result'):
                try:
                    task['result'] = json.loads(task['result'])
                except (json.JSONDecodeError, TypeError):
                    task['result'] = None

            # Convert boolean fields
            task['stop_requested'] = bool(task.get('stop_requested', 0))
            task['pause_requested'] = bool(task.get('pause_requested', 0))

            # Format datetime fields
            task['created_at'] = self._format_datetime(task.get('created_at'))
            task['updated_at'] = self._format_datetime(task.get('updated_at'))
            task['completed_at'] = self._format_datetime(task.get('completed_at'))

            return task

        except Exception as e:
            logging.error(f"Error getting task {task_id}: {e}")
            return None

    def _format_datetime(self, dt):
        """
        Convert datetime to ISO 8601 format with timezone

        Args:
            dt: Datetime string, pandas Timestamp, datetime object, or None

        Returns:
            ISO 8601 formatted string with timezone or None
        """
        if not dt:
            return None

        # Handle pandas Timestamp
        if hasattr(dt, 'isoformat'):
            return dt.isoformat()

        # Handle datetime string
        if isinstance(dt, str):
            try:
                # Parse the datetime string (assume it's in local timezone)
                dt_obj = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
                # Format as ISO 8601 with timezone info
                timestamp = dt_obj.timestamp()
                tz_offset = datetime.fromtimestamp(timestamp) - datetime.utcfromtimestamp(timestamp)
                tz_offset_secs = int(tz_offset.total_seconds())
                tz_offset_hours = tz_offset_secs // 3600
                tz_offset_mins = abs(tz_offset_secs // 60) % 60
                tz_sign = '+' if tz_offset_hours >= 0 else '-'
                tz_str = f"{tz_sign}{abs(tz_offset_hours):02d}:{tz_offset_mins:02d}"

                return f"{dt_obj.isoformat()}{tz_str}"
            except (ValueError, TypeError):
                return dt

        return dt

    def get_all_tasks(self, status=None, limit=None):
        """
        Get all tasks from database

        Args:
            status: Optional status filter
            limit: Optional maximum number of tasks to return

        Returns:
            List of task dictionaries
        """
        # Use a temporary connection to avoid conflicts with worker
        from src.db.duckdb_manager import DuckDBManager
        manager = DuckDBManager(read_only=True)

        try:
            query = 'SELECT * FROM tasks'
            params = []

            if status:
                query += ' WHERE status = ?'
                params.append(status)

            query += ' ORDER BY created_at DESC'

            if limit:
                query += ' LIMIT ?'
                params.append(limit)

            if params:
                df = manager.connect().execute(query, params).fetchdf()
            else:
                df = manager.connect().execute(query).fetchdf()

            tasks = []
            for _, row in df.iterrows():
                task = row.to_dict()

                # Parse JSON fields
                if task.get('stats'):
                    try:
                        task['stats'] = json.loads(task['stats'])
                    except (json.JSONDecodeError, TypeError):
                        task['stats'] = {'success': 0, 'failed': 0, 'skipped': 0}

                if task.get('params'):
                    try:
                        task['params'] = json.loads(task['params'])
                    except (json.JSONDecodeError, TypeError):
                        task['params'] = {}

                if task.get('metadata'):
                    try:
                        task['metadata'] = json.loads(task['metadata'])
                    except (json.JSONDecodeError, TypeError):
                        task['metadata'] = {}

                if task.get('result'):
                    try:
                        task['result'] = json.loads(task['result'])
                    except (json.JSONDecodeError, TypeError):
                        task['result'] = None

                # Convert boolean fields
                task['stop_requested'] = bool(task.get('stop_requested', 0))
                task['pause_requested'] = bool(task.get('pause_requested', 0))

                # Format datetime fields
                task['created_at'] = self._format_datetime(task.get('created_at'))
                task['updated_at'] = self._format_datetime(task.get('updated_at'))
                task['completed_at'] = self._format_datetime(task.get('completed_at'))

                tasks.append(task)

            return tasks

        except Exception as e:
            logging.error(f"Error getting all tasks: {e}")
            return []
        finally:
            manager.close()

    def claim_task(self):
        """
        Atomically claim a pending task for execution.

        This method uses an atomic UPDATE to ensure that only one worker
        can claim a specific task. Multiple workers calling this method
        will get different tasks (or None if no tasks available).

        Returns:
            Task dictionary if successfully claimed, None if no tasks available
        """
        manager = self._get_writer()

        try:
            # Get the oldest pending task
            df = manager.connect().execute('''
                SELECT * FROM tasks
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
            ''').fetchdf()

            if len(df) == 0:
                return None

            task = df.iloc[0].to_dict()

            # Parse JSON fields
            if task.get('stats'):
                try:
                    task['stats'] = json.loads(task['stats'])
                except (json.JSONDecodeError, TypeError):
                    task['stats'] = {'success': 0, 'failed': 0, 'skipped': 0}

            if task.get('params'):
                try:
                    task['params'] = json.loads(task['params'])
                except (json.JSONDecodeError, TypeError):
                    task['params'] = {}

            # Atomically claim the task by updating its status
            # Only one worker will succeed because of the WHERE status = 'pending' condition
            result = manager.connect().execute('''
                UPDATE tasks
                SET status = 'running',
                    updated_at = CURRENT_TIMESTAMP,
                    message = 'Worker正在执行任务...'
                WHERE task_id = ?
                  AND status = 'pending'
            ''', [task['task_id']])

            # Check if the update was successful
            if result.fetchone()[0] == 0:
                # Another worker already claimed this task
                return None

            # Convert boolean fields
            task['stop_requested'] = bool(task.get('stop_requested', 0))
            task['pause_requested'] = bool(task.get('pause_requested', 0))

            # Format datetime fields
            task['created_at'] = self._format_datetime(task.get('created_at'))
            task['updated_at'] = self._format_datetime(task.get('updated_at'))
            task['completed_at'] = self._format_datetime(task.get('completed_at'))

            return task

        except Exception as e:
            logging.error(f"Error claiming task: {e}")
            return None
        finally:
            manager.close()

    def delete_task(self, task_id):
        """
        Delete a task from database (cleanup)

        Args:
            task_id: Task identifier
        """
        # First check task status
        task = self.get_task(task_id)
        if not task:
            return

        # If task is stopped/completed/failed, we can delete
        if task['status'] in ['stopped', 'completed', 'failed', 'cancelled']:
            manager = self._get_writer()
            try:
                # Delete checkpoint first
                manager.connect().execute('DELETE FROM task_checkpoints WHERE task_id = ?', [task_id])
                # Delete task
                manager.connect().execute('DELETE FROM tasks WHERE task_id = ?', [task_id])
            finally:
                manager.close()

            # Also clear from memory sets
            with self._memory_lock:
                self._stop_requested.discard(task_id)
                self._pause_requested.discard(task_id)
            return

        # For running/paused tasks, request stop first
        with self._memory_lock:
            self._stop_requested.add(task_id)

        # Try to acquire lock with timeout
        acquired = False
        try:
            acquired = self.lock.acquire(blocking=True, timeout=2)
            if not acquired:
                import time
                time.sleep(0.5)
                acquired = self.lock.acquire(blocking=True, timeout=2)
                if not acquired:
                    raise TimeoutError(f"Could not acquire lock to delete task {task_id}")

            manager = self._get_writer()
            try:
                # Delete checkpoint first
                manager.connect().execute('DELETE FROM task_checkpoints WHERE task_id = ?', [task_id])
                # Delete task
                manager.connect().execute('DELETE FROM tasks WHERE task_id = ?', [task_id])
            finally:
                manager.close()

        finally:
            if acquired:
                self.lock.release()

        # Also clear from memory sets
        with self._memory_lock:
            self._stop_requested.discard(task_id)
            self._pause_requested.discard(task_id)

    def cleanup_old_tasks(self, max_age_hours=24):
        """
        Cleanup old completed/failed tasks from database

        Args:
            max_age_hours: Maximum age in hours to keep tasks
        """
        with self.lock:
            manager = self._get_writer()

            try:
                cutoff = (datetime.now() - timedelta(hours=max_age_hours)).isoformat()

                # Delete checkpoints first
                manager.connect().execute('''
                    DELETE FROM task_checkpoints
                    WHERE task_id IN (
                        SELECT task_id FROM tasks
                        WHERE status IN ('completed', 'failed', 'cancelled')
                        AND completed_at < ?
                    )
                ''', [cutoff])

                # Delete tasks
                result = manager.connect().execute('''
                    DELETE FROM tasks
                    WHERE status IN ('completed', 'failed', 'cancelled')
                    AND completed_at < ?
                ''', [cutoff])

                deleted_count = result.fetchone()[0] if result else 0
                return deleted_count

            finally:
                manager.close()

    def cleanup_stale_tasks(self, stale_threshold_hours=24):
        """
        将卡在 pending/running 状态过久的任务标记为 'failed'

        Args:
            stale_threshold_hours: 多少小时后认为任务陈旧

        Returns:
            int: 清理的任务数量
        """
        with self.lock:
            manager = self._get_writer()

            try:
                cutoff = (datetime.now() - timedelta(hours=stale_threshold_hours)).strftime('%Y-%m-%d %H:%M:%S')

                # 查找陈旧任务
                result = manager.connect().execute('''
                    UPDATE tasks
                    SET status = 'failed',
                        message = '任务已超时',
                        completed_at = ?
                    WHERE status IN ('pending', 'running', 'paused')
                    AND created_at < ?
                ''', [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), cutoff])

                cleaned_count = result.fetchone()[0] if result else 0

                if cleaned_count > 0:
                    logging.info(f"Cleaned up {cleaned_count} stale tasks")

                return cleaned_count

            except Exception as e:
                logging.error(f"Error cleaning stale tasks: {e}")
                return 0
            finally:
                manager.close()

    def pause_task(self, task_id):
        """
        Request to pause a running task

        Args:
            task_id: Task identifier

        Returns:
            bool: True if task was paused, False otherwise
        """
        with self.lock:
            task = self.get_task(task_id)
            if task and task['status'] == 'running':
                self.update_task(task_id, pause_requested=True)
                return True
        return False

    def resume_task(self, task_id):
        """
        Resume a paused task

        Args:
            task_id: Task identifier

        Returns:
            bool: True if task was resumed, False otherwise
        """
        # Clear memory flag first
        with self._memory_lock:
            self._pause_requested.discard(task_id)

        with self.lock:
            task = self.get_task(task_id)
            if task and task['status'] == 'paused':
                self.update_task(task_id, pause_requested=False, status='running')
                return True
        return False

    def get_unfinished_tasks(self):
        """
        Get all unfinished tasks (running or paused)

        Returns:
            List of unfinished task dictionaries
        """
        manager = self._get_reader()

        try:
            df = manager.connect().execute('''
                SELECT * FROM tasks
                WHERE status IN ('running', 'paused')
                ORDER BY created_at ASC
            ''').fetchdf()

            tasks = []
            for _, row in df.iterrows():
                task = row.to_dict()

                # Parse JSON fields
                if task.get('params'):
                    try:
                        task['params'] = json.loads(task['params'])
                    except (json.JSONDecodeError, TypeError):
                        task['params'] = {}

                if task.get('stats'):
                    try:
                        task['stats'] = json.loads(task['stats'])
                    except (json.JSONDecodeError, TypeError):
                        task['stats'] = {'success': 0, 'failed': 0, 'skipped': 0}

                # Convert boolean fields
                task['stop_requested'] = bool(task.get('stop_requested', 0))
                task['pause_requested'] = bool(task.get('pause_requested', 0))

                tasks.append(task)

            return tasks

        except Exception as e:
            logging.error(f"Error getting unfinished tasks: {e}")
            return []

    def save_checkpoint(self, task_id, current_index, stats=None, stage='stock'):
        """
        Save task checkpoint to database for resume capability

        Args:
            task_id: Task identifier
            current_index: Current stock index
            stats: Optional stats dict (success, failed, skipped counts)
            stage: Optional stage identifier for multi-stage tasks
        """
        with self.lock:
            manager = self._get_writer()

            try:
                checkpoint_data = json.dumps(stats or {})
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                manager.connect().execute('''
                    INSERT OR REPLACE INTO task_checkpoints (task_id, current_index, stats, stage, created_at)
                    VALUES (?, ?, ?, ?, ?)
                ''', [task_id, current_index, checkpoint_data, stage, now])

                return True
            except Exception as e:
                logging.error(f"Error saving checkpoint: {e}")
                return False
            finally:
                manager.close()

    def load_checkpoint(self, task_id):
        """
        Load task checkpoint from database

        Args:
            task_id: Task identifier

        Returns:
            dict with 'current_index', 'stats', and 'stage', or None if not found
        """
        manager = self._get_reader()

        try:
            df = manager.connect().execute('SELECT * FROM task_checkpoints WHERE task_id = ?', [task_id]).fetchdf()

            if len(df) > 0:
                row = df.iloc[0]
                stats_json = row.get('stats')
                return {
                    'task_id': row['task_id'],
                    'current_index': row['current_index'],
                    'stats': json.loads(stats_json) if stats_json else {},
                    'stage': row.get('stage', 'stock'),
                    'timestamp': row['created_at']
                }
            return None

        except Exception as e:
            logging.error(f"Error loading checkpoint: {e}")
            return None

    def delete_checkpoint(self, task_id):
        """
        Delete task checkpoint from database

        Args:
            task_id: Task identifier
        """
        with self.lock:
            manager = self._get_writer()

            try:
                manager.connect().execute('DELETE FROM task_checkpoints WHERE task_id = ?', [task_id])
            except Exception as e:
                logging.error(f"Error deleting checkpoint: {e}")
            finally:
                manager.close()

    def increment_stats(self, task_id, stat_type, count=1):
        """
        Increment task statistics

        Args:
            task_id: Task identifier
            stat_type: Type of stat ('success', 'failed', 'skipped')
            count: Amount to increment (default 1)
        """
        with self.lock:
            task = self.get_task(task_id)
            if task:
                stats = task.get('stats', {'success': 0, 'failed': 0, 'skipped': 0})
                stats[stat_type] = stats.get(stat_type, 0) + count
                self.update_task(task_id, stats=stats)


# Global instance for DuckDB
task_manager_duckdb = None


def init_task_manager_duckdb(checkpoint_dir=None, init_db=True):
    """
    Initialize the global DuckDB task manager

    Args:
        checkpoint_dir: Path to checkpoint directory (optional)
        init_db: Whether to initialize database schema (default: True)

    Returns:
        TaskManagerDuckDB instance
    """
    global task_manager_duckdb
    if task_manager_duckdb is None:
        task_manager_duckdb = TaskManagerDuckDB(checkpoint_dir=checkpoint_dir, init_db=init_db)
    return task_manager_duckdb
