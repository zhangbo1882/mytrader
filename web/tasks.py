"""
Background task management for async operations with SQLite persistence
"""
import threading
import uuid
import json
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from filelock import FileLock


class TaskManager:
    """
    Database-backed task manager for tracking background async operations.
    Thread-safe implementation for Flask multi-threaded environment.
    Tasks persist across application restarts.
    """

    def __init__(self, db_path=None, checkpoint_dir=None):
        """
        Initialize TaskManager with database path

        Args:
            db_path: Path to SQLite database (default: ./data/tasks.db)
            checkpoint_dir: Path to checkpoint files directory
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

        # Initialize database schema
        self._init_db()

    def _init_db(self):
        """Create task tables if they don't exist"""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (task_id) REFERENCES tasks(task_id)
            )
        ''')

        # Create indexes for performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks(task_type)')

        conn.commit()
        conn.close()

    def is_stop_requested(self, task_id):
        """Check if task stop has been requested (lock-free)"""
        with self._memory_lock:
            return task_id in self._stop_requested

    def is_pause_requested(self, task_id):
        """Check if task pause has been requested (lock-free)"""
        with self._memory_lock:
            return task_id in self._pause_requested

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
            task_type: 任务类型（如 'update_favorites'）
            params: 任务参数
            metadata: 可选元数据（如 total_stocks）

        Returns:
            task_id: 任务唯一标识符

        Raises:
            TaskExistsError: 如果活动任务已存在
        """
        # 关键：先获取锁防止竞态条件
        with self.lock:
            # 检查现有活动任务（原子操作）
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()

            # 查询活动任务（pending, running, paused）
            cursor.execute('''
                SELECT task_id, task_type, status, created_at,
                       current_stock_index, total_stocks, progress
                FROM tasks
                WHERE status IN ('pending', 'running', 'paused')
                ORDER BY created_at DESC
                LIMIT 1
            ''')

            existing_task = cursor.fetchone()
            conn.close()

            # 如果活动任务存在，抛出详细错误
            if existing_task:
                from web.exceptions import TaskExistsError

                task_dict = {
                    'task_id': existing_task[0],
                    'task_type': existing_task[1],
                    'status': existing_task[2],
                    'created_at': existing_task[3],
                    'current_stock_index': existing_task[4],
                    'total_stocks': existing_task[5],
                    'progress': existing_task[6]
                }
                raise TaskExistsError(
                    message=f"无法创建新任务：活动任务 {task_dict['task_id'][:8]}... 状态为 '{task_dict['status']}'",
                    existing_task=task_dict
                )

            # 没有活动任务，继续创建
            task_id = str(uuid.uuid4())
            checkpoint_path = str(self.checkpoint_dir / f"{task_id}.json")
            created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            conn = sqlite3.connect(self.db_path, check_same_thread=False)
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

        return task

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

            tasks.append(task)

        return tasks

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
        with self.lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
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

    def save_checkpoint(self, task_id, current_index, stats=None):
        """
        Save task checkpoint to database for resume capability

        Args:
            task_id: Task identifier
            current_index: Current stock index
            stats: Optional stats dict (success, failed, skipped counts)
        """
        with self.lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()

            try:
                checkpoint_data = json.dumps(stats or {})
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute('''
                    INSERT OR REPLACE INTO task_checkpoints (task_id, current_index, stats, created_at)
                    VALUES (?, ?, ?, ?)
                ''', (task_id, current_index, checkpoint_data, now))

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
            dict with 'current_index' and 'stats', or None if not found
        """
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT * FROM task_checkpoints WHERE task_id = ?', (task_id,))
            row = cursor.fetchone()

            if row:
                stats_json = row['stats']
                return {
                    'task_id': row['task_id'],
                    'current_index': row['current_index'],
                    'stats': json.loads(stats_json) if stats_json else {},
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


def init_task_manager(db_path=None, checkpoint_dir=None):
    """
    Initialize the global task manager

    Args:
        db_path: Path to task database (optional)
        checkpoint_dir: Path to checkpoint directory (optional)
    """
    global task_manager
    if task_manager is None:
        task_manager = TaskManager(db_path=db_path, checkpoint_dir=checkpoint_dir)
    return task_manager
