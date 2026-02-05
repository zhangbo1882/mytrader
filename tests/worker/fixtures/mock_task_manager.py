"""
Mock TaskManager for unit testing.

This module provides a mock implementation of TaskManager that doesn't
use database or file I/O, making it ideal for fast unit tests.
"""
import uuid


class MockTaskManager:
    """
    Completely in-memory TaskManager Mock, no database operations.

    This mock implements the same interface as TaskManager but stores
    all data in memory dictionaries, making it fast and isolated for testing.
    """

    def __init__(self):
        self.tasks = {}
        self.checkpoints = {}
        self.stop_requests = set()
        self.pause_requests = set()
        self._memory_lock = None  # Not needed for single-threaded tests

    # Core interface
    def create_task(self, task_type, params, metadata=None):
        """Create a new task in memory."""
        task_id = str(uuid.uuid4())
        self.tasks[task_id] = {
            'task_id': task_id,
            'task_type': task_type,
            'status': 'pending',
            'progress': 0,
            'stats': {'success': 0, 'failed': 0, 'skipped': 0},
            'params': params,
            'total_stocks': params.get('total_items', 100),
            'current_stock_index': 0,
            'message': 'Task created',
            'stop_requested': False,
            'pause_requested': False,
            'error': None,
            'result': None
        }
        return task_id

    def update_task(self, task_id, **kwargs):
        """Update task fields in memory."""
        if task_id in self.tasks:
            self.tasks[task_id].update(kwargs)

    def get_task(self, task_id):
        """Get task from memory."""
        return self.tasks.get(task_id)

    def get_all_tasks(self, status=None, limit=None):
        """Get all tasks from memory."""
        tasks = list(self.tasks.values())

        if status:
            tasks = [t for t in tasks if t.get('status') == status]

        if limit:
            tasks = tasks[:limit]

        return tasks

    def delete_task(self, task_id):
        """Delete task from memory."""
        self.tasks.pop(task_id, None)
        self.checkpoints.pop(task_id, None)
        self.stop_requests.discard(task_id)
        self.pause_requests.discard(task_id)

    # Control interface
    def is_stop_requested(self, task_id):
        """Check if stop has been requested."""
        return task_id in self.stop_requests

    def is_pause_requested(self, task_id):
        """Check if pause has been requested."""
        return task_id in self.pause_requests

    def request_stop(self, task_id):
        """Request task to stop."""
        self.stop_requests.add(task_id)
        # Also update the task if it exists and is pending
        task = self.tasks.get(task_id)
        if task and task['status'] == 'pending':
            self.update_task(task_id, status='stopped', message='Task cancelled')
            return True
        return True

    def request_pause(self, task_id):
        """Request task to pause."""
        self.pause_requests.add(task_id)
        return True

    def clear_stop_request(self, task_id):
        """Clear stop request."""
        self.stop_requests.discard(task_id)

    def clear_pause_request(self, task_id):
        """Clear pause request."""
        self.pause_requests.discard(task_id)

    def resume_task(self, task_id):
        """Resume a paused task."""
        self.pause_requests.discard(task_id)
        task = self.tasks.get(task_id)
        if task and task['status'] == 'paused':
            task['status'] = 'running'
            return True
        return False

    # Checkpoint interface
    def save_checkpoint(self, task_id, current_index, stats=None, stage='stock'):
        """Save checkpoint to memory."""
        self.checkpoints[task_id] = {
            'current_index': current_index,
            'stats': stats or {},
            'stage': stage
        }

    def load_checkpoint(self, task_id):
        """Load checkpoint from memory."""
        return self.checkpoints.get(task_id)

    def delete_checkpoint(self, task_id):
        """Delete checkpoint from memory."""
        self.checkpoints.pop(task_id, None)

    # Stats interface
    def increment_stats(self, task_id, stat_type, count=1):
        """Increment task statistics."""
        task = self.tasks.get(task_id)
        if task:
            task['stats'][stat_type] = task['stats'].get(stat_type, 0) + count

    # Additional methods for compatibility with TaskManager
    def has_active_task(self):
        """Check if there's an active task."""
        for task in self.tasks.values():
            if task['status'] in ('pending', 'running', 'paused'):
                return True, task
        return False, None

    def get_unfinished_tasks(self):
        """Get all unfinished tasks."""
        return [
            t for t in self.tasks.values()
            if t['status'] in ('running', 'paused')
        ]

    def cleanup_old_tasks(self, max_age_hours=24):
        """Cleanup old tasks (no-op for mock)."""
        return 0

    def cleanup_stale_tasks(self, stale_threshold_hours=24):
        """Cleanup stale tasks (no-op for mock)."""
        return 0

    def pause_task(self, task_id):
        """Pause a running task."""
        task = self.tasks.get(task_id)
        if task and task['status'] == 'running':
            self.update_task(task_id, pause_requested=True)
            return True
        return False
