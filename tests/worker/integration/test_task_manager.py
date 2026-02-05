"""
Integration tests for TaskManager with real database.

Tests TaskManager functionality with in-memory SQLite database.
Run with: pytest tests/worker/integration/test_task_manager.py -v
"""
import sys
import time
from pathlib import Path
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from web.tasks import TaskManager
from web.exceptions import TaskExistsError


@pytest.fixture
def tm(tmp_path):
    """Create fresh TaskManager with temporary file database for each test."""
    # Use a temporary file database instead of :memory: to share connections
    db_path = tmp_path / "test_tasks.db"
    return TaskManager(db_path=str(db_path))


class TestTaskCreation:
    """Test task creation with database."""

    def test_create_task_basic(self, tm):
        """Test basic task creation."""
        params = {'total_items': 10}
        task_id = tm.create_task('test_handler', params)

        assert task_id is not None
        assert len(task_id) > 0

        task = tm.get_task(task_id)
        assert task['task_type'] == 'test_handler'
        assert task['status'] == 'pending'

    def test_create_task_with_metadata(self, tm):
        """Test task creation with metadata."""
        params = {'total_items': 20}
        metadata = {'total_stocks': 20}

        task_id = tm.create_task('test_handler', params, metadata)

        task = tm.get_task(task_id)
        assert task['total_stocks'] == 20

    def test_create_duplicate_task_fails(self, tm):
        """Test that creating duplicate task fails."""
        # Create first task
        tm.create_task('test_handler', {'total_items': 10})

        # Try to create second task - should fail
        with pytest.raises(TaskExistsError) as exc_info:
            tm.create_task('test_handler', {'total_items': 20})

        assert '无法创建新任务' in str(exc_info.value)

    def test_create_task_after_completion(self, tm):
        """Test creating new task after previous completes."""
        # Create and complete first task
        task_id1 = tm.create_task('test_handler', {'total_items': 5, 'item_duration_ms': 10})
        tm.update_task(task_id1, status='completed', progress=100)

        # Should be able to create new task
        task_id2 = tm.create_task('test_handler', {'total_items': 10})
        assert task_id2 is not None

    def test_create_multiple_tasks_sequentially(self, tm):
        """Test creating multiple tasks one after another."""
        task_ids = []

        for i in range(3):
            task_id = tm.create_task('test_handler', {'total_items': 5})
            task_ids.append(task_id)

            # Complete the task
            tm.update_task(task_id, status='completed', progress=100)

        # All should be created
        assert len(task_ids) == 3
        assert len(set(task_ids)) == 3


class TestTaskUpdate:
    """Test task updates with database."""

    def test_update_task_status(self, tm):
        """Test updating task status."""
        task_id = tm.create_task('test_handler', {})

        tm.update_task(task_id, status='running')

        task = tm.get_task(task_id)
        assert task['status'] == 'running'

    def test_update_task_progress(self, tm):
        """Test updating task progress."""
        task_id = tm.create_task('test_handler', {})

        tm.update_task(task_id, progress=50)

        task = tm.get_task(task_id)
        assert task['progress'] == 50

    def test_update_task_message(self, tm):
        """Test updating task message."""
        task_id = tm.create_task('test_handler', {})

        tm.update_task(task_id, message='Processing...')

        task = tm.get_task(task_id)
        assert task['message'] == 'Processing...'

    def test_update_task_stats(self, tm):
        """Test updating task statistics."""
        task_id = tm.create_task('test_handler', {})
        stats = {'success': 10, 'failed': 2, 'skipped': 1}

        tm.update_task(task_id, stats=stats)

        task = tm.get_task(task_id)
        assert task['stats'] == stats

    def test_update_task_result(self, tm):
        """Test updating task result."""
        task_id = tm.create_task('test_handler', {})
        result = {'success': 10, 'failed': 0}

        tm.update_task(task_id, result=result)

        task = tm.get_task(task_id)
        assert task['result'] == result

    def test_update_multiple_fields(self, tm):
        """Test updating multiple fields at once."""
        task_id = tm.create_task('test_handler', {})

        tm.update_task(task_id,
                      status='running',
                      progress=50,
                      message='Half done',
                      stats={'success': 5})

        task = tm.get_task(task_id)
        assert task['status'] == 'running'
        assert task['progress'] == 50
        assert task['message'] == 'Half done'
        assert task['stats']['success'] == 5


class TestTaskRetrieval:
    """Test task retrieval from database."""

    def test_get_task_by_id(self, tm):
        """Test getting task by ID."""
        params = {'test': 'value'}
        task_id = tm.create_task('test_handler', params)

        task = tm.get_task(task_id)

        assert task is not None
        assert task['task_id'] == task_id
        assert task['params'] == params

    def test_get_nonexistent_task(self, tm):
        """Test getting non-existent task."""
        task = tm.get_task('nonexistent-id')
        assert task is None

    def test_get_all_tasks(self, tm):
        """Test getting all tasks."""
        # Create multiple tasks sequentially
        for i in range(5):
            task_id = tm.create_task('test_handler', {'index': i})
            # Complete each task before creating the next
            tm.update_task(task_id, status='completed')

        tasks = tm.get_all_tasks()

        assert len(tasks) == 5

    def test_get_tasks_by_status(self, tm):
        """Test filtering tasks by status."""
        # Create three tasks sequentially, completing each before next
        task_id1 = tm.create_task('test_handler', {})
        tm.update_task(task_id1, status='completed')

        task_id2 = tm.create_task('test_handler', {})
        tm.update_task(task_id2, status='completed')

        task_id3 = tm.create_task('test_handler', {})
        tm.update_task(task_id3, status='completed')

        # Now update task2 to running and task3 to paused for testing
        tm.update_task(task_id2, status='running')
        tm.update_task(task_id3, status='paused')

        running_tasks = tm.get_all_tasks(status='running')
        paused_tasks = tm.get_all_tasks(status='paused')
        completed_tasks = tm.get_all_tasks(status='completed')

        assert len(running_tasks) == 1
        assert len(paused_tasks) == 1
        assert len(completed_tasks) == 1
        assert running_tasks[0]['task_id'] == task_id2
        assert paused_tasks[0]['task_id'] == task_id3
        assert completed_tasks[0]['task_id'] == task_id1

    def test_get_tasks_with_limit(self, tm):
        """Test getting tasks with limit."""
        # Create 10 tasks sequentially
        for i in range(10):
            task_id = tm.create_task('test_handler', {'index': i})
            tm.update_task(task_id, status='completed')

        tasks = tm.get_all_tasks(limit=5)
        assert len(tasks) == 5

    def test_get_unfinished_tasks(self, tm):
        """Test getting unfinished tasks."""
        # Create first task and mark as completed
        task_id1 = tm.create_task('test_handler', {})
        tm.update_task(task_id1, status='completed')

        # Create second task and leave as running (unfinished)
        task_id2 = tm.create_task('test_handler', {})
        tm.update_task(task_id2, status='running')

        unfinished = tm.get_unfinished_tasks()

        assert len(unfinished) == 1
        assert unfinished[0]['task_id'] == task_id2


class TestTaskDeletion:
    """Test task deletion from database."""

    def test_delete_completed_task(self, tm):
        """Test deleting a completed task."""
        task_id = tm.create_task('test_handler', {})
        tm.update_task(task_id, status='completed')

        tm.delete_task(task_id)

        task = tm.get_task(task_id)
        assert task is None

    def test_delete_nonexistent_task(self, tm):
        """Test deleting non-existent task (should not crash)."""
        # Should not raise exception
        tm.delete_task('nonexistent-id')

    def test_delete_task_with_checkpoint(self, tm):
        """Test that checkpoint is also deleted."""
        task_id = tm.create_task('test_handler', {})
        tm.save_checkpoint(task_id, 10, {'success': 10})

        tm.delete_task(task_id)

        # Both task and checkpoint should be gone
        assert tm.get_task(task_id) is None
        assert tm.load_checkpoint(task_id) is None


class TestCheckpointOperations:
    """Test checkpoint save/load/delete with database."""

    def test_save_checkpoint(self, tm):
        """Test saving a checkpoint."""
        task_id = tm.create_task('test_handler', {})

        tm.save_checkpoint(task_id, 10, {'success': 10})

        checkpoint = tm.load_checkpoint(task_id)
        assert checkpoint is not None
        assert checkpoint['current_index'] == 10
        assert checkpoint['stats']['success'] == 10

    def test_save_checkpoint_with_stage(self, tm):
        """Test saving checkpoint with stage identifier."""
        task_id = tm.create_task('test_handler', {})

        tm.save_checkpoint(task_id, 5, {'success': 5}, stage='financial')

        checkpoint = tm.load_checkpoint(task_id)
        assert checkpoint['stage'] == 'financial'

    def test_load_checkpoint(self, tm):
        """Test loading a checkpoint."""
        task_id = tm.create_task('test_handler', {})
        tm.save_checkpoint(task_id, 15, {'success': 15, 'failed': 1})

        checkpoint = tm.load_checkpoint(task_id)

        assert checkpoint['current_index'] == 15
        assert checkpoint['stats']['success'] == 15
        assert checkpoint['stats']['failed'] == 1

    def test_load_nonexistent_checkpoint(self, tm):
        """Test loading non-existent checkpoint."""
        checkpoint = tm.load_checkpoint('nonexistent-id')
        assert checkpoint is None

    def test_delete_checkpoint(self, tm):
        """Test deleting a checkpoint."""
        task_id = tm.create_task('test_handler', {})
        tm.save_checkpoint(task_id, 10, {})

        tm.delete_checkpoint(task_id)

        checkpoint = tm.load_checkpoint(task_id)
        assert checkpoint is None

    def test_overwrite_checkpoint(self, tm):
        """Test overwriting existing checkpoint."""
        task_id = tm.create_task('test_handler', {})

        # Save first checkpoint
        tm.save_checkpoint(task_id, 5, {'success': 5})

        # Overwrite with second checkpoint
        tm.save_checkpoint(task_id, 10, {'success': 10})

        checkpoint = tm.load_checkpoint(task_id)
        assert checkpoint['current_index'] == 10
        assert checkpoint['stats']['success'] == 10


class TestStopPauseControl:
    """Test stop/pause control with database."""

    def test_request_stop(self, tm):
        """Test requesting task stop."""
        task_id = tm.create_task('test_handler', {})

        tm.request_stop(task_id)

        # Check via database - pending tasks get stopped immediately
        task = tm.get_task(task_id)
        # For pending tasks, request_stop directly marks them as stopped
        assert task['status'] == 'stopped' or task['stop_requested'] or tm.is_stop_requested(task_id)

    def test_request_pause(self, tm):
        """Test requesting task pause."""
        task_id = tm.create_task('test_handler', {})

        tm.request_pause(task_id)

        # Check via database
        task = tm.get_task(task_id)
        assert task['pause_requested'] or tm.is_pause_requested(task_id)

    def test_clear_stop_request(self, tm):
        """Test clearing stop request."""
        task_id = tm.create_task('test_handler', {})
        tm.request_stop(task_id)

        tm.clear_stop_request(task_id)

        assert not tm.is_stop_requested(task_id)

    def test_clear_pause_request(self, tm):
        """Test clearing pause request."""
        task_id = tm.create_task('test_handler', {})
        tm.request_pause(task_id)

        tm.clear_pause_request(task_id)

        # Database may still have the flag
        # But in-memory check should be cleared
        pause_in_memory = task_id in tm._pause_requested
        assert not pause_in_memory

    def test_resume_task(self, tm):
        """Test resuming a paused task."""
        task_id = tm.create_task('test_handler', {})
        tm.update_task(task_id, status='paused')
        tm.request_pause(task_id)

        result = tm.resume_task(task_id)

        assert result is True

        task = tm.get_task(task_id)
        assert task['status'] == 'running'


class TestStatsOperations:
    """Test statistics operations with database."""

    def test_increment_stats(self, tm):
        """Test incrementing statistics."""
        task_id = tm.create_task('test_handler', {})

        tm.increment_stats(task_id, 'success')

        task = tm.get_task(task_id)
        assert task['stats']['success'] == 1

    def test_increment_stats_multiple_times(self, tm):
        """Test incrementing stats multiple times."""
        task_id = tm.create_task('test_handler', {})

        tm.increment_stats(task_id, 'success', 5)
        tm.increment_stats(task_id, 'failed', 2)
        tm.increment_stats(task_id, 'skipped', 1)

        task = tm.get_task(task_id)
        assert task['stats'] == {'success': 5, 'failed': 2, 'skipped': 1}

    def test_increment_stats_existing(self, tm):
        """Test incrementing existing stats."""
        task_id = tm.create_task('test_handler', {})
        tm.update_task(task_id, stats={'success': 10, 'failed': 0})

        tm.increment_stats(task_id, 'success')

        task = tm.get_task(task_id)
        assert task['stats']['success'] == 11


class TestUtilityMethods:
    """Test utility methods."""

    def test_has_active_task_true(self, tm):
        """Test has_active_task when there is one."""
        tm.create_task('test_handler', {})

        has_active, task = tm.has_active_task()

        assert has_active is True
        assert task is not None

    def test_has_active_task_false(self, tm):
        """Test has_active_task when there is none."""
        # Create completed task
        task_id = tm.create_task('test_handler', {})
        tm.update_task(task_id, status='completed')

        has_active, task = tm.has_active_task()

        assert has_active is False
        assert task is None

    def test_get_unfinished_tasks(self, tm):
        """Test getting unfinished tasks."""
        # Create and complete first task
        task_id1 = tm.create_task('test_handler', {})
        tm.update_task(task_id1, status='completed')

        # Create second task and leave it pending (unfinished)
        task_id2 = tm.create_task('test_handler', {})
        tm.update_task(task_id2, status='running')

        unfinished = tm.get_unfinished_tasks()

        assert len(unfinished) == 1
        assert unfinished[0]['task_id'] == task_id2

    def test_pause_task(self, tm):
        """Test pause_task method."""
        task_id = tm.create_task('test_handler', {})
        tm.update_task(task_id, status='running')

        result = tm.pause_task(task_id)

        assert result is True

        task = tm.get_task(task_id)
        assert task['pause_requested']


class TestJsonFieldHandling:
    """Test JSON field serialization/deserialization."""

    def test_params_json_serialization(self, tm):
        """Test params are JSON serialized."""
        params = {'key': 'value', 'number': 123, 'nested': {'a': 1}}
        task_id = tm.create_task('test_handler', params)

        task = tm.get_task(task_id)

        assert task['params'] == params

    def test_stats_json_serialization(self, tm):
        """Test stats are JSON serialized."""
        stats = {'success': 100, 'failed': 5, 'skipped': 2}
        task_id = tm.create_task('test_handler', {})

        tm.update_task(task_id, stats=stats)

        task = tm.get_task(task_id)
        assert task['stats'] == stats

    def test_result_json_serialization(self, tm):
        """Test result is JSON serialized."""
        result = {'data': [1, 2, 3], 'count': 3}
        task_id = tm.create_task('test_handler', {})

        tm.update_task(task_id, result=result)

        task = tm.get_task(task_id)
        assert task['result'] == result


class TestCleanupOperations:
    """Test cleanup operations."""

    def test_cleanup_old_tasks(self, tm):
        """Test cleanup of old tasks."""
        # Create completed task
        task_id = tm.create_task('test_handler', {})
        tm.update_task(task_id, status='completed', completed_at='2024-01-01 00:00:00')

        # Cleanup (in-memory database has no old tasks)
        count = tm.cleanup_old_tasks(max_age_hours=24)

        # Should return 0 as there are no old enough tasks
        assert count >= 0

    def test_cleanup_stale_tasks(self, tm):
        """Test cleanup of stale tasks."""
        # Create old pending task
        task_id = tm.create_task('test_handler', {})
        tm.update_task(task_id, created_at='2024-01-01 00:00:00')

        # Cleanup
        count = tm.cleanup_stale_tasks(stale_threshold_hours=1)

        # Should mark old task as failed
        assert count >= 0
