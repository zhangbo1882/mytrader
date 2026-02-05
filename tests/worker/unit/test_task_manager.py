"""
Unit tests for MockTaskManager.

Tests the mock implementation to ensure it correctly mimics TaskManager behavior.
Run with: pytest tests/worker/unit/test_task_manager.py -v
"""
import sys
from pathlib import Path
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from tests.worker.fixtures.mock_task_manager import MockTaskManager
from tests.worker.fixtures.test_data import TestDataGenerator, AssertionHelper


@pytest.fixture
def tm():
    """Create fresh MockTaskManager for each test."""
    return MockTaskManager()


class TestTaskCreation:
    """Test task creation functionality."""

    def test_create_basic_task(self, tm):
        """Test basic task creation."""
        params = {'total_items': 10}
        task_id = tm.create_task('test_handler', params)

        assert task_id is not None
        task = tm.get_task(task_id)
        assert task is not None
        assert task['task_type'] == 'test_handler'
        assert task['status'] == 'pending'
        assert task['params'] == params

    def test_create_task_with_metadata(self, tm):
        """Test task creation with metadata."""
        params = {'total_items': 20}
        metadata = {'total_stocks': 20}

        task_id = tm.create_task('test_handler', params, metadata)

        task = tm.get_task(task_id)
        assert task['total_stocks'] == 20

    def test_multiple_tasks(self, tm):
        """Test creating multiple tasks."""
        task_id1 = tm.create_task('test_handler', {'total_items': 10})
        task_id2 = tm.create_task('test_handler', {'total_items': 20})

        assert task_id1 != task_id2
        assert tm.get_task(task_id1) is not None
        assert tm.get_task(task_id2) is not None

    def test_task_unique_ids(self, tm):
        """Test that task IDs are unique."""
        task_ids = [tm.create_task('test_handler', {}) for _ in range(100)]

        assert len(set(task_ids)) == 100
        assert all(tm.get_task(tid) is not None for tid in task_ids)


class TestTaskUpdate:
    """Test task update functionality."""

    def test_update_status(self, tm):
        """Test updating task status."""
        task_id = tm.create_task('test_handler', {})

        tm.update_task(task_id, status='running')

        task = tm.get_task(task_id)
        assert task['status'] == 'running'

    def test_update_multiple_fields(self, tm):
        """Test updating multiple fields."""
        task_id = tm.create_task('test_handler', {})

        tm.update_task(task_id,
                      status='running',
                      progress=50,
                      message='Processing...')

        task = tm.get_task(task_id)
        assert task['status'] == 'running'
        assert task['progress'] == 50
        assert task['message'] == 'Processing...'

    def test_update_stats(self, tm):
        """Test updating task stats."""
        task_id = tm.create_task('test_handler', {})
        new_stats = {'success': 10, 'failed': 2, 'skipped': 1}

        tm.update_task(task_id, stats=new_stats)

        task = tm.get_task(task_id)
        assert task['stats'] == new_stats

    def test_update_nonexistent_task(self, tm):
        """Test updating non-existent task (should not crash)."""
        # Should not raise exception
        tm.update_task('nonexistent-id', status='running')

        task = tm.get_task('nonexistent-id')
        assert task is None


class TestTaskRetrieval:
    """Test task retrieval functionality."""

    def test_get_existing_task(self, tm):
        """Test getting an existing task."""
        task_id = tm.create_task('test_handler', {'total_items': 10})

        task = tm.get_task(task_id)

        assert task is not None
        assert task['task_id'] == task_id

    def test_get_nonexistent_task(self, tm):
        """Test getting a non-existent task."""
        task = tm.get_task('nonexistent-id')
        assert task is None

    def test_get_all_tasks(self, tm):
        """Test getting all tasks."""
        tm.create_task('test_handler', {})
        tm.create_task('test_handler', {})
        tm.create_task('test_handler', {})

        tasks = tm.get_all_tasks()

        assert len(tasks) == 3

    def test_get_tasks_by_status(self, tm):
        """Test filtering tasks by status."""
        task_id1 = tm.create_task('test_handler', {})
        task_id2 = tm.create_task('test_handler', {})

        tm.update_task(task_id1, status='running')
        tm.update_task(task_id2, status='completed')

        running_tasks = tm.get_all_tasks(status='running')
        completed_tasks = tm.get_all_tasks(status='completed')

        assert len(running_tasks) == 1
        assert len(completed_tasks) == 1
        assert running_tasks[0]['task_id'] == task_id1
        assert completed_tasks[0]['task_id'] == task_id2

    def test_get_tasks_with_limit(self, tm):
        """Test getting tasks with limit."""
        for _ in range(10):
            tm.create_task('test_handler', {})

        tasks = tm.get_all_tasks(limit=5)

        assert len(tasks) == 5


class TestTaskDeletion:
    """Test task deletion functionality."""

    def test_delete_existing_task(self, tm):
        """Test deleting an existing task."""
        task_id = tm.create_task('test_handler', {})

        tm.delete_task(task_id)

        task = tm.get_task(task_id)
        assert task is None

    def test_delete_nonexistent_task(self, tm):
        """Test deleting non-existent task (should not crash)."""
        # Should not raise exception
        tm.delete_task('nonexistent-id')

    def test_delete_with_checkpoint(self, tm):
        """Test that checkpoint is also deleted."""
        task_id = tm.create_task('test_handler', {})
        tm.save_checkpoint(task_id, 5, {'success': 5})

        tm.delete_task(task_id)

        checkpoint = tm.load_checkpoint(task_id)
        assert checkpoint is None

    def test_delete_clears_control_flags(self, tm):
        """Test that delete clears stop/pause requests."""
        task_id = tm.create_task('test_handler', {})
        tm.request_stop(task_id)
        tm.request_pause(task_id)

        tm.delete_task(task_id)

        assert not tm.is_stop_requested(task_id)
        assert not tm.is_pause_requested(task_id)


class TestStopControl:
    """Test stop control functionality."""

    def test_request_stop(self, tm):
        """Test requesting task stop."""
        task_id = tm.create_task('test_handler', {})

        tm.request_stop(task_id)

        assert tm.is_stop_requested(task_id)

    def test_stop_pending_task(self, tm):
        """Test stopping a pending task."""
        task_id = tm.create_task('test_handler', {})

        result = tm.request_stop(task_id)

        assert result is True
        task = tm.get_task(task_id)
        assert task['status'] == 'stopped'

    def test_clear_stop_request(self, tm):
        """Test clearing stop request."""
        task_id = tm.create_task('test_handler', {})
        tm.request_stop(task_id)

        tm.clear_stop_request(task_id)

        assert not tm.is_stop_requested(task_id)

    def test_multiple_stop_requests(self, tm):
        """Test multiple stop requests (idempotent)."""
        task_id = tm.create_task('test_handler', {})

        tm.request_stop(task_id)
        tm.request_stop(task_id)
        tm.request_stop(task_id)

        assert tm.is_stop_requested(task_id)


class TestPauseControl:
    """Test pause control functionality."""

    def test_request_pause(self, tm):
        """Test requesting task pause."""
        task_id = tm.create_task('test_handler', {})

        tm.request_pause(task_id)

        assert tm.is_pause_requested(task_id)

    def test_clear_pause_request(self, tm):
        """Test clearing pause request."""
        task_id = tm.create_task('test_handler', {})
        tm.request_pause(task_id)

        tm.clear_pause_request(task_id)

        assert not tm.is_pause_requested(task_id)

    def test_resume_paused_task(self, tm):
        """Test resuming a paused task."""
        task_id = tm.create_task('test_handler', {})
        tm.update_task(task_id, status='paused')
        tm.request_pause(task_id)

        result = tm.resume_task(task_id)

        assert result is True
        assert not tm.is_pause_requested(task_id)

    def test_resume_non_paused_task(self, tm):
        """Test resuming a task that's not paused."""
        task_id = tm.create_task('test_handler', {})

        result = tm.resume_task(task_id)

        assert result is False


class TestCheckpointManagement:
    """Test checkpoint save/load/delete functionality."""

    def test_save_checkpoint(self, tm):
        """Test saving a checkpoint."""
        task_id = tm.create_task('test_handler', {})

        tm.save_checkpoint(task_id, 10, {'success': 10})

        checkpoint = tm.load_checkpoint(task_id)
        assert checkpoint is not None
        assert checkpoint['current_index'] == 10
        assert checkpoint['stats'] == {'success': 10}

    def test_load_checkpoint(self, tm):
        """Test loading a checkpoint."""
        task_id = tm.create_task('test_handler', {})
        tm.save_checkpoint(task_id, 5, {'success': 5, 'failed': 1})

        checkpoint = tm.load_checkpoint(task_id)

        assert checkpoint['current_index'] == 5
        assert checkpoint['stats']['success'] == 5
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

    def test_checkpoint_with_stage(self, tm):
        """Test checkpoint with different stages."""
        task_id = tm.create_task('test_handler', {})

        tm.save_checkpoint(task_id, 10, {'success': 10}, stage='financial')

        checkpoint = tm.load_checkpoint(task_id)
        assert checkpoint['stage'] == 'financial'

    def test_update_checkpoint(self, tm):
        """Test updating existing checkpoint."""
        task_id = tm.create_task('test_handler', {})

        tm.save_checkpoint(task_id, 5, {'success': 5})
        tm.save_checkpoint(task_id, 10, {'success': 10})

        checkpoint = tm.load_checkpoint(task_id)
        assert checkpoint['current_index'] == 10


class TestStatisticsManagement:
    """Test statistics tracking functionality."""

    def test_increment_stats_default(self, tm):
        """Test incrementing stats with default count."""
        task_id = tm.create_task('test_handler', {})

        tm.increment_stats(task_id, 'success')

        task = tm.get_task(task_id)
        assert task['stats']['success'] == 1

    def test_increment_stats_custom_count(self, tm):
        """Test incrementing stats with custom count."""
        task_id = tm.create_task('test_handler', {})

        tm.increment_stats(task_id, 'success', 5)

        task = tm.get_task(task_id)
        assert task['stats']['success'] == 5

    def test_increment_multiple_stats(self, tm):
        """Test incrementing multiple stat types."""
        task_id = tm.create_task('test_handler', {})

        tm.increment_stats(task_id, 'success', 10)
        tm.increment_stats(task_id, 'failed', 2)
        tm.increment_stats(task_id, 'skipped', 1)

        task = tm.get_task(task_id)
        assert task['stats'] == {'success': 10, 'failed': 2, 'skipped': 1}

    def test_increment_nonexistent_task(self, tm):
        """Test incrementing stats for non-existent task."""
        # Should not crash
        tm.increment_stats('nonexistent-id', 'success')


class TestUtilityMethods:
    """Test utility/helper methods."""

    def test_has_active_task_true(self, tm):
        """Test has_active_task when there is one."""
        task_id = tm.create_task('test_handler', {})

        has_active, task = tm.has_active_task()

        assert has_active is True
        assert task is not None

    def test_has_active_task_false(self, tm):
        """Test has_active_task when there is none."""
        has_active, task = tm.has_active_task()

        assert has_active is False
        assert task is None

    def test_get_unfinished_tasks(self, tm):
        """Test getting unfinished tasks."""
        task_id1 = tm.create_task('test_handler', {})
        task_id2 = tm.create_task('test_handler', {})

        tm.update_task(task_id1, status='running')
        tm.update_task(task_id2, status='completed')

        unfinished = tm.get_unfinished_tasks()

        assert len(unfinished) == 1
        assert unfinished[0]['task_id'] == task_id1

    def test_cleanup_old_tasks(self, tm):
        """Test cleanup method (no-op for mock)."""
        count = tm.cleanup_old_tasks()
        assert count == 0

    def test_cleanup_stale_tasks(self, tm):
        """Test stale task cleanup (no-op for mock)."""
        count = tm.cleanup_stale_tasks()
        assert count == 0


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_concurrent_task_access(self, tm):
        """Test that mock handles concurrent access reasonably."""
        task_ids = []
        for _ in range(50):
            task_ids.append(tm.create_task('test_handler', {}))

        # All tasks should be retrievable
        for task_id in task_ids:
            task = tm.get_task(task_id)
            assert task is not None

    def test_empty_params(self, tm):
        """Test task with empty params."""
        task_id = tm.create_task('test_handler', {})

        task = tm.get_task(task_id)
        assert task['params'] == {}

    def test_special_characters_in_params(self, tm):
        """Test params with special characters."""
        params = {'message': 'Test with "quotes" and \'apostrophes\''}
        task_id = tm.create_task('test_handler', params)

        task = tm.get_task(task_id)
        assert task['params'] == params

    def test_large_stats(self, tm):
        """Test handling large stat counts."""
        task_id = tm.create_task('test_handler', {})

        tm.increment_stats(task_id, 'success', 999999)

        task = tm.get_task(task_id)
        assert task['stats']['success'] == 999999
