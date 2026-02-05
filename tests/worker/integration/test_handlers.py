"""
Integration tests for worker handlers.

Tests handler execution with real database (in-memory) to verify persistence.
Run with: pytest tests/worker/integration/test_handlers.py -v
"""
import sys
import time
import threading
from pathlib import Path
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from web.tasks import TaskManager
from worker.handlers import execute_test_handler
from tests.worker.fixtures.test_data import TestDataGenerator, AssertionHelper


@pytest.fixture
def real_tm(tmp_path):
    """Create real TaskManager with temporary file database."""
    # Use a temporary file database instead of :memory: to share connections
    db_path = tmp_path / "test_tasks.db"
    tm = TaskManager(db_path=str(db_path))
    yield tm
    # Cleanup is automatic with temporary directory


class TestHandlerDatabaseIntegration:
    """Test handler integration with real database."""

    def test_task_persisted_to_db(self, real_tm):
        """Test task is saved to database."""
        params = {
            'total_items': 5,
            'item_duration_ms': 10
        }
        task_id = real_tm.create_task('test_handler', params)

        # Verify can retrieve from database
        task = real_tm.get_task(task_id)
        assert task is not None
        assert task['task_type'] == 'test_handler'
        assert task['params'] == params

    def test_checkpoint_persisted_to_db(self, real_tm):
        """Test checkpoint is saved to database."""
        params = {
            'total_items': 100,
            'item_duration_ms': 50
        }
        task_id = real_tm.create_task('test_handler', params)

        # Run and stop mid-execution
        thread = threading.Thread(
            target=execute_test_handler,
            args=(real_tm, task_id, params),
            daemon=True
        )
        thread.start()
        time.sleep(0.2)

        real_tm.request_stop(task_id)
        thread.join(timeout=2)

        # Verify checkpoint in database
        checkpoint = real_tm.load_checkpoint(task_id)
        assert checkpoint is not None
        assert 'current_index' in checkpoint
        assert checkpoint['current_index'] > 0

    def test_progress_updates_persisted(self, real_tm):
        """Test progress updates are persisted."""
        params = {
            'total_items': 10,
            'item_duration_ms': 20
        }
        task_id = real_tm.create_task('test_handler', params)

        execute_test_handler(real_tm, task_id, params)

        task = real_tm.get_task(task_id)
        assert task['progress'] == 100
        assert task['status'] == 'completed'

    @pytest.mark.parametrize("total_items,expected_progress", [
        (5, 100),
        (10, 100),
        (20, 100),
    ])
    def test_various_task_sizes(self, real_tm, total_items, expected_progress):
        """Parameterized test: different task sizes."""
        params = {
            'total_items': total_items,
            'item_duration_ms': 10
        }
        task_id = real_tm.create_task('test_handler', params)

        execute_test_handler(real_tm, task_id, params)

        task = real_tm.get_task(task_id)
        assert task['progress'] == expected_progress
        assert task['status'] == 'completed'

    def test_stats_persisted_across_execution(self, real_tm):
        """Test statistics are persisted."""
        params = {
            'total_items': 10,
            'item_duration_ms': 10
        }
        task_id = real_tm.create_task('test_handler', params)

        execute_test_handler(real_tm, task_id, params)

        task = real_tm.get_task(task_id)
        assert task['stats']['success'] == 10
        assert task['stats']['failed'] == 0


class TestDatabasePersistenceAcrossRecovery:
    """Test database persistence for recovery scenarios."""

    def test_checkpoint_recovery_simulation(self, real_tm):
        """Test simulating checkpoint recovery."""
        params = {
            'total_items': 100,
            'item_duration_ms': 50
        }
        task_id = real_tm.create_task('test_handler', params)

        # Run and stop
        thread = threading.Thread(
            target=execute_test_handler,
            args=(real_tm, task_id, params),
            daemon=True
        )
        thread.start()
        time.sleep(0.3)

        real_tm.request_stop(task_id)
        thread.join(timeout=2)

        # Get checkpoint
        checkpoint = real_tm.load_checkpoint(task_id)
        assert checkpoint is not None

        # Verify task state
        task = real_tm.get_task(task_id)
        assert task['status'] == 'stopped'

    def test_multiple_checkpoints_overwrite(self, real_tm):
        """Test that multiple checkpoints overwrite each other."""
        # Save first checkpoint
        real_tm.save_checkpoint('test-1', 10, {'success': 10})
        checkpoint1 = real_tm.load_checkpoint('test-1')

        # Save second checkpoint with same ID
        real_tm.save_checkpoint('test-1', 20, {'success': 20})
        checkpoint2 = real_tm.load_checkpoint('test-1')

        # Second should overwrite first
        assert checkpoint1['current_index'] == 10
        assert checkpoint2['current_index'] == 20

    def test_checkpoint_deletion(self, real_tm):
        """Test checkpoint deletion."""
        task_id = real_tm.create_task('test_handler', {'total_items': 10})

        real_tm.save_checkpoint(task_id, 5, {'success': 5})
        assert real_tm.load_checkpoint(task_id) is not None

        real_tm.delete_checkpoint(task_id)
        assert real_tm.load_checkpoint(task_id) is None


class TestConcurrentDatabaseAccess:
    """Test concurrent access to database."""

    def test_multiple_tasks(self, real_tm):
        """Test creating and running multiple tasks sequentially."""
        results = []

        for i in range(3):
            params = {'total_items': 5, 'item_duration_ms': 10}
            task_id = real_tm.create_task('test_handler', params)

            execute_test_handler(real_tm, task_id, params)
            results.append(real_tm.get_task(task_id))

        # All should complete successfully
        assert all(r['status'] == 'completed' for r in results)

    def test_task_deletion_from_database(self, real_tm):
        """Test deleting task removes from database."""
        task_id = real_tm.create_task('test_handler', {'total_items': 5})

        # Verify task exists
        assert real_tm.get_task(task_id) is not None

        # Delete task
        real_tm.delete_task(task_id)

        # Verify task is gone
        assert real_tm.get_task(task_id) is None


class TestPauseResumeWithDatabase:
    """Test pause/resume with real database persistence."""

    def test_pause_state_persisted(self, real_tm):
        """Test pause state is persisted to database."""
        params = {
            'total_items': 20,
            'item_duration_ms': 50
        }
        task_id = real_tm.create_task('test_handler', params)

        thread = threading.Thread(
            target=execute_test_handler,
            args=(real_tm, task_id, params),
            daemon=True
        )
        thread.start()

        # Wait for progress
        time.sleep(0.2)

        # Pause
        real_tm.request_pause(task_id)
        time.sleep(0.2)

        # Verify pause state in database
        task = real_tm.get_task(task_id)
        assert task['status'] == 'paused' or task['pause_requested']

        # Resume and complete
        real_tm.resume_task(task_id)
        thread.join(timeout=5)

        task = real_tm.get_task(task_id)
        assert task['status'] == 'completed'


class TestStatsPersistence:
    """Test statistics persistence across database operations."""

    def test_increment_stats_persists(self, real_tm):
        """Test increment_stats persists to database."""
        task_id = real_tm.create_task('test_handler', {'total_items': 10})

        # Increment stats
        real_tm.increment_stats(task_id, 'success', 5)
        real_tm.increment_stats(task_id, 'failed', 2)

        # Verify in database
        task = real_tm.get_task(task_id)
        assert task['stats']['success'] == 5
        assert task['stats']['failed'] == 2

    def test_stats_update_with_update_task(self, real_tm):
        """Test stats are updated with update_task."""
        task_id = real_tm.create_task('test_handler', {})

        new_stats = {'success': 100, 'failed': 5, 'skipped': 3}
        real_tm.update_task(task_id, stats=new_stats)

        task = real_tm.get_task(task_id)
        assert task['stats'] == new_stats


class TestTaskLifecycleWithDatabase:
    """Test complete task lifecycle with database."""

    def test_full_lifecycle(self, real_tm):
        """Test pending -> running -> completed lifecycle."""
        params = {'total_items': 5, 'item_duration_ms': 10}
        task_id = real_tm.create_task('test_handler', params)

        # Initial state
        task = real_tm.get_task(task_id)
        assert task['status'] == 'pending'

        # Run to completion
        execute_test_handler(real_tm, task_id, params)

        # Final state
        task = real_tm.get_task(task_id)
        assert task['status'] == 'completed'
        assert task['progress'] == 100

    def test_stop_lifecycle(self, real_tm):
        """Test pending -> running -> stopped lifecycle."""
        params = {'total_items': 100, 'item_duration_ms': 50}
        task_id = real_tm.create_task('test_handler', params)

        thread = threading.Thread(
            target=execute_test_handler,
            args=(real_tm, task_id, params),
            daemon=True
        )
        thread.start()
        time.sleep(0.3)

        real_tm.request_stop(task_id)
        thread.join(timeout=2)

        task = real_tm.get_task(task_id)
        assert task['status'] == 'stopped'


class TestGetAllTasks:
    """Test getting all tasks from database."""

    def test_get_all_tasks(self, real_tm):
        """Test retrieving all tasks."""
        # Create multiple tasks sequentially
        for _ in range(5):
            task_id = real_tm.create_task('test_handler', {'total_items': 5})
            # Complete each task before creating next
            real_tm.update_task(task_id, status='completed')

        tasks = real_tm.get_all_tasks()
        assert len(tasks) == 5

    def test_get_all_tasks_with_status_filter(self, real_tm):
        """Test filtering tasks by status."""
        task_id1 = real_tm.create_task('test_handler', {'total_items': 5})
        # Complete first task before creating second
        execute_test_handler(real_tm, task_id1, {'total_items': 5, 'item_duration_ms': 10})

        task_id2 = real_tm.create_task('test_handler', {'total_items': 5})
        # Mark second as completed
        real_tm.update_task(task_id2, status='completed')

        # Get completed tasks
        completed_tasks = real_tm.get_all_tasks(status='completed')

        assert len(completed_tasks) == 2

    def test_get_all_tasks_with_limit(self, real_tm):
        """Test limiting results."""
        # Create 10 tasks sequentially
        for _ in range(10):
            task_id = real_tm.create_task('test_handler', {})
            real_tm.update_task(task_id, status='completed')

        tasks = real_tm.get_all_tasks(limit=5)
        assert len(tasks) == 5
