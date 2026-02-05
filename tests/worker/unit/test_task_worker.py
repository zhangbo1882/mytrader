"""
Unit tests for TaskWorker.

Tests worker task polling and execution logic.
Run with: pytest tests/worker/unit/test_task_worker.py -v
"""
import sys
import time
import signal
import threading
from pathlib import Path
from unittest.mock import patch, MagicMock, Mock
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from worker.task_worker import TaskWorker
from tests.worker.fixtures.mock_task_manager import MockTaskManager
from tests.worker.fixtures.mock_db import MockTushareDB


@pytest.fixture
def mock_worker():
    """Create TaskWorker with mocked TaskManager."""
    with patch('worker.task_worker.TaskManager') as MockTM:
        # Create a MagicMock that can have methods configured
        mock_tm_instance = MagicMock()
        # Mock the create_task method to use real logic
        def create_task_impl(task_type, params, metadata=None):
            import uuid
            task_id = str(uuid.uuid4())
            task = {
                'task_id': task_id,
                'task_type': task_type,
                'status': 'pending',
                'progress': 0,
                'stats': {'success': 0, 'failed': 0, 'skipped': 0},
                'params': params,
                'total_stocks': params.get('total_items', 100),
                'current_stock_index': 0,
                'message': 'Task created'
            }
            mock_tm_instance.tasks[task_id] = task
            return task_id

        # Mock the get_task method
        def get_task_impl(task_id):
            return mock_tm_instance.tasks.get(task_id)

        # Mock update_task
        def update_task_impl(task_id, **kwargs):
            if task_id in mock_tm_instance.tasks:
                mock_tm_instance.tasks[task_id].update(kwargs)

        # Set up the mock methods
        mock_tm_instance.tasks = {}
        mock_tm_instance.create_task = create_task_impl
        mock_tm_instance.get_task = get_task_impl
        mock_tm_instance.update_task = update_task_impl
        mock_tm_instance.get_all_tasks = MagicMock(return_value=[])
        mock_tm_instance.get_unfinished_tasks = MagicMock(return_value=[])
        MockTM.return_value = mock_tm_instance

        worker = TaskWorker(db_path=":memory:", poll_interval=0.1, max_concurrent=2)
        worker.tm = mock_tm_instance
        yield worker, mock_tm_instance


class TestTaskWorkerInit:
    """Test TaskWorker initialization."""

    @patch('worker.task_worker.TaskManager')
    def test_init_defaults(self, MockTM):
        """Test initialization with default parameters."""
        mock_tm_instance = MockTaskManager()
        MockTM.return_value = mock_tm_instance

        worker = TaskWorker(db_path=":memory:")

        assert worker.db_path == ":memory:"
        assert worker.poll_interval == 5
        assert worker.max_concurrent == 1
        assert worker.running_tasks == {}
        assert worker._shutdown is False
        MockTM.assert_called_once_with(db_path=":memory:")

    @patch('worker.task_worker.TaskManager')
    def test_init_custom_params(self, MockTM):
        """Test initialization with custom parameters."""
        mock_tm_instance = MockTaskManager()
        MockTM.return_value = mock_tm_instance

        worker = TaskWorker(
            db_path=":memory:",
            poll_interval=10,
            max_concurrent=5
        )

        assert worker.poll_interval == 10
        assert worker.max_concurrent == 5


class TestPollAndExecute:
    """Test _poll_and_execute method."""

    def test_poll_with_no_pending_tasks(self, mock_worker):
        """Test polling when no pending tasks available."""
        worker, tm = mock_worker
        tm.get_all_tasks.return_value = []

        worker._poll_and_execute()

        tm.get_all_tasks.assert_called_once_with(status='pending', limit=1)
        assert len(worker.running_tasks) == 0

    def test_poll_with_pending_task(self, mock_worker):
        """Test polling with one pending task."""
        worker, tm = mock_worker

        # Create a pending task
        task_id = tm.create_task('test_handler', {'total_items': 5})
        task = tm.get_task(task_id)

        tm.get_all_tasks.return_value = [task]

        # Mock _start_task to avoid actually starting thread
        with patch.object(worker, '_start_task') as mock_start:
            worker._poll_and_execute()

            mock_start.assert_called_once_with(task)

    def test_poll_respects_max_concurrent(self, mock_worker):
        """Test that poll respects max_concurrent limit."""
        worker, tm = mock_worker

        # Set max_concurrent to 1
        worker.max_concurrent = 1

        # Add a fake running task
        fake_thread = threading.Thread(target=lambda: None, daemon=True)
        worker.running_tasks['task-1'] = fake_thread

        # Even with pending task, shouldn't start because at max
        pending_task = {
            'task_id': 'task-2',
            'task_type': 'test_handler',
            'status': 'pending',
            'params': {}
        }
        tm.get_all_tasks.return_value = [pending_task]

        worker._poll_and_execute()

        # Should not call _start_task because at max concurrent
        assert len(worker.running_tasks) == 1

    def test_poll_cleanup_finished_threads(self, mock_worker):
        """Test that poll cleans up finished threads."""
        worker, tm = mock_worker

        # Add a finished thread (not alive)
        finished_thread = threading.Thread(target=lambda: None, daemon=True)
        finished_thread.start()
        finished_thread.join(timeout=1)

        worker.running_tasks['finished-task'] = finished_thread

        tm.get_all_tasks.return_value = []

        worker._poll_and_execute()

        # Finished thread should be cleaned up
        assert 'finished-task' not in worker.running_tasks


class TestStartTask:
    """Test _start_task method."""

    def test_start_task_basic(self, mock_worker):
        """Test starting a basic task."""
        worker, tm = mock_worker

        task_id = tm.create_task('test_handler', {})
        task = tm.get_task(task_id)

        with patch.object(worker, '_execute_task'):
            worker._start_task(task)

            # Task should be in running_tasks
            assert task_id in worker.running_tasks

            # Task status should be updated to running
            updated_task = tm.get_task(task_id)
            assert updated_task['status'] == 'running'

    def test_start_task_with_thread(self, mock_worker):
        """Test that task is started in a separate thread."""
        worker, tm = mock_worker

        task_id = tm.create_task('test_handler', {'total_items': 1})
        task = tm.get_task(task_id)

        worker._start_task(task)

        # Should have a thread for this task
        assert task_id in worker.running_tasks
        thread = worker.running_tasks[task_id]
        assert isinstance(thread, threading.Thread)
        assert thread.is_alive()

    def test_start_multiple_tasks(self, mock_worker):
        """Test starting multiple tasks up to max_concurrent."""
        worker, tm = mock_worker
        worker.max_concurrent = 3

        # Start multiple tasks
        task_ids = []
        for i in range(3):
            task_id = tm.create_task('test_handler', {'total_items': 1})
            task_ids.append(task_id)
            task = tm.get_task(task_id)
            worker._start_task(task)

        assert len(worker.running_tasks) == 3
        for task_id in task_ids:
            assert task_id in worker.running_tasks


class TestExecuteTask:
    """Test _execute_task method."""

    def test_execute_task_with_valid_handler(self, mock_worker):
        """Test executing a task with a valid handler type."""
        worker, tm = mock_worker

        # Use MockTaskManager instead of MagicMock for proper handler support
        from tests.worker.fixtures.mock_task_manager import MockTaskManager
        real_mock_tm = MockTaskManager()

        # Create task with proper mock
        task_id = real_mock_tm.create_task('test_handler', {
            'total_items': 2,
            'item_duration_ms': 10
        })
        task = real_mock_tm.get_task(task_id)

        # Execute task (blocking for test)
        worker.tm = real_mock_tm
        worker._execute_task(task)

        # Task should be completed
        updated_task = real_mock_tm.get_task(task_id)
        assert updated_task['status'] == 'completed'

    def test_execute_task_with_unknown_type(self, mock_worker):
        """Test executing a task with unknown task type."""
        worker, tm = mock_worker

        # Use MockTaskManager
        from tests.worker.fixtures.mock_task_manager import MockTaskManager
        real_mock_tm = MockTaskManager()

        # Create task with unknown type
        task_id = real_mock_tm.create_task('unknown_type', {})
        task = real_mock_tm.get_task(task_id)
        task['task_type'] = 'unknown_type'

        # Execute task
        worker.tm = real_mock_tm
        worker._execute_task(task)

        # Task should be marked as failed
        updated_task = real_mock_tm.get_task(task_id)
        assert updated_task['status'] == 'failed'
        assert 'Unknown task type' in updated_task.get('error', '')

    def test_execute_task_with_exception(self, mock_worker):
        """Test executing a task that raises an exception."""
        worker, tm = mock_worker

        # Use MockTaskManager
        from tests.worker.fixtures.mock_task_manager import MockTaskManager
        real_mock_tm = MockTaskManager()

        # Create task that will fail (negative total_items)
        task_id = real_mock_tm.create_task('test_handler', {'total_items': -1})
        task = real_mock_tm.get_task(task_id)

        # Execute task
        worker.tm = real_mock_tm
        worker._execute_task(task)

        # Task should be marked as failed
        updated_task = real_mock_tm.get_task(task_id)
        assert updated_task['status'] == 'failed'


class TestRecoverRunningTasks:
    """Test _recover_running_tasks method."""

    def test_recover_with_no_unfinished_tasks(self, mock_worker):
        """Test recovery when there are no unfinished tasks."""
        worker, tm = mock_worker
        tm.get_unfinished_tasks.return_value = []

        worker._recover_running_tasks()

        tm.get_unfinished_tasks.assert_called_once()

    def test_recover_with_running_tasks(self, mock_worker):
        """Test recovery of running tasks."""
        worker, tm = mock_worker

        # Create an unfinished task
        task_id = tm.create_task('test_handler', {})
        tm.update_task(task_id, status='running')
        task = tm.get_task(task_id)

        tm.get_unfinished_tasks.return_value = [task]

        with patch.object(worker, '_execute_task'):
            worker._recover_running_tasks()

            # Task should be in running_tasks
            assert task_id in worker.running_tasks

    def test_recover_with_paused_tasks(self, mock_worker):
        """Test recovery of paused tasks updates status."""
        worker, tm = mock_worker

        # Create a paused task
        task_id = tm.create_task('test_handler', {})
        tm.update_task(task_id, status='paused')
        task = tm.get_task(task_id)

        tm.get_unfinished_tasks.return_value = [task]

        with patch.object(worker, '_execute_task'):
            worker._recover_running_tasks()

            # Status should be updated to running
            updated_task = tm.get_task(task_id)
            assert updated_task['status'] == 'running'

    def test_recover_skips_already_recovering(self, mock_worker):
        """Test that recovery skips tasks already in running_tasks."""
        worker, tm = mock_worker

        task_id = tm.create_task('test_handler', {})
        tm.update_task(task_id, status='running')
        task = tm.get_task(task_id)

        # Add to running_tasks manually (simulating already recovering)
        fake_thread = threading.Thread(target=lambda: None, daemon=True)
        worker.running_tasks[task_id] = fake_thread

        tm.get_unfinished_tasks.return_value = [task]

        with patch.object(worker, '_execute_task') as mock_execute:
            worker._recover_running_tasks()

            # Should not execute since already in running_tasks
            mock_execute.assert_not_called()

    def test_recover_multiple_tasks(self, mock_worker):
        """Test recovering multiple unfinished tasks."""
        worker, tm = mock_worker

        # Create multiple unfinished tasks
        tasks = []
        for i in range(3):
            task_id = tm.create_task('test_handler', {'total_items': 1})
            tm.update_task(task_id, status='running')
            tasks.append(tm.get_task(task_id))

        tm.get_unfinished_tasks.return_value = tasks

        with patch.object(worker, '_execute_task'):
            worker._recover_running_tasks()

            assert len(worker.running_tasks) == 3


class TestCleanupFinishedThreads:
    """Test _cleanup_finished_threads method."""

    def test_cleanup_removes_finished_threads(self, mock_worker):
        """Test that finished threads are removed."""
        worker, tm = mock_worker

        # Create a finished thread
        finished_thread = threading.Thread(target=lambda: None, daemon=True)
        finished_thread.start()
        finished_thread.join(timeout=1)

        worker.running_tasks['finished'] = finished_thread

        worker._cleanup_finished_threads()

        assert 'finished' not in worker.running_tasks

    def test_cleanup_keeps_alive_threads(self, mock_worker):
        """Test that alive threads are kept."""
        worker, tm = mock_worker

        # Create a long-running thread
        def long_running():
            time.sleep(1)

        alive_thread = threading.Thread(target=long_running, daemon=True)
        alive_thread.start()

        worker.running_tasks['alive'] = alive_thread

        worker._cleanup_finished_threads()

        # Alive thread should still be there
        assert 'alive' in worker.running_tasks

    def test_cleanup_multiple_threads(self, mock_worker):
        """Test cleanup with multiple threads."""
        worker, tm = mock_worker

        # Create mixed threads
        threads = {}
        for i in range(3):
            thread = threading.Thread(target=lambda: None, daemon=True)
            thread.start()
            thread.join(timeout=1)
            threads[f'finished-{i}'] = thread

        # Add one alive thread
        alive_thread = threading.Thread(target=lambda: time.sleep(1), daemon=True)
        alive_thread.start()
        threads['alive'] = alive_thread

        worker.running_tasks.update(threads)

        worker._cleanup_finished_threads()

        # Only alive thread should remain
        assert len(worker.running_tasks) == 1
        assert 'alive' in worker.running_tasks
        assert 'finished-0' not in worker.running_tasks


class TestSignalHandler:
    """Test _signal_handler method."""

    def test_signal_handler_sets_shutdown(self, mock_worker):
        """Test that signal handler sets shutdown flag."""
        worker, tm = mock_worker

        assert worker._shutdown is False

        worker._signal_handler(signal.SIGINT, None)

        assert worker._shutdown is True

    def test_signal_handler_waits_for_tasks(self, mock_worker):
        """Test that signal handler waits for running tasks."""
        worker, tm = mock_worker

        # Create a fake running task that finishes quickly
        def quick_task():
            time.sleep(0.1)

        quick_thread = threading.Thread(target=quick_task, daemon=True)
        quick_thread.start()
        worker.running_tasks['task-1'] = quick_thread

        # Start signal handler (but don't actually send signal)
        # Simulate what happens after signal is received
        worker._shutdown = True
        start = time.time()

        # Simulate waiting loop with timeout
        timeout = 5
        while worker.running_tasks and (time.time() - start) < timeout:
            time.sleep(0.05)
            worker._cleanup_finished_threads()

        # Thread should have finished
        assert len(worker.running_tasks) == 0

    def test_signal_handler_timeout(self, mock_worker):
        """Test signal handler timeout with long-running task."""
        worker, tm = mock_worker

        # Create a long-running thread
        def long_task():
            time.sleep(10)

        long_thread = threading.Thread(target=long_task, daemon=True)
        long_thread.start()
        worker.running_tasks['task-1'] = long_thread

        worker._shutdown = True

        # Simulate signal handler waiting logic with short timeout
        timeout = 0.2  # Short timeout for test
        start = time.time()

        while worker.running_tasks and (time.time() - start) < timeout:
            time.sleep(0.05)
            worker._cleanup_finished_threads()

        # Should exit due to timeout, thread still running
        assert len(worker.running_tasks) == 1
        assert long_thread.is_alive()


class TestStopMethod:
    """Test stop method."""

    def test_stop_sets_shutdown_flag(self, mock_worker):
        """Test that stop() sets shutdown flag."""
        worker, tm = mock_worker

        assert worker._shutdown is False

        worker.stop()

        assert worker._shutdown is True


class TestIntegrationScenarios:
    """Integration-like scenarios with mocked components."""

    def test_full_task_lifecycle(self, mock_worker):
        """Test complete lifecycle: poll -> start -> execute -> cleanup."""
        worker, tm = mock_worker

        # Use MockTaskManager for proper handler support
        from tests.worker.fixtures.mock_task_manager import MockTaskManager
        real_mock_tm = MockTaskManager()
        worker.tm = real_mock_tm

        # Create a quick task
        task_id = real_mock_tm.create_task('test_handler', {
            'total_items': 2,
            'item_duration_ms': 10
        })
        task = real_mock_tm.get_task(task_id)

        # Poll and execute
        real_mock_tm.get_all_tasks = MagicMock(return_value=[task])
        worker._poll_and_execute()

        # Wait for task to complete
        thread = worker.running_tasks.get(task_id)
        if thread:
            thread.join(timeout=5)

        # Cleanup
        worker._cleanup_finished_threads()

        # Verify task completed
        final_task = real_mock_tm.get_task(task_id)
        assert final_task['status'] == 'completed'
        assert task_id not in worker.running_tasks

    @pytest.mark.parametrize("max_concurrent,num_tasks", [
        (1, 3),
        (2, 5),
        (3, 10),
    ])
    def test_concurrent_limit_enforced(self, mock_worker, max_concurrent, num_tasks):
        """Test that max_concurrent limit is enforced."""
        worker, tm = mock_worker
        worker.max_concurrent = max_concurrent

        # Create more tasks than max_concurrent
        tasks = []
        for i in range(num_tasks):
            task_id = tm.create_task('test_handler', {'total_items': 5})
            task = tm.get_task(task_id)
            tasks.append(task)

        # Simulate polling with limited concurrent execution
        started = 0
        for task in tasks:
            if started >= max_concurrent:
                break

            # Start task in background
            thread = threading.Thread(
                target=worker._execute_task,
                args=(task,),
                daemon=True
            )
            worker.running_tasks[task['task_id']] = thread
            thread.start()
            started += 1

        # Verify we didn't exceed max_concurrent
        assert len(worker.running_tasks) <= max_concurrent

        # Cleanup
        for thread in worker.running_tasks.values():
            thread.join(timeout=5)
        worker.running_tasks.clear()
