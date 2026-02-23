"""
Task Worker Service

Polls database for pending tasks and executes them in separate threads.
"""
import sys
import signal
import time
import threading
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from web.tasks import init_task_manager


class TaskWorker:
    """
    Task execution worker that polls SQLite database for pending tasks.

    The worker continuously polls the SQLite database for tasks with status='pending',
    claims them by updating status to 'running', and dispatches them to
    appropriate handlers based on task_type.

    Uses SQLite backend (separate from DuckDB business data) to avoid lock conflicts.
    """

    def __init__(self, db_path=None, poll_interval=5, max_concurrent=1):
        """
        Initialize TaskWorker.

        Args:
            db_path: Path to SQLite tasks database (default: TASKS_DB_PATH from settings)
            poll_interval: Seconds between database polls (default: 5)
            max_concurrent: Maximum number of tasks to run simultaneously (default: 1)
        """
        self.db_path = db_path
        self.poll_interval = poll_interval
        self.max_concurrent = max_concurrent
        self.running_tasks = {}  # task_id -> thread
        # Initialize TaskManager (now uses SQLite backend by default)
        # Use init_db=False to avoid table creation conflicts with API service
        self.tm = init_task_manager(db_path=db_path, init_db=False)
        # Get the actual database path from TaskManager (for logging)
        if hasattr(self.tm, '_get_reader'):
            # DuckDB backend
            self.actual_db_path = self.tm._get_reader().db_path
        elif hasattr(self.tm, 'db_path'):
            # SQLite backend
            self.actual_db_path = self.tm.db_path
        else:
            self.actual_db_path = db_path or 'unknown'
        self._shutdown = False

    def start(self):
        """
        Start the worker main loop.

        This method blocks until shutdown is requested via SIGINT/SIGTERM.
        """
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        print(f"[Worker] Starting task worker...")
        print(f"[Worker] Database: {self.actual_db_path}")
        print(f"[Worker] Poll interval: {self.poll_interval}s")
        print(f"[Worker] Max concurrent tasks: {self.max_concurrent}")
        print(f"[Worker] Press Ctrl+C to stop")

        # Recover unfinished tasks from previous runs
        self._recover_running_tasks()

        # Main worker loop
        while not self._shutdown:
            try:
                self._poll_and_execute()
            except Exception as e:
                print(f"[Worker] Error in poll loop: {e}")
                import traceback
                traceback.print_exc()

            # Sleep before next poll
            time.sleep(self.poll_interval)

    def _poll_and_execute(self):
        """
        Poll database for pending tasks and execute them.

        This method:
        1. Cleans up finished threads
        2. Checks if we can start more tasks (max_concurrent)
        3. Atomically claims a pending task (using UPDATE ... WHERE status='pending')
        4. Starts task in a separate thread if successfully claimed

        The atomic claim ensures multiple workers will get different tasks.
        """
        # Clean up finished threads first
        self._cleanup_finished_threads()

        # Check if we can start more tasks
        if len(self.running_tasks) >= self.max_concurrent:
            print(f"[Worker] Max concurrent tasks reached ({self.max_concurrent}), skipping poll")
            return

        # Atomically claim a pending task
        # Multiple workers calling this will get different tasks (or None)
        task = self.tm.claim_task()

        if task is None:
            return

        print(f"[Worker] Claimed task {task['task_id'][:8]} (type: {task['task_type']})")
        # Start task execution
        self._start_task(task)

    def _start_task(self, task):
        """
        Start a task execution in a separate thread.

        Args:
            task: Task dictionary from database (already claimed by claim_task)
        """
        task_id = task['task_id']

        # Note: Task status is already 'running' from claim_task()
        # No need to update status again here

        # Create and start execution thread
        thread = threading.Thread(
            target=self._execute_task,
            args=(task,),
            daemon=True,
            name=f"Task-{task_id[:8]}"
        )
        self.running_tasks[task_id] = thread
        thread.start()

        print(f"[Worker] Started task {task_id[:8]} (type: {task['task_type']})")

    def _execute_task(self, task):
        """
        Execute a single task by dispatching to appropriate handler.

        Args:
            task: Task dictionary from database
        """
        from worker.handlers import TASK_HANDLERS

        task_id = task['task_id']
        task_type = task['task_type']

        try:
            # Get handler for this task type
            if task_type not in TASK_HANDLERS:
                raise ValueError(f"Unknown task type: {task_type}")

            handler = TASK_HANDLERS[task_type]

            # Execute task with handler
            handler(self.tm, task_id, task.get('params', {}))

            print(f"[Worker] Task {task_id[:8]} completed")

        except Exception as e:
            import traceback
            traceback.print_exc()
            self.tm.update_task(
                task_id,
                status='failed',
                error=str(e),
                message=f'任务执行失败: {str(e)}'
            )
            print(f"[Worker] Task {task_id[:8]} failed: {e}")

    def _recover_running_tasks(self):
        """
        Recover unfinished tasks from previous worker runs.

        Tasks that were in 'running' or 'paused' state when the worker
        stopped will be resumed from their checkpoints.
        """
        unfinished = self.tm.get_unfinished_tasks()

        if not unfinished:
            return

        print(f"[Worker] Recovering {len(unfinished)} unfinished tasks...")

        for task in unfinished:
            task_id = task['task_id']
            status = task['status']

            # Skip if already being recovered
            if task_id in self.running_tasks:
                continue

            # Update status based on previous state
            if status == 'paused':
                self.tm.update_task(task_id, status='running', message='任务已恢复执行')

            # Start recovery thread
            thread = threading.Thread(
                target=self._execute_task,
                args=(task,),
                daemon=True,
                name=f"Recovery-{task_id[:8]}"
            )
            self.running_tasks[task_id] = thread
            thread.start()

            print(f"[Worker] Recovered task {task_id[:8]} (status: {status})")

    def _cleanup_finished_threads(self):
        """
        Remove finished threads from the running_tasks dict.
        """
        finished = [
            task_id for task_id, thread in self.running_tasks.items()
            if not thread.is_alive()
        ]
        for task_id in finished:
            del self.running_tasks[task_id]

    def _signal_handler(self, signum, _frame):
        """
        Handle shutdown signals (SIGINT, SIGTERM).

        Args:
            signum: Signal number
            _frame: Current stack frame (unused)
        """
        print(f"\n[Worker] Received signal {signum}, shutting down...")
        self._shutdown = True

        # Wait for running tasks to complete (max 30 seconds)
        timeout = 30
        start = time.time()

        while self.running_tasks and (time.time() - start) < timeout:
            print(f"[Worker] Waiting for {len(self.running_tasks)} tasks to finish...")
            time.sleep(1)
            self._cleanup_finished_threads()

        if self.running_tasks:
            print(f"[Worker] Warning: {len(self.running_tasks)} tasks still running")

        print("[Worker] Shutdown complete")

    def stop(self):
        """Stop the worker gracefully."""
        self._shutdown = True
