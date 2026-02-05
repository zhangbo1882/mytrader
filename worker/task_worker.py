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

from web.tasks import TaskManager


class TaskWorker:
    """
    Task execution worker that polls database for pending tasks.

    The worker continuously polls the database for tasks with status='pending',
    claims them by updating status to 'running', and dispatches them to
    appropriate handlers based on task_type.
    """

    def __init__(self, db_path, poll_interval=5, max_concurrent=1):
        """
        Initialize TaskWorker.

        Args:
            db_path: Path to tasks database
            poll_interval: Seconds between database polls (default: 5)
            max_concurrent: Maximum number of tasks to run simultaneously (default: 1)
        """
        self.db_path = db_path
        self.poll_interval = poll_interval
        self.max_concurrent = max_concurrent
        self.running_tasks = {}  # task_id -> thread
        self.tm = TaskManager(db_path=db_path)
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
        print(f"[Worker] Database: {self.db_path}")
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
        3. Queries database for pending tasks
        4. Starts tasks in separate threads
        """
        # Clean up finished threads first
        self._cleanup_finished_threads()

        # Check if we can start more tasks
        if len(self.running_tasks) >= self.max_concurrent:
            return

        # Get pending tasks from database
        pending_tasks = self.tm.get_all_tasks(status='pending', limit=1)

        if not pending_tasks:
            return

        # Start tasks up to max_concurrent limit
        for task in pending_tasks:
            # Check concurrent limit again
            if len(self.running_tasks) >= self.max_concurrent:
                break

            # Start task execution
            self._start_task(task)

    def _start_task(self, task):
        """
        Start a task execution in a separate thread.

        Args:
            task: Task dictionary from database
        """
        task_id = task['task_id']

        # Update task status to 'running'
        self.tm.update_task(task_id, status='running', message='Worker正在执行任务...')

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
