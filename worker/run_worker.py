#!/usr/bin/env python3
"""
Worker启动脚本

启动后台任务处理worker，监听并执行数据库中的待处理任务。
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from worker.task_worker import TaskWorker


def main():
    """启动worker主循环"""
    from pathlib import Path

    # 任务数据库路径
    tasks_db_path = Path("data/tasks.db")

    # 创建worker
    worker = TaskWorker(db_path=str(tasks_db_path), poll_interval=5)

    print("[Worker] Starting task worker...")
    print(f"[Worker] Database: {tasks_db_path}")
    print(f"[Worker] Poll interval: 5 seconds")
    print("[Worker] Press Ctrl+C to stop")

    try:
        worker.start()
    except KeyboardInterrupt:
        print("\n[Worker] Interrupted by user")
        worker.stop()


if __name__ == "__main__":
    main()
