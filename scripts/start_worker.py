#!/usr/bin/env python
"""
Task Worker Startup Script

Usage:
    python scripts/start_worker.py [--poll-interval SECONDS] [--max-concurrent N] [--log-file PATH]

Examples:
    # Start with default settings (5s poll, 1 concurrent task)
    python scripts/start_worker.py

    # Start with 2 second poll interval
    python scripts/start_worker.py --poll-interval 2

    # Start with 3 concurrent tasks
    python scripts/start_worker.py --max-concurrent 3

    # Start with custom settings and log file
    python scripts/start_worker.py --poll-interval 2 --max-concurrent 2 --log-file logs/worker.log
"""
import sys
import os
import argparse
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from worker.task_worker import TaskWorker
from config.settings import TASKS_DB_PATH


def main():
    parser = argparse.ArgumentParser(
        description='Start MyTrader task worker service',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--poll-interval',
        type=int,
        default=5,
        metavar='SECONDS',
        help='Database poll interval in seconds (default: 5)'
    )
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=1,
        metavar='N',
        help='Maximum concurrent tasks to run (default: 1)'
    )
    parser.add_argument(
        '--log-file',
        type=str,
        default=None,
        metavar='PATH',
        help='Log file path (default: stdout only)'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )

    args = parser.parse_args()

    # Configure logging
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    log_handlers = [logging.StreamHandler()]

    if args.log_file:
        # Create log directory if needed
        log_path = Path(args.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_handlers.append(logging.FileHandler(args.log_file))

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format=log_format,
        handlers=log_handlers
    )

    logger = logging.getLogger(__name__)

    # Create worker
    worker = TaskWorker(
        db_path=str(TASKS_DB_PATH),
        poll_interval=args.poll_interval,
        max_concurrent=args.max_concurrent
    )

    logger.info("[Worker] Starting task worker...")
    logger.info(f"[Worker] Database: {TASKS_DB_PATH}")
    logger.info(f"[Worker] Poll interval: {args.poll_interval}s")
    logger.info(f"[Worker] Max concurrent tasks: {args.max_concurrent}")
    logger.info("[Worker] Press Ctrl+C to stop")

    try:
        worker.start()
    except KeyboardInterrupt:
        logger.info("[Worker] Shutting down gracefully...")
        worker.stop()
        logger.info("[Worker] Worker stopped")
    except Exception as e:
        logger.error(f"[Worker] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
