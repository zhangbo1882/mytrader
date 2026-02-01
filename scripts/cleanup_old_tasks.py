#!/usr/bin/env python3
"""
Cleanup script for old completed/failed tasks

This script removes old tasks from the tasks database that are older than
a specified number of days. It's safe to run this periodically via cron.

Usage:
    python scripts/cleanup_old_tasks.py [days]

Arguments:
    days: Number of days to keep tasks (default: 7)

Examples:
    # Delete tasks older than 7 days (default)
    python scripts/cleanup_old_tasks.py

    # Delete tasks older than 30 days
    python scripts/cleanup_old_tasks.py 30

Cron setup (runs weekly on Sunday at 2 AM):
    0 2 * * 0 /usr/bin/python3 /path/to/scripts/cleanup_old_tasks.py 7
"""
import sys
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import TASKS_DB_PATH


def cleanup_old_tasks(days=7):
    """
    Cleanup old completed/failed tasks from database

    Args:
        days: Number of days to keep tasks (default: 7)

    Returns:
        deleted_count: Number of tasks deleted
    """
    db_path = TASKS_DB_PATH

    # Check if database exists
    if not Path(db_path).exists():
        print(f"Database not found: {db_path}")
        return 0

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Calculate cutoff time
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()

    try:
        # First, get count of tasks to be deleted
        cursor.execute('''
            SELECT COUNT(*) FROM tasks
            WHERE status IN ('completed', 'failed', 'stopped', 'cancelled')
            AND completed_at < ?
        ''', (cutoff,))
        total_to_delete = cursor.fetchone()[0]

        if total_to_delete == 0:
            print(f"No old tasks found (older than {days} days)")
            return 0

        # Delete checkpoints first (foreign key)
        cursor.execute('''
            DELETE FROM task_checkpoints
            WHERE task_id IN (
                SELECT task_id FROM tasks
                WHERE status IN ('completed', 'failed', 'stopped', 'cancelled')
                AND completed_at < ?
            )
        ''', (cutoff,))

        checkpoints_deleted = cursor.rowcount

        # Delete tasks
        cursor.execute('''
            DELETE FROM tasks
            WHERE status IN ('completed', 'failed', 'stopped', 'cancelled')
            AND completed_at < ?
        ''', (cutoff,))

        tasks_deleted = cursor.rowcount

        conn.commit()

        print(f"Deleted {tasks_deleted} old tasks and {checkpoints_deleted} checkpoints (older than {days} days)")
        return tasks_deleted

    except Exception as e:
        conn.rollback()
        print(f"Error during cleanup: {e}")
        return 0
    finally:
        conn.close()


def vacuum_database():
    """
    Run VACUUM to reclaim space after deleting old records
    """
    db_path = TASKS_DB_PATH

    if not Path(db_path).exists():
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('VACUUM')
        conn.close()
        print("Database vacuumed successfully")
    except Exception as e:
        print(f"Error vacuuming database: {e}")


def show_task_stats():
    """Show current task statistics"""
    db_path = TASKS_DB_PATH

    if not Path(db_path).exists():
        print(f"Database not found: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Total tasks by status
        cursor.execute('''
            SELECT status, COUNT(*) as count
            FROM tasks
            GROUP BY status
            ORDER BY count DESC
        ''')

        print("\nCurrent task statistics:")
        print("-" * 40)
        total = 0
        for row in cursor.fetchall():
            status, count = row
            print(f"  {status}: {count}")
            total += count

        print(f"  Total: {total}")

        # Oldest tasks
        cursor.execute('''
            SELECT status, MIN(created_at) as oldest
            FROM tasks
            GROUP BY status
        ''')

        print("\nOldest tasks by status:")
        print("-" * 40)
        for row in cursor.fetchall():
            status, oldest = row
            if oldest:
                print(f"  {status}: {oldest}")

    finally:
        conn.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Cleanup old tasks from database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        'days',
        type=int,
        nargs='?',
        default=7,
        help='Number of days to keep tasks (default: 7)'
    )
    parser.add_argument(
        '--vacuum',
        action='store_true',
        help='Run VACUUM after cleanup to reclaim space'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show task statistics without cleanup'
    )

    args = parser.parse_args()

    if args.stats:
        show_task_stats()
    else:
        print(f"Cleaning up tasks older than {args.days} days...")
        deleted = cleanup_old_tasks(days=args.days)

        if args.vacuum and deleted > 0:
            vacuum_database()

        show_task_stats()
