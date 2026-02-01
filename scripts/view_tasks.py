#!/usr/bin/env python3
"""
Quick script to view tasks in the database

Usage:
    python scripts/view_tasks.py              # Show all tasks
    python scripts/view_tasks.py --running    # Show running tasks
    python scripts/view_tasks.py --pending    # Show pending tasks
    python scripts/view_tasks.py --id <id>    # Show specific task
    python scripts/view_tasks.py --stats      # Show statistics
"""
import sys
import sqlite3
import argparse
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import TASKS_DB_PATH


def print_header(text):
    """Print section header"""
    print(f"\n{'='*80}")
    print(f"  {text}")
    print(f"{'='*80}")


def print_task(task):
    """Print a single task"""
    print(f"\n{'─'*80}")
    print(f"Task ID:     {task['task_id']}")
    print(f"Type:        {task['task_type']}")
    print(f"Status:      {task['status']}")
    print(f"Progress:    {task['progress']}%")
    print(f"Message:     {task['message'] or 'N/A'}")

    if task['current_stock_index'] > 0 or task['total_stocks'] > 0:
        print(f"Stock Index: {task['current_stock_index']} / {task['total_stocks']}")

    if task['stats']:
        import json
        stats = json.loads(task['stats'])
        print(f"Stats:       Success={stats.get('success',0)}, Failed={stats.get('failed',0)}, Skipped={stats.get('skipped',0)}")

    print(f"Created:     {task['created_at']}")
    if task['completed_at']:
        print(f"Completed:   {task['completed_at']}")

    if task['error']:
        print(f"Error:       {task['error']}")

    if task['params']:
        import json
        params = json.loads(task['params'])
        print(f"Params:      {params}")


def view_all_tasks(status=None, limit=None):
    """View all tasks"""
    conn = sqlite3.connect(TASKS_DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = "SELECT * FROM tasks"
    params = []

    if status:
        query += " WHERE status = ?"
        params.append(status)

    query += " ORDER BY created_at DESC"

    if limit:
        query += f" LIMIT {limit}"

    cursor.execute(query, params)
    tasks = cursor.fetchall()
    conn.close()

    if not tasks:
        print("\nNo tasks found")
        return

    status_filter = f" (status: {status})" if status else ""
    print_header(f"Tasks{status_filter} - {len(tasks)} total")

    for task in tasks:
        print_task(dict(task))


def view_task_by_id(task_id):
    """View specific task"""
    conn = sqlite3.connect(TASKS_DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
    task = cursor.fetchone()
    conn.close()

    if not task:
        print(f"\nTask not found: {task_id}")
        return

    print_header("Task Details")
    print_task(dict(task))


def view_stats():
    """View task statistics"""
    conn = sqlite3.connect(TASKS_DB_PATH)
    cursor = conn.cursor()

    # Count by status
    cursor.execute("""
        SELECT status, COUNT(*) as count
        FROM tasks
        GROUP BY status
        ORDER BY count DESC
    """)
    status_counts = cursor.fetchall()

    # Total tasks
    cursor.execute("SELECT COUNT(*) FROM tasks")
    total = cursor.fetchone()[0]

    # Recent tasks
    cursor.execute("""
        SELECT DATE(created_at) as date, COUNT(*) as count
        FROM tasks
        GROUP BY DATE(created_at)
        ORDER BY date DESC
        LIMIT 7
    """)
    recent = cursor.fetchall()

    conn.close()

    print_header("Task Statistics")

    print(f"\nTotal Tasks: {total}")
    print(f"\nBy Status:")
    for status, count in status_counts:
        print(f"  {status:15} {count:5}")

    print(f"\nRecent Activity (last 7 days):")
    for date, count in recent:
        print(f"  {date}: {count} tasks")


def main():
    parser = argparse.ArgumentParser(
        description='View tasks in database',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--running', action='store_true', help='Show running tasks')
    parser.add_argument('--pending', action='store_true', help='Show pending tasks')
    parser.add_argument('--completed', action='store_true', help='Show completed tasks')
    parser.add_argument('--failed', action='store_true', help='Show failed tasks')
    parser.add_argument('--paused', action='store_true', help='Show paused tasks')
    parser.add_argument('--id', help='Show specific task by ID')
    parser.add_argument('--limit', type=int, help='Limit number of results')
    parser.add_argument('--stats', action='store_true', help='Show statistics')

    args = parser.parse_args()

    # Check if database exists
    if not Path(TASKS_DB_PATH).exists():
        print(f"Database not found: {TASKS_DB_PATH}")
        return

    if args.stats:
        view_stats()
    elif args.id:
        view_task_by_id(args.id)
    else:
        status = None
        if args.running:
            status = 'running'
        elif args.pending:
            status = 'pending'
        elif args.completed:
            status = 'completed'
        elif args.failed:
            status = 'failed'
        elif args.paused:
            status = 'paused'

        view_all_tasks(status=status, limit=args.limit)


if __name__ == '__main__':
    main()
