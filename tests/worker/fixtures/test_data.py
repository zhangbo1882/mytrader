"""
Test data generators for worker tests.

This module provides helper functions to generate test data
for various testing scenarios.
"""
import random
from datetime import datetime, timedelta


class TestDataGenerator:
    """Test data generator for worker tests."""

    @staticmethod
    def generate_stock_list(count=10):
        """
        Generate a list of stock codes.

        Args:
            count: Number of stock codes to generate

        Returns:
            List of stock code strings
        """
        return [f"{i:06d}" for i in range(600000, 600000 + count)]

    @staticmethod
    def generate_index_list(count=5):
        """
        Generate a list of index codes.

        Args:
            count: Number of index codes to generate

        Returns:
            List of index code strings
        """
        indices = []
        for i in range(count):
            if i % 2 == 0:
                indices.append(f"{i+1:03d}.SH")
            else:
                indices.append(f"{i+1:03d}.SZ")
        return indices

    @staticmethod
    def generate_task_params(task_type='test_handler', **kwargs):
        """
        Generate task parameters for testing.

        Args:
            task_type: Type of task to generate params for
            **kwargs: Additional parameters to override defaults

        Returns:
            Dictionary of task parameters
        """
        defaults = {
            'test_handler': {
                'total_items': 10,
                'item_duration_ms': 10,
                'checkpoint_interval': 5,
                'failure_rate': 0.0,
                'simulate_pause': False
            },
            'update_stock_prices': {
                'stock_range': 'custom',
                'custom_stocks': ['600382', '600711', '000001'],
                'stocks': []
            },
            'update_industry_classification': {
                'src': 'SW2021',
                'force': False
            },
            'update_financial_reports': {
                'stock_range': 'custom',
                'custom_stocks': ['600382', '600711'],
                'include_indicators': True,
                'include_reports': True
            },
            'update_index_data': {
                'markets': ['SSE', 'SZSE'],
                'start_date': '20240101',
                'end_date': datetime.now().strftime('%Y%m%d')
            }
        }

        params = defaults.get(task_type, {}).copy()
        params.update(kwargs)
        return params

    @staticmethod
    def generate_stats(success=0, failed=0, skipped=0):
        """
        Generate task statistics.

        Args:
            success: Number of successful items
            failed: Number of failed items
            skipped: Number of skipped items

        Returns:
            Statistics dictionary
        """
        return {
            'success': success,
            'failed': failed,
            'skipped': skipped
        }

    @staticmethod
    def generate_checkpoint(current_index=0, stats=None, stage='stock'):
        """
        Generate task checkpoint data.

        Args:
            current_index: Current processing index
            stats: Optional stats dictionary
            stage: Processing stage identifier

        Returns:
            Checkpoint dictionary
        """
        if stats is None:
            stats = {'success': 0, 'failed': 0, 'skipped': 0}

        return {
            'current_index': current_index,
            'stats': stats,
            'stage': stage
        }

    @staticmethod
    def random_stock_code():
        """
        Generate a random stock code.

        Returns:
            Random stock code string
        """
        num = random.randint(600000, 605000)
        return f"{num:06d}"

    @staticmethod
    def random_index_code():
        """
        Generate a random index code.

        Returns:
            Random index code string
        """
        num = random.randint(1, 999)
        market = random.choice(['SH', 'SZ'])
        return f"{num:03d}.{market}"


class DateTimeHelper:
    """Helper for datetime operations in tests."""

    @staticmethod
    def now_str():
        """Get current datetime as string."""
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def date_str(days_ago=0):
        """
        Get date string for days ago.

        Args:
            days_ago: Number of days in the past

        Returns:
            Date string in YYYYMMDD format
        """
        date = datetime.now() - timedelta(days=days_ago)
        return date.strftime('%Y%m%d')

    @staticmethod
    def hours_ago_str(hours):
        """
        Get datetime string for hours ago.

        Args:
            hours: Number of hours in the past

        Returns:
            Datetime string
        """
        dt = datetime.now() - timedelta(hours=hours)
        return dt.strftime('%Y-%m-%d %H:%M:%S')


class TaskHelper:
    """Helper for creating and manipulating tasks in tests."""

    @staticmethod
    def create_completed_task(task_id='test-task-id'):
        """Create a mock completed task."""
        return {
            'task_id': task_id,
            'task_type': 'test_handler',
            'status': 'completed',
            'progress': 100,
            'stats': {'success': 10, 'failed': 0, 'skipped': 0},
            'message': 'Task completed successfully',
            'result': {'success': 10, 'failed': 0, 'skipped': 0}
        }

    @staticmethod
    def create_running_task(task_id='test-task-id', progress=50):
        """Create a mock running task."""
        return {
            'task_id': task_id,
            'task_type': 'test_handler',
            'status': 'running',
            'progress': progress,
            'stats': {'success': 5, 'failed': 0, 'skipped': 0},
            'message': f'Task in progress ({progress}%)'
        }

    @staticmethod
    def create_failed_task(task_id='test-task-id', error_msg='Test error'):
        """Create a mock failed task."""
        return {
            'task_id': task_id,
            'task_type': 'test_handler',
            'status': 'failed',
            'progress': 50,
            'stats': {'success': 5, 'failed': 1, 'skipped': 0},
            'message': 'Task failed',
            'error': error_msg
        }

    @staticmethod
    def create_paused_task(task_id='test-task-id', progress=50):
        """Create a mock paused task."""
        return {
            'task_id': task_id,
            'task_type': 'test_handler',
            'status': 'paused',
            'progress': progress,
            'stats': {'success': 5, 'failed': 0, 'skipped': 0},
            'message': 'Task paused',
            'pause_requested': True
        }


class AssertionHelper:
    """Helper for common assertions in tests."""

    @staticmethod
    def assert_task_completed(task):
        """Assert task is completed."""
        assert task['status'] == 'completed', \
            f"Expected status 'completed', got '{task['status']}'"
        assert task['progress'] == 100, \
            f"Expected progress 100, got {task['progress']}"

    @staticmethod
    def assert_task_failed(task):
        """Assert task is failed."""
        assert task['status'] == 'failed', \
            f"Expected status 'failed', got '{task['status']}'"

    @staticmethod
    def assert_task_stopped(task):
        """Assert task is stopped."""
        assert task['status'] == 'stopped', \
            f"Expected status 'stopped', got '{task['status']}'"

    @staticmethod
    def assert_task_paused(task):
        """Assert task is paused."""
        assert task['status'] == 'paused', \
            f"Expected status 'paused', got '{task['status']}'"

    @staticmethod
    def assert_stats_equal(task, expected_success, expected_failed=0, expected_skipped=0):
        """Assert task statistics match expected values."""
        stats = task.get('stats', {})
        assert stats.get('success', 0) == expected_success, \
            f"Expected {expected_success} success, got {stats.get('success', 0)}"
        assert stats.get('failed', 0) == expected_failed, \
            f"Expected {expected_failed} failed, got {stats.get('failed', 0)}"
        assert stats.get('skipped', 0) == expected_skipped, \
            f"Expected {expected_skipped} skipped, got {stats.get('skipped', 0)}"
