"""
APScheduler based task scheduler for automated stock updates
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import logging

# Setup logging
logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.WARNING)

# Global scheduler instance
scheduler = None


def init_scheduler(db_path, timezone='Asia/Shanghai'):
    """
    Initialize and start the scheduler

    Args:
        db_path: Path to SQLite database for job persistence
        timezone: Timezone for scheduled jobs

    Returns:
        scheduler instance
    """
    global scheduler

    if scheduler is not None and scheduler.running:
        return scheduler

    # Configure jobstores (persistent storage)
    jobstores = {
        'default': SQLAlchemyJobStore(url=f'sqlite:///{db_path}')
    }

    # Configure executors
    executors = {
        'default': ThreadPoolExecutor(20)
    }

    # Configure job defaults
    job_defaults = {
        'coalesce': True,          # Combine missed runs into one
        'max_instances': 1,         # Only one instance of a job at a time
        'misfire_grace_time': 300,  # Grace period for missed runs (5 minutes)
        'replace_existing': True    # Replace existing job when adding
    }

    scheduler = BackgroundScheduler(
        jobstores=jobstores,
        executors=executors,
        job_defaults=job_defaults,
        timezone=timezone
    )

    scheduler.start()
    print(f"[Scheduler] Started with timezone: {timezone}")
    return scheduler


def get_scheduler():
    """Get the global scheduler instance"""
    return scheduler


def run_scheduled_task(task_type, params):
    """
    定时任务执行函数（模块级函数，可被APScheduler序列化）

    Args:
        task_type: 任务类型 (update_stock_prices/update_financial_reports/update_industry_classification/update_index_data)
        params: 任务参数字典
    """
    try:
        from web.services.task_creation_service import create_task

        logging.info(f"[ScheduledJob] Executing task: {task_type} with params: {params}")

        # 创建任务
        result, status_code = create_task({
            'task_type': task_type,
            'params': params
        })

        if status_code != 201:
            logging.error(f"[ScheduledJob] Task creation failed: {result}")
            return False

        logging.info(f"[ScheduledJob] Task created successfully: {result.get('task_id')}")
        return True

    except Exception as e:
        logging.error(f"[ScheduledJob] Error executing task {task_type}: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False


def add_scheduled_job(job_id, func, cron_expression, func_args=None, func_kwargs=None, name=None):
    """
    Add a scheduled cron job

    Args:
        job_id: Unique identifier for the job
        func: Function to execute (can be function object or string reference like 'module:function')
        cron_expression: Cron expression (e.g., "0 18 * * 1-5")
        func_args: Positional arguments for the function
        func_kwargs: Keyword arguments for the function
        name: Human-readable name for the job

    Returns:
        job_id if job was added successfully, False otherwise
    """
    global scheduler

    if scheduler is None:
        logging.error("[Scheduler] Error: Scheduler not initialized")
        return False

    try:
        # Parse cron expression
        # Format: minute hour day month day_of_week
        parts = cron_expression.strip().split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {cron_expression}")

        minute, hour, day, month, day_of_week = parts

        # Handle string reference to function
        if isinstance(func, str):
            # String reference format: 'web.scheduler:run_scheduled_task'
            # APScheduler can import and use this
            func_ref = func
        else:
            func_ref = func

        # Create the job
        scheduler.add_job(
            func_ref,
            trigger=CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                timezone=scheduler.timezone
            ),
            id=job_id,
            name=name or job_id,
            args=func_args or [],
            kwargs=func_kwargs or {},
            replace_existing=True
        )

        logging.info(f"[Scheduler] Added job: {job_id} with cron: {cron_expression}")
        return job_id

    except Exception as e:
        logging.error(f"[Scheduler] Error adding job {job_id}: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False


def remove_scheduled_job(job_id):
    """
    Remove a scheduled job

    Args:
        job_id: Job identifier

    Returns:
        True if job was removed successfully
    """
    global scheduler

    if scheduler is None:
        return False

    try:
        scheduler.remove_job(job_id)
        print(f"[Scheduler] Removed job: {job_id}")
        return True
    except Exception as e:
        print(f"[Scheduler] Error removing job {job_id}: {e}")
        return False


def pause_scheduled_job(job_id):
    """
    Pause a scheduled job

    Args:
        job_id: Job identifier

    Returns:
        True if job was paused successfully
    """
    global scheduler

    if scheduler is None:
        return False

    try:
        scheduler.pause_job(job_id)
        print(f"[Scheduler] Paused job: {job_id}")
        return True
    except Exception as e:
        print(f"[Scheduler] Error pausing job {job_id}: {e}")
        return False


def resume_scheduled_job(job_id):
    """
    Resume a paused scheduled job

    Args:
        job_id: Job identifier

    Returns:
        True if job was resumed successfully
    """
    global scheduler

    if scheduler is None:
        return False

    try:
        scheduler.resume_job(job_id)
        print(f"[Scheduler] Resumed job: {job_id}")
        return True
    except Exception as e:
        print(f"[Scheduler] Error resuming job {job_id}: {e}")
        return False


def get_scheduled_jobs():
    """
    Get all scheduled jobs

    Returns:
        List of job info dictionaries
    """
    global scheduler

    if scheduler is None:
        return []

    jobs = []
    for job in scheduler.get_jobs():
        # Extract task_type from job.id (format: "{task_type}_{name}")
        # Use last underscore as separator since task_type contains underscores
        job_id = job.id
        task_type = None
        if '_' in job_id:
            # Split by last underscore to separate task_type from name
            last_underscore_idx = job_id.rfind('_')
            task_type = job_id[:last_underscore_idx] if last_underscore_idx > 0 else None

        job_info = {
            'id': job.id,
            'name': job.name,
            'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
            'trigger': str(job.trigger),
            'enabled': not job.next_run_time == None,
            'task_type': task_type  # Extract task_type from job_id
        }
        jobs.append(job_info)

    return jobs


def get_job_info(job_id):
    """
    Get info about a specific job

    Args:
        job_id: Job identifier

    Returns:
        Job info dict or None
    """
    global scheduler

    if scheduler is None:
        return None

    try:
        job = scheduler.get_job(job_id)
        if job:
            return {
                'id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger),
                'enabled': True
            }
    except Exception as e:
        print(f"[Scheduler] Error getting job {job_id}: {e}")

    return None


def shutdown_scheduler(wait=False):
    """
    Shutdown the scheduler

    Args:
        wait: Wait for all jobs to complete
    """
    global scheduler

    if scheduler is not None and scheduler.running:
        scheduler.shutdown(wait=wait)
        print("[Scheduler] Shutdown complete")
        scheduler = None


def parse_cron_expression(cron_expr):
    """
    Parse cron expression into human-readable description

    Args:
        cron_expr: Cron expression (e.g., "0 18 * * 1-5")

    Returns:
        Human-readable description
    """
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        return cron_expr

    minute, hour, day, month, day_of_week = parts

    # Day of week mapping
    dow_map = {
        '0': '周日',
        '1': '周一',
        '2': '周二',
        '3': '周三',
        '4': '周四',
        '5': '周五',
        '6': '周六',
        '1-5': '周一至周五',
        '0-6': '每天',
        '*': '每天'
    }

    dow_desc = dow_map.get(day_of_week, day_of_week)

    if day == '*' and month == '*':
        return f"每{dow_desc} {hour}:{minute.zfill(2)}"
    else:
        return f"{cron_expr}"
