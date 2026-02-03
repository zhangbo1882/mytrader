"""
Flask Web Application for Stock Query System
"""
from flask import Flask
from flask_cors import CORS
import sys
import os
import json
import threading
from pathlib import Path
from datetime import timedelta

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 创建 Flask 应用
app = Flask(__name__,
            template_folder='templates',
            static_folder='static')
app.config['JSON_AS_ASCII'] = False  # 支持中文
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.secret_key = 'your-secret-key-here'  # Required for session

# 启用 CORS
CORS(app)

# 注册路由 Blueprint
from web.routes import bp
app.register_blueprint(bp)

# 初始化任务管理器和调度器
from web.tasks import init_task_manager
from web.scheduler import init_scheduler
from config.settings import CHECKPOINT_DIR, SCHEDULE_DB_PATH, SCHEDULER_TIMEZONE, TASKS_DB_PATH

# Initialize on application startup
_initialized = False


def resume_unfinished_tasks(tm):
    """
    Resume unfinished tasks after web restart

    Args:
        tm: TaskManager instance
    """
    unfinished = tm.get_unfinished_tasks()

    if not unfinished:
        return

    print(f"[Task Recovery] Found {len(unfinished)} unfinished tasks, resuming...")

    for task in unfinished:
        task_id = task['task_id']
        task_type = task['task_type']
        params = task.get('params', {})

        # Log task recovery
        print(f"[Task Recovery] Resuming task {task_id} (type: {task_type}, status: {task['status']})")

        # Load checkpoint if exists
        checkpoint = tm.load_checkpoint(task_id)

        # Start recovery thread based on task type
        if task_type == 'update_all_stocks':
            thread = threading.Thread(
                target=run_update_all_stocks_recovery,
                args=(tm, task_id, params, checkpoint)
            )
            thread.daemon = True
            thread.start()
        elif task_type == 'update_favorites':
            thread = threading.Thread(
                target=run_update_favorites_recovery,
                args=(tm, task_id, params)
            )
            thread.daemon = True
            thread.start()
        else:
            # For other task types, just mark as failed
            tm.update_task(
                task_id,
                status='failed',
                message=f'任务类型 {task_type} 不支持恢复'
            )


def run_update_all_stocks_recovery(tm, task_id, params, checkpoint=None):
    """
    Resume execution of stock update task

    Args:
        tm: TaskManager instance
        task_id: Task identifier
        params: Task parameters
        checkpoint: Optional checkpoint data
    """
    try:
        from src.data_sources.tushare import TushareDB
        from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH

        db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

        mode = params.get('mode', 'incremental')
        stock_range = params.get('stock_range', 'all')
        custom_stocks = params.get('custom_stocks', [])

        # Get stock list
        stock_list = get_stock_list_for_recovery(stock_range, custom_stocks, db)

        if not stock_list:
            tm.update_task(
                task_id,
                status='failed',
                message='无法获取股票列表'
            )
            return

        # Resume from checkpoint
        start_index = 0
        stats = {'success': 0, 'failed': 0, 'skipped': 0}

        if checkpoint:
            start_index = checkpoint.get('current_index', 0)
            stats = checkpoint.get('stats', stats)
            print(f"[Task Recovery] Resuming from index {start_index}")

        # Update progress
        tm.update_task(
            task_id,
            status='running',
            current_stock_index=start_index,
            stats=stats,
            message=f'任务已恢复，正在更新股票数据...'
        )

        # Continue execution
        from datetime import datetime
        import time

        end_date = datetime.now().strftime('%Y%m%d')

        for i in range(start_index, len(stock_list)):
            stock_code = stock_list[i]

            # Check stop request (memory flag first - faster and lock-free)
            if tm.is_stop_requested(task_id):
                print(f"[TASK-{task_id[:8]}] Stop requested, stopping at index {i}")
                task = tm.get_task(task_id)
                task_stats = task.get('stats') if task else stats
                tm.save_checkpoint(task_id, i, task_stats)
                tm.update_task(task_id, status='stopped', message='任务已停止')
                tm.clear_stop_request(task_id)
                return

            # Check pause request (memory flag first - faster and lock-free)
            while tm.is_pause_requested(task_id):
                time.sleep(1)
                # Re-check stop request while paused
                if tm.is_stop_requested(task_id):
                    task = tm.get_task(task_id)
                    task_stats = task.get('stats') if task else stats
                    tm.save_checkpoint(task_id, i, task_stats)
                    tm.update_task(task_id, status='stopped', message='任务已停止')
                    tm.clear_stop_request(task_id)
                    tm.clear_pause_request(task_id)
                    return

            try:
                # Update progress
                progress = int((i / len(stock_list)) * 100)
                tm.update_task(task_id,
                    current_stock_index=i,
                    progress=progress,
                    message=f'正在更新 {stock_code} ({i+1}/{len(stock_list)})...'
                )

                # Save checkpoint every 10 stocks
                if i % 10 == 0:
                    # Get current stats from task
                    current_task = tm.get_task(task_id)
                    current_stats = current_task.get('stats') if current_task else stats
                    tm.save_checkpoint(task_id, i, current_stats)

                # Update stock data
                print(f"[TASK-{task_id[:8]}] Updating stock {stock_code} ({i+1}/{len(stock_list)})...")
                if mode == 'full':
                    stats_result = db.save_all_stocks_by_code(
                        default_start_date='20200101',
                        end_date=end_date,
                        stock_list=[stock_code]
                    )
                else:  # incremental
                    stats_result = db.save_all_stocks_by_code_incremental(
                        default_start_date='20240101',
                        end_date=end_date,
                        stock_list=[stock_code]
                    )
                print(f"[TASK-{task_id[:8]}] Stock {stock_code} update complete")

                # Update stats
                if stats_result:
                    if stats_result.get('success', 0) > 0:
                        tm.increment_stats(task_id, 'success')
                    if stats_result.get('failed', 0) > 0:
                        tm.increment_stats(task_id, 'failed')
                    if stats_result.get('skipped', 0) > 0:
                        tm.increment_stats(task_id, 'skipped')
                else:
                    tm.increment_stats(task_id, 'failed')

            except Exception as e:
                tm.increment_stats(task_id, 'failed')
                print(f"[ERROR] Failed to update {stock_code}: {e}")

        # Complete
        tm.delete_checkpoint(task_id)
        final_stats = tm.get_task(task_id).get('stats', {})
        tm.update_task(task_id,
            status='completed',
            message='更新完成',
            result=final_stats
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        tm.update_task(task_id,
            status='failed',
            error=str(e),
            message=f'更新失败: {str(e)}'
        )


def run_update_favorites_recovery(tm, task_id, params):
    """
    Resume execution of favorites update task

    Args:
        tm: TaskManager instance
        task_id: Task identifier
        params: Task parameters
    """
    try:
        from src.data_sources.tushare import TushareDB
        from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH
        from datetime import datetime
        import time

        db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))
        end_date = datetime.now().strftime('%Y%m%d')

        stock_list = params.get('stocks', [])
        if not stock_list:
            tm.update_task(
                task_id,
                status='failed',
                message='股票列表为空'
            )
            return

        tm.update_task(
            task_id,
            status='running',
            message=f'任务已恢复，正在更新收藏股票...'
        )

        # Track results
        detailed_results = {
            'success': [],
            'failed': [],
            'skipped': []
        }

        for i, stock_code in enumerate(stock_list):
            # Check stop request (memory flag - faster and lock-free)
            if tm.is_stop_requested(task_id):
                tm.update_task(task_id, status='stopped', message='任务已停止')
                tm.clear_stop_request(task_id)
                return

            # Check pause request (memory flag - faster and lock-free)
            while tm.is_pause_requested(task_id):
                time.sleep(1)
                # Re-check stop request while paused
                if tm.is_stop_requested(task_id):
                    tm.update_task(task_id, status='stopped', message='任务已停止')
                    tm.clear_stop_request(task_id)
                    tm.clear_pause_request(task_id)
                    return

            try:
                tm.update_task(task_id,
                    message=f'正在更新 {stock_code} ({i+1}/{len(stock_list)})...'
                )

                stats_result = db.save_all_stocks_by_code_incremental(
                    default_start_date='20240101',
                    end_date=end_date,
                    stock_list=[stock_code]
                )

                if stats_result and stats_result.get('success', 0) > 0:
                    detailed_results['success'].append(stock_code)
                elif stats_result and stats_result.get('skipped', 0) > 0:
                    detailed_results['skipped'].append(stock_code)
                else:
                    detailed_results['failed'].append(stock_code)

            except Exception as e:
                detailed_results['failed'].append(stock_code)
                print(f"[ERROR] Failed to update {stock_code}: {e}")

        # Complete
        summary_stats = {
            'total': len(stock_list),
            'success': len(detailed_results['success']),
            'failed': len(detailed_results['failed']),
            'skipped': len(detailed_results['skipped']),
            'details': detailed_results
        }

        tm.update_task(task_id,
            status='completed',
            message='更新完成',
            result=summary_stats
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        tm.update_task(task_id,
            status='failed',
            error=str(e),
            message=f'更新失败: {str(e)}'
        )


def get_stock_list_for_recovery(stock_range, custom_stocks, db):
    """
    Get stock list for task recovery

    Args:
        stock_range: Stock range type ('all', 'favorites', 'custom')
        custom_stocks: Custom stock list
        db: TushareDB instance

    Returns:
        List of stock codes
    """
    if stock_range == 'custom':
        return custom_stocks
    elif stock_range == 'favorites':
        return ["600382", "600711", "000001"]  # Default favorites
    else:  # all
        try:
            from sqlalchemy import text
            with db.engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT DISTINCT symbol FROM bars WHERE symbol LIKE '____%'"
                ))
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            print(f"[Task Recovery] Error getting stock list: {e}")
            return []


@app.before_request
def initialize():
    """Initialize task manager and scheduler before first request"""
    global _initialized
    if not _initialized:
        print("[Init] Initializing task manager...")
        # Initialize task manager (this updates the global in tasks.py)
        tm = init_task_manager(
            db_path=str(TASKS_DB_PATH),
            checkpoint_dir=CHECKPOINT_DIR
        )
        print(f"[Init] Task manager initialized: {tm}")

        # 清理陈旧任务
        print("[Init] 清理陈旧任务...")
        tm.cleanup_stale_tasks(stale_threshold_hours=24)

        # Initialize scheduled job configs storage
        print("[Init] Initializing scheduled job configs...")
        from web.scheduled_jobs import _init_configs_db, _load_configs_from_db
        _init_configs_db(str(SCHEDULE_DB_PATH))  # Use same DB as scheduler
        _load_configs_from_db()  # Load configs into memory

        # Initialize scheduler
        print("[Init] Initializing scheduler...")
        init_scheduler(str(SCHEDULE_DB_PATH), timezone=SCHEDULER_TIMEZONE)

        # Resume unfinished tasks
        resume_unfinished_tasks(tm)

        _initialized = True
        print("[Init] Initialization complete")


if __name__ == '__main__':
    import os
    # 从环境变量读取配置，支持开发和生产环境
    # 默认开发环境: 端口5001, debug模式
    # 生产环境: 端口8000, 非debug模式
    env = os.getenv('FLASK_ENV', 'development')
    if env == 'production':
        port = int(os.getenv('PORT', 8000))
        debug = False
    else:
        port = int(os.getenv('PORT', 5001))
        debug = True

    app.run(host='0.0.0.0', port=port, debug=debug)
