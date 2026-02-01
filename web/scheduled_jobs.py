"""
定时任务执行函数模块

这些函数需要在模块级别定义，以便APScheduler可以序列化它们
"""
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


# 存储定时任务配置的字典
job_configs = {}


def register_job_config(job_id, config):
    """注册定时任务配置"""
    job_configs[job_id] = config
    logger.info(f"[ScheduledJobs] Registered config for {job_id}: {config}")


def unregister_job_config(job_id):
    """注销定时任务配置"""
    if job_id in job_configs:
        del job_configs[job_id]
        logger.info(f"[ScheduledJobs] Unregistered config for {job_id}")


def run_stock_update_job(job_id=None):
    """
    执行股票更新任务

    Args:
        job_id: 任务ID (格式: stock_update_{name})
    """
    try:
        from web.tasks import init_task_manager as init_tm
        from src.data_sources.tushare import TushareDB
        from web.routes import TaskExistsError, TUSHARE_TOKEN, TASKS_DB_PATH, TUSHARE_DB_PATH, CHECKPOINT_DIR

        # 获取任务配置
        config = job_configs.get(job_id)
        if not config:
            logger.error(f"[ScheduledJobs] No config found for {job_id}")
            return

        name = config.get('name', 'unknown')
        mode = config.get('mode', 'incremental')
        stock_range = config.get('stock_range', 'all')
        custom_stocks = config.get('custom_stocks', [])

        logger.info(f"[ScheduledJobs] Running stock update job: {name}")

        tm = init_tm(db_path=str(TASKS_DB_PATH), checkpoint_dir=CHECKPOINT_DIR)
        db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))
        end_date = datetime.now().strftime('%Y%m%d')

        # 获取股票列表
        if stock_range == 'custom':
            stock_list = custom_stocks
        elif stock_range == 'favorites':
            # TODO: 从持久化存储获取收藏列表
            stock_list = ["600382", "600711", "000001"]
        else:  # all
            from sqlalchemy import text
            with db.engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT DISTINCT symbol FROM bars WHERE symbol LIKE '____%'"
                ))
                stock_list = [row[0] for row in result.fetchall()]

        if not stock_list:
            logger.warning(f"[ScheduledJobs] No stocks to update for {job_id}")
            return

        logger.info(f"[ScheduledJobs] Updating {len(stock_list)} stocks for {name}")

        # 创建任务 (如果活动任务存在会抛出 TaskExistsError)
        try:
            task_id = tm.create_task(
                f'scheduled_{name}',
                {'mode': mode, 'stock_range': stock_range},
                metadata={'total_stocks': len(stock_list)}
            )
            tm.update_task(task_id, status='running', message=f'定时任务执行中: {name}')
        except TaskExistsError:
            # 活动任务存在，静默跳过此次运行
            logger.info(f"[ScheduledJobs] Skipping scheduled job: active task exists")
            return

        # 执行更新
        detailed_results = {'success': [], 'failed': [], 'skipped': []}

        for i, stock_code in enumerate(stock_list):
            # 检查停止请求（在每次循环开始时）
            current_task = tm.get_task(task_id)
            if current_task and current_task.get('stop_requested'):
                logger.info(f"[ScheduledJobs] Stop requested for {name}")
                tm.update_task(task_id, status='stopped', message='任务已停止')
                tm.clear_stop_request(task_id)
                return

            try:
                tm.update_task(task_id,
                    current_stock_index=i,
                    progress=int((i / len(stock_list)) * 100),
                    message=f'正在更新 {stock_code} ({i+1}/{len(stock_list)})...'
                )

                if mode == 'full':
                    stats = db.save_all_stocks_by_code('20200101', end_date, [stock_code])
                else:
                    stats = db.save_all_stocks_by_code_incremental('20240101', end_date, [stock_code])

                if stats:
                    if stats.get('success', 0) > 0 and stats.get('failed', 0) == 0:
                        detailed_results['success'].append(stock_code)
                    if stats.get('failed', 0) > 0:
                        detailed_results['failed'].append(stock_code)
                    if stats.get('skipped', 0) > 0:
                        detailed_results['skipped'].append(stock_code)
                    tm.increment_stats(task_id, 'success')
                else:
                    detailed_results['failed'].append(stock_code)
                    tm.increment_stats(task_id, 'failed')

            except Exception as e:
                detailed_results['failed'].append(stock_code)
                tm.increment_stats(task_id, 'failed')
                logger.error(f"[ScheduledJobs] Failed to update {stock_code}: {e}")

        # 完成任务
        summary_stats = {
            'total': len(stock_list),
            'success': len(detailed_results['success']),
            'failed': len(detailed_results['failed']),
            'skipped': len(detailed_results['skipped']),
            'details': detailed_results
        }

        tm.update_task(task_id,
            status='completed',
            message=f'定时任务完成: {name}',
            current_stock_index=len(stock_list),
            progress=100,
            result=summary_stats
        )

        logger.info(f"[ScheduledJobs] Job completed: {name}")

    except Exception as e:
        logger.error(f"[ScheduledJobs] Error in stock update job {job_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
