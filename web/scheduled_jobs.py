"""
定时任务执行函数模块

这些函数需要在模块级别定义，以便APScheduler可以序列化它们
"""
import logging
import sqlite3
import json
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


# 存储定时任务配置的字典（内存缓存）
job_configs = {}

# 配置数据库路径
_configs_db_path = None


def _init_configs_db(db_path):
    """初始化配置数据库"""
    global _configs_db_path
    _configs_db_path = Path(db_path)

    # 确保父目录存在
    _configs_db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(_configs_db_path))
    cursor = conn.cursor()

    # 创建配置表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS job_configs (
            job_id TEXT PRIMARY KEY,
            config TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.commit()
    conn.close()
    logger.info(f"[ScheduledJobs] Configs database initialized: {_configs_db_path}")


def _load_configs_from_db():
    """从数据库加载所有任务配置到内存"""
    global job_configs, _configs_db_path

    if _configs_db_path is None or not _configs_db_path.exists():
        return

    conn = sqlite3.connect(str(_configs_db_path))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('SELECT job_id, config FROM job_configs')
    rows = cursor.fetchall()
    conn.close()

    job_configs.clear()
    for row in rows:
        try:
            config = json.loads(row['config'])
            job_configs[row['job_id']] = config
            logger.info(f"[ScheduledJobs] Loaded config for {row['job_id']}: {config}")
        except json.JSONDecodeError as e:
            logger.error(f"[ScheduledJobs] Failed to load config for {row['job_id']}: {e}")

    logger.info(f"[ScheduledJobs] Loaded {len(job_configs)} job configs from database")


def register_job_config(job_id, config):
    """注册定时任务配置（持久化到数据库）"""
    global job_configs, _configs_db_path

    # 保存到内存
    job_configs[job_id] = config

    # 保存到数据库
    if _configs_db_path:
        try:
            conn = sqlite3.connect(str(_configs_db_path))
            cursor = conn.cursor()

            cursor.execute('''
                INSERT OR REPLACE INTO job_configs (job_id, config, updated_at)
                VALUES (?, ?, datetime('now', 'localtime'))
            ''', (job_id, json.dumps(config)))

            conn.commit()
            conn.close()
            logger.info(f"[ScheduledJobs] Registered config for {job_id}: {config}")
        except Exception as e:
            logger.error(f"[ScheduledJobs] Failed to save config for {job_id}: {e}")
    else:
        logger.warning(f"[ScheduledJobs] Configs DB not initialized, config not persisted: {job_id}")


def unregister_job_config(job_id):
    """注销定时任务配置（从数据库删除）"""
    global job_configs, _configs_db_path

    # 从内存删除
    if job_id in job_configs:
        del job_configs[job_id]

    # 从数据库删除
    if _configs_db_path:
        try:
            conn = sqlite3.connect(str(_configs_db_path))
            cursor = conn.cursor()
            cursor.execute('DELETE FROM job_configs WHERE job_id = ?', (job_id,))
            conn.commit()
            conn.close()
            logger.info(f"[ScheduledJobs] Unregistered config for {job_id}")
        except Exception as e:
            logger.error(f"[ScheduledJobs] Failed to delete config for {job_id}: {e}")


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
            # 从 Tushare API 获取股票列表（避免包含指数代码）
            try:
                stock_list_df = db._retry_api_call(
                    db.pro.stock_basic,
                    exchange='',
                    list_status='L',
                    fields='ts_code'
                )
                if stock_list_df is not None and not stock_list_df.empty:
                    stock_list = stock_list_df['ts_code'].tolist()
                else:
                    logger.error(f"[ScheduledJobs] Failed to get stock list from Tushare API")
                    return
            except Exception as e:
                logger.error(f"[ScheduledJobs] Error getting stock list: {e}")
                return

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


def run_index_update_job(job_id=None):
    """
    执行指数更新任务

    Args:
        job_id: 任务ID (格式: index_update_{name})
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
        markets = config.get('markets', ['SSE', 'SZSE'])

        logger.info(f"[ScheduledJobs] Running index update job: {name}")

        tm = init_tm(db_path=str(TASKS_DB_PATH), checkpoint_dir=CHECKPOINT_DIR)
        db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

        # 创建任务
        try:
            task_id = tm.create_task(
                f'scheduled_{name}',
                {'content_type': 'index', 'markets': markets},
                metadata={'total_indices': 0}
            )
            tm.update_task(task_id, status='running', message=f'定时任务执行中: {name}')
        except TaskExistsError:
            # 活动任务存在，静默跳过此次运行
            logger.info(f"[ScheduledJobs] Skipping scheduled job: active task exists")
            return

        # 执行指数更新（支持增量/全量模式）
        mode = config.get('mode', 'incremental')
        start_date = '20240101' if mode == 'full' else None
        end_date = datetime.now().strftime('%Y%m%d')
        stats = db.save_all_indices(start_date=start_date, end_date=end_date, markets=markets)

        # 完成任务
        tm.update_task(task_id,
            status='completed',
            message=f'定时任务完成: {name}',
            result=stats
        )

        logger.info(f"[ScheduledJobs] Index update job completed: {name}")

    except Exception as e:
        logger.error(f"[ScheduledJobs] Error in index update job {job_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())


def run_scheduled_job_dispatcher(job_id=None):
    """
    统一分发函数：根据content_type调用对应的执行函数

    Args:
        job_id: 任务ID (格式: {content_type}_update_{name})
    """
    config = job_configs.get(job_id)
    if not config:
        logger.error(f"[ScheduledJobs] No config found for {job_id}")
        return

    content_type = config.get('content_type', 'stock')  # 默认为stock，保持向后兼容

    if content_type == 'index':
        run_index_update_job(job_id)
    else:
        # 默认为股票更新（保持向后兼容）
        run_stock_update_job(job_id)
