"""
Task Execution Handlers

These functions handle the actual execution of different task types.
Each handler receives the TaskManager, task_id, and task parameters.
"""
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def execute_update_stock_prices(tm, task_id, params):
    """
    Execute stock price update task.

    Args:
        tm: TaskManager instance
        task_id: Task identifier
        params: Task parameters (stock_range, custom_stocks, etc.)
    """
    from src.data_sources.tushare import TushareDB
    from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH
    from worker.utils import get_stock_list_for_task

    db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

    # Get parameters
    stock_range = params.get('stock_range', 'all')
    custom_stocks = params.get('custom_stocks', [])
    stocks_param = params.get('stocks', [])  # For backward compatibility

    # Get stock list
    stock_list = get_stock_list_for_task(stock_range, custom_stocks, db, stocks_param)

    if not stock_list:
        tm.update_task(
            task_id,
            status='failed',
            message='无法获取股票列表'
        )
        return

    # Load checkpoint if exists
    checkpoint = tm.load_checkpoint(task_id)
    start_index = 0
    stats = {'success': 0, 'failed': 0, 'skipped': 0}

    if checkpoint:
        start_index = checkpoint.get('current_index', 0)
        stats = checkpoint.get('stats', stats)
        print(f"[Task-{task_id[:8]}] Resuming from index {start_index}")

    # Update task progress
    tm.update_task(
        task_id,
        total_stocks=len(stock_list),
        current_stock_index=start_index,
        stats=stats,
        message=f'正在更新股票数据 ({len(stock_list)} 只)...'
    )

    # Execute updates
    end_date = datetime.now().strftime('%Y%m%d')

    for i in range(start_index, len(stock_list)):
        stock_code = stock_list[i]

        # Check stop request
        if tm.is_stop_requested(task_id):
            print(f"[Task-{task_id[:8]}] Stop requested, stopping at index {i}")
            task = tm.get_task(task_id)
            task_stats = task.get('stats') if task else stats
            tm.save_checkpoint(task_id, i, task_stats)
            tm.update_task(task_id, status='stopped', message='任务已停止')
            tm.clear_stop_request(task_id)
            return

        # Check pause request
        while tm.is_pause_requested(task_id):
            tm.update_task(task_id, status='paused', message='任务已暂停')
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
            # Resume if pause cleared
            if not tm.is_pause_requested(task_id):
                tm.update_task(task_id, status='running', message='任务已恢复执行')

        try:
            # Update progress
            progress = int(((i + 1) / len(stock_list)) * 100)
            tm.update_task(
                task_id,
                current_stock_index=i + 1,
                progress=progress,
                message=f'正在更新 {stock_code} ({i+1}/{len(stock_list)})...'
            )

            # Save checkpoint every 10 stocks
            if i % 10 == 0:
                current_task = tm.get_task(task_id)
                current_stats = current_task.get('stats') if current_task else stats
                tm.save_checkpoint(task_id, i, current_stats)

            # Update stock data
            print(f"[Task-{task_id[:8]}] Updating stock {stock_code} ({i+1}/{len(stock_list)})...")
            stats_result = db.save_all_stocks_by_code_incremental(
                default_start_date='20240101',
                end_date=end_date,
                stock_list=[stock_code]
            )
            print(f"[Task-{task_id[:8]}] Stock {stock_code} update complete")

            # Update stats
            if stats_result:
                if stats_result.get('success', 0) > 0:
                    stats['success'] += stats_result.get('success', 0)
                    tm.increment_stats(task_id, 'success', stats_result.get('success', 0))
                if stats_result.get('failed', 0) > 0:
                    stats['failed'] += stats_result.get('failed', 0)
                    tm.increment_stats(task_id, 'failed', stats_result.get('failed', 0))
                if stats_result.get('skipped', 0) > 0:
                    stats['skipped'] += stats_result.get('skipped', 0)
                    tm.increment_stats(task_id, 'skipped', stats_result.get('skipped', 0))
            else:
                stats['failed'] += 1
                tm.increment_stats(task_id, 'failed')

        except Exception as e:
            stats['failed'] += 1
            tm.increment_stats(task_id, 'failed')
            print(f"[ERROR] Failed to update {stock_code}: {e}")

    # Complete task
    tm.delete_checkpoint(task_id)
    tm.update_task(task_id,
        status='completed',
        progress=100,
        current_stock_index=len(stock_list),
        message='更新完成',
        result={'updated_stocks': stats.get('success', 0)},
        stats=stats
    )


def execute_update_industry_classification(tm, task_id, params):
    """
    Execute SW industry classification update task.

    Args:
        tm: TaskManager instance
        task_id: Task identifier
        params: Task parameters (src, force)
    """
    from src.data_sources.tushare import TushareDB
    from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH

    src = params.get('src', 'SW2021')
    force = params.get('force', False)

    db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

    tm.update_task(
        task_id,
        status='running',
        message=f'正在更新申万行业分类数据...'
    )

    # Step 1: Save industry classification
    print(f"[Task-{task_id[:8]}] Step 1: Saving industry classification...")
    classify_count = db.save_sw_classify(src=src, update_timestamp=True)

    if classify_count == 0:
        tm.update_task(
            task_id,
            status='failed',
            message='获取行业分类失败',
            stats={'success': 0, 'failed': 0, 'skipped': 0}
        )
        return

    # Step 2: Get all industry codes
    try:
        # Try to get indices from database method if available
        if hasattr(db, 'get_sw_industry_codes'):
            all_indices = db.get_sw_industry_codes(src=src)
        else:
            # Fall back to query
            import pandas as pd
            query = "SELECT index_code FROM sw_classify WHERE src = :src"
            with db.engine.connect() as conn:
                df_indices = pd.read_sql_query(query, conn, params={"src": src})
            all_indices = df_indices['index_code'].tolist()
    except Exception as e:
        print(f"[Task-{task_id[:8]}] Failed to get industry codes: {e}")
        tm.update_task(
            task_id,
            status='failed',
            message=f'获取行业代码失败: {e}',
            stats={'success': 0, 'failed': 0, 'skipped': 0}
        )
        return

    total_count = len(all_indices)

    if total_count == 0:
        tm.update_task(
            task_id,
            status='failed',
            message='没有找到行业分类',
            stats={'success': 0, 'failed': 0, 'skipped': 0}
        )
        return

    # Initialize tracking variables
    failed_indices = []
    members_count = 0

    # Load checkpoint if exists
    checkpoint = tm.load_checkpoint(task_id)
    start_index = 0

    if checkpoint:
        start_index = checkpoint.get('current_index', 0)
        failed_indices = checkpoint.get('failed_indices', [])
        members_count = checkpoint.get('members_count', 0)
        print(f"[Task-{task_id[:8]}] Resuming from index {start_index}")

    tm.update_task(
        task_id,
        total_stocks=1,  # Only one batch operation
        current_stock_index=0,
        stats={'success': 0, 'failed': 0, 'skipped': 0},
        message=f'正在更新申万行业成分股数据...'
    )

    # Step 3: Save all industry members in one batch
    print(f"[Task-{task_id[:8]}] Saving all industry members...")
    try:
        # Delete old data if force update
        if force:
            logger.info(f"[Task-{task_id[:8]}] Force update: deleting old data...")
            with db.engine.connect() as conn:
                from sqlalchemy import text
                # Get count before deletion
                old_count = conn.execute(text("SELECT COUNT(*) FROM sw_members")).scalar()
                logger.info(f"[Task-{task_id[:8]}] Old records count: {old_count}")
                # Delete all
                conn.execute(text("DELETE FROM sw_members"))
                conn.commit()
                # Verify deletion
                new_count = conn.execute(text("SELECT COUNT(*) FROM sw_members")).scalar()
                logger.info(f"[Task-{task_id[:8]}] Records after deletion: {new_count}")
                logger.info(f"[Task-{task_id[:8]}] Deleted {old_count} old records, fetching fresh data from API...")
                print(f"[Task-{task_id[:8]}] Deleted {old_count} old records, fetching fresh data from API...")

        # Save all members - use new method to get all stocks by iterating L1 industries
        # This bypasses the 2000 row API limit
        count = db.save_sw_members_all(is_new='Y', src=src)

        if count > 0:
            members_count = count
            tm.update_task(
                task_id,
                current_stock_index=1,
                progress=100,
                message=f'已保存 {count} 条行业成分股记录'
            )
            tm.increment_stats(task_id, 'success')
        else:
            tm.update_task(
                task_id,
                status='failed',
                message='保存行业成分股失败'
            )
            tm.increment_stats(task_id, 'failed')
            return

    except Exception as e:
        print(f"[ERROR] Failed to save industry members: {e}")
        import traceback
        traceback.print_exc()
        tm.update_task(
            task_id,
            status='failed',
            message=f'保存行业成分股失败: {e}'
        )
        tm.increment_stats(task_id, 'failed')
        return

    # Complete task
    tm.delete_checkpoint(task_id)
    final_stats = tm.get_task(task_id).get('stats', {})

    result = {
        'classify_count': classify_count,
        'members_count': members_count,
        'total_indices': total_count,
        'skipped_indices': 0,
        'failed_indices': failed_indices
    }

    tm.update_task(task_id,
        status='completed',
        progress=100,
        current_stock_index=total_count,
        message='申万行业分类更新完成',
        result=result,
        stats=final_stats
    )


def execute_update_financial_reports(tm, task_id, params):
    """
    Execute financial reports update task.

    Args:
        tm: TaskManager instance
        task_id: Task identifier
        params: Task parameters (stock_range, custom_stocks, include_indicators, include_reports)
    """
    from src.data_sources.tushare import TushareDB
    from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH
    from worker.utils import get_stock_list_for_task

    db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

    stock_range = params.get('stock_range', 'all')
    custom_stocks = params.get('custom_stocks', [])
    include_indicators = params.get('include_indicators', True)
    include_reports = params.get('include_reports', True)
    stocks_param = params.get('stocks', [])

    # Get stock list
    stock_list = get_stock_list_for_task(stock_range, custom_stocks, db, stocks_param)

    if not stock_list:
        tm.update_task(
            task_id,
            status='failed',
            message='无法获取股票列表'
        )
        return

    tm.update_task(
        task_id,
        total_stocks=len(stock_list),
        status='running',
        message=f'正在更新财务数据 ({len(stock_list)} 只股票)...'
    )

    # Load checkpoint if exists
    checkpoint = tm.load_checkpoint(task_id)
    start_index = 0
    stats = {'success': 0, 'failed': 0, 'skipped': 0}

    if checkpoint:
        start_index = checkpoint.get('current_index', 0)
        stats = checkpoint.get('stats', stats)
        print(f"[Financial-{task_id[:8]}] Resuming from index {start_index}")

    # Update each stock's financial data
    for i in range(start_index, len(stock_list)):
        stock_code = stock_list[i]

        # Check stop request
        if tm.is_stop_requested(task_id):
            task = tm.get_task(task_id)
            task_stats = task.get('stats', stats) if task else stats
            tm.save_checkpoint(task_id, i, task_stats)
            tm.update_task(task_id, status='stopped', message='任务已停止')
            tm.clear_stop_request(task_id)
            return

        # Check pause request
        while tm.is_pause_requested(task_id):
            tm.update_task(task_id, status='paused', message='任务已暂停')
            time.sleep(1)
            if tm.is_stop_requested(task_id):
                tm.update_task(task_id, status='stopped', message='任务已停止')
                tm.clear_stop_request(task_id)
                tm.clear_pause_request(task_id)
                return
            if not tm.is_pause_requested(task_id):
                tm.update_task(task_id, status='running', message='任务已恢复执行')

        try:
            # Update progress
            progress = int(((i + 1) / len(stock_list)) * 100)
            tm.update_task(
                task_id,
                progress=progress,
                current_stock_index=i + 1,
                message=f'正在更新 {stock_code} ({i+1}/{len(stock_list)})...'
            )

            # Save checkpoint every 10 stocks
            if i % 10 == 0:
                current_task = tm.get_task(task_id)
                current_stats = current_task.get('stats', stats) if current_task else stats
                tm.save_checkpoint(task_id, i, current_stats)

            # Standardize stock code
            ts_code = db._standardize_code(stock_code)

            # Update financial data
            if include_reports:
                # Update main financial reports (income, balance sheet, cash flow)
                db.save_all_financial(ts_code, include_indicators=include_indicators)
            elif include_indicators:
                # Only update financial indicators
                db.save_fina_indicator(ts_code)

            stats['success'] += 1
            tm.increment_stats(task_id, 'success')
            print(f"[Financial-{task_id[:8]}] ✓ {stock_code} updated successfully")

        except Exception as e:
            stats['failed'] += 1
            tm.increment_stats(task_id, 'failed')
            print(f"[Financial-{task_id[:8]}] ✗ {stock_code} failed: {e}")

    # Complete task
    tm.delete_checkpoint(task_id)
    tm.update_task(task_id,
        status='completed',
        progress=100,
        current_stock_index=len(stock_list),
        message=f'财务数据更新完成 (成功:{stats["success"]}, 失败:{stats["failed"]})',
        result=stats,
        stats=stats
    )
    print(f"[Financial-{task_id[:8]}] All tasks completed with stats: {stats}")


def execute_update_index_data(tm, task_id, params):
    """
    Execute index data update task.

    Args:
        tm: TaskManager instance
        task_id: Task identifier
        params: Task parameters (markets, start_date, end_date)
    """
    from src.data_sources.tushare import TushareDB
    from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH
    from sqlalchemy import text

    db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

    markets = params.get('markets', ['SSE', 'SZSE'])
    start_date = '20240101'
    end_date = datetime.now().strftime('%Y%m%d')

    tm.update_task(
        task_id,
        status='running',
        message='正在获取指数列表...'
    )

    # Get all indices
    all_indices = []
    for market in markets:
        try:
            count = db.save_index_basic(market=market)
            if count > 0:
                with db.engine.connect() as conn:
                    query = "SELECT ts_code FROM index_names"
                    if market == 'SSE':
                        query += " WHERE ts_code LIKE '%.SH'"
                    elif market == 'SZSE':
                        query += " WHERE ts_code LIKE '%.SZ'"
                    result = conn.execute(text(query))
                    all_indices.extend([row[0] for row in result.fetchall()])
        except Exception as e:
            print(f"Failed to get {market} indices: {e}")

    # Remove duplicates
    all_indices = list(set(all_indices))
    total_indices = len(all_indices)

    if not total_indices:
        tm.update_task(
            task_id,
            status='failed',
            message='没有找到指数'
        )
        return

    # Update task with total count
    tm.update_task(
        task_id,
        message=f'正在更新 {total_indices} 个指数数据...',
        total_stocks=total_indices
    )

    # Load checkpoint if exists
    checkpoint = tm.load_checkpoint(task_id)
    start_index = 0
    stats = {'success': 0, 'failed': 0, 'skipped': 0}

    if checkpoint:
        start_index = checkpoint.get('current_index', 0)
        stats = checkpoint.get('stats', stats)
        print(f"[Task-{task_id[:8]}] Resuming from index {start_index}")

    # Update each index individually to track progress
    for i in range(start_index, total_indices):
        ts_code = all_indices[i]

        # Check stop request
        if tm.is_stop_requested(task_id):
            task = tm.get_task(task_id)
            task_stats = task.get('stats', stats) if task else stats
            tm.save_checkpoint(task_id, i, task_stats)
            tm.update_task(task_id, status='stopped', message='任务已停止')
            tm.clear_stop_request(task_id)
            return

        # Check pause request
        while tm.is_pause_requested(task_id):
            tm.update_task(task_id, status='paused', message='任务已暂停')
            time.sleep(1)
            if tm.is_stop_requested(task_id):
                tm.update_task(task_id, status='stopped', message='任务已停止')
                tm.clear_stop_request(task_id)
                tm.clear_pause_request(task_id)
                return
            if not tm.is_pause_requested(task_id):
                tm.update_task(task_id, status='running', message='任务已恢复执行')

        try:
            # Update progress
            progress = int(((i + 1) / total_indices) * 100)
            tm.update_task(
                task_id,
                progress=progress,
                current_stock_index=i + 1,
                message=f'正在更新 {ts_code} ({i+1}/{total_indices})...'
            )

            # Save checkpoint every 10 indices
            if i % 10 == 0:
                current_task = tm.get_task(task_id)
                current_stats = current_task.get('stats', stats) if current_task else stats
                tm.save_checkpoint(task_id, i, current_stats)

            # Update index daily data
            result = db.save_index_daily(ts_code, start_date, end_date)

            # Update stats
            if result > 0:
                stats['success'] += 1
                tm.increment_stats(task_id, 'success')
            elif result == 0:
                stats['skipped'] += 1
                tm.increment_stats(task_id, 'skipped')
            else:
                stats['failed'] += 1
                tm.increment_stats(task_id, 'failed')

        except Exception as e:
            stats['failed'] += 1
            tm.increment_stats(task_id, 'failed')
            print(f"ERROR: Failed to update {ts_code}: {e}")

    # Complete task
    tm.delete_checkpoint(task_id)
    tm.update_task(task_id,
        status='completed',
        progress=100,
        current_stock_index=total_indices,
        message='指数数据更新完成',
        result={'updated_indices': stats.get('success', 0)},
        stats=stats
    )


def execute_test_handler(tm, task_id, params):
    """
    执行测试任务以验证任务管理功能。

    该处理器模拟一个长时间运行的任务，处理一系列项目，用于全面测试：
    - 任务生命周期 (pending -> running -> completed/failed/stopped)
    - 进度跟踪和更新
    - 暂停/恢复/停止操作
    - 断点续传机制（保存/加载检查点）
    - 统计信息收集 (success/failed/skipped)
    - 错误处理和恢复

    Args:
        tm: TaskManager 实例
        task_id: 任务标识符
        params: 任务参数字典:
            - total_items: int (默认: 100) - 要处理的总项目数
            - item_duration_ms: int (默认: 100) - 每个项目处理时间（毫秒）
            - checkpoint_interval: int (默认: 10) - 每N个项目保存检查点
            - failure_rate: float (默认: 0.0) - 随机失败率 (0.0-1.0)
            - simulate_pause: bool (默认: False) - 在50%时自动暂停用于测试
    """
    import random
    import time

    # 提取参数并设置默认值
    total_items = params.get('total_items', 100)
    item_duration_ms = params.get('item_duration_ms', 100)
    checkpoint_interval = params.get('checkpoint_interval', 10)
    failure_rate = params.get('failure_rate', 0.0)
    simulate_pause = params.get('simulate_pause', False)

    # 验证参数
    if total_items <= 0:
        tm.update_task(
            task_id,
            status='failed',
            message='Invalid parameter: total_items must be > 0'
        )
        return

    if item_duration_ms < 0:
        tm.update_task(
            task_id,
            status='failed',
            message='Invalid parameter: item_duration_ms must be >= 0'
        )
        return

    if not (0.0 <= failure_rate <= 1.0):
        tm.update_task(
            task_id,
            status='failed',
            message='Invalid parameter: failure_rate must be between 0.0 and 1.0'
        )
        return

    # 加载检查点（如果存在，用于恢复测试）
    checkpoint = tm.load_checkpoint(task_id)
    start_index = 0
    stats = {'success': 0, 'failed': 0, 'skipped': 0}

    if checkpoint:
        start_index = checkpoint.get('current_index', 0)
        stats = checkpoint.get('stats', stats)
        print(f"[TestHandler-{task_id[:8]}] Resuming from item {start_index}")

    # 更新任务初始进度
    tm.update_task(
        task_id,
        total_stocks=total_items,  # 复用 total_stocks 字段
        current_stock_index=start_index,  # 复用 current_stock_index 字段
        stats=stats,
        message=f'测试任务启动，准备处理 {total_items} 个项目...'
    )

    # 处理每个项目
    for i in range(start_index, total_items):
        item_name = f"item-{i+1:03d}"

        # 检查停止请求
        if tm.is_stop_requested(task_id):
            print(f"[TestHandler-{task_id[:8]}] Stop requested at item {i}")
            task = tm.get_task(task_id)
            task_stats = task.get('stats') if task else stats
            tm.save_checkpoint(task_id, i, task_stats)
            tm.update_task(task_id, status='stopped', message='任务已停止')
            tm.clear_stop_request(task_id)
            return

        # 检查暂停请求
        while tm.is_pause_requested(task_id):
            tm.update_task(task_id, status='paused', message=f'任务已暂停 (项目 {i+1}/{total_items})')
            time.sleep(1)
            # 暂停时重新检查停止请求
            if tm.is_stop_requested(task_id):
                task = tm.get_task(task_id)
                task_stats = task.get('stats') if task else stats
                tm.save_checkpoint(task_id, i, task_stats)
                tm.update_task(task_id, status='stopped', message='任务已停止')
                tm.clear_stop_request(task_id)
                tm.clear_pause_request(task_id)
                return
            # 如果清除暂停标志则恢复
            if not tm.is_pause_requested(task_id):
                tm.update_task(task_id, status='running', message='任务已恢复执行')

        # 更新进度
        progress = int(((i + 1) / total_items) * 100)
        tm.update_task(
            task_id,
            current_stock_index=i + 1,
            progress=progress,
            message=f'正在处理 {item_name} ({i+1}/{total_items})...'
        )

        # 按间隔保存检查点
        if i % checkpoint_interval == 0:
            current_task = tm.get_task(task_id)
            current_stats = current_task.get('stats') if current_task else stats
            tm.save_checkpoint(task_id, i, current_stats)
            print(f"[TestHandler-{task_id[:8]}] Checkpoint saved at item {i}")

        # 如果请求则在50%时模拟暂停
        if simulate_pause and i == total_items // 2:
            print(f"[TestHandler-{task_id[:8]}] Auto-pause at 50% (item {i})")
            tm.request_pause(task_id)
            continue  # 下一次循环将进入暂停处理

        # 模拟工作（休眠）
        if item_duration_ms > 0:
            time.sleep(item_duration_ms / 1000.0)

        # 模拟失败
        if failure_rate > 0 and random.random() < failure_rate:
            stats['failed'] += 1
            tm.increment_stats(task_id, 'failed')
            print(f"[TestHandler-{task_id[:8]}] {item_name} failed (simulated)")
        else:
            stats['success'] += 1
            tm.increment_stats(task_id, 'success')
            print(f"[TestHandler-{task_id[:8]}] {item_name} completed")

    # 完成任务
    tm.delete_checkpoint(task_id)
    tm.update_task(task_id,
        status='completed',
        progress=100,
        current_stock_index=total_items,
        message=f'测试完成: {stats.get("success", 0)} 成功, {stats.get("failed", 0)} 失败',
        result=stats,
        stats=stats
    )
    print(f"[TestHandler-{task_id[:8]}] Test completed with stats: {stats}")


def execute_train_quarterly_model(tm, task_id, params):
    """
    Execute quarterly financial prediction model training task.

    Args:
        tm: TaskManager instance
        task_id: Task identifier
        params: Task parameters:
            - symbols: List of stock codes
            - start_quarter: Start quarter (e.g., "2020Q1")
            - end_quarter: End quarter (e.g., "2024Q4")
            - feature_mode: Feature mode ("financial_only", "with_reports", "with_valuation")
            - train_mode: Training mode ("single" or "multi")
            - model_id: Optional model ID
            - optimize_hyperparams: Whether to optimize hyperparameters (default: False)
            - train_ratio: Training set ratio (default: 0.7)
            - val_ratio: Validation set ratio (default: 0.15)
    """
    from src.ml.trainers.quarterly_trainer import QuarterlyTrainer
    from config.settings import TUSHARE_DB_PATH

    print(f"[QuarterlyModel-{task_id[:8]}] Starting quarterly model training")

    # Get parameters
    symbols = params.get('symbols', [])
    start_quarter = params.get('start_quarter', '2020Q1')
    end_quarter = params.get('end_quarter', '2024Q4')
    feature_mode = params.get('feature_mode', 'financial_only')
    train_mode = params.get('train_mode', 'multi')
    model_id = params.get('model_id')
    optimize_hyperparams = params.get('optimize_hyperparams', False)
    train_ratio = params.get('train_ratio', 0.7)
    val_ratio = params.get('val_ratio', 0.15)

    # Validate parameters
    if not symbols:
        tm.update_task(
            task_id,
            status='failed',
            message='必须提供股票代码列表 (symbols)'
        )
        return

    if train_mode == 'single' and len(symbols) > 1:
        tm.update_task(
            task_id,
            status='failed',
            message='单股票模式 (single) 只能提供一只股票代码'
        )
        return

    # Update task
    tm.update_task(
        task_id,
        status='running',
        message=f'开始训练季度模型: {train_mode} 模式, {len(symbols)} 只股票, {start_quarter} - {end_quarter}'
    )

    try:
        # Initialize trainer
        trainer = QuarterlyTrainer(db_path=str(TUSHARE_DB_PATH))

        # Check stop request
        if tm.is_stop_requested(task_id):
            print(f"[QuarterlyModel-{task_id[:8]}] Stop requested before training")
            tm.update_task(task_id, status='stopped', message='任务已停止')
            tm.clear_stop_request(task_id)
            return

        # Train model
        print(f"[QuarterlyModel-{task_id[:8]}] Loading data and training model...")
        result = trainer.train(
            symbols=symbols,
            start_quarter=start_quarter,
            end_quarter=end_quarter,
            feature_mode=feature_mode,
            train_mode=train_mode,
            train_ratio=train_ratio,
            val_ratio=val_ratio,
            optimize_hyperparams=optimize_hyperparams,
            model_id=model_id
        )

        # Check stop request during training
        if tm.is_stop_requested(task_id):
            print(f"[QuarterlyModel-{task_id[:8]}] Stop requested during training")
            tm.update_task(task_id, status='stopped', message='训练已停止')
            tm.clear_stop_request(task_id)
            return

        # Generate report
        report = trainer.generate_report(result)
        print(report)

        # Complete task
        tm.update_task(
            task_id,
            status='completed',
            progress=100,
            message=f'训练完成: MAE={result["test_metrics"]["mae"]:.4f}, R²={result["test_metrics"]["r2"]:.4f}',
            result={
                'model_id': result['model_id'],
                'train_mode': train_mode,
                'symbols': symbols,
                'mae': result['test_metrics']['mae'],
                'r2': result['test_metrics']['r2'],
                'direction_accuracy': result['test_metrics']['direction_accuracy'],
                'n_features': result['n_features'],
                'train_samples': result['train_samples'],
                'test_samples': result['test_samples']
            }
        )

        print(f"[QuarterlyModel-{task_id[:8]}] Training completed successfully")

    except Exception as e:
        print(f"[ERROR] Quarterly model training failed: {e}")
        import traceback
        traceback.print_exc()

        tm.update_task(
            task_id,
            status='failed',
            message=f'训练失败: {str(e)}'
        )


def execute_update_industry_statistics(tm, task_id, params):
    """
    Execute industry statistics update task.

    Args:
        tm: TaskManager instance
        task_id: Task identifier
        params: Task parameters (metrics list)
    """
    from src.screening.calculators.industry_statistics_calculator import IndustryStatisticsCalculator
    from config.settings import TUSHARE_DB_PATH
    import pandas as pd
    from datetime import datetime

    try:
        metrics = params.get('metrics', ['pe_ttm', 'pb', 'ps_ttm', 'total_mv', 'circ_mv'])

        tm.update_task(
            task_id,
            status='running',
            message=f'正在更新行业统计数据 ({len(metrics)}个指标)...'
        )

        print(f"[Task-{task_id[:8]}] Updating industry statistics...")

        # Create calculator
        stats_calc = IndustryStatisticsCalculator(str(TUSHARE_DB_PATH))

        # Calculate statistics
        calculated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        stats_df = stats_calc.calculate_industry_statistics(
            calculated_at=calculated_at,
            metrics=metrics
        )

        if stats_df.empty:
            tm.update_task(
                task_id,
                status='failed',
                message='未生成统计数据'
            )
            return

        # Delete old data
        print(f"[Task-{task_id[:8]}] Deleting old statistics...")
        with stats_calc.engine.connect() as conn:
            from sqlalchemy import text
            conn.execute(text("DELETE FROM industry_statistics"))
            conn.commit()

        # Save new statistics
        print(f"[Task-{task_id[:8]}] Saving {len(stats_df)} records...")

        # 清理可能存在的tuple类型数据
        for col in ['sw_l1', 'sw_l2', 'sw_l3']:
            if col in stats_df.columns:
                stats_df[col] = stats_df[col].apply(
                    lambda x: x[0] if isinstance(x, tuple) and len(x) == 1 else x
                )

        stats_df.to_sql('industry_statistics', stats_calc.engine, if_exists='append', index=False)

        # Summary
        summary = {
            'total_records': len(stats_df),
            'l1_industries': int(stats_df['sw_l1'].nunique()),
            'l2_industries': int(stats_df['sw_l2'].nunique()),
            'l3_industries': int(stats_df['sw_l3'].nunique()),
            'metrics': int(stats_df['metric_name'].nunique()),
            'calculated_at': calculated_at
        }

        tm.update_task(
            task_id,
            status='completed',
            progress=100,
            message=f'行业统计更新完成: {summary["l1_industries"]}个一级行业, {summary["metrics"]}个指标',
            result=summary
        )

        print(f"[Task-{task_id[:8]}] Industry statistics updated successfully")
        print(f"  Total records: {summary['total_records']}")
        print(f"  L1 industries: {summary['l1_industries']}")
        print(f"  L2 industries: {summary['l2_industries']}")
        print(f"  L3 industries: {summary['l3_industries']}")
        print(f"  Metrics: {summary['metrics']}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        tm.update_task(
            task_id,
            status='failed',
            error=str(e),
            message=f'更新失败: {str(e)}'
        )


def execute_backtest(tm, task_id, params):
    """
    Execute backtest task (支持多策略).

    Args:
        tm: TaskManager instance
        task_id: Task identifier
        params: Task parameters:
            回测共有参数:
                - stock: 股票代码（必需）
                - start_date: 开始日期（必需）
                - end_date: 结束日期（可选）
                - cash: 初始资金（可选，默认100万）
                - commission: 手续费率（可选，默认0.2%）
                - benchmark: 基准指数（可选）

            策略参数:
                - strategy: 策略类型（必需）
                - strategy_params: 策略特定参数（必需）
    """
    from web.services.backtest_service import run_single_backtest

    # 获取参数
    stock_code = params.get('stock')
    start_date = params.get('start_date')
    strategy_type = params.get('strategy', 'sma_cross')

    strategy_name = {
        'sma_cross': 'SMA交叉策略'
    }.get(strategy_type, strategy_type)

    tm.update_task(
        task_id,
        status='running',
        message=f'正在执行{strategy_name}回测: {stock_code} ({start_date} 开始)...'
    )

    try:
        print(f"[Backtest-{task_id[:8]}] Starting backtest for {stock_code}")
        logger.info(f"[Backtest-{task_id[:8]}] Starting backtest for {stock_code}")

        # 运行回测
        result = run_single_backtest(params)

        logger.info(f"[Backtest-{task_id[:8]}] Backtest completed, total_trades={result['trade_stats']['total_trades']}")

        # 完成任务
        tm.update_task(
            task_id,
            status='completed',
            progress=100,
            message=f'{strategy_name}回测完成: {stock_code}, 总收益率: {result["basic_info"]["total_return"]:.2%}',
            result=result
        )

        print(f"[Backtest-{task_id[:8]}] Backtest completed successfully")
        print(f"  Total Return: {result['basic_info']['total_return']:.2%}")
        print(f"  Sharpe Ratio: {result['health_metrics']['sharpe_ratio']:.2f}")
        print(f"  Max Drawdown: {result['health_metrics']['max_drawdown']:.2%}")

    except Exception as e:
        print(f"[ERROR] Backtest failed: {e}")
        import traceback
        traceback.print_exc()

        tm.update_task(
            task_id,
            status='failed',
            message=f'{strategy_name}回测失败: {str(e)}'
        )


def execute_update_moneyflow(tm, task_id, params):
    """
    Execute moneyflow data update task.

    只负责获取个股资金流向数据，不包含行业汇总计算。

    Args:
        tm: TaskManager instance
        task_id: Task identifier
        params: Task parameters
            - mode: "incremental" | "full" (默认: "incremental")
            - stock_range: "all" | "favorites" | "custom" (默认: "all")
            - custom_stocks: 自定义股票列表
            - start_date: 开始日期 (可选)
            - end_date: 结束日期 (可选)
            - exclude_st: 是否排除ST股 (默认 True)
    """
    from src.data_sources.tushare import TushareDB
    from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH
    from worker.utils import get_stock_list_for_task
    from datetime import datetime, timedelta
    from sqlalchemy import text

    mode = params.get('mode', 'incremental')
    stock_range = params.get('stock_range', 'all')
    custom_stocks = params.get('custom_stocks', [])
    start_date = params.get('start_date')
    end_date = params.get('end_date')
    exclude_st = params.get('exclude_st', True)

    db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

    # 更新任务状态
    mode_text = '增量' if mode == 'incremental' else '全量'
    tm.update_task(task_id, status='running', message=f'正在获取资金流向数据（{mode_text}更新）...')

    try:
        stats = {}

        # 根据更新模式确定起始日期
        if start_date is None:
            if mode == 'incremental':
                # 增量更新：从数据库最新日期的下一天开始
                if stock_range == 'all':
                    query = text("SELECT MAX(trade_date) as max_date FROM stock_moneyflow")
                    with db.engine.connect() as conn:
                        result = conn.execute(query).fetchone()
                        if result and result[0]:
                            last_date = datetime.strptime(result[0], '%Y%m%d')
                            start_date = (last_date + timedelta(days=1)).strftime('%Y%m%d')
                            print(f"[Task-{task_id[:8]}] 增量更新：从数据库最新日期 {result[0]} 的下一天 {start_date} 开始")
                else:
                    # 按股票列表的增量更新：查询每只股票的最新日期
                    # 简化处理：从1年前开始
                    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
                    print(f"[Task-{task_id[:8]}] 按股票列表更新：从 {start_date} 开始")
            else:
                # 全量更新：从1年前开始
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
                print(f"[Task-{task_id[:8]}] 全量更新：从 {start_date} 开始")

        # 根据股票范围选择不同的获取方式
        if stock_range == 'all':
            # 按日期获取全市场数据
            print(f"[Task-{task_id[:8]}] Updating moneyflow data for all stocks (by date)...")
            tm.update_task(task_id, message=f'正在获取全市场资金流向数据（{mode_text}更新）...')
            stats = db.save_all_moneyflow_incremental(start_date=start_date, exclude_st=exclude_st)

        else:
            # 按股票列表获取（favorites 或 custom）
            stock_list = get_stock_list_for_task(stock_range, custom_stocks, db, [])

            if not stock_list:
                tm.update_task(
                    task_id,
                    status='failed',
                    message='无法获取股票列表'
                )
                return

            print(f"[Task-{task_id[:8]}] Updating moneyflow data for {len(stock_list)} stocks...")
            tm.update_task(task_id, message=f'正在获取 {len(stock_list)} 只股票的资金流向数据（{mode_text}更新）...')

            stats = db.save_moneyflow_by_stocks(
                stock_list=stock_list,
                start_date=start_date,
                end_date=end_date,
                exclude_st=exclude_st
            )

        # 完成任务
        tm.update_task(
            task_id,
            status='completed',
            message=f'资金流向{mode_text}更新完成 (成功:{stats.get("success", 0)}, 失败:{stats.get("failed", 0)})',
            result=stats
        )

        print(f"[Task-{task_id[:8]}] Moneyflow data update completed: {stats}")

        # 如果是全市场更新，自动触发行业汇总计算
        if stock_range == 'all':
            print(f"[Task-{task_id[:8]}] Auto-triggering industry moneyflow calculation...")
            industry_task_id = tm.create_task(
                task_type='calculate_industry_moneyflow',
                params={
                    'start_date': start_date,
                    'end_date': end_date,
                }
            )
            print(f"[Task-{task_id[:8]}] Created industry moneyflow task: {industry_task_id[:8]}...")

    except Exception as e:
        import traceback
        traceback.print_exc()
        tm.update_task(
            task_id,
            status='failed',
            message=f'资金流向数据更新失败: {str(e)}'
        )


def execute_calculate_industry_moneyflow(tm, task_id, params):
    """
    Execute industry moneyflow summary calculation task.

    Args:
        tm: TaskManager instance
        task_id: Task identifier
        params: Task parameters
            - start_date: 开始日期 YYYYMMDD (可选)
            - end_date: 结束日期 YYYYMMDD (可选)
    """
    from src.data_sources.tushare import TushareDB
    from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH

    start_date = params.get('start_date')
    end_date = params.get('end_date')

    db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

    # 更新任务状态
    tm.update_task(task_id, status='running', message='正在计算行业资金流向汇总...')

    try:
        print(f"[Task-{task_id[:8]}] Calculating industry moneyflow summary...")
        stats = db.save_industry_moneyflow_batch(start_date=start_date, end_date=end_date)

        # 完成任务
        tm.update_task(
            task_id,
            status='completed',
            message=f'行业资金流向汇总计算完成 (成功:{stats.get("success", 0)}, 失败:{stats.get("failed", 0)})',
            result=stats
        )

        print(f"[Task-{task_id[:8]}] Industry moneyflow calculation completed: {stats}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        tm.update_task(
            task_id,
            status='failed',
            message=f'行业资金流向汇总计算失败: {str(e)}'
        )


def execute_update_dragon_list(tm, task_id, params):
    """
    执行龙虎榜数据更新任务

    Args:
        tm: TaskManager instance
        task_id: Task identifier
        params: Task parameters
            - mode: 'incremental' or 'batch'
            - start_date: Start date YYYY-MM-DD (for batch mode)
            - end_date: End date YYYY-MM-DD (for batch mode)
    """
    from src.data_sources.tushare import TushareDB
    from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH
    from datetime import datetime

    mode = params.get('mode', 'incremental')
    start_date = params.get('start_date')
    end_date = params.get('end_date')

    try:
        # 初始化数据源
        db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

        if mode == 'batch' and start_date:
            # 批量模式：回填历史数据
            tm.update_task(task_id, status='running',
                             message=f'开始批量更新龙虎榜数据: {start_date} 至 {end_date or "今天"}')

            # 转换日期格式
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y%m%d')
            end_dt = end_date
            if end_dt:
                end_dt = datetime.strptime(end_dt, '%Y-%m-%d').strftime('%Y%m%d')

            total_count = db.save_dragon_list_batch(start_dt, end_dt)

            tm.update_task(
                task_id,
                status='completed',
                message=f'批量更新完成，共保存 {total_count} 条龙虎榜记录'
            )

        else:
            # 增量模式：更新最新数据
            tm.update_task(task_id, status='running',
                             message='正在获取最新龙虎榜数据...')

            count = db.save_dragon_list()

            tm.update_task(
                task_id,
                status='completed',
                message=f'增量更新完成，共保存 {count} 条龙虎榜记录'
            )

    except Exception as e:
        tm.update_task(
            task_id,
            status='failed',
            message=f'龙虎榜数据更新失败: {str(e)}'
        )
        raise


# Task type to handler mapping
TASK_HANDLERS = {
    'update_stock_prices': execute_update_stock_prices,
    'update_industry_classification': execute_update_industry_classification,
    'update_financial_reports': execute_update_financial_reports,
    'update_index_data': execute_update_index_data,
    'update_industry_statistics': execute_update_industry_statistics,  # 行业统计更新
    'update_moneyflow': execute_update_moneyflow,  # 资金流向数据更新
    'calculate_industry_moneyflow': execute_calculate_industry_moneyflow,  # 行业资金流向汇总计算
    'update_dragon_list': execute_update_dragon_list,  # 龙虎榜数据更新
    'test_handler': execute_test_handler,
    'train_quarterly_model': execute_train_quarterly_model,  # 季度模型训练
    'backtest': execute_backtest,  # 回测任务
    # Backward compatibility for old task types
    'update_all_stocks': execute_update_stock_prices,
    'update_favorites': execute_update_stock_prices,
}
