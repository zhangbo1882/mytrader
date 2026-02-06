"""
Task Creation Service

Handles task creation and routing to appropriate handlers.
This service validates task parameters and creates tasks using the TaskManager.
Tasks are created with status='pending' and will be picked up by the Worker Service.
"""
from web.services.task_service import get_task_manager


def create_task(request_data):
    """
    Create a new task based on task_type and params.

    Tasks are created with status='pending' and will be executed by the Worker Service.

    Args:
        request_data: Dictionary containing 'task_type' and 'params'

    Returns:
        tuple: (response_dict, status_code)
    """
    task_type = request_data.get('task_type')
    params = request_data.get('params', {})

    if not task_type:
        return {'error': '缺少 task_type 参数'}, 400

    # Route to appropriate handler based on task_type
    if task_type == 'update_stock_prices':
        return handle_update_stock_prices(params)
    elif task_type == 'update_industry_classification':
        return handle_update_industry_classification(params)
    elif task_type == 'update_financial_reports':
        return handle_update_financial_reports(params)
    elif task_type == 'update_index_data':
        return handle_update_index_data(params)
    elif task_type == 'update_industry_statistics':
        return handle_update_industry_statistics(params)
    elif task_type == 'test_handler':
        return handle_test_handler(params)
    else:
        return {'error': f'未知的任务类型: {task_type}'}, 400


def handle_update_stock_prices(params):
    """
    Handle stock price update task (merged update_all_stocks + update_favorites).

    Args:
        params: Dictionary with keys:
            - stock_range: "all" | "favorites" | "custom" (default: "all")
            - custom_stocks: List of stock codes (required when stock_range="custom")
    """
    stock_range = params.get('stock_range', 'all')
    custom_stocks = params.get('custom_stocks', [])

    # Validate parameters
    if stock_range not in ['all', 'favorites', 'custom']:
        return {'error': f'无效的 stock_range: {stock_range}，必须是 all/favorites/custom'}, 400

    if stock_range == 'custom':
        if not custom_stocks or not isinstance(custom_stocks, list):
            return {'error': '当 stock_range="custom" 时，custom_stocks 必须是非空列表'}, 400

    # Build task parameters (always incremental mode now)
    task_params = {
        'mode': 'incremental',
        'stock_range': stock_range,
        'custom_stocks': custom_stocks
    }

    # If favorites, get favorites list from default config
    if stock_range == 'favorites':
        task_params['stocks'] = ["600382", "600711", "000001"]  # Default favorites

    # Create task with status='pending' - Worker will pick it up
    tm = get_task_manager()
    task_id = tm.create_task(
        task_type='update_stock_prices',
        params=task_params
    )
    tm.update_task(task_id, message='任务已创建，等待Worker执行...')

    return {
        'success': True,
        'task_id': task_id,
        'status': 'pending',
        'message': f'股票价格更新任务已创建，stock_range={stock_range}'
    }, 201


def handle_update_industry_classification(params):
    """
    Handle SW industry classification update task.

    Args:
        params: Dictionary with keys:
            - src: "SW2021" | "SW2014" (default: "SW2021")
            - force: Force re-fetch (default: False)
    """
    src = params.get('src', 'SW2021')
    force = params.get('force', False)

    # Validate parameters
    if src not in ['SW2021', 'SW2014']:
        return {'error': f'无效的 src: {src}，必须是 SW2021/SW2014'}, 400

    # Build task parameters
    task_params = {
        'src': src,
        'force': force
    }

    # Create task with status='pending' - Worker will pick it up
    tm = get_task_manager()
    task_id = tm.create_task(
        task_type='update_industry_classification',
        params=task_params
    )
    tm.update_task(task_id, message=f'任务已创建，等待Worker执行...')

    return {
        'success': True,
        'task_id': task_id,
        'status': 'pending',
        'message': f'申万行业分类更新任务已创建，src={src}'
    }, 201


def handle_update_financial_reports(params):
    """
    Handle financial reports update task.

    Args:
        params: Dictionary with keys:
            - stock_range: "all" | "favorites" | "custom" (default: "all")
            - custom_stocks: List of stock codes (required when stock_range="custom")
            - include_indicators: Include fina_indicator table (default: True)
            - include_reports: Include income/balance/cashflow (default: True)
    """
    stock_range = params.get('stock_range', 'all')
    custom_stocks = params.get('custom_stocks', [])
    include_indicators = params.get('include_indicators', True)
    include_reports = params.get('include_reports', True)

    # Validate parameters
    if stock_range not in ['all', 'favorites', 'custom']:
        return {'error': f'无效的 stock_range: {stock_range}，必须是 all/favorites/custom'}, 400

    if stock_range == 'custom':
        if not custom_stocks or not isinstance(custom_stocks, list):
            return {'error': '当 stock_range="custom" 时，custom_stocks 必须是非空列表'}, 400

    # Build task parameters
    task_params = {
        'stock_range': stock_range,
        'custom_stocks': custom_stocks,
        'include_indicators': include_indicators,
        'include_reports': include_reports
    }

    # If favorites, get favorites list
    if stock_range == 'favorites':
        task_params['stocks'] = ["600382", "600711", "000001"]  # Default favorites

    # Create task with status='pending' - Worker will pick it up
    tm = get_task_manager()
    task_id = tm.create_task(
        task_type='update_financial_reports',
        params=task_params
    )
    tm.update_task(task_id, message='任务已创建，等待Worker执行...')

    return {
        'success': True,
        'task_id': task_id,
        'status': 'pending',
        'message': f'财务报表更新任务已创建，stock_range={stock_range}'
    }, 201


def handle_update_index_data(params):
    """
    Handle index data update task.

    Args:
        params: Dictionary with keys:
            - markets: List of markets ["SSE", "SZSE"] (default: ["SSE", "SZSE"])
    """
    markets = params.get('markets', ['SSE', 'SZSE'])

    # Validate parameters
    valid_markets = ['SSE', 'SZSE']
    for market in markets:
        if market not in valid_markets:
            return {'error': f'无效的市场代码: {market}，必须是 SSE/SZSE'}, 400

    # Build task parameters
    task_params = {
        'markets': markets
    }

    # Create task with status='pending' - Worker will pick it up
    tm = get_task_manager()
    task_id = tm.create_task(
        task_type='update_index_data',
        params=task_params
    )
    tm.update_task(task_id, message='任务已创建，等待Worker执行...')

    return {
        'success': True,
        'task_id': task_id,
        'status': 'pending',
        'message': f'指数数据更新任务已创建，markets={",".join(markets)}'
    }, 201


def handle_test_handler(params):
    """
    Handle test handler task for testing worker functionality.

    Args:
        params: Dictionary with keys:
            - total_items: int (default: 100) - Total items to process
            - item_duration_ms: int (default: 100) - Processing time per item (ms)
            - checkpoint_interval: int (default: 10) - Checkpoint save interval
            - failure_rate: float (default: 0.0) - Random failure rate (0.0-1.0)
            - simulate_pause: bool (default: False) - Auto-pause at 50%
    """
    # Extract parameters with defaults
    total_items = params.get('total_items', 100)
    item_duration_ms = params.get('item_duration_ms', 100)
    checkpoint_interval = params.get('checkpoint_interval', 10)
    failure_rate = params.get('failure_rate', 0.0)
    simulate_pause = params.get('simulate_pause', False)

    # Validate parameters
    if not isinstance(total_items, int) or total_items <= 0:
        return {'error': 'total_items must be a positive integer'}, 400

    if not isinstance(item_duration_ms, int) or item_duration_ms < 0:
        return {'error': 'item_duration_ms must be a non-negative integer'}, 400

    if not isinstance(checkpoint_interval, int) or checkpoint_interval <= 0:
        return {'error': 'checkpoint_interval must be a positive integer'}, 400

    if not isinstance(failure_rate, (int, float)) or not (0.0 <= failure_rate <= 1.0):
        return {'error': 'failure_rate must be a number between 0.0 and 1.0'}, 400

    if not isinstance(simulate_pause, bool):
        return {'error': 'simulate_pause must be a boolean'}, 400

    # Build task parameters
    task_params = {
        'total_items': total_items,
        'item_duration_ms': item_duration_ms,
        'checkpoint_interval': checkpoint_interval,
        'failure_rate': failure_rate,
        'simulate_pause': simulate_pause
    }

    # Create task with status='pending' - Worker will pick it up
    tm = get_task_manager()
    task_id = tm.create_task(
        task_type='test_handler',
        params=task_params
    )
    tm.update_task(task_id, message='测试任务已创建，等待Worker执行...')

    return {
        'success': True,
        'task_id': task_id,
        'status': 'pending',
        'message': f'测试任务已创建，total_items={total_items}'
    }, 201


def handle_update_industry_statistics(params):
    """
    Handle industry statistics update task.

    Args:
        params: Dictionary with keys:
            - metrics: List of metric names (default: ["pe_ttm", "pb", "ps_ttm", "total_mv", "circ_mv"])
    """
    metrics = params.get('metrics', ['pe_ttm', 'pb', 'ps_ttm', 'total_mv', 'circ_mv'])

    # Validate parameters
    valid_metrics = ['pe_ttm', 'pb', 'ps_ttm', 'total_mv', 'circ_mv']
    if not metrics or not isinstance(metrics, list):
        return {'error': 'metrics 必须是非空列表'}, 400

    for metric in metrics:
        if metric not in valid_metrics:
            return {'error': f'无效的指标: {metric}，有效值为: {", ".join(valid_metrics)}'}, 400

    # Build task parameters
    task_params = {
        'metrics': metrics
    }

    # Create task with status='pending' - Worker will pick it up
    tm = get_task_manager()
    task_id = tm.create_task(
        task_type='update_industry_statistics',
        params=task_params
    )
    tm.update_task(task_id, message=f'任务已创建，等待Worker执行...')

    return {
        'success': True,
        'task_id': task_id,
        'status': 'pending',
        'message': f'行业统计更新任务已创建，metrics={len(metrics)}个指标'
    }, 201

