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
    elif task_type == 'update_moneyflow':
        return handle_update_moneyflow(params)
    elif task_type == 'calculate_industry_moneyflow':
        return handle_calculate_industry_moneyflow(params)
    elif task_type == 'update_dragon_list':
        return handle_update_dragon_list(params)
    elif task_type == 'test_handler':
        return handle_test_handler(params)
    elif task_type == 'backtest':
        return handle_backtest(params)
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


def handle_backtest(params):
    """
    Handle backtest task (支持多策略).

    Args:
        params: Dictionary with keys:
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
    from src.strategies.registry import (
        validate_strategy_params,
        get_strategy_class
    )

    # 提取回测共有参数
    stock_code = params.get('stock')
    start_date = params.get('start_date')
    end_date = params.get('end_date')
    cash = params.get('cash', 1000000)
    commission = params.get('commission', 0.0002)
    benchmark = params.get('benchmark')

    # 提取策略参数
    strategy_type = params.get('strategy', 'sma_cross')
    strategy_params = params.get('strategy_params', {})

    # 验证回测共有参数
    if not stock_code:
        return {'error': '缺少必需参数: stock'}, 400

    if not start_date:
        return {'error': '缺少必需参数: start_date'}, 400

    # 验证策略类型
    strategy_class = get_strategy_class(strategy_type)
    if not strategy_class:
        return {'error': f'不支持的策略类型: {strategy_type}'}, 400

    # 验证策略参数
    is_valid, error = validate_strategy_params(strategy_type, strategy_params)
    if not is_valid:
        return {'error': f'策略参数验证失败: {error}'}, 400

    # 构建任务参数
    task_params = {
        'stock': stock_code,
        'start_date': start_date,
        'end_date': end_date,
        'cash': cash,
        'commission': commission,
        'benchmark': benchmark,
        'strategy': strategy_type,
        'strategy_params': strategy_params
    }

    # 创建任务
    tm = get_task_manager()
    task_id = tm.create_task(
        task_type='backtest',
        params=task_params
    )
    tm.update_task(task_id, message=f'回测任务已创建，等待Worker执行...')

    strategy_name = {
        'sma_cross': 'SMA交叉策略',
        'price_breakout': '价格突破策略'
    }.get(strategy_type, strategy_type)

    return {
        'success': True,
        'task_id': task_id,
        'status': 'pending',
        'message': f'{strategy_name}回测任务已创建，股票: {stock_code}'
    }, 201


def handle_update_moneyflow(params):
    """
    Handle moneyflow data update task.
    只负责获取个股资金流向数据，不包含行业汇总计算。

    Args:
        params: Dictionary with keys:
            - mode: "incremental" | "full" (默认: "incremental")
            - stock_range: "all" | "favorites" | "custom" (默认: "all")
            - custom_stocks: 自定义股票列表 (当 stock_range="custom" 时必需)
            - start_date: 开始日期 YYYYMMDD (可选)
            - end_date: 结束日期 YYYYMMDD (可选)
            - exclude_st: 是否排除ST股 (默认 True)
    """
    mode = params.get('mode', 'incremental')
    stock_range = params.get('stock_range', 'all')
    custom_stocks = params.get('custom_stocks', [])

    # 验证 mode 参数
    if mode not in ['incremental', 'full']:
        return {'error': f'无效的 mode: {mode}，必须是 incremental/full'}, 400

    # 验证 stock_range 参数
    if stock_range not in ['all', 'favorites', 'custom']:
        return {'error': f'无效的 stock_range: {stock_range}，必须是 all/favorites/custom'}, 400

    if stock_range == 'custom':
        if not custom_stocks or not isinstance(custom_stocks, list):
            return {'error': '当 stock_range="custom" 时，custom_stocks 必须是非空列表'}, 400

    tm = get_task_manager()
    task_params = {
        'mode': mode,
        'stock_range': stock_range,
        'custom_stocks': custom_stocks,
        'start_date': params.get('start_date'),
        'end_date': params.get('end_date'),
        'exclude_st': params.get('exclude_st', True)  # 默认排除ST股
    }

    task_id = tm.create_task(
        task_type='update_moneyflow',
        params=task_params
    )

    mode_text = '增量' if mode == 'incremental' else '全量'
    return {
        'task_id': task_id,
        'message': f'资金流向数据更新任务已创建，{mode_text}更新，stock_range={stock_range}'
    }, 201


def handle_calculate_industry_moneyflow(params):
    """
    Handle industry moneyflow summary calculation task.

    Args:
        params: Dictionary with keys:
            - start_date: 开始日期 YYYYMMDD (可选，默认从最早日期开始)
            - end_date: 结束日期 YYYYMMDD (可选，默认到最新日期)
    """
    start_date = params.get('start_date')
    end_date = params.get('end_date')

    tm = get_task_manager()
    task_params = {
        'start_date': start_date,
        'end_date': end_date,
    }

    task_id = tm.create_task(
        task_type='calculate_industry_moneyflow',
        params=task_params
    )

    return {
        'task_id': task_id,
        'message': '行业资金流向汇总计算任务已创建'
    }, 201


def handle_update_dragon_list(params):
    """
    处理龙虎榜数据更新任务创建

    Args:
        params: dict containing:
            - start_date: 开始日期 YYYY-MM-DD（可选，用于历史回填）
            - end_date: 结束日期 YYYY-MM-DD（可选）
            - mode: 'incremental'（增量）或 'batch'（批量回填）

    Returns:
        JSON response
    """
    try:
        mode = params.get('mode', 'incremental')
        start_date = params.get('start_date')
        end_date = params.get('end_date')

        tm = get_task_manager()
        task_params = {
            'mode': mode,
            'start_date': start_date,
            'end_date': end_date
        }

        task_id = tm.create_task(
            task_type='update_dragon_list',
            params=task_params
        )
        tm.update_task(task_id, message='龙虎榜数据更新任务已创建，等待执行')

        return {
            'success': True,
            'task_id': task_id,
            'message': '龙虎榜数据更新任务已创建'
        }, 200

    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }, 500

