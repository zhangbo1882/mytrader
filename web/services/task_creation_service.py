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
    elif task_type == 'update_hk_prices':
        return handle_update_hk_prices(params)
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
    elif task_type == 'update_a_share_batch':
        return handle_update_a_share_batch(params)
    elif task_type == 'update_hk_batch':
        return handle_update_hk_batch(params)
    elif task_type == 'update_valuation_prior':
        return handle_update_valuation_prior(params)
    elif task_type == 'update':
        # 通用更新任务，根据 params.content_type 分发
        content_type = params.get('content_type', 'stock')
        if content_type == 'stock':
            return handle_update_stock_prices(params)
        elif content_type == 'hk':
            return handle_update_hk_prices(params)
        elif content_type == 'industry':
            return handle_update_industry_classification(params)
        elif content_type == 'financial':
            return handle_update_financial_reports(params)
        elif content_type == 'index':
            return handle_update_index_data(params)
        elif content_type == 'statistics':
            return handle_update_industry_statistics(params)
        elif content_type == 'moneyflow':
            return handle_update_moneyflow(params)
        elif content_type == 'dragon_list':
            return handle_update_dragon_list(params)
        else:
            return {'error': f'未知的 content_type: {content_type}'}, 400
    else:
        return {'error': f'未知的任务类型: {task_type}'}, 400


def handle_update_stock_prices(params):
    """
    Handle stock price update task.

    全市场更新自动使用批量模式，收藏/自定义列表使用逐股票更新。

    Args:
        params: Dictionary with keys:
            - stock_range: "all" | "favorites" | "custom" | "market" (default: "all")
            - custom_stocks: List of stock codes (required when stock_range="custom")
            - markets: List of market types ["main", "gem", "star", "bse"] (required when stock_range="market")
            - exclude_st: Exclude ST stocks (default: True)
            - mode: "incremental" | "full" (default: "incremental")
            - start_date: Start date YYYY-MM-DD (optional, for full mode)
            - end_date: End date YYYY-MM-DD (optional)
    """
    stock_range = params.get('stock_range') or 'all'
    custom_stocks = params.get('custom_stocks', [])
    markets = params.get('markets', [])
    exclude_st = params.get('exclude_st', True)
    mode = params.get('mode') or 'incremental'
    start_date = params.get('start_date')
    end_date = params.get('end_date')

    # Validate parameters
    if stock_range not in ['all', 'favorites', 'custom', 'market']:
        return {'error': f'无效的 stock_range: {stock_range}，必须是 all/favorites/custom/market'}, 400

    if mode not in ['incremental', 'full']:
        return {'error': f'无效的 mode: {mode}，必须是 incremental/full'}, 400

    if stock_range == 'custom':
        if not custom_stocks or not isinstance(custom_stocks, list):
            return {'error': '当 stock_range="custom" 时，custom_stocks 必须是非空列表'}, 400

    if stock_range == 'market':
        if not markets or not isinstance(markets, list):
            return {'error': '当 stock_range="market" 时，markets 必须是非空列表'}, 400
        valid_markets = ['main', 'gem', 'star', 'bse']
        for m in markets:
            if m not in valid_markets:
                return {'error': f'无效的市场类型: {m}，必须是 {"/".join(valid_markets)}'}, 400

    tm = get_task_manager()
    created_tasks = []

    # If favorites, separate A-shares and HK stocks and create separate tasks
    if stock_range == 'favorites':
        # Get favorites list (混合了A股和港股)
        all_favorites = custom_stocks if custom_stocks else ["600382", "600711", "000001", "00762.HK", "00941.HK"]

        # 分离A股和港股
        a_share_stocks = []
        hk_stocks = []

        for stock in all_favorites:
            # 港股判断：包含.HK后缀 或者是4-5位数字且不在A股代码范围内
            if '.HK' in stock.upper():
                hk_stocks.append(stock)
            elif '.' not in stock:
                # 纯数字代码，判断是A股还是港股
                if len(stock) in [4, 5]:
                    # 检查是否是A股代码前缀
                    if stock.startswith(('600', '601', '603', '604', '605', '688', '689',  # 上交所
                                      '000', '001', '002', '003', '300', '301')):  # 深交所
                        a_share_stocks.append(stock)
                    else:
                        # 默认为港股
                        hk_stocks.append(stock)
                else:
                    # 6位是A股
                    a_share_stocks.append(stock)

        # 创建A股更新任务（逐股票模式，因为是收藏列表）
        if a_share_stocks:
            a_task_params = {
                'mode': mode,
                'stock_range': 'custom',
                'custom_stocks': a_share_stocks,
                'start_date': start_date,
                'end_date': end_date,
                'exclude_st': exclude_st,
            }
            a_task_id = tm.create_task(
                task_type='update_stock_prices',
                params=a_task_params
            )
            task_msg = f'A股股价更新 ({len(a_share_stocks)}只股票)'
            tm.update_task(a_task_id, message=task_msg)
            created_tasks.append({
                'task_id': a_task_id,
                'market': 'A股',
                'market_type': 'A-share',
                'count': len(a_share_stocks),
                'stocks': a_share_stocks[:10],  # 显示前10个
                'message': task_msg
            })

        # 创建港股更新任务（逐股票模式，因为是收藏列表）
        if hk_stocks:
            hk_task_params = {
                'mode': mode,
                'stock_range': 'custom',
                'custom_stocks': hk_stocks,
                'start_date': start_date,
                'end_date': end_date,
            }
            hk_task_id = tm.create_task(
                task_type='update_hk_prices',
                params=hk_task_params
            )
            task_msg = f'港股股价更新 ({len(hk_stocks)}只股票)'
            tm.update_task(hk_task_id, message=task_msg)
            created_tasks.append({
                'task_id': hk_task_id,
                'market': '港股',
                'market_type': 'HK',
                'count': len(hk_stocks),
                'stocks': hk_stocks[:10],  # 显示前10个
                'message': task_msg
            })

        if not created_tasks:
            return {'error': '收藏列表中没有有效的股票代码'}, 400

        # 构建详细的消息
        task_summary = ', '.join([f"{t['market']}{t['count']}只" for t in created_tasks])
        return {
            'success': True,
            'tasks': created_tasks,
            'message': f'已创建 {len(created_tasks)} 个更新任务: {task_summary}'
        }, 201

    # 全市场或自定义列表：自定义列表用逐股票，全市场用批量
    if stock_range == 'custom':
        # 自定义列表：逐股票更新
        task_params = {
            'mode': mode,
            'stock_range': stock_range,
            'custom_stocks': custom_stocks,
            'start_date': start_date,
            'end_date': end_date,
            'exclude_st': exclude_st,
        }
        task_id = tm.create_task(
            task_type='update_stock_prices',
            params=task_params
        )
        tm.update_task(task_id, message=f'A股数据更新任务已创建 ({len(custom_stocks)}只股票)...')

        return {
            'success': True,
            'task_id': task_id,
            'status': 'pending',
            'message': f'A股数据更新任务已创建，{len(custom_stocks)}只股票'
        }, 201

    # 全市场更新：自动使用批量模式
    task_params = {
        'mode': mode,
        'stock_range': stock_range,
        'markets': markets,
        'exclude_st': exclude_st,
    }

    # 计算回溯天数
    days_back = 1
    if mode == 'full' and start_date:
        from datetime import datetime
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, datetime.now().strftime('%Y-%m-%d')) if end_date else datetime.now()
            days_back = max(1, (end - start).days + 1)
        except:
            days_back = 30  # 默认30天

    task_params['days_back'] = days_back
    task_id = tm.create_task(
        task_type='update_a_share_batch',
        params=task_params
    )

    market_info = f', markets={",".join(markets)}' if stock_range == 'market' else ''
    tm.update_task(task_id, message=f'A股批量更新任务已创建{market_info}...')

    return {
        'success': True,
        'task_id': task_id,
        'status': 'pending',
        'message': f'A股批量更新任务已创建，stock_range={stock_range}{market_info}，回溯{days_back}天'
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

    # If favorites, use the custom_stocks passed from frontend (actual favorites list)
    if stock_range == 'favorites':
        # Frontend passes the actual favorites list in custom_stocks
        # Only use default if no stocks were provided
        if custom_stocks:
            task_params['stocks'] = custom_stocks
        else:
            task_params['stocks'] = ["600382", "600711", "000001"]  # Fallback defaults

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
    from src.strategies.base.registry import (
        validate_strategy_params,
        get_strategy_class
    )

    # 提取回测共有参数
    stock_code = params.get('stock')
    start_date = params.get('start_date')
    end_date = params.get('end_date')
    interval = params.get('interval', '1d')  # 默认日线
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
        'interval': interval,  # 添加 interval 参数
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
        'price_breakout': '价格突破策略',
        'price_breakout_v2': '价格突破策略V2'
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
    # 注意：资金流向不支持 market，自动转换为 all
    valid_stock_ranges = ['all', 'favorites', 'custom']
    if stock_range == 'market':
        stock_range = 'all'  # 自动转换
    elif stock_range not in valid_stock_ranges:
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


def handle_update_hk_prices(params):
    """
    Handle HK stock price update task creation.

    Args:
        params: Dictionary with keys:
            - stock_range: "all" | "favorites" | "custom" (default: "all")
            - custom_stocks: List of stock codes (required when stock_range="custom")
            - mode: "incremental" | "full" (default: "incremental")
            - start_date: Start date YYYY-MM-DD (optional, for full mode)
            - end_date: End date YYYY-MM-DD (optional)
    """
    stock_range = params.get('stock_range') or 'all'
    custom_stocks = params.get('custom_stocks', [])
    mode = params.get('mode') or 'incremental'
    start_date = params.get('start_date')
    end_date = params.get('end_date')

    # Validate parameters
    if stock_range not in ['all', 'favorites', 'custom']:
        return {'error': f'无效的 stock_range: {stock_range}，必须是 all/favorites/custom'}, 400

    if mode not in ['incremental', 'full']:
        return {'error': f'无效的 mode: {mode}，必须是 incremental/full'}, 400

    if stock_range == 'custom':
        if not custom_stocks or not isinstance(custom_stocks, list):
            return {'error': '当 stock_range="custom" 时，custom_stocks 必须是非空列表'}, 400

    # 智能选择批量更新模式：增量更新且全市场时，自动使用批量模式
    actual_task_type = 'update_hk_prices'
    if mode == 'incremental' and stock_range == 'all':
        actual_task_type = 'update_hk_batch'

    # Build task parameters
    task_params = {
        'mode': mode,
        'stock_range': stock_range,
        'custom_stocks': custom_stocks,
        'start_date': start_date,
        'end_date': end_date,
    }

    # Note: When stock_range='favorites', custom_stocks should contain the HK stocks from favorites.
    # The frontend is responsible for extracting HK stocks from favorites and passing them in custom_stocks.
    # The worker's get_hk_stock_list_for_task function will use these when stock_range='favorites' or 'custom'.

    # Create task with status='pending' - Worker will pick it up
    tm = get_task_manager()
    task_id = tm.create_task(
        task_type=actual_task_type,
        params=task_params
    )
    tm.update_task(task_id, message='港股数据更新任务已创建，等待Worker执行...')

    task_type_desc = '批量更新' if actual_task_type == 'update_hk_batch' else '逐股票更新'
    return {
        'success': True,
        'task_id': task_id,
        'status': 'pending',
        'message': f'港股价格更新任务已创建({task_type_desc})，stock_range={stock_range}'
    }, 201


def handle_update_a_share_batch(params):
    """
    Handle A-share batch update task (efficient mode).

    This task uses the batch API to fetch all A-share data for a given date
    in a single API call, which is 1000x faster than per-stock fetching.

    Args:
        params: Dictionary with keys:
            - trade_date: Specific date YYYYMMDD (optional, default: today)
            - days_back: Number of days to look back (optional, default: 1)
    """
    from web.services.task_service import get_task_manager

    trade_date = params.get('trade_date')
    days_back = params.get('days_back', 1)

    tm = get_task_manager()

    task_params = {
        'trade_date': trade_date,
        'days_back': days_back,
    }

    task_id = tm.create_task(
        task_type='update_a_share_batch',
        params=task_params
    )

    return {
        'success': True,
        'task_id': task_id,
        'status': 'pending',
        'message': f'A股批量更新任务已创建，days_back={days_back}'
    }, 201


def handle_update_hk_batch(params):
    """
    Handle HK stock batch update task (efficient mode).

    This task uses the batch API to fetch all HK stock data for a given date
    in a single API call.

    Args:
        params: Dictionary with keys:
            - trade_date: Specific date YYYYMMDD (optional, default: today)
            - days_back: Number of days to look back (optional, default: 1)
    """
    from web.services.task_service import get_task_manager

    trade_date = params.get('trade_date')
    days_back = params.get('days_back', 1)

    tm = get_task_manager()

    task_params = {
        'trade_date': trade_date,
        'days_back': days_back,
    }

    task_id = tm.create_task(
        task_type='update_hk_batch',
        params=task_params
    )

    return {
        'success': True,
        'task_id': task_id,
        'status': 'pending',
        'message': f'港股批量更新任务已创建，days_back={days_back}'
    }, 201


def handle_update_valuation_prior(params):
    """
    处理贝叶斯估值先验矩阵更新任务创建

    Args:
        params: 任务参数:
            - years: 回测年数（默认: 3）
            - level: 回测粒度 ('L1'|'L2'|'both'，默认: 'both')
            - industry_codes: 行业代码列表（默认: None=全部，仅L1）
            - min_stocks: L2行业最少股票数（默认: 15）

    Returns:
        tuple: (response_dict, status_code)
    """
    years = params.get('years', 3)
    level = params.get('level', 'both')
    industry_codes = params.get('industry_codes')
    min_stocks = params.get('min_stocks', 15)

    # 参数验证
    if not isinstance(years, int) or years <= 0 or years > 10:
        return {'error': 'years 必须是 1-10 之间的整数'}, 400
    if level not in ('L1', 'L2', 'both'):
        return {'error': "level 必须是 'L1'、'L2' 或 'both'"}, 400

    task_params = {
        'years': years,
        'level': level,
        'industry_codes': industry_codes,
        'min_stocks': min_stocks,
    }

    tm = get_task_manager()
    task_id = tm.create_task(
        task_type='update_valuation_prior',
        params=task_params
    )
    tm.update_task(task_id, message='贝叶斯先验矩阵更新任务已创建，等待Worker执行...')

    ind_desc = f'{len(industry_codes)}个行业' if industry_codes else '全部行业'
    return {
        'success': True,
        'task_id': task_id,
        'status': 'pending',
        'message': f'贝叶斯先验矩阵更新任务已创建，{ind_desc}，years={years}'
    }, 201
