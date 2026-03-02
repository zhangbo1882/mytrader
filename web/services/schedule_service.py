"""
Schedule management business logic services
"""
from web.scheduler import get_scheduled_jobs, remove_scheduled_job, pause_scheduled_job, resume_scheduled_job, add_scheduled_job, update_scheduled_job


# 不同任务类型支持的 stock_range 选项
STOCK_RANGE_OPTIONS = {
    'stock': ['all', 'favorites', 'custom', 'market'],  # 股价更新支持 market
    'moneyflow': ['all', 'favorites', 'custom'],  # 资金流向不支持 market
    'financial': ['all', 'favorites', 'custom', 'market'],  # 财务数据支持 market
}


def validate_stock_range(content_type, stock_range):
    """
    验证并修正 stock_range 参数

    Args:
        content_type: 内容类型
        stock_range: 股票范围

    Returns:
        tuple: (valid_stock_range, error_message)
    """
    valid_options = STOCK_RANGE_OPTIONS.get(content_type, ['all', 'favorites', 'custom', 'market'])

    if stock_range not in valid_options:
        # 如果是 market 但不支持，自动转换为 all
        if stock_range == 'market' and 'market' not in valid_options:
            return 'all', None
        return None, f'无效的 stock_range: {stock_range}，必须是 {"/".join(valid_options)}'

    return stock_range, None


def get_jobs():
    """获取定时任务列表"""
    jobs = get_scheduled_jobs()
    return {'jobs': jobs}, 200


def create_job():
    """创建定时任务"""
    from flask import request

    try:
        data = request.json
        if not data:
            return {'error': '请求数据为空'}, 400

        name = data.get('name')
        task_type = data.get('task_type')
        trigger = data.get('trigger')

        if not all([name, task_type, trigger]):
            return {'error': '缺少必要参数：name, task_type, trigger'}, 400

        # 解析触发器配置
        cron_expression = trigger.get('cron_expression')
        if not cron_expression:
            return {'error': '缺少 cron_expression 参数'}, 400

        # 构建任务参数
        task_params = {}

        # 根据任务类型提取参数
        if data.get('content_type') == 'stock':
            stock_range = data.get('stock_range', 'all')
            stock_range, error = validate_stock_range('stock', stock_range)
            if error:
                return {'error': error}, 400
            task_params['stock_range'] = stock_range
            task_params['mode'] = data.get('mode', 'incremental')
            task_params['content_type'] = 'stock'
            # 按市场选择时，添加市场参数
            if stock_range == 'market':
                task_params['markets'] = data.get('markets', ['main'])
            task_params['exclude_st'] = data.get('exclude_st', True)
        elif data.get('content_type') == 'moneyflow':
            stock_range = data.get('stock_range', 'all')
            stock_range, error = validate_stock_range('moneyflow', stock_range)
            if error:
                return {'error': error}, 400
            task_params['stock_range'] = stock_range
            task_params['mode'] = data.get('mode', 'incremental')
            task_params['exclude_st'] = True
            task_params['content_type'] = 'moneyflow'
        elif data.get('content_type') == 'index':
            task_params['markets'] = data.get('markets', ['SSE', 'SZSE'])
            task_params['content_type'] = 'index'
        elif data.get('content_type') == 'industry':
            task_params['src'] = data.get('src', 'SW2021')
            task_params['force'] = data.get('force', False)
            task_params['content_type'] = 'industry'
        elif data.get('content_type') == 'financial':
            stock_range = data.get('stock_range', 'all')
            stock_range, error = validate_stock_range('financial', stock_range)
            if error:
                return {'error': error}, 400
            task_params['stock_range'] = stock_range
            task_params['include_indicators'] = data.get('include_indicators', True)
            task_params['include_reports'] = data.get('include_reports', True)
            task_params['content_type'] = 'financial'

        # 使用模块级函数字符串引用（APScheduler可以序列化）
        func_ref = 'web.scheduler:run_scheduled_task'

        # 清理name中的空格和特殊字符，创建唯一的job_id
        safe_name = name.replace(' ', '_').replace('-', '_')
        job_id = f"{task_type}_{safe_name}"

        # 添加定时任务
        job_id_result = add_scheduled_job(
            job_id=job_id,
            func=func_ref,
            cron_expression=cron_expression,
            name=name,
            func_kwargs={
                'task_type': task_type,
                'params': task_params
            }
        )

        return {
            'success': True,
            'job_id': job_id,
            'message': f'定时任务 "{name}" 已创建'
        }, 201

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'创建定时任务失败: {str(e)}'}, 500


def delete_job(job_id):
    """删除定时任务"""
    remove_scheduled_job(job_id)
    return {'message': '定时任务已删除'}, 200


def pause_job_service(job_id):
    """暂停定时任务"""
    pause_scheduled_job(job_id)
    return {'message': '定时任务已暂停'}, 200


def resume_job_service(job_id):
    """恢复定时任务"""
    resume_scheduled_job(job_id)
    return {'message': '定时任务已恢复'}, 200


def update_job(job_id):
    """更新定时任务"""
    from flask import request

    try:
        data = request.json
        if not data:
            return {'error': '请求数据为空'}, 400

        # 获取可选的更新字段
        name = data.get('name')
        task_type = data.get('task_type')
        trigger = data.get('trigger')
        cron_expression = trigger.get('cron_expression') if trigger else None

        # 构建任务参数（如果提供了任务相关字段）
        func_kwargs = None
        if task_type or data.get('content_type'):
            # 需要更新任务参数
            task_params = {}

            if data.get('content_type') == 'stock':
                stock_range = data.get('stock_range', 'all')
                stock_range, error = validate_stock_range('stock', stock_range)
                if error:
                    return {'error': error}, 400
                task_params['stock_range'] = stock_range
                task_params['mode'] = data.get('mode', 'incremental')
                task_params['content_type'] = 'stock'
                # 按市场选择时，添加市场参数
                if stock_range == 'market':
                    task_params['markets'] = data.get('markets', ['main'])
                task_params['exclude_st'] = data.get('exclude_st', True)
            elif data.get('content_type') == 'moneyflow':
                stock_range = data.get('stock_range', 'all')
                stock_range, error = validate_stock_range('moneyflow', stock_range)
                if error:
                    return {'error': error}, 400
                task_params['stock_range'] = stock_range
                task_params['mode'] = data.get('mode', 'incremental')
                task_params['exclude_st'] = True
                task_params['content_type'] = 'moneyflow'
            elif data.get('content_type') == 'index':
                task_params['markets'] = data.get('markets', ['SSE', 'SZSE'])
                task_params['content_type'] = 'index'
            elif data.get('content_type') == 'industry':
                task_params['src'] = data.get('src', 'SW2021')
                task_params['force'] = data.get('force', False)
                task_params['content_type'] = 'industry'
            elif data.get('content_type') == 'financial':
                stock_range = data.get('stock_range', 'all')
                stock_range, error = validate_stock_range('financial', stock_range)
                if error:
                    return {'error': error}, 400
                task_params['stock_range'] = stock_range
                task_params['include_indicators'] = data.get('include_indicators', True)
                task_params['include_reports'] = data.get('include_reports', True)
                task_params['content_type'] = 'financial'

            # 如果提供了 task_type，使用它；否则根据 content_type 推断
            if not task_type:
                content_type = data.get('content_type', 'stock')
                task_type_map = {
                    'stock': 'update_stock_prices',
                    'hk': 'update_hk_prices',
                    'index': 'update_index_data',
                    'industry': 'update_industry_classification',
                    'financial': 'update_financial_reports',
                    'statistics': 'update_industry_statistics',
                    'moneyflow': 'update_moneyflow',
                    'dragon_list': 'update_dragon_list',
                }
                task_type = task_type_map.get(content_type, 'update_stock_prices')

            func_kwargs = {
                'task_type': task_type,
                'params': task_params
            }

        # 调用更新函数
        result = update_scheduled_job(
            job_id=job_id,
            func='web.scheduler:run_scheduled_task' if func_kwargs else None,
            cron_expression=cron_expression,
            func_kwargs=func_kwargs,
            name=name
        )

        if result:
            return {
                'success': True,
                'job_id': job_id,
                'message': f'定时任务 "{name or job_id}" 已更新'
            }, 200
        else:
            return {'error': '更新定时任务失败，任务可能不存在'}, 404

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'更新定时任务失败: {str(e)}'}, 500
