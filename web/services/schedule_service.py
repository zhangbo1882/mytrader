"""
Schedule management business logic services
"""
from web.scheduler import get_scheduled_jobs, remove_scheduled_job, pause_scheduled_job, resume_scheduled_job, add_scheduled_job


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
            task_params['stock_range'] = data.get('stock_range', 'all')
            task_params['mode'] = data.get('mode', 'incremental')
            task_params['content_type'] = 'stock'
        elif data.get('content_type') == 'index':
            task_params['markets'] = data.get('markets', ['SSE', 'SZSE'])
            task_params['content_type'] = 'index'
        elif data.get('content_type') == 'industry':
            task_params['src'] = data.get('src', 'SW2021')
            task_params['force'] = data.get('force', False)
            task_params['content_type'] = 'industry'
        elif data.get('content_type') == 'financial':
            task_params['stock_range'] = data.get('stock_range', 'all')
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
