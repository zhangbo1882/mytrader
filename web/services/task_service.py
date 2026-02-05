"""
Task management business logic services
"""
from flask import request, jsonify
from web.tasks import init_task_manager
from config.settings import CHECKPOINT_DIR, TASKS_DB_PATH


def get_task_manager():
    """
    Get the global task manager instance, initializing if necessary.
    This ensures we always have the correct instance from tasks.py
    """
    from web import tasks
    if tasks.task_manager is None:
        tm = init_task_manager(
            db_path=str(TASKS_DB_PATH),
            checkpoint_dir=CHECKPOINT_DIR
        )
        # Update the module's global variable
        tasks.task_manager = tm
    return tasks.task_manager


def list_tasks(status_filter=None):
    """获取任务列表"""
    tm = get_task_manager()
    return tm.list_tasks(status_filter)


def get_task(task_id):
    """获取任务详情"""
    tm = get_task_manager()
    task = tm.get_task(task_id)
    if not task:
        return None, 404
    return task, 200


def delete_task(task_id):
    """删除任务"""
    tm = get_task_manager()
    tm.delete_task(task_id)
    return {'message': '任务已删除'}, 200


def cancel_task(task_id):
    """取消任务"""
    tm = get_task_manager()
    tm.cancel_task(task_id)
    return {'message': '任务已取消'}, 200


def pause_task(task_id):
    """暂停任务"""
    tm = get_task_manager()
    tm.request_pause(task_id)
    return {'message': '任务已暂停'}, 200


def resume_task(task_id):
    """恢复任务"""
    tm = get_task_manager()
    success = tm.resume_task(task_id)
    if success:
        return {'message': '任务已恢复'}, 200
    else:
        return {'error': '无法恢复任务，任务可能不在暂停状态'}, 400


def stop_task(task_id):
    """停止任务"""
    tm = get_task_manager()
    tm.request_stop(task_id)
    return {'message': '任务已停止'}, 200


def cleanup_tasks():
    """清理陈旧任务"""
    tm = get_task_manager()
    tm.cleanup_stale_tasks(stale_threshold_hours=24)
    return {'message': '任务已清理'}, 200


def active_check():
    """检查是否有活跃任务"""
    tm = get_task_manager()
    has_active, task_info = tm.has_active_task()
    return {'has_active': has_active, 'task': task_info}, 200
