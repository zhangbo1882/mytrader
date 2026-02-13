#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
回测历史记录服务

提供回测历史记录的查询、详情查看和删除功能
"""
import sqlite3
import json
import logging
from flask import request
from web.services.task_service import get_task_manager
from src.strategies.registry import get_strategy_description
from src.utils.stock_lookup import get_stock_name_from_code

logger = logging.getLogger(__name__)


def get_backtest_history():
    """
    查询所有task_type='backtest'且status='completed'的任务

    查询参数:
        - page: 页码（默认1）
        - page_size: 每页数量（默认20）
        - stock: 按股票代码筛选（可选）
        - strategy: 按策略类型筛选（可选）

    Returns:
        {
            "success": true,
            "total": 100,
            "history": [...]
        }
    """
    try:
        # 获取查询参数
        page = request.args.get('page', 1, type=int)
        page_size = request.args.get('page_size', 20, type=int)
        stock_filter = request.args.get('stock', '').strip()
        strategy_filter = request.args.get('strategy', '').strip()

        # 获取task_manager
        tm = get_task_manager()
        conn = sqlite3.connect(tm.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 构建查询条件
        where_conditions = ["task_type = 'backtest'", "status = 'completed'"]
        params = []

        if stock_filter:
            where_conditions.append("json_extract(params, '$.stock') = ?")
            params.append(stock_filter)

        if strategy_filter:
            where_conditions.append("json_extract(params, '$.strategy') = ?")
            params.append(strategy_filter)

        where_clause = " AND ".join(where_conditions)

        # 查询总数
        count_query = f"SELECT COUNT(*) as total FROM tasks WHERE {where_clause}"
        cursor.execute(count_query, params)
        total_row = cursor.fetchone()
        total = total_row['total'] if total_row else 0

        # 查询历史记录（分页）
        offset = (page - 1) * page_size
        query = f'''
            SELECT
                task_id,
                params,
                result,
                created_at,
                completed_at,
                json_extract(params, '$.stock') as stock,
                json_extract(params, '$.strategy') as strategy,
                json_extract(result, '$.basic_info.total_return') as total_return,
                json_extract(result, '$.health_metrics.sharpe_ratio') as sharpe_ratio,
                json_extract(result, '$.health_metrics.max_drawdown') as max_drawdown
            FROM tasks
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        '''
        params.extend([page_size, offset])
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        # 构建返回数据
        history = []
        for row in rows:
            row_dict = dict(row)

            # 解析JSON字段
            params_dict = {}
            if row_dict.get('params'):
                try:
                    params_dict = json.loads(row_dict['params'])
                except (json.JSONDecodeError, TypeError):
                    pass

            # 获取策略名称
            strategy = row_dict.get('strategy', '')
            strategy_info = get_strategy_description(strategy)
            strategy_name = strategy_info.get('name', strategy) if strategy_info else strategy

            # 获取股票名称
            stock = row_dict.get('stock', '')
            stock_name = get_stock_name_from_code(stock)

            # 生成记录名称: {stock_name}-{strategy}-{created_at}
            created_at = row_dict.get('created_at', '')
            name = f"{stock_name}-{strategy}-{created_at}"

            history.append({
                'task_id': row_dict['task_id'],
                'name': name,
                'stock': stock,
                'stock_name': stock_name,
                'strategy': strategy,
                'strategy_name': strategy_name,
                'total_return': row_dict.get('total_return') or 0.0,
                'sharpe_ratio': row_dict.get('sharpe_ratio') or 0.0,
                'max_drawdown': row_dict.get('max_drawdown') or 0.0,
                'created_at': created_at,
                'completed_at': row_dict.get('completed_at', '')
            })

        return {
            'success': True,
            'total': total,
            'history': history
        }

    except Exception as e:
        logger.error(f"Error getting backtest history: {e}")
        return {
            'success': False,
            'message': f'获取历史记录失败: {str(e)}'
        }


def get_backtest_history_detail(task_id: str):
    """
    获取单个回测任务的完整结果

    Args:
        task_id: 任务ID

    Returns:
        {
            "success": true,
            "detail": {
                "task_id": "xxx",
                "name": "...",
                "params": {...},
                "result": {...},
                "created_at": "...",
                "completed_at": "..."
            }
        }
    """
    try:
        tm = get_task_manager()
        task = tm.get_task(task_id)

        if not task:
            return {
                'success': False,
                'message': '任务不存在'
            }

        if task.get('task_type') != 'backtest':
            return {
                'success': False,
                'message': '该任务不是回测任务'
            }

        if task.get('status') != 'completed':
            return {
                'success': False,
                'message': f'任务状态为 {task.get("status")}，无法查看详情'
            }

        # 解析参数
        params = task.get('params', {})
        result = task.get('result', {})

        # 生成记录名称
        stock = params.get('stock', '')
        strategy = params.get('strategy', '')
        created_at = task.get('created_at', '')
        name = f"{stock}-{strategy}-{created_at}"

        return {
            'success': True,
            'detail': {
                'task_id': task_id,
                'name': name,
                'params': params,
                'result': result,
                'created_at': created_at,
                'completed_at': task.get('completed_at', '')
            }
        }

    except Exception as e:
        logger.error(f"Error getting backtest history detail: {e}")
        return {
            'success': False,
            'message': f'获取详情失败: {str(e)}'
        }


def delete_backtest_history(task_id: str):
    """
    删除回测历史记录

    Args:
        task_id: 任务ID

    Returns:
        {
            "success": true,
            "message": "删除成功"
        }
    """
    try:
        tm = get_task_manager()
        task = tm.get_task(task_id)

        if not task:
            return {
                'success': False,
                'message': '任务不存在'
            }

        if task.get('task_type') != 'backtest':
            return {
                'success': False,
                'message': '该任务不是回测任务'
            }

        # 删除任务
        tm.delete_task(task_id)

        return {
            'success': True,
            'message': '删除成功'
        }

    except Exception as e:
        logger.error(f"Error deleting backtest history: {e}")
        return {
            'success': False,
            'message': f'删除失败: {str(e)}'
        }
