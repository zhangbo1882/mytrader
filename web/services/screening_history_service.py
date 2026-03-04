"""筛选历史服务"""
from flask import request
import json
import logging
from sqlalchemy import create_engine, text
from config.settings import TUSHARE_DB_PATH
import pandas as pd

from .screening_service import _convert_config_units

logger = logging.getLogger(__name__)


def save_screening_history():
    """
    保存筛选历史

    请求体:
    {
        "name": "我的筛选策略",
        "config": {...},  // 筛选条件配置
        "stocks": [...]  // 筛选结果股票列表（可选）
    }
    """
    try:
        data = request.json
        if not data:
            return {'error': '请求数据为空'}, 400

        name = data.get('name')
        config = data.get('config')
        stocks = data.get('stocks', [])

        if not name:
            return {'error': '请提供筛选名称'}, 400

        if not config:
            return {'error': '请提供筛选条件'}, 400

        user_id = request.args.get('user_id', 'default')

        # 获取结果数量
        result_count = config.get('result_count', len(stocks))

        # 保存到数据库
        engine = create_engine(
            f'sqlite:///{TUSHARE_DB_PATH}',
            connect_args={
                'check_same_thread': False,
                'timeout': 30  # 30秒超时
            }
        )
        with engine.connect() as conn:
            # 插入筛选历史记录
            cursor = conn.execute(text('''
                INSERT INTO screening_history (user_id, name, config, result_count, stocks_count)
                VALUES (:user_id, :name, :config, :result_count, :stocks_count)
            '''), {
                'user_id': user_id,
                'name': name,
                'config': json.dumps(config, ensure_ascii=False),
                'result_count': result_count,
                'stocks_count': len(stocks)
            })

            history_id = cursor.lastrowid

            # 如果提供了股票列表，保存股票详情
            if stocks:
                stock_data = []
                for rank, stock in enumerate(stocks, 1):
                    stock_data.append({
                        'history_id': history_id,
                        'stock_code': stock.get('code', ''),
                        'rank': rank,
                        'stock_name': stock.get('name', ''),
                        'close_price': stock.get('latest_close'),
                        'pe_ttm': stock.get('pe_ttm'),
                        'pb': stock.get('pb'),
                        'total_mv_yi': stock.get('total_mv_yi')
                    })

                # 批量插入股票数据
                if stock_data:
                    df = pd.DataFrame(stock_data)
                    df.to_sql('screening_history_stocks', conn, if_exists='append', index=False)

            conn.commit()

        return {
            'success': True,
            'history_id': history_id,
            'message': f'筛选历史"{name}"已保存'
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'保存失败: {str(e)}'}, 500


def get_screening_history():
    """
    获取筛选历史列表

    查询参数:
        user_id: 用户ID（可选，默认 'default'）

    返回数据:
    {
        "success": true,
        "history": [
            {
                "id": 1,
                "name": "我的筛选策略",
                "result_count": 1167,
                "created_at": "2024-01-01 10:00:00"
            }
        ]
    }
    """
    try:
        user_id = request.args.get('user_id', 'default')

        engine = create_engine(
            f'sqlite:///{TUSHARE_DB_PATH}',
            connect_args={
                'check_same_thread': False,
                'timeout': 30  # 30秒超时
            }
        )

        with engine.connect() as conn:
            query = text('''
                SELECT id, name, result_count, stocks_count, created_at
                FROM screening_history
                WHERE user_id = :user_id
                ORDER BY created_at DESC
            ''')

            df = pd.read_sql_query(query, conn, params={'user_id': user_id})

            history = []
            for _, row in df.iterrows():
                history.append({
                    'id': int(row['id']),
                    'name': row['name'],
                    'result_count': int(row['result_count']),
                    'stocks_count': int(row['stocks_count']),
                    'created_at': row['created_at']
                })

        return {
            'success': True,
            'history': history
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'获取历史记录失败: {str(e)}'}, 500


def get_screening_history_detail(history_id: int):
    """
    获取筛选历史详情

    路径参数:
        history_id: 历史记录ID

    返回数据:
    {
        "success": true,
        "detail": {
            "id": 1,
            "name": "我的筛选策略",
            "config": {...},
            "stocks_count": 1167,
            "created_at": "2024-01-01 10:00:00"
        }
    }
    """
    try:
        user_id = request.args.get('user_id', 'default')

        engine = create_engine(
            f'sqlite:///{TUSHARE_DB_PATH}',
            connect_args={
                'check_same_thread': False,
                'timeout': 30  # 30秒超时
            }
        )

        with engine.connect() as conn:
            # 获取历史记录详情
            query = text('''
                SELECT id, name, config, result_count, stocks_count, created_at
                FROM screening_history
                WHERE id = :history_id AND user_id = :user_id
            ''')

            df = pd.read_sql_query(query, conn, params={'history_id': history_id, 'user_id': user_id})

            if df.empty:
                return {'error': '历史记录不存在'}, 404

            row = df.iloc[0]

            detail = {
                'id': int(row['id']),
                'name': row['name'],
                'config': json.loads(row['config']),
                'result_count': int(row['result_count']),
                'stocks_count': int(row['stocks_count']),
                'created_at': row['created_at']
            }

            # 获取保存的股票列表
            stocks_query = text('''
                SELECT stock_code, stock_name, close_price, pe_ttm, pb, total_mv_yi
                FROM screening_history_stocks
                WHERE history_id = :history_id
                ORDER BY rank
            ''')

            stocks_df = pd.read_sql_query(stocks_query, conn, params={'history_id': history_id})

            stocks = []
            for _, row in stocks_df.iterrows():
                stocks.append({
                    'code': row['stock_code'],
                    'name': row['stock_name'],
                    'latest_close': float(row['close_price']) if pd.notna(row['close_price']) else None,
                    'pe_ttm': float(row['pe_ttm']) if pd.notna(row['pe_ttm']) else None,
                    'pb': float(row['pb']) if pd.notna(row['pb']) else None,
                    'total_mv_yi': float(row['total_mv_yi']) if pd.notna(row['total_mv_yi']) else None
                })

            detail['stocks'] = stocks

        return {
            'success': True,
            'detail': detail
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'获取历史详情失败: {str(e)}'}, 500


def re_run_screening(history_id: int):
    """
    重新执行历史筛选

    路径参数:
        history_id: 历史记录ID

    查询参数:
        limit: 返回结果数量限制（可选，默认2000）

    返回数据:
    {
        "success": true,
        "stocks": [...]
    }
    """
    try:
        from src.screening.rule_engine import RuleEngine
        from src.screening.screening_engine import ScreeningEngine

        user_id = request.args.get('user_id', 'default')
        limit = int(request.args.get('limit', 2000))

        engine = create_engine(
            f'sqlite:///{TUSHARE_DB_PATH}',
            connect_args={
                'check_same_thread': False,
                'timeout': 30  # 30秒超时
            }
        )

        with engine.connect() as conn:
            # 获取历史记录详情
            query = text('''
                SELECT id, name, config
                FROM screening_history
                WHERE id = :history_id AND user_id = :user_id
            ''')

            df = pd.read_sql_query(query, conn, params={'history_id': history_id, 'user_id': user_id})

            if df.empty:
                return {'error': '历史记录不存在'}, 404

            config = json.loads(df.iloc[0]['config'])
            logger.info(f"[ReRun] Original config: {config}")

        # 转换配置中的单位（亿 -> 元）
        config = _convert_config_units(config)
        logger.info(f"[ReRun] Converted config: {config}")

        # 重新执行筛选
        criteria = RuleEngine.build_from_config(config, db_path=str(TUSHARE_DB_PATH))
        screening_engine = ScreeningEngine(str(TUSHARE_DB_PATH))
        df = screening_engine.screen(criteria)

        # 限制结果数量
        df = df.head(limit)

        # 转换结果
        results = []
        for _, row in df.iterrows():
            # 优先使用ScreeningEngine已经计算好的total_mv_yi列
            if 'total_mv_yi' in row and pd.notna(row.get('total_mv_yi')):
                total_mv_yi = round(float(row['total_mv_yi']), 2)
            else:
                total_mv_yi = round(float(row.get('total_mv', 0)) / 10000, 2) if pd.notna(row.get('total_mv')) else None

            results.append({
                'code': row.get('symbol', ''),
                'name': row.get('stock_name', ''),
                'latest_close': round(float(row.get('close', 0)), 2) if pd.notna(row.get('close')) else None,
                'pe_ttm': round(float(row.get('pe_ttm', 0)), 2) if pd.notna(row.get('pe_ttm')) else None,
                'pb': round(float(row.get('pb', 0)), 2) if pd.notna(row.get('pb')) else None,
                'total_mv_yi': total_mv_yi,
            })

        return {
            'success': True,
            'count': len(results),
            'stocks': results
        }

    except ValueError as e:
        return {'error': f'配置错误: {str(e)}'}, 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'重新筛选失败: {str(e)}'}, 500


def delete_screening_history(history_id: int):
    """
    删除筛选历史

    路径参数:
        history_id: 历史记录ID

    查询参数:
        user_id: 用户ID（可选，默认 'default'）
    """
    try:
        user_id = request.args.get('user_id', 'default')

        engine = create_engine(
            f'sqlite:///{TUSHARE_DB_PATH}',
            connect_args={
                'check_same_thread': False,
                'timeout': 30  # 30秒超时
            }
        )

        with engine.connect() as conn:
            # 先检查是否存在
            check_query = text('''
                SELECT id FROM screening_history
                WHERE id = :history_id AND user_id = :user_id
            ''')

            check_df = pd.read_sql_query(check_query, conn, params={'history_id': history_id, 'user_id': user_id})

            if check_df.empty:
                return {'error': '历史记录不存在'}, 404

            # 删除历史记录（CASCADE会自动删除关联的股票数据）
            conn.execute(text('''
                DELETE FROM screening_history
                WHERE id = :history_id AND user_id = :user_id
            '''), {'history_id': history_id, 'user_id': user_id})

            conn.commit()

        return {
            'success': True,
            'message': '历史记录已删除'
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'删除失败: {str(e)}'}, 500
