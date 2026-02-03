"""
Flask routes for the stock query web server
"""
from flask import Blueprint, render_template, request, jsonify, session
from src.utils.stock_lookup import search_stocks, get_stock_name_from_code
from web.exceptions import TaskExistsError
from web.utils.export import export_to_csv, export_to_excel
from src.data_sources.tushare import TushareDB
from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH, SCHEDULE_DB_PATH, SCHEDULER_TIMEZONE, CHECKPOINT_DIR
from web.tasks import init_task_manager
from config.settings import TASKS_DB_PATH
import json
import pandas as pd
import threading
import time
from datetime import datetime
from functools import lru_cache
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/tmp/flask_routes.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 创建 Blueprint
bp = Blueprint('routes', __name__)

# 导入 ML Blueprint 并注册
from web import ml_routes
bp.register_blueprint(ml_routes.ml_bp)


def get_task_manager():
    """
    Get the global task manager instance, initializing if necessary.
    This ensures we always have the correct instance from tasks.py
    """
    from web import tasks
    if tasks.task_manager is None:
        tm = tasks.init_task_manager(
            db_path=str(TASKS_DB_PATH),
            checkpoint_dir=CHECKPOINT_DIR
        )
        # Update the module's global variable
        tasks.task_manager = tm
    return tasks.task_manager

# 初始化数据库连接（懒加载，避免启动时数据库不存在导致错误）
_db = None
_query = None

# 内存缓存用于存储查询结果（替代session，避免cookie过大）
_query_cache = {}
_query_cache_lock = threading.Lock()


def get_db():
    """获取数据库连接（懒加载）"""
    global _db, _query
    if _db is None:
        try:
            _db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))
            _query = _db.query()
        except Exception as e:
            print(f"Warning: Failed to initialize database: {e}")
            _db = False
            _query = False
    return _db, _query


@bp.route('/')
def index():
    """主页"""
    return render_template('index.html')


@bp.route('/api/health')
def health_check():
    """健康检查 API"""
    db, query = get_db()
    return jsonify({
        'status': 'ok' if db else 'error',
        'database': str(TUSHARE_DB_PATH)
    })


@bp.route('/api/stock/search')
def api_stock_search():
    """
    股票搜索 API（自动补全，支持股票和指数）

    Query params:
        q: 搜索关键词（代码或名称）
        limit: 返回结果数量（默认10）
        type: 资产类型 ('stock', 'index', 'all')，默认 'all'
    """
    q = request.args.get('q', '')
    limit = int(request.args.get('limit', 10))
    asset_type = request.args.get('type', 'all')

    results = search_stocks(q, limit=limit, asset_type=asset_type)
    return jsonify(results)


@bp.route('/api/stock/query', methods=['POST'])
def api_stock_query():
    """
    股票查询 API

    Body (JSON):
        symbols: 股票代码列表
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        price_type: 价格类型 ('qfq'=前复权, ''=不复权)
    """
    db, query = get_db()
    if not db or not query:
        return jsonify({'error': '数据库连接失败'}), 500

    try:
        data = request.json
        if not data:
            return jsonify({'error': '请求数据为空'}), 400

        symbols = data.get('symbols', [])
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        price_type = data.get('price_type', 'qfq')

        if not symbols:
            return jsonify({'error': '请选择至少一只股票'}), 400

        if not start_date or not end_date:
            return jsonify({'error': '请指定日期范围'}), 400

        # 查询每只股票的数据
        results = {}
        for symbol in symbols:
            try:
                df = query.query_bars(symbol, start_date, end_date, price_type=price_type)

                # 将 DataFrame 转换为字典列表
                # 转换 datetime 为字符串
                if not df.empty:
                    df = df.copy()
                    if 'datetime' in df.columns:
                        df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d')

                    # 只保留需要的列
                    columns_to_keep = [
                        'datetime', 'open', 'high', 'low', 'close',
                        'volume', 'turnover', 'pct_chg', 'amount'
                    ]
                    df = df[[col for col in columns_to_keep if col in df.columns]]

                    # 转换为字典列表，并处理NaN值
                    records = []
                    for _, row in df.iterrows():
                        record = {}
                        for col in df.columns:
                            val = row[col]
                            # 检查是否为NaN (使用pd.isna检查，包括None和NaN)
                            if pd.isna(val):
                                record[col] = None
                            else:
                                record[col] = val
                        records.append(record)

                    results[symbol] = records
                else:
                    results[symbol] = []

            except Exception as e:
                results[symbol] = []
                print(f"Error querying {symbol}: {e}")

        # 保存到内存缓存用于导出（不使用session避免cookie过大）
        cache_key = f"query_{int(time.time())}_{id(results)}"
        with _query_cache_lock:
            # 清理旧缓存（保留最近10个）
            if len(_query_cache) > 10:
                old_keys = sorted(_query_cache.keys())[:len(_query_cache) - 10]
                for key in old_keys:
                    del _query_cache[key]
            _query_cache[cache_key] = {
                'data': results,
                'timestamp': time.time()
            }

        # 在session中只保存缓存key
        session['last_query_cache_key'] = cache_key

        return jsonify(results)

    except Exception as e:
        return jsonify({'error': f'查询失败: {str(e)}'}), 500


@bp.route('/api/stock/export/<format>')
def api_stock_export(format):
    """
    数据导出 API

    Args:
        format: 导出格式 (csv 或 excel)
    """
    if format not in ['csv', 'excel']:
        return jsonify({'error': '不支持的导出格式'}), 400

    # 从session获取缓存key
    cache_key = session.get('last_query_cache_key')
    if not cache_key:
        return jsonify({'error': '没有可导出的数据，请先执行查询'}), 400

    # 从内存缓存获取数据
    with _query_cache_lock:
        cached = _query_cache.get(cache_key)
        if not cached:
            return jsonify({'error': '查询数据已过期，请重新执行查询'}), 400

        data = cached['data']

    try:
        if format == 'csv':
            return export_to_csv(data)
        else:
            return export_to_excel(data)

    except Exception as e:
        return jsonify({'error': f'导出失败: {str(e)}'}), 500


@bp.route('/api/stock/name/<code>')
def api_get_stock_name(code):
    """
    获取股票名称 API

    Args:
        code: 股票代码
    """
    name = get_stock_name_from_code(code)
    return jsonify({'code': code, 'name': name})


@bp.route('/api/stock/min-date')
def api_get_min_date():
    """
    获取数据库中最早的交易日期 API

    Returns:
        最早的日期 (YYYY-MM-DD)
    """
    db, query = get_db()
    if not db or not query:
        return jsonify({'date': '2020-01-01'})

    try:
        from sqlalchemy import text

        # 查询bars表中最早的日期
        with db.engine.connect() as conn:
            result = conn.execute(text("SELECT MIN(datetime) FROM bars"))
            min_date = result.fetchone()

            if min_date and min_date[0]:
                date_str = min_date[0]
                # 如果是 datetime 类型，转换为字符串
                if hasattr(date_str, 'strftime'):
                    date_str = date_str.strftime('%Y-%m-%d')
                elif ' ' in date_str:
                    date_str = date_str.split()[0]
                return jsonify({'date': date_str})

        return jsonify({'date': '2020-01-01'})

    except Exception as e:
        print(f"Error getting min date: {e}")
        return jsonify({'date': '2020-01-01'})


@bp.route('/api/stock/screen', methods=['POST'])
def api_stock_screen():
    """股票筛选API - 根据条件筛选股票列表"""
    db, query = get_db()
    if not db or not query:
        return jsonify({'error': '数据库连接失败'}), 500

    try:
        data = request.json

        # 调用StockQuery的筛选方法
        df = query.screen_stocks(
            days=data.get('days', 5),
            turnover_min=data.get('turnover_min'),
            turnover_max=data.get('turnover_max'),
            pct_chg_min=data.get('pct_chg_min'),
            pct_chg_max=data.get('pct_chg_max'),
            price_min=data.get('price_min'),
            price_max=data.get('price_max'),
            volume_min=data.get('volume_min'),
            volume_max=data.get('volume_max')
        )

        # 转换为JSON格式
        results = []
        for _, row in df.iterrows():
            results.append({
                'code': row['symbol'],
                'name': row['name'],
                'latest_date': str(row['latest_date']),
                'latest_close': round(float(row['latest_close']), 2) if pd.notna(row['latest_close']) else None,
                'avg_turnover': round(float(row['avg_turnover']), 2) if pd.notna(row['avg_turnover']) else None,
                'avg_pct_chg': round(float(row['avg_pct_chg']), 2) if pd.notna(row['avg_pct_chg']) else None
            })

        return jsonify({
            'success': True,
            'count': len(results),
            'symbols': results[:500],  # 最多返回500只
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'筛选失败: {str(e)}'}), 500


@bp.route('/api/stock/ai-screen', methods=['POST'])
def api_ai_stock_screen():
    """
    AI驱动的股票筛选API
    使用Claude AI解析自然语言查询并提取结构化参数

    Body (JSON):
        query: 自然语言查询文本（例如："查找最近5天涨幅超过5%的股票"）

    Returns:
        {
            "success": true,
            "query": "原始查询文本",
            "params": {
                "days": 5,
                "pct_chg_min": 5,
                ...
            }
        }
    """
    try:
        data = request.json
        if not data:
            return jsonify({'error': '请求数据为空'}), 400

        query = data.get('query', '').strip()
        if not query:
            return jsonify({'error': '查询文本不能为空'}), 400

        # Import AI client
        try:
            from src.ai.claude_client import create_claude_client
        except ImportError as e:
            return jsonify({'error': f'AI模块未正确配置: {str(e)}'}), 500

        # Create Claude client
        claude_client = create_claude_client()
        if claude_client is None:
            return jsonify({
                'error': 'Claude CLI未找到，请确保Claude Code CLI已安装并在PATH中，或设置CLAUDE_CLI_PATH环境变量'
            }), 500

        # Parse query
        params = claude_client.parse_screening_query(query, language='zh')

        # Return success with extracted parameters
        return jsonify({
            'success': True,
            'query': query,
            'params': params
        })

    except ValueError as e:
        # Validation or parsing error
        return jsonify({'error': str(e)}), 400
    except RuntimeError as e:
        # API or connection error
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'处理失败: {str(e)}'}), 500


@bp.route('/api/stock/ai-screen-chat', methods=['POST'])
def api_ai_stock_screen_chat():
    """
    AI驱动的股票筛选对话API（支持多轮对话）
    使用Claude AI解析自然语言查询并提取结构化参数，返回对话式回复

    Body (JSON):
        query: 自然语言查询文本
        history: 对话历史（可选）

    Returns:
        {
            "success": true,
            "response": "AI回复文本",
            "params": {...},  // 提取的参数
            "should_screen": true/false  // 是否应该执行筛选
        }
    """
    try:
        data = request.json
        if not data:
            return jsonify({'error': '请求数据为空'}), 400

        query = data.get('query', '').strip()
        if not query:
            return jsonify({'error': '查询文本不能为空'}), 400

        history = data.get('history', [])

        # Import AI client
        try:
            from src.ai.claude_client import create_claude_client
        except ImportError as e:
            return jsonify({'error': f'AI模块未正确配置: {str(e)}'}), 500

        # Create Claude client
        claude_client = create_claude_client()
        if claude_client is None:
            return jsonify({
                'error': 'Claude CLI未找到，请确保Claude Code CLI已安装并在PATH中，或设置CLAUDE_CLI_PATH环境变量'
            }), 500

        # Parse query with chat support
        result = claude_client.parse_screening_query_chat(query, history=history, language='zh')

        # Determine if should execute screening
        should_screen = result.get('params') is not None and len(result.get('params', {})) > 0

        # Return success with response
        return jsonify({
            'success': True,
            'response': result.get('response', ''),
            'params': result.get('params'),
            'should_screen': should_screen
        })

    except ValueError as e:
        # Validation or parsing error
        return jsonify({'error': str(e)}), 400
    except RuntimeError as e:
        # API or connection error
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'处理失败: {str(e)}'}), 500


@bp.route('/api/stock/update-favorites', methods=['POST'])
def update_favorites():
    """
    Update favorites stock data

    Body (JSON):
        stocks: List of stock codes to update
    """
    data = request.json
    stock_list = data.get('stocks', [])

    if not stock_list:
        return jsonify({'success': False, 'error': '收藏列表为空'})

    # Create task
    try:
        task_id = get_task_manager().create_task('update_favorites', {'stocks': stock_list})
    except TaskExistsError as e:
        existing = e.existing_task
        status_label = {
            'pending': '等待中',
            'running': '运行中',
            'paused': '已暂停'
        }.get(existing['status'], existing['status'])

        error_message = f"无法创建新任务：系统中已有任务在{status_label}状态"

        return jsonify({
            'success': False,
            'error': error_message,
            'error_type': 'task_exists',
            'existing_task': existing
        }), 409

    # Start background thread
    def run_update():
        try:
            get_task_manager().update_task(task_id, status='running', message='正在更新数据...')

            from src.data_sources.tushare import TushareDB
            db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

            # Calculate date range (from 2024-01-01 to today)
            end_date = datetime.now().strftime('%Y%m%d')

            # Track results for each stock
            detailed_results = {
                'success': [],
                'failed': [],
                'skipped': []
            }

            # Helper function to check if stock has data in database
            def stock_has_data(code):
                """Check if stock has any bar data in database"""
                try:
                    from sqlalchemy import text
                    # Create a new engine for this thread to avoid connection issues
                    from sqlalchemy import create_engine
                    from config.settings import TUSHARE_DB_PATH
                    engine = create_engine(f"sqlite:///{TUSHARE_DB_PATH}")
                    with engine.connect() as conn:
                        result = conn.execute(text(
                            "SELECT COUNT(*) FROM bars WHERE symbol = :code"
                        ), {"code": code})
                        row = result.fetchone()
                        count = row[0] if row else 0
                        return count > 0
                except Exception as e:
                    # If check fails, assume no data (conservative approach)
                    print(f"[DEBUG] Error checking data for {code}: {e}")
                    return False

            # Update each stock individually to track results
            for i, stock_code in enumerate(stock_list):
                # Check stop request (memory flag - fast and lock-free)
                if get_task_manager().is_stop_requested(task_id):
                    print(f"[TASK-{task_id[:8]}] Stop requested at index {i}")
                    get_task_manager().update_task(task_id, status='stopped', message='任务已停止')
                    get_task_manager().clear_stop_request(task_id)
                    return

                try:
                    get_task_manager().update_task(task_id,
                        message=f'正在更新 {stock_code} ({i+1}/{len(stock_list)})...'
                    )

                    # Check if stock already has data BEFORE update
                    had_data_before = stock_has_data(stock_code)

                    # Get stock name with error handling
                    try:
                        from src.utils.stock_lookup import get_stock_name_from_code
                        stock_name = get_stock_name_from_code(stock_code)
                        print(f"[DEBUG] {stock_code}: stock_name={repr(stock_name)}, type={type(stock_name)}")
                        # If stock_name equals stock_code, it means lookup failed
                        if stock_name == stock_code:
                            stock_display = stock_code
                            print(f"[DEBUG] {stock_code}: Using code only (name lookup failed)")
                        else:
                            stock_display = f"{stock_name}({stock_code})"
                            print(f"[DEBUG] {stock_code}: Using formatted display: {repr(stock_display)}")
                    except Exception as e:
                        print(f"[DEBUG] {stock_code}: Exception getting name: {e}")
                        stock_display = stock_code

                    # Try to update this stock
                    stats = db.save_all_stocks_by_code_incremental(
                        default_start_date='20240101',
                        end_date=end_date,
                        stock_list=[stock_code]
                    )

                    # Debug logging
                    print(f"[UPDATE] {stock_code}: had_data={had_data_before}, stats={stats}")

                    if stats:
                        # Check result categories
                        success_count = stats.get('success', 0)
                        failed_count = stats.get('failed', 0)
                        skipped_count = stats.get('skipped', 0)

                        if skipped_count > 0:
                            # Explicitly marked as skipped
                            detailed_results['skipped'].append(f"{stock_display}(已是最新)")
                        elif success_count > 0 and failed_count == 0:
                            # Check if stock already had data (no new data downloaded)
                            if had_data_before:
                                detailed_results['skipped'].append(f"{stock_display}(已是最新)")
                            else:
                                # First time download
                                detailed_results['success'].append(stock_display)
                        elif success_count > 0 and failed_count > 0:
                            # Partial success
                            detailed_results['skipped'].append(f"{stock_display}(部分成功)")
                        elif stats.get('total', 0) > 0 and had_data_before:
                            # Processed but no new data, and stock had data before
                            detailed_results['skipped'].append(f"{stock_display}(已是最新)")
                        else:
                            # No data found for this stock
                            detailed_results['failed'].append(f"{stock_display}(无数据)")
                    else:
                        # Stats is None, treat as failed
                        detailed_results['failed'].append(f"{stock_display}(更新失败)")

                except Exception as e:
                    detailed_results['failed'].append(f"{stock_code}(错误: {str(e)})")

            # Create summary stats
            summary_stats = {
                'total': len(stock_list),
                'success': len(detailed_results['success']),
                'failed': len(detailed_results['failed']),
                'skipped': len(detailed_results['skipped']),
                'details': detailed_results
            }

            get_task_manager().update_task(task_id,
                status='completed',
                message='更新完成',
                current_stock_index=len(stock_list),
                progress=100,
                result=summary_stats
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            get_task_manager().update_task(task_id,
                status='failed',
                message=f'更新失败: {str(e)}'
            )

    thread = threading.Thread(target=run_update)
    thread.daemon = True
    thread.start()

    return jsonify({'success': True, 'task_id': task_id})


@bp.route('/api/tasks/<task_id>', methods=['GET'])
def get_task_status(task_id):
    """
    Get task status

    Args:
        task_id: Task identifier

    Returns:
        Task information
    """
    task = get_task_manager().get_task(task_id)
    if not task:
        return jsonify({'success': False, 'error': '任务不存在'})

    return jsonify({'success': True, 'task': task})


@bp.route('/api/tasks/<task_id>/cancel', methods=['POST'])
def cancel_task(task_id):
    """
    Cancel a task

    Args:
        task_id: Task identifier

    Returns:
        Success/error response
    """
    task = get_task_manager().get_task(task_id)
    if not task:
        return jsonify({'success': False, 'error': '任务不存在'})

    if task['status'] in ['pending', 'running']:
        get_task_manager().update_task(task_id, status='cancelled', message='任务已取消')
        return jsonify({'success': True})

    return jsonify({'success': False, 'error': '任务无法取消'})


# ==================== Task Manager API ====================

@bp.route('/api/tasks', methods=['GET'])
def get_all_tasks():
    """获取所有任务列表"""
    if get_task_manager() is None:
        return jsonify({'success': True, 'tasks': []})
    return jsonify({'success': True, 'tasks': get_task_manager().get_all_tasks()})


@bp.route('/api/tasks/<task_id>', methods=['DELETE'])
def delete_task(task_id):
    """删除任务记录"""
    if get_task_manager() is None:
        return jsonify({'success': False, 'error': '任务管理器未初始化'})

    try:
        get_task_manager().delete_task(task_id)
        return jsonify({'success': True})
    except TimeoutError as e:
        return jsonify({'success': False, 'error': '任务正在执行，无法删除。请先停止任务。'})
    except Exception as e:
        return jsonify({'success': False, 'error': f'删除失败: {str(e)}'})


@bp.route('/api/tasks/<task_id>/pause', methods=['POST'])
def pause_task(task_id):
    """暂停任务"""
    if get_task_manager() is None:
        return jsonify({'success': False, 'error': '任务管理器未初始化'})

    get_task_manager().request_pause(task_id)
    return jsonify({'success': True, 'message': '任务暂停请求已发送'})


@bp.route('/api/tasks/<task_id>/resume', methods=['POST'])
def resume_task(task_id):
    """恢复任务"""
    if get_task_manager() is None:
        return jsonify({'success': False, 'error': '任务管理器未初始化'})

    get_task_manager().clear_pause_request(task_id)
    get_task_manager().update_task(task_id, status='running', message='任务已恢复')
    return jsonify({'success': True, 'message': '任务已恢复'})


@bp.route('/api/tasks/<task_id>/stop', methods=['POST'])
def stop_task(task_id):
    """停止任务"""
    if get_task_manager() is None:
        return jsonify({'success': False, 'error': '任务管理器未初始化'})

    get_task_manager().request_stop(task_id)
    return jsonify({'success': True, 'message': '任务停止请求已发送'})


@bp.route('/api/tasks/cleanup', methods=['POST'])
def cleanup_old_tasks():
    """
    清理旧的已完成/失败任务

    Query params:
        max_age_hours: 最大保留时间（小时）
    """
    if get_task_manager() is None:
        return jsonify({'success': False, 'error': '任务管理器未初始化'})

    max_age_hours = int(request.args.get('max_age_hours', 168))  # 默认7天
    deleted_count = get_task_manager().cleanup_old_tasks(max_age_hours=max_age_hours)

    return jsonify({
        'success': True,
        'deleted_count': deleted_count,
        'message': f'已清理 {deleted_count} 个旧任务'
    })


@bp.route('/api/tasks/active-check', methods=['GET'])
def check_active_task():
    """检查是否存在活动任务"""
    try:
        has_active, task_info = get_task_manager().has_active_task()

        return jsonify({
            'success': True,
            'has_active': has_active,
            'task': task_info
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@bp.route('/api/stock/update-all', methods=['POST'])
def update_all_stocks():
    """
    全量/增量股票更新API

    Body (JSON):
        mode: "incremental" (增量) 或 "full" (全量)
        stock_range: "all" (全部), "favorites" (收藏), "custom" (自定义)
        custom_stocks: 自定义股票列表 (可选)
    """
    data = request.json
    if not data:
        return jsonify({'success': False, 'error': '请求数据为空'}), 400

    mode = data.get('mode', 'incremental')
    stock_range = data.get('stock_range', 'all')
    custom_stocks = data.get('custom_stocks', [])

    logger.info(f"/api/stock/update-all called: mode={mode}, stock_range={stock_range}")

    # Get stock list based on range
    stock_list = []
    if stock_range == 'custom':
        # Handle both string and list input
        if isinstance(custom_stocks, str):
            # Parse comma-separated string
            stock_list = [s.strip() for s in custom_stocks.split(',') if s.strip()]
        else:
            # Already a list
            stock_list = custom_stocks
    elif stock_range == 'favorites':
        # Check if custom_stocks is provided (from frontend localStorage)
        if custom_stocks:
            # Handle both string and list input
            if isinstance(custom_stocks, str):
                stock_list = [s.strip() for s in custom_stocks.split(',') if s.strip()]
            else:
                stock_list = custom_stocks
        else:
            # Fallback to session or default list
            favorites = session.get('favorites', [])
            stock_list = favorites if favorites else ["600382", "600711", "000001"]
    else:  # all
        # 从 Tushare API 获取股票列表（避免包含指数代码）
        try:
            db, query = get_db()
            if db:
                stock_list_df = db._retry_api_call(
                    db.pro.stock_basic,
                    exchange='',
                    list_status='L',
                    fields='ts_code'
                )
                if stock_list_df is not None and not stock_list_df.empty:
                    stock_list = stock_list_df['ts_code'].tolist()
                else:
                    return jsonify({'success': False, 'error': '获取股票列表失败'}), 500
        except Exception as e:
            return jsonify({'success': False, 'error': f'获取股票列表失败: {str(e)}'}), 500

    if not stock_list:
        return jsonify({'success': False, 'error': '股票列表为空'}), 400

    # Create task with metadata
    logger.info(f"Creating task...")
    try:
        task_id = get_task_manager().create_task(
            'update_all_stocks',
            {'mode': mode, 'stock_range': stock_range, 'custom_stocks': custom_stocks},
            metadata={'total_stocks': len(stock_list)}
        )
        logger.info(f"Task created: {task_id}")
    except TaskExistsError as e:
        # 处理任务已存在错误
        existing = e.existing_task
        task_id_short = existing['task_id'][:8]
        status_label = {
            'pending': '等待中',
            'running': '运行中',
            'paused': '已暂停'
        }.get(existing['status'], existing['status'])

        error_message = f"""无法创建新任务：系统中已有任务在{status_label}

现有任务信息：
  - 任务ID: {task_id_short}...
  - 任务状态: {status_label}
  - 创建时间: {existing.get('created_at', 'unknown')}
  - 当前进度: {existing.get('current_stock_index', 0)}/{existing.get('total_stocks', 0)}

请先在"任务历史"页面停止现有任务后再创建新任务。""".strip()

        logger.warning(f"Task creation blocked: {str(e)}")
        return jsonify({
            'success': False,
            'error': error_message,
            'error_type': 'task_exists',
            'existing_task': {
                'task_id': existing['task_id'],
                'status': existing['status'],
                'created_at': existing.get('created_at'),
                'progress': existing.get('progress', 0),
                'current_stock_index': existing.get('current_stock_index', 0),
                'total_stocks': existing.get('total_stocks', 0)
            }
        }), 409

    # Start background thread
    def run_update():
        try:
            logger.info(f"[THREAD] Thread started for task {task_id}")
            logger.info(f"[THREAD] About to call get_task_manager()")
            tm = get_task_manager()
            logger.info(f"[THREAD] Got task_manager: {tm}")
            logger.info(f"[THREAD] About to update task status")
            try:
                tm.update_task(task_id, status='running', message='正在更新股票数据...')
                logger.info(f"[THREAD] Task status updated to 'running'")
            except Exception as e:
                logger.error(f"[THREAD] Failed to update task status: {e}")
                import traceback
                logger.error(traceback.format_exc())
                raise

            from src.data_sources.tushare import TushareDB
            from src.utils.stock_lookup import get_stock_name_from_code

            db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))
            end_date = datetime.now().strftime('%Y%m%d')

            # Track results for each stock
            detailed_results = {
                'success': [],
                'failed': [],
                'skipped': []
            }

            # Load checkpoint if resuming
            checkpoint = get_task_manager().load_checkpoint(task_id)
            start_index = 0
            if checkpoint:
                start_index = checkpoint.get('current_index', 0)
                stats = checkpoint.get('stats', {'success': 0, 'failed': 0, 'skipped': 0})
                get_task_manager().update_task(task_id, stats=stats)
                print(f"[UPDATE] Resuming from checkpoint: index {start_index}")

            # Update each stock
            tm = get_task_manager()
            for i in range(start_index, len(stock_list)):
                stock_code = stock_list[i]

                # Check stop request (lock-free)
                if tm.is_stop_requested(task_id):
                    tm.save_checkpoint(task_id, i, tm.get_task(task_id).get('stats'))
                    tm.update_task(task_id, status='stopped', message='任务已停止')
                    tm.clear_stop_request(task_id)
                    return

                # Check pause request (lock-free)
                if tm.is_pause_requested(task_id):
                    tm.save_checkpoint(task_id, i, tm.get_task(task_id).get('stats'))
                    tm.update_task(task_id, status='paused', message='任务已暂停')
                    # Wait for resume
                    while True:
                        time.sleep(1)
                        # Check if stopped while paused (lock-free)
                        if tm.is_stop_requested(task_id):
                            tm.update_task(task_id, status='stopped', message='任务已停止')
                            tm.clear_stop_request(task_id)
                            tm.clear_pause_request(task_id)
                            return
                        # Check if resumed (lock-free)
                        if not tm.is_pause_requested(task_id):
                            tm.update_task(task_id, message='任务已恢复')
                            break

                try:
                    # Update progress
                    progress = int((i / len(stock_list)) * 100)
                    get_task_manager().update_task(task_id,
                        current_stock_index=i,
                        progress=progress,
                        message=f'正在更新 {stock_code} ({i+1}/{len(stock_list)})...'
                    )

                    # Save checkpoint every 10 stocks
                    if i % 10 == 0:
                        current_task = get_task_manager().get_task(task_id)
                        get_task_manager().save_checkpoint(task_id, i, current_task.get('stats') if current_task else None)

                    # Check stop request AGAIN before blocking DB operation
                    if tm.is_stop_requested(task_id):
                        print(f"[TASK-{task_id[:8]}] Stop requested before DB operation, stopping at index {i}")
                        get_task_manager().update_task(task_id, status='stopped', message='任务已停止')
                        tm.clear_stop_request(task_id)
                        return

                    # Update stock data (this may block for a long time!)
                    print(f"[TASK-{task_id[:8]}] Starting DB update for {stock_code}...")
                    if mode == 'full':
                        stats = db.save_all_stocks_by_code(
                            default_start_date='20200101',
                            end_date=end_date,
                            stock_list=[stock_code]
                        )
                    else:  # incremental
                        stats = db.save_all_stocks_by_code_incremental(
                            default_start_date='20240101',
                            end_date=end_date,
                            stock_list=[stock_code]
                        )
                    print(f"[TASK-{task_id[:8]}] DB update for {stock_code} complete")

                    # Update stats and track detailed results
                    if stats:
                        # Get stock name for display
                        stock_name = get_stock_name_from_code(stock_code)
                        stock_display = f"{stock_code} {stock_name}" if stock_name else stock_code

                        if stats.get('success', 0) > 0:
                            get_task_manager().increment_stats(task_id, 'success')
                            if stats.get('failed', 0) == 0 and stats.get('skipped', 0) == 0:
                                detailed_results['success'].append(stock_display)
                        if stats.get('failed', 0) > 0:
                            get_task_manager().increment_stats(task_id, 'failed')
                            detailed_results['failed'].append(f"{stock_display}(更新失败)")
                        if stats.get('skipped', 0) > 0:
                            get_task_manager().increment_stats(task_id, 'skipped')
                            detailed_results['skipped'].append(f"{stock_display}(已是最新)")
                    else:
                        get_task_manager().increment_stats(task_id, 'failed')
                        stock_name = get_stock_name_from_code(stock_code)
                        stock_display = f"{stock_code} {stock_name}" if stock_name else stock_code
                        detailed_results['failed'].append(f"{stock_display}(无数据)")

                except Exception as e:
                    get_task_manager().increment_stats(task_id, 'failed')
                    stock_name = get_stock_name_from_code(stock_code)
                    stock_display = f"{stock_code} {stock_name}" if stock_name else stock_code
                    detailed_results['failed'].append(f"{stock_display}(错误: {str(e)})")
                    print(f"[ERROR] Failed to update {stock_code}: {e}")

            # Complete
            get_task_manager().delete_checkpoint(task_id)
            final_stats = get_task_manager().get_task(task_id).get('stats', {})

            # Add detailed results to final stats
            summary_stats = {
                'total': len(stock_list),
                'success': final_stats.get('success', 0),
                'failed': final_stats.get('failed', 0),
                'skipped': final_stats.get('skipped', 0),
                'details': detailed_results
            }

            get_task_manager().update_task(task_id,
                status='completed',
                message='更新完成',
                current_stock_index=len(stock_list),
                progress=100,
                result=summary_stats
            )
            logger.info(f"[THREAD] Task {task_id} completed")

            # 自动更新换手率数据（补充可能缺失的 daily_basic 数据）
            logger.info(f"[THREAD] Auto-updating turnover data for recent dates...")
            try:
                from datetime import timedelta
                end_date = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')

                turnover_updated = db.update_turnover_only(
                    symbols=stock_list,
                    start_date=start_date,
                    end_date=end_date
                )
                logger.info(f"[THREAD] Turnover data updated: {turnover_updated} records")
            except Exception as e:
                logger.warning(f"[THREAD] Failed to auto-update turnover data: {e}")

        except Exception as e:
            import traceback
            traceback.print_exc()
            get_task_manager().update_task(task_id,
                status='failed',
                message=f'更新失败: {str(e)}'
            )
            logger.error(f"[THREAD] Task {task_id} failed: {e}")

    logger.info(f"Starting background thread...")
    thread = threading.Thread(target=run_update)
    thread.daemon = True
    thread.start()
    logger.info(f"Background thread started")

    return jsonify({'success': True, 'task_id': task_id})


@bp.route('/api/financial/update', methods=['POST'])
def update_financial_data():
    """
    财务数据更新API - 自动检测并更新财务数据

    Body (JSON):
        stock_range: "all" | "favorites" | "custom"
        custom_stocks: list of stock codes (for custom range)
        include_indicators: bool (默认 True) - 是否同时更新财务指标
    """
    data = request.json
    if not data:
        return jsonify({'success': False, 'error': '请求数据为空'}), 400

    stock_range = data.get('stock_range', 'all')
    custom_stocks = data.get('custom_stocks', [])
    include_indicators = data.get('include_indicators', True)

    logger.info(f"/api/financial/update called: stock_range={stock_range}")

    # Get stock list based on range (reuse logic from stock update)
    stock_list = []
    if stock_range == 'custom':
        if isinstance(custom_stocks, str):
            stock_list = [s.strip() for s in custom_stocks.split(',') if s.strip()]
        else:
            stock_list = custom_stocks
    elif stock_range == 'favorites':
        if custom_stocks:
            if isinstance(custom_stocks, str):
                stock_list = [s.strip() for s in custom_stocks.split(',') if s.strip()]
            else:
                stock_list = custom_stocks
        else:
            favorites = session.get('favorites', [])
            stock_list = favorites if favorites else ["600382", "600711", "000001"]
    else:  # all
        # 从 Tushare API 获取股票列表（避免包含指数代码）
        try:
            db, query = get_db()
            if db:
                stock_list_df = db._retry_api_call(
                    db.pro.stock_basic,
                    exchange='',
                    list_status='L',
                    fields='ts_code'
                )
                if stock_list_df is not None and not stock_list_df.empty:
                    stock_list = stock_list_df['ts_code'].tolist()
                else:
                    return jsonify({'success': False, 'error': '获取股票列表失败'}), 500
        except Exception as e:
            return jsonify({'success': False, 'error': f'获取股票列表失败: {str(e)}'}), 500

    if not stock_list:
        return jsonify({'success': False, 'error': '股票列表为空'}), 400

    # Create task with metadata
    logger.info(f"Creating financial update task...")
    try:
        task_id = get_task_manager().create_task(
            'update_financial_data',
            {'stock_range': stock_range, 'custom_stocks': custom_stocks, 'include_indicators': include_indicators},
            metadata={'total_stocks': len(stock_list)}
        )
        logger.info(f"Financial update task created: {task_id}")
    except TaskExistsError as e:
        existing = e.existing_task
        task_id_short = existing['task_id'][:8]
        status_label = {
            'pending': '等待中',
            'running': '运行中',
            'paused': '已暂停'
        }.get(existing['status'], existing['status'])

        error_message = f"""无法创建新任务：系统中已有任务在{status_label}

现有任务信息：
  - 任务ID: {task_id_short}...
  - 任务状态: {status_label}
  - 创建时间: {existing.get('created_at', 'unknown')}
  - 当前进度: {existing.get('current_stock_index', 0)}/{existing.get('total_stocks', 0)}

请先在"任务历史"页面停止现有任务后再创建新任务。""".strip()

        logger.warning(f"Task creation blocked: {str(e)}")
        return jsonify({
            'success': False,
            'error': error_message,
            'error_type': 'task_exists',
            'existing_task': {
                'task_id': existing['task_id'],
                'status': existing['status'],
                'created_at': existing.get('created_at'),
                'progress': existing.get('progress', 0),
                'current_stock_index': existing.get('current_stock_index', 0),
                'total_stocks': existing.get('total_stocks', 0)
            }
        }), 409

    # Start background thread
    def run_financial_update():
        try:
            logger.info(f"[THREAD] Financial update thread started for task {task_id}")
            tm = get_task_manager()
            tm.update_task(task_id, status='running', message='正在更新财务数据...')

            from src.data_sources.tushare import TushareDB
            from src.utils.stock_lookup import get_stock_name_from_code

            db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))
            end_date = datetime.now().strftime('%Y%m%d')

            # Track results for each stock
            detailed_results = {
                'success': [],
                'failed': [],
                'skipped': []
            }

            # Load checkpoint if resuming
            checkpoint = get_task_manager().load_checkpoint(task_id)
            start_index = 0
            if checkpoint:
                start_index = checkpoint.get('current_index', 0)
                stats = checkpoint.get('stats', {'success': 0, 'failed': 0, 'skipped': 0})
                get_task_manager().update_task(task_id, stats=stats)
                print(f"[FINANCIAL UPDATE] Resuming from checkpoint: index {start_index}")

            # Update each stock's financial data
            tm = get_task_manager()
            for i in range(start_index, len(stock_list)):
                stock_code = stock_list[i]

                # Check stop request (lock-free)
                if tm.is_stop_requested(task_id):
                    tm.save_checkpoint(task_id, i, tm.get_task(task_id).get('stats'))
                    tm.update_task(task_id, status='stopped', message='任务已停止')
                    tm.clear_stop_request(task_id)
                    return

                # Check pause request (lock-free)
                if tm.is_pause_requested(task_id):
                    tm.save_checkpoint(task_id, i, tm.get_task(task_id).get('stats'))
                    tm.update_task(task_id, status='paused', message='任务已暂停')
                    # Wait for resume
                    while True:
                        time.sleep(1)
                        if tm.is_stop_requested(task_id):
                            tm.update_task(task_id, status='stopped', message='任务已停止')
                            tm.clear_stop_request(task_id)
                            tm.clear_pause_request(task_id)
                            return
                        if not tm.is_pause_requested(task_id):
                            tm.update_task(task_id, message='任务已恢复')
                            break

                try:
                    # Update progress
                    progress = int((i / len(stock_list)) * 100)
                    get_task_manager().update_task(task_id,
                        current_stock_index=i,
                        progress=progress,
                        message=f'正在更新财务数据 {stock_code} ({i+1}/{len(stock_list)})...'
                    )

                    # Save checkpoint every 10 stocks
                    if i % 10 == 0:
                        current_task = get_task_manager().get_task(task_id)
                        get_task_manager().save_checkpoint(task_id, i, current_task.get('stats') if current_task else None)

                    # Check stop request AGAIN before blocking DB operation
                    if tm.is_stop_requested(task_id):
                        print(f"[TASK-{task_id[:8]}] Stop requested before DB operation, stopping at index {i}")
                        get_task_manager().update_task(task_id, status='stopped', message='任务已停止')
                        tm.clear_stop_request(task_id)
                        return

                    # Check latest financial date and determine start_date
                    print(f"[TASK-{task_id[:8]}] Checking latest financial date for {stock_code}...")
                    latest_date = db.get_latest_financial_date(stock_code, 'income')

                    if latest_date:
                        # Has existing data, only download new reports
                        start_date = latest_date
                        update_type = "增量更新"
                    else:
                        # No existing data, download all
                        start_date = '20200101'
                        update_type = "首次下载"

                    print(f"[TASK-{task_id[:8]}] {stock_code}: {update_type} (from {start_date})")

                    # Download financial data (get include_indicators from task params)
                    task_data = get_task_manager().get_task(task_id)
                    include_indicators = task_data.get('params', {}).get('include_indicators', True) if task_data else True
                    result = db.save_all_financial(stock_code, start_date, end_date, include_indicators=include_indicators)
                    print(f"[TASK-{task_id[:8]}] Financial update for {stock_code} complete: {result} records")

                    # Update stats and track detailed results
                    stock_name = get_stock_name_from_code(stock_code)
                    stock_display = f"{stock_code} {stock_name}" if stock_name else stock_code

                    if result and result > 0:
                        get_task_manager().increment_stats(task_id, 'success')
                        detailed_results['success'].append(f"{stock_display}({update_type})")
                    elif result == 0:
                        get_task_manager().increment_stats(task_id, 'skipped')
                        detailed_results['skipped'].append(f"{stock_display}(已是最新)")
                    else:
                        get_task_manager().increment_stats(task_id, 'failed')
                        detailed_results['failed'].append(f"{stock_display}(无数据)")

                except Exception as e:
                    get_task_manager().increment_stats(task_id, 'failed')
                    stock_name = get_stock_name_from_code(stock_code)
                    stock_display = f"{stock_code} {stock_name}" if stock_name else stock_code
                    detailed_results['failed'].append(f"{stock_display}(错误: {str(e)})")
                    print(f"[ERROR] Failed to update financial data for {stock_code}: {e}")

            # Complete
            get_task_manager().delete_checkpoint(task_id)
            final_stats = get_task_manager().get_task(task_id).get('stats', {})

            # Add detailed results to final stats
            summary_stats = {
                'total': len(stock_list),
                'success': final_stats.get('success', 0),
                'failed': final_stats.get('failed', 0),
                'skipped': final_stats.get('skipped', 0),
                'details': detailed_results
            }

            get_task_manager().update_task(task_id,
                status='completed',
                message='财务数据更新完成',
                current_stock_index=len(stock_list),
                progress=100,
                result=summary_stats
            )
            logger.info(f"[THREAD] Financial update task {task_id} completed")

        except Exception as e:
            import traceback
            traceback.print_exc()
            get_task_manager().update_task(task_id,
                status='failed',
                message=f'财务数据更新失败: {str(e)}'
            )
            logger.error(f"[THREAD] Financial update task {task_id} failed: {e}")

    logger.info(f"Starting financial update background thread...")
    thread = threading.Thread(target=run_financial_update)
    thread.daemon = True
    thread.start()
    logger.info(f"Financial update background thread started")

    return jsonify({'success': True, 'task_id': task_id})


@bp.route('/api/index/update', methods=['POST'])
def update_indices():
    """
    指数数据更新API

    Body (JSON):
        start_date: 开始日期（默认 20240101，全量更新使用 20200101）
        markets: 市场列表 ['SSE', 'SZSE']（默认全部）

    更新模式:
        - 增量更新: start_date='20240101'
        - 全量更新: start_date='20200101'
    """
    data = request.json
    if not data:
        return jsonify({'success': False, 'error': '请求数据为空'}), 400

    start_date = data.get('start_date', '20240101')
    markets = data.get('markets', ['SSE', 'SZSE'])

    logger.info(f"/api/index/update called: start_date={start_date}, markets={markets}")

    # Create task
    try:
        task_id = get_task_manager().create_task('update_indices', {
            'start_date': start_date,
            'markets': markets
        })
    except TaskExistsError as e:
        existing = e.existing_task
        return jsonify({
            'success': False,
            'error': f'无法创建新任务：系统中已有任务在运行',
            'error_type': 'task_exists',
            'existing_task': existing
        }), 409

    # Start background thread
    def run_index_update():
        try:
            logger.info(f"[THREAD] Index update thread started for task {task_id}")
            tm = get_task_manager()
            tm.update_task(task_id, status='running', message='正在更新指数数据...')

            from src.data_sources.tushare import TushareDB

            db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))
            end_date = datetime.now().strftime('%Y%m%d')

            # Track results
            detailed_results = {
                'success': [],
                'failed': [],
                'skipped': []
            }

            # Get index list
            print(f"[TASK-{task_id[:8]}] Getting index list...")
            all_indices = []

            for market in markets:
                try:
                    count = db.save_index_basic(market=market)
                    # 无论数据是新插入还是已存在，都从数据库读取指数代码
                    if count >= 0:  # count >= 0 表示有数据（包括已存在的情况）
                        # Read index codes from database
                        query = "SELECT ts_code FROM index_names"
                        if market == 'SSE':
                            query += " WHERE ts_code LIKE '%.SH'"
                        elif market == 'SZSE':
                            query += " WHERE ts_code LIKE '%.SZ'"

                        with db.engine.connect() as conn:
                            df = pd.read_sql_query(query, conn)
                            all_indices.extend(df['ts_code'].tolist())
                        print(f"[TASK-{task_id[:8]}] {market}: 获取到 {len(df['ts_code'].tolist())} 个指数")
                except Exception as e:
                    print(f"  ❌ 获取 {market} 指数列表失败: {e}")

            if not all_indices:
                tm.update_task(task_id, status='failed', message='没有找到指数')
                return

            # Remove duplicates
            all_indices = list(set(all_indices))
            total_indices = len(all_indices)

            tm.update_task(task_id, message=f'开始更新 {total_indices} 个指数...')

            # Update each index
            for i, ts_code in enumerate(all_indices):
                # Check stop request
                if tm.is_stop_requested(task_id):
                    tm.update_task(task_id, status='stopped', message='任务已停止')
                    tm.clear_stop_request(task_id)
                    return

                try:
                    progress = int((i / total_indices) * 100)
                    tm.update_task(task_id,
                        progress=progress,
                        message=f'正在更新 {ts_code} ({i+1}/{total_indices})...'
                    )

                    result = db.save_index_daily(ts_code, start_date, end_date)

                    # Get index name from database
                    clean_code = ts_code.split('.')[0]
                    try:
                        from src.utils.stock_lookup import _load_index_names
                        index_names_dict = _load_index_names()
                        index_name = index_names_dict.get(ts_code, ts_code)
                    except:
                        index_name = ts_code

                    if result > 0:
                        tm.increment_stats(task_id, 'success')
                        detailed_results['success'].append(f"{clean_code} {index_name}")
                    elif result == 0:
                        tm.increment_stats(task_id, 'skipped')
                        detailed_results['skipped'].append(f"{clean_code} {index_name}")
                    else:
                        tm.increment_stats(task_id, 'failed')
                        detailed_results['failed'].append(f"{clean_code} {index_name}")

                except Exception as e:
                    tm.increment_stats(task_id, 'failed')
                    detailed_results['failed'].append(f"{ts_code}(错误: {str(e)})")
                    print(f"[ERROR] Failed to update index {ts_code}: {e}")

            # Complete
            final_stats = tm.get_task(task_id).get('stats', {})
            summary_stats = {
                'total': total_indices,
                'success': final_stats.get('success', 0),
                'failed': final_stats.get('failed', 0),
                'skipped': final_stats.get('skipped', 0),
                'details': detailed_results
            }

            tm.update_task(task_id,
                status='completed',
                message='指数数据更新完成',
                progress=100,
                result=summary_stats
            )
            logger.info(f"[THREAD] Index update task {task_id} completed")

        except Exception as e:
            import traceback
            traceback.print_exc()
            get_task_manager().update_task(task_id,
                status='failed',
                message=f'指数数据更新失败: {str(e)}'
            )
            logger.error(f"[THREAD] Index update task {task_id} failed: {e}")

    logger.info(f"Starting index update background thread...")
    thread = threading.Thread(target=run_index_update)
    thread.daemon = True
    thread.start()
    logger.info(f"Index update background thread started")

    return jsonify({'success': True, 'task_id': task_id})


@bp.route('/api/update/both', methods=['POST'])
def update_both_data():
    """
    组合更新API - 先更新股价数据，再更新财务数据

    Body (JSON):
        stock_range: "all" | "favorites" | "custom"
        mode: "incremental" | "full" (for stock data)
        custom_stocks: list of stock codes
    """
    data = request.json
    if not data:
        return jsonify({'success': False, 'error': '请求数据为空'}), 400

    mode = data.get('mode', 'incremental')
    stock_range = data.get('stock_range', 'all')
    custom_stocks = data.get('custom_stocks', [])

    logger.info(f"/api/update/both called: mode={mode}, stock_range={stock_range}")

    # Get stock list based on range (reuse logic from stock update)
    stock_list = []
    if stock_range == 'custom':
        if isinstance(custom_stocks, str):
            stock_list = [s.strip() for s in custom_stocks.split(',') if s.strip()]
        else:
            stock_list = custom_stocks
    elif stock_range == 'favorites':
        if custom_stocks:
            if isinstance(custom_stocks, str):
                stock_list = [s.strip() for s in custom_stocks.split(',') if s.strip()]
            else:
                stock_list = custom_stocks
        else:
            favorites = session.get('favorites', [])
            stock_list = favorites if favorites else ["600382", "600711", "000001"]
    else:  # all
        # 从 Tushare API 获取股票列表（避免包含指数代码）
        try:
            db, query = get_db()
            if db:
                stock_list_df = db._retry_api_call(
                    db.pro.stock_basic,
                    exchange='',
                    list_status='L',
                    fields='ts_code'
                )
                if stock_list_df is not None and not stock_list_df.empty:
                    stock_list = stock_list_df['ts_code'].tolist()
                else:
                    return jsonify({'success': False, 'error': '获取股票列表失败'}), 500
        except Exception as e:
            return jsonify({'success': False, 'error': f'获取股票列表失败: {str(e)}'}), 500

    if not stock_list:
        return jsonify({'success': False, 'error': '股票列表为空'}), 400

    # Create task with metadata
    logger.info(f"Creating combined update task...")
    try:
        task_id = get_task_manager().create_task(
            'update_both',
            {'mode': mode, 'stock_range': stock_range, 'custom_stocks': custom_stocks},
            metadata={'total_stocks': len(stock_list)}
        )
        logger.info(f"Combined update task created: {task_id}")
    except TaskExistsError as e:
        existing = e.existing_task
        task_id_short = existing['task_id'][:8]
        status_label = {
            'pending': '等待中',
            'running': '运行中',
            'paused': '已暂停'
        }.get(existing['status'], existing['status'])

        error_message = f"""无法创建新任务：系统中已有任务在{status_label}

现有任务信息：
  - 任务ID: {task_id_short}...
  - 任务状态: {status_label}
  - 创建时间: {existing.get('created_at', 'unknown')}
  - 当前进度: {existing.get('current_stock_index', 0)}/{existing.get('total_stocks', 0)}

请先在"任务历史"页面停止现有任务后再创建新任务。""".strip()

        logger.warning(f"Task creation blocked: {str(e)}")
        return jsonify({
            'success': False,
            'error': error_message,
            'error_type': 'task_exists',
            'existing_task': {
                'task_id': existing['task_id'],
                'status': existing['status'],
                'created_at': existing.get('created_at'),
                'progress': existing.get('progress', 0),
                'current_stock_index': existing.get('current_stock_index', 0),
                'total_stocks': existing.get('total_stocks', 0)
            }
        }), 409

    # Start background thread
    def run_combined_update():
        try:
            logger.info(f"[THREAD] Combined update thread started for task {task_id}")
            tm = get_task_manager()
            tm.update_task(task_id, status='running', message='正在更新股价数据...')

            from src.data_sources.tushare import TushareDB

            db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))
            end_date = datetime.now().strftime('%Y%m%d')

            # Track results for each stock
            detailed_results = {
                'success': [],
                'failed': [],
                'skipped': []
            }

            # Load checkpoint if resuming
            checkpoint = get_task_manager().load_checkpoint(task_id)
            start_index = 0
            stage = checkpoint.get('stage', 'stock') if checkpoint else 'stock'
            stats = checkpoint.get('stats', {'success': 0, 'failed': 0, 'skipped': 0}) if checkpoint else {'success': 0, 'failed': 0, 'skipped': 0}
            start_index = checkpoint.get('current_index', 0) if checkpoint else 0

            get_task_manager().update_task(task_id, stats=stats)
            print(f"[COMBINED UPDATE] Resuming from checkpoint: stage={stage}, index={start_index}")

            # Stage 1: Stock data update
            if stage == 'stock':
                tm.update_task(task_id, message='阶段 1/2: 正在更新股价数据...')
                for i in range(start_index, len(stock_list)):
                    stock_code = stock_list[i]

                    # Check stop request
                    if tm.is_stop_requested(task_id):
                        tm.save_checkpoint(task_id, i, tm.get_task(task_id).get('stats'), stage='stock')
                        tm.update_task(task_id, status='stopped', message='任务已停止')
                        tm.clear_stop_request(task_id)
                        return

                    # Check pause request
                    if tm.is_pause_requested(task_id):
                        tm.save_checkpoint(task_id, i, tm.get_task(task_id).get('stats'), stage='stock')
                        tm.update_task(task_id, status='paused', message='任务已暂停')
                        while True:
                            time.sleep(1)
                            if tm.is_stop_requested(task_id):
                                tm.update_task(task_id, status='stopped', message='任务已停止')
                                tm.clear_stop_request(task_id)
                                tm.clear_pause_request(task_id)
                                return
                            if not tm.is_pause_requested(task_id):
                                tm.update_task(task_id, message='任务已恢复')
                                break

                    try:
                        progress = int((i / len(stock_list)) * 50)  # Stock update is 50% of total
                        get_task_manager().update_task(task_id,
                            current_stock_index=i,
                            progress=progress,
                            message=f'阶段 1/2: 正在更新股价 {stock_code} ({i+1}/{len(stock_list)})...'
                        )

                        if i % 10 == 0:
                            current_task = get_task_manager().get_task(task_id)
                            get_task_manager().save_checkpoint(task_id, i, current_task.get('stats') if current_task else None, stage='stock')

                        if tm.is_stop_requested(task_id):
                            get_task_manager().update_task(task_id, status='stopped', message='任务已停止')
                            tm.clear_stop_request(task_id)
                            return

                        # Update stock data
                        print(f"[TASK-{task_id[:8]}] Starting stock update for {stock_code}...")
                        if mode == 'full':
                            stats = db.save_all_stocks_by_code(
                                default_start_date='20200101',
                                end_date=end_date,
                                stock_list=[stock_code]
                            )
                        else:  # incremental
                            stats = db.save_all_stocks_by_code_incremental(
                                default_start_date='20240101',
                                end_date=end_date,
                                stock_list=[stock_code]
                            )
                        print(f"[TASK-{task_id[:8]}] Stock update for {stock_code} complete")

                        # Update stats
                        if stats:
                            if stats.get('success', 0) > 0:
                                get_task_manager().increment_stats(task_id, 'success')
                            if stats.get('failed', 0) > 0:
                                get_task_manager().increment_stats(task_id, 'failed')
                            if stats.get('skipped', 0) > 0:
                                get_task_manager().increment_stats(task_id, 'skipped')
                        else:
                            get_task_manager().increment_stats(task_id, 'failed')

                    except Exception as e:
                        get_task_manager().increment_stats(task_id, 'failed')
                        print(f"[ERROR] Failed to update stock data for {stock_code}: {e}")

                # Reset index for financial update stage
                start_index = 0

            # Stage 2: Financial data update
            tm.update_task(task_id, message='阶段 2/2: 正在更新财务数据...')
            for i in range(start_index, len(stock_list)):
                stock_code = stock_list[i]

                # Check stop request
                if tm.is_stop_requested(task_id):
                    tm.save_checkpoint(task_id, i, tm.get_task(task_id).get('stats'), stage='financial')
                    tm.update_task(task_id, status='stopped', message='任务已停止')
                    tm.clear_stop_request(task_id)
                    return

                # Check pause request
                if tm.is_pause_requested(task_id):
                    tm.save_checkpoint(task_id, i, tm.get_task(task_id).get('stats'), stage='financial')
                    tm.update_task(task_id, status='paused', message='任务已暂停')
                    while True:
                        time.sleep(1)
                        if tm.is_stop_requested(task_id):
                            tm.update_task(task_id, status='stopped', message='任务已停止')
                            tm.clear_stop_request(task_id)
                            tm.clear_pause_request(task_id)
                            return
                        if not tm.is_pause_requested(task_id):
                            tm.update_task(task_id, message='任务已恢复')
                            break

                try:
                    progress = 50 + int((i / len(stock_list)) * 50)  # Financial update is 50-100%
                    get_task_manager().update_task(task_id,
                        current_stock_index=i,
                        progress=progress,
                        message=f'阶段 2/2: 正在更新财务数据 {stock_code} ({i+1}/{len(stock_list)})...'
                    )

                    if i % 10 == 0:
                        current_task = get_task_manager().get_task(task_id)
                        get_task_manager().save_checkpoint(task_id, i, current_task.get('stats') if current_task else None, stage='financial')

                    if tm.is_stop_requested(task_id):
                        get_task_manager().update_task(task_id, status='stopped', message='任务已停止')
                        tm.clear_stop_request(task_id)
                        return

                    # Check latest financial date and determine start_date
                    print(f"[TASK-{task_id[:8]}] Checking latest financial date for {stock_code}...")
                    latest_date = db.get_latest_financial_date(stock_code, 'income')

                    if latest_date:
                        start_date = latest_date
                        update_type = "增量更新"
                    else:
                        start_date = '20200101'
                        update_type = "首次下载"

                    print(f"[TASK-{task_id[:8]}] {stock_code}: Financial {update_type} (from {start_date})")

                    # Download financial data
                    result = db.save_all_financial(stock_code, start_date, end_date)
                    print(f"[TASK-{task_id[:8]}] Financial update for {stock_code} complete: {result} records")

                    # Update stats
                    if result and result > 0:
                        get_task_manager().increment_stats(task_id, 'success')
                    elif result == 0:
                        get_task_manager().increment_stats(task_id, 'skipped')
                    else:
                        get_task_manager().increment_stats(task_id, 'failed')

                except Exception as e:
                    get_task_manager().increment_stats(task_id, 'failed')
                    print(f"[ERROR] Failed to update financial data for {stock_code}: {e}")

            # Complete
            get_task_manager().delete_checkpoint(task_id)
            final_stats = get_task_manager().get_task(task_id).get('stats', {})

            summary_stats = {
                'total': len(stock_list),
                'success': final_stats.get('success', 0),
                'failed': final_stats.get('failed', 0),
                'skipped': final_stats.get('skipped', 0),
                'details': detailed_results
            }

            get_task_manager().update_task(task_id,
                status='completed',
                message='股价和财务数据更新完成',
                current_stock_index=len(stock_list),
                progress=100,
                result=summary_stats
            )
            logger.info(f"[THREAD] Combined update task {task_id} completed")

        except Exception as e:
            import traceback
            traceback.print_exc()
            get_task_manager().update_task(task_id,
                status='failed',
                message=f'组合更新失败: {str(e)}'
            )
            logger.error(f"[THREAD] Combined update task {task_id} failed: {e}")

    logger.info(f"Starting combined update background thread...")
    thread = threading.Thread(target=run_combined_update)
    thread.daemon = True
    thread.start()
    logger.info(f"Combined update background thread started")

    return jsonify({'success': True, 'task_id': task_id})


# ==================== Scheduled Jobs API ====================

@bp.route('/api/schedule/jobs', methods=['GET'])
def get_scheduled_jobs():
    """获取所有定时任务"""
    try:
        from web.scheduler import get_scheduled_jobs
        jobs = get_scheduled_jobs()
        return jsonify({'success': True, 'jobs': jobs})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@bp.route('/api/schedule/jobs', methods=['POST'])
def create_scheduled_job():
    """
    创建定时任务

    Body (JSON):
        name: 任务名称
        cron_expression: Cron表达式
        content_type: 内容类型 (stock/index) - 默认为stock
        mode: 更新模式 (incremental/full)
        stock_range: 股票范围 (all/favorites/custom) - 仅股票类型
        custom_stocks: 自定义股票列表 (可选) - 仅股票类型
        markets: 市场选择 (['SSE', 'SZSE']) - 仅指数类型
    """
    try:
        from web.scheduler import init_scheduler, add_scheduled_job
        from web.scheduled_jobs import register_job_config

        data = request.json
        if not data:
            return jsonify({'success': False, 'error': '请求数据为空'}), 400

        name = data.get('name')
        cron_expression = data.get('cron_expression')
        content_type = data.get('content_type', 'stock')  # 默认为stock

        if not name or not cron_expression:
            return jsonify({'success': False, 'error': '任务名称和Cron表达式必填'}), 400

        # Initialize scheduler if needed
        sched = init_scheduler(str(SCHEDULE_DB_PATH), timezone=SCHEDULER_TIMEZONE)

        # Register job config
        job_id = f"{content_type}_update_{name}"
        config = {
            'name': name,
            'content_type': content_type,
            'mode': data.get('mode', 'incremental')
        }

        # 根据内容类型添加不同的配置
        if content_type == 'index':
            config['markets'] = data.get('markets', ['SSE', 'SZSE'])
        else:  # stock
            config['stock_range'] = data.get('stock_range', 'all')
            config['custom_stocks'] = data.get('custom_stocks', [])

        register_job_config(job_id, config)

        # Add the scheduled job using dispatcher function
        logger.info(f"[Schedule] Adding job: {job_id}, cron: {cron_expression}, type: {content_type}")
        success = add_scheduled_job(
            job_id=job_id,
            func='web.scheduled_jobs:run_scheduled_job_dispatcher',
            cron_expression=cron_expression,
            name=name,
            func_kwargs={'job_id': job_id}
        )
        logger.info(f"[Schedule] Add job result: {success}")

        if success:
            return jsonify({'success': True, 'job_id': job_id})
        else:
            return jsonify({'success': False, 'error': '添加定时任务失败'}), 500

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@bp.route('/api/schedule/jobs/<job_id>', methods=['DELETE'])
def delete_scheduled_job(job_id):
    """删除定时任务"""
    try:
        from web.scheduler import remove_scheduled_job
        from web.scheduled_jobs import unregister_job_config

        # Remove from scheduler and unregister config
        if remove_scheduled_job(job_id):
            unregister_job_config(job_id)  # Also remove from config storage
            return jsonify({'success': True})
        return jsonify({'success': False, 'error': '删除任务失败'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@bp.route('/api/schedule/jobs/<job_id>/pause', methods=['POST'])
def pause_scheduled_job(job_id):
    """暂停定时任务"""
    try:
        from web.scheduler import pause_scheduled_job
        if pause_scheduled_job(job_id):
            return jsonify({'success': True})
        return jsonify({'success': False, 'error': '暂停任务失败'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@bp.route('/api/schedule/jobs/<job_id>/resume', methods=['POST'])
def resume_scheduled_job(job_id):
    """恢复定时任务"""
    try:
        from web.scheduler import resume_scheduled_job
        if resume_scheduled_job(job_id):
            return jsonify({'success': True})
        return jsonify({'success': False, 'error': '恢复任务失败'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== Financial Data API ====================

@bp.route('/api/financial/summary/<symbol>', methods=['GET'])
def api_financial_summary(symbol):
    """获取股票财务摘要数据（最新期核心指标，包括财务指标和估值指标）"""
    try:
        from src.data_sources.query.financial_query import FinancialQuery
        import numpy as np
        fq = FinancialQuery('data/tushare_data.db')

        # 获取完整财务数据（包含财务指标）
        data = fq.query_all_financial(symbol, include_indicators=True)
        if not data or data['income'].empty:
            return jsonify({'success': False, 'error': '暂无财务数据'}), 404

        # 获取最新一期数据（合并报表，report_type=1）
        # 需要先过滤再检查是否为空，避免IndexError
        income_filtered = data['income'][data['income']['report_type'] == 1] if not data['income'].empty else pd.DataFrame()
        balance_filtered = data['balancesheet'][data['balancesheet']['report_type'] == 1] if not data['balancesheet'].empty else pd.DataFrame()
        cashflow_filtered = data['cashflow'][data['cashflow']['report_type'] == 1] if not data['cashflow'].empty else pd.DataFrame()

        # 处理财务指标数据（如果存在）
        indicator_df = data.get('fina_indicator', pd.DataFrame())
        indicator_filtered = indicator_df[indicator_df['report_type'] == 1] if not indicator_df.empty else pd.DataFrame()
        indicator = indicator_filtered.iloc[0] if not indicator_filtered.empty else None

        income = income_filtered.iloc[0] if not income_filtered.empty else None
        balance = balance_filtered.iloc[0] if not balance_filtered.empty else None
        cashflow = cashflow_filtered.iloc[0] if not cashflow_filtered.empty else None

        # 构建扁平化的摘要数据
        summary = {}

        # 基本信息
        if income is not None:
            summary['end_date'] = income.get('end_date')
            summary['ann_date'] = income.get('ann_date')

        # 利润指标
        if income is not None:
            summary['total_operate_revenue'] = income.get('total_revenue') or income.get('revenue')
            summary['operate_profit'] = income.get('operate_profit')
            summary['net_profit'] = income.get('n_income')
            summary['basic_eps'] = income.get('basic_eps')

        # 资产负债
        if balance is not None:
            summary['total_assets'] = balance.get('total_assets')
            summary['total_liability'] = balance.get('total_liab')
            summary['total_hldr_eqy_exc_min_int'] = balance.get('total_hldr_eqy_exc_min_int')

        # 现金流
        if cashflow is not None:
            summary['n_cashflow_act'] = cashflow.get('n_cashflow_act')
            summary['free_cashflow'] = cashflow.get('free_cashflow')
            summary['sales_cash'] = cashflow.get('c_fr_sale_sg')  # 销售收现

        # 财务指标（8个核心指标）
        if indicator is not None:
            # 盈利能力
            summary['roe'] = indicator.get('roe')  # 净资产收益率
            summary['roa'] = indicator.get('roa')  # 总资产报酬率
            summary['netprofit_margin'] = indicator.get('netprofit_margin')  # 销售净利率
            summary['grossprofit_margin'] = indicator.get('grossprofit_margin')  # 销售毛利率

            # 成长能力
            summary['or_yoy'] = indicator.get('or_yoy')  # 营业收入同比增长率
            summary['netprofit_yoy'] = indicator.get('netprofit_yoy')  # 净利润同比增长率

            # 偿债能力
            summary['current_ratio'] = indicator.get('current_ratio')  # 流动比率

            # 营运能力
            summary['assets_turn'] = indicator.get('assets_turn')  # 总资产周转率

        # 获取估值指标（PE、PB、市值等）
        try:
            code = fq._standardize_code(symbol)
            valuation_query = """
            SELECT datetime, close, pe, pe_ttm, pb, ps, ps_ttm, total_mv, circ_mv
            FROM bars
            WHERE symbol LIKE :symbol
            AND pe IS NOT NULL
            ORDER BY datetime DESC
            LIMIT 1
            """
            with fq.engine.connect() as conn:
                valuation_df = pd.read_sql_query(valuation_query, conn, params={'symbol': f'{code}%'})

            if not valuation_df.empty:
                valuation = valuation_df.iloc[0]
                summary['valuation_date'] = valuation.get('datetime')
                summary['close'] = valuation.get('close')
                summary['pe'] = valuation.get('pe')
                summary['pe_ttm'] = valuation.get('pe_ttm')
                summary['pb'] = valuation.get('pb')
                summary['ps'] = valuation.get('ps')
                summary['ps_ttm'] = valuation.get('ps_ttm')
                summary['total_mv'] = valuation.get('total_mv')
                summary['circ_mv'] = valuation.get('circ_mv')
        except Exception as e:
            # 估值指标获取失败不影响整体结果
            logger.warning(f"Failed to get valuation for {symbol}: {e}")

        # 清理NaN值
        for key, value in summary.items():
            if isinstance(value, float) and (np.isnan(value) or str(value) == 'nan'):
                summary[key] = None

        return jsonify({
            'success': True,
            'symbol': symbol,
            'summary': summary
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@bp.route('/api/financial/full/<symbol>', methods=['GET'])
def api_financial_full(symbol):
    """获取完整财务报表（最近8个季度）和估值指标（最近30天）"""
    try:
        from src.data_sources.query.financial_query import FinancialQuery
        import numpy as np
        fq = FinancialQuery('data/tushare_data.db')

        data = fq.query_all_financial(symbol)
        if not data or data['income'].empty:
            return jsonify({'success': False, 'error': '暂无财务数据'}), 404

        # 转换DataFrame为列表，限制最近8条，并处理NaN值
        result = {}
        for table_type, df in data.items():
            df_sorted = df.sort_values('end_date', ascending=False).head(8)
            # 将NaN替换为None，避免JSON解析错误
            df_clean = df_sorted.replace({np.nan: None})
            result[table_type] = df_clean.to_dict('records')

        # 获取估值指标（最近30天的数据）
        try:
            code = fq._standardize_code(symbol)
            valuation_query = """
            SELECT datetime, close, pe, pe_ttm, pb, ps, ps_ttm,
                   total_mv / 10000 as total_mv_yi,
                   circ_mv / 10000 as circ_mv_yi
            FROM bars
            WHERE symbol LIKE :symbol
            AND pe IS NOT NULL
            ORDER BY datetime DESC
            LIMIT 30
            """
            with fq.engine.connect() as conn:
                valuation_df = pd.read_sql_query(valuation_query, conn, params={'symbol': f'{code}%'})

            if not valuation_df.empty:
                # 将NaN替换为None
                valuation_df = valuation_df.replace({np.nan: None})
                result['valuation'] = valuation_df.to_dict('records')
        except Exception as e:
            # 估值指标获取失败不影响整体结果
            logger.warning(f"Failed to get valuation for {symbol}: {e}")
            result['valuation'] = []

        return jsonify({
            'success': True,
            'symbol': symbol,
            'data': result
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@bp.route('/api/financial/check/<symbol>', methods=['GET'])
def api_financial_check(symbol):
    """检查股票是否有财务数据"""
    try:
        from src.data_sources.query.financial_query import FinancialQuery
        fq = FinancialQuery('data/tushare_data.db')

        latest_date = fq.get_latest_report_date(symbol, 'income')
        has_data = latest_date is not None

        return jsonify({
            'success': True,
            'symbol': symbol,
            'has_data': has_data,
            'latest_date': latest_date
        })
    except Exception as e:
        return jsonify({'success': True, 'has_data': False})


@bp.route('/api/financial/indicators/<symbol>', methods=['GET'])
def api_financial_indicators(symbol):
    """获取股票财务指标数据（最近8个季度）"""
    try:
        from src.data_sources.query.financial_query import FinancialQuery
        import numpy as np
        fq = FinancialQuery('data/tushare_data.db')

        # 查询财务指标数据
        df = fq.query_fina_indicator(symbol)
        if df.empty:
            return jsonify({'success': False, 'error': '暂无财务指标数据'}), 404

        # 限制最近8条，并处理NaN值
        df_sorted = df.sort_values('end_date', ascending=False).head(8)
        df_clean = df_sorted.replace({np.nan: None})

        return jsonify({
            'success': True,
            'symbol': symbol,
            'data': df_clean.to_dict('records')
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@bp.route('/api/financial/valuation/<symbol>', methods=['GET'])
def api_financial_valuation(symbol):
    """获取股票最新估值指标（PE、PB、市值等）"""
    try:
        from src.data_sources.tushare import TushareDB
        db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

        # 标准化代码
        code = db._extract_stock_code(symbol)
        ts_code_std = db._standardize_code(symbol)

        # 查询最新的估值指标（日线数据）
        query = f"""
        SELECT datetime, close, pe, pe_ttm, pb, ps, ps_ttm, total_mv, circ_mv
        FROM bars
        WHERE symbol LIKE :symbol
        AND pe IS NOT NULL
        ORDER BY datetime DESC
        LIMIT 1
        """

        import pandas as pd
        with db.engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params={'symbol': f'{code}%'})

        if df.empty:
            return jsonify({'success': False, 'error': '暂无估值数据'}), 404

        # 转换为字典并处理NaN值
        import numpy as np
        result = df.iloc[0].to_dict()
        for key, value in result.items():
            if isinstance(value, float) and (np.isnan(value) or str(value) == 'nan'):
                result[key] = None

        return jsonify({
            'success': True,
            'symbol': symbol,
            'valuation': result
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


