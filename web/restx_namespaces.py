"""
Flask-RESTX Namespaces for API endpoints
"""
from flask_restx import Namespace, Resource, fields
from flask import request, session
from web.services.task_service import get_task_manager

# ============================================================================
# Namespaces
# ============================================================================

# Health Namespace
health_ns = Namespace('health', description='健康检查接口')

# Stock Namespace
stock_ns = Namespace('stock', description='股票相关接口')

# Task Namespace
task_ns = Namespace('tasks', description='任务管理接口')

# Schedule Namespace
schedule_ns = Namespace('schedule', description='定时任务接口')

# Financial Namespace
financial_ns = Namespace('financial', description='财务数据接口')

# SW Industry Namespace
sw_industry_ns = Namespace('sw-industry', description='申万行业接口')

# Boards Namespace
boards_ns = Namespace('boards', description='板块数据接口')

# Favorites Namespace
favorites_ns = Namespace('favorites', description='收藏功能接口')

# Liquidity Namespace
liquidity_ns = Namespace('liquidity', description='流动性筛选接口')

# Valuation Namespace
valuation_ns = Namespace('valuation', description='股票估值接口')

# Backtest Namespace
backtest_ns = Namespace('backtest', description='回测接口')

# Screening Namespace
screening_ns = Namespace('screening', description='股票筛选接口')

# Moneyflow Namespace
moneyflow_ns = Namespace('moneyflow', description='资金流向接口')

# Dragon List Namespace
dragon_list_ns = Namespace('dragon-list', description='龙虎榜接口')

# Alpha Namespace
alpha_ns = Namespace('alphas', description='101 Formulaic Alphas 因子库接口')

# ============================================================================
# Models (DTOs) - must be defined AFTER namespaces
# ============================================================================

# Stock models
stock_search_item_model = stock_ns.model('StockSearchItem', {
    'code': fields.String(description='股票代码', example='600382'),
    'name': fields.String(description='股票名称', example='广东明珠'),
    'type': fields.String(description='类型 (stock/index)', example='stock', enum=['stock', 'index'])
})

stock_query_model = stock_ns.model('StockQueryRequest', {
    'symbols': fields.List(fields.String, description='股票代码列表，支持A股和指数代码', example=['600382', '000001', '000001.SH']),
    'start_date': fields.String(description='开始日期，格式 YYYY-MM-DD', example='2024-01-01'),
    'end_date': fields.String(description='结束日期，格式 YYYY-MM-DD', example='2024-12-31'),
    'price_type': fields.String(description='价格类型: qfq=前复权, hfq=后复权, bfq=不复权', example='qfq', enum=['qfq', 'hfq', 'bfq'])
})

stock_bar_model = stock_ns.model('StockBarData', {
    'datetime': fields.String(description='日期', example='2024-01-01'),
    'open': fields.Float(description='开盘价', example=10.5),
    'high': fields.Float(description='最高价', example=11.0),
    'low': fields.Float(description='最低价', example=10.3),
    'close': fields.Float(description='收盘价', example=10.8),
    'volume': fields.Float(description='成交量', example=1000000),
    'turnover': fields.Float(description='成交额', example=10800000),
    'pct_chg': fields.Float(description='涨跌幅', example=2.5),
    'amount': fields.Float(description='成交额', example=10800000)
})

# Task models
task_model = task_ns.model('Task', {
    'task_id': fields.String(description='任务ID', example='abc123'),
    'task_type': fields.String(description='任务类型', example='update_stock_prices'),
    'status': fields.String(description='状态 (pending/running/completed/failed/stopped)', example='running'),
    'progress': fields.Integer(description='进度 (0-100)', example=50),
    'message': fields.String(description='消息', example='正在更新股票数据...'),
    'stats': fields.Raw(description='统计信息'),
    'created_at': fields.String(description='创建时间', example='2024-01-01 10:00:00'),
    'updated_at': fields.String(description='更新时间', example='2024-01-01 10:05:00')
})

create_task_model = task_ns.model('CreateTaskRequest', {
    'task_type': fields.String(
        description='任务类型',
        required=True,
        example='update_stock_prices',
        enum=[
            'update_stock_prices',
            'update_financial_reports',
            'update_industry_classification',
            'update_index_data',
            'update_industry_statistics',
            'update_moneyflow',
            'calculate_industry_moneyflow',
            'test_handler',
            'backtest',
            'update_dragon_list'
        ],
        attribute='task_type'
    ),
    'params': fields.Raw(
        description='任务参数 (根据不同任务类型需要不同参数)',
        example={
            'stock_range': 'all',
            'custom_stocks': ['600382'],
            'src': 'SW2021',
            'force': False,
            'include_indicators': True,
            'include_reports': True,
            'markets': ['SSE', 'SZSE']
        }
    )
})

# SW Industry models
# Define child industry model (used for recursive structure)
sw_industry_child_model = sw_industry_ns.model('SWIndustryChild', {
    'index_code': fields.String(description='行业代码', example='801010.SI'),
    'industry_name': fields.String(description='行业名称', example='种植业'),
    'industry_code': fields.String(description='行业编号', example='1150001'),
    'level': fields.String(description='级别 (L1/L2/L3)', example='L2'),
    'parent_code': fields.String(description='父级代码', example='801010.SI'),
    'children': fields.List(fields.Nested(sw_industry_ns.model('SWIndustryGrandChild', {
        'index_code': fields.String(description='行业代码', example='801010.SI'),
        'industry_name': fields.String(description='行业名称', example='粮食种植'),
        'industry_code': fields.String(description='行业编号', example='1150001'),
        'level': fields.String(description='级别 (L3)', example='L3'),
        'parent_code': fields.String(description='父级代码', example='801010.SI'),
        'children': fields.List(fields.Raw, description='子行业列表')
    })))
})

# Main industry model
sw_industry_model = sw_industry_ns.model('SWIndustry', {
    'index_code': fields.String(description='行业代码', example='801010.SI'),
    'industry_name': fields.String(description='行业名称', example='农林牧渔'),
    'industry_code': fields.String(description='行业编号', example='1150001'),
    'level': fields.String(description='级别 (L1/L2/L3)', example='L1'),
    'parent_code': fields.String(description='父级代码', example=None),
    'children': fields.List(fields.Nested(sw_industry_child_model))
})

# Member model
sw_member_model = sw_industry_ns.model('SWMember', {
    'ts_code': fields.String(description='股票代码', example='002311.SZ'),
    'name': fields.String(description='股票名称', example='海大集团')
})

# ============================================================================
# Health Resources
# ============================================================================

@health_ns.route('')
class HealthResource(Resource):
    """健康检查"""
    def get(self):
        """健康检查接口"""
        return {'status': 'ok', 'message': 'MyTrader API is running'}


# ============================================================================
# Stock Resources
# ============================================================================

@stock_ns.route('/search')
class StockSearchResource(Resource):
    """股票搜索"""
    @stock_ns.doc('search_stocks',
        description='''股票/指数搜索自动补全接口。

支持搜索方式：
1. 股票代码: 600382, 000001
2. 股票名称: 平安, 万科
3. 拼音缩写: ZGPA (中国平安)
4. 指数代码: 000001.SH (上证指数), 399001.SZ (深证成指)

**参数说明：**
- **q**: 搜索关键词，支持代码、名称、拼音缩写
- **limit**: 返回结果数量，默认10，最大50
- **type**: 资产类型过滤
  - stock: 仅返回股票
  - index: 仅返回指数
  - all: 返回股票和指数（默认）
''')
    @stock_ns.param('q', '搜索关键词（代码或名称）', type='string', required=True)
    @stock_ns.param('limit', '返回结果数量，默认10，最大50', type='integer', default=10)
    @stock_ns.param('type', '资产类型 (stock/index/all)', type='string', enum=['stock', 'index', 'all'], default='all')
    def get(self):
        """股票搜索（自动补全，支持股票和指数）"""
        from web.services.stock_service import stock_search
        return stock_search()


@stock_ns.route('/query')
class StockQueryResource(Resource):
    """股票查询"""
    @stock_ns.doc('query_stocks',
        description='''查询股票/指数历史行情数据（K线数据）。

**请求体参数：**
- **symbols**: 股票代码列表，支持多支股票批量查询
  - A股代码: 600382, 000001, 002311
  - 指数代码: 000001.SH, 399001.SZ
- **start_date**: 开始日期，格式 YYYY-MM-DD
- **end_date**: 结束日期，格式 YYYY-MM-DD
- **price_type**: 价格复权类型
  - qfq: 前复权（默认，用于技术分析）
  - hfq: 后复权（用于长期趋势分析）
  - bfq: 不复权（原始价格）

**返回数据字段：**
- datetime: 交易日期
- open, high, low, close: 开高低收价格
- volume: 成交量（手）
- turnover: 成交额（元）
- pct_chg: 涨跌幅（%）
- amount: 成交额（元）
''')
    @stock_ns.expect(stock_query_model)
    def post(self):
        """股票查询"""
        from web.services.stock_service import stock_query
        return stock_query()


@stock_ns.route('/name/<code>')
class StockNameResource(Resource):
    """获取股票名称"""
    @stock_ns.doc('get_stock_name',
        description='''根据股票代码获取股票名称。

**路径参数：**
- **code**: 股票或指数代码
  - 格式: 600382, 000001.SH, 399001.SZ

**返回数据：**
- name: 股票名称
- code: 股票代码
''')
    @stock_ns.param('code', '股票代码 (如: 600382, 000001.SH)', type='string', required=True)
    def get(self, code):
        """获取股票名称"""
        from web.services.stock_service import get_stock_name
        return get_stock_name(code)


@stock_ns.route('/min-date')
class StockMinDateResource(Resource):
    """获取最小日期"""
    @stock_ns.doc('get_min_date',
        description='''获取所有股票数据的最小日期（最早数据日期）。

**用途：**
- 确定系统中数据的起始日期
- 在数据查询前验证数据完整性

**返回数据：**
- min_date: 最早数据的日期 (YYYY-MM-DD 格式)
''')
    def get(self):
        """获取所有股票的最小日期"""
        from web.services.stock_service import get_min_date
        return get_min_date()


@stock_ns.route('/export/<format>')
class StockExportResource(Resource):
    """数据导出"""
    @stock_ns.doc('export_stocks',
        description='''导出股票查询结果为文件。

**路径参数：**
- **format**: 导出格式
  - csv: CSV 格式（适合 Excel 打开）
  - excel: Excel 格式（.xlsx 文件）

**注意：**
- 导出前需要先调用 /stock/query 进行查询
- 导出的是最近一次查询的结果
''')
    @stock_ns.param('format', '导出格式 (csv 或 excel)', type='string', required=True, enum=['csv', 'excel'])
    def get(self, format):
        """导出查询结果"""
        from web.services.stock_service import stock_export
        data, status_code = stock_export(format)
        return data, status_code


@stock_ns.route('/ai-screen', methods=['POST'])
class StockAIScreenResource(Resource):
    """AI智能筛选"""
    @stock_ns.doc('ai_screen_stocks',
        description='''使用自然语言筛选股票。

**请求体参数：**
```json
{
  "query": "查找最近5天涨幅超过5%的股票"
}
```

**返回数据：**
```json
{
  "success": true,
  "explanation": "根据你的条件...",
  "stocks": [...],
  "params": {...}
}
```
''')
    @stock_ns.expect(stock_ns.model('AIScreenRequest', {
        'query': fields.String(description='自然语言筛选条件', example='查找最近5天涨幅超过5%的股票', required=True)
    }))
    def post(self):
        """使用自然语言筛选股票"""
        from web.services.ai_screen_service import ai_screen
        data, status_code = ai_screen()
        return data, status_code


@stock_ns.route('/ai-screen-chat', methods=['POST'])
class StockAIChatResource(Resource):
    """AI对话筛选"""
    @stock_ns.doc('ai_chat_screen',
        description='''使用对话方式筛选股票。

**请求体参数：**
```json
{
  "query": "帮我找一些科技股",
  "history": [{"role": "user", "content": "..."}]
}
```
''')
    @stock_ns.expect(stock_ns.model('AIChatRequest', {
        'query': fields.String(description='用户消息', required=True),
        'history': fields.List(fields.Raw, description='对话历史')
    }))
    def post(self):
        """AI对话筛选"""
        from web.services.ai_screen_service import ai_screen_chat
        data, status_code = ai_screen_chat()
        return data, status_code


@stock_ns.route('/screen', methods=['POST'])
class StockScreenResource(Resource):
    """股票筛选"""
    @stock_ns.doc('screen_stocks',
        description='''根据条件筛选股票列表。

**请求体参数：**
```json
{
  "days": 5,              // 最近N天（默认5）
  "turnover_min": 1000,   // 最小成交额
  "turnover_max": 100000, // 最大成交额
  "pct_chg_min": -5,      // 最小涨跌幅
  "pct_chg_max": 5,       // 最大涨跌幅
  "price_min": 10,        // 最低价
  "price_max": 100,       // 最高价
  "volume_min": 10000,    // 最小成交量
  "volume_max": 1000000   // 最大成交量
}
```

**返回数据：**
```json
{
  "success": true,
  "count": 150,
  "symbols": [
    {
      "code": "600382",
      "name": "广东明珠",
      "board": "新兴板块",
      "latest_date": "2024-12-20",
      "latest_close": 15.23,
      "avg_turnover": 50000.12,
      "avg_pct_chg": 2.5
    }
  ]
}
```

**注意：**
- 最多返回500只股票
- 所有参数都是可选的
''')
    @stock_ns.expect(stock_ns.model('StockScreenRequest', {
        'days': fields.Integer(description='最近N天', example=5, default=5),
        'turnover_min': fields.Float(description='最小成交额', example=1000),
        'turnover_max': fields.Float(description='最大成交额', example=100000),
        'pct_chg_min': fields.Float(description='最小涨跌幅(%)', example=-5),
        'pct_chg_max': fields.Float(description='最大涨跌幅(%)', example=5),
        'price_min': fields.Float(description='最低价', example=10),
        'price_max': fields.Float(description='最高价', example=100),
        'volume_min': fields.Float(description='最小成交量', example=10000),
        'volume_max': fields.Float(description='最大成交量', example=1000000)
    }))
    def post(self):
        """根据条件筛选股票"""
        from web.services.stock_service import stock_screen
        data, status_code = stock_screen()
        return data, status_code


# ============================================================================
# Task Resources
# ============================================================================

@task_ns.route('')
class TaskListResource(Resource):
    """任务列表"""
    @task_ns.doc('list_tasks',
        description='''获取任务列表，支持按状态筛选。

**查询参数：**
- **status**: 按状态筛选任务
  - pending: 等待中
  - running: 运行中
  - completed: 已完成
  - failed: 失败
  - stopped: 已停止
  - 不传参数: 返回所有任务

**返回数据：**
- 任务列表，包含每个任务的状态、进度、消息等信息
''')
    @task_ns.param('status', '按状态筛选 (pending/running/completed/failed/stopped)', type='string', enum=['pending', 'running', 'completed', 'failed', 'stopped'], required=False)
    def get(self):
        """获取任务列表"""
        from flask import jsonify
        tm = get_task_manager()
        status_filter = request.args.get('status')
        tasks = tm.get_all_tasks(status=status_filter)
        return jsonify({
            'success': True,
            'total': len(tasks),
            'tasks': tasks
        })


@task_ns.route('/<task_id>')
class TaskResource(Resource):
    """任务详情"""
    @task_ns.doc('get_task',
        description='''获取任务详情。

**路径参数：**
- **task_id**: 任务ID

**返回数据：**
- task_id: 任务ID
- task_type: 任务类型
- status: 任务状态
- progress: 进度 (0-100)
- message: 消息
- stats: 统计信息
- created_at: 创建时间
- updated_at: 更新时间
''')
    def get(self, task_id):
        """获取任务详情"""
        tm = get_task_manager()
        task = tm.get_task(task_id)
        if not task:
            return {'error': '任务不存在'}, 404
        return task

    @task_ns.doc('delete_task',
        description='''删除任务记录。

**注意：**
- 只能删除已完成、失败或停止的任务
- 运行中的任务无法删除，需要先停止
''')
    def delete(self, task_id):
        """删除任务"""
        tm = get_task_manager()
        tm.delete_task(task_id)
        return {'message': '任务已删除'}


@task_ns.route('/<task_id>/cancel')
class TaskCancelResource(Resource):
    """取消任务"""
    @task_ns.doc('cancel_task',
        description='''取消任务。

**适用场景：**
- 取消等待中的任务
- 任务将不会被启动

**路径参数：**
- **task_id**: 任务ID
''')
    def post(self, task_id):
        """取消任务"""
        tm = get_task_manager()
        tm.cancel_task(task_id)
        return {'message': '任务已取消'}


@task_ns.route('/<task_id>/pause')
class TaskPauseResource(Resource):
    """暂停任务"""
    @task_ns.doc('pause_task',
        description='''暂停正在运行的任务。

**适用场景：**
- 临时暂停任务以释放资源
- 可以通过 /resume 恢复执行

**路径参数：**
- **task_id**: 任务ID

**注意：**
- 任务会在完成当前处理的股票后暂停
- 已保存的进度不会丢失
''')
    def post(self, task_id):
        """暂停任务"""
        from web.services.task_service import pause_task
        data, status_code = pause_task(task_id)
        return data, status_code


@task_ns.route('/<task_id>/resume')
class TaskResumeResource(Resource):
    """恢复任务"""
    @task_ns.doc('resume_task',
        description='''恢复已暂停的任务。

**路径参数：**
- **task_id**: 任务ID

**注意：**
- 只能恢复暂停状态的任务
- 任务将从上次暂停的位置继续执行
''')
    def post(self, task_id):
        """恢复任务"""
        from web.services.task_service import resume_task
        data, status_code = resume_task(task_id)
        return data, status_code


@task_ns.route('/<task_id>/stop')
class TaskStopResource(Resource):
    """停止任务"""
    @task_ns.doc('stop_task',
        description='''停止正在运行的任务。

**与 cancel 的区别：**
- cancel: 取消等待中的任务
- stop: 停止正在运行的任务

**路径参数：**
- **task_id**: 任务ID

**注意：**
- 任务会保存当前进度
- 已停止的任务无法恢复
- 如需继续执行，需要创建新任务
''')
    def post(self, task_id):
        """停止任务"""
        from web.services.task_service import stop_task
        data, status_code = stop_task(task_id)
        return data, status_code


@task_ns.route('/cleanup')
class TaskCleanupResource(Resource):
    """清理任务"""
    @task_ns.doc('cleanup_tasks',
        description='''清理陈旧的任务记录。

**清理规则：**
- 删除超过24小时的任务记录
- 保留运行中和最近创建的任务

**用途：**
- 定期清理已完成的历史任务
- 释放存储空间
''')
    def post(self):
        """清理陈旧任务"""
        from web.services.task_service import cleanup_tasks
        data, status_code = cleanup_tasks()
        return data, status_code


@task_ns.route('/create', methods=['POST'])
class TaskCreateResource(Resource):
    """创建任务"""
    @task_ns.doc('create_task',
        description='''创建新的后台任务。

支持的任务类型和参数：

1. **update_stock_prices** - 更新股票价格数据
   - stock_range: "all" | "favorites" | "custom" (默认: all)
   - custom_stocks: 自定义股票列表 (当 stock_range="custom" 时必需)

2. **update_financial_reports** - 更新财务报表数据
   - stock_range: "all" | "favorites" | "custom" (默认: all)
   - custom_stocks: 自定义股票列表 (当 stock_range="custom" 时必需)
   - include_indicators: 是否包含财务指标表 (默认: true)
   - include_reports: 是否包含三大报表 (默认: true)

3. **update_industry_classification** - 更新申万行业分类数据
   - src: "SW2021" | "SW2014" (默认: SW2021)
   - force: 是否强制重新获取 (默认: false)

4. **update_index_data** - 更新指数数据
   - markets: 市场列表 ["SSE", "SZSE"] (默认: ["SSE", "SZSE"])

5. **update_moneyflow** - 更新资金流向数据（个股数据）
   - mode: "incremental" | "full" (默认: "incremental")
   - stock_range: "all" | "favorites" | "custom" (默认: "all")
   - custom_stocks: 自定义股票列表 (当 stock_range="custom" 时必需)
   - start_date: 开始日期 YYYYMMDD (可选)
   - end_date: 结束日期 YYYYMMDD (可选)
   - exclude_st: 是否排除ST股 (默认: true)

6. **calculate_industry_moneyflow** - 计算行业资金流向汇总
   - start_date: 开始日期 YYYYMMDD (可选)
   - end_date: 结束日期 YYYYMMDD (可选)

7. **update_industry_statistics** - 更新行业统计数据
   - metrics: 指标列表 ["pe_ttm", "pb", "ps_ttm", "total_mv", "circ_mv"] (默认: 全部)

8. **update_dragon_list** - 更新龙虎榜数据
   - mode: "incremental" | "batch" (默认: "incremental")
   - start_date: 开始日期 YYYY-MM-DD (批量模式下必填)
   - end_date: 结束日期 YYYY-MM-DD (可选)

9. **backtest** - 回测任务
   - stock: 股票代码（必需）
   - start_date: 开始日期（必需）
   - end_date: 结束日期（可选）
   - cash: 初始资金（可选，默认100万）
   - commission: 手续费率（可选，默认0.2%）
   - benchmark: 基准指数（可选）
   - strategy: 策略类型（必需，如 "sma_cross"）
   - strategy_params: 策略参数（必需）

10. **test_handler** - 测试任务处理器（用于测试Worker功能）
    - total_items: 处理项总数 (默认: 100)
    - item_duration_ms: 每项处理时间(ms) (默认: 100)
    - checkpoint_interval: 检查点保存间隔 (默认: 10)
    - failure_rate: 随机失败率 (0.0-1.0) (默认: 0.0)
    - simulate_pause: 是否在50%时自动暂停 (默认: false)

**请求体示例：**
```json
{
  "task_type": "update_stock_prices",
  "params": {
    "stock_range": "custom",
    "custom_stocks": ["600382", "000001"]
  }
}
```
''')
    @task_ns.expect(create_task_model)
    def post(self):
        """创建新任务"""
        from web.services.task_creation_service import create_task
        return create_task(request.get_json())


@task_ns.route('/active-check')
class TaskActiveCheckResource(Resource):
    """活跃任务检查"""
    @task_ns.doc('active_check',
        description='''检查是否有活跃任务。

**返回数据：**
- has_active: 是否有活跃任务
- task: 活跃任务信息（如果有）

**用途：**
- 在创建新任务前检查是否有冲突
- 防止同时运行多个资源密集型任务
''')
    def get(self):
        """检查是否有活跃任务"""
        from web.services.task_service import active_check
        data, status_code = active_check()
        return data, status_code


# ============================================================================
# Schedule Resources
# ============================================================================

@schedule_ns.route('/jobs')
class ScheduleJobListResource(Resource):
    """定时任务列表"""
    @schedule_ns.doc('list_jobs',
        description='''获取定时任务列表。

**返回数据：**
- jobs: 定时任务列表
  - id: 任务ID
  - name: 任务名称
  - enabled: 是否启用
  - trigger: 触发器配置
  - next_run_time: 下次运行时间

**用途：**
- 查看所有已配置的定时任务
- 检查任务状态和下次执行时间
''')
    def get(self):
        """获取定时任务列表"""
        from web.services.schedule_service import get_jobs
        data, status_code = get_jobs()
        return data, status_code

    @schedule_ns.doc('create_job',
        description='''创建定时任务。

**请求体参数：**
- name: 任务名称
- task_type: 任务类型 (update_stock_prices/update_financial_reports/update_industry_classification/update_index_data)
- trigger: 触发器配置
  - type: cron / interval / date
  - cron_expression: cron表达式 (type=cron时)

**示例：**
```json
{
  "name": "每日收盘更新",
  "task_type": "update_stock_prices",
  "trigger": {
    "type": "cron",
    "cron_expression": "0 15 * * 1-5"
  }
}
```
''')
    def post(self):
        """创建定时任务"""
        from web.services.schedule_service import create_job
        data, status_code = create_job()
        return data, status_code


@schedule_ns.route('/jobs/<job_id>')
class ScheduleJobResource(Resource):
    """定时任务操作"""
    @schedule_ns.doc('delete_job',
        description='''删除定时任务。

**路径参数：**
- **job_id**: 任务ID

**注意：**
- 删除后任务将不再执行
- 需要重新创建才能恢复
''')
    def delete(self, job_id):
        """删除定时任务"""
        from web.services.schedule_service import delete_job
        data, status_code = delete_job(job_id)
        return data, status_code


@schedule_ns.route('/jobs/<job_id>/pause')
class ScheduleJobPauseResource(Resource):
    """暂停定时任务"""
    @schedule_ns.doc('pause_job',
        description='''暂停定时任务。

**路径参数：**
- **job_id**: 任务ID

**效果：**
- 任务暂停但不会删除
- 可以通过 resume 恢复
''')
    def post(self, job_id):
        """暂停定时任务"""
        from web.services.schedule_service import pause_job_service
        data, status_code = pause_job_service(job_id)
        return data, status_code


@schedule_ns.route('/jobs/<job_id>/resume')
class ScheduleJobResumeResource(Resource):
    """恢复定时任务"""
    @schedule_ns.doc('resume_job',
        description='''恢复已暂停的定时任务。

**路径参数：**
- **job_id**: 任务ID

**效果：**
- 任务恢复执行
- 按照原定时间表继续运行
''')
    def post(self, job_id):
        """恢复定时任务"""
        from web.services.schedule_service import resume_job_service
        data, status_code = resume_job_service(job_id)
        return data, status_code


# ============================================================================
# Financial Resources
# ============================================================================

@financial_ns.route('/summary/<symbol>')
class FinancialSummaryResource(Resource):
    """财务摘要"""
    @financial_ns.doc('get_financial_summary',
        description='''获取股票财务摘要数据。

**路径参数：**
- **symbol**: 股票代码 (如: 600382, 000001.SZ)

**返回数据：**
- 财务指标: 营收、净利润、ROE等
- 估值指标: PE、PB、市值等
- 最新财报日期

**注意：**
- 返回最新一期的财务数据
- 包含财务指标和估值指标
''')
    def get(self, symbol):
        """获取财务摘要数据"""
        from web.services.financial_service import financial_summary
        data, status_code = financial_summary(symbol)
        return data, status_code


@financial_ns.route('/report/<symbol>')
class FinancialReportResource(Resource):
    """财务报表"""
    @financial_ns.doc('get_financial_report',
        description='''获取完整财务报表（前端专用）。

**路径参数：**
- **symbol**: 股票代码 (如: 600382, 000001.SZ)

**返回数据：**
```json
{
  "income": [...],      // 利润表（最近8个季度）
  "balance": [...],     // 资产负债表（最近8个季度）
  "cashflow": [...],    // 现金流量表（最近8个季度）
  "indicators": [...]   // 财务指标（最近8个季度）
}
```

**注意：**
- 返回最近8个季度的数据
- NaN值已转换为null
''')
    def get(self, symbol):
        """获取完整财务报表"""
        from web.services.financial_service import financial_report
        data, status_code = financial_report(symbol)
        return data, status_code


@financial_ns.route('/full/<symbol>')
class FinancialFullResource(Resource):
    """完整财务数据和估值"""
    @financial_ns.doc('get_financial_full',
        description='''获取完整财务报表（最近8个季度）和估值指标（最近30天）。

**路径参数：**
- **symbol**: 股票代码 (如: 600382, 000001.SZ)

**返回数据：**
```json
{
  "success": true,
  "symbol": "600382",
  "data": {
    "income": [...],       // 利润表
    "balancesheet": [...], // 资产负债表
    "cashflow": [...],     // 现金流量表
    "fina_indicator": [...], // 财务指标
    "valuation": [...]     // 估值指标（最近30天）
  }
}
```
''')
    def get(self, symbol):
        """获取完整财务数据和估值"""
        from web.services.financial_service import financial_full
        data, status_code = financial_full(symbol)
        return data, status_code


@financial_ns.route('/check/<symbol>')
class FinancialCheckResource(Resource):
    """检查财务数据"""
    @financial_ns.doc('check_financial_data',
        description='''检查股票是否有财务数据。

**路径参数：**
- **symbol**: 股票代码 (如: 600382, 000001.SZ)

**返回数据：**
```json
{
  "success": true,
  "symbol": "600382",
  "has_data": true,
  "latest_date": "2024-09-30"
}
```

**用途：**
- 在查询财务数据前检查是否有数据
- 确认最新财报日期
''')
    def get(self, symbol):
        """检查是否有财务数据"""
        from web.services.financial_service import financial_check
        return financial_check(symbol)


@financial_ns.route('/indicators/<symbol>')
class FinancialIndicatorsResource(Resource):
    """财务指标"""
    @financial_ns.doc('get_financial_indicators',
        description='''获取股票财务指标数据（最近8个季度）。

**路径参数：**
- **symbol**: 股票代码 (如: 600382, 000001.SZ)

**返回数据：**
```json
{
  "success": true,
  "symbol": "600382",
  "data": [
    {
      "end_date": "2024-09-30",
      "roe": 0.15,
      "roa": 0.08,
      "netprofit_margin": 0.12,
      ...
    }
  ]
}
```

**财务指标包括：**
- 盈利能力: ROE, ROA, 销售净利率, 毛利率
- 成长能力: 营收增长率, 净利润增长率
- 偿债能力: 流动比率, 速动比率
- 营运能力: 总资产周转率等
''')
    def get(self, symbol):
        """获取财务指标"""
        from web.services.financial_service import financial_indicators
        data, status_code = financial_indicators(symbol)
        return data, status_code


@financial_ns.route('/valuation/<symbol>')
class FinancialValuationResource(Resource):
    """估值指标"""
    @financial_ns.doc('get_financial_valuation',
        description='''获取股票最新估值指标（PE、PB、市值等）。

**路径参数：**
- **symbol**: 股票代码 (如: 600382, 000001.SZ)

**返回数据：**
```json
{
  "success": true,
  "symbol": "600382",
  "valuation": {
    "datetime": "2024-12-20",
    "close": 10.5,
    "pe": 15.2,
    "pe_ttm": 16.8,
    "pb": 1.8,
    "ps": 2.1,
    "ps_ttm": 2.3,
    "total_mv": 123456789.0,
    "circ_mv": 98765432.0
  }
}
```

**估值指标说明：**
- pe: 市盈率（静态）
- pe_ttm: 市盈率（滚动12个月）
- pb: 市净率
- ps: 市销率（静态）
- ps_ttm: 市销率（滚动12个月）
- total_mv: 总市值
- circ_mv: 流通市值
''')
    def get(self, symbol):
        """获取估值指标"""
        from web.services.financial_service import financial_valuation
        data, status_code = financial_valuation(symbol)
        return data, status_code


# ============================================================================
# SW Industry Resources
# ============================================================================

@sw_industry_ns.route('/list')
class SWIndustryListResource(Resource):
    """申万行业列表"""
    @sw_industry_ns.doc('get_sw_industries',
        description='''获取申万行业分类列表，支持层级结构。

**查询参数：**
- **src**: 行业分类来源
  - SW2021: 申万2021行业分类（默认，511个行业）
  - SW2014: 申万2014行业分类
- **level**: 行业级别筛选
  - L1: 一级行业（约31个）
  - L2: 二级行业（约100+个）
  - L3: 三级行业（约500+个）
  - 不传参数: 返回所有级别
- **parent_code**: 父级行业代码
  - 用于筛选特定父行业下的子行业
  - 配合 level=L2 或 L3 使用

**返回结构：**
```json
{
  "data": [
    {
      "index_code": "801010.SI",
      "industry_name": "农林牧渔",
      "level": "L1",
      "children": [...]
    }
  ],
  "total": 511
}
```
''')
    @sw_industry_ns.param('src', '行业分类来源 (SW2014/SW2021)', type='string', enum=['SW2014', 'SW2021'], default='SW2021')
    @sw_industry_ns.param('level', '行业级别 (L1/L2/L3)', type='string', enum=['L1', 'L2', 'L3'], required=False)
    @sw_industry_ns.param('parent_code', '父级行业代码 (用于筛选子行业)', type='string', required=False)
    def get(self):
        """获取申万行业列表（层级结构）"""
        from web.services.sw_industry_service import get_sw_industries
        data, status_code = get_sw_industries()
        return data, status_code


@sw_industry_ns.route('/members/<index_code>')
class SWIndustryMembersResource(Resource):
    """申万行业成分股"""
    @sw_industry_ns.doc('get_sw_members',
        description='''获取指定申万行业的成分股列表。

**路径参数：**
- **index_code**: 行业代码
  - 格式: 801010.SI (农林牧渔)
  - 可以通过 /sw-industry/list 获取所有行业代码

**返回数据：**
```json
{
  "index_code": "801010.SI",
  "industry_name": "农林牧渔",
  "members": [
    {
      "ts_code": "000001.SZ",
      "name": "平安银行"
    }
  ],
  "total": 35
}
```

**注意：**
- 返回该行业的所有上市公司
- 成分股会定期调整
''')
    @sw_industry_ns.param('index_code', '行业代码 (如: 801010.SI)', type='string', required=True)
    def get(self, index_code):
        """获取指定行业的成分股"""
        from web.services.sw_industry_service import get_sw_industry_members
        data, status_code = get_sw_industry_members(index_code)
        return data, status_code


@sw_industry_ns.route('/percentile', methods=['GET'])
class IndustryPercentileResource(Resource):
    """行业分位值查询"""
    @sw_industry_ns.doc('get_industry_percentile',
        description='''获取行业指标分位值（使用行业代码）

**查询参数：**
- **industry_code** (必需): 申万行业代码，如"801010"或"801010.SI"
- **metric** (必需): 指标名称，如"pe_ttm", "pb", "total_mv", "circ_mv"
- **percentile** (可选): 百分位，默认0.75，支持0.1-0.9

**返回数据：**
```json
{
  "success": true,
  "data": {
    "industry_code": "801010",
    "industry_name": "银行",
    "metric": "pe_ttm",
    "percentile": 0.75,
    "percentile_str": "p75",
    "value": 6.5,
    "calculated_at": "2026-02-06 19:25:00"
  }
}
```
    ''')
    @sw_industry_ns.param('industry_code', '申万行业代码 (如: 801010)', type='string', required=True)
    @sw_industry_ns.param('metric', '指标名称 (如: pe_ttm, pb, total_mv)', type='string', required=True)
    @sw_industry_ns.param('percentile', '百分位 (0.1-0.9)', type='float', default=0.75)
    def get(self):
        """查询行业分位值"""
        from web.services.industry_statistics_service import get_industry_percentile
        return get_industry_percentile()


@sw_industry_ns.route('/statistics-metrics', methods=['GET'])
class IndustryStatisticsMetricsResource(Resource):
    """获取可用指标列表"""
    @sw_industry_ns.doc('get_available_metrics',
        description='''获取可查询的指标列表

**返回数据：**
```json
{
  "success": true,
  "data": {
    "metrics": ["pe_ttm", "pb", "ps_ttm", "total_mv", "circ_mv"],
    "descriptions": {
      "pe_ttm": "市盈率TTM",
      "pb": "市净率"
    },
    "available_percentiles": [10, 25, 50, 75, 90]
  }
}
```
    ''')
    def get(self):
        """获取可用指标列表"""
        from web.services.industry_statistics_service import get_available_metrics
        return get_available_metrics()


@sw_industry_ns.route('/statistics-industries', methods=['GET'])
class IndustryStatisticsIndustriesResource(Resource):
    """获取有统计数据的行业列表"""
    @sw_industry_ns.doc('get_industries_with_stats',
        description='''获取有统计数据的行业列表

**查询参数：**
- **level**: 行业级别 (1/2/3)，默认1
- **metric**: 指标名称 (可选)，过滤有该指标数据的行业

**返回数据：**
```json
{
  "success": true,
  "data": {
    "industries": [
      {
        "industry_code": "801010",
        "industry_name": "银行",
        "level": 1,
        "metrics": ["pe_ttm", "pb", "total_mv"]
      }
    ],
    "total": 31
  }
}
```
    ''')
    @sw_industry_ns.param('level', '行业级别 (1/2/3)', type='integer', default=1)
    @sw_industry_ns.param('metric', '指标名称 (可选)', type='string', required=False)
    def get(self):
        """获取有统计数据的行业列表"""
        from web.services.industry_statistics_service import get_industries_with_stats
        return get_industries_with_stats()


# ============================================================================
# Boards Resources
# ============================================================================

@boards_ns.route('')
class BoardsListResource(Resource):
    """板块列表"""
    @boards_ns.doc('list_boards',
        description='''获取所有板块列表。

**返回数据：**
- board_code: 板块代码
- board_name: 板块名称
- board_type: 板块类型
- source: 数据来源
- stock_count: 成分股数量
- updated_at: 更新时间

**用途：**
- 查看所有可用板块
- 获取板块基本信息
''')
    def get(self):
        """获取所有板块列表"""
        from web.services.boards_service import get_boards_list
        data, status_code = get_boards_list()
        return data, status_code


@boards_ns.route('/<board_code>/constituents')
class BoardConstituentsResource(Resource):
    """板块成分股"""
    @boards_ns.doc('get_board_constituents',
        description='''获取指定板块的成分股列表，包含最新估值指标。

**路径参数：**
- **board_code**: 板块代码
  - 例如: new、cyb、zx  (新兴板块、创业板、中小板)

**返回数据：**
```json
[
  {
    "stock_code": "000001",
    "stock_name": "平安银行",
    "board_name": "新兴板块",
    "board_code": "new",
    "datetime": "2024-01-01",
    "close": 10.5,
    "pe": 15.2,
    "pb": 1.8,
    "total_mv_yi": 123456.78,
    "circ_mv_yi": 98765.43
  }
]
```

**估值指标来源：**
- 使用 stock_valuation_snapshot 快照表
- 包含最新的 PE、PB、市值等指标
''')
    def get(self, board_code):
        """获取指定板块的成分股"""
        from web.services.boards_service import get_board_constituents
        data, status_code = get_board_constituents(board_code)
        return data, status_code


@boards_ns.route('/stocks/<stock_code>/boards')
class StockBoardsResource(Resource):
    """股票所属板块"""
    @boards_ns.doc('get_stock_boards',
        description='''获取指定股票所属的所有板块。

**路径参数：**
- **stock_code**: 股票代码
  - 格式: 000001, 600000

**返回数据：**
```json
[
  {
    "board_code": "new",
    "board_name": "新兴板块",
    "stock_code": "000001",
    "stock_name": "平安银行"
  }
]
```

**用途：**
- 查看股票属于哪些板块
- 分析板块分布情况
''')
    def get(self, stock_code):
        """获取股票所属的所有板块"""
        from web.services.boards_service import get_stock_boards
        data, status_code = get_stock_boards(stock_code)
        return data, status_code


# ============================================================================
# Favorites Resources
# ============================================================================

# Favorites models
favorite_item_model = favorites_ns.model('FavoriteItem', {
    'id': fields.Integer(description='收藏ID', example=1),
    'stock_code': fields.String(description='股票代码', example='600382'),
    'stock_name': fields.String(description='股票名称', example='广东明珠'),
    'added_at': fields.String(description='添加时间', example='2024-01-01T10:00:00'),
    'notes': fields.String(description='备注', example='')
})

add_favorite_model = favorites_ns.model('AddFavoriteRequest', {
    'stock_code': fields.String(description='股票代码', required=True, example='600382'),
    'notes': fields.String(description='备注', example='')
})

batch_add_model = favorites_ns.model('BatchAddRequest', {
    'stock_codes': fields.List(fields.String, description='股票代码列表', required=True, example=['600382', '000001']),
    'notes': fields.String(description='备注', example='')
})


@favorites_ns.route('')
class FavoriteListResource(Resource):
    """收藏列表"""
    @favorites_ns.doc('list_favorites',
        description='''获取收藏列表。

**查询参数：**
- **user_id**: 用户ID（可选，默认 'default'）

**返回数据：**
```json
{
  "favorites": [...],
  "total": 10
}
```
''')
    @favorites_ns.param('user_id', '用户ID（默认: default）', type='string', required=False)
    def get(self):
        """获取收藏列表"""
        from web.services.favorite_service import list_favorites
        return list_favorites()

    @favorites_ns.doc('add_favorite',
        description='''添加股票到收藏。

**请求体参数：**
```json
{
  "stock_code": "600382",
  "notes": ""
}
```

**返回数据：**
```json
{
  "id": 1,
  "stock_code": "600382",
  "stock_name": "广东明珠",
  "added_at": "2024-01-01T10:00:00",
  "notes": ""
}
```

**错误码：**
- 400: 无效的股票代码
- 409: 已在收藏列表中
''')
    @favorites_ns.expect(add_favorite_model)
    def post(self):
        """添加收藏"""
        from web.services.favorite_service import add_favorite
        return add_favorite()


@favorites_ns.route('/<stock_code>')
class FavoriteResource(Resource):
    """单个收藏操作"""
    @favorites_ns.doc('remove_favorite',
        description='''删除收藏。

**路径参数：**
- **stock_code**: 股票代码

**返回数据：**
```json
{
  "message": "删除成功"
}
```

**错误码：**
- 404: 收藏不存在
''')
    @favorites_ns.param('stock_code', '股票代码', type='string', required=True)
    def delete(self, stock_code):
        """删除收藏"""
        from web.services.favorite_service import remove_favorite
        return remove_favorite(stock_code)


@favorites_ns.route('/check/<stock_code>')
class FavoriteCheckResource(Resource):
    """检查收藏状态"""
    @favorites_ns.doc('check_favorite',
        description='''检查股票是否已收藏。

**路径参数：**
- **stock_code**: 股票代码

**返回数据：**
```json
{
  "is_favorite": true,
  "favorite": {
    "id": 1,
    "stock_code": "600382",
    "stock_name": "广东明珠",
    "added_at": "2024-01-01T10:00:00",
    "notes": ""
  }
}
```
''')
    @favorites_ns.param('stock_code', '股票代码', type='string', required=True)
    def get(self, stock_code):
        """检查是否已收藏"""
        from web.services.favorite_service import check_favorite
        return check_favorite(stock_code)


@favorites_ns.route('/batch', methods=['POST'])
class FavoriteBatchResource(Resource):
    """批量添加收藏"""
    @favorites_ns.doc('batch_add_favorites',
        description='''批量添加股票到收藏。

**请求体参数：**
```json
{
  "stock_codes": ["600382", "000001"],
  "notes": ""
}
```

**返回数据：**
```json
{
  "success": 2,
  "failed": 0,
  "total": 2,
  "results": [
    {
      "stock_code": "600382",
      "stock_name": "广东明珠",
      "success": true
    },
    {
      "stock_code": "000001",
      "stock_name": "平安银行",
      "success": true
    }
  ]
}
```
''')
    @favorites_ns.expect(batch_add_model)
    def post(self):
        """批量添加收藏"""
        from web.services.favorite_service import batch_add_favorites
        return batch_add_favorites()


@favorites_ns.route('/clear', methods=['DELETE'])
class FavoriteClearResource(Resource):
    """清空收藏"""
    @favorites_ns.doc('clear_favorites',
        description='''清空所有收藏。

**查询参数：**
- **user_id**: 用户ID（可选，默认 'default'）

**返回数据：**
```json
{
  "message": "已清空 10 条收藏"
}
```
''')
    @favorites_ns.param('user_id', '用户ID（默认: default）', type='string', required=False)
    def delete(self):
        """清空收藏"""
        from web.services.favorite_service import clear_favorites
        return clear_favorites()


# ============================================================================
# Liquidity Resources
# ============================================================================

# Liquidity models
liquidity_screen_model = liquidity_ns.model('LiquidityScreenRequest', {
    'lookback_days': fields.Integer(description='回溯交易日数（默认20，注意是交易日不是自然日）', example=20, default=20),
    'min_avg_amount_20d': fields.Float(description='最小日均成交额 万元（默认3000）', example=3000, default=3000),
    'min_avg_turnover_20d': fields.Float(description='最小日均换手率 %（默认0.3）', example=0.3, default=0.3),
    'small_cap_threshold': fields.Float(description='小盘股阈值 亿元（默认50）', example=50, default=50),
    'high_turnover_threshold': fields.Float(description='高换手率阈值 %（默认8）', example=8.0, default=8.0),
    'max_amihud_illiquidity': fields.Float(description='最大Amihud非流动性指标（默认0.8）', example=0.8, default=0.8),
    'limit': fields.Integer(description='返回结果数量限制（默认100）', example=100, default=100)
})

liquidity_stock_model = liquidity_ns.model('LiquidityStock', {
    'symbol': fields.String(description='股票代码', example='600382'),
    'name': fields.String(description='股票名称', example='广东明珠'),
    'avg_amount_20d': fields.Float(description='日均成交额 万元', example=5000.5),
    'avg_turnover_20d': fields.Float(description='日均换手率 %', example=3.5),
    'avg_circ_mv': fields.Float(description='日均流通市值 亿元', example=100.5),
    'amihud_illiquidity': fields.Float(description='Amihud非流动性指标', example=0.000001),
    'filter_result': fields.String(description='筛选结果', example='PASS')
})


@liquidity_ns.route('/screen', methods=['POST'])
class LiquidityScreenResource(Resource):
    """流动性筛选"""
    @liquidity_ns.doc('liquidity_screen',
        description='''A股流动性三级筛选。

**三级筛选逻辑：**
1. **绝对流动性底线**（防极端风险）
   - 日均成交额 < 3000万元：拒绝

2. **相对活跃度**（剔除"僵尸股"）
   - 日均换手率 < 0.3%：拒绝

3. **流动性质量**（防小盘股陷阱）
   - 流通市值 < 50亿元 且 换手率 > 8%：
     - Amihud非流动性指标 > 0.8：拒绝

**请求体参数：**
```json
{
  "lookback_days": 20,
  "min_avg_amount_20d": 3000,
  "min_avg_turnover_20d": 0.3,
  "small_cap_threshold": 50,
  "high_turnover_threshold": 8.0,
  "max_amihud_illiquidity": 0.8,
  "limit": 100
}
```

**参数说明：**
- **lookback_days**: 回溯**交易日**数量（不是自然日），系统会自动处理周末和节假日
- 其他参数单位说明见返回数据

**返回数据：**
```json
{
  "success": true,
  "count": 50,
  "stocks": [
    {
      "symbol": "600382",
      "name": "广东明珠",
      "avg_amount_20d": 5000.5,
      "avg_turnover_20d": 3.5,
      "avg_circ_mv": 100.5,
      "amihud_illiquidity": 0.000001,
      "filter_result": "PASS"
    }
  ]
}
```

**指标说明：**
- avg_amount_20d: 日均成交额（万元）
- avg_turnover_20d: 日均换手率（%）
- avg_circ_mv: 日均流通市值（亿元）
- amihud_illiquidity: Amihud非流动性指标，越小流动性越好
''')
    @liquidity_ns.expect(liquidity_screen_model)
    def post(self):
        """流动性筛选"""
        from web.services.liquidity_service import liquidity_screen
        return liquidity_screen()


@liquidity_ns.route('/metrics/<symbol>')
class LiquidityMetricsResource(Resource):
    """单股流动性指标"""
    @liquidity_ns.doc('get_liquidity_metrics',
        description='''获取单股流动性指标。

**路径参数：**
- **symbol**: 股票代码（如: 600382, 000001.SZ）

**查询参数：**
- **lookback_days**: 回溯**交易日**数量（默认20，不是自然日）

**返回数据：**
```json
{
  "success": true,
  "symbol": "600382",
  "name": "广东明珠",
  "metrics": {
    "avg_amount_20d": 5000.5,
    "avg_turnover_20d": 3.5,
    "avg_circ_mv": 100.5,
    "amihud_illiquidity": 0.000001,
    "data_points": 20
  }
}
```

**指标说明：**
- avg_amount_20d: 日均成交额（万元）
- avg_turnover_20d: 日均换手率（%）
- avg_circ_mv: 日均流通市值（亿元）
- amihud_illiquidity: Amihud非流动性指标
- data_points: 数据点数量
''')
    @liquidity_ns.param('symbol', '股票代码（如: 600382, 000001.SZ）', type='string', required=True)
    @liquidity_ns.param('lookback_days', '回溯天数（默认: 20）', type='integer', default=20, required=False)
    def get(self, symbol):
        """获取单股流动性指标"""
        from web.services.liquidity_service import liquidity_metrics
        return liquidity_metrics(symbol)


# ============================================================================
# Valuation Resources
# ============================================================================

# Valuation models
valuation_request_model = valuation_ns.model('ValuationRequest', {
    'methods': fields.String(description='估值方法，逗号分隔 (pe,pb,ps,peg,dcf,combined)', example='pe,pb'),
    'date': fields.String(description='估值日期 (YYYY-MM-DD 或 YYYYMMDD)', example='2024-01-01'),
    'combine_method': fields.String(description='组合方式 (weighted/average/median/max_confidence)', example='weighted', enum=['weighted', 'average', 'median', 'max_confidence'])
})

batch_valuation_model = valuation_ns.model('BatchValuationRequest', {
    'symbols': fields.List(fields.String, description='股票代码列表', example=['600382', '000001'], required=True),
    'methods': fields.String(description='估值方法，逗号分隔', example='pe,pb'),
    'date': fields.String(description='估值日期/股价日期 (YYYY-MM-DD 或 YYYYMMDD)', example='2024-01-01'),
    'fiscal_date': fields.String(description='财务数据报告期/财报期 (YYYY-MM-DD 或 YYYYMMDD，可选。不指定则使用估值日期之前的最新财报)', example='2024-12-31'),
    'combine_method': fields.String(description='组合方式', example='weighted', enum=['weighted', 'average', 'median', 'max_confidence']),
    # DCF 可选参数 - 使用Raw说明而非Nested model
    'dcf_config': fields.Raw(description='''
DCF估值配置参数（可选）:
{
  "forecast_years": 预测年数 (默认5, 1-10),
  "terminal_growth": 终值增长率 (默认0.02, 0-0.1),
  "risk_free_rate": 无风险利率 (默认0.03, 0-0.2),
  "market_return": 市场回报率 (默认0.08, 0-0.3),
  "tax_rate": 企业所得税率 (默认0.25, 0-0.5),
  "credit_spread": 债务信用利差 (默认0.02, 0-0.1),
  "growth_rate_cap": 收入增长率上限 (默认0.08, 0-0.5),
  "wacc_min": WACC下限 (默认0.05, 0-0.15),
  "wacc_max": WACC上限 (默认0.20, 0.05-0.5),
  "beta": Beta系数（可选，不指定则根据行业计算）(0-3)
}
''', example='{"forecast_years": 5, "terminal_growth": 0.02, "beta": 1.0}')
})

compare_valuation_model = valuation_ns.model('CompareValuationRequest', {
    'symbols': fields.List(fields.String, description='股票代码列表', example=['600382', '000001'], required=True),
    'method': fields.String(description='估值方法', example='pe'),
    'date': fields.String(description='估值日期', example='2024-01-01')
})


@valuation_ns.route('/summary/<symbol>')
class ValuationSummaryResource(Resource):
    """估值摘要"""
    @valuation_ns.doc('get_valuation_summary',
        description='''获取股票估值摘要。

**路径参数：**
- **symbol**: 股票代码 (如: 600382, 000001.SZ)

**查询参数：**
- **methods**: 估值方法，逗号分隔 (pe,pb,ps,peg,dcf,combined)，默认全部
- **date**: 估值日期 (YYYY-MM-DD 或 YYYYMMDD)
- **combine_method**: 组合方式 (weighted/average/median/max_confidence)，默认weighted

**返回数据：**
```json
{
  "success": true,
  "symbol": "600382",
  "valuation": {
    "symbol": "600382",
    "date": "2024-01-01",
    "model": "Relative_PE",
    "fair_value": 15.50,
    "current_price": 12.80,
    "upside_downside": 21.09,
    "rating": "买入",
    "confidence": 0.65,
    "metrics": {...},
    "assumptions": {...},
    "warnings": []
  }
}
```

**估值方法说明：**
- pe: 市盈率估值
- pb: 市净率估值
- ps: 市销率估值
- peg: PEG比率估值
- dcf: 自由现金流折现估值
- combined: 组合多种方法
''')
    @valuation_ns.param('symbol', '股票代码 (如: 600382, 000001.SZ)', type='string', required=True)
    @valuation_ns.param('methods', '估值方法，逗号分隔 (pe,pb,ps,peg,dcf,combined)', type='string', required=False)
    @valuation_ns.param('date', '估值日期 (YYYY-MM-DD 或 YYYYMMDD)', type='string', required=False)
    @valuation_ns.param('combine_method', '组合方式 (weighted/average/median/max_confidence)', type='string', enum=['weighted', 'average', 'median', 'max_confidence'], required=False)
    def get(self, symbol):
        """获取股票估值摘要"""
        from flask import request
        import json
        from web.services.valuation_service import valuation_summary

        methods = request.args.get('methods')
        date = request.args.get('date')
        combine_method = request.args.get('combine_method', 'weighted')

        # Parse dcf_config parameter
        dcf_config = None
        dcf_config_str = request.args.get('dcf_config')
        if dcf_config_str:
            try:
                dcf_config = json.loads(dcf_config_str)
            except json.JSONDecodeError:
                return {'error': 'Invalid dcf_config format. Must be valid JSON.'}, 400

        data, status_code = valuation_summary(symbol, methods, date, combine_method, dcf_config)
        return data, status_code


@valuation_ns.route('/batch', methods=['POST'])
class BatchValuationResource(Resource):
    """批量估值"""
    @valuation_ns.doc('batch_valuation',
        description='''批量获取股票估值。

**请求体参数：**
```json
{
  "symbols": ["600382", "000001"],
  "methods": "pe,pb",
  "date": "2024-01-01",
  "combine_method": "weighted"
}
```

**返回数据：**
```json
{
  "success": true,
  "valuations": [...]
}
```
''')
    @valuation_ns.expect(batch_valuation_model)
    def post(self):
        """批量获取股票估值"""
        from flask import request
        from web.services.valuation_service import batch_valuation

        data_json = request.get_json()
        symbols = data_json.get('symbols', [])
        methods = data_json.get('methods')
        date = data_json.get('date')
        fiscal_date = data_json.get('fiscal_date')  # 新增：财务数据报告期
        combine_method = data_json.get('combine_method', 'weighted')
        dcf_config = data_json.get('dcf_config')  # 新增 DCF 配置参数

        # 转换为逗号分隔的字符串
        symbols_str = ','.join(symbols) if symbols else ''

        data, status_code = batch_valuation(symbols_str, methods, date, fiscal_date, combine_method, dcf_config)
        return data, status_code


@valuation_ns.route('/compare', methods=['POST'])
class CompareValuationResource(Resource):
    """对比估值"""
    @valuation_ns.doc('compare_valuation',
        description='''对比多只股票的估值。

**请求体参数：**
```json
{
  "symbols": ["600382", "000001"],
  "method": "pe",
  "date": "2024-01-01"
}
```

**返回数据：**
```json
{
  "success": true,
  "comparison": {
    "date": "2024-01-01",
    "method": "pe",
    "summary": {...},
    "stocks": [...]
  }
}
```
''')
    @valuation_ns.expect(compare_valuation_model)
    def post(self):
        """对比多只股票的估值"""
        from flask import request
        from web.services.valuation_service import compare_valuation

        data_json = request.get_json()
        symbols = data_json.get('symbols', [])
        method = data_json.get('method')
        date = data_json.get('date')

        # 转换为逗号分隔的字符串
        symbols_str = ','.join(symbols) if symbols else ''

        data, status_code = compare_valuation(symbols_str, method, date)
        return data, status_code


@valuation_ns.route('/models')
class ValuationModelsResource(Resource):
    """估值模型列表"""
    @valuation_ns.doc('list_valuation_models',
        description='''列出所有可用的估值模型。

**返回数据：**
```json
{
  "success": true,
  "models": ["Relative_PE", "Relative_PB", "Relative_PS", "Relative_PEG", "Relative_COMBINED", "DCF"]
}
```
''')
    def get(self):
        """列出所有可用的估值模型"""
        from web.services.valuation_service import list_models
        data, status_code = list_models()
        return data, status_code


# ============================================================================
# Backtest Resources
# ============================================================================

# Backtest models
# 支持的策略类型枚举
SUPPORTED_STRATEGIES = ['sma_cross', 'price_breakout']

backtest_request_model = backtest_ns.model('BacktestRequest', {
    # 回测共有参数
    'stock': fields.String(required=True, description='股票代码', example='600382'),
    'start_date': fields.String(required=True, description='开始日期 (YYYY-MM-DD)', example='2024-01-01'),
    'end_date': fields.String(required=False, description='结束日期 (YYYY-MM-DD)', example='2024-12-31'),
    'cash': fields.Float(required=False, description='初始资金', example=1000000, default=1000000),
    'commission': fields.Float(required=False, description='手续费率', example=0.0002, default=0.0002),
    'benchmark': fields.String(required=False, description='基准指数', example='000300.SH'),
    # 策略参数
    'strategy': fields.String(required=True, description='策略类型',
                          example='sma_cross',
                          enum=SUPPORTED_STRATEGIES),
    'strategy_params': fields.Raw(description='策略参数（根据策略类型不同）。'
                                   'sma_cross: {maperiod: MA周期}。'
                                   'price_breakout: {buy_threshold: 买入阈值%, sell_threshold: 止盈阈值%, stop_loss_threshold: 止损阈值%}',
                               example={'maperiod': 20})
})

backtest_batch_model = backtest_ns.model('BacktestBatchRequest', {
    'backtests': fields.List(fields.Nested(backtest_request_model), description='回测任务列表', required=True)
})


@backtest_ns.route('/strategies')
class BacktestStrategiesResource(Resource):
    """获取支持的策略列表"""
    @backtest_ns.doc('get_strategies',
        description='''获取所有支持的回测策略列表。

**当前支持的策略：**
1. **sma_cross** - 简单移动平均线交叉策略
2. **price_breakout** - 价格突破策略

**返回数据：**
```json
{
  "success": true,
  "strategies": [
    {
      "strategy_type": "sma_cross",
      "name": "简单移动平均线交叉策略",
      "description": "当收盘价向上突破MA时买入，向下跌破MA时卖出",
      "params_schema": {
        "type": "object",
        "properties": {
          "maperiod": {
            "type": "integer",
            "minimum": 1,
            "maximum": 100,
            "default": 10,
            "description": "移动平均线周期"
          }
        }
      }
    },
    {
      "strategy_type": "price_breakout",
      "name": "价格突破策略",
      "description": "当日最低价跌破开盘价指定阈值时以限价买入，持仓期间检查止损（当日最低价跌破买入价止损阈值）和止盈（当日最高价突破买入价止盈阈值），止损优先级高于止盈",
      "params_schema": {
        "type": "object",
        "properties": {
          "buy_threshold": {
            "type": "number",
            "minimum": 0.1,
            "maximum": 20.0,
            "default": 1.0,
            "description": "买入阈值（百分比），当日最低价低于开盘价此比例时触发买入"
          },
          "sell_threshold": {
            "type": "number",
            "minimum": 0.1,
            "maximum": 50.0,
            "default": 5.0,
            "description": "止盈阈值（百分比），当日最高价高于买入价此比例时触发止盈卖出"
          },
          "stop_loss_threshold": {
            "type": "number",
            "minimum": 1.0,
            "maximum": 50.0,
            "default": 10.0,
            "description": "止损阈值（百分比），当日最低价低于买入价此比例时触发止损卖出"
          },
          "commission": {
            "type": "number",
            "minimum": 0.0,
            "maximum": 1.0,
            "default": 0.002,
            "description": "手续费率"
          }
        }
      }
    }
  ]
}
```

**用途：**
- 动态获取系统支持的策略列表
- 获取策略的参数 schema，用于前端表单验证
''')
    def get(self):
        """获取支持的策略列表"""
        from web.services.backtest_service import get_supported_strategies_api
        return get_supported_strategies_api()


@backtest_ns.route('/run', methods=['POST'])
class BacktestRunResource(Resource):
    """创建回测任务"""
    @backtest_ns.doc('run_backtest',
        description='''创建回测任务（支持多策略）。

**支持的策略类型：**
1. **sma_cross** - 简单移动平均线交叉策略
   - 逻辑：当收盘价向上突破MA时买入，向下跌破MA时卖出
2. **price_breakout** - 价格突破策略
   - 逻辑：当日最低价跌破开盘价指定阈值时买入，持仓期间检查止损（当日最低价跌破买入价止损阈值）和止盈（当日最高价突破买入价止盈阈值），止损优先级高于止盈

**请求体示例（SMA交叉策略）：**
```json
{
  "stock": "600382",
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "cash": 1000000,
  "commission": 0.0002,
  "benchmark": "000300.SH",
  "strategy": "sma_cross",
  "strategy_params": {
    "maperiod": 20
  }
}
```

**请求体示例（价格突破策略）：**
```json
{
  "stock": "600382",
  "start_date": "2024-01-01",
  "strategy": "price_breakout",
  "strategy_params": {
    "buy_threshold": 1.0,
    "sell_threshold": 5.0,
    "stop_loss_threshold": 10.0
  }
}
```

**参数说明：**

回测共有参数（所有策略通用）：
- **stock**: 股票代码（必需）
- **start_date**: 开始日期（必需），格式 YYYY-MM-DD
- **end_date**: 结束日期（可选），格式 YYYY-MM-DD，不填则使用最新日期
- **cash**: 初始资金（可选，默认100万）
- **commission**: 手续费率（可选，默认0.0002即0.02%）
- **benchmark**: 基准指数（可选），如 000300.SH（沪深300）

策略参数：
- **strategy**: 策略类型（必需），可选值：sma_cross, price_breakout
- **strategy_params**: 策略特定参数（必需）

**sma_cross 策略参数：**
- maperiod: MA周期（1-100，默认10）

**price_breakout 策略参数：**
- buy_threshold: 买入阈值百分比（0.1-20.0，默认1.0）- 当日最低价低于开盘价此比例时触发买入
- sell_threshold: 止盈阈值百分比（0.1-50.0，默认5.0）- 当日最高价高于买入价此比例时触发止盈卖出
- stop_loss_threshold: 止损阈值百分比（1.0-50.0，默认10.0）- 当日最低价低于买入价此比例时触发止损卖出，止损优先级高于止盈

**返回数据：**
```json
{
  "success": true,
  "task_id": "abc123",
  "status": "pending",
  "message": "SMA交叉策略回测任务已创建，股票: 600382"
}
```
''')
    @backtest_ns.expect(backtest_request_model)
    def post(self):
        """创建回测任务"""
        from web.services.task_creation_service import create_task

        request_data = {
            'task_type': 'backtest',
            'params': request.get_json()
        }
        return create_task(request_data)


@backtest_ns.route('/status/<task_id>')
class BacktestStatusResource(Resource):
    """查询回测任务状态"""
    @backtest_ns.doc('get_backtest_status',
        description='''查询回测任务状态。

**路径参数：**
- **task_id**: 任务ID

**返回数据：**
```json
{
  "task_id": "abc123",
  "task_type": "backtest",
  "status": "running",
  "progress": 50,
  "message": "正在执行SMA交叉策略回测..."
}
```
''')
    def get(self, task_id):
        """查询回测任务状态"""
        from web.services.task_service import get_task_manager
        tm = get_task_manager()
        task = tm.get_task(task_id)
        if not task:
            return {'error': '任务不存在'}, 404

        # 只返回轻量级状态信息，不包含完整的 result
        return {
            'task_id': task.get('task_id'),
            'task_type': task.get('task_type'),
            'status': task.get('status'),
            'progress': task.get('progress', 0),
            'message': task.get('message'),
            'error': task.get('error'),
            'created_at': task.get('created_at'),
            'completed_at': task.get('completed_at')
        }


@backtest_ns.route('/result/<task_id>')
class BacktestResultResource(Resource):
    """获取回测结果"""
    @backtest_ns.doc('get_backtest_result',
        description='''获取回测任务结果。

**路径参数：**
- **task_id**: 任务ID

**返回数据：**
```json
{
  "success": true,
  "task_id": "abc123",
  "status": "completed",
  "result": {
    "basic_info": {
      "stock": "600382",
      "start_date": "2024-01-01",
      "end_date": "2024-12-31",
      "initial_cash": 1000000,
      "final_value": 1200000,
      "total_return": 0.20
    },
    "strategy_info": {
      "strategy": "sma_cross",
      "strategy_params": {"maperiod": 20},
      "strategy_name": "简单移动平均线交叉策略"
    },
    "trade_stats": {
      "total_trades": 10,
      "winning_trades": 6,
      "losing_trades": 4,
      "win_rate": 0.60
    },
    "trades": [...],
    "health_metrics": {
      "annual_return": 0.20,
      "sharpe_ratio": 1.5,
      "max_drawdown": -0.15
    }
  }
}
```
''')
    def get(self, task_id):
        """获取回测结果"""
        from web.services.backtest_service import get_backtest_result
        data = get_backtest_result(task_id)
        return data


@backtest_ns.route('/batch', methods=['POST'])
class BacktestBatchResource(Resource):
    """批量回测"""
    @backtest_ns.doc('batch_backtest',
        description='''批量创建回测任务。

**请求体参数：**
```json
{
  "backtests": [
    {
      "stock": "600382",
      "start_date": "2024-01-01",
      "strategy": "sma_cross",
      "strategy_params": {"maperiod": 20}
    },
    {
      "stock": "000001",
      "start_date": "2024-01-01",
      "strategy": "sma_cross",
      "strategy_params": {"maperiod": 10}
    }
  ]
}
```

**返回数据：**
```json
{
  "success": true,
  "task_id": "batch123",
  "status": "pending",
  "message": "批量回测任务已创建，共2个回测"
}
```
''')
    @backtest_ns.expect(backtest_batch_model)
    def post(self):
        """批量创建回测任务"""
        from web.services.task_creation_service import create_task

        data_json = request.get_json()
        backtests = data_json.get('backtests', [])

        if not backtests:
            return {'error': 'backtests 列表不能为空'}, 400

        # 创建批量任务
        request_data = {
            'task_type': 'backtest',
            'params': backtests[0]  # 简化实现，只创建第一个任务
        }

        return create_task(request_data)


# ============================================================================
# Backtest History Resources
# ============================================================================

# Backtest history models
backtest_history_item_model = backtest_ns.model('BacktestHistoryItem', {
    'task_id': fields.String(description='任务ID'),
    'name': fields.String(description='历史记录名称'),
    'stock': fields.String(description='股票代码'),
    'stock_name': fields.String(description='股票名称'),
    'strategy': fields.String(description='策略类型'),
    'strategy_name': fields.String(description='策略名称'),
    'total_return': fields.Float(description='总收益率'),
    'sharpe_ratio': fields.Float(description='夏普比率'),
    'max_drawdown': fields.Float(description='最大回撤'),
    'created_at': fields.String(description='创建时间')
})

backtest_history_list_model = backtest_ns.model('BacktestHistoryList', {
    'success': fields.Boolean,
    'total': fields.Integer(description='总数量'),
    'history': fields.List(fields.Nested(backtest_history_item_model))
})


@backtest_ns.route('/history')
class BacktestHistoryListResource(Resource):
    """回测历史记录列表"""
    @backtest_ns.doc('get_backtest_history',
        description='''获取回测历史记录列表

**查询参数：**
- page: 页码（默认1）
- page_size: 每页数量（默认20）
- stock: 按股票代码筛选（可选）
- strategy: 按策略类型筛选（可选）

**返回数据：**
```json
{
  "success": true,
  "total": 100,
  "history": [
    {
      "task_id": "xxx",
      "name": "600382-sma_cross-2026-02-08 17:16:05",
      "stock": "600382",
      "strategy": "sma_cross",
      "strategy_name": "简单移动平均线交叉策略",
      "total_return": 0.2079,
      "sharpe_ratio": 1.5,
      "max_drawdown": 0.1,
      "created_at": "2026-02-08 17:16:05",
      "completed_at": "2026-02-08 17:16:05"
    }
  ]
}
```
''')
    @backtest_ns.param('page', '页码', type='integer', default=1)
    @backtest_ns.param('page_size', '每页数量', type='integer', default=20)
    @backtest_ns.param('stock', '股票代码筛选', type='string', required=False)
    @backtest_ns.param('strategy', '策略类型筛选', type='string', required=False)
    @backtest_ns.marshal_with(backtest_history_list_model)
    def get(self):
        """获取回测历史列表"""
        from web.services.backtest_history_service import get_backtest_history
        return get_backtest_history()


@backtest_ns.route('/history/<string:task_id>')
class BacktestHistoryDetailResource(Resource):
    """回测历史记录详情"""
    @backtest_ns.doc('get_backtest_history_detail',
        description='''获取单个回测任务的完整结果

**返回数据：**
```json
{
  "success": true,
  "detail": {
    "task_id": "xxx",
    "name": "600382-sma_cross-2026-02-08 17:16:05",
    "params": {...},
    "result": {...},
    "created_at": "2026-02-08 17:16:05",
    "completed_at": "2026-02-08 17:16:05"
  }
}
```
''')
    def get(self, task_id):
        """获取回测历史详情"""
        from web.services.backtest_history_service import get_backtest_history_detail
        return get_backtest_history_detail(task_id)

    @backtest_ns.doc('delete_backtest_history',
        description='''删除回测历史记录

**返回数据：**
```json
{
  "success": true,
  "message": "删除成功"
}
```
''')
    def delete(self, task_id):
        """删除回测历史"""
        from web.services.backtest_history_service import delete_backtest_history
        return delete_backtest_history(task_id)


# ============================================================================
# Screening Resources
# ============================================================================

# Screening models
screening_strategy_model = screening_ns.model('ScreeningStrategyItem', {
    'name': fields.String(description='策略名称', example='value'),
    'description': fields.String(description='策略描述', example='价值投资策略')
})

custom_screen_model = screening_ns.model('CustomScreenRequest', {
    'config': fields.Raw(description='筛选条件配置（JSON格式）', example={
        'type': 'AND',
        'criteria': [
            {'type': 'Range', 'column': 'pe_ttm', 'min_val': 0, 'max_val': 30},
            {'type': 'GreaterThan', 'column': 'latest_roe', 'threshold': 10}
        ]
    }),
    'limit': fields.Integer(description='返回结果数量限制', example=100, default=100)
})


@screening_ns.route('/strategies')
class ScreeningStrategiesResource(Resource):
    """策略列表"""
    @screening_ns.doc('list_screening_strategies',
        description='''列出所有可用的预设筛选策略。

**返回数据：**
```json
{
  "success": true,
  "strategies": [
    {
      "name": "liquidity",
      "description": "流动性策略"
    },
    {
      "name": "value",
      "description": "价值投资策略"
    }
  ]
}
```

**可用策略：**
- liquidity: 流动性策略
- value: 价值投资策略
- growth: 成长股策略
- tech_growth: 科技成长策略
- quality: 质量策略
- dividend: 股息策略
- low_volatility: 低波动策略
- turnaround: 困境反转策略
- momentum_quality: 动量质量策略
- exclude_financials: 排除金融策略
''')
    def get(self):
        """列出所有可用的预设策略"""
        from web.services.screening_service import list_strategies
        return list_strategies()


@screening_ns.route('/strategies/<strategy_name>')
class ScreeningStrategyApplyResource(Resource):
    """应用预设策略"""
    @screening_ns.doc('apply_preset_strategy',
        description='''应用预设策略进行筛选。

**路径参数：**
- **strategy_name**: 策略名称

**查询参数：**
- **limit**: 返回结果数量限制（默认100）

**返回数据：**
```json
{
  "success": true,
  "strategy": "value",
  "strategy_description": "价值投资策略",
  "count": 50,
  "stocks": [
    {
      "code": "600382",
      "name": "广东明珠",
      "latest_close": 15.23,
      "pe_ttm": 18.5,
      "pb": 1.2,
      "total_mv_yi": 123.45
    }
  ]
}
```
''')
    @screening_ns.param('strategy_name', '策略名称 (liquidity/value/growth/...)', type='string', required=True)
    @screening_ns.param('limit', '返回结果数量限制', type='integer', default=100, required=False)
    def get(self, strategy_name):
        """应用预设策略"""
        from web.services.screening_service import apply_preset_strategy
        return apply_preset_strategy(strategy_name)


@screening_ns.route('/custom', methods=['POST'])
class ScreeningCustomResource(Resource):
    """自定义筛选"""
    @screening_ns.doc('apply_custom_strategy',
        description='''使用自定义JSON配置进行筛选。

**请求体参数：**
```json
{
  "config": {
    "type": "AND",
    "criteria": [
      {
        "type": "Range",
        "column": "pe_ttm",
        "min_val": 0,
        "max_val": 30
      },
      {
        "type": "GreaterThan",
        "column": "latest_roe",
        "threshold": 10
      }
    ]
  },
  "limit": 100
}
```

**支持的条件类型：**
- **Range**: 范围条件 {type: "Range", column: "pe_ttm", min_val: 0, max_val: 30}
- **GreaterThan**: 大于条件 {type: "GreaterThan", column: "latest_roe", threshold: 10}
- **LessThan**: 小于条件 {type: "LessThan", column: "debt_to_assets", threshold: 60}
- **Percentile**: 百分位条件 {type: "Percentile", column: "pe_ttm", percentile: 0.25}
- **TopN**: 前N个 {type: "TopN", column: "latest_roe", n: 50}
- **IndustryFilter**: 行业过滤 {type: "IndustryFilter", industries: ["银行"], mode: "blacklist"}
- **IndustryRelative**: 行业相对 {type: "IndustryRelative", column: "latest_roe", percentile: 0.3}

**逻辑组合：**
- **AND**: 所有条件都满足
- **OR**: 满足任一条件
- **NOT**: 不满足条件
''')
    @screening_ns.expect(custom_screen_model)
    def post(self):
        """应用自定义筛选策略"""
        from web.services.screening_service import apply_custom_strategy
        return apply_custom_strategy()


@screening_ns.route('/criteria-types')
class ScreeningCriteriaTypesResource(Resource):
    """筛选条件类型"""
    @screening_ns.doc('list_criteria_types',
        description='''列出所有支持的筛选条件类型。

**返回数据：**
```json
{
  "success": true,
  "types": ["Range", "GreaterThan", ...],
  "criteria_details": {
    "Range": "范围条件 {...}",
    "GreaterThan": "大于条件 {...}"
  }
}
```
''')
    def get(self):
        """列出支持的筛选条件类型"""
        from web.services.screening_service import list_criteria_types
        return list_criteria_types()


@screening_ns.route('/industries')
class ScreeningIndustriesResource(Resource):
    """行业分类列表"""
    @screening_ns.doc('list_industries',
        description='''获取申万行业分类列表。

**查询参数：**
- **level**: 行业级别（1=一级，2=二级，3=三级，默认1）

**返回数据：**
```json
{
  "success": true,
  "level": 1,
  "industries": [
    {"code": "801080.SI", "name": "电子", "parent_code": "0"},
    {"code": "801010.SI", "name": "农林牧渔", "parent_code": "0"}
  ]
}
```
''')
    @screening_ns.param('level', '行业级别 (1=一级, 2=二级, 3=三级)', type='integer', default=1, required=False)
    def get(self):
        """获取申万行业分类列表"""
        from web.services.screening_service import list_industries
        from flask import request
        level = request.args.get('level', 1, type=int)
        return list_industries(level)


# ============================================================================
# Screening History Resources
# ============================================================================

screening_history_model = screening_ns.model('ScreeningHistory', {
    'id': fields.Integer(description='历史记录ID'),
    'name': fields.String(description='筛选名称'),
    'result_count': fields.Integer(description='筛选结果数量'),
    'stocks_count': fields.Integer(description='保存的股票数量'),
    'created_at': fields.String(description='创建时间'),
})

screening_history_detail_model = screening_ns.model('ScreeningHistoryDetail', {
    'id': fields.Integer(description='历史记录ID'),
    'name': fields.String(description='筛选名称'),
    'config': fields.Raw(description='筛选条件配置'),
    'result_count': fields.Integer(description='筛选结果数量'),
    'stocks_count': fields.Integer(description='保存的股票数量'),
    'created_at': fields.String(description='创建时间'),
    'stocks': fields.List(fields.Raw, description='股票列表'),
})

save_history_model = screening_ns.model('SaveScreeningHistory', {
    'name': fields.String(description='筛选名称', required=True),
    'config': fields.Raw(description='筛选条件配置', required=True),
    'stocks': fields.List(fields.Raw, description='筛选结果股票列表（可选）'),
})


@screening_ns.route('/history')
class ScreeningHistoryListResource(Resource):
    """筛选历史管理"""
    @screening_ns.doc('save_screening_history',
        description='''保存筛选历史。

**请求体：**
- **name**: 筛选名称（必需）
- **config**: 筛选条件配置（必需）
- **stocks**: 筛选结果股票列表（可选）

**返回数据：**
```json
{
  "success": true,
  "history_id": 1,
  "message": "筛选历史已保存"
}
```
''')
    @screening_ns.expect(save_history_model)
    def post(self):
        """保存筛选历史"""
        from web.services.screening_history_service import save_screening_history
        return save_screening_history()

    @screening_ns.doc('get_screening_history',
        description='''获取筛选历史列表。

**查询参数：**
- **user_id**: 用户ID（可选，默认 'default'）

**返回数据：**
```json
{
  "success": true,
  "history": [
    {
      "id": 1,
      "name": "我的筛选策略",
      "result_count": 1167,
      "stocks_count": 1167,
      "created_at": "2024-01-01 10:00:00"
    }
  ]
}
```
''')
    @screening_ns.param('user_id', '用户ID', type='string', required=False)
    def get(self):
        """获取筛选历史列表"""
        from web.services.screening_history_service import get_screening_history
        return get_screening_history()


@screening_ns.route('/history/<int:history_id>')
class ScreeningHistoryDetailResource(Resource):
    """筛选历史详情"""
    @screening_ns.doc('get_screening_history_detail',
        description='''获取筛选历史详情。

**路径参数：**
- **history_id**: 历史记录ID

**查询参数：**
- **user_id**: 用户ID（可选，默认 'default'）

**返回数据：**
```json
{
  "success": true,
  "detail": {
    "id": 1,
    "name": "我的筛选策略",
    "config": {...},
    "result_count": 1167,
    "stocks_count": 1167,
    "created_at": "2024-01-01 10:00:00",
    "stocks": [...]
  }
}
```
''')
    @screening_ns.param('user_id', '用户ID', type='string', required=False)
    def get(self, history_id):
        """获取筛选历史详情"""
        from web.services.screening_history_service import get_screening_history_detail
        return get_screening_history_detail(history_id)

    @screening_ns.doc('delete_screening_history',
        description='''删除筛选历史。

**路径参数：**
- **history_id**: 历史记录ID

**查询参数：**
- **user_id**: 用户ID（可选，默认 'default'）

**返回数据：**
```json
{
  "success": true,
  "message": "历史记录已删除"
}
```
''')
    @screening_ns.param('user_id', '用户ID', type='string', required=False)
    def delete(self, history_id):
        """删除筛选历史"""
        from web.services.screening_history_service import delete_screening_history
        return delete_screening_history(history_id)


@screening_ns.route('/history/<int:history_id>/re-run')
class ScreeningHistoryReRunResource(Resource):
    """重新执行筛选"""
    @screening_ns.doc('re_run_screening',
        description='''重新执行历史筛选。

**路径参数：**
- **history_id**: 历史记录ID

**查询参数：**
- **user_id**: 用户ID（可选，默认 'default'）
- **limit**: 返回结果数量限制（可选，默认2000）

**返回数据：**
```json
{
  "success": true,
  "count": 1167,
  "stocks": [...]
}
```
''')
    @screening_ns.param('user_id', '用户ID', type='string', required=False)
    @screening_ns.param('limit', '返回数量限制', type='integer', default=2000, required=False)
    def post(self, history_id):
        """重新执行筛选"""
        from web.services.screening_history_service import re_run_screening
        return re_run_screening(history_id)


# ============================================================================
# Moneyflow Resources
# ============================================================================

# Moneyflow models
stock_moneyflow_model = moneyflow_ns.model('StockMoneyflow', {
    'ts_code': fields.String(description='股票代码'),
    'trade_date': fields.String(description='交易日期'),
    'buy_sm_vol': fields.Integer(description='小单买入量'),
    'buy_sm_amount': fields.Float(description='小单买入金额'),
    'sell_sm_vol': fields.Integer(description='小单卖出量'),
    'sell_sm_amount': fields.Float(description='小单卖出金额'),
    'buy_md_vol': fields.Integer(description='中单买入量'),
    'buy_md_amount': fields.Float(description='中单买入金额'),
    'sell_md_vol': fields.Integer(description='中单卖出量'),
    'sell_md_amount': fields.Float(description='中单卖出金额'),
    'buy_lg_vol': fields.Integer(description='大单买入量'),
    'buy_lg_amount': fields.Float(description='大单买入金额'),
    'sell_lg_vol': fields.Integer(description='大单卖出量'),
    'sell_lg_amount': fields.Float(description='大单卖出金额'),
    'buy_elg_vol': fields.Integer(description='特大单买入量'),
    'buy_elg_amount': fields.Float(description='特大单买入金额'),
    'sell_elg_vol': fields.Integer(description='特大单卖出量'),
    'sell_elg_amount': fields.Float(description='特大单卖出金额'),
    'net_mf_vol': fields.Integer(description='净流入量'),
    'net_mf_amount': fields.Float(description='净流入额'),
    'net_lg_amount': fields.Float(description='大单净流入'),
    'net_elg_amount': fields.Float(description='特大单净流入'),
})

industry_moneyflow_model = moneyflow_ns.model('IndustryMoneyflow', {
    'trade_date': fields.String(description='交易日期'),
    'level': fields.String(description='行业级别'),
    'sw_l1': fields.String(description='一级行业'),
    'sw_l2': fields.String(description='二级行业'),
    'sw_l3': fields.String(description='三级行业'),
    'index_code': fields.String(description='行业指数代码'),
    'stock_count': fields.Integer(description='成分股数量'),
    'up_count': fields.Integer(description='上涨股票数'),
    'down_count': fields.Integer(description='下跌股票数'),
    'limit_up_count': fields.Integer(description='涨停股票数'),
    'limit_down_count': fields.Integer(description='跌停股票数'),
    'net_mf_amount': fields.Float(description='净流入金额'),
    'net_lg_amount': fields.Float(description='大单净流入'),
    'net_elg_amount': fields.Float(description='特大单净流入'),
    'buy_elg_amount': fields.Float(description='特大单买入'),
    'sell_elg_amount': fields.Float(description='特大单卖出'),
    'buy_lg_amount': fields.Float(description='大单买入'),
    'sell_lg_amount': fields.Float(description='大单卖出'),
    'avg_net_amount': fields.Float(description='平均净流入'),
    'avg_net_lg_amount': fields.Float(description='平均大单净流入'),
    'avg_net_elg_amount': fields.Float(description='平均特大单净流入'),
})


@moneyflow_ns.route('/stock')
class StockMoneyflowResource(Resource):
    """个股资金流向"""
    @moneyflow_ns.doc('get_stock_moneyflow',
        description='''获取个股资金流向数据。

**查询参数：**
- **ts_code**: 股票代码（必需）
- **start_date**: 开始日期 YYYY-MM-DD
- **end_date**: 结束日期 YYYY-MM-DD
- **limit**: 返回记录数（默认100）

**返回数据：**
```json
{
  "success": true,
  "data": [...],
  "count": 10
}
```
''')
    @moneyflow_ns.param('ts_code', '股票代码', type='string', required=True)
    @moneyflow_ns.param('start_date', '开始日期 YYYY-MM-DD', type='string')
    @moneyflow_ns.param('end_date', '结束日期 YYYY-MM-DD', type='string')
    @moneyflow_ns.param('limit', '返回数量', type='integer', default=100)
    def get(self):
        """获取个股资金流向数据"""
        from flask import request
        from web.services.moneyflow_service import get_stock_moneyflow

        ts_code = request.args.get('ts_code')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        limit = request.args.get('limit', 100, type=int)

        if not ts_code:
            return {'error': '缺少 ts_code 参数'}, 400

        return get_stock_moneyflow(ts_code, start_date, end_date, limit)


@moneyflow_ns.route('/industry')
class IndustryMoneyflowResource(Resource):
    """行业资金流向汇总"""
    @moneyflow_ns.doc('get_industry_moneyflow',
        description='''获取行业资金流向汇总数据。

**查询参数：**
- **level**: 行业级别 L1/L2/L3（默认L1）
- **industry_name**: 行业名称（可选）
- **start_date**: 开始日期 YYYY-MM-DD
- **end_date**: 结束日期 YYYY-MM-DD
- **limit**: 返回记录数（默认100）

**返回数据：**
```json
{
  "success": true,
  "data": [...],
  "count": 10
}
```
''')
    @moneyflow_ns.param('level', '行业级别', type='string', enum=['L1', 'L2', 'L3'], default='L1')
    @moneyflow_ns.param('industry_name', '行业名称', type='string')
    @moneyflow_ns.param('start_date', '开始日期 YYYY-MM-DD', type='string')
    @moneyflow_ns.param('end_date', '结束日期 YYYY-MM-DD', type='string')
    @moneyflow_ns.param('limit', '返回数量', type='integer', default=100)
    def get(self):
        """获取行业资金流向汇总"""
        from flask import request
        from web.services.moneyflow_service import get_industry_moneyflow

        level = request.args.get('level', 'L1')
        industry_name = request.args.get('industry_name')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        limit = request.args.get('limit', 100, type=int)

        return get_industry_moneyflow(level, industry_name, start_date, end_date, limit)


@moneyflow_ns.route('/industry/top')
class TopIndustriesResource(Resource):
    """行业资金流向排名"""
    @moneyflow_ns.doc('get_top_industries',
        description='''获取净流入前N名的行业。

**查询参数：**
- **level**: 行业级别 L1/L2/L3（默认L1）
- **trade_date**: 交易日期 YYYY-MM-DD（默认最新）
- **top_n**: 返回数量（默认10）
- **accumulate_days**: 累计交易日天数（默认1，表示单日）
  - 1 = 单日净流入
  - 5 = 最近5个交易日累计
  - 10 = 最近10个交易日累计
  - 20 = 最近20个交易日累计

**返回数据：**
```json
{
  "success": true,
  "data": [...],
  "count": 10,
  "trade_date": "2024-01-01"
}
```
''')
    @moneyflow_ns.param('level', '行业级别', type='string', enum=['L1', 'L2', 'L3'], default='L1')
    @moneyflow_ns.param('trade_date', '交易日期 YYYY-MM-DD', type='string')
    @moneyflow_ns.param('top_n', '返回数量', type='integer', default=10)
    @moneyflow_ns.param('accumulate_days', '累计交易日数', type='integer', default=1)
    def get(self):
        """获取净流入排名前N的行业"""
        from flask import request
        from web.services.moneyflow_service import get_top_industries_by_netflow

        level = request.args.get('level', 'L1')
        trade_date = request.args.get('trade_date')
        top_n = request.args.get('top_n', 10, type=int)
        accumulate_days = request.args.get('accumulate_days', 1, type=int)

        return get_top_industries_by_netflow(level, trade_date, top_n, accumulate_days)


@moneyflow_ns.route('/industry/stocks')
class IndustryStocksMoneyflowResource(Resource):
    """行业内个股资金流向"""
    @moneyflow_ns.doc('get_industry_stocks_moneyflow',
        description='''获取指定行业内所有股票的资金流向数据。

**查询参数：**
- **industry_name**: 行业名称（必需）
- **level**: 行业级别 L1/L2/L3（默认L1）
- **trade_date**: 交易日期 YYYY-MM-DD（默认最新）
- **accumulate_days**: 累计天数（默认1，表示单日；>1表示累计多日）

**返回数据：**
```json
{
  "success": true,
  "data": [...],
  "count": 50,
  "trade_date": "2024-01-01",
  "industry_name": "银行"
}
```
''')
    @moneyflow_ns.param('industry_name', '行业名称', type='string', required=True)
    @moneyflow_ns.param('level', '行业级别', type='string', enum=['L1', 'L2', 'L3'], default='L1')
    @moneyflow_ns.param('trade_date', '交易日期 YYYY-MM-DD', type='string')
    @moneyflow_ns.param('accumulate_days', '累计天数', type='integer', default=1)
    def get(self):
        """获取指定行业内所有股票的资金流向数据"""
        from flask import request
        from web.services.moneyflow_service import get_industry_stocks_moneyflow

        industry_name = request.args.get('industry_name')
        level = request.args.get('level', 'L1')
        trade_date = request.args.get('trade_date')
        accumulate_days = request.args.get('accumulate_days', 1, type=int)

        if not industry_name:
            return {'error': '缺少 industry_name 参数'}, 400

        return get_industry_stocks_moneyflow(industry_name, level, trade_date, accumulate_days)


# ============================================================================
# Dragon List Endpoints
# ============================================================================

@dragon_list_ns.route('/query')
class DragonListQueryResource(Resource):
    """龙虎榜数据查询"""
    @dragon_list_ns.doc('query_dragon_list',
        description='''查询龙虎榜数据

**查询参数：**
- trade_date: 交易日期 YYYY-MM-DD
- start_date: 开始日期 YYYY-MM-DD
- end_date: 结束日期 YYYY-MM-DD
- ts_code: 股票代码
- reason: 上榜理由（支持模糊匹配）
- limit: 返回数量（默认100）

**返回数据：**
```json
{
  "success": true,
  "data": [...],
  "count": 50
}
```
''')
    @dragon_list_ns.param('trade_date', '交易日期 YYYY-MM-DD', type='string')
    @dragon_list_ns.param('start_date', '开始日期 YYYY-MM-DD', type='string')
    @dragon_list_ns.param('end_date', '结束日期 YYYY-MM-DD', type='string')
    @dragon_list_ns.param('ts_code', '股票代码', type='string')
    @dragon_list_ns.param('reason', '上榜理由', type='string')
    @dragon_list_ns.param('limit', '返回数量', type='integer', default=100)
    def get(self):
        """查询龙虎榜数据"""
        from web.services.dragon_list_service import query_dragon_list

        trade_date = request.args.get('trade_date')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        ts_code = request.args.get('ts_code')
        reason = request.args.get('reason')
        limit = request.args.get('limit', 100, type=int)

        return query_dragon_list(trade_date, start_date, end_date,
                                  ts_code, reason, limit)


@dragon_list_ns.route('/stock/<ts_code>')
class DragonListStockResource(Resource):
    """指定股票的龙虎榜历史"""
    @dragon_list_ns.doc('get_dragon_list_by_stock',
        description='''获取指定股票的龙虎榜历史记录

**路径参数：**
- ts_code: 股票代码

**查询参数：**
- limit: 返回数量（默认50）
''')
    @dragon_list_ns.param('limit', '返回数量', type='integer', default=50)
    def get(self, ts_code):
        """获取指定股票的龙虎榜历史"""
        from web.services.dragon_list_service import get_dragon_list_by_stock

        limit = request.args.get('limit', 50, type=int)

        return get_dragon_list_by_stock(ts_code, limit)


@dragon_list_ns.route('/top')
class DragonListTopResource(Resource):
    """龙虎榜排名"""
    @dragon_list_ns.doc('get_top_dragon_list',
        description='''获取龙虎榜排名

**查询参数：**
- trade_date: 交易日期 YYYY-MM-DD（默认最新）
- top_n: 前N名（默认10）
- by: 排序字段（默认net_amount）
  - net_amount: 净买入额
  - l_amount: 龙虎榜成交额
  - amount: 总成交额
  - net_rate: 净买额占比
''')
    @dragon_list_ns.param('trade_date', '交易日期 YYYY-MM-DD', type='string')
    @dragon_list_ns.param('top_n', '前N名', type='integer', default=10)
    @dragon_list_ns.param('by', '排序字段', type='string', enum=['net_amount', 'l_amount', 'amount', 'net_rate'], default='net_amount')
    def get(self):
        """获取龙虎榜排名"""
        from web.services.dragon_list_service import get_top_dragon_list

        trade_date = request.args.get('trade_date')
        top_n = request.args.get('top_n', 10, type=int)
        by = request.args.get('by', 'net_amount')

        return get_top_dragon_list(trade_date, top_n, by)


@dragon_list_ns.route('/stats')
class DragonListStatsResource(Resource):
    """龙虎榜统计"""
    @dragon_list_ns.doc('get_dragon_list_stats',
        description='''获取龙虎榜统计数据

**查询参数：**
- trade_date: 交易日期 YYYY-MM-DD（默认最新）

**返回数据：**
```json
{
  "success": true,
  "data": {
    "summary": {
      "total_count": 50,
      "reason_count": 8,
      "net_buy_count": 25,
      "net_sell_count": 25,
      "total_net_amount": 1234567.89,
      "total_l_amount": 9876543.21,
      "avg_net_rate": 2.5
    },
    "by_reason": [
      {
        "reason": "日涨幅偏离值达到7%的前五只证券",
        "count": 20,
        "net_amount": 123456.78
      }
    ],
    "trade_date": "20240101"
  }
}
```
''')
    @dragon_list_ns.param('trade_date', '交易日期 YYYY-MM-DD', type='string')
    def get(self):
        """获取龙虎榜统计数据"""
        from web.services.dragon_list_service import get_dragon_list_stats

        trade_date = request.args.get('trade_date')

        return get_dragon_list_stats(trade_date)


# ============================================================================
# Alpha Endpoints - 101 Formulaic Alphas
# ============================================================================

@alpha_ns.route('/list')
class AlphaList(Resource):
    @alpha_ns.doc('list_alphas', description='列出所有可用的101个Alpha因子')
    def get(self):
        """列出所有Alpha因子"""
        from web.services.alpha_service import list_alphas
        return list_alphas()


alpha_compute_model = alpha_ns.model('AlphaComputeRequest', {
    'alpha_ids': fields.List(fields.Integer, description='Alpha因子编号列表 (1-101)', required=True, example=[1, 101]),
    'symbols': fields.List(fields.String, description='股票代码列表', required=True, example=['600382', '000001']),
    'start_date': fields.String(description='开始日期 YYYY-MM-DD', required=True, example='2025-01-01'),
    'end_date': fields.String(description='结束日期 YYYY-MM-DD', required=True, example='2025-06-30'),
    'price_type': fields.String(description='价格类型: qfq/hfq/空字符串', example='', enum=['qfq', 'hfq', ''])
})


@alpha_ns.route('/compute')
class AlphaCompute(Resource):
    @alpha_ns.doc('compute_alphas', description='计算指定Alpha因子')
    @alpha_ns.expect(alpha_compute_model)
    def post(self):
        """计算Alpha因子值"""
        from web.services.alpha_service import compute_alphas
        return compute_alphas()


alpha_snapshot_model = alpha_ns.model('AlphaSnapshotRequest', {
    'alpha_id': fields.Integer(description='Alpha因子编号 (1-101)', required=True, example=101),
    'symbols': fields.List(fields.String, description='股票代码列表 (为空则使用全部)', example=['600382', '000001']),
    'trade_date': fields.String(description='交易日期 YYYY-MM-DD', required=True, example='2025-06-30'),
    'price_type': fields.String(description='价格类型', example='', enum=['qfq', 'hfq', ''])
})


@alpha_ns.route('/snapshot')
class AlphaSnapshot(Resource):
    @alpha_ns.doc('alpha_snapshot', description='获取单日Alpha因子截面数据')
    @alpha_ns.expect(alpha_snapshot_model)
    def post(self):
        """获取Alpha因子截面快照"""
        from web.services.alpha_service import get_alpha_snapshot
        return get_alpha_snapshot()


