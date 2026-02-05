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
            'test_handler'
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
