# MyTrader - 量化交易系统

一个基于 Python 的量化交易回测系统，支持多数据源（Tushare、AKShare）。

## 📋 目录

- [项目结构](#项目结构)
- [架构说明](#架构说明)
- [快速开始](#快速开始)
- [运行服务](#运行服务)
- [数据源](#数据源)
- [功能特性](#功能特性)
- [配置说明](#配置说明)
- [开发说明](#开发说明)

## 📁 项目结构

```
mytrader/
├── README.md                   # 项目说明
├── requirements.txt            # Python 依赖
├── config/                     # 配置文件
│   └── settings.py            # 项目配置
│
├── data/                      # 数据目录
│   ├── tushare_data.db        # Tushare 数据库
│   ├── tasks.db               # 任务数据库
│   └── akshare_data.db        # AKShare 数据库
│
├── scripts/                   # 可执行脚本
│   ├── start_worker.py        # Worker 服务启动脚本
│   ├── download_tushare.py    # Tushare 数据下载
│   ├── download_akshare.py    # AKShare 数据下载
│   ├── query_turnover.py      # 换手率查询
│   └── run_backtest.py        # 运行回测
│
├── src/                       # 源代码
│   ├── __init__.py
│   ├── data_sources/          # 数据源模块
│   │   ├── __init__.py
│   │   ├── base.py            # 基类
│   │   ├── tushare.py         # Tushare 实现
│   │   └── akshare.py         # AKShare 实现
│   ├── strategies/            # 交易策略
│   │   └── ma_strategy.py     # 移动平均线策略
│   └── utils/                 # 工具模块
│       └── stock_names.py     # 股票名称映射
│
├── web/                       # Web 应用
│   ├── app.py                 # Flask 应用入口
│   ├── routes.py              # API 路由
│   ├── tasks.py               # 任务管理器
│   └── services/              # 服务层
│
├── worker/                    # Worker 服务
│   ├── task_worker.py         # 任务执行器
│   ├── handlers.py            # 任务处理器
│   └── utils.py               # 工具函数
│
└── tests/                     # 测试代码
    └── test_data_sources.py
```

## 🏗️ 架构说明

本系统采用 **API + Worker 分离架构**：

```
┌─────────────┐      创建任务       ┌─────────────┐
│   Web API   │ ──────────────────> │  Task DB    │
│  (Flask)    │                     │  (SQLite)   │
└─────────────┘                     └──────┬──────┘
      │                                    │
      │ 提供API                             │ 轮询任务
      v                                    v
┌─────────────┐                     ┌─────────────┐
│   前端界面   │                     │   Worker    │
│  (Browser)  │ <────────────────── │  Service    │
└─────────────┘      查询状态        └─────────────┘
```

**关键组件：**

- **Web API**: 提供 RESTful API，处理任务创建和查询
- **Task DB**: SQLite 数据库，持久化任务状态
- **Worker Service**: 独立进程，轮询并执行后台任务
- **前端界面**: Vue.js 单页应用，实时显示任务进度

**优势：**

- ✅ API 和 Worker 完全解耦，独立扩展
- ✅ API 重启不影响运行中的任务
- ✅ 支持任务恢复（checkpoint）
- ✅ 可跨机器部署 Worker

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置

编辑 `config/settings.py`，设置你的 Tushare Token：

```python
TUSHARE_TOKEN = "你的_Tushare_Token"
```

获取 Token：https://tushare.pro/user/token

### 3. 下载数据

**使用 Tushare 下载：**
```bash
python scripts/download_tushare.py
```

**使用 AKShare 下载：**
```bash
python scripts/download_akshare.py
```

### 4. 运行回测

```bash
python scripts/run_backtest.py
```

## 🚀 运行服务

### 开发环境启动

需要启动两个服务：**API 服务器**和 **Worker 服务**。

#### 方式一：手动启动（推荐用于开发）

**Terminal 1: 启动 API 服务器**
```bash
# 确保虚拟环境已激活
source .venv/bin/activate

# 启动 API 服务器
python web/app.py

# API 将运行在 http://localhost:5001
```

**Terminal 2: 启动 Worker 服务**
```bash
# 确保虚拟环境已激活
source .venv/bin/activate

# 启动 Worker（使用默认配置）
python scripts/start_worker.py

# 或者自定义配置
python scripts/start_worker.py --poll-interval 2 --max-concurrent 2

# Worker 将开始轮询任务数据库并执行任务
```

#### 方式二：使用后台进程（推荐用于生产）

```bash
# 启动 API 服务器（后台）
nohup python web/app.py > logs/api.log 2>&1 &

# 启动 Worker 服务（后台）
nohup python scripts/start_worker.py --poll-interval 5 --max-concurrent 1 > logs/worker.log 2>&1 &

# 查看日志
tail -f logs/worker.log
```

### 环境变量配置

可以通过环境变量配置 Worker 行为：

```bash
# .env 文件
WORKER_POLL_INTERVAL=5      # Worker 轮询间隔（秒）
WORKER_MAX_CONCURRENT=1     # 最大并发任务数
WORKER_LOG_FILE=logs/worker.log  # 日志文件路径
```

### 验证服务状态

1. **检查 API 服务**
   ```bash
   curl http://localhost:5001/api/tasks
   ```

2. **检查 Worker 服务**
   - 查看 Worker 日志输出
   - 在 Web UI 中创建任务，观察状态变化：`pending` → `running` → `completed`

3. **创建测试任务**
   ```bash
   curl -X POST http://localhost:5001/api/tasks/create \
     -H "Content-Type: application/json" \
     -d '{
       "task_type": "update_stock_prices",
       "params": {
         "stock_range": "custom",
         "custom_stocks": ["600382"]
       }
     }'
   ```

### 停止服务

```bash
# 停止 API 服务器
pkill -f "python web/app.py"

# 停止 Worker 服务（优雅关闭，等待当前任务完成）
pkill -f "start_worker.py"

# 或者使用 Ctrl+C 停止前台进程
```

## 📊 数据源

### Tushare
- ✅ A 股日线数据
- ✅ 前复权/后复权
- ✅ 复权因子
- ⚠️ 换手率需要积分（2000+）

### AKShare
- ✅ A 股日线数据
- ✅ 港股日线数据
- ✅ 前复权
- ✅ 换手率（免费）

## 💡 功能特性

### 数据下载
- ✅ 自动检查本地数据，避免重复下载
- ✅ 智能判断数据是否需要更新
- ✅ 支持增量更新
- ✅ 统一的数据库格式

### 数据查询
```python
from src.data_sources.tushare import TushareDB

db = TushareDB(token="YOUR_TOKEN")

# 加载数据
df = db.load_bars("600382", "2025-01-01", "2025-12-31")

# 获取股票名称
name = db.get_stock_name("600382")  # "广东明珠"
```

### 回测
- ✅ 支持多种交易策略
- ✅ 自动记录买卖交易
- ✅ 性能分析和可视化
- ✅ 手续费计算

## 📝 开发说明

### 添加新数据源

1. 继承 `BaseStockDB` 基类
2. 实现 `save_daily()` 方法
3. 可选：实现 `_get_stock_name_from_api()`

示例：
```python
from src.data_sources.base import BaseStockDB

class MyDataSource(BaseStockDB):
    def save_daily(self, symbol, start_date, end_date, adjust="qfq"):
        # 实现数据下载逻辑
        pass
```

### 添加新策略

在 `src/strategies/` 下创建新策略文件：
```python
import backtrader as bt

class MyStrategy(bt.Strategy):
    def __init__(self):
        # 策略初始化
        pass

    def next(self):
        # 交易逻辑
        pass
```

## ⚙️ 配置说明

主要配置项在 `config/settings.py`：

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| TUSHARE_TOKEN | Tushare API Token | - |
| TUSHARE_DB_PATH | Tushare 数据库路径 | data/tushare_data.db |
| AKSHARE_DB_PATH | AKShare 数据库路径 | data/akshare_data.db |
| DEFAULT_INITIAL_CASH | 初始资金 | 1000000 |
| DEFAULT_COMMISSION | 手续费率 | 0.002 |
| WORKER_POLL_INTERVAL | Worker 轮询间隔（秒） | 5 |
| WORKER_MAX_CONCURRENT | Worker 最大并发任务数 | 1 |
| WORKER_LOG_FILE | Worker 日志文件路径 | 空（仅控制台） |

## 📄 License

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！
