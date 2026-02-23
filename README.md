# MyTrader - 量化交易系统

一个基于 Python 的量化交易回测系统，使用 Tushare 数据源，提供完整的股票筛选、估值分析和策略回测功能。

## 目录

- [项目结构](#项目结构)
- [架构说明](#架构说明)
- [快速开始](#快速开始)
- [运行服务](#运行服务)
- [功能特性](#功能特性)
- [API 端点](#api-端点)
- [数据源](#数据源)
- [配置说明](#配置说明)
- [开发说明](#开发说明)

## 项目结构

```
mytrader/
├── README.md                   # 项目说明
├── requirements.txt            # Python 依赖
├── CLAUDE.md                   # Claude Code 指南
├── config/                     # 配置文件
│   └── settings.py            # 项目配置
│
├── data/                       # 数据目录
│   ├── tushare_data.db        # Tushare 数据库
│   └── tasks.db               # 任务数据库
│
├── frontend/                   # React 前端
│   ├── src/
│   │   ├── components/        # UI 组件
│   │   │   ├── backtest/      # 回测组件
│   │   │   ├── screening/     # 筛选组件
│   │   │   ├── tasks/         # 任务组件
│   │   │   └── valuation/     # 估值组件
│   │   ├── pages/             # 页面 (13个)
│   │   │   ├── BacktestPage.tsx
│   │   │   ├── ScreeningPage.tsx
│   │   │   ├── ValuationPage.tsx
│   │   │   ├── MoneyFlowPage.tsx
│   │   │   ├── DragonListPage.tsx
│   │   │   └── ...
│   │   ├── hooks/             # React Hooks
│   │   ├── services/          # API 服务
│   │   └── types/             # TypeScript 类型
│   └── package.json
│
├── scripts/                    # 可执行脚本
│   ├── start_frontend.sh      # 前端启动脚本
│   ├── start_web.sh           # 后端启动脚本
│   └── start_worker.py        # Worker 启动脚本
│
├── src/                        # 源代码
│   ├── data_sources/          # 数据源模块
│   │   ├── base.py            # 基类
│   │   ├── tushare.py         # Tushare 实现
│   │   └── query/             # 查询模块
│   ├── strategies/            # 交易策略
│   │   ├── sma_cross_strategy.py
│   │   ├── price_breakout_strategy.py
│   │   ├── analyzers.py
│   │   └── metrics.py
│   ├── screening/             # 股票筛选系统
│   │   ├── criteria/          # 筛选条件
│   │   │   ├── basic_criteria.py
│   │   │   ├── industry_criteria.py
│   │   │   ├── amplitude_criteria.py
│   │   │   ├── turnover_criteria.py
│   │   │   ├── market_criteria.py
│   │   │   └── ...
│   │   ├── calculators/       # 计算器
│   │   ├── strategies/        # 预定义策略
│   │   ├── rule_engine.py     # 规则引擎
│   │   └── screening_engine.py
│   ├── valuation/             # 估值系统
│   ├── ml/                    # 机器学习模块
│   │   ├── data_loader.py
│   │   └── financial_feature_engineer.py
│   └── utils/                 # 工具模块
│
├── web/                        # Web 应用
│   ├── app.py                 # Flask 应用入口
│   ├── restx_api.py           # REST API
│   ├── restx_namespaces.py    # API 命名空间
│   └── services/              # 服务层 (19个服务)
│       ├── backtest_service.py
│       ├── screening_service.py
│       ├── valuation_service.py
│       ├── moneyflow_service.py
│       ├── dragon_list_service.py
│       └── ...
│
├── worker/                     # Worker 服务
│   ├── task_worker.py         # 任务执行器
│   ├── handlers.py            # 任务处理器
│   └── run_worker.py          # Worker 启动
│
├── logs/                       # 日志目录
│   ├── api.log
│   ├── frontend.log
│   └── worker.log
│
└── tests/                      # 测试代码
```

## 架构说明

本系统采用 **前后端分离 + Worker 任务队列** 架构：

```
┌─────────────────┐     HTTP/REST     ┌─────────────────┐
│   Frontend      │ ◄───────────────► │   Web API       │
│   (React)       │                   │   (Flask)       │
│   Port: 5002    │                   │   Port: 5001    │
└─────────────────┘                   └────────┬────────┘
                                               │
                                               │ 创建任务
                                               v
                                      ┌─────────────────┐
                                      │    Task DB      │
                                      │   (SQLite)      │
                                      └────────┬────────┘
                                               │
                                               │ 轮询任务
                                               v
                                      ┌─────────────────┐
                                      │    Worker       │
                                      │   Service       │
                                      └─────────────────┘
```

**技术栈：**

| 层级 | 技术 |
|------|------|
| 前端 | React 18 + TypeScript + Ant Design + Vite |
| 后端 | Flask + Flask-RESTX |
| 数据库 | SQLite |
| 任务队列 | 自定义 Worker 轮询 |
| 策略引擎 | Backtrader |

**关键组件：**

- **Frontend**: React 单页应用，提供 13 个功能页面
- **Web API**: RESTful API，处理数据查询和任务管理
- **Task DB**: SQLite 数据库，持久化任务状态
- **Worker Service**: 独立进程，轮询并执行后台任务

## 快速开始

### 1. 安装依赖

```bash
# 激活虚拟环境
source .venv/bin/activate

# 安装 Python 依赖
pip install -r requirements.txt

# 安装前端依赖
cd frontend && npm install && cd ..
```

### 2. 配置

编辑 `config/settings.py`，设置你的 Tushare Token：

```python
TUSHARE_TOKEN = "你的_Tushare_Token"
```

获取 Token：https://tushare.pro/user/token

## 运行服务

需要启动三个服务：**前端**、**API 服务器** 和 **Worker 服务**。

### 启动前端 (http://localhost:5002)

```bash
source .venv/bin/activate
./scripts/start_frontend.sh
# 日志保存至: ./logs/frontend.log
```

### 启动后端 (http://localhost:5001)

```bash
source .venv/bin/activate
./scripts/start_web.sh
# 日志保存至: ./logs/api.log
```

### 启动 Worker

```bash
source .venv/bin/activate
python scripts/start_worker.py
# 日志保存至: ./logs/worker.log
```

### 验证服务状态

```bash
# 检查 API 服务
curl http://localhost:5001/api/tasks

# 访问前端界面
open http://localhost:5002
```

## 功能特性

### 股票筛选

支持 8 种筛选条件类型：

| 筛选类型 | 说明 |
|----------|------|
| 基础条件 | 市值、股价、PE、PB 等基础指标 |
| 行业条件 | 申万行业分类筛选 |
| 振幅条件 | 价格振幅筛选 |
| 换手率条件 | 成交量换手率筛选 |
| 上涨天数条件 | 连续上涨/下跌天数 |
| 市场条件 | 主板/创业板/科创板 |
| 字段条件 | 自定义字段筛选 |
| 振幅列条件 | 振幅相关列筛选 |

### 股票估值

- PE/PB/PS/PEG 估值
- DCF 现金流折现
- 组合估值模型

### 策略回测

- SMA 交叉策略
- 价格突破策略
- 自定义策略支持
- 性能分析和可视化

### 其他功能

- **资金流向**: 资金流动追踪分析
- **龙虎榜**: 龙虎榜数据查询
- **AI 功能**: AI 智能筛选、AI 预测
- **财务分析**: 财务指标查询
- **申万行业**: 行业分类统计
- **板块管理**: 板块数据管理
- **收藏功能**: 股票收藏管理

## API 端点

| 端点 | 说明 |
|------|------|
| `/api/stock` | 股票数据 |
| `/api/valuation` | 估值数据 |
| `/api/backtest` | 策略回测 |
| `/api/screening` | 股票筛选 |
| `/api/moneyflow` | 资金流向 |
| `/api/dragon-list` | 龙虎榜 |
| `/api/financial` | 财务数据 |
| `/api/sw-industry` | 申万行业 |
| `/api/boards` | 板块数据 |
| `/api/favorites` | 收藏管理 |
| `/api/liquidity` | 流动性数据 |
| `/api/tasks` | 任务管理 |

## 数据源

### Tushare
- A 股日线数据
- 前复权/后复权
- 复权因子
- 换手率（需要积分 2000+）

## 配置说明

主要配置项在 `config/settings.py`：

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| TUSHARE_TOKEN | Tushare API Token | - |
| TUSHARE_DB_PATH | Tushare 数据库路径 | data/tushare_data.db |
| DEFAULT_INITIAL_CASH | 初始资金 | 1000000 |
| DEFAULT_COMMISSION | 手续费率 | 0.002 |
| WORKER_POLL_INTERVAL | Worker 轮询间隔（秒） | 5 |
| WORKER_MAX_CONCURRENT | Worker 最大并发任务数 | 1 |

## 开发说明

### 添加新数据源

1. 继承 `BaseStockDB` 基类
2. 实现 `save_daily()` 方法

```python
from src.data_sources.base import BaseStockDB

class MyDataSource(BaseStockDB):
    def save_daily(self, symbol, start_date, end_date, adjust="qfq"):
        # 实现数据下载逻辑
        pass
```

### 添加新筛选条件

在 `src/screening/criteria/` 下创建新的条件文件：

```python
from src.screening.base_criteria import BaseCriteria

class MyCriteria(BaseCriteria):
    def filter(self, df):
        # 实现筛选逻辑
        return filtered_df
```

### 添加新策略

在 `src/strategies/` 下创建新策略文件：

```python
import backtrader as bt

class MyStrategy(bt.Strategy):
    def __init__(self):
        pass

    def next(self):
        # 交易逻辑
        pass
```

## License

MIT License
