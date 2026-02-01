# MyTrader - 量化交易系统

一个基于 Python 的量化交易回测系统，支持多数据源（Tushare、AKShare）。

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
│   └── akshare_data.db        # AKShare 数据库
│
├── scripts/                   # 可执行脚本
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
└── tests/                     # 测试代码
    └── test_data_sources.py
```

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

## 📄 License

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！
