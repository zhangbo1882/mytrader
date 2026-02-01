# 股票数据库查询 API 文档

## 概述

`StockQuery` 类提供了一个功能丰富、易于使用的股票数据查询接口，支持多种查询方式、数据完整性检查、统计分析和技术指标计算。

## 目录

- [初始化](#初始化)
- [基础查询](#基础查询)
- [条件过滤查询](#条件过滤查询)
- [复合条件查询](#复合条件查询)
- [批量查询](#批量查询)
- [数据完整性检查](#数据完整性检查)
- [统计分析](#统计分析)
- [技术指标](#技术指标)
- [使用示例](#使用示例)
- [性能优化](#性能优化)
- [常见问题](#常见问题)

---

## 初始化

### 通过工厂方法创建（推荐）

```python
from src.data_sources.tushare import TushareDB

db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))
query = db.query()
```

### 直接实例化

```python
from src.data_sources.query.stock_query import StockQuery

query = StockQuery("data/tushare_data.db")
```

---

## 基础查询

### query_bars()

查询K线数据，支持不复权/前复权。

**参数：**
- `symbol` (str): 股票代码，如 "600382"
- `start` (str): 开始日期，格式 "YYYY-MM-DD"
- `end` (str): 结束日期，格式 "YYYY-MM-DD"
- `interval` (str): 时间周期，默认 "1d"
- `price_type` (str): 价格类型，""=不复权（默认），"qfq"=前复权

**返回：** pandas DataFrame，包含 OHLCV 数据

**示例：**
```python
# 查询不复权数据
df = query.query_bars("600382", "2025-01-01", "2025-12-31")

# 查询前复权数据
df_qfq = query.query_bars("600382", "2025-01-01", "2025-12-31", price_type="qfq")
```

**返回列：**
- `datetime`: 交易日期
- `open`: 开盘价
- `high`: 最高价
- `low`: 最低价
- `close`: 收盘价
- `volume`: 成交量
- `turnover`: 换手率（如果可用）
- `pct_chg`: 涨跌幅（%）
- `amount`: 成交额

---

## 条件过滤查询

### query_by_price_range()

按价格范围查询。

**参数：**
- `symbol` (str): 股票代码
- `start` (str): 开始日期
- `end` (str): 结束日期
- `min_price` (float, optional): 最低价格
- `max_price` (float, optional): 最高价格
- `price_column` (str): 价格列，默认 "close"

**示例：**
```python
# 查询价格在 10-50 元之间的交易日
df = query.query_by_price_range("600382", "2025-01-01", "2025-12-31",
                                min_price=10, max_price=50)
```

### query_by_volume()

按成交量范围查询。

**参数：**
- `symbol` (str): 股票代码
- `start` (str): 开始日期
- `end` (str): 结束日期
- `min_volume` (float, optional): 最小成交量
- `max_volume` (float, optional): 最大成交量

**示例：**
```python
# 查询成交量 > 100万股的交易日
df = query.query_by_volume("600382", "2025-01-01", "2025-12-31",
                          min_volume=1000000)
```

### query_by_turnover()

按换手率范围查询。

**参数：**
- `symbol` (str): 股票代码
- `start` (str): 开始日期
- `end` (str): 结束日期
- `min_turnover` (float, optional): 最小换手率（%）
- `max_turnover` (float, optional): 最大换手率（%）

**示例：**
```python
# 查询换手率 > 3% 的交易日
df = query.query_by_turnover("600382", "2025-01-01", "2025-12-31",
                             min_turnover=3.0)
```

### query_by_change()

按涨跌幅范围查询。

**参数：**
- `symbol` (str): 股票代码
- `start` (str): 开始日期
- `end` (str): 结束日期
- `min_change` (float, optional): 最小涨跌幅（%）
- `max_change` (float, optional): 最大涨跌幅（%）

**示例：**
```python
# 查询涨停股（涨跌幅 > 9.5%）
df = query.query_by_change("600382", "2025-01-01", "2025-12-31",
                           min_change=9.5)

# 查询跌幅 > 5% 的交易日
df = query.query_by_change("600382", "2025-01-01", "2025-12-31",
                           max_change=-5.0)
```

---

## 复合条件查询

### query_with_filters()

使用多个条件组合查询。

**参数：**
- `symbol` (str): 股票代码
- `start` (str): 开始日期
- `end` (str): 结束日期
- `filters` (dict): 过滤条件字典

**filters 格式：**
```python
{
    'close': (10, 50),          # 价格 10-50
    'volume': (1000000, None),  # 成交量 > 100万
    'pct_chg': (-5, 5)          # 涨跌幅 -5% 到 5%
}
```

元组格式：`(最小值, 最大值)`，`None` 表示无限制

**示例：**
```python
df = query.query_with_filters(
    "600382", "2025-01-01", "2025-12-31",
    filters={
        'close': (10, 50),          # 价格在 10-50 元之间
        'volume': (1000000, None),  # 成交量 > 100万股
        'pct_chg': (-5, 5)          # 涨跌幅在 -5% 到 5% 之间
    }
)

# 只查询价格 > 50 的数据
df = query.query_with_filters(
    "600382", "2025-01-01", "2025-12-31",
    filters={'close': (50, None)}
)
```

---

## 批量查询

### query_multiple_symbols()

批量查询多只股票。

**参数：**
- `symbols` (list): 股票代码列表
- `start` (str): 开始日期
- `end` (str): 结束日期
- `interval` (str): 时间周期
- `price_type` (str): 价格类型

**返回：** 字典，key 为股票代码，value 为对应的 DataFrame

**示例：**
```python
symbols = ["600382", "600711", "000001"]
results = query.query_multiple_symbols(symbols, "2025-01-01", "2025-12-31")

# 访问每只股票的数据
for symbol, df in results.items():
    print(f"{symbol}: {len(df)} 条记录")
    if not df.empty:
        print(f"  最新收盘价: {df.iloc[-1]['close']:.2f}")
```

---

## 数据完整性检查

### check_data_completeness()

检查数据的完整性，包括缺失天数、完整率等。

**参数：**
- `symbol` (str): 股票代码
- `start` (str): 开始日期
- `end` (str): 结束日期
- `interval` (str): 时间周期

**返回：** 包含完整性信息的字典

**返回字段：**
- `total_days` (int): 应有的交易日数量
- `actual_days` (int): 实际数据天数
- `missing_count` (int): 缺失天数
- `completeness_rate` (float): 完整率 0-1
- `missing_days` (list): 缺失日期列表
- `first_date` (str): 首个数据日期
- `last_date` (str): 最后数据日期

**示例：**
```python
info = query.check_data_completeness("600382", "2025-01-01", "2025-12-31")
print(f"完整率: {info['completeness_rate']:.2%}")
print(f"应有交易日: {info['total_days']}")
print(f"实际数据: {info['actual_days']}")
print(f"缺失天数: {info['missing_count']}")
print(f"缺失日期: {info['missing_days']}")

# 判断数据是否完整
if info['completeness_rate'] < 0.95:
    print("⚠️  数据不完整，建议补充下载")
```

### find_missing_dates()

查找缺失的交易日。

**参数：**
- `symbol` (str): 股票代码
- `start` (str): 开始日期
- `end` (str): 结束日期
- `interval` (str): 时间周期

**返回：** 缺失日期列表

**示例：**
```python
missing_dates = query.find_missing_dates("600382", "2025-01-01", "2025-12-31")
if missing_dates:
    print(f"缺失 {len(missing_dates)} 个交易日")
    print(f"缺失日期: {missing_dates}")
```

---

## 统计分析

### calculate_returns()

计算收益率。

**参数：**
- `symbol` (str): 股票代码
- `start` (str): 开始日期
- `end` (str): 结束日期
- `period` (int): 周期，1=日收益，5=周收益，20=月收益

**返回：** 包含收益率列 'return' 的 DataFrame

**示例：**
```python
# 计算日收益率
df = query.calculate_returns("600382", "2025-01-01", "2025-12-31", period=1)

# 计算周收益率
df_weekly = query.calculate_returns("600382", "2025-01-01", "2025-12-31", period=5)

# 计算月收益率
df_monthly = query.calculate_returns("600382", "2025-01-01", "2025-12-31", period=20)
```

### calculate_volatility()

计算波动率。

**参数：**
- `symbol` (str): 股票代码
- `start` (str): 开始日期
- `end` (str): 结束日期
- `window` (int): 窗口大小，默认 20

**返回：** 包含波动率列 'volatility' 的 DataFrame

**示例：**
```python
# 计算20日波动率
df = query.calculate_volatility("600382", "2025-01-01", "2025-12-31", window=20)

# 查看最近波动率
latest_vol = df.iloc[-1]['volatility']
print(f"最新波动率: {latest_vol:.4f}")
```

### get_summary_stats()

获取汇总统计信息。

**参数：**
- `symbol` (str): 股票代码
- `start` (str): 开始日期
- `end` (str): 结束日期

**返回：** 统计信息字典

**返回字段：**
- `total_days` (int): 总交易日数
- `avg_volume` (float): 平均成交量
- `avg_turnover` (float): 平均换手率
- `total_return` (float): 总收益率（%）
- `annual_return` (float): 年化收益率（%）
- `volatility` (float): 波动率（%）
- `max_drawdown` (float): 最大回撤（%）
- `max_price` (float): 最高价
- `min_price` (float): 最低价

**示例：**
```python
stats = query.get_summary_stats("600382", "2025-01-01", "2025-12-31")
print(f"总收益率: {stats['total_return']:.2%}")
print(f"年化收益率: {stats['annual_return']:.2%}")
print(f"最大回撤: {stats['max_drawdown']:.2%}")
print(f"波动率: {stats['volatility']:.2%}")
print(f"最高价: {stats['max_price']:.2f}")
print(f"最低价: {stats['min_price']:.2f}")
```

---

## 技术指标

`TechnicalIndicators` 类提供了常用技术指标的计算方法。所有方法都是静态方法，可以直接调用。

### 移动平均线

#### SMA - 简单移动平均线

```python
from src.data_sources.query.technical import TechnicalIndicators

df = query.query_bars("600382", "2025-01-01", "2025-12-31")

# 计算5日、20日移动平均线
df['ma5'] = TechnicalIndicators.sma(df, period=5)
df['ma20'] = TechnicalIndicators.sma(df, period=20)
```

#### EMA - 指数移动平均线

```python
# 计算12日、26日 EMA
df['ema12'] = TechnicalIndicators.ema(df, period=12)
df['ema26'] = TechnicalIndicators.ema(df, period=26)
```

### 动量指标

#### RSI - 相对强弱指标

```python
# 计算14日RSI
df['rsi'] = TechnicalIndicators.rsi(df, period=14)

# RSI > 70 超买，RSI < 30 超卖
overbought = df[df['rsi'] > 70]
oversold = df[df['rsi'] < 30]
```

#### MACD - 指数平滑异同移动平均线

```python
# 计算 MACD（会返回添加了新列的DataFrame）
df = TechnicalIndicators.macd(df, fast=12, slow=26, signal=9)

# 新增列：macd, macd_signal, macd_hist
# MACD金叉：macd上穿macd_signal
# MACD死叉：macd下穿macd_signal
```

#### Williams %R - 威廉指标

```python
# 计算14日威廉指标
df['williams_r'] = TechnicalIndicators.williams_r(df, period=14)

# -80以下超卖，-20以上超买
```

#### ROC - 变动率指标

```python
# 计算12日变动率
df['roc'] = TechnicalIndicators.roc(df, period=12)
```

### 波动率指标

#### 布林带

```python
# 计算布林带
df = TechnicalIndicators.bollinger_bands(df, period=20, std_dev=2.0)

# 新增列：
# - bb_upper: 上轨
# - bb_middle: 中轨（SMA）
# - bb_lower: 下轨
# - bb_width: 带宽
# - bb_pct: %B位置指标

# 价格突破上轨可能超买，触及下轨可能超卖
```

#### ATR - 平均真实波幅

```python
# 计算14日ATR
df['atr'] = TechnicalIndicators.atr(df, period=14)

# ATR越大，波动率越高
```

### 成交量指标

#### OBV - 能量潮

```python
# 计算OBV
df['obv'] = TechnicalIndicators.obv(df)

# OBV上升表示资金流入，下降表示资金流出
```

### 其他指标

#### 随机指标

```python
# 计算随机指标
df = TechnicalIndicators.stochastic(df, k_period=14, d_period=3)

# 新增列：stoch_k, stoch_d
# K > 80 超买，K < 20 超卖
```

#### CCI - 商品路径指标

```python
# 计算CCI
df['cci'] = TechnicalIndicators.cci(df, period=20)

# CCI > 100 超买，CCI < -100 超卖
```

---

## 使用示例

### 示例1：查询高换手率股票

```python
from src.data_sources.tushare import TushareDB
from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH

db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))
query = db.query()

# 查询换手率 > 5% 的交易日
df = query.query_by_turnover("600382", "2025-01-01", "2025-12-31", min_turnover=5.0)

print(f"换手率 > 5% 的天数: {len(df)}")
for _, row in df.iterrows():
    print(f"{row['datetime']}: 换手率 {row['turnover']:.2f}%, 涨跌幅 {row['pct_chg']:.2f}%")
```

### 示例2：检查数据完整性

```python
# 检查数据完整性
info = query.check_data_completeness("600382", "2025-01-01", "2025-12-31")

print(f"完整率: {info['completeness_rate']:.2%}")
if info['missing_count'] > 0:
    print(f"缺失 {info['missing_count']} 个交易日:")
    for date in info['missing_days']:
        print(f"  - {date}")
else:
    print("✅ 数据完整")
```

### 示例3：技术分析

```python
from src.data_sources.query.technical import TechnicalIndicators

# 获取数据
df = query.query_bars("600382", "2025-01-01", "2025-12-31", price_type="qfq")

# 计算技术指标
df['ma5'] = TechnicalIndicators.sma(df, period=5)
df['ma20'] = TechnicalIndicators.sma(df, period=20)
df['ma60'] = TechnicalIndicators.sma(df, period=60)
df['rsi'] = TechnicalIndicators.rsi(df, period=14)
df = TechnicalIndicators.macd(df)
df = TechnicalIndicators.bollinger_bands(df)

# 分析最新数据
latest = df.iloc[-1]
print(f"日期: {latest['datetime']}")
print(f"收盘价: {latest['close']:.2f}")
print(f"MA5: {latest['ma5']:.2f}")
print(f"MA20: {latest['ma20']:.2f}")
print(f"MA60: {latest['ma60']:.2f}")
print(f"RSI: {latest['rsi']:.2f}")
print(f"MACD: {latest['macd']:.2f}")

# 判断趋势
if latest['ma5'] > latest['ma20'] > latest['ma60']:
    print("趋势: 多头排列")
elif latest['ma5'] < latest['ma20'] < latest['ma60']:
    print("趋势: 空头排列")
else:
    print("趋势: 盘整")

# 判断超买超卖
if latest['rsi'] > 70:
    print("RSI: 超买")
elif latest['rsi'] < 30:
    print("RSI: 超卖")
```

### 示例4：多策略筛选

```python
# 策略1：金叉策略
df = query.query_bars("600382", "2025-01-01", "2025-12-31")
df['ma5'] = TechnicalIndicators.sma(df, period=5)
df['ma20'] = TechnicalIndicators.sma(df, period=20)

# 找出金叉点（MA5上穿MA20）
df['ma5_above'] = df['ma5'] > df['ma20']
df['cross'] = df['ma5_above'] != df['ma5_above'].shift(1)
golden_cross = df[df['cross'] & df['ma5_above']]
print("金叉日期:")
for _, row in golden_cross.iterrows():
    print(f"  {row['datetime']}: 收盘价 {row['close']:.2f}")

# 策略2：突破布林带上轨
df = TechnicalIndicators.bollinger_bands(df)
breakouts = df[df['close'] > df['bb_upper']]
print(f"\n突破布林带上轨的天数: {len(breakouts)}")

# 策略3：RSI超卖反弹
df['rsi'] = TechnicalIndicators.rsi(df)
oversold_rebound = df[(df['rsi'] < 30) & (df['pct_chg'] > 3)]
print(f"\nRSI超卖后反弹的天数: {len(oversold_rebound)}")
```

---

## 性能优化

### 数据库索引

系统已在以下列上创建索引以提升查询性能：
- `symbol`: 股票代码索引
- `datetime`: 日期时间索引
- `(symbol, datetime)`: 复合索引
- `turnover`: 换手率索引

### 查询优化建议

1. **限制查询范围**
   ```python
   # 好的做法
   df = query.query_bars("600382", "2025-01-01", "2025-12-31")

   # 避免查询过长时间范围
   # df = query.query_bars("600382", "2000-01-01", "2025-12-31")
   ```

2. **使用批量查询**
   ```python
   # 好的做法
   symbols = ["600382", "600711", "000001"]
   results = query.query_multiple_symbols(symbols, "2025-01-01", "2025-12-31")

   # 避免
   # for symbol in symbols:
   #     df = query.query_bars(symbol, ...)
   ```

3. **只查询需要的列**
   ```python
   # 查询后只选择需要的列
   df = query.query_bars("600382", "2025-01-01", "2025-12-31")
   df = df[['datetime', 'close', 'volume']]
   ```

4. **使用条件过滤**
   ```python
   # 好的做法 - 在数据库层面过滤
   df = query.query_by_turnover("600382", "2025-01-01", "2025-12-31", min_turnover=3.0)

   # 避免先全部查询再过滤
   # df = query.query_bars(...)
   # df = df[df['turnover'] > 3.0]
   ```

---

## 常见问题

### Q1: 为什么查询结果是空的？

**A:** 可能的原因：
1. 数据库中没有该股票的数据
2. 查询的日期范围内没有数据
3. 股票代码格式错误（应使用 "600382" 而非 "600382.SH"）

**解决方法：**
```python
# 检查数据是否存在
info = query.check_data_completeness("600382", "2025-01-01", "2025-12-31")
print(f"数据范围: {info['first_date']} 到 {info['last_date']}")
```

### Q2: 换手率数据为什么是 NaN？

**A:** 换手率数据需要调用 Tushare 的 `daily_basic` 接口，需要相应的积分权限。如果没有权限，换手率列将为空。

**解决方法：**
```python
# 检查换手率列是否存在
df = query.query_bars("600382", "2025-01-01", "2025-12-31")
if 'turnover' not in df.columns or df['turnover'].isna().all():
    print("⚠️  换手率数据不可用")
```

### Q3: 如何判断数据质量？

**A:** 使用 `check_data_completeness()` 方法：

```python
info = query.check_data_completeness("600382", "2025-01-01", "2025-12-31")

if info['completeness_rate'] > 0.95:
    print("✅ 数据质量良好")
elif info['completeness_rate'] > 0.8:
    print("⚠️  数据质量一般，有少量缺失")
else:
    print("❌ 数据质量较差，建议重新下载")
```

### Q4: 如何处理缺失日期？

**A:** 可以使用 `find_missing_dates()` 查找缺失日期，然后补充下载：

```python
missing = query.find_missing_dates("600382", "2025-01-01", "2025-12-31")
if missing:
    print(f"缺失日期: {missing}")
    # 使用 db.save_daily() 补充下载缺失日期的数据
```

### Q5: 技术指标计算出现 NaN 是否正常？

**A:** 是正常的。由于技术指标需要一定的历史数据，初始值会是 NaN：

```python
# RSI需要至少14天数据
df['rsi'] = TechnicalIndicators.rsi(df, period=14)
# 前13个值会是NaN

# 解决方法：删除NaN
df = df.dropna()
```

### Q6: 如何提高查询速度？

**A:** 参考上面的"性能优化"部分，主要方法：
1. 限制查询范围
2. 使用批量查询
3. 只查询需要的列
4. 使用条件过滤
5. 确保数据库索引已创建

---

## 更多示例

查看 `scripts/query_turnover.py` 和 `scripts/` 目录下的其他脚本以获取更多使用示例。
