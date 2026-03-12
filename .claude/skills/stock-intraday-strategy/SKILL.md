---
name: stock-intraday-strategy
description: A股日内交易策略分析。当用户提供股票代码询问日内交易策略、高开低开分析、日内波动特征、冲高下探概率时触发。适用场景：A股日内波段交易、开盘策略制定、波动率分析。
---

# A股日内交易策略分析

## 技能简介

你是一名专精「**A股日内交易策略**」的量化分析师。当用户提供股票代码时，从DuckDB数据库获取历史行情数据，计算过去20日和60日的日内波动特征，给出交易策略建议。

**适用场景**：
- 用户提供股票代码（如 601231、600519）
- 询问日内交易策略
- 询问高开低开特征
- 询问冲高下探概率
- 询问日内波动幅度

---

## 数据来源

- **数据库**: DuckDB (`data/financial_data.duckdb`)
- **表**: `bars_a_1d`（A股日行情数据）
- **关键字段**:
  - `stock_code`: 股票代码（6位纯数字，如 '601231'）
  - `datetime`: 交易日期
  - `open`: 开盘价
  - `high`: 最高价
  - `low`: 最低价
  - `close`: 收盘价
  - `pre_close`: 昨收价
  - `pct_chg`: 涨跌幅(%)

---

## 执行流程

### Step 1: 获取数据

使用以下SQL查询获取最近65个交易日的数据：

```python
import duckdb

conn = duckdb.connect('data/financial_data.duckdb', read_only=True)

query = """
SELECT
    datetime as trade_date,
    open,
    high,
    low,
    close,
    pre_close,
    pct_chg,
    -- 高开低开判断
    CASE WHEN open > pre_close THEN 1 ELSE 0 END as is_high_open,
    CASE WHEN open < pre_close THEN 1 ELSE 0 END as is_low_open,
    -- 最高价与开盘价差价百分比（冲高幅度）
    ROUND((high - open) / open * 100, 2) as high_open_pct,
    -- 最低价与开盘价差价百分比（下探幅度）
    ROUND((low - open) / open * 100, 2) as low_open_pct,
    -- 日内振幅（用于T+0策略）
    ROUND((high - low) / open * 100, 2) as intraday_range
FROM bars_a_1d
WHERE stock_code = ?
ORDER BY datetime DESC
LIMIT 65
"""

df = conn.execute(query, [stock_code]).fetchdf()
conn.close()
```

### Step 2: 计算统计指标

对每个股票，分别计算20日和60日的以下指标：

| 指标 | 计算方式 |
|------|---------|
| 高开日数 | is_high_open = 1 的天数 |
| 低开日数 | is_low_open = 1 的天数 |
| 高开日冲高概率 | 高开日中 high > open 的比例 |
| 高开日冲高幅度 | high_open_pct 的平均值/分位值(P25/P50/P90)/最大/最小 |
| 低开日下探概率 | 低开日中 low < open 的比例 |
| 低开日下探幅度 | low_open_pct 的平均值/分位值(P25/P50/P90)/最大/最小 |
| 高开日最终涨跌幅 | 高开日 pct_chg 的平均值/收涨比例 |
| 低开日最终涨跌幅 | 低开日 pct_chg 的平均值/收涨比例 |
| **日内振幅** | (high - low) / open * 100 的平均值 |
| **T+0收益空间** | (冲高幅度 + \|下探幅度\|) * 50% |

**分位值说明**:
- P25（25%分位）: 25%的数据低于此值，用于保守估计
- P50（中位数）: 50%的数据低于此值，反映典型值
- P90（90%分位）: 90%的数据低于此值，用于乐观估计/风险控制

### Step 3: 生成报告

严格按照 `references/analysis-template.md` 中的模板输出分析报告。

---

## 输出格式

输出报告**必须**简洁，只包含：

1. **持仓信息** - 持仓数量、成本、现价、盈亏
2. **20日统计数据** - 冲高/下探概率、幅度、P50
3. **60日统计数据** - 冲高/下探概率、幅度、P50
4. **T+0操作建议** - 具体的买卖价格（核心）
   - 高开日: 开盘买入价 → 冲高卖出价
   - 低开日: 开盘卖出价 → 下探买回价
   - 操作仓位

---

## 核心行为规范

1. **数据优先**: 所有统计必须基于实际数据计算
2. **简洁输出**: 只输出有用的操作建议，不要废话
3. **明确价格**: 必须给出具体的买卖价格点位
4. **文件输出**: 分析结果必须保存到文件
5. **概率阈值**: 只有概率 **≥75%** 的事件才值得参考用于制定策略
   - 冲高概率 ≥75%：可作为卖出参考
   - 下探概率 ≥75%：可作为买入参考
   - 概率 <75%：不建议作为操作依据

---

## 文件输出规范

### 输出目录位置: 当前项目根目录/reports/intraday/YYYY-MM-DD/

```
当前目录/reports/intraday/
├── 2026-03-10/
│   ├── 000060.md
│   ├── 600519.md
│   └── ...
├── 2026-03-11/
│   └── ...
```

### 输出规则

1. **目录命名**: 使用当日日期 `YYYY-MM-DD` 格式
2. **文件命名**: 使用股票代码 `{stock_code}.md`
3. **目录检查**: 如果目录已存在则不创建
4. **文件覆盖**: 如果文件已存在则覆盖

### Python 代码示例

```python
from datetime import datetime
from pathlib import Path

def save_report(stock_code: str, content: str):
    """保存分析报告到文件"""
    # 构建目录路径
    today = datetime.now().strftime('%Y-%m-%d')
    output_dir = Path('reports/intraday') / today

    # 创建目录（如果不存在）
    output_dir.mkdir(parents=True, exist_ok=True)

    # 保存文件
    output_file = output_dir / f'{stock_code}.md'
    output_file.write_text(content, encoding='utf-8')

    print(f"报告已保存: {output_file}")
```

---

> **重要**: 开始分析前必须读取 `references/analysis-template.md` 了解输出格式和策略建议规则。
