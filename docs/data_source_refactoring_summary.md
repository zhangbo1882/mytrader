# 数据源对比和重构总结

## 📊 测试结果

### 1. 数据源字段对比

**AKShare 特点：**
- ✅ 字段名使用中文，更直观
- ✅ 提供振幅、换手率等额外字段
- ✅ 数据单位：成交量（股）、成交额（元）
- ✅ 日期格式：YYYY-MM-DD
- ✅ 股票代码：6位纯数字
- ✅ 免费无限制

**Tushare 特点：**
- ✅ 字段名使用英文，更专业
- ✅ 提供昨收价（pre_close）等字段
- ⚠️ 数据单位：成交量（手）、成交额（千元）
- ⚠️ 日期格式：YYYYMMDD
- ⚠️ 股票代码：带交易所后缀（如 000001.SZ）
- ❌ 需要积分，有频率限制

### 2. 字段映射关系

| 含义 | AKShare | Tushare | 标准字段 |
|-----|---------|---------|---------|
| 日期 | 日期 | trade_date | datetime |
| 代码 | 股票代码 | ts_code | symbol |
| 开盘 | 开盘 | open | open |
| 收盘 | 收盘 | close | close |
| 最高 | 最高 | high | high |
| 最低 | 最低 | low | low |
| 成交量 | 成交量 | vol | volume |
| 成交额 | 成交额 | amount | turnover |
| 涨跌幅 | 涨跌幅 | pct_chg | pct_chg |

### 3. 主要差异

1. **单位差异**
   - 成交量：AKShare（股）vs Tushare（手，需×100）
   - 成交额：AKShare（元）vs Tushare（千元，需×1000）

2. **格式差异**
   - 日期：AKShare（YYYY-MM-DD）vs Tushare（YYYYMMDD）
   - 代码：AKShare（000001）vs Tushare（000001.SZ）

3. **字段差异**
   - AKShare 独有：振幅、换手率
   - Tushare 独有：昨收价（pre_close）

## 🔧 重构完成的工作

### 1. 数据库结构优化

**新增 `stock_names` 表：**
```sql
CREATE TABLE stock_names (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    updated_at TEXT
);
```

- 存储 5473 支 A 股股票的代码和名称
- 从 akshare 免费接口获取
- 支持快速搜索和查询

### 2. 代码结构优化

**移动文件：**
- ❌ 删除：`web/utils/stock_lookup.py`
- ❌ 删除：`src/utils/stock_names.py`（手动维护的映射文件）
- ✅ 新建：`src/utils/stock_lookup.py`（从数据库读取）
- ✅ 新建：`src/utils/data_standardize.py`（数据标准化工具）
- ✅ 新建：`scripts/init_stock_names.py`（初始化股票名称）

### 3. 数据流程优化

**之前：**
```
用户搜索 → 调用 akshare API → 获取股票名称 → 显示
（每次都调用API，慢且有频率限制）
```

**现在：**
```
用户搜索 → 从数据库读取 → 显示股票名称
（一次初始化，永久快速使用）
```

### 4. 数据源策略

**优先级：**
1. **AKShare**（主要数据源）
   - 免费无限制
   - 字段更丰富
   - 数据更新及时

2. **Tushare**（备用数据源）
   - 作为 AKShare 不可用时的备选
   - 用于数据验证和对比
   - 需要积分权限

## 📁 新增的文件

### 1. 测试脚本
- `scripts/test_data_sources.py` - 对比测试两个数据源
- `scripts/init_stock_names.py` - 初始化股票名称到数据库

### 2. 工具模块
- `src/utils/stock_lookup.py` - 从数据库查询股票名称
- `src/utils/data_standardize.py` - 数据标准化工具

### 3. 文档
- `docs/data_source_fields_comparison.md` - 字段对比详细文档
- `docs/data_source_refactoring_summary.md` - 本总结文档

## 🎯 使用建议

### 1. 初始化股票名称

```bash
# 首次使用时运行
python scripts/init_stock_names.py
```

### 2. 数据下载策略

```python
# 优先使用 AKShare
try:
    data = ak.stock_zh_a_hist(symbol=code, ...)
except:
    # AKShare 失败时使用 Tushare
    data = pro.daily(ts_code=ts_code, ...)
```

### 3. 数据标准化

```python
from src.utils.data_standardize import (
    standardize_akshare_data,
    standardize_tushare_data,
    merge_data_sources
)

# 标准化 AKShare 数据
ak_data_std = standardize_akshare_data(ak_data)

# 标准化 Tushare 数据
ts_data_std = standardize_tushare_data(ts_data)

# 合并数据
merged_data = merge_data_sources(ak_data_std, ts_data_std)
```

## ✅ 验证测试

运行测试脚本查看详细对比：

```bash
python scripts/test_data_sources.py
```

输出包括：
- 字段列表对比
- 数据示例
- 字段映射关系
- 数据类型检查
- 数据质量验证

## 🚀 下一步建议

1. **数据下载优化**
   - 实现优先使用 AKShare 的下载逻辑
   - 添加自动重试和降级机制

2. **数据验证**
   - 定期对比两个数据源的一致性
   - 建立数据质量监控

3. **性能优化**
   - 使用连接池管理数据库连接
   - 实现批量查询和缓存

4. **错误处理**
   - 完善异常处理机制
   - 添加日志记录

## 📊 数据统计

- **数据库股票数量**: 755 支
- **股票名称表**: 5473 支（覆盖所有 A 股）
- **数据记录数**: 381,489 条
- **日期范围**: 2020-01-02 至 2026-01-26
- **Web 搜索**: 支持搜索所有 5473 支股票

## 🎉 总结

通过这次重构：
1. ✅ 解耦了数据源和展示层
2. ✅ 建立了标准化的数据格式
3. ✅ 优化了股票名称的存储和查询
4. ✅ 提供了完善的数据对比工具
5. ✅ 为后续扩展打下良好基础

系统现在可以：
- 免费从 akshare 获取所有股票名称
- 快速从数据库查询股票信息
- 灵活切换不同的数据源
- 统一不同数据源的数据格式
