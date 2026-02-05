# 申万行业成分股获取功能

## 功能概述

通过后端API异步获取申万行业分类及其成分股数据，支持申万2014和2021两个版本。

## 快速开始

### 1. 获取单个行业的成分股（测试用）

```bash
# 激活虚拟环境
source .venv/bin/activate

# 获取农林牧渔行业的成分股
python scripts/init_sw_industry.py --index-code 801010.SI
```

### 2. 获取所有行业成分股（完整数据）

**方法1: 使用Python脚本**
```bash
# 直接运行（会获取全部511个行业，耗时10-15分钟）
python scripts/init_sw_industry.py
```

**方法2: 使用API（推荐）**
```bash
# 启动Flask应用
python web/app.py

# 在另一个终端运行测试脚本
python scripts/test_sw_industry_api.py
```

## API使用

### 创建任务

```bash
curl -X POST http://localhost:5555/api/tasks/create \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "sw_industry",
    "params": {
      "src": "SW2021",
      "force": false
    }
  }'
```

### 查询任务状态

```bash
curl http://localhost:5555/api/tasks/<task_id>
```

### Python示例

```python
import requests
import time

# 创建任务
response = requests.post('http://localhost:5555/api/tasks/create', json={
    'task_type': 'sw_industry',
    'params': {'src': 'SW2021'}
})

task_id = response.json()['task_id']
print(f"任务已创建: {task_id}")

# 监控状态
while True:
    status = requests.get(f'http://localhost:5555/api/tasks/{task_id}').json()['task']
    print(f"状态: {status['status']}, 进度: {status['progress']}%")

    if status['status'] in ['completed', 'failed', 'stopped']:
        break
    time.sleep(5)
```

## 数据库表结构

### sw_classify (行业分类表)

| 字段 | 类型 | 说明 |
|------|------|------|
| index_code | TEXT | 行业代码（主键），如801010.SI |
| industry_name | TEXT | 行业名称，如"农林牧渔" |
| parent_code | TEXT | 父级代码 |
| level | TEXT | 级别（L1一级，L2二级，L3三级） |
| industry_code | TEXT | 行业编码 |
| is_pub | TEXT | 是否发布 |
| src | TEXT | 数据源（SW2014/SW2021） |
| updated_at | TEXT | 更新时间 |

### sw_members (成分股表)

| 字段 | 类型 | 说明 |
|------|------|------|
| index_code | TEXT | 行业代码 |
| ts_code | TEXT | 股票代码 |
| name | TEXT | 股票名称 |
| in_date | TEXT | 纳入日期 |
| out_date | TEXT | 剔除日期（NULL表示仍在成分中） |
| is_new | TEXT | 是否最新成分 |

主键: (index_code, ts_code)

## 常用查询

```sql
-- 查询所有行业分类
SELECT * FROM sw_classify WHERE src='SW2021' ORDER BY industry_code;

-- 查询某个行业的成分股
SELECT m.ts_code, m.name, m.in_date
FROM sw_members m
WHERE m.index_code = '801010.SI'
ORDER BY m.ts_code;

-- 查询某只股票所属的行业
SELECT c.index_code, c.industry_name, c.level
FROM sw_members m
JOIN sw_classify c ON m.index_code = c.index_code
WHERE m.ts_code = '600382.SH' AND m.is_new = 'Y'
ORDER BY c.level;

-- 统计每个行业的成分股数量
SELECT
    c.index_code,
    c.industry_name,
    c.level,
    COUNT(m.ts_code) as member_count
FROM sw_classify c
LEFT JOIN sw_members m ON c.index_code = m.index_code AND m.is_new = 'Y'
WHERE c.src='SW2021' AND c.level = 'L1'
GROUP BY c.index_code, c.industry_name, c.level
ORDER BY member_count DESC;

-- 查找成分股最多的行业
SELECT
    c.index_code,
    c.industry_name,
    COUNT(m.ts_code) as count
FROM sw_classify c
JOIN sw_members m ON c.index_code = m.index_code
WHERE c.src='SW2021' AND m.is_new = 'Y'
GROUP BY c.index_code, c.industry_name
ORDER BY count DESC
LIMIT 10;
```

## 常用行业代码

### 申万2021一级行业（部分）

| 代码 | 名称 | 成分股数量 |
|------|------|-----------|
| 801010.SI | 农林牧渔 | ~30 |
| 801030.SI | 基础化工 | ~200 |
| 801040.SI | 钢铁 | ~40 |
| 801050.SI | 有色金属 | ~80 |
| 801080.SI | 电子 | ~300 |
| 801120.SI | 食品饮料 | ~100 |
| 801150.SI | 医药生物 | ~300 |
| 801760.SI | 传媒 | ~100 |
| 801770.SI | 通信 | ~100 |
| 801780.SI | 银行 | ~40 |
| 801790.SI | 非银金融 | ~80 |
| 801750.SI | 计算机 | ~250 |

完整列表请查看:
```sql
SELECT index_code, industry_name, level
FROM sw_classify
WHERE src='SW2021' AND level='L1'
ORDER BY industry_code;
```

## 注意事项

1. **API频率限制**: Tushare限制每分钟50次调用
2. **耗时**: 获取全部511个行业需要10-15分钟
3. **权限**: 建议Tushare账户2000+积分
4. **数据量**: 约15000+条成分股记录
5. **任务冲突**: 同时只能运行一个任务

## 故障排查

### 问题1: 创建任务失败，提示"已有任务运行"

**解决**: 等待现有任务完成，或先停止该任务
```bash
# 停止现有任务
curl -X POST http://localhost:5555/api/tasks/<task_id>/stop
```

### 问题2: API返回"无权限"

**解决**: 升级Tushare积分到2000+
   访问: https://tushare.pro

### 问题3: 数据库中没有数据

**检查**:
```bash
# 检查任务状态
curl http://localhost:5555/api/tasks/<task_id>

# 检查数据库
sqlite3 data/tushare_data.db "SELECT COUNT(*) FROM sw_members;"
```

## 文件说明

- `scripts/init_sw_industry.py` - 命令行脚本
- `scripts/test_sw_industry_api.py` - API测试脚本
- `docs/sw_industry_api.md` - 详细API文档
- `src/data_sources/tushare.py` - TushareDB类（包含save_all_sw_industry方法）

## 更新日志

### 2024-02-04
- ✅ 添加API端点 `/api/tasks/create`
- ✅ 支持申万2021和2014两个版本
- ✅ 添加任务进度监控
- ✅ 修复API返回数据缺少index_code的问题
- ✅ 添加详细文档和测试脚本
