# 申万行业成分股API使用文档

## API端点

### 创建任务

**POST** `/api/tasks/create`

创建一个异步任务来获取申万行业成分股数据。

#### 请求体

```json
{
  "task_type": "sw_industry",
  "params": {
    "src": "SW2021",    // 可选: 'SW2021'(默认) 或 'SW2014'
    "force": false      // 可选: 是否强制更新（删除旧数据）
  }
}
```

#### 响应

成功（201）:
```json
{
  "success": true,
  "task_id": "abc123-def456-..."
}
```

失败（409 - 已有任务运行）:
```json
{
  "success": false,
  "error": "无法创建新任务：系统中有运行中任务...",
  "error_type": "task_exists",
  "existing_task": {
    "task_id": "...",
    "task_type": "...",
    "status": "running"
  }
}
```

#### 使用示例

```bash
# 使用curl
curl -X POST http://localhost:5555/api/tasks/create \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "sw_industry",
    "params": {"src": "SW2021", "force": false}
  }'

# 使用Python
import requests

response = requests.post('http://localhost:5555/api/tasks/create', json={
    'task_type': 'sw_industry',
    'params': {'src': 'SW2021', 'force': False}
})

task_id = response.json()['task_id']
print(f"任务已创建: {task_id}")
```

### 查询任务状态

**GET** `/api/tasks/<task_id>`

查询任务的执行状态和进度。

#### 响应

```json
{
  "success": true,
  "task": {
    "task_id": "abc123...",
    "task_type": "sw_industry",
    "status": "running",           // pending/running/completed/failed/stopped
    "progress": 45,                // 0-100
    "message": "正在获取行业成分股...",
    "result": {
      "classify_count": 511,
      "members_count": 15000,
      "total_indices": 511,
      "failed_indices": []
    },
    "created_at": "2024-01-01 10:00:00",
    "updated_at": "2024-01-01 10:05:00"
  }
}
```

#### Python示例

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

# 轮询任务状态
while True:
    status_response = requests.get(f'http://localhost:5555/api/tasks/{task_id}')
    task = status_response.json()['task']

    print(f"状态: {task['status']}, 进度: {task['progress']}%, 消息: {task['message']}")

    if task['status'] in ['completed', 'failed', 'stopped']:
        if task['status'] == 'completed':
            print(f"任务完成! 结果: {task['result']}")
        break

    time.sleep(5)
```

### 取消/停止任务

**POST** `/api/tasks/<task_id>/stop`

```bash
curl -X POST http://localhost:5555/api/tasks/<task_id>/stop
```

### 删除任务

**DELETE** `/api/tasks/<task_id>`

```bash
curl -X DELETE http://localhost:5555/api/tasks/<task_id>
```

## 任务类型说明

### 1. sw_industry - 获取申万行业成分股

获取所有申万行业的分类和成分股数据。

**参数:**
- `src`: 行业分类版本
  - `SW2021`: 申万2021版本（511个行业，默认）
  - `SW2014`: 申万2014版本（约300个行业）
- `force`: 是否强制更新
  - `false`: 保留已有数据（默认）
  - `true`: 删除旧数据重新获取

**结果:**
```json
{
  "classify_count": 511,      // 行业分类数量
  "members_count": 15000,     // 成分股总数量
  "total_indices": 511,       // 处理的行业总数
  "failed_indices": []        // 失败的行业代码列表
}
```

### 2. update_favorites - 更新收藏列表

更新指定股票列表的行情数据。

**参数:**
```json
{
  "stocks": ["000001", "600000", "600382"]
}
```

### 3. update_all_stocks - 更新全部A股

更新所有A股的行情数据。

**参数:**
```json
{
  "mode": "incremental",    // "incremental"（增量）或 "full"（全量）
  "stock_range": "all"      // "all"（全部）
}
```

## 完整示例

### Flask应用启动

```bash
# 终端1: 启动Flask应用
cd /path/to/mytrader
source .venv/bin/activate
python web/app.py
```

### Python脚本示例

```python
#!/usr/bin/env python3
"""
获取申万行业成分股并监控任务状态
"""
import requests
import time

API_BASE = 'http://localhost:5555'

def get_sw_industry():
    """获取申万行业成分股"""

    # 1. 创建任务
    print("创建任务...")
    response = requests.post(f'{API_BASE}/api/tasks/create', json={
        'task_type': 'sw_industry',
        'params': {
            'src': 'SW2021',
            'force': False
        }
    })

    if not response.json().get('success'):
        print(f"创建任务失败: {response.json()}")
        return

    task_id = response.json()['task_id']
    print(f"任务已创建: {task_id}")

    # 2. 监控任务状态
    print("\n开始监控任务状态...")
    while True:
        status_resp = requests.get(f'{API_BASE}/api/tasks/{task_id}')
        task_data = status_resp.json()['task']

        status = task_data['status']
        progress = task_data.get('progress', 0)
        message = task_data.get('message', '')

        # 显示进度
        print(f"[{status.upper()}] {progress}% - {message}")

        # 检查是否完成
        if status in ['completed', 'failed', 'stopped']:
            print(f"\n任务{status}!")

            if status == 'completed' and task_data.get('result'):
                result = task_data['result']
                print(f"\n结果统计:")
                print(f"  行业分类: {result['classify_count']} 条")
                print(f"  成分股: {result['members_count']} 条")
                print(f"  失败: {len(result['failed_indices'])} 个")

                if result['failed_indices']:
                    print(f"  失败行业: {result['failed_indices'][:5]}...")

            break

        time.sleep(3)  # 每3秒检查一次

    return task_id

if __name__ == '__main__':
    get_sw_industry()
```

### JavaScript/Node.js示例

```javascript
const axios = require('axios');

const API_BASE = 'http://localhost:5555';

async function getSWIndustry() {
  // 创建任务
  const createResp = await axios.post(`${API_BASE}/api/tasks/create`, {
    task_type: 'sw_industry',
    params: {
      src: 'SW2021',
      force: false
    }
  });

  const taskId = createResp.data.task_id;
  console.log(`任务已创建: ${taskId}`);

  // 监控任务状态
  while (true) {
    const statusResp = await axios.get(`${API_BASE}/api/tasks/${taskId}`);
    const task = statusResp.data.task;

    console.log(`[${task.status.toUpperCase()}] ${task.progress}% - ${task.message}`);

    if (['completed', 'failed', 'stopped'].includes(task.status)) {
      if (task.status === 'completed') {
        console.log('任务完成!', task.result);
      }
      break;
    }

    await new Promise(resolve => setTimeout(resolve, 3000));
  }
}

getSWIndustry().catch(console.error);
```

## 查询申万行业数据

获取完成后，可以通过以下SQL查询数据：

```sql
-- 查询所有行业分类
SELECT * FROM sw_classify WHERE src='SW2021' ORDER BY industry_code;

-- 查询某个行业的成分股
SELECT m.index_code, c.industry_name, m.ts_code, m.name
FROM sw_members m
JOIN sw_classify c ON m.index_code = c.index_code
WHERE m.index_code = '801010.SI'
ORDER BY m.ts_code;

-- 查询某只股票所属的行业
SELECT m.index_code, c.industry_name, c.level
FROM sw_members m
JOIN sw_classify c ON m.index_code = c.index_code
WHERE m.ts_code = '600382.SH' AND m.is_new = 'Y'
ORDER BY c.level;

-- 统计每个行业的成分股数量
SELECT c.index_code, c.industry_name, COUNT(*) as member_count
FROM sw_classify c
LEFT JOIN sw_members m ON c.index_code = m.index_code
WHERE c.src='SW2021'
GROUP BY c.index_code, c.industry_name
ORDER BY member_count DESC;
```

## 注意事项

1. **API限流**: Tushare API有频率限制（每分钟50次），获取511个行业可能需要10-15分钟
2. **任务冲突**: 同时只能运行一个任务，需要等待当前任务完成
3. **数据量**: 申万2021版本包含511个行业，约15000+条成分股记录
4. **权限要求**: 需要Tushare Pro账户，建议2000+积分以获得完整数据访问权限

## 错误处理

### 409 Conflict - 已有任务运行

```json
{
  "success": false,
  "error": "无法创建新任务：系统中有运行中任务...",
  "error_type": "task_exists",
  "existing_task": {
    "task_id": "...",
    "status": "running"
  }
}
```

**解决方案**: 等待现有任务完成，或先停止该任务

### 400 Bad Request - 参数错误

```json
{
  "success": false,
  "error": "缺少task_type参数"
}
```

**解决方案**: 检查请求体，确保包含 `task_type` 字段

## Swagger API文档

启动应用后，访问 http://localhost:5555/api/docs 查看交互式API文档。
