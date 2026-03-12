#!/usr/bin/env python3
"""
创建更新问题股票财务数据的任务
"""

import requests
import json

# 问题股票列表（21只）
stocks = [
    "001220",
    "002687",
    "002841",
    "300143",
    "300209",
    "300607",
    "300972",
    "600007",
    "600167",
    "600310",
    "600756",
    "600793",
    "600794",
    "600816",
    "601112",
    "601696",
    "601766",
    "603352",
    "603392",
    "688759",
    "688796",
]

# 创建任务
response = requests.post(
    "http://127.0.0.1:5001/api/tasks/create",
    json={
        "task_type": "update_financial_reports",
        "stock_range": "custom",
        "custom_stocks": stocks,
        "include_reports": True,
        "include_indicators": True,
    },
    headers={"Content-Type": "application/json"},
)

print("创建任务响应:")
print(json.dumps(response.json(), indent=2, ensure_ascii=False))

# 获取任务ID
if response.status_code == 201:
    task_id = response.json().get("task_id")
    print(f"\n任务ID: {task_id}")

    # 等待几秒让任务开始执行
    import time

    time.sleep(3)

    # 检查任务详情
    task_response = requests.get(f"http://127.0.0.1:5001/api/tasks/{task_id}")
    print("\n任务详情:")
    print(json.dumps(task_response.json(), indent=2, ensure_ascii=False))
else:
    print(f"创建任务失败: {response.status_code}")
