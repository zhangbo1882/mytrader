#!/usr/bin/env python3
"""
定时任务功能测试脚本

测试内容：
1. 后端 API 测试
   - 获取定时任务列表
   - 创建定时任务
   - 暂停/恢复定时任务
   - 删除定时任务

2. 前端功能验证
   - 定时任务列表显示
   - 添加定时任务对话框
   - 暂停/恢复/删除操作

3. 实际执行验证
   - 短间隔定时任务创建
   - 等待任务执行
   - 验证任务是否创建了更新任务
"""

import requests
import time
import json
from datetime import datetime, timedelta

BASE_URL = "http://localhost:5001"


def test_get_scheduled_jobs():
    """测试获取定时任务列表"""
    print("\n=== 测试1: 获取定时任务列表 ===")
    response = requests.get(f"{BASE_URL}/api/schedule/jobs")
    data = response.json()

    if data.get('success'):
        jobs = data.get('jobs', [])
        print(f"✓ 成功获取定时任务列表: {len(jobs)} 个任务")
        for job in jobs:
            print(f"  - {job.get('name')} (ID: {job.get('id')})")
            print(f"    下次运行: {job.get('next_run_time')}")
            print(f"    触发器: {job.get('trigger')}")
        return True
    else:
        print(f"✗ 失败: {data.get('error')}")
        return False


def test_create_scheduled_job():
    """测试创建定时任务"""
    print("\n=== 测试2: 创建定时任务 ===")

    # 使用每分钟执行一次的 cron 表达式进行测试
    # 格式: 分 时 日 月 周
    # "*/2 * * * *" 表示每2分钟执行一次
    job_data = {
        "name": f"测试定时任务-{datetime.now().strftime('%H%M%S')}",
        "cron_expression": "*/2 * * * *",  # 每2分钟执行一次
        "mode": "incremental",
        "stock_range": "custom",
        "custom_stocks": ["600382", "600711"]  # 测试用2只股票
    }

    print(f"创建任务: {job_data['name']}")
    print(f"Cron表达式: {job_data['cron_expression']} (每2分钟执行)")
    print(f"股票列表: {job_data['custom_stocks']}")

    response = requests.post(
        f"{BASE_URL}/api/schedule/jobs",
        json=job_data,
        headers={"Content-Type": "application/json"}
    )
    data = response.json()

    if data.get('success'):
        print(f"✓ 成功创建定时任务")
        return True
    else:
        print(f"✗ 失败: {data.get('error')}")
        return False


def test_pause_resume_job():
    """测试暂停和恢复定时任务"""
    print("\n=== 测试3: 暂停/恢复定时任务 ===")

    # 先获取任务列表
    response = requests.get(f"{BASE_URL}/api/schedule/jobs")
    data = response.json()

    if not data.get('success') or not data.get('jobs'):
        print("✗ 没有可用的定时任务")
        return False

    jobs = data.get('jobs')
    job_id = jobs[0]['id']
    job_name = jobs[0]['name']

    print(f"测试任务: {job_name} (ID: {job_id})")

    # 测试暂停
    print("  1. 暂停任务...")
    response = requests.post(f"{BASE_URL}/api/schedule/jobs/{job_id}/pause")
    data = response.json()

    if data.get('success'):
        print(f"  ✓ 暂停成功")
    else:
        print(f"  ✗ 暂停失败: {data.get('error')}")
        return False

    # 等待一下
    time.sleep(1)

    # 测试恢复
    print("  2. 恢复任务...")
    response = requests.post(f"{BASE_URL}/api/schedule/jobs/{job_id}/resume")
    data = response.json()

    if data.get('success'):
        print(f"  ✓ 恢复成功")
        return True
    else:
        print(f"  ✗ 恢复失败: {data.get('error')}")
        return False


def test_delete_job():
    """测试删除定时任务"""
    print("\n=== 测试4: 删除定时任务 ===")

    # 先获取任务列表
    response = requests.get(f"{BASE_URL}/api/schedule/jobs")
    data = response.json()

    if not data.get('success') or not data.get('jobs'):
        print("✗ 没有可用的定时任务")
        return False

    jobs = data.get('jobs')
    job_id = jobs[0]['id']
    job_name = jobs[0]['name']

    print(f"删除任务: {job_name} (ID: {job_id})")

    response = requests.delete(f"{BASE_URL}/api/schedule/jobs/{job_id}")
    data = response.json()

    if data.get('success'):
        print(f"✓ 删除成功")
        return True
    else:
        print(f"✗ 失败: {data.get('error')}")
        return False


def test_job_execution():
    """测试定时任务实际执行"""
    print("\n=== 测试5: 验证定时任务实际执行 ===")

    # 创建一个每分钟执行的任务
    job_data = {
        "name": f"执行测试-{datetime.now().strftime('%H%M%S')}",
        "cron_expression": "* * * * *",  # 每分钟执行
        "mode": "incremental",
        "stock_range": "custom",
        "custom_stocks": ["600382"]
    }

    print(f"创建测试任务 (每分钟执行)...")
    response = requests.post(
        f"{BASE_URL}/api/schedule/jobs",
        json=job_data,
        headers={"Content-Type": "application/json"}
    )
    data = response.json()

    if not data.get('success'):
        print(f"✗ 创建任务失败: {data.get('error')}")
        return False

    print("✓ 任务创建成功，等待执行...")

    # 获取当前任务数
    before_response = requests.get(f"{BASE_URL}/api/tasks")
    before_data = before_response.json()
    before_count = len(before_data.get('tasks', []))
    print(f"  当前任务数: {before_count}")

    print("  等待 70 秒...")
    for i in range(70):
        print(f"\r  进度: [{'=' * (i // 7)}] {i}%", end='', flush=True)
        time.sleep(1)

    print()  # 换行

    # 检查是否有新任务创建
    after_response = requests.get(f"{BASE_URL}/api/tasks")
    after_data = after_response.json()
    after_count = len(after_data.get('tasks', []))

    print(f"  执行后任务数: {after_count}")

    if after_count > before_count:
        print(f"✓ 定时任务成功执行，创建了 {after_count - before_count} 个新任务")

        # 显示最新任务详情
        tasks = after_data.get('tasks', [])
        if tasks:
            latest_task = tasks[0]
            print(f"\n最新任务:")
            print(f"  ID: {latest_task.get('task_id', '')[:8]}...")
            print(f"  状态: {latest_task.get('status')}")
            print(f"  消息: {latest_task.get('message')}")
            print(f"  创建时间: {latest_task.get('created_at')}")

        return True
    else:
        print(f"✗ 定时任务未执行")
        return False


def main():
    """主测试函数"""
    print("=" * 60)
    print("定时任务功能测试")
    print("=" * 60)

    results = []

    # 测试1: 获取定时任务列表
    results.append(("获取定时任务列表", test_get_scheduled_jobs()))

    # 测试2: 创建定时任务
    results.append(("创建定时任务", test_create_scheduled_job()))

    # 测试3: 暂停/恢复任务
    results.append(("暂停/恢复任务", test_pause_resume_job()))

    # 测试4: 删除任务
    results.append(("删除定时任务", test_delete_job()))

    # 测试5: 验证实际执行
    print("\n" + "=" * 60)
    print("注意: 测试5需要等待70秒验证实际执行")
    choice = input("是否执行测试5? (y/n): ").strip().lower()
    if choice == 'y':
        results.append(("验证实际执行", test_job_execution()))
    else:
        print("跳过测试5")

    # 打印测试结果
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)

    for test_name, result in results:
        status = "✓ 通过" if result else "✗ 失败"
        print(f"{test_name}: {status}")

    passed = sum(1 for _, r in results if r)
    total = len(results)
    print(f"\n总计: {passed}/{total} 测试通过")

    # 前端测试说明
    print("\n" + "=" * 60)
    print("前端测试步骤")
    print("=" * 60)
    print("""
请在浏览器中打开 http://localhost:5001 并验证以下功能：

1. 定时任务列表显示
   - 查看"更新管理"页面的"定时任务列表"卡片
   - 验证任务列表正确显示（如果有任务的话）

2. 添加定时任务
   - 点击"添加任务"按钮
   - 填写任务信息：
     * 任务名称: 测试任务
     * Cron表达式: */5 * * * * (每5分钟)
     * 更新模式: 增量更新
     * 股票范围: 收藏列表
   - 点击"保存"
   - 验证任务出现在列表中

3. 暂停/恢复任务
   - 点击任务的"暂停"按钮
   - 验证任务状态变为暂停
   - 点击"恢复"按钮
   - 验证任务状态恢复

4. 删除任务
   - 点击任务的"删除"按钮
   - 确认删除
   - 验证任务从列表中消失

5. 查看任务详情
   - 验证"下次执行时间"正确显示
   - 验证"Cron表达式"正确显示
    """)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n测试被中断")
    except Exception as e:
        print(f"\n\n测试出错: {e}")
        import traceback
        traceback.print_exc()
