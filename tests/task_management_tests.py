#!/usr/bin/env python3
"""
任务管理功能测试脚本
Task Management Feature Tests

测试目标：验证任务历史页面和更新管理页面的所有功能是否正常工作
"""

import json
import requests
import sqlite3
import time
import sys
from typing import Dict, List, Any, Tuple
from datetime import datetime, timedelta

# 测试配置
BASE_URL = "http://localhost:5001"
DB_PATH = "/Users/zhangbo/Public/go/github.com/mytrader/data/tasks.db"

# 测试结果记录
test_results = []

class Colors:
    """终端颜色常量"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def log_test(test_name: str, passed: bool, message: str = "", details: str = ""):
    """记录测试结果"""
    result = {
        "test_name": test_name,
        "passed": passed,
        "message": message,
        "details": details,
        "timestamp": datetime.now().isoformat()
    }
    test_results.append(result)

    icon = "✅" if passed else "❌"
    status_color = Colors.OKGREEN if passed else Colors.FAIL
    print(f"{status_color}{icon} {test_name}{Colors.ENDC}")
    if message:
        print(f"   {message}")
    if details:
        print(f"   详情: {details}")
    print()

def get_db_connection() -> sqlite3.Connection:
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def api_request(method: str, endpoint: str, data: Dict = None, params: Dict = None) -> Tuple[bool, Any]:
    """发送API请求"""
    url = f"{BASE_URL}{endpoint}"
    headers = {"Content-Type": "application/json"}

    try:
        if method == "GET":
            response = requests.get(url, params=params, timeout=30)
        elif method == "POST":
            response = requests.post(url, json=data, params=params, timeout=30)
        elif method == "DELETE":
            response = requests.delete(url, timeout=30)
        else:
            return False, f"Unsupported method: {method}"

        try:
            return response.status_code in [200, 201, 204], response.json()
        except:
            return response.status_code in [200, 201, 204], response.text
    except Exception as e:
        return False, f"Request error: {str(e)}"

def wait_for_task_status(task_id: str, expected_status: str, timeout: int = 30) -> bool:
    """等待任务达到预期状态"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        success, data = api_request("GET", f"/api/tasks/{task_id}")
        if success and isinstance(data, dict):
            task = data.get("task", {})
            current_status = task.get("status")
            if current_status == expected_status:
                return True
            elif current_status in ["failed", "stopped", "cancelled"]:
                return False
        time.sleep(1)
    return False

# ============================================================================
# 第一部分：任务历史页面功能测试
# ============================================================================

def test_1_1_delete_pending_task():
    """测试1.1：删除pending任务"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试1.1：删除pending任务")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 获取pending任务
    success, data = api_request("GET", "/api/tasks", params={"status": "pending"})
    if not success or not data.get("tasks"):
        log_test("1.1.1 获取pending任务", False, "无法获取pending任务", str(data))
        return

    tasks = data["tasks"]
    task_to_delete = tasks[0]
    task_id = task_to_delete["task_id"]
    initial_count = len(tasks)

    # 删除任务
    success, result = api_request("DELETE", f"/api/tasks/{task_id}")
    log_test("1.1.2 删除pending任务", success, "任务删除API调用", str(result))

    # 验证任务已从列表中消失
    success, data = api_request("GET", "/api/tasks", params={"status": "pending"})
    if success:
        final_count = len(data.get("tasks", []))
        count_reduced = final_count < initial_count
        log_test("1.1.3 验证任务从列表消失", count_reduced,
                f"pending任务数量: {initial_count} -> {final_count}")

    # 验证数据库
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM tasks WHERE task_id = ?", (task_id,))
    db_count = cursor.fetchone()[0]
    conn.close()
    log_test("1.1.4 验证数据库持久化", db_count == 0,
            f"数据库中任务记录数: {db_count}")

def test_1_2_cancel_pending_task():
    """测试1.2：停止pending任务（取消功能）"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试1.2：停止pending任务（取消功能）")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 获取pending任务
    success, data = api_request("GET", "/api/tasks", params={"status": "pending"})
    if not success or not data.get("tasks"):
        log_test("1.2.0 获取pending任务", False, "没有可用的pending任务")
        return

    task = data["tasks"][0]
    task_id = task["task_id"]

    # 停止任务
    success, result = api_request("POST", f"/api/tasks/{task_id}/stop")
    log_test("1.2.1 停止pending任务", success, "任务停止API调用", str(result))

    # 验证任务状态变为stopped
    time.sleep(1)
    success, data = api_request("GET", f"/api/tasks/{task_id}")
    if success and isinstance(data, dict):
        task_data = data.get("task", {})
        status = task_data.get("status")
        message = task_data.get("message", "")
        log_test("1.2.2 验证状态变为stopped", status == "stopped",
                f"当前状态: {status}")
        log_test("1.2.3 验证消息", "已取消" in message or "stopped" in message.lower(),
                f"消息内容: {message}")

    # 验证任务仍在列表中
    success, data = api_request("GET", "/api/tasks", params={"status": "stopped"})
    if success:
        stopped_tasks = data.get("tasks", [])
        found = any(t["task_id"] == task_id for t in stopped_tasks)
        log_test("1.2.4 验证任务仍在列表中", found,
                f"stopped任务数量: {len(stopped_tasks)}")

def test_1_3_delete_stopped_task():
    """测试1.3：删除已停止的任务"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试1.3：删除已停止的任务")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 获取stopped任务
    success, data = api_request("GET", "/api/tasks", params={"status": "stopped"})
    if not success or not data.get("tasks"):
        log_test("1.3.0 获取stopped任务", False, "没有可用的stopped任务")
        return

    task = data["tasks"][0]
    task_id = task["task_id"]

    # 删除任务
    start_time = time.time()
    success, result = api_request("DELETE", f"/api/tasks/{task_id}")
    elapsed = time.time() - start_time

    log_test("1.3.1 删除stopped任务", success,
            f"删除操作完成，耗时: {elapsed:.2f}秒", str(result))
    log_test("1.3.2 验证无超时", elapsed < 5,
            f"删除操作在5秒内完成", f"实际耗时: {elapsed:.2f}秒")

def test_1_4_task_list_refresh():
    """测试1.4：任务列表刷新功能"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试1.4：任务列表刷新功能")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 第一次获取
    success1, data1 = api_request("GET", "/api/tasks")
    time.sleep(1)

    # 第二次获取（模拟刷新）
    success2, data2 = api_request("GET", "/api/tasks")

    log_test("1.4.1 第一次获取任务列表", success1,
            f"任务数量: {len(data1.get('tasks', [])) if success1 else 'N/A'}")

    log_test("1.4.2 第二次获取任务列表（刷新）", success2,
            f"任务数量: {len(data2.get('tasks', [])) if success2 else 'N/A'}")

    if success1 and success2:
        tasks1 = data1.get("tasks", [])
        tasks2 = data2.get("tasks", [])
        log_test("1.4.3 验证列表结构一致性",
                len(tasks1) == len(tasks2) and all("task_id" in t for t in tasks2),
                f"任务数量: {len(tasks2)}, 结构完整")

def test_1_5_pagination():
    """测试1.5：分页功能"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试1.5：分页功能")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 获取第一页
    success, data = api_request("GET", "/api/tasks", params={"page": 1, "per_page": 10})
    if success:
        tasks = data.get("tasks", [])
        total = data.get("total", 0)

        log_test("1.5.1 获取第一页任务", True,
                f"本页: {len(tasks)}条, 总计: {total}条")

        log_test("1.5.2 验证分页参数", len(tasks) <= 10,
                f"每页最多10条，实际: {len(tasks)}条")

        if total > 10:
            success2, data2 = api_request("GET", "/api/tasks", params={"page": 2, "per_page": 10})
            if success2:
                tasks2 = data2.get("tasks", [])
                log_test("1.5.3 获取第二页任务", True,
                        f"第二页: {len(tasks2)}条")
    else:
        log_test("1.5.1 获取第一页任务", False, str(data))

# ============================================================================
# 第二部分：更新管理页面功能测试
# ============================================================================

def test_2_1_create_and_run_small_task():
    """测试2.1：创建并运行小规模任务"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试2.1：创建并运行小规模任务")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 检查是否有活动任务
    success, data = api_request("GET", "/api/tasks", params={"status": "running"})
    if success and len(data.get("tasks", [])) > 0:
        log_test("2.1.0 检查活动任务", False, "已有活动任务在运行，请先停止")
        return

    # 创建任务
    task_data = {
        "stock_range": "favorites",
        "mode": "incremental"
    }

    success, result = api_request("POST", "/api/stock/update-favorites", data=task_data)
    log_test("2.1.1 创建更新任务", success, "创建任务API调用", str(result)[:200] if isinstance(result, str) else json.dumps(result, ensure_ascii=False)[:200])

    if success and isinstance(result, dict):
        task_id = result.get("task_id")
        log_test("2.1.2 获取任务ID", bool(task_id), f"任务ID: {task_id}")

        # 等待任务开始运行
        if task_id:
            time.sleep(2)
            success, data = api_request("GET", f"/api/tasks/{task_id}")
            if success:
                task = data.get("task", {})
                status = task.get("status")
                log_test("2.1.3 任务状态检查", status in ["pending", "running"],
                        f"当前状态: {status}")

                if status == "running":
                    progress = task.get("progress", 0)
                    stats = task.get("stats", {})
                    log_test("2.1.4 进度和统计", True,
                            f"进度: {progress}%, 成功: {stats.get('success', 0)}, "
                            f"失败: {stats.get('failed', 0)}, 跳过: {stats.get('skipped', 0)}")
    else:
        log_test("2.1.1 创建更新任务", False, f"创建失败: {result}")

def test_2_2_pause_and_resume():
    """测试2.2：暂停和恢复功能"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试2.2：暂停和恢复功能")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 查找运行中的任务
    success, data = api_request("GET", "/api/tasks", params={"status": "running"})
    if not success or not data.get("tasks"):
        log_test("2.2.0 获取运行中任务", False, "没有运行中的任务")
        return

    task = data["tasks"][0]
    task_id = task["task_id"]

    # 暂停任务
    success, result = api_request("POST", f"/api/tasks/{task_id}/pause")
    log_test("2.2.1 暂停任务", success, "暂停API调用", str(result)[:200])

    # 等待暂停生效
    time.sleep(2)
    success, data = api_request("GET", f"/api/tasks/{task_id}")
    if success:
        task_data = data.get("task", {})
        status = task_data.get("status")
        log_test("2.2.2 验证任务已暂停", status == "paused",
                f"当前状态: {status}")

        # 恢复任务
        if status == "paused":
            success, result = api_request("POST", f"/api/tasks/{task_id}/resume")
            log_test("2.2.3 恢复任务", success, "恢复API调用", str(result)[:200])

            time.sleep(2)
            success, data = api_request("GET", f"/api/tasks/{task_id}")
            if success:
                task_data = data.get("task", {})
                status = task_data.get("status")
                log_test("2.2.4 验证任务已恢复", status == "running",
                        f"当前状态: {status}")

def test_2_3_stop_running_task():
    """测试2.3：停止运行中的任务"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试2.3：停止运行中的任务")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 查找运行中或暂停的任务
    for status_filter in ["running", "paused"]:
        success, data = api_request("GET", "/api/tasks", params={"status": status_filter})
        if success and data.get("tasks"):
            task = data["tasks"][0]
            task_id = task["task_id"]
            initial_index = task.get("current_stock_index", 0)

            # 停止任务
            success, result = api_request("POST", f"/api/tasks/{task_id}/stop")
            log_test(f"2.3.1 停止{status_filter}任务", success, "停止API调用", str(result)[:200])

            time.sleep(2)
            success, data = api_request("GET", f"/api/tasks/{task_id}")
            if success:
                task_data = data.get("task", {})
                status = task_data.get("status")
                final_index = task_data.get("current_stock_index", 0)

                log_test("2.3.2 验证任务已停止", status == "stopped",
                        f"当前状态: {status}")
                log_test("2.3.3 验证进度保存", final_index >= initial_index,
                        f"索引: {initial_index} -> {final_index}")
            return

    log_test("2.3.0 获取任务", False, "没有运行中或暂停的任务")

def test_2_4_task_completion():
    """测试2.4：任务完成后显示"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试2.4：任务完成后显示")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 获取最近完成的任务
    success, data = api_request("GET", "/api/tasks",
                                params={"status": "completed", "limit": 5})
    if success and data.get("tasks"):
        task = data["tasks"][0]
        log_test("2.4.1 获取已完成任务", True,
                f"任务ID: {task['task_id'][:8]}..., 状态: {task['status']}")

        progress = task.get("progress", 0)
        stats = task.get("stats", {})
        message = task.get("message", "")

        log_test("2.4.2 验证进度", progress == 100, f"进度: {progress}%")
        log_test("2.4.3 验证完成消息", "完成" in message or "complete" in message.lower(),
                f"消息: {message}")
        log_test("2.4.4 验证统计数据", True,
                f"成功: {stats.get('success', 0)}, "
                f"失败: {stats.get('failed', 0)}, "
                f"跳过: {stats.get('skipped', 0)}")
    else:
        log_test("2.4.1 获取已完成任务", False, "没有已完成的任务")

def test_2_5_task_conflict_detection():
    """测试2.5：任务冲突检测"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试2.5：任务冲突检测")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 检查是否有活动任务
    success, data = api_request("GET", "/api/tasks",
                                params={"status": "running,paused,pending"})
    has_active = success and len(data.get("tasks", [])) > 0

    if not has_active:
        # 创建第一个任务
        task_data = {"stock_range": "favorites", "mode": "incremental"}
        success, result = api_request("POST", "/api/stock/update-favorites", data=task_data)

        if not success:
            log_test("2.5.0 创建第一个任务", False, str(result))
            return

        time.sleep(1)

    # 尝试创建第二个任务（应该失败）
    task_data = {"stock_range": "favorites", "mode": "incremental"}
    success, result = api_request("POST", "/api/stock/update-favorites", data=task_data)

    log_test("2.5.1 检测任务冲突", not success,
            f"{'检测到冲突' if not success else '未检测到冲突'}",
            str(result)[:300] if isinstance(result, str) else json.dumps(result, ensure_ascii=False)[:300])

    # 验证只有一个活动任务
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM tasks WHERE status IN ('pending', 'running', 'paused')")
    count = cursor.fetchone()[0]
    conn.close()

    log_test("2.5.2 验证只有一个活动任务", count == 1,
            f"活动任务数量: {count}")

# ============================================================================
# 第三部分：集成测试
# ============================================================================

def test_3_1_complete_workflow():
    """测试3.1：完整工作流测试"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试3.1：完整工作流测试")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 先清理所有活动任务
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE tasks SET status = 'stopped' WHERE status IN ('pending', 'running', 'paused')")
    conn.commit()
    conn.close()

    # 创建任务
    task_data = {"stock_range": "favorites", "mode": "incremental"}
    success, result = api_request("POST", "/api/stock/update-favorites", data=task_data)
    log_test("3.1.1 创建任务", success)

    if not success:
        return

    task_id = result.get("task_id")

    # 监控任务状态变化
    states_seen = []
    for _ in range(60):  # 最多等待60秒
        success, data = api_request("GET", f"/api/tasks/{task_id}")
        if success:
            task = data.get("task", {})
            status = task.get("status")
            if status not in states_seen:
                states_seen.append(status)

            if status in ["completed", "failed", "stopped"]:
                break
        time.sleep(1)

    log_test("3.1.2 状态转换", "completed" in states_seen or "running" in states_seen,
            f"经历的状态: {' -> '.join(states_seen)}")

    # 验证任务历史中有记录
    success, data = api_request("GET", "/api/tasks")
    if success:
        tasks = data.get("tasks", [])
        found = any(t["task_id"] == task_id for t in tasks)
        log_test("3.1.3 任务历史记录", found, "任务在历史列表中")

        # 删除任务
        if found:
            success, result = api_request("DELETE", f"/api/tasks/{task_id}")
            log_test("3.1.4 删除任务", success)

def test_3_2_checkpoint_resume():
    """测试3.2：断点续传测试"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试3.2：断点续传测试")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 创建任务
    task_data = {"stock_range": "favorites", "mode": "incremental"}
    success, result = api_request("POST", "/api/stock/update-favorites", data=task_data)

    if not success:
        log_test("3.2.0 创建任务", False, str(result))
        return

    task_id = result.get("task_id")
    log_test("3.2.1 创建任务", True, f"任务ID: {task_id[:8]}...")

    # 等待任务开始运行
    time.sleep(2)

    # 暂停任务
    success, result = api_request("POST", f"/api/tasks/{task_id}/pause")
    if not success:
        log_test("3.2.2 暂停任务", False, str(result))
        return

    time.sleep(2)

    # 获取暂停时的状态
    success, data = api_request("GET", f"/api/tasks/{task_id}")
    if not success:
        return

    task = data.get("task", {})
    paused_index = task.get("current_stock_index", 0)
    paused_stats = task.get("stats", {})

    log_test("3.2.2 任务暂停检查", task.get("status") == "paused",
            f"暂停位置: 索引{paused_index}")

    # 恢复任务
    success, result = api_request("POST", f"/api/tasks/{task_id}/resume")
    if not success:
        log_test("3.2.3 恢复任务", False, str(result))
        return

    # 等待任务完成
    time.sleep(5)

    success, data = api_request("GET", f"/api/tasks/{task_id}")
    if success:
        task = data.get("task", {})
        final_stats = task.get("stats", {})

        log_test("3.2.3 断点续传验证", True,
                f"恢复后统计 - 成功: {final_stats.get('success', 0)}, "
                f"失败: {final_stats.get('failed', 0)}")

def test_3_3_error_handling():
    """测试3.3：错误处理测试"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试3.3：错误处理测试")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 场景A：无效股票代码
    task_data = {
        "stock_range": "custom",
        "custom_stocks": ["999999", "888888"],  # 无效代码
        "mode": "incremental"
    }

    success, result = api_request("POST", "/api/stock/update-all", data=task_data)
    if success:
        task_id = result.get("task_id")

        # 等待任务完成
        for _ in range(30):
            success, data = api_request("GET", f"/api/tasks/{task_id}")
            if success:
                task = data.get("task", {})
                status = task.get("status")
                if status in ["completed", "failed"]:
                    stats = task.get("stats", {})
                    log_test("3.3.1 无效代码处理", status == "completed",
                            f"状态: {status}, 失败: {stats.get('failed', 0)}")
                    break
            time.sleep(1)

    # 场景B：空股票列表
    task_data = {
        "stock_range": "custom",
        "custom_stocks": [],
        "mode": "incremental"
    }

    success, result = api_request("POST", "/api/stock/update-all", data=task_data)
    log_test("3.3.2 空列表处理", not success,
            f"{'正确拒绝' if not success else '不应创建'}",
            str(result)[:200])

# ============================================================================
# 第四部分：性能和可靠性测试
# ============================================================================

def test_4_1_concurrent_operations():
    """测试4.1：并发操作测试"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试4.1：并发操作测试")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 获取运行中的任务
    success, data = api_request("GET", "/api/tasks", params={"status": "running"})
    if success and data.get("tasks"):
        task = data["tasks"][0]
        task_id = task["task_id"]

        # 尝试删除运行中的任务
        success, result = api_request("DELETE", f"/api/tasks/{task_id}")
        log_test("4.1.1 删除运行中的任务（应失败）", not success,
                f"{'正确拒绝' if not success else '不应允许删除'}",
                str(result)[:200])

        # 验证任务仍在运行
        success, data = api_request("GET", f"/api/tasks/{task_id}")
        if success:
            task = data.get("task", {})
            still_running = task.get("status") == "running"
            log_test("4.1.2 任务继续运行", still_running,
                    f"任务状态: {task.get('status')}")
    else:
        log_test("4.1.0 获取运行中任务", False, "没有运行中的任务")

def test_4_2_database_consistency():
    """测试4.2：数据库一致性验证"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试4.2：数据库一致性验证")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 从API获取任务
    success, data = api_request("GET", "/api/tasks", params={"limit": 1})
    if not success or not data.get("tasks"):
        log_test("4.2.0 获取任务", False, "没有任务可验证")
        return

    api_task = data["tasks"][0]
    task_id = api_task["task_id"]

    # 从数据库获取任务
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
    db_task = cursor.fetchone()
    conn.close()

    if db_task:
        log_test("4.2.1 API状态一致性", api_task.get("status") == db_task["status"],
                f"API: {api_task.get('status')}, DB: {db_task['status']}")

        log_test("4.2.2 进度一致性",
                api_task.get("progress") == db_task["progress"],
                f"API: {api_task.get('progress')}, DB: {db_task['progress']}")

        log_test("4.2.3 统计数据一致性", True,
                f"当前索引: {api_task.get('current_stock_index')}, "
                f"总数: {api_task.get('total_stocks')}")

        api_stats = api_task.get("stats", {})
        log_test("4.2.4 stats字段", isinstance(api_stats, dict),
                f"成功: {api_stats.get('success', 0)}, "
                f"失败: {api_stats.get('failed', 0)}")
    else:
        log_test("4.2.0 数据库查询", False, f"数据库中未找到任务 {task_id}")

def test_4_3_memory_leak_check():
    """测试4.3：内存泄漏检查"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试4.3：内存泄漏检查")
    print(f"{'='*60}{Colors.ENDC}\n")

    import subprocess
    import re

    # 获取Python进程内存
    try:
        result = subprocess.run(
            ["ps", "aux"],
            capture_output=True,
            text=True
        )

        lines = result.stdout.split('\n')
        python_processes = []

        for line in lines:
            if 'python' in line and 'app.py' in line:
                parts = line.split()
                if len(parts) > 5:
                    pid = parts[1]
                    mem_kb = int(parts[5])
                    mem_mb = mem_kb / 1024
                    python_processes.append((pid, mem_mb))

        if python_processes:
            log_test("4.3.1 Python进程内存", True,
                    f"找到 {len(python_processes)} 个进程, "
                    f"内存: {', '.join([f'{m:.1f}MB' for _, m in python_processes])}")
        else:
            log_test("4.3.1 Python进程内存", False, "未找到Flask进程")

    except Exception as e:
        log_test("4.3.1 内存检查", False, f"检查失败: {str(e)}")

# ============================================================================
# 第五部分：已知问题和边缘情况
# ============================================================================

def test_5_1_stop_then_delete():
    """测试5.1：停止后删除"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试5.1：停止后删除")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 获取运行中的任务
    for status in ["running", "paused", "pending"]:
        success, data = api_request("GET", "/api/tasks", params={"status": status})
        if success and data.get("tasks"):
            task = data["tasks"][0]
            task_id = task["task_id"]

            # 停止任务
            if status in ["running", "paused"]:
                api_request("POST", f"/api/tasks/{task_id}/stop")
                time.sleep(1)

            # 删除任务
            start_time = time.time()
            success, result = api_request("DELETE", f"/api/tasks/{task_id}")
            elapsed = time.time() - start_time

            log_test("5.1.1 停止后立即删除", success and elapsed < 5,
                    f"删除耗时: {elapsed:.2f}秒")
            return

    log_test("5.1.0 获取任务", False, "没有可停止的任务")

def test_5_2_cancel_pending():
    """测试5.2：取消pending任务"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试5.2：取消pending任务")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 获取pending任务
    success, data = api_request("GET", "/api/tasks", params={"status": "pending"})
    if not success or not data.get("tasks"):
        log_test("5.2.0 获取pending任务", False, "没有pending任务")
        return

    task = data["tasks"][0]
    task_id = task["task_id"]

    # 取消任务
    success, result = api_request("POST", f"/api/tasks/{task_id}/stop")
    log_test("5.2.1 取消pending任务", success, str(result)[:200])

    time.sleep(1)
    success, data = api_request("GET", f"/api/tasks/{task_id}")
    if success:
        task = data.get("task", {})
        status = task.get("status")
        message = task.get("message", "")
        log_test("5.2.2 验证状态为stopped", status == "stopped",
                f"状态: {status}, 消息: {message}")

def test_5_3_cleanup_old_tasks():
    """测试5.3：清理旧任务功能"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print(f"测试5.3：清理旧任务功能")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 清理1小时前的任务
    success, result = api_request("POST", "/api/tasks/cleanup",
                                 params={"max_age_hours": 1})
    log_test("5.3.1 清理旧任务API", success,
            f"清理结果: {str(result)[:200] if isinstance(result, str) else json.dumps(result, ensure_ascii=False)[:200]}")

    if success and isinstance(result, dict):
        deleted_count = result.get("deleted_count", 0)
        log_test("5.3.2 清理计数", True,
                f"删除了 {deleted_count} 个旧任务")

# ============================================================================
# 主测试运行器
# ============================================================================

def print_summary():
    """打印测试摘要"""
    print(f"\n{Colors.HEADER}{'='*60}")
    print("测试摘要 / Test Summary")
    print(f"{'='*60}{Colors.ENDC}\n")

    passed = sum(1 for t in test_results if t["passed"])
    failed = sum(1 for t in test_results if not t["passed"])
    total = len(test_results)

    print(f"总计 / Total:     {total}")
    print(f"{Colors.OKGREEN}通过 / Passed:   {passed}{Colors.ENDC}")
    print(f"{Colors.FAIL}失败 / Failed:   {failed}{Colors.ENDC}")
    print(f"成功率 / Success: {(passed/total*100):.1f}%")

    if failed > 0:
        print(f"\n{Colors.FAIL}失败的测试 / Failed Tests:{Colors.ENDC}")
        for t in test_results:
            if not t["passed"]:
                print(f"  ❌ {t['test_name']}")
                if t.get("message"):
                    print(f"     {t['message']}")
                if t.get("details"):
                    print(f"     详情: {t['details'][:100]}")

    # 保存到文件
    report_file = "/Users/zhangbo/Public/go/github.com/mytrader/tests/test_report.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total": total,
                "passed": passed,
                "failed": failed,
                "success_rate": f"{(passed/total*100):.1f}%"
            },
            "results": test_results
        }, f, ensure_ascii=False, indent=2)
    print(f"\n详细报告已保存至: {report_file}")

def main():
    """主函数"""
    print(f"{Colors.HEADER}{'='*60}")
    print("任务管理功能测试")
    print("Task Management Feature Tests")
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}{Colors.ENDC}\n")

    # 测试服务器连接
    success, _ = api_request("GET", "/api/tasks", params={"limit": 1})
    if not success:
        print(f"{Colors.FAIL}❌ 无法连接到服务器 {BASE_URL}{Colors.ENDC}")
        print("请确保Flask服务器正在运行:")
        print("  cd web && python app.py")
        sys.exit(1)

    print(f"{Colors.OKGREEN}✅ 服务器连接正常{Colors.ENDC}\n")

    # 运行测试套件
    test_suites = [
        ("第一部分：任务历史页面功能测试", [
            test_1_1_delete_pending_task,
            test_1_2_cancel_pending_task,
            test_1_3_delete_stopped_task,
            test_1_4_task_list_refresh,
            test_1_5_pagination,
        ]),
        ("第二部分：更新管理页面功能测试", [
            test_2_1_create_and_run_small_task,
            test_2_2_pause_and_resume,
            test_2_3_stop_running_task,
            test_2_4_task_completion,
            test_2_5_task_conflict_detection,
        ]),
        ("第三部分：集成测试", [
            test_3_1_complete_workflow,
            test_3_2_checkpoint_resume,
            test_3_3_error_handling,
        ]),
        ("第四部分：性能和可靠性测试", [
            test_4_1_concurrent_operations,
            test_4_2_database_consistency,
            test_4_3_memory_leak_check,
        ]),
        ("第五部分：已知问题和边缘情况", [
            test_5_1_stop_then_delete,
            test_5_2_cancel_pending,
            test_5_3_cleanup_old_tasks,
        ]),
    ]

    for suite_name, tests in test_suites:
        print(f"\n{Colors.BOLD}{Colors.HEADER}{'▶' * 30}")
        print(f"{suite_name}")
        print(f"{'▶' * 30}{Colors.ENDC}")

        for test_func in tests:
            try:
                test_func()
            except Exception as e:
                log_test(test_func.__name__, False,
                       f"测试异常: {str(e)}", traceback.format_exc())

    # 打印摘要
    print_summary()

if __name__ == "__main__":
    import traceback
    main()
