#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
批量删除所有回测历史记录脚本

使用方法:
    python scripts/delete_all_backtest_history.py [--force] [--dry-run]

参数:
    --force: 强制删除，跳过确认步骤
    --dry-run: 模拟运行，只显示将要删除的记录但不实际删除

说明:
    1. 使用已有的API获取所有回测历史记录
    2. 使用已有的API逐条删除所有记录
    3. 默认需要确认，避免误删除
    4. 显示删除进度和结果统计
"""

import requests
import time
import sys
import argparse

# API配置
BASE_URL = "http://127.0.0.1:5001/api"
HISTORY_ENDPOINT = "/backtest/history"
DELETE_ENDPOINT = "/backtest/history"

def get_all_backtest_history():
    """
    获取所有回测历史记录

    Returns:
        list: 所有历史记录列表，每个元素包含task_id等信息
    """
    print("正在获取所有回测历史记录...")

    # 设置较大的page_size以一次性获取所有记录
    params = {
        'page': 1,
        'page_size': 1000  # 设置足够大，确保获取所有记录
    }

    try:
        response = requests.get(f"{BASE_URL}{HISTORY_ENDPOINT}", params=params)
        response.raise_for_status()
        data = response.json()

        if data.get('success'):
            total = data.get('total', 0)
            histories = data.get('history', [])
            print(f"找到 {total} 条回测历史记录")
            return histories
        else:
            print(f"获取历史记录失败: {data.get('message')}")
            return []

    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return []
    except Exception as e:
        print(f"处理响应失败: {e}")
        return []

def delete_backtest_history(task_id, dry_run=False):
    """
    删除单条回测历史记录

    Args:
        task_id: 任务ID
        dry_run: 模拟运行模式，不实际删除

    Returns:
        bool: 删除是否成功（模拟运行始终返回True）
    """
    if dry_run:
        print(f"  [模拟运行] 将删除: {task_id}")
        return True

    try:
        response = requests.delete(f"{BASE_URL}{DELETE_ENDPOINT}/{task_id}")
        response.raise_for_status()
        data = response.json()

        if data.get('success'):
            return True
        else:
            print(f"删除失败 {task_id}: {data.get('message')}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"删除请求失败 {task_id}: {e}")
        return False
    except Exception as e:
        print(f"处理删除响应失败 {task_id}: {e}")
        return False

def confirm_deletion(histories, force=False, dry_run=False):
    """
    确认删除操作

    Args:
        histories: 历史记录列表
        force: 是否强制删除，跳过确认
        dry_run: 是否模拟运行模式

    Returns:
        bool: 是否确认删除
    """
    if not histories:
        print("没有需要删除的历史记录")
        return False

    print("\n" + "="*60)
    print(f"即将删除 {len(histories)} 条回测历史记录")
    print("="*60)

    # 显示前几条记录作为示例
    print("示例记录:")
    for i, history in enumerate(histories[:3]):
        print(f"  {i+1}. {history.get('name')} (ID: {history.get('task_id')})")

    if len(histories) > 3:
        print(f"  ... 还有 {len(histories) - 3} 条记录")

    print("\n警告: 此操作不可逆！")

    # 如果是强制模式或模拟运行模式，直接确认
    if force or dry_run:
        mode = "模拟运行" if dry_run else "强制删除"
        print(f"\n{mode}模式已启用，跳过确认")
        return True

    # 获取用户确认
    while True:
        try:
            confirm = input(f"\n确认删除 {len(histories)} 条记录？(输入 'YES' 确认，其他取消): ").strip()
            if confirm == 'YES':
                return True
            else:
                print("取消删除操作")
                return False
        except KeyboardInterrupt:
            print("\n取消删除操作")
            return False

def main():
    """
    主函数
    """
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='批量删除回测历史记录')
    parser.add_argument('--force', action='store_true', help='强制删除，跳过确认步骤')
    parser.add_argument('--dry-run', action='store_true', help='模拟运行，只显示将要删除的记录但不实际删除')
    args = parser.parse_args()

    print("="*60)
    print("批量删除回测历史记录工具")
    if args.force:
        print("模式: 强制删除（跳过确认）")
    if args.dry_run:
        print("模式: 模拟运行（不实际删除）")
    print("="*60)

    # 1. 获取所有历史记录
    histories = get_all_backtest_history()

    if not histories:
        print("没有找到回测历史记录，程序退出")
        return

    # 2. 确认删除（除非强制模式或模拟运行模式）
    if not confirm_deletion(histories, force=args.force, dry_run=args.dry_run):
        return

    # 3. 执行删除
    mode_text = "模拟删除" if args.dry_run else "删除"
    print(f"\n开始{mode_text} {len(histories)} 条记录...")
    print("-"*40)

    deleted_count = 0
    failed_count = 0
    failed_ids = []

    for i, history in enumerate(histories, 1):
        task_id = history.get('task_id')
        name = history.get('name')

        print(f"[{i}/{len(histories)}] 正在{mode_text}: {name}")

        if delete_backtest_history(task_id, dry_run=args.dry_run):
            deleted_count += 1
            if not args.dry_run:
                print(f"    删除成功")
        else:
            failed_count += 1
            failed_ids.append(task_id)
            if not args.dry_run:
                print(f"    删除失败")

        # 避免请求过于频繁
        if not args.dry_run and i < len(histories):
            time.sleep(0.1)  # 100毫秒间隔

    # 4. 显示结果
    print("\n" + "="*60)
    print(f"{mode_text}完成!")
    print("="*60)
    print(f"总计: {len(histories)} 条记录")
    if args.dry_run:
        print(f"模拟{mode_text}: {len(histories)} 条")
    else:
        print(f"成功: {deleted_count} 条")
        print(f"失败: {failed_count} 条")

    if not args.dry_run and failed_ids:
        print("\n失败的记录ID:")
        for task_id in failed_ids:
            print(f"  {task_id}")

        print("\n您可以手动重试删除这些记录:")
        for task_id in failed_ids:
            print(f"  curl -X DELETE '{BASE_URL}{DELETE_ENDPOINT}/{task_id}'")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n用户中断程序")
        sys.exit(1)