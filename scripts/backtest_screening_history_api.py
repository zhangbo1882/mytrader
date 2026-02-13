#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
使用API模式对筛选历史中的股票进行批量回测

这个脚本通过HTTP API创建回测任务，处理任务系统的并发限制。
由于任务系统只允许同时运行一个回测任务，脚本会：
1. 检查并清理现有的回测任务
2. 逐个创建回测任务（等待每个完成后再创建下一个）
3. 收集所有结果

使用步骤：
python scripts/backtest_screening_history_api.py --history-name "PE0-200_市场:主板_市值50-200_等2条件_20260212_2209" --start-date 2025-12-01 --end-date 2025-12-30 --strategy price_breakout
"""
import sys
import argparse
import json
import requests
import time
from typing import List, Dict, Any
from datetime import datetime
import pandas as pd

# API基础URL
BASE_URL = "http://127.0.0.1:5001/api"

def get_screening_history_list() -> List[Dict[str, Any]]:
    """获取筛选历史列表"""
    url = f"{BASE_URL}/screening/history"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"获取筛选历史列表失败: {response.status_code} - {response.text}")

    data = response.json()
    if not data.get('success'):
        raise Exception(f"API返回错误: {data.get('error', '未知错误')}")

    return data.get('history', [])

def get_screening_history_detail(history_id: int) -> Dict[str, Any]:
    """获取筛选历史详情"""
    url = f"{BASE_URL}/screening/history/{history_id}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"获取筛选历史详情失败: {response.status_code} - {response.text}")

    data = response.json()
    if not data.get('success'):
        raise Exception(f"API返回错误: {data.get('error', '未知错误')}")

    return data.get('detail', {})

def find_history_by_name(name: str) -> Dict[str, Any]:
    """通过名称查找筛选历史记录"""
    history_list = get_screening_history_list()
    for history in history_list:
        if history.get('name') == name:
            return history
    return None

def get_all_tasks() -> List[Dict[str, Any]]:
    """获取所有任务"""
    url = f"{BASE_URL}/tasks"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"获取任务列表失败: {response.status_code} - {response.text}")

    data = response.json()
    if not data.get('success'):
        raise Exception(f"API返回错误: {data.get('error', '未知错误')}")

    return data.get('tasks', [])

def get_pending_backtest_tasks() -> List[Dict[str, Any]]:
    """获取待处理的回测任务"""
    all_tasks = get_all_tasks()
    backtest_tasks = []

    for task in all_tasks:
        if task.get('task_type') == 'backtest' and task.get('status') in ['pending', 'running', 'paused']:
            backtest_tasks.append(task)

    return backtest_tasks

def delete_task(task_id: str) -> bool:
    """删除任务"""
    url = f"{BASE_URL}/tasks/{task_id}"
    response = requests.delete(url)
    if response.status_code == 200:
        return True
    elif response.status_code == 404:
        return False
    else:
        data = response.json()
        raise Exception(f"删除任务失败: {response.status_code} - {data.get('error', '未知错误')}")

def cancel_backtest_tasks() -> int:
    """取消所有待处理的回测任务"""
    pending_tasks = get_pending_backtest_tasks()
    cancelled = 0

    for task in pending_tasks:
        task_id = task.get('task_id')
        print(f"  取消回测任务: {task_id}")
        if delete_task(task_id):
            cancelled += 1

    return cancelled

def create_backtest_task(params: Dict[str, Any]) -> str:
    """通过API创建回测任务"""
    url = f"{BASE_URL}/backtest/run"
    response = requests.post(url, json=params)

    if response.status_code == 201:
        data = response.json()
        if data.get('success'):
            return data.get('task_id')
        else:
            raise Exception(f"API返回错误: {data.get('error', '未知错误')}")
    elif response.status_code == 409:
        # 任务已存在，需要先清理
        raise Exception("存在待处理的回测任务，请先清理或等待完成")
    else:
        raise Exception(f"创建回测任务失败: {response.status_code} - {response.text}")

def get_task_status(task_id: str) -> Dict[str, Any]:
    """获取任务状态"""
    url = f"{BASE_URL}/backtest/status/{task_id}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"获取任务状态失败: {response.status_code} - {response.text}")

    data = response.json()
    if not data.get('success'):
        raise Exception(f"API返回错误: {data.get('error', '未知错误')}")

    return data

def get_task_result(task_id: str) -> Dict[str, Any]:
    """获取任务结果"""
    url = f"{BASE_URL}/backtest/result/{task_id}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        if data.get('success'):
            return data.get('result', {})
        else:
            raise Exception(f"API返回错误: {data.get('error', '未知错误')}")
    elif response.status_code == 202:
        # 任务仍在运行中
        return None
    else:
        raise Exception(f"获取任务结果失败: {response.status_code} - {response.text}")

def wait_for_task_completion(task_id: str, max_wait_seconds: int = 600) -> Dict[str, Any]:
    """等待任务完成"""
    start_time = time.time()
    poll_interval = 5  # 轮询间隔（秒）

    print(f"  等待任务 {task_id[:8]}... 完成: ", end="", flush=True)

    while time.time() - start_time < max_wait_seconds:
        result = get_task_result(task_id)

        if result is not None:
            # 任务完成
            print("✓")
            return result

        # 任务仍在运行中，显示进度点
        print(".", end="", flush=True)
        time.sleep(poll_interval)

    print("✗ (超时)")
    raise Exception(f"任务 {task_id} 在 {max_wait_seconds} 秒内未完成")

def main():
    parser = argparse.ArgumentParser(description='使用API模式对筛选历史中的股票进行批量回测')
    parser.add_argument('--history-name', type=str, required=True,
                       help='筛选历史记录名称')
    parser.add_argument('--start-date', type=str, required=True,
                       help='回测开始日期，格式: YYYY-MM-DD')
    parser.add_argument('--end-date', type=str, required=True,
                       help='回测结束日期，格式: YYYY-MM-DD')
    parser.add_argument('--strategy', type=str, default='price_breakout',
                       help='回测策略类型（默认: price_breakout）')
    parser.add_argument('--strategy-params', type=json.loads, default='{}',
                       help='策略参数，JSON格式字符串（默认: {}）')
    parser.add_argument('--cash', type=float, default=1000000,
                       help='初始资金（默认: 1000000）')
    parser.add_argument('--commission', type=float, default=0.0002,
                       help='手续费率（默认: 0.02%）')
    parser.add_argument('--limit', type=int, default=None,
                       help='限制回测股票数量（默认: 无限制）')
    parser.add_argument('--cleanup-first', action='store_true',
                       help='先清理所有待处理的回测任务')
    parser.add_argument('--max-wait-per-task', type=int, default=600,
                       help='每个任务最大等待时间（秒，默认: 600）')

    args = parser.parse_args()

    print("=" * 80)
    print("筛选历史批量回测工具 (API模式)")
    print("=" * 80)

    # 1. 检查并清理待处理任务
    print(f"\n1. 检查任务状态")
    pending_tasks = get_pending_backtest_tasks()

    if pending_tasks:
        print(f"   发现 {len(pending_tasks)} 个待处理的回测任务:")
        for task in pending_tasks:
            print(f"     - {task.get('task_id')}: {task.get('status')}")

        if args.cleanup_first:
            print(f"   警告: 即将清理 {len(pending_tasks)} 个回测任务")
            print(f"   这些任务将会被取消且无法恢复")
            print(f"   清理待处理任务...")
            cancelled = cancel_backtest_tasks()
            print(f"   已取消 {cancelled} 个任务")
        else:
            print(f"   警告: 存在待处理任务，可能会影响新任务创建")
            print(f"   使用 --cleanup-first 参数自动清理（仅清理回测任务，不影响其他类型任务）")
    else:
        print(f"   没有待处理的回测任务")

    # 2. 查找筛选历史记录
    print(f"\n2. 查找筛选历史记录: {args.history_name}")
    history = find_history_by_name(args.history_name)
    if not history:
        print(f"错误: 未找到名称为 '{args.history_name}' 的筛选历史记录")
        sys.exit(1)

    history_id = history['id']
    print(f"   找到历史记录: ID={history_id}, 名称={history['name']}, 股票数量={history.get('stocks_count', 0)}")

    # 3. 获取筛选历史详情（包含股票列表）
    print(f"\n3. 获取筛选历史详情")
    detail = get_screening_history_detail(history_id)
    stocks = detail.get('stocks', [])

    if not stocks:
        print("错误: 筛选历史记录中没有股票数据")
        sys.exit(1)

    print(f"   获取到 {len(stocks)} 支股票")

    # 4. 准备回测参数
    print(f"\n4. 准备回测参数")
    print(f"   策略类型: {args.strategy}")
    print(f"   回测期间: {args.start_date} 到 {args.end_date}")
    print(f"   初始资金: {args.cash}")
    print(f"   手续费率: {args.commission}")

    params_list = []
    for i, stock in enumerate(stocks):
        if args.limit and i >= args.limit:
            break

        params = {
            'stock': stock['code'],
            'start_date': args.start_date,
            'end_date': args.end_date,
            'cash': args.cash,
            'commission': args.commission,
            'strategy': args.strategy,
            'strategy_params': args.strategy_params
        }
        params_list.append(params)

    print(f"   准备对 {len(params_list)} 支股票进行回测")

    # 5. 执行批量回测（逐个处理）
    print(f"\n5. 执行批量回测 (API模式)")
    print(f"   注意: 由于任务系统限制，每次只能运行一个回测任务")
    print(f"   预计总时间: {len(params_list) * args.max_wait_per_task // 60} 分钟")

    results = []

    for i, params in enumerate(params_list):
        stock_code = params['stock']
        stock_name = ""
        for stock in stocks:
            if stock['code'] == stock_code:
                stock_name = stock.get('name', '')
                break

        print(f"\n   [{i+1}/{len(params_list)}] 回测股票: {stock_code} ({stock_name})")

        try:
            # 创建任务
            print(f"     创建回测任务... ", end="", flush=True)
            task_id = create_backtest_task(params)
            print(f"✓ (任务ID: {task_id[:8]}...)")

            # 等待任务完成
            print(f"     等待任务完成 ", end="", flush=True)
            result = wait_for_task_completion(task_id, args.max_wait_per_task)

            results.append({
                'index': i,
                'stock': stock_code,
                'stock_name': stock_name,
                'task_id': task_id,
                'status': 'success',
                'result': result
            })

            # 显示简要结果
            total_return = result.get('basic_info', {}).get('total_return', 0)
            trades = result.get('trade_stats', {}).get('total_trades', 0)
            print(f"     结果: 收益率={total_return:.2%}, 交易次数={trades}")

        except Exception as e:
            print(f"     失败: {str(e)}")
            results.append({
                'index': i,
                'stock': stock_code,
                'stock_name': stock_name,
                'status': 'failed',
                'error': str(e)
            })

            # 如果是因为任务已存在失败，建议清理
            if "存在待处理的回测任务" in str(e):
                print(f"     建议: 使用 --cleanup-first 参数清理现有任务")

    # 6. 汇总结果
    print(f"\n6. 回测结果汇总")
    print("=" * 80)

    successful = [r for r in results if r.get('status') == 'success']
    failed = [r for r in results if r.get('status') == 'failed']

    print(f"回测完成: 成功 {len(successful)} 个, 失败 {len(failed)} 个")

    if successful:
        # 计算统计指标
        total_returns = []
        sharpe_ratios = []
        max_drawdowns = []
        trade_counts = []

        for result in successful:
            r = result.get('result', {})
            total_returns.append(r.get('basic_info', {}).get('total_return', 0))
            sharpe_ratios.append(r.get('health_metrics', {}).get('sharpe_ratio', 0))
            max_drawdowns.append(r.get('health_metrics', {}).get('max_drawdown', 0))
            trade_counts.append(r.get('trade_stats', {}).get('total_trades', 0))

        print(f"\n回测结果统计:")
        print(f"平均总收益率: {sum(total_returns)/len(total_returns) if total_returns else 0:.2%}")
        print(f"平均夏普比率: {sum(sharpe_ratios)/len(sharpe_ratios) if sharpe_ratios else 0:.2f}")
        print(f"平均最大回撤: {sum(max_drawdowns)/len(max_drawdowns) if max_drawdowns else 0:.2%}")
        print(f"平均交易次数: {sum(trade_counts)/len(trade_counts) if trade_counts else 0:.1f}")
        print(f"总交易次数: {sum(trade_counts)}")

        print(f"\n收益率分布:")
        positive = sum(1 for r in total_returns if r > 0)
        negative = sum(1 for r in total_returns if r < 0)
        zero = sum(1 for r in total_returns if r == 0)
        print(f"  正收益: {positive} 支 ({positive/len(total_returns)*100:.1f}%)")
        print(f"  负收益: {negative} 支 ({negative/len(total_returns)*100:.1f}%)")
        print(f"  零收益: {zero} 支 ({zero/len(total_returns)*100:.1f}%)")

        if total_returns:
            print(f"  最高收益: {max(total_returns):.2%}")
            print(f"  最低收益: {min(total_returns):.2%}")
            print(f"  中位数收益: {sorted(total_returns)[len(total_returns)//2]:.2%}")

        print(f"\n详细结果:")
        print(f"{'序号':<4} {'股票代码':<10} {'股票名称':<15} {'总收益率':<12} {'夏普比率':<12} {'最大回撤':<12} {'交易次数':<10}")
        print("-" * 80)

        for i, result in enumerate(successful):
            r = result.get('result', {})
            stock_code = result.get('stock', '')
            stock_name = result.get('stock_name', '')[:15]

            basic_info = r.get('basic_info', {})
            health_metrics = r.get('health_metrics', {})
            trade_stats = r.get('trade_stats', {})

            print(f"{i+1:<4} {stock_code:<10} {stock_name:<15} "
                  f"{basic_info.get('total_return', 0):>11.2%} "
                  f"{health_metrics.get('sharpe_ratio', 0):>11.2f} "
                  f"{health_metrics.get('max_drawdown', 0):>11.2%} "
                  f"{trade_stats.get('total_trades', 0):>9}")

    if failed:
        print(f"\n失败的回测 ({len(failed)} 个):")
        for result in failed:
            print(f"  股票 {result.get('stock')} ({result.get('stock_name', '')}): {result.get('error')}")

    print(f"\n批量回测完成!")

    # 7. 保存结果到文件
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"backtest_api_results_{args.history_name}_{timestamp}.json"

    # 准备保存的数据
    save_data = {
        'history_name': args.history_name,
        'history_id': history_id,
        'start_date': args.start_date,
        'end_date': args.end_date,
        'strategy': args.strategy,
        'strategy_params': args.strategy_params,
        'cash': args.cash,
        'commission': args.commission,
        'results': results,
        'summary': {
            'total': len(results),
            'successful': len(successful),
            'failed': len(failed),
            'success_rate': len(successful) / len(results) if results else 0
        }
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(save_data, f, ensure_ascii=False, indent=2)

    print(f"\n结果已保存到: {output_file}")

if __name__ == '__main__':
    main()