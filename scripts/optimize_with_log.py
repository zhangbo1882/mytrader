#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
优化脚本 - 完整日志输出到文件

使用方法:
    python scripts/optimize_with_log.py 00941 2021-01-01 2025-12-31
    python scripts/optimize_with_log.py 01810 2021-01-01 2025-12-31
"""

import sys
import os
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def main():
    if len(sys.argv) < 2:
        print("使用方法: python optimize_with_log.py <股票代码> [开始日期] [结束日期]")
        print("示例: python optimize_with_log.py 00941 2021-01-01 2025-12-31")
        sys.exit(1)

    stock_code = sys.argv[1]
    start_date = sys.argv[2] if len(sys.argv) > 2 else "2021-01-01"
    end_date = sys.argv[3] if len(sys.argv) > 3 else "2025-12-31"

    # 创建日志目录
    log_dir = Path("logs/optimization")
    log_dir.mkdir(parents=True, exist_ok=True)

    # 生成日志文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{stock_code}_FULL_{timestamp}.log"

    print("=" * 70)
    print("优化任务开始")
    print("=" * 70)
    print(f"股票代码: {stock_code}")
    print(f"开始日期: {start_date}")
    print(f"结束日期: {end_date}")
    print(f"完整日志: {log_file}")
    print("=" * 70)
    print()

    # 导入优化模块
    from src.strategies.price_breakout.optimizer.standalone import optimize_single_stock

    # 重定向stdout和stderr到日志文件
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    try:
        with open(log_file, 'w', encoding='utf-8') as f:
            sys.stdout = f
            sys.stderr = f

            # 运行优化
            result = optimize_single_stock(
                stock_code=stock_code,
                start_date=start_date,
                end_date=end_date,
                n_workers=6,
                save=True,
                mode='anchored',
                use_hybrid_space=True,
                run_robustness=True
            )

    finally:
        # 恢复stdout和stderr
        sys.stdout = original_stdout
        sys.stderr = original_stderr

    print()
    print("=" * 70)
    print("优化任务完成")
    print(f"退出代码: {result}")
    print(f"完整日志已保存到: {log_file}")

    # 显示日志文件大小
    if log_file.exists():
        file_size = log_file.stat().st_size
        if file_size > 1024 * 1024:
            size_str = f"{file_size / 1024 / 1024:.2f} MB"
        elif file_size > 1024:
            size_str = f"{file_size / 1024:.2f} KB"
        else:
            size_str = f"{file_size} bytes"
        print(f"日志文件大小: {size_str}")

    print("=" * 70)

    return result


if __name__ == "__main__":
    sys.exit(main() or 0)
