#!/usr/bin/env python3
"""
财务数据一致性全量检查脚本

用法：
    python scripts/check_financial_data_consistency.py --all
    python scripts/check_financial_data_consistency.py --stocks 601958,600000
    python scripts/check_financial_data_consistency.py --output report.json
"""

import sys
import os
import argparse
import json
from datetime import datetime

# 添加项目路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.valuation.data.financial_data_consistency_checker import (
    FinancialDataConsistencyChecker,
)
from src.data_sources.tushare import TushareDB
from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH


def main():
    parser = argparse.ArgumentParser(description="财务数据一致性检查")
    parser.add_argument("--all", action="store_true", help="检查所有股票")
    parser.add_argument("--stocks", type=str, help="检查指定股票（逗号分隔）")
    parser.add_argument("--output", type=str, help="输出报告文件路径")
    parser.add_argument(
        "--limit", type=int, default=None, help="最多检查多少只股票（默认不限制）"
    )
    parser.add_argument(
        "--check-historical",
        action="store_true",
        help="检查过去3年的历史数据完整性（默认False）",
    )
    parser.add_argument(
        "--historical-years",
        type=int,
        default=3,
        help="检查过去几年的历史数据（默认3年）",
    )
    parser.add_argument(
        "--required-fields",
        type=str,
        help="必需字段，格式：balancesheet.total_assets,income.revenue",
    )

    args = parser.parse_args()

    # 初始化检查器
    checker = FinancialDataConsistencyChecker(str(TUSHARE_DB_PATH))
    db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

    # 解析必需字段
    required_fields = None
    if args.required_fields:
        required_fields = parse_required_fields(args.required_fields)

    # 获取股票列表
    stock_list = []

    if args.stocks:
        stock_list = [s.strip() for s in args.stocks.split(",")]
    elif args.all:
        print("正在获取所有股票列表...")
        try:
            # 从数据库直接查询有财务数据的股票
            import pandas as pd

            query = """
            SELECT DISTINCT ts_code 
            FROM balancesheet 
            WHERE report_type = '1'
            ORDER BY ts_code
            """
            df = pd.read_sql_query(query, checker.engine)
            stock_list = df["ts_code"].tolist()

            if args.limit:
                stock_list = stock_list[: args.limit]
            print(f"共获取 {len(stock_list)} 只股票")
        except Exception as e:
            print(f"获取股票列表失败: {e}")
            return
    else:
        print("请使用 --all 检查所有股票，或使用 --stocks 指定股票")
        parser.print_help()
        return

    if not stock_list:
        print("股票列表为空")
        return

    print(f"\n{'=' * 80}")
    print(f"开始检查 {len(stock_list)} 只股票的财务数据一致性")
    print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 80}\n")

    # 批量检查
    start_time = datetime.now()

    try:
        results = checker.batch_check_consistency(
            stock_list, required_fields=required_fields
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # 打印结果
        print(f"\n{'=' * 80}")
        print("财务数据一致性检查报告")
        print(f"{'=' * 80}")
        print(f"检查时间: {results['checked_at']}")
        print(f"检查耗时: {duration:.2f} 秒")
        print(f"检查股票数: {results['total_stocks']}")
        print(f"\n统计结果:")
        print(f"  ✅ 数据完整: {results['passed']} ({results['pass_rate'] * 100:.1f}%)")
        print(
            f"  ❌ 数据不完整: {results['failed']} ({results['fail_rate'] * 100:.1f}%)"
        )

        print(f"\n质量分布:")
        print(f"  A级（优秀）: {results['quality_distribution']['A']}")
        print(f"  B级（良好）: {results['quality_distribution']['B']}")
        print(f"  C级（一般）: {results['quality_distribution']['C']}")
        print(f"  D级（不可用）: {results['quality_distribution']['D']}")

        if results["failed_stocks"]:
            print(f"\n问题股票 TOP 10:")
            for i, stock in enumerate(results["failed_stocks"][:10], 1):
                issues = ", ".join(stock["issues"][:3])  # 只显示前3个问题
                print(f"  {i}. {stock['code']}: {issues}")

        print(f"\n{'=' * 80}")

        # 保存报告
        if args.output:
            output_path = args.output
        else:
            # 默认保存到 reports 目录
            os.makedirs("reports", exist_ok=True)
            output_path = f"reports/financial_data_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        print(f"✅ 详细报告已保存到: {output_path}")
        print(f"{'=' * 80}\n")

    except Exception as e:
        print(f"\n❌ 检查过程中发生错误: {e}")
        import traceback

        traceback.print_exc()


def parse_required_fields(fields_str):
    """
    解析必需字段字符串

    Args:
        fields_str: "balancesheet.total_assets,income.revenue,cashflow.free_cashflow"

    Returns:
        {
            'balancesheet': ['total_assets'],
            'income': ['revenue'],
            'cashflow': ['free_cashflow']
        }
    """
    result = {}

    for field_spec in fields_str.split(","):
        field_spec = field_spec.strip()
        if "." not in field_spec:
            continue

        table, field = field_spec.split(".", 1)
        table = table.strip()
        field = field.strip()

        if table not in result:
            result[table] = []

        result[table].append(field)

    return result


if __name__ == "__main__":
    main()
