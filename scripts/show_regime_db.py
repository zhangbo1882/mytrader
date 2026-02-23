#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
显示 regime_params.db 的内容

Usage:
    python scripts/show_regime_db.py              # 显示所有内容
    python scripts/show_regime_db.py --json       # 输出JSON格式
    python scripts/show_regime_db.py --stocks     # 只显示已优化股票
    python scripts/show_regime_db.py --params     # 只显示默认参数
"""

import sys
import os
import json
import argparse

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.strategies.price_breakout.optimizer.params_db import (
    show_database_content,
    print_database_content,
    RegimeParamsDB
)


def main():
    parser = argparse.ArgumentParser(description='显示 regime_params.db 内容')
    parser.add_argument('--json', action='store_true', help='输出JSON格式')
    parser.add_argument('--stocks', action='store_true', help='只显示已优化股票')
    parser.add_argument('--params', action='store_true', help='只显示默认策略参数')
    parser.add_argument('--stats', action='store_true', help='只显示统计信息')
    parser.add_argument('--db', default='data/regime_params.db', help='数据库路径')
    
    args = parser.parse_args()
    
    if args.json:
        # JSON输出
        content = show_database_content(args.db)
        print(json.dumps(content, indent=2, ensure_ascii=False))
    elif args.stocks:
        # 只显示已优化股票（详细版）
        content = show_database_content(args.db)
        print("="*80)
        print("已优化股票列表（详细）")
        print("="*80)
        for i, item in enumerate(content['best_params'], 1):
            print(f"\n  {i}. {item['stock_code']}")
            print(f"     得分: {item['score']:.4f} | 收益: {item['total_return']*100:.2f}% | "
                  f"夏普: {item['sharpe_ratio']:.2f} | 回撤: {item['max_drawdown']*100:.2f}%")
            print(f"     参数: 买入{item['strategy_params'].get('base_buy_threshold', 0)}% | "
                  f"止盈{item['strategy_params'].get('base_sell_threshold', 0)}% | "
                  f"止损{item['strategy_params'].get('base_stop_loss_threshold', 0)}%")
    elif args.params:
        # 只显示默认策略参数
        content = show_database_content(args.db)
        print("="*80)
        print("默认策略参数")
        print("="*80)
        for item in content['default_strategy_params']:
            print(f"\n{item['strategy_name']}:")
            print(f"  牛市: buy×{item['bull']['buy_multiplier']} sell×{item['bull']['sell_multiplier']} stop×{item['bull']['stop_multiplier']}")
            print(f"  熊市: buy×{item['bear']['buy_multiplier']} sell×{item['bear']['sell_multiplier']} stop×{item['bear']['stop_multiplier']}")
            print(f"  阈值: Bull>={item['bull_threshold']}, Bear<={item['bear_threshold']}")
    elif args.stats:
        # 只显示统计信息
        db = RegimeParamsDB(args.db)
        stats = db.get_statistics()
        print("="*80)
        print("数据库统计")
        print("="*80)
        print(f"已优化股票数: {stats['total_stocks']}")
        print(f"平均得分: {stats['avg_score']:.4f}")
        print(f"最高得分: {stats['max_score']:.4f}")
        print(f"最低得分: {stats['min_score']:.4f}")
        db.close()
    else:
        # 显示所有内容
        print_database_content(args.db)


if __name__ == '__main__':
    main()
