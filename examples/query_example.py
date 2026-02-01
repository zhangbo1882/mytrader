#!/usr/bin/env python3
"""
StockQuery 使用示例
演示如何使用新的查询API
"""

import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_sources.tushare import TushareDB
from src.data_sources.query.technical import TechnicalIndicators
from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH


def main():
    """主函数"""
    # 初始化数据库
    db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

    # 获取查询器
    query = db.query()

    # 示例1：基础查询
    print("=" * 60)
    print("示例1：基础查询")
    print("=" * 60)
    df = query.query_bars("600382", "2025-01-01", "2025-01-31")
    print(f"查询到 {len(df)} 条记录")
    if not df.empty:
        print(f"最新数据日期: {df.iloc[-1]['datetime']}")
        print(f"最新收盘价: {df.iloc[-1]['close']:.2f}\n")

    # 示例2：条件过滤查询
    print("=" * 60)
    print("示例2：查询换手率 > 1% 的交易日")
    print("=" * 60)
    df = query.query_by_turnover("600382", "2025-01-01", "2025-12-31", min_turnover=1.0)
    print(f"查询到 {len(df)} 条记录")
    if not df.empty and 'turnover' in df.columns:
        print(f"平均换手率: {df['turnover'].mean():.2f}%\n")

    # 示例3：复合条件查询
    print("=" * 60)
    print("示例3：复合条件查询（价格10-50元，成交量>100万股）")
    print("=" * 60)
    df = query.query_with_filters(
        "600382", "2025-01-01", "2025-12-31",
        filters={
            'close': (10, 50),
            'volume': (1000000, None),
            'pct_chg': (-5, 5)
        }
    )
    print(f"查询到 {len(df)} 条记录\n")

    # 示例4：数据完整性检查
    print("=" * 60)
    print("示例4：数据完整性检查")
    print("=" * 60)
    info = query.check_data_completeness("600382", "2025-01-01", "2025-12-31")
    print(f"完整率: {info['completeness_rate']:.2%}")
    print(f"应有交易日: {info['total_days']}")
    print(f"实际数据: {info['actual_days']}")
    print(f"缺失天数: {info['missing_count']}")
    if info['missing_count'] > 0:
        print(f"缺失日期: {info['missing_days'][:5]}...")  # 只显示前5个
    print()

    # 示例5：统计分析
    print("=" * 60)
    print("示例5：统计分析")
    print("=" * 60)
    stats = query.get_summary_stats("600382", "2025-01-01", "2025-12-31")
    print(f"总收益率: {stats['total_return']:.2%}")
    print(f"年化收益率: {stats['annual_return']:.2%}")
    print(f"最大回撤: {stats['max_drawdown']:.2%}")
    print(f"波动率: {stats['volatility']:.2%}\n")

    # 示例6：技术指标
    print("=" * 60)
    print("示例6：技术指标计算")
    print("=" * 60)
    df = query.query_bars("600382", "2025-01-01", "2025-12-31", price_type="qfq")

    if not df.empty:
        # 计算技术指标
        df['ma5'] = TechnicalIndicators.sma(df, period=5)
        df['ma20'] = TechnicalIndicators.sma(df, period=20)
        df['rsi'] = TechnicalIndicators.rsi(df, period=14)
        df = TechnicalIndicators.macd(df)
        df = TechnicalIndicators.bollinger_bands(df)

        latest = df.iloc[-1]
        print(f"日期: {latest['datetime']}")
        print(f"收盘价: {latest['close']:.2f}")
        if pd.notna(latest['ma5']):
            print(f"MA5: {latest['ma5']:.2f}")
        if pd.notna(latest['ma20']):
            print(f"MA20: {latest['ma20']:.2f}")
        if pd.notna(latest['rsi']):
            print(f"RSI: {latest['rsi']:.2f}")
        if pd.notna(latest.get('macd')):
            print(f"MACD: {latest['macd']:.2f}")

        # 判断趋势
        if pd.notna(latest['ma5']) and pd.notna(latest['ma20']):
            if latest['ma5'] > latest['ma20']:
                print("趋势: 短期均线向上")
            else:
                print("趋势: 短期均线向下")

        # 判断超买超卖
        if pd.notna(latest['rsi']):
            if latest['rsi'] > 70:
                print("RSI: 超买")
            elif latest['rsi'] < 30:
                print("RSI: 超卖")

    print("\n" + "=" * 60)
    print("示例运行完成！")
    print("=" * 60)


if __name__ == "__main__":
    import pandas as pd  # 示例中用到
    main()
