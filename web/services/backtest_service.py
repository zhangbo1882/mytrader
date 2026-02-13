#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
回测服务层

提供回测功能的核心服务，支持多策略类型
"""
import backtrader as bt
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional, List
import logging

from src.strategies.registry import (
    get_strategy_class,
    validate_strategy_params,
    get_strategy_description,
    get_default_params,
    get_supported_strategies
)
from src.strategies.analyzers import PortfolioValueAnalyzer, TradeAnalyzer, calculate_strategy_metrics
from src.strategies.metrics import strategy_health_check, calculate_backtest_returns
from src.data_sources.tushare import TushareDB
from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH

logger = logging.getLogger(__name__)


# ============================================================================
# 回测共有参数默认值
# ============================================================================

DEFAULT_BACKTEST_PARAMS = {
    'cash': 1000000,           # 初始资金100万
    'commission': 0.0002,       # 手续费率0.02%
    'start_date': '2026-01-01',
    'end_date': None,          # None表示使用最新日期
}


def _serialize_trade(trade: Dict[str, Any]) -> Dict[str, Any]:
    """
    序列化交易记录，将日期对象转换为字符串

    Args:
        trade: 原始交易记录

    Returns:
        序列化后的交易记录
    """
    serialized = trade.copy()
    if 'buy_date' in serialized and hasattr(serialized['buy_date'], 'strftime'):
        serialized['buy_date'] = serialized['buy_date'].strftime('%Y-%m-%d')
    if 'sell_date' in serialized and hasattr(serialized['sell_date'], 'strftime'):
        serialized['sell_date'] = serialized['sell_date'].strftime('%Y-%m-%d')
    return serialized


# ============================================================================
# 回测执行函数
# ============================================================================

def run_single_backtest(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    执行单股票回测（支持多策略）

    Args:
        params: 回测参数，包含：
            回测共有参数:
                - stock: 股票代码（必需）
                - start_date: 开始日期（必需）
                - end_date: 结束日期（可选）
                - cash: 初始资金（可选，默认100万）
                - commission: 手续费率（可选，默认0.02%）
                - benchmark: 基准指数（可选）

            策略参数:
                - strategy: 策略类型（必需）
                - strategy_params: 策略特定参数（必需）

    Returns:
        回测结果字典:
        {
            'basic_info': {...},           # 基础信息
            'strategy_info': {...},         # 策略信息
            'trade_stats': {...},           # 交易统计
            'trades': [...],                # 详细交易记录
            'health_metrics': {...},        # 策略健康指标
            'benchmark_comparison': {...}   # 基准对比（可选）
        }
    """
    # 1. 提取回测共有参数
    stock_code = params.get('stock')
    start_date = params.get('start_date')
    end_date = params.get('end_date')
    cash = params.get('cash', DEFAULT_BACKTEST_PARAMS['cash'])
    commission = params.get('commission', DEFAULT_BACKTEST_PARAMS['commission'])
    benchmark = params.get('benchmark')

    # 2. 提取策略参数
    strategy_type = params.get('strategy', 'sma_cross')
    strategy_params = params.get('strategy_params', {})

    # 3. 验证回测共有参数
    if not stock_code:
        raise ValueError("缺少必需参数: stock")

    if not start_date:
        raise ValueError("缺少必需参数: start_date")

    # 4. 验证策略类型
    strategy_class = get_strategy_class(strategy_type)
    if not strategy_class:
        raise ValueError(f"不支持的策略类型: {strategy_type}")

    # 5. 验证策略参数
    is_valid, error = validate_strategy_params(strategy_type, strategy_params)
    if not is_valid:
        raise ValueError(f"策略参数验证失败: {error}")

    # 6. 合并默认策略参数
    # 注意：commission是回测共有参数，不应该传递给策略类
    # 从strategy_params中移除commission，避免覆盖策略的默认值
    strategy_params_filtered = {k: v for k, v in strategy_params.items() if k != 'commission'}
    default_params = get_default_params(strategy_type)
    merged_strategy_params = {**default_params, **strategy_params_filtered}

    # 7. 获取股票数据
    db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

    # 标准化股票代码并加载数据
    ts_code = db._standardize_code(stock_code)
    end = end_date or datetime.now().strftime('%Y-%m-%d')

    # 加载不复权数据（用于策略回测和显示）
    stock_df_qfq = db.load_bars(
        symbol=stock_code,
        start=start_date,
        end=end,
        interval='1d',
        price_type=''  # 使用不复权数据
    )

    # 复制一份用于显示（不复权数据本身就是原始价格）
    stock_df_original = stock_df_qfq.copy()

    if stock_df_qfq is None or stock_df_qfq.empty:
        raise ValueError(f"数据库中没有股票 {stock_code} 在指定时间范围内的数据")

    # 8. 准备数据：将 datetime 设置为索引（backtrader 要求）
    stock_df = stock_df_qfq.set_index('datetime')
    stock_df = stock_df.sort_index()

    # 创建不复权价格的日期映射，供分析器使用
    original_price_map = {}
    for idx, row in stock_df.iterrows():
        date_str = idx.strftime('%Y-%m-%d')
        original_price_map[date_str] = {
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': row['close']
        }

    # 9. 创建 Cerebro 引擎
    cerebro = bt.Cerebro()

    # 10. 添加数据
    data = bt.feeds.PandasData(
        dataname=stock_df,
        datetime=None,
        open='open',
        high='high',
        low='low',
        close='close',
        volume='volume',
        openinterest=-1
    )
    cerebro.adddata(data)

    # 10. 添加策略
    cerebro.addstrategy(strategy_class, **merged_strategy_params)

    # 11. 设置初始资金
    cerebro.broker.setcash(cash)

    # 12. 设置手续费
    cerebro.broker.setcommission(commission=commission)

    # 13. 添加分析器
    cerebro.addanalyzer(PortfolioValueAnalyzer, _name='portfolio_value')
    cerebro.addanalyzer(TradeAnalyzer, _name='trade_analyzer', original_price_map=original_price_map)

    # 14. 运行回测
    logger.info(f"[Backtest] Running cerebro.run() for {stock_code}...")
    strats = cerebro.run()
    strat = strats[0]
    logger.info(f"[Backtest] Cerebro.run() completed")

    # 15. 获取分析器结果
    portfolio_analysis = strat.analyzers.portfolio_value.get_analysis()
    trade_analysis = strat.analyzers.trade_analyzer.get_analysis()

    # 16. 计算策略健康指标（传入日期以正确计算年化收益率）
    returns = calculate_backtest_returns(
        cash,
        cerebro.broker.getvalue(),
        portfolio_analysis['portfolio_values'],
        start_date=start_date,
        end_date=end
    )
    health_metrics = strategy_health_check(returns)

    # 17. 获取基准数据
    # 如果没有指定 benchmark，默认使用当前股票的"买入持有"策略作为基准
    # 这样可以对比"策略交易" vs "简单买入持有"
    benchmark_comparison = None
    benchmark_symbol = benchmark if benchmark else stock_code  # 默认使用当前股票

    try:
        # 使用 load_bars 获取基准数据
        benchmark_df = db.load_bars(
            symbol=benchmark_symbol,
            start=start_date,
            end=end,
            interval='1d'
        )
        if benchmark_df is not None and not benchmark_df.empty:
            # 计算买入持有策略的收益
            # 假设在开始日期用全部资金买入，一直持有到结束
            initial_price = benchmark_df['close'].iloc[0]
            shares = cash / initial_price  # 可买入的股数
            benchmark_portfolio_values = (shares * benchmark_df['close']).tolist()
            final_benchmark_value = shares * benchmark_df['close'].iloc[-1]

            benchmark_returns = calculate_backtest_returns(
                cash,  # 初始资金
                final_benchmark_value,  # 最终价值
                benchmark_portfolio_values,  # 每日持仓价值
                start_date=start_date,
                end_date=end
            )
            benchmark_metrics = strategy_health_check(benchmark_returns)

            # 确定基准名称
            benchmark_name = stock_code if benchmark_symbol == stock_code else benchmark
            benchmark_type = '买入持有' if benchmark_symbol == stock_code else '指数对比'

            benchmark_comparison = {
                'benchmark': benchmark_symbol,
                'benchmark_name': benchmark_name,
                'benchmark_type': benchmark_type,
                'benchmark_metrics': benchmark_metrics,
                'excess_return': health_metrics['total_return'] - benchmark_metrics['total_return'],  # 使用总收益率
                'excess_sharpe': health_metrics['sharpe_ratio'] - benchmark_metrics['sharpe_ratio']
            }
            logger.info(f"[Backtest] Benchmark comparison: {benchmark_type} ({benchmark_symbol})")
    except Exception as e:
        logger.warning(f"获取基准数据失败: {e}")

    # 18. 构建返回结果
    # 序列化交易记录，将日期对象转换为字符串
    trades_list = trade_analysis.get('trades', [])
    serialized_trades = [_serialize_trade(t) for t in trades_list]

    # 获取结束日期（从索引中获取，因为datetime已经被设置为索引）
    end_date_str = end or stock_df.index[-1].strftime('%Y-%m-%d')

    result = {
        'basic_info': {
            'stock': stock_code,
            'start_date': start_date,
            'end_date': end_date_str,
            'initial_cash': cash,
            'final_value': cerebro.broker.getvalue(),
            'total_return': (cerebro.broker.getvalue() - cash) / cash,
            'commission': commission
        },
        'strategy_info': {
            'strategy': strategy_type,
            'strategy_params': merged_strategy_params,
            'strategy_name': get_strategy_description(strategy_type)['name']
        },
        'trade_stats': {
            'total_trades': trade_analysis.get('total_trades', 0),
            'winning_trades': trade_analysis.get('winning_trades', 0),
            'losing_trades': trade_analysis.get('losing_trades', 0),
            'win_rate': trade_analysis.get('win_rate', 0.0),
            'total_profit': trade_analysis.get('total_profit', 0.0),
            'total_loss': trade_analysis.get('total_loss', 0.0),
            'profit_factor': trade_analysis.get('profit_factor', 0.0)
        },
        'trades': serialized_trades,
        'health_metrics': health_metrics,
        'benchmark_comparison': benchmark_comparison
    }

    return result


def run_batch_backtest(params_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    批量执行回测（支持多策略）

    Args:
        params_list: 回测参数列表

    Returns:
        回测结果列表
    """
    results = []

    for i, params in enumerate(params_list):
        try:
            result = run_single_backtest(params)
            results.append({
                'index': i,
                'status': 'success',
                'result': result
            })
        except Exception as e:
            results.append({
                'index': i,
                'status': 'failed',
                'error': str(e)
            })

    return results


def get_supported_strategies_api() -> Dict[str, Any]:
    """
    获取支持的策略列表（API接口用）

    Returns:
        策略列表信息
    """
    strategies = get_supported_strategies()

    return {
        'success': True,
        'strategies': [
            {
                'strategy_type': strategy_type,
                'name': info['description']['name'],
                'description': info['description']['description'],
                'params_schema': info['schema']
            }
            for strategy_type, info in strategies.items()
        ]
    }


def get_backtest_result(task_id: str) -> Dict[str, Any]:
    """
    获取回测任务结果

    Args:
        task_id: 任务ID

    Returns:
        任务结果
    """
    from web.services.task_service import get_task_manager
    tm = get_task_manager()
    task = tm.get_task(task_id)

    if not task:
        return {'error': '任务不存在'}, 404

    status = task.get('status')

    # 任务还在运行中
    if status in ('pending', 'running'):
        return {
            'error': '任务尚未完成',
            'task_id': task_id,
            'status': status
        }, 202

    # 任务失败
    if status == 'failed':
        return {
            'success': False,
            'task_id': task_id,
            'status': status,
            'error': task.get('message', '任务执行失败'),
            'params': task.get('params')
        }, 200

    # 任务已完成但没有结果
    result = task.get('result')
    if status == 'completed' and not result:
        return {'error': '任务已完成但无结果'}, 500

    return {
        'success': True,
        'task_id': task_id,
        'status': status,
        'result': result
    }
