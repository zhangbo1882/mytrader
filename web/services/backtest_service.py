#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
回测服务层

提供回测功能的核心服务，支持多策略类型
"""
import backtrader as bt
import pandas as pd
from datetime import datetime, date
from typing import Dict, Any, Optional, List
import logging

from src.strategies.base.registry import (
    get_strategy_class,
    validate_strategy_params,
    get_strategy_description,
    get_default_params,
    get_supported_strategies
)
from src.strategies.base.analyzers import PortfolioValueAnalyzer, TradeAnalyzer, calculate_strategy_metrics
from src.strategies.base.metrics import strategy_health_check, calculate_backtest_returns
from src.backtrader.slippage import set_percentage_slippage
from src.utils.stock_lookup import search_stocks

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
    serialized = {}
    for key, value in trade.items():
        if isinstance(value, date):
            serialized[key] = value.strftime('%Y-%m-%d')
        elif hasattr(value, 'strftime'):  # Handle datetime objects
            serialized[key] = value.strftime('%Y-%m-%d')
        else:
            serialized[key] = value
    return serialized


def _serialize_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    递归序列化结果字典，将所有日期对象转换为字符串

    Args:
        result: 原始结果字典

    Returns:
        序列化后的结果字典
    """
    def serialize_value(value):
        if isinstance(value, dict):
            return {k: serialize_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [serialize_value(v) for v in value]
        elif isinstance(value, (date,)):
            return value.strftime('%Y-%m-%d')
        elif hasattr(value, 'strftime'):  # Handle datetime, Timestamp, etc.
            return value.strftime('%Y-%m-%d')
        else:
            return value

    return serialize_value(result)


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
                - interval: 时间周期（可选，默认'1d'，支持'1d', '5m', '15m', '30m', '60m'）
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
    import os

    # Check for quiet mode (used during optimization to reduce log noise)
    quiet_mode = os.environ.get('BACKTEST_QUIET_MODE', '0') == '1'

    # Set quiet mode for strategy logs
    if quiet_mode:
        os.environ['STRATEGY_QUIET_MODE'] = '1'

    # 1. 提取回测共有参数
    stock_code = params.get('stock')
    start_date = params.get('start_date')
    end_date = params.get('end_date')
    interval = params.get('interval', '1d')  # 默认日线
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

    # 7. 获取股票数据（从 DuckDB）
    end = end_date or datetime.now().strftime('%Y-%m-%d')

    # 打印实际使用的策略参数
    logger.info(f"[Backtest] 策略参数详情:")
    logger.info(f"  策略类型: {strategy_type}")
    logger.info(f"  策略名称: {get_strategy_description(strategy_type)['name']}")
    logger.info(f"  用户传入参数: {strategy_params}")
    logger.info(f"  策略默认参数: {default_params}")
    logger.info(f"  实际使用参数: {merged_strategy_params}")
    logger.info(f"  回测共有参数: stock={stock_code}, start={start_date}, end={end}, cash={cash}, commission={commission}")

    # 打印牛熊市自适应参数（适用于 price_breakout_v2 策略）
    if strategy_type in ['price_breakout', 'price_breakout_v2']:
        try:
            from src.market.market_regime import get_regime_params
            regime_params = get_regime_params()
            logger.info(f"  牛熊市自适应参数:")
            for regime, params in regime_params.items():
                logger.info(f"    {regime}: buy_mult={params['buy_threshold_multiplier']}, "
                           f"sell_mult={params['sell_threshold_multiplier']}, "
                           f"stop_mult={params['stop_loss_multiplier']}")
        except Exception as e:
            logger.warning(f"  获取牛熊市参数失败: {e}")

    # 预热期：为市场状态分析提前加载至少250个交易日数据
    # 假设一年有250个交易日，为确保有足够数据，提前500天（约1.4年）
    # 这样可以应对股票停牌、节假日等情况，确保至少有240+个交易日
    from datetime import datetime as dt, timedelta
    start_dt = dt.strptime(start_date, '%Y-%m-%d')
    warmup_start_dt = start_dt - timedelta(days=500)  # 提前500天（约340个交易日）
    warmup_start_date = warmup_start_dt.strftime('%Y-%m-%d')

    logger.info(f"开始加载回测数据: stock={stock_code}, start={start_date}, end={end}, interval={interval}")
    logger.info(f"预热期: 从 {warmup_start_date} 加载数据以满足市场状态分析需求（至少250个交易日）")

    # 从 DuckDB 加载数据
    stock_df_qfq = None
    actual_code_used = stock_code  # 记录实际使用的代码
    code_warning_message = None  # 存储代码提示信息

    try:
        from web.services.duckdb_query_service import get_duckdb_query_service

        duckdb_service = get_duckdb_query_service()
        duckdb_results = duckdb_service.query_bars(
            symbols=[stock_code],
            start_date=warmup_start_date,  # 使用预热期开始日期
            end_date=end,
            interval=interval,
            price_type='qfq'  # 前复权 - 保持价格序列连续性，技术指标更准确
        )

        logger.info(f"DuckDB 查询结果 keys: {list(duckdb_results.keys())}")

        if stock_code in duckdb_results and duckdb_results[stock_code]:
            duckdb_data_list = duckdb_results[stock_code]
            logger.info(f"DuckDB 返回 {len(duckdb_data_list)} 条数据 for {stock_code}")

            if duckdb_data_list:
                # 检查实际返回的股票代码
                first_record = duckdb_data_list[0]
                actual_code = first_record.get('ts_code', first_record.get('stock_code', stock_code))
                actual_code_used = actual_code
                exchange = first_record.get('exchange', '')

                # 如果用户输入纯数字，但实际使用的是带后缀的代码，给出提示
                if stock_code.isdigit() and actual_code != stock_code:
                    logger.warning(f"用户输入: {stock_code}, 实际使用: {actual_code}")
                    # 搜索相似代码给出提示
                    similar_stocks = search_stocks(stock_code, limit=5, asset_type='stock')
                    if similar_stocks:
                        suggestions = []
                        for stock in similar_stocks:
                            code = stock['code']
                            name = stock['name']
                            if code == actual_code:
                                suggestions.append(f"✓ {code} ({name})")
                            else:
                                suggestions.append(f"  {code} ({name})")
                        # 将提示信息转换为字符串
                        code_warning_message = (
                            f"代码提示: 输入 '{stock_code}'，实际使用 '{actual_code}'\\n"
                            f"可选代码:\\n" + "\\n".join(suggestions)
                        )
                        logger.warning(f"股票代码提示: {code_warning_message}")

                # DuckDB 有数据
                stock_df_qfq = pd.DataFrame(duckdb_data_list)
                # 确保必需列存在
                required_cols = ['datetime', 'open', 'high', 'low', 'close', 'volume']
                if all(col in stock_df_qfq.columns for col in required_cols):
                    stock_df_qfq['datetime'] = pd.to_datetime(stock_df_qfq['datetime'])
                    logger.info(f"从 DuckDB 加载了 {len(stock_df_qfq)} 条 {interval} 数据用于 {stock_code} (实际代码: {actual_code_used})")
                else:
                    logger.warning(f"DuckDB 数据缺少必需列: {stock_df_qfq.columns.tolist()}")
                    stock_df_qfq = None
            else:
                logger.warning(f"DuckDB 返回空数据列表 for {stock_code}")
                stock_df_qfq = None
        else:
            logger.warning(f"DuckDB 查询结果中不包含 {stock_code}，keys: {list(duckdb_results.keys())}")
    except Exception as e:
        logger.error(f"DuckDB 查询失败: {e}")
        stock_df_qfq = None

    # 如果没有数据，尝试查找相似的股票代码
    if stock_df_qfq is None or stock_df_qfq.empty:
        # 搜索相似的股票代码
        similar_stocks = search_stocks(stock_code, limit=5, asset_type='stock')

        error_msg = f"数据库中没有股票 {stock_code} 在指定时间范围内的数据"
        if similar_stocks:
            # 构建提示信息
            suggestions = []
            for stock in similar_stocks:
                code = stock['code']
                name = stock['name']
                if code == stock_code:
                    # 完全匹配的代码但没有数据
                    suggestions.append(f"- {code} ({name}): 代码正确，但可能在指定时间范围内无数据")
                else:
                    # 相似代码
                    suggestions.append(f"- {code} ({name}): 相似代码，可能您想查询这个？")

            error_msg += "\n\n您是否在找以下股票：\n" + "\n".join(suggestions)
            error_msg += f"\n\n提示："
            error_msg += f"\n- A股代码通常是6位数字，如 600941（中国移动）"
            error_msg += f"\n- 港股代码通常是5位数字，如 00941.HK"
            error_msg += f"\n- 请检查输入的股票代码是否正确"

        raise ValueError(error_msg)

    # 8. 准备数据：将 datetime 设置为索引（backtrader 要求）
    stock_df = stock_df_qfq.set_index('datetime')
    stock_df = stock_df.sort_index()

    # 加载不复权数据用于交易明细显示
    stock_df_bfq = None
    try:
        # 使用actual_code_used查询，确保能找到数据
        duckdb_results_bfq = duckdb_service.query_bars(
            symbols=[actual_code_used],  # 使用实际代码（带交易所后缀）
            start_date=start_date,  # 不复权数据只需要回测期间
            end_date=end,
            interval=interval,
            price_type=''  # 空字符串表示不复权
        )
        # 检查返回的key，可能是actual_code_used或stock_code
        bfq_data = None
        if actual_code_used in duckdb_results_bfq and duckdb_results_bfq[actual_code_used]:
            bfq_data = duckdb_results_bfq[actual_code_used]
        elif stock_code in duckdb_results_bfq and duckdb_results_bfq[stock_code]:
            bfq_data = duckdb_results_bfq[stock_code]

        if bfq_data:
            stock_df_bfq = pd.DataFrame(bfq_data)
            stock_df_bfq['datetime'] = pd.to_datetime(stock_df_bfq['datetime'])
            stock_df_bfq = stock_df_bfq.set_index('datetime').sort_index()
            logger.info(f"从 DuckDB 加载了 {len(stock_df_bfq)} 条不复权数据用于 {actual_code_used}")
        else:
            logger.warning(f"不复权数据查询返回为空: keys={list(duckdb_results_bfq.keys())}")
    except Exception as e:
        logger.warning(f"加载不复权数据失败: {e}")

    # 创建不复权价格的日期映射，供分析器使用（交易明细显示实际价格）
    bfq_price_map = {}
    if stock_df_bfq is not None:
        for idx, row in stock_df_bfq.iterrows():
            date_str = idx.strftime('%Y-%m-%d')
            bfq_price_map[date_str] = {
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close']
            }
    else:
        # 如果无法加载不复权数据，使用前复权数据作为后备
        for idx, row in stock_df.iterrows():
            date_str = idx.strftime('%Y-%m-%d')
            bfq_price_map[date_str] = {
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close']
            }
        logger.warning("无法加载不复权数据，交易明细将显示前复权价格")

    # 9. 创建 Cerebro 引擎
    cerebro = bt.Cerebro()

    # 10. 添加数据
    data = bt.feeds.PandasData(
        dataname=stock_df,
        name=stock_code,  # 设置数据源名称，供策略日志显示股票代码
        datetime=None,
        open='open',
        high='high',
        low='low',
        close='close',
        volume='volume',
        openinterest=-1
    )
    cerebro.adddata(data)

    # 10. 添加策略（传递回测开始日期参数）
    # 策略可以使用这个参数来跳过预热期的交易
    merged_strategy_params_with_warmup = {
        **merged_strategy_params,
        'backtest_start_date': start_date  # 传递回测实际开始日期
    }
    cerebro.addstrategy(strategy_class, **merged_strategy_params_with_warmup)

    # 11. 设置初始资金
    cerebro.broker.setcash(cash)

    # 12. 设置手续费
    cerebro.broker.setcommission(commission=commission)

    # 12.5 设置滑点（针对 price_breakout_v2 策略）
    if strategy_type == 'price_breakout_v2':
        # Note: backtrader's built-in slippage uses same percentage for both buy and sell
        # We use the average of buy and sell slippage parameters
        slippage_buy = merged_strategy_params.get('slippage_buy', 0.002)
        slippage_sell = merged_strategy_params.get('slippage_sell', 0.002)
        slippage_avg = (slippage_buy + slippage_sell) / 2
        set_percentage_slippage(cerebro, slip_perc=slippage_avg)
        logger.info(f"[Backtest] Slippage enabled for {strategy_type}: avg={slippage_avg:.4f} (buy={slippage_buy}, sell={slippage_sell})")

    # 13. 添加分析器
    cerebro.addanalyzer(PortfolioValueAnalyzer, _name='portfolio_value')
    cerebro.addanalyzer(TradeAnalyzer, _name='trade_analyzer', price_map=bfq_price_map)  # 使用不复权价格用于交易明细

    # 14. 运行回测
    logger.info(f"[Backtest] Running cerebro.run() for {stock_code}...")
    logger.info(f"[Backtest] 执行策略: {strategy_type}, 股票: {stock_code}, 日期范围: {start_date} ~ {end}")
    strats = cerebro.run()
    strat = strats[0]
    logger.info(f"[Backtest] Cerebro.run() completed")
    logger.info(f"[Backtest] 回测结果: 初始资金={cash:.2f}, 最终价值={cerebro.broker.getvalue():.2f}, 收益率={((cerebro.broker.getvalue() - cash) / cash * 100):.2f}%")

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
        # 从 DuckDB 获取基准数据
        benchmark_df = None

        try:
            from web.services.duckdb_query_service import get_duckdb_query_service
            duckdb_service = get_duckdb_query_service()
            duckdb_results = duckdb_service.query_bars(
                symbols=[benchmark_symbol],
                start_date=start_date,
                end_date=end,
                interval='1d',
                price_type='qfq'  # 前复权 - 与策略回测保持一致
            )

            if benchmark_symbol in duckdb_results and duckdb_results[benchmark_symbol]:
                benchmark_df = pd.DataFrame(duckdb_results[benchmark_symbol])
                benchmark_df['datetime'] = pd.to_datetime(benchmark_df['datetime'])
                logger.info(f"从 DuckDB 加载了基准数据 {benchmark_symbol}")
        except Exception as duckdb_err:
            logger.warning(f"DuckDB 加载基准数据失败: {duckdb_err}")

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
            'actual_code': actual_code_used,  # 实际查询的代码（可能是 00941.HK）
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
        'benchmark_comparison': benchmark_comparison,
        'code_warning': code_warning_message  # 股票代码提示信息（如果有）
    }

    # Add market state distribution if available (for price_breakout strategy)
    if hasattr(strat, 'market_state_distribution'):
        result['market_state_distribution'] = strat.market_state_distribution

    # Serialize the entire result to ensure all dates are strings
    result = _serialize_result(result)

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
