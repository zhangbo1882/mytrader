#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
价格突破策略 V2 - 鲁棒性增强版

策略改进：
1. 动态阈值：根据股票自身市场状态（牛熊市）自适应调整买入/止盈/止损阈值
2. 订单执行改进：
   - 买入限价使用开盘价 + 缓冲区（更现实）
   - 止损使用市价单确保成交
   - 添加滑点模拟
3. 鲁棒性增强：
   - 动态ROC阈值（基于ATR）
   - 成交量确认（量价配合）
   - 波动率分位数判断
4. 详细日志：
   - 每次状态检测的详细评分
   - 完整的交易记录
   - 回测总结报告

参数说明：
- base_buy_threshold: 基础买入阈值（会根据市场状态调整）
- base_sell_threshold: 基础止盈阈值（会根据市场状态调整）
- base_stop_loss_threshold: 基础止损阈值（会根据市场状态调整）
- buy_price_buffer: 买入价格缓冲（限价高于开盘价的幅度）
- sell_price_buffer: 卖出价格缓冲（限价低于收盘价的幅度）
- use_market_order_for_stop: 止损是否使用市价单
- enable_adaptive_thresholds: 是否启用自适应阈值
- enable_fundamental_filter: 是否启用基本面过滤
- state_detection_interval: 市场状态检测间隔（天）
"""

import os
import logging
from datetime import date, datetime
import backtrader as bt

from src.market.stock_state_detector import StockStateDetector
from src.market.fundamental_filter import FundamentalFilter
from src.market.market_regime import (
    MarketRegime,
    REGIME_PARAMS,
    ORDER_EXECUTION_CONFIG,
    FUNDAMENTAL_FILTER_CONFIG,
    get_regime_params
)

logger = logging.getLogger(__name__)


class PriceBreakoutStrategyV2(bt.Strategy):
    """
    价格突破策略 V2 - 鲁棒性增强版

    Parameters
    ----------
    base_buy_threshold : float
        基础买入阈值（百分比），默认1.0
    base_sell_threshold : float
        基础止盈阈值（百分比），默认5.0
    base_stop_loss_threshold : float
        基础止损阈值（百分比），默认10.0
    buy_price_buffer : float
        买入价格缓冲（小数），默认0.003（0.3%）
    sell_price_buffer : float
        卖出价格缓冲（小数），默认0.002（0.2%）
    use_market_order_for_stop : bool
        止损是否使用市价单，默认True
    enable_adaptive_thresholds : bool
        是否启用自适应阈值，默认True
    enable_fundamental_filter : bool
        是否启用基本面过滤，默认False
    state_detection_interval : int
        市场状态检测间隔（天），默认5
    enable_blacklist : bool
        是否启用交易黑名单，默认False（黑名单无实际意义，会导致每次回测只有一次交易）
    commission : float
        手续费率，默认0.02%
    slippage_buy : float
        买入滑点，默认0.002（0.2%）
    slippage_sell : float
        卖出滑点，默认0.002（0.2%）
    """

    params = (
        # Base thresholds (will be adjusted based on market state)
        ("base_buy_threshold", 1.0),
        ("base_sell_threshold", 5.0),
        ("base_stop_loss_threshold", 10.0),

        # Order execution parameters
        ("buy_price_buffer", 0.003),
        ("sell_price_buffer", 0.002),
        ("use_market_order_for_stop", True),

        # Feature flags
        ("enable_adaptive_thresholds", True),
        ("enable_fundamental_filter", False),
        ("state_detection_interval", 5),
        ("enable_blacklist", False),

        # Commission and slippage
        ("commission", 0.0002),  # 0.02%
        ("slippage_buy", 0.002),  # 0.2%
        ("slippage_sell", 0.002),  # 0.2%

        # Warmup period
        ("backtest_start_date", None),  # 回测实际开始日期（用于跳过预热期）
    )

    def __init__(self):
        """初始化函数"""
        # Data references
        self.data_open = self.datas[0].open
        self.data_high = self.datas[0].high
        self.data_low = self.datas[0].low
        self.data_close = self.datas[0].close

        # Order and position tracking
        self.order = None
        self.buy_price = None
        self.buy_date = None
        self.buy_commission = 0
        self.pending_buy_price = None
        self.sell_reason = None

        # Trade blacklist (each stock trades only once)
        self.traded_stocks = set()

        # Failed order tracking
        self.failed_orders = []

        # Trade log
        self.trade_log = []

        # Market state detection
        self.stock_state = None
        self.state_detector = StockStateDetector(lookback_days=180)  # Reduced from 240 to 180
        self.state_detection_counter = 0
        self.state_history = []  # Track state history

        # Fundamental filter
        if self.params.enable_fundamental_filter:
            self.fundamental_filter = FundamentalFilter()
        else:
            self.fundamental_filter = None

        # Initial cash for summary
        self.initial_cash = self.broker.getvalue()

    def notify_order(self, order):
        """
        订单状态通知

        Parameters
        ----------
        order : Order
            订单对象
        """
        if order.status in [order.Submitted, order.Accepted]:
            return

        current_date = self.datas[0].datetime.date(0)
        open_price = self.data_open[0]
        high_price = self.data_high[0]
        low_price = self.data_low[0]
        close_price = self.data_close[0]

        if order.status in [order.Completed]:
            if order.isbuy():
                # Record buy price
                self.buy_price = order.executed.price
                self.buy_date = current_date
                self.buy_commission = order.executed.comm
                self.pending_buy_price = None

                # Calculate slippage
                slippage_pct = (order.executed.price / self.pending_buy_price - 1) * 100 if self.pending_buy_price else 0

                # Get current scores from stock state
                scores = {}
                if self.stock_state:
                    scores = {
                        'trend': self.stock_state.ma_trend,
                        'momentum': self.stock_state.momentum,
                        'position': self.stock_state.position_ratio * 20,
                        'volume': self.stock_state.volume_confirm,
                        'volatility': self.stock_state.volatility,
                    }

                # Record trade log
                trade_info = {
                    'date': current_date,
                    'action': '买入',
                    'trigger_price': self.data_low[0],
                    'limit_price': self.pending_buy_price,
                    'executed_price': order.executed.price,
                    'size': order.executed.size,
                    'commission': order.executed.comm,
                    'slippage_pct': slippage_pct,
                    'market_regime': self.stock_state.regime.value if self.stock_state else 'N/A',
                    'market_confidence': self.stock_state.confidence if self.stock_state else 0,
                    'scores': scores,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price
                }
                self.trade_log.append(trade_info)

                # Log buy execution
                self._log_buy_execution(order, current_date, open_price, high_price, low_price, close_price, slippage_pct)

            elif order.issell():
                # Calculate profit
                profit_pct = 0.0
                if self.buy_price:
                    profit_pct = (order.executed.price / self.buy_price - 1) * 100

                # Add to blacklist if enabled
                stock_code = self.datas[0]._name
                if self.params.enable_blacklist:
                    self.traded_stocks.add(stock_code)
                    blacklist_status = "已加入黑名单"
                else:
                    blacklist_status = "允许再次交易"

                # Record trade log
                holding_days = (current_date - self.buy_date).days if self.buy_date else 0
                trade_info = {
                    'date': current_date,
                    'action': '卖出',
                    'sell_reason': self.sell_reason,
                    'buy_price': self.buy_price,
                    'limit_price': order.created.price if order.created else order.executed.price,
                    'executed_price': order.executed.price,
                    'size': order.executed.size,
                    'commission': order.executed.comm,
                    'profit_pct': profit_pct,
                    'profit_amount': order.executed.value - (self.buy_price * order.executed.size if self.buy_price else 0),
                    'holding_days': holding_days,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price
                }
                self.trade_log.append(trade_info)

                # Log sell execution
                reason_desc = self.sell_reason if self.sell_reason else '卖出'
                self._log_sell_execution(order, current_date, open_price, high_price, low_price, close_price,
                                        reason_desc, profit_pct, blacklist_status)

                # Reset
                self.buy_price = None
                self.buy_date = None
                self.sell_reason = None

        elif order.status in [order.Canceled]:
            order_type = "买入" if order.isbuy() else "卖出"
            logger.warning(f"【订单已取消】{current_date} {order_type}")
            logger.warning(f"  订单状态: {order.getstatusname()}")
            if order.created and order.created.price is not None:
                logger.warning(f"  订单价格: {order.created.price:.2f}, 数量: {order.created.size}")

            self.failed_orders.append({
                'date': current_date,
                'status': 'Canceled',
                'order_type': order_type,
                'price': order.created.price if order.created else None,
                'size': order.created.size if order.created else None,
                'reason': '订单已取消'
            })

        elif order.status in [order.Margin, order.Rejected]:
            order_type = "买入" if order.isbuy() else "卖出"
            logger.error(f"【订单失败】{current_date} {order_type}")
            logger.error(f"  订单状态: {order.getstatusname()}")
            if order.created and order.created.price is not None:
                logger.error(f"  订单价格: {order.created.price:.2f}, 数量: {order.created.size}")

            self.failed_orders.append({
                'date': current_date,
                'status': order.getstatusname(),
                'order_type': order_type,
                'price': order.created.price if order.created else None,
                'size': order.created.size if order.created else None,
                'reason': '保证金不足或订单被拒绝'
            })

        self.order = None

    def next(self):
        """
        执行交易逻辑

        改进点：
        1. 动态阈值：根据市场状态调整买卖参数
        2. 买入限价：使用开盘价 + 缓冲区（更现实）
        3. 止损订单：可选使用市价单确保成交
        """
        # 跳过预热期的数据（如果指定了回测开始日期）
        if self.params.backtest_start_date:
            current_date = self.datas[0].datetime.date(0)
            start_date = datetime.strptime(self.params.backtest_start_date, '%Y-%m-%d').date()
            if current_date < start_date:
                return  # 跳过预热期，不执行交易

        # Cancel pending buy orders
        if self.order and self.order.isbuy():
            self.cancel(self.order)
            self.order = None
            self.pending_buy_price = None

        # Detect market state periodically
        stock_code = self.datas[0]._name
        current_date = self.datas[0].datetime.date(0)

        if self.state_detection_counter % self.params.state_detection_interval == 0:
            self.stock_state = self.state_detector.detect_state(
                self.datas[0],
                stock_code=stock_code,
                verbose=True
            )
            self.state_history.append(self.stock_state)
        self.state_detection_counter += 1

        # Get adaptive thresholds
        if self.params.enable_adaptive_thresholds and self.stock_state:
            regime_key = self.stock_state.regime.value
            # Get current regime params (thread-safe for optimization)
            regime_params = get_regime_params()
            # Apply multipliers to base thresholds
            buy_threshold = self.params.base_buy_threshold * regime_params[regime_key]['buy_threshold_multiplier']
            sell_threshold = self.params.base_sell_threshold * regime_params[regime_key]['sell_threshold_multiplier']
            stop_loss_threshold = self.params.base_stop_loss_threshold * regime_params[regime_key]['stop_loss_multiplier']
        else:
            buy_threshold = self.params.base_buy_threshold
            sell_threshold = self.params.base_sell_threshold
            stop_loss_threshold = self.params.base_stop_loss_threshold

        # Buy condition
        buy_threshold_price = self.data_open[0] * (1 - buy_threshold / 100)
        buy_condition_met = self.data_low[0] < buy_threshold_price

        if not self.position:
            # No position

            # Check blacklist
            if self.params.enable_blacklist and stock_code in self.traded_stocks:
                return

            # Check fundamental filter
            if self.params.enable_fundamental_filter and self.fundamental_filter:
                is_risky, risk_reason = self.fundamental_filter.check_event_risk(stock_code, current_date)
                if is_risky:
                    logger.info(f"【跳过买入】{stock_code} {current_date} - {risk_reason}")
                    return

            if buy_condition_met:
                # Improved buy limit price: open + buffer
                buy_limit_price = self.data_open[0] * (1 - buy_threshold / 100 + self.params.buy_price_buffer)

                # Ensure not below lower limit (down limit ~9.05%)
                lower_limit = self.data_close[-1] * 0.905
                buy_limit_price = max(buy_limit_price, lower_limit)

                # Calculate size (with commission buffer to avoid margin error)
                cash = self.broker.getcash()
                # Reserve extra 5% for commission and slippage
                commission_buffer = 1.05
                size = int(cash / buy_limit_price / commission_buffer / 100) * 100

                if size > 0:
                    self.order = self.buy(price=buy_limit_price, size=size)
                    self.pending_buy_price = buy_limit_price

                    self._log_buy_signal(current_date, stock_code, buy_threshold, buy_threshold_price,
                                        buy_limit_price, cash, size)

        else:
            # Has position - check stop loss and take profit
            if self.buy_price:
                # Stop loss condition
                stop_loss_price = self.buy_price * (1 - stop_loss_threshold / 100)
                stop_loss_condition_met = self.data_low[0] <= stop_loss_price

                # Take profit condition
                take_profit_price = self.buy_price * (1 + sell_threshold / 100)
                take_profit_condition_met = self.data_high[0] > take_profit_price

                # Determine sell reason (stop loss has priority)
                self.sell_reason = None
                if stop_loss_condition_met:
                    self.sell_reason = '止损'
                elif take_profit_condition_met:
                    self.sell_reason = '止盈'

                if self.sell_reason:
                    size = self.position.size

                    # Use market order for stop loss if enabled
                    if self.sell_reason == '止损' and self.params.use_market_order_for_stop:
                        self.order = self.sell(size=size)
                        sell_price = None  # Market order
                    else:
                        sell_price = self.data_close[0] * (1 - self.params.sell_price_buffer)
                        self.order = self.sell(price=sell_price, size=size)

                    self._log_sell_signal(current_date, stock_code, stop_loss_threshold,
                                         stop_loss_price, sell_threshold, take_profit_price,
                                         sell_price, size)

    def stop(self):
        """策略结束时输出总结"""
        # Calculate market state distribution (always, even in quiet mode)
        if self.state_history:
            bull_count = sum(1 for s in self.state_history if s.regime == MarketRegime.BULL)
            bear_count = sum(1 for s in self.state_history if s.regime == MarketRegime.BEAR)
            neutral_count = sum(1 for s in self.state_history if s.regime == MarketRegime.NEUTRAL)
            total = len(self.state_history)

            # Store for programmatic access (used by optimization system)
            self.market_state_distribution = {
                'bull': {'count': bull_count, 'percentage': bull_count/total*100 if total > 0 else 0},
                'bear': {'count': bear_count, 'percentage': bear_count/total*100 if total > 0 else 0},
                'neutral': {'count': neutral_count, 'percentage': neutral_count/total*100 if total > 0 else 0},
                'total': total
            }

        # In quiet mode, skip detailed summary (but distribution is already calculated above)
        quiet_mode = os.environ.get('STRATEGY_QUIET_MODE', '0') == '1'
        if quiet_mode:
            return

        logger.info("")
        logger.info("=" * 80)
        logger.info(" " * 20 + "策略回测总结报告 V2")
        logger.info("=" * 80)

        # Basic info
        logger.info(f"\n【基础信息】")
        logger.info(f"  股票代码: {self.datas[0]._name}")
        # 显示用户指定的回测周期
        if self.params.backtest_start_date:
            logger.info(f"  回测周期: {self.params.backtest_start_date} - {self.datas[0].datetime.date(0)}")
        else:
            logger.info(f"  回测周期: {self.datas[0].datetime.date(-1)} - {self.datas[0].datetime.date(0)}")
        logger.info(f"  初始资金: {self.initial_cash:.2f}")
        logger.info(f"  最终资金: {self.broker.getvalue():.2f}")
        logger.info(f"  总收益率: {(self.broker.getvalue() / self.initial_cash - 1) * 100:.2f}%")

        # Trade statistics
        logger.info(f"\n【交易统计】")
        buy_trades = [t for t in self.trade_log if t['action'] == '买入']
        sell_trades = [t for t in self.trade_log if t['action'] == '卖出']
        logger.info(f"  买入次数: {len(buy_trades)}")
        logger.info(f"  卖出次数: {len(sell_trades)}")
        logger.info(f"  总交易次数: {len(buy_trades) + len(sell_trades)}")

        # Profit/Loss statistics
        if sell_trades:
            profitable = [t for t in sell_trades if t.get('profit_pct', 0) > 0]
            loss_trades = [t for t in sell_trades if t.get('profit_pct', 0) <= 0]

            logger.info(f"\n【盈亏统计】")
            logger.info(f"  盈利交易: {len(profitable)}次")
            logger.info(f"  亏损交易: {len(loss_trades)}次")
            if sell_trades:
                logger.info(f"  胜率: {len(profitable) / len(sell_trades) * 100:.2f}%")

            if profitable:
                avg_profit = sum(t['profit_pct'] for t in profitable) / len(profitable)
                best = max(profitable, key=lambda t: t['profit_pct'])
                logger.info(f"  平均盈利: {avg_profit:.2f}%")
                logger.info(f"  最佳交易: {best['date']} (+{best['profit_pct']:.2f}%)")

            if loss_trades:
                avg_loss = sum(t['profit_pct'] for t in loss_trades) / len(loss_trades)
                worst = min(loss_trades, key=lambda t: t['profit_pct'])
                logger.info(f"  平均亏损: {avg_loss:.2f}%")
                logger.info(f"  最差交易: {worst['date']} ({worst['profit_pct']:.2f}%)")

            if loss_trades and profitable:
                profit_factor = abs(sum(t['profit_pct'] for t in profitable) / sum(t['profit_pct'] for t in loss_trades))
                logger.info(f"  盈亏比: {profit_factor:.2f}")

        # Market state statistics (already calculated at the beginning of stop())
        if self.state_history:
            logger.info(f"\n【市场状态分布】")
            dist = self.market_state_distribution
            logger.info(f"  牛市: {dist['bull']['count']}天 ({dist['bull']['percentage']:.1f}%)")
            logger.info(f"  熊市: {dist['bear']['count']}天 ({dist['bear']['percentage']:.1f}%)")
            logger.info(f"  震荡: {dist['neutral']['count']}天 ({dist['neutral']['percentage']:.1f}%)")

        # Failed orders
        if self.failed_orders:
            logger.info(f"\n【订单失败统计】")
            logger.info(f"  失败次数: {len(self.failed_orders)}")
            by_status = {}
            for order in self.failed_orders:
                status = order['status']
                by_status[status] = by_status.get(status, 0) + 1
            for status, count in by_status.items():
                logger.info(f"  {status}: {count}次")

        # Blacklist
        if self.traded_stocks:
            status = "启用" if self.params.enable_blacklist else "未启用"
            logger.info(f"\n【交易黑名单】({status})")
            logger.info(f"  已交易股票: {len(self.traded_stocks)}支")
            for stock in sorted(self.traded_stocks):
                logger.info(f"    - {stock}")

        logger.info("=" * 80)

    def _log_buy_signal(self, current_date, stock_code, buy_threshold, buy_threshold_price,
                       buy_limit_price, cash, size):
        """Log buy signal"""
        if os.environ.get('STRATEGY_QUIET_MODE', '0') == '1':
            return

        logger.info("=" * 60)
        logger.info(f"【买入信号】{current_date} - {stock_code}")
        logger.info("-" * 60)
        logger.info(f"触发条件:")
        logger.info(f"  开盘价: {self.data_open[0]:.2f}")
        logger.info(f"  买入阈值: {buy_threshold:.2f}%")
        logger.info(f"  阈值价格: {buy_threshold_price:.2f}")
        logger.info(f"  当日最低价: {self.data_low[0]:.2f}")
        logger.info(f"  条件满足: {self.data_low[0]:.2f} < {buy_threshold_price:.2f} ✓")

        if self.stock_state:
            logger.info(f"市场环境:")
            logger.info(f"  市场状态: {self.stock_state.regime.value}")
            logger.info(f"  置信度: {self.stock_state.confidence:.2f}")
            logger.info(f"  使用参数: buy_threshold={buy_threshold:.2f}% (自适应)")

        logger.info(f"订单信息:")
        logger.info(f"  限价: {buy_limit_price:.2f}")
        logger.info(f"  数量: {size}股 ({size//100}手)")
        logger.info(f"  可用资金: {cash:.2f}")
        logger.info(f"  预计成交额: {buy_limit_price * size:.2f}")
        logger.info("=" * 60)

    def _log_buy_execution(self, order, current_date, open_price, high_price, low_price, close_price, slippage_pct):
        """Log buy execution"""
        if os.environ.get('STRATEGY_QUIET_MODE', '0') == '1':
            return

        logger.info("=" * 60)
        logger.info(f"【买入成交】{current_date} - {self.datas[0]._name}")
        logger.info("-" * 60)
        logger.info(f"当日行情:")
        logger.info(f"  开盘: {open_price:.2f}")
        logger.info(f"  最高: {high_price:.2f}")
        logger.info(f"  最低: {low_price:.2f}")
        logger.info(f"  收盘: {close_price:.2f}")
        logger.info(f"成交信息:")
        logger.info(f"  限价: {self.pending_buy_price:.2f}" if self.pending_buy_price is not None else f"  限价: N/A")
        logger.info(f"  实际成交价: {order.executed.price:.2f}")
        logger.info(f"  成交数量: {order.executed.size}股")
        logger.info(f"  成交金额: {order.executed.value:.2f}")
        logger.info(f"  手续费: {order.executed.comm:.2f}")
        logger.info(f"  滑点: {slippage_pct:.3f}%" if slippage_pct is not None else f"  滑点: N/A")
        logger.info("=" * 60)

    def _log_sell_signal(self, current_date, stock_code, stop_loss_threshold, stop_loss_price,
                        sell_threshold, take_profit_price, sell_price, size):
        """Log sell signal"""
        if os.environ.get('STRATEGY_QUIET_MODE', '0') == '1':
            return

        logger.info("=" * 60)
        logger.info(f"【{self.sell_reason}信号】{current_date} - {stock_code}")
        logger.info("-" * 60)
        logger.info(f"持仓信息:")
        logger.info(f"  买入价: {self.buy_price:.2f}")
        logger.info(f"  当前价: {self.data_close[0]:.2f}")
        logger.info(f"  持仓数量: {size}股")

        profit_pct = (self.data_close[0] / self.buy_price - 1) * 100 if self.buy_price else 0
        logger.info(f"  持有收益: {profit_pct:.2f}%")

        logger.info(f"触发条件:")
        if self.sell_reason == '止损':
            logger.info(f"  止损阈值: {stop_loss_threshold:.2f}%")
            logger.info(f"  止损价格: {stop_loss_price:.2f}")
            logger.info(f"  当日最低价: {self.data_low[0]:.2f}")
            logger.info(f"  条件满足: {self.data_low[0]:.2f} <= {stop_loss_price:.2f} ✓")
            logger.info(f"  订单类型: 市价单 (确保成交)" if self.params.use_market_order_for_stop else f"  订单类型: 限价单 {sell_price:.2f}")
        else:
            logger.info(f"  止盈阈值: {sell_threshold:.2f}%")
            logger.info(f"  止盈价格: {take_profit_price:.2f}")
            logger.info(f"  当日最高价: {self.data_high[0]:.2f}")
            logger.info(f"  条件满足: {self.data_high[0]:.2f} > {take_profit_price:.2f} ✓")
            logger.info(f"  订单类型: 限价单 {sell_price:.2f}" if sell_price else "  订单类型: 市价单")
        logger.info("=" * 60)

    def _log_sell_execution(self, order, current_date, open_price, high_price, low_price, close_price,
                           reason_desc, profit_pct, blacklist_status):
        """Log sell execution"""
        if os.environ.get('STRATEGY_QUIET_MODE', '0') == '1':
            return

        logger.info("=" * 60)
        logger.info(f"【{reason_desc}成交】{current_date} - {self.datas[0]._name}")
        logger.info("-" * 60)
        logger.info(f"当日行情:")
        logger.info(f"  开盘: {open_price:.2f}")
        logger.info(f"  最高: {high_price:.2f}")
        logger.info(f"  最低: {low_price:.2f}")
        logger.info(f"  收盘: {close_price:.2f}")
        logger.info(f"交易统计:")
        logger.info(f"  买入价格: {self.buy_price:.2f}")
        logger.info(f"  卖出价格: {order.executed.price:.2f}")
        logger.info(f"  卖出数量: {order.executed.size}股")
        logger.info(f"  持有收益: {profit_pct:.2f}%")
        profit_amount = order.executed.value - (self.buy_price * order.executed.size if self.buy_price else 0)
        logger.info(f"  收益金额: {profit_amount:.2f}")
        logger.info(f"  手续费(买入+卖出): {self.buy_commission + order.executed.comm:.2f}")
        logger.info(f"  状态: {blacklist_status}")
        logger.info("=" * 60)
