#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
牛熊市转换策略

策略逻辑：
- 从熊市或震荡市转入牛市 → 触发买入信号
- 持有期间，只要市场状态保持牛市或震荡市，继续持有
- 进入熊市 → 卖出信号

与 price_breakout 策略的区别：
- price_breakout: 基于价格突破阈值，市场状态仅用于调整参数
- market_regime_switch: 直接基于市场状态转换作为买卖信号
"""

import os
import logging
from datetime import datetime
from collections import deque
import backtrader as bt

from src.market.stock_state_detector import StockStateDetector
from src.market.market_regime import MarketRegime

logger = logging.getLogger(__name__)


class MarketRegimeSwitchStrategy(bt.Strategy):
    """
    牛熊市转换策略

    核心逻辑：
    1. 使用 StockStateDetector 检测市场状态
    2. 追踪市场状态历史，识别状态转换
    3. 从熊市/震荡市 → 牛市：买入
    4. 从牛市/震荡市 → 熊市：卖出
    5. 使用确认机制避免频繁交易
    """

    params = (
        # 市场状态检测配置
        ("lookback_days", 60),              # 市场状态检测回看天数（已废弃，请使用 cycle）
        ("cycle", "medium"),                # 周期类型: short/medium/long

        # 转换确认参数
        ("bull_confirm_days", 2),           # 牛市确认天数（连续N天牛市才买入）
        ("bear_confirm_days", 1),           # 熊市确认天数（连续N天熊市才卖出）

        # 订单执行参数
        ("buy_price_buffer", 0.003),        # 买入价格缓冲（0.3%）
        ("use_market_order_for_sell", True),# 卖出时是否使用市价单

        # 回测配置
        ("backtest_start_date", None),      # 回测实际开始日期（跳过预热期）
        ("bfq_price_map", None),            # 不复权价格映射（由回测服务传入）
    )

    def __init__(self):
        """初始化函数"""
        # Data references
        self.data_close = self.datas[0].close
        self.data_open = self.datas[0].open
        self.data_high = self.datas[0].high
        self.data_low = self.datas[0].low

        # Order tracking
        self.order = None
        self.buy_price = None
        self.buy_date = None
        self.buy_commission = 0

        # Market state detection
        self.state_detector = StockStateDetector(cycle=self.params.cycle)
        self.current_state = None
        self.prev_state = None

        # State history for confirmation
        self.state_history = deque(maxlen=max(
            self.params.bull_confirm_days,
            self.params.bear_confirm_days
        ) + 2)

        # Trade log
        self.trade_log = []
        self.failed_orders = []
        self.state_change_log = []  # 记录状态转换历史

        # Initial cash
        self.initial_cash = self.broker.getvalue()

        # 买入信号标志（避免重复买入）
        self.buy_signal_triggered = False

    def notify_order(self, order):
        """订单状态通知"""
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            current_date = self.datas[0].datetime.date(0)
            open_price = self.datas[0].open[0]
            high_price = self.datas[0].high[0]
            low_price = self.datas[0].low[0]
            close_price = self.datas[0].close[0]

            if order.isbuy():
                self.buy_price = order.executed.price
                self.buy_date = current_date
                self.buy_commission = order.executed.comm
                self.buy_signal_triggered = False  # 重置标志

                logger.info(f"【买入】 {current_date}")
                logger.info(f"  当日价格: 开盘={open_price:.2f}, 最高={high_price:.2f}, 最低={low_price:.2f}, 收盘={close_price:.2f}")
                logger.info(f"  执行价格: {order.executed.price:.2f}, 数量: {order.executed.size}, 手续费: {order.executed.comm:.2f}")

                self.trade_log.append({
                    'type': 'buy',
                    'date': current_date,
                    'price': order.executed.price,
                    'size': order.executed.size,
                    'commission': order.executed.comm,
                })

            elif order.issell():
                sell_price = order.executed.price
                pnl = (sell_price - self.buy_price) * abs(order.executed.size) if self.buy_price else 0
                pnl_pct = (sell_price / self.buy_price - 1) * 100 if self.buy_price else 0

                logger.info(f"【卖出】 {current_date}")
                logger.info(f"  当日价格: 开盘={open_price:.2f}, 最高={high_price:.2f}, 最低={low_price:.2f}, 收盘={close_price:.2f}")
                logger.info(f"  执行价格: {sell_price:.2f}, 数量: {abs(order.executed.size)}, 手续费: {order.executed.comm:.2f}")
                logger.info(f"  盈亏: {pnl:.2f} ({pnl_pct:.2f}%)")

                self.trade_log.append({
                    'type': 'sell',
                    'date': current_date,
                    'price': sell_price,
                    'size': abs(order.executed.size),
                    'commission': order.executed.comm,
                    'pnl': pnl,
                    'pnl_pct': pnl_pct,
                    'buy_price': self.buy_price,
                    'hold_days': (current_date - self.buy_date).days if self.buy_date else 0,
                })

                # 重置持仓信息
                self.buy_price = None
                self.buy_date = None

        elif order.status in [order.Canceled]:
            order_type = "买入" if order.isbuy() else "卖出"
            current_date = self.datas[0].datetime.date(0)
            logger.warning(f"【订单已取消】{current_date} {order_type}")
            logger.warning(f"  订单状态: {order.getstatusname()}")
            if order.created:
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
            current_date = self.datas[0].datetime.date(0)
            logger.error(f"【订单失败】{current_date} {order_type}")
            logger.error(f"  订单状态: {order.getstatusname()}")
            if order.created:
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

        核心流程：
        1. 检测当前市场状态
        2. 更新状态历史
        3. 检查是否满足买入/卖出确认条件
        4. 执行交易
        """
        # 跳过预热期
        if self.params.backtest_start_date:
            current_date = self.datas[0].datetime.date(0)
            start_date = datetime.strptime(
                self.params.backtest_start_date, '%Y-%m-%d'
            ).date()
            if current_date < start_date:
                return

        # 如果有订单等待执行，跳过
        if self.order:
            return

        # 检测市场状态
        stock_code = self.datas[0]._name
        current_date = self.datas[0].datetime.date(0)

        # 设置静默模式减少日志输出
        quiet_mode = os.environ.get('BACKTEST_QUIET_MODE', '0') == '1'
        verbose = not quiet_mode

        self.current_state = self.state_detector.detect_state(
            self.datas[0],
            stock_code=stock_code,
            verbose=verbose
        )

        # 更新状态历史
        self.state_history.append(self.current_state.regime)

        # 检测状态转换
        regime_change = self._detect_regime_change()
        if regime_change:
            self._log_regime_change(current_date, stock_code, regime_change)

        # 交易逻辑
        if not self.position:
            # 无持仓：检查买入条件
            if self._check_buy_signal():
                self._execute_buy(current_date, stock_code)
        else:
            # 有持仓：检查卖出条件
            if self._check_sell_signal():
                self._execute_sell(current_date, stock_code)

        # 更新前一状态
        self.prev_state = self.current_state

    def _detect_regime_change(self):
        """
        检测市场状态转换

        Returns:
            dict or None: 转换信息，如 {'from': 'bear', 'to': 'bull'}
        """
        if len(self.state_history) < 2:
            return None

        prev_regime = self.state_history[-2]
        curr_regime = self.state_history[-1]

        if prev_regime != curr_regime:
            return {
                'from': prev_regime.value,
                'to': curr_regime.value
            }
        return None

    def _log_regime_change(self, current_date, stock_code, regime_change):
        """记录状态转换日志"""
        logger.info(f"【状态转换】{stock_code} {current_date}: "
                   f"{regime_change['from']} → {regime_change['to']}")

        self.state_change_log.append({
            'date': current_date,
            'stock_code': stock_code,
            'from': regime_change['from'],
            'to': regime_change['to'],
            'score': self.current_state.confidence * 100 if self.current_state else 0,
        })

    def _check_buy_signal(self):
        """
        检查买入信号

        条件：连续 N 天从熊市/震荡市转入牛市
        """
        if len(self.state_history) < self.params.bull_confirm_days:
            return False

        # 检查最近 N 天是否都是牛市
        recent_states = list(self.state_history)[-self.params.bull_confirm_days:]
        if all(s == MarketRegime.BULL for s in recent_states):
            # 检查转换前是否是熊市或震荡市
            if len(self.state_history) > self.params.bull_confirm_days:
                prev_state = self.state_history[-self.params.bull_confirm_days - 1]
                if prev_state in [MarketRegime.BEAR, MarketRegime.NEUTRAL]:
                    logger.info(f"【买入信号】检测到牛市转换：{prev_state.value} → bull，连续{self.params.bull_confirm_days}天确认")
                    return True
            else:
                # 历史数据不足，但已连续N天牛市，也买入
                logger.info(f"【买入信号】连续{self.params.bull_confirm_days}天牛市确认")
                return True

        return False

    def _check_sell_signal(self):
        """
        检查卖出信号

        条件：连续 N 天进入熊市
        """
        if len(self.state_history) < self.params.bear_confirm_days:
            return False

        # 检查最近 N 天是否都是熊市
        recent_states = list(self.state_history)[-self.params.bear_confirm_days:]
        if all(s == MarketRegime.BEAR for s in recent_states):
            logger.info(f"【卖出信号】连续{self.params.bear_confirm_days}天熊市确认")
            return True

        return False

    def _execute_buy(self, current_date, stock_code):
        """执行买入"""
        cash = self.broker.getcash()
        # 使用次日开盘价作为参考（因为信号是在当日收盘后产生的）
        price = self.data_open[0]
        size = int(cash / price / 100) * 100  # 按整手买入

        if size > 0:
            # 使用市价单买入，确保及时入场
            self.order = self.buy(size=size, exectype=bt.Order.Market)
            logger.info(f"【下单买入】{stock_code} {current_date}")
            logger.info(f"  可用资金: {cash:.2f}, 参考价格: {price:.2f}, 数量: {size}股")

    def _execute_sell(self, current_date, stock_code):
        """执行卖出"""
        size = self.position.size
        if size > 0:
            if self.params.use_market_order_for_sell:
                # 使用市价单卖出（确保成交）
                self.order = self.sell(size=size, exectype=bt.Order.Market)
                logger.info(f"【下单卖出】{stock_code} {current_date}")
                logger.info(f"  持仓数量: {size}股, 市价卖出")
            else:
                # 使用限价单卖出
                price = self.data_open[0] * (1 - 0.002)
                self.order = self.sell(size=size, exectype=bt.Order.Limit, price=price)
                logger.info(f"【下单卖出】{stock_code} {current_date}")
                logger.info(f"  持仓数量: {size}股, 限价: {price:.2f}")

    def stop(self):
        """策略结束时输出总结"""
        logger.info("=" * 60)
        logger.info("策略交易总结 - 牛熊市转换策略")
        logger.info("=" * 60)

        # 输出基础信息
        logger.info(f"  股票代码: {self.datas[0]._name}")
        logger.info(f"  周期配置: {self.params.cycle}")
        logger.info(f"  牛市确认天数: {self.params.bull_confirm_days}")
        logger.info(f"  熊市确认天数: {self.params.bear_confirm_days}")

        if self.params.backtest_start_date:
            logger.info(f"  回测周期: {self.params.backtest_start_date} - {self.datas[0].datetime.date(0)}")
        else:
            logger.info(f"  回测周期: {self.datas[0].datetime.date(-1)} - {self.datas[0].datetime.date(0)}")

        # 输出交易统计
        if self.trade_log:
            buy_trades = [t for t in self.trade_log if t['type'] == 'buy']
            sell_trades = [t for t in self.trade_log if t['type'] == 'sell']

            logger.info(f"\n交易统计:")
            logger.info(f"  买入次数: {len(buy_trades)}")
            logger.info(f"  卖出次数: {len(sell_trades)}")

            if sell_trades:
                wins = [t for t in sell_trades if t.get('pnl', 0) > 0]
                losses = [t for t in sell_trades if t.get('pnl', 0) <= 0]
                total_pnl = sum(t.get('pnl', 0) for t in sell_trades)
                avg_pnl_pct = sum(t.get('pnl_pct', 0) for t in sell_trades) / len(sell_trades)

                logger.info(f"  盈利次数: {len(wins)}")
                logger.info(f"  亏损次数: {len(losses)}")
                logger.info(f"  胜率: {len(wins)/len(sell_trades)*100:.1f}%")
                logger.info(f"  总盈亏: {total_pnl:.2f}")
                logger.info(f"  平均盈亏%: {avg_pnl_pct:.2f}%")

        # 输出状态转换统计
        if self.state_change_log:
            logger.info(f"\n状态转换统计: {len(self.state_change_log)} 次")
            transition_counts = {}
            for change in self.state_change_log:
                key = f"{change['from']} → {change['to']}"
                transition_counts[key] = transition_counts.get(key, 0) + 1

            for key, count in sorted(transition_counts.items(), key=lambda x: -x[1]):
                logger.info(f"    {key}: {count} 次")

        # 输出订单失败统计
        if self.failed_orders:
            logger.info(f"\n订单失败统计: {len(self.failed_orders)} 次")
            by_status = {}
            for failed_order in self.failed_orders:
                status = failed_order['status']
                by_status[status] = by_status.get(status, 0) + 1

            for status, count in by_status.items():
                logger.info(f"    - {status}: {count} 次")
        else:
            logger.info("\n订单失败统计: 0 次（所有订单均成功执行）")

        # 最终收益
        final_value = self.broker.getvalue()
        total_return = (final_value / self.initial_cash - 1) * 100
        logger.info(f"\n最终收益:")
        logger.info(f"  初始资金: {self.initial_cash:.2f}")
        logger.info(f"  最终资金: {final_value:.2f}")
        logger.info(f"  总收益率: {total_return:.2f}%")

        logger.info("=" * 60)
