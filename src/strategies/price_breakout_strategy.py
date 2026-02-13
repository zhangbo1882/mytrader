#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
价格突破策略

策略逻辑：
- 买入条件：当日最低价 < 开盘价 × (1 - 买入阈值/100) 时触发买入
- 买入价格：限价订单，价格为开盘价 × (1 - 买入阈值/100)
- 止损条件：当日最低价 <= 买入价 × (1 - 止损阈值/100) 时触发止损卖出
- 止盈条件：当日最高价 > 买入价 × (1 + 止盈阈值/100) 时触发止盈卖出
- 卖出价格：限价订单，价格为当日收盘价（实际会在下一交易日开盘价成交）

说明：
1. 在无持仓状态下，当日内最低价低于开盘价的指定阈值时，以限价订单买入
2. 在持仓状态下，每个交易日检查止损和止盈条件：
   - 止损优先：当日最低价跌破买入价的止损阈值时止损
   - 止盈：当日最高价突破买入价的止盈阈值时止盈
   - 止损和止盈互斥，每个交易日只执行一次卖出
3. **重要**：由于backtrader的日bar处理机制，卖出订单会在下一个交易日执行，通常以次日开盘价成交
4. 如果没有触发卖出条件，继续持有
5. 在每个交易日开始时，取消前一天未成交的订单
"""
import logging
import backtrader as bt

logger = logging.getLogger(__name__)


class PriceBreakoutStrategy(bt.Strategy):
    """
    价格突破策略

    Parameters
    ----------
    buy_threshold : float
        买入阈值（百分比），默认为1.0，表示当日最低价比开盘价低1%时买入
    sell_threshold : float
        止盈阈值（百分比），默认为5.0，表示当日最高价比买入价高5%时止盈卖出
    stop_loss_threshold : float
        止损阈值（百分比），默认为10.0，表示当日最低价比买入价低10%时止损卖出
    commission : float
        手续费率，默认0.2%
    """

    params = (
        ("buy_threshold", 1.0),      # 买入阈值（百分比）
        ("sell_threshold", 5.0),     # 止盈阈值（百分比）
        ("stop_loss_threshold", 10.0),  # 止损阈值（百分比）
        ("commission", 0.002),       # 手续费率，默认0.2%
    )

    def __init__(self):
        """
        初始化函数
        """
        # 保存数据引用
        self.data_open = self.datas[0].open
        self.data_high = self.datas[0].high
        self.data_low = self.datas[0].low
        self.data_close = self.datas[0].close

        # 初始化交易指令和买入价格
        self.order = None
        self.buy_price = None
        self.pending_buy_price = None  # 记录待执行的买入限价
        self.sell_reason = None  # 记录卖出原因（止损/止盈）

        # 交易黑名单：已卖出过的股票不再买入（每只股票只交易一次）
        self.traded_stocks = set()

        # 添加日志记录
        self.trade_log = []

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
                # 记录买入价格
                self.buy_price = order.executed.price
                self.pending_buy_price = None

                # 记录交易日志
                trade_info = {
                    'date': current_date,
                    'action': '买入',
                    'price': order.executed.price,
                    'size': order.executed.size,
                    'commission': order.executed.comm,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price
                }
                self.trade_log.append(trade_info)

                logger.info(f"【买入成交】 {current_date}")
                logger.info(f"  当日价格: 开盘={open_price:.2f}, 最高={high_price:.2f}, 最低={low_price:.2f}, 收盘={close_price:.2f}")
                logger.info(f"  执行价格: {order.executed.price:.2f}, 数量: {order.executed.size}, 手续费: {order.executed.comm:.2f}")

            elif order.issell():
                # 计算持有期和收益
                profit_pct = 0.0
                if self.buy_price:
                    profit_pct = (order.executed.price / self.buy_price - 1) * 100

                # 将已卖出的股票加入黑名单（每只股票只交易一次）
                stock_code = self.datas[0]._name
                self.traded_stocks.add(stock_code)
                reason_desc = "止损" if self.sell_reason == '止损' else ("止盈" if self.sell_reason == '止盈' else "卖出")
                logger.info(f"  股票 {stock_code} 已{reason_desc}，加入黑名单，本策略周期内不再买入")

                # 记录交易日志
                trade_info = {
                    'date': current_date,
                    'action': '卖出',
                    'price': order.executed.price,
                    'size': order.executed.size,
                    'commission': order.executed.comm,
                    'buy_price': self.buy_price,
                    'profit_pct': profit_pct,
                    'sell_reason': self.sell_reason,  # 记录卖出原因
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price
                }
                self.trade_log.append(trade_info)

                # 根据卖出原因使用不同的日志标题
                reason = self.sell_reason if self.sell_reason else '卖出'
                logger.info(f"【{reason}成交】 {current_date}")
                logger.info(f"  当日价格: 开盘={open_price:.2f}, 最高={high_price:.2f}, 最低={low_price:.2f}, 收盘={close_price:.2f}")
                logger.info(f"  执行价格: {order.executed.price:.2f}, 数量: {abs(order.executed.size)}, 手续费: {order.executed.comm:.2f}")
                if self.buy_price:
                    logger.info(f"  买入价格: {self.buy_price:.2f}, 持有收益: {profit_pct:.2f}%")

                # 重置买入价格和卖出原因
                self.buy_price = None
                self.sell_reason = None

        elif order.status in [order.Canceled]:
            logger.warning(f"订单已取消: {order.getstatusname()}")

        elif order.status in [order.Margin, order.Rejected]:
            logger.error(f"订单失败: {order.getstatusname()}")

        self.order = None

    def next(self):
        """
        执行交易逻辑

        交易规则：
        - 无持仓时，如果当日最低价低于开盘价×(1-买入阈值%)，则以限价单买入
        - 有持仓时，检查止损和止盈条件：
          * 止损优先：当日最低价 <= 买入价×(1-止损阈值%)，触发止损
          * 止盈：当日最高价 > 买入价×(1+止盈阈值%)，触发止盈
          * 止损和止盈互斥，每个交易日只执行一次卖出
        - **注意**：由于日bar数据限制，卖出订单会在下一个交易日执行（通常以次日开盘价成交）
        """
        # 取消前一天的未成交订单
        if self.order:
            self.cancel(self.order)
            self.order = None
            self.pending_buy_price = None

        # 买入条件：当日最低价 < 开盘价 × (1 - 买入阈值/100)
        # 例如：买入阈值=1.0%，则条件为 low < open × 0.99
        buy_threshold_price = self.data_open[0] * (1 - self.params.buy_threshold / 100)
        buy_condition_met = self.data_low[0] < buy_threshold_price

        # 获取当前股票代码
        stock_code = self.datas[0]._name

        # 检查是否持仓
        if not self.position:  # 没有持仓
            # 检查是否在交易黑名单中（已卖出过的股票不再买入）
            if stock_code in self.traded_stocks:
                logger.info(f"  跳过买入: 股票 {stock_code} 已交易过，本周期不再买入")
            elif buy_condition_met:  # 买入条件满足
                # 计算限价：使用当日最低价，确保订单能够成交
                # 注意：在日bar数据中，限价单会在下一根bar执行
                # 为了提高成交率，使用接近当日最低价的价格
                buy_limit_price = self.data_low[0] * 1.001  # 略高于最低价，确保成交

                # 全仓买入：计算可用资金能买入多少股
                cash = self.broker.getcash()

                # 计算可买入数量（考虑手续费，按整手买入）
                comm = self.params.commission
                buy_value = cash / (1 + comm)  # 扣除手续费后的可用金额
                size = int(buy_value / buy_limit_price / 100) * 100  # 按整手买入

                if size > 0:
                    self.order = self.buy(price=buy_limit_price, size=size)
                    self.pending_buy_price = buy_limit_price
                    logger.info(f"【买入信号】 {self.datas[0].datetime.date(0)}")
                    logger.info(f"  买入条件: 最低价{self.data_low[0]:.2f} < 开盘价{self.data_open[0]:.2f} × {100-self.params.buy_threshold:.1f}% = {buy_threshold_price:.2f}")
                    logger.info(f"  可用资金: {cash:.2f}, 限价: {buy_limit_price:.2f}, 买入数量: {size}股")

        else:  # 有持仓
            # 检查止损和止盈条件
            if self.buy_price:
                # 止损条件：当日最低价 <= 买入价 × (1 - 止损阈值/100)
                # 例如：止损阈值=10.0%，则条件为 low <= buy_price × 0.90
                stop_loss_price = self.buy_price * (1 - self.params.stop_loss_threshold / 100)
                stop_loss_condition_met = self.data_low[0] <= stop_loss_price

                # 止盈条件：当日最高价 > 买入价 × (1 + 止盈阈值/100)
                # 例如：止盈阈值=5.0%，则条件为 high > buy_price × 1.05
                take_profit_price = self.buy_price * (1 + self.params.sell_threshold / 100)
                take_profit_condition_met = self.data_high[0] > take_profit_price

                # 确定卖出原因（优先级：止损 > 止盈）
                self.sell_reason = None
                if stop_loss_condition_met:
                    self.sell_reason = '止损'
                elif take_profit_condition_met:
                    self.sell_reason = '止盈'

                if self.sell_reason:
                    size = self.position.size
                    # 使用当前bar收盘价作为预期价格，但实际会以次日开盘价成交
                    sell_price = self.data_close[0]
                    if size > 0:
                        self.order = self.sell(price=sell_price, size=size)
                        logger.info(f"【{self.sell_reason}信号】 {self.datas[0].datetime.date(0)}")

                        # 根据卖出原因输出不同的条件信息
                        if self.sell_reason == '止损':
                            logger.info(f"  止损条件: 最低价{self.data_low[0]:.2f} <= 买入价{self.buy_price:.2f} × {100-self.params.stop_loss_threshold:.1f}% = {stop_loss_price:.2f}")
                        else:  # 止盈
                            logger.info(f"  止盈条件: 最高价{self.data_high[0]:.2f} > 买入价{self.buy_price:.2f} × {100+self.params.sell_threshold:.1f}% = {take_profit_price:.2f}")

                        logger.info(f"  持仓数量: {size}股, 预期成交价(当日收盘): {sell_price:.2f}")

    def stop(self):
        """
        策略结束时的回调函数
        """
        # 输出交易总结
        logger.info("=" * 60)
        logger.info("策略交易总结")
        logger.info("=" * 60)

        # 输出交易黑名单统计
        if self.traded_stocks:
            logger.info(f"交易黑名单统计: {len(self.traded_stocks)} 支股票在本回测周期内已卖出，不再买入")
            for stock in sorted(self.traded_stocks):
                logger.info(f"  - {stock}")
        else:
            logger.info("交易黑名单: 无（本回测周期未完成任何交易）")

        if self.trade_log:
            total_trades = len([t for t in self.trade_log if t['action'] in ['买入', '卖出']])
            buy_trades = [t for t in self.trade_log if t['action'] == '买入']
            sell_trades = [t for t in self.trade_log if t['action'] == '卖出']

            logger.info(f"总交易次数: {total_trades}")
            logger.info(f"买入次数: {len(buy_trades)}")
            logger.info(f"卖出次数: {len(sell_trades)}")

            if sell_trades:
                # 计算平均收益
                total_profit = sum(t.get('profit_pct', 0) for t in sell_trades)
                avg_profit = total_profit / len(sell_trades)

                # 找出最佳和最差交易
                profitable_trades = [t for t in sell_trades if t.get('profit_pct', 0) > 0]
                loss_trades = [t for t in sell_trades if t.get('profit_pct', 0) < 0]

                logger.info(f"盈利交易次数: {len(profitable_trades)}")
                logger.info(f"亏损交易次数: {len(loss_trades)}")
                logger.info(f"平均收益率: {avg_profit:.2f}%")

                if profitable_trades:
                    best_trade = max(profitable_trades, key=lambda t: t.get('profit_pct', 0))
                    logger.info(f"最佳交易: {best_trade['date']}, 收益率: {best_trade.get('profit_pct', 0):.2f}%")

                if loss_trades:
                    worst_trade = min(loss_trades, key=lambda t: t.get('profit_pct', 0))
                    logger.info(f"最差交易: {worst_trade['date']}, 收益率: {worst_trade.get('profit_pct', 0):.2f}%")
        else:
            logger.info("没有交易记录")

        logger.info("=" * 60)
