#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简单移动平均线交叉策略

策略逻辑：
- 当收盘价向上突破移动平均线时买入
- 当收盘价向下跌破移动平均线时卖出
"""
import logging
from datetime import datetime
import backtrader as bt

logger = logging.getLogger(__name__)


class SMACrossStrategy(bt.Strategy):
    """
    简单移动平均线交叉策略

    Parameters
    ----------
    maperiod : int
        移动平均线的周期，默认为20日
    """

    params = (
        ("maperiod", 20),
        ("backtest_start_date", None),  # 回测实际开始日期（用于跳过预热期）
    )

    def __init__(self):
        """
        初始化函数
        """
        self.data_close = self.datas[0].close  # 指定价格序列
        # 初始化交易指令、买卖价格和手续费
        self.order = None
        self.buy_price = None
        self.buy_comm = None
        # 添加移动均线指标
        self.sma = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.maperiod
        )
        # 订单失败统计
        self.failed_orders = []  # 记录失败的订单详情

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

        if order.status in [order.Completed]:
            current_date = self.datas[0].datetime.date(0)
            open_price = self.datas[0].open[0]
            high_price = self.datas[0].high[0]
            low_price = self.datas[0].low[0]
            close_price = self.datas[0].close[0]

            if order.isbuy():
                logger.info(f"【买入】 {current_date}")
                logger.info(f"  当日价格: 开盘={open_price:.2f}, 最高={high_price:.2f}, 最低={low_price:.2f}, 收盘={close_price:.2f}")
                logger.info(f"  执行价格: {order.executed.price:.2f}, 数量: {order.executed.size}, 手续费: {order.executed.comm:.2f}")
            elif order.issell():
                logger.info(f"【卖出】 {current_date}")
                logger.info(f"  当日价格: 开盘={open_price:.2f}, 最高={high_price:.2f}, 最低={low_price:.2f}, 收盘={close_price:.2f}")
                logger.info(f"  执行价格: {order.executed.price:.2f}, 数量: {abs(order.executed.size)}, 手续费: {order.executed.comm:.2f}")

        elif order.status in [order.Canceled]:
            # 记录取消的订单
            order_type = "买入" if order.isbuy() else ("卖出" if order.issell() else "未知")
            current_date = self.datas[0].datetime.date(0)
            logger.warning(f"【订单已取消】{current_date} {order_type}")
            logger.warning(f"  订单状态: {order.getstatusname()}")
            logger.warning(f"  订单价格: {order.created.price:.2f}, 数量: {order.created.size}")

            # 记录到失败列表
            self.failed_orders.append({
                'date': current_date,
                'status': 'Canceled',
                'order_type': order_type,
                'price': order.created.price if order.created else None,
                'size': order.created.size if order.created else None,
                'reason': '订单已取消'
            })

        elif order.status in [order.Margin, order.Rejected]:
            # 记录失败的订单
            order_type = "买入" if order.isbuy() else ("卖出" if order.issell() else "未知")
            current_date = self.datas[0].datetime.date(0)
            logger.error(f"【订单失败】{current_date} {order_type}")
            logger.error(f"  订单状态: {order.getstatusname()}")
            if order.created:
                logger.error(f"  订单价格: {order.created.price:.2f}, 数量: {order.created.size}")

            # 记录到失败列表
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

        交易规则：
        - 无持仓时，如果收盘价突破移动平均线，全仓买入
        - 有持仓时，如果收盘价跌破移动平均线，清仓卖出
        """
        # 跳过预热期的数据（如果指定了回测开始日期）
        if self.params.backtest_start_date:
            current_date = self.datas[0].datetime.date(0)
            start_date = datetime.strptime(self.params.backtest_start_date, '%Y-%m-%d').date()
            if current_date < start_date:
                return  # 跳过预热期，不执行交易

        if self.order:  # 检查是否有指令等待执行
            return

        # 检查是否持仓
        if not self.position:  # 没有持仓
            if self.data_close[0] > self.sma[0]:  # 执行买入条件判断：收盘价格上涨突破移动平均线
                # 全仓买入：计算可用资金能买入多少股
                cash = self.broker.getcash()  # 获取可用现金
                price = self.data_close[0]  # 当前价格
                # 计算可买入数量（按整手买入）
                # 注意：backtrader 会自动通过 broker.setcommission() 扣除手续费
                size = int(cash / price / 100) * 100  # 按整手买入

                if size > 0:
                    self.order = self.buy(size=size)
                    logger.info(f"【买入信号】 {self.datas[0].datetime.date(0)}")
                    logger.info(f"  可用资金: {cash:.2f}, 买入价格: {price:.2f}, 买入数量: {size}股")
        else:
            if self.data_close[0] < self.sma[0]:  # 执行卖出条件判断：收盘价格跌破移动平均线
                # 清仓卖出：卖出所有持仓
                size = self.position.size  # 当前持仓数量
                if size > 0:
                    self.order = self.sell(size=size)
                    logger.info(f"【卖出信号】 {self.datas[0].datetime.date(0)}")
                    logger.info(f"  持仓数量: {size}股, 卖出价格: {self.data_close[0]:.2f}")

    def stop(self):
        """
        策略结束时的回调函数
        """
        # 输出交易总结
        logger.info("=" * 60)
        logger.info("策略交易总结")
        logger.info("=" * 60)

        # 输出基础信息
        logger.info(f"  股票代码: {self.datas[0]._name}")
        # 显示用户指定的回测周期
        if self.params.backtest_start_date:
            logger.info(f"  回测周期: {self.params.backtest_start_date} - {self.datas[0].datetime.date(0)}")
        else:
            logger.info(f"  回测周期: {self.datas[0].datetime.date(-1)} - {self.datas[0].datetime.date(0)}")

        # 输出订单失败统计
        if self.failed_orders:
            logger.info(f"\n订单失败统计: {len(self.failed_orders)} 次")
            by_status = {}
            by_type = {}

            for failed_order in self.failed_orders:
                status = failed_order['status']
                order_type = failed_order['order_type']

                by_status[status] = by_status.get(status, 0) + 1
                by_type[order_type] = by_type.get(order_type, 0) + 1

            logger.info("  按失败原因统计:")
            for status, count in by_status.items():
                logger.info(f"    - {status}: {count} 次")

            logger.info("  按订单类型统计:")
            for order_type, count in by_type.items():
                logger.info(f"    - {order_type}单: {count} 次")

            # 输出最近5次失败订单详情
            logger.info("\n  最近失败订单详情:")
            for failed_order in self.failed_orders[-5:]:
                logger.info(f"    [{failed_order['date']}] {failed_order['order_type']} - {failed_order['status']}")
                logger.info(f"      原因: {failed_order['reason']}")
                if failed_order['price']:
                    logger.info(f"      价格: {failed_order['price']:.2f}, 数量: {failed_order['size']}")
        else:
            logger.info("订单失败统计: 0 次（所有订单均成功执行）")

        logger.info("=" * 60)
