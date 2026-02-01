#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简单移动平均线交叉策略

策略逻辑：
- 当收盘价向上突破移动平均线时买入
- 当收盘价向下跌破移动平均线时卖出
"""
import backtrader as bt


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
        ("commission", 0.002),  # 手续费率，默认0.2%
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
                print(f"\n【买入】 {current_date}")
                print(f"  当日价格: 开盘={open_price:.2f}, 最高={high_price:.2f}, 最低={low_price:.2f}, 收盘={close_price:.2f}")
                print(f"  执行价格: {order.executed.price:.2f}, 数量: {order.executed.size}, 手续费: {order.executed.comm:.2f}")
            elif order.issell():
                print(f"\n【卖出】 {current_date}")
                print(f"  当日价格: 开盘={open_price:.2f}, 最高={high_price:.2f}, 最低={low_price:.2f}, 收盘={close_price:.2f}")
                print(f"  执行价格: {order.executed.price:.2f}, 数量: {abs(order.executed.size)}, 手续费: {order.executed.comm:.2f}")

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            print(f"订单失败: {order.getstatusname()}")

        self.order = None

    def next(self):
        """
        执行交易逻辑

        交易规则：
        - 无持仓时，如果收盘价突破移动平均线，全仓买入
        - 有持仓时，如果收盘价跌破移动平均线，清仓卖出
        """
        if self.order:  # 检查是否有指令等待执行
            return

        # 检查是否持仓
        if not self.position:  # 没有持仓
            if self.data_close[0] > self.sma[0]:  # 执行买入条件判断：收盘价格上涨突破移动平均线
                # 全仓买入：计算可用资金能买入多少股
                cash = self.broker.getcash()  # 获取可用现金
                price = self.data_close[0]  # 当前价格
                # 计算可买入数量（考虑手续费，按整手买入）
                comm = self.params.commission  # 手续费率
                buy_value = cash / (1 + comm)  # 扣除手续费后的可用金额
                size = int(buy_value / price / 100) * 100  # 按整手买入

                if size > 0:
                    self.order = self.buy(size=size)
                    print(f"\n【买入信号】 {self.datas[0].datetime.date(0)}")
                    print(f"  可用资金: {cash:.2f}, 买入价格: {price:.2f}, 买入数量: {size}股")
        else:
            if self.data_close[0] < self.sma[0]:  # 执行卖出条件判断：收盘价格跌破移动平均线
                # 清仓卖出：卖出所有持仓
                size = self.position.size  # 当前持仓数量
                if size > 0:
                    self.order = self.sell(size=size)
                    print(f"\n【卖出信号】 {self.datas[0].datetime.date(0)}")
                    print(f"  持仓数量: {size}股, 卖出价格: {self.data_close[0]:.2f}")
