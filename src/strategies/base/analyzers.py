#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Backtrader 自定义分析器

提供策略回测性能分析功能
"""
import logging
import backtrader as bt
import pandas as pd
from typing import Dict, Any

logger = logging.getLogger(__name__)


class PortfolioValueAnalyzer(bt.Analyzer):
    """
    组合价值分析器

    记录每日的组合价值、收益率等数据
    """

    def __init__(self):
        """初始化分析器"""
        self.portfolio_values = []
        self.dates = []
        self.returns = []

    def next(self):
        """
        每个交易日调用
        """
        # 获取当前日期
        date = self.strategy.datas[0].datetime.date(0)
        # 获取当前组合价值
        portfolio_value = self.strategy.broker.getvalue()

        self.dates.append(date)
        self.portfolio_values.append(portfolio_value)

    def get_analysis(self):
        """
        返回分析结果

        Returns:
            包含日期、组合价值和收益率的字典
        """
        # 计算收益率
        portfolio_values_series = pd.Series(self.portfolio_values)
        returns = portfolio_values_series.pct_change().fillna(0)

        return {
            'dates': self.dates,
            'portfolio_values': self.portfolio_values,
            'returns': returns.tolist(),
            'final_value': self.portfolio_values[-1] if self.portfolio_values else 0
        }


class TradeAnalyzer(bt.Analyzer):
    """
    交易分析器

    记录每笔交易的详细信息
    注意：回测使用前复权价格，分析器记录的也是前复权价格
    """

    params = (
        ('price_map', {}),  # 价格映射（与回测使用相同的价格类型，通常是前复权）
    )

    def __init__(self):
        """初始化分析器"""
        self.trades = []
        self.current_position = None
        self.buy_order = None
        self.buy_date = None
        self.buy_price = None
        self.buy_size = None
        self.buy_commission = None
        self.buy_original_price = None  # 买入时的实际价格
        self.sell_original_price = None  # 卖出时的实际价格
        # 统计所有成交的订单（包括买入和卖出）
        self.total_orders = 0  # 总订单数
        self.buy_orders = 0    # 买入订单数
        self.sell_orders = 0   # 卖出订单数
        # 记录所有单独的订单（包括未配对的买入）
        self.all_orders_list = []  # 所有订单列表

    def notify_order(self, order):
        """
        订单状态通知

        Args:
            order: 订单对象
        """
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            # 统计成交的订单
            self.total_orders += 1

            current_date = self.strategy.datas[0].datetime.date(0)
            current_price = order.executed.price

            # 获取当日价格映射（与回测使用的价格类型一致）
            date_str = current_date.strftime('%Y-%m-%d')
            price_map = self.params.price_map.get(date_str, {})

            if order.isbuy():
                # 记录买入信息
                self.buy_orders += 1
                self.buy_date = current_date
                self.buy_price = current_price
                self.buy_size = order.executed.size
                self.buy_commission = order.executed.comm
                self.buy_order = order
                self.buy_original_price = price_map.get('close', current_price)

                # 记录到所有订单列表
                self.all_orders_list.append({
                    'order_type': 'buy',
                    'date': current_date,
                    'price': current_price,
                    'price_map': price_map.get('close', current_price),
                    'size': order.executed.size,
                    'commission': order.executed.comm,
                    'value': current_price * order.executed.size
                })

            elif order.issell():
                self.sell_orders += 1
                if self.buy_order is not None:
                    # 卖出完成，计算完整交易记录
                    sell_date = current_date
                    sell_price = current_price
                    # 使用买入时的数量，而不是卖出订单的数量
                    sell_size = self.buy_size
                    sell_commission = order.executed.comm
                    self.sell_original_price = price_map.get('close', current_price)

                    # 计算持仓天数
                    hold_days = (sell_date - self.buy_date).days

                    # 计算盈亏（使用复权价格）
                    buy_value = self.buy_price * self.buy_size
                    sell_value = sell_price * sell_size
                    gross_pnl = sell_value - buy_value
                    net_pnl = gross_pnl - self.buy_commission - sell_commission

                    # 计算盈亏百分比
                    pnl_pct = (net_pnl / buy_value) * 100 if buy_value > 0 else 0

                    # 记录完整交易
                    self.trades.append({
                        'buy_date': self.buy_date,
                        'sell_date': sell_date,
                        'buy_price': self.buy_price,  # 复权买入价
                        'sell_price': sell_price,  # 复权卖出价
                        'buy_price_original': self.buy_original_price,  # 实际买入价
                        'sell_price_original': self.sell_original_price,  # 实际卖出价
                        'size': self.buy_size,
                        'hold_days': hold_days,
                        'buy_value': buy_value,
                        'sell_value': sell_value,
                        'gross_pnl': gross_pnl,
                        'commission': self.buy_commission + sell_commission,
                        'net_pnl': net_pnl,
                        'pnl_pct': pnl_pct
                    })

                    # 重置买入信息
                    self.buy_order = None
                    self.buy_date = None
                    self.buy_price = None
                    self.buy_size = None
                    self.buy_commission = None
                    self.buy_original_price = None
                    self.sell_original_price = None

    def get_analysis(self):
        """
        返回分析结果

        Returns:
            交易记录字典
        """
        logger.info(f"[TradeAnalyzer.get_analysis] total_orders={self.total_orders}, buy_orders={self.buy_orders}, sell_orders={self.sell_orders}, completed_trades={len(self.trades)}")

        # 如果有未配对的买入订单，添加到 trades 列表（只在第一次调用时添加）
        if self.buy_order is not None and self.buy_date is not None:
            # 检查是否已经添加过（通过检查最后一个交易是否是未平仓的买入）
            has_unclosed = len(self.trades) > 0 and not self.trades[-1].get('is_closed', True)

            if not has_unclosed:
                # 创建未平仓的买入记录
                buy_value = self.buy_price * self.buy_size
                unclosed_trade = {
                    'buy_date': self.buy_date,
                    'sell_date': None,  # 未卖出
                    'buy_price': self.buy_price,
                    'sell_price': None,  # 未卖出
                    'buy_price_original': self.buy_original_price,
                    'sell_price_original': None,
                    'size': self.buy_size,
                    'hold_days': 0,
                    'buy_value': buy_value,
                    'sell_value': 0,
                    'gross_pnl': 0,
                    'commission': self.buy_commission,
                    'net_pnl': -self.buy_commission,  # 只损失了手续费
                    'pnl_pct': 0.0,
                    'is_closed': False  # 标记为未平仓
                }
                self.trades.append(unclosed_trade)

        # 如果没有任何交易
        if not self.trades:
            return {
                'trades': [],
                'total_orders': self.total_orders,  # 总订单数（买入+卖出）
                'buy_orders': self.buy_orders,        # 买入订单数
                'sell_orders': self.sell_orders,      # 卖出订单数
                'total_trades': self.total_orders,    # 总订单数
                'completed_trades': len(self.trades), # 完整交易对数
                'winning_trades': 0,
                'losing_trades': 0,
                'total_pnl': 0,
                'gross_pnl': 0,
                'total_commission': 0,
                'avg_pnl': 0,
                'avg_hold_days': 0,
                'max_profit': 0,
                'max_loss': 0,
                'win_rate': 0
            }

        # 计算统计数据（只计算已平仓的交易）
        closed_trades = [t for t in self.trades if t.get('is_closed', True)]
        completed_trades = len(closed_trades)  # 完整的交易对数

        if completed_trades > 0:
            winning_trades = sum(1 for t in closed_trades if t.get('net_pnl', 0) > 0)
            losing_trades = completed_trades - winning_trades

            # 计算总盈利和总亏损
            profits = [t['net_pnl'] for t in closed_trades if t['net_pnl'] > 0]
            losses = [t['net_pnl'] for t in closed_trades if t['net_pnl'] < 0]
            total_profit = sum(profits) if profits else 0.0
            total_loss = sum(losses) if losses else 0.0

            gross_pnl = sum(t['gross_pnl'] for t in closed_trades)
            total_pnl = sum(t['net_pnl'] for t in closed_trades)
            total_commission = sum(t['commission'] for t in closed_trades)
            avg_pnl = total_pnl / completed_trades
            avg_hold_days = sum(t['hold_days'] for t in closed_trades) / completed_trades
            max_profit = max((t['net_pnl'] for t in closed_trades), default=0)
            max_loss = min((t['net_pnl'] for t in closed_trades), default=0)
            win_rate = winning_trades / completed_trades

            # 计算盈亏比 (盈利/亏损的绝对值)
            if total_loss != 0:
                profit_factor = abs(total_profit / total_loss)
            elif total_profit > 0:
                # 没有亏损但有一定盈利，盈亏比设为一个大数值表示极高
                profit_factor = 999999.99
            else:
                # 既没有盈利也没有亏损
                profit_factor = 0.0
        else:
            winning_trades = 0
            losing_trades = 0
            total_profit = 0.0
            total_loss = 0.0
            gross_pnl = 0
            total_pnl = 0
            total_commission = 0
            avg_pnl = 0
            avg_hold_days = 0
            max_profit = 0
            max_loss = 0
            win_rate = 0
            profit_factor = 0.0

        return {
            'trades': self.trades,
            'total_orders': self.total_orders,  # 总订单数（买入+卖出）
            'buy_orders': self.buy_orders,        # 买入订单数
            'sell_orders': self.sell_orders,      # 卖出订单数
            'total_trades': self.total_orders,    # 兼容旧字段，等于总订单数
            'completed_trades': completed_trades, # 完整交易对数
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'total_pnl': total_pnl,
            'gross_pnl': gross_pnl,
            'total_commission': total_commission,
            'total_profit': total_profit,
            'total_loss': total_loss,
            'profit_factor': profit_factor,
            'avg_pnl': avg_pnl,
            'avg_hold_days': avg_hold_days,
            'max_profit': max_profit,
            'max_loss': max_loss,
            'win_rate': win_rate
        }


def calculate_strategy_metrics(
    portfolio_analyzer: Dict[str, Any],
    initial_cash: float
) -> Dict[str, Any]:
    """
    从分析器结果计算策略指标

    Args:
        portfolio_analyzer: 组合分析器结果
        initial_cash: 初始资金

    Returns:
        策略指标字典
    """
    from ..ml.evaluators.strategy_metrics import strategy_health_check

    # 获取收益率序列
    returns = pd.Series(portfolio_analyzer['returns'])

    # 计算健康指标
    health_metrics = strategy_health_check(returns)

    # 添加其他指标
    portfolio_values = portfolio_analyzer['portfolio_values']
    total_return = (portfolio_values[-1] - initial_cash) / initial_cash

    return {
        **health_metrics,
        'total_return_calculated': float(total_return),
        'final_value': float(portfolio_values[-1]),
        'initial_cash': float(initial_cash)
    }
