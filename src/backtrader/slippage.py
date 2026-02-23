"""
Slippage Simulator for Backtrader

Provides utility functions for setting slippage in backtrader.
Note: backtrader's built-in slippage uses the same percentage for both buy and sell.
For different buy/sell slippage, custom broker implementation would be needed.
"""

import backtrader as bt


def set_percentage_slippage(cerebro, slip_perc: float = 0.002):
    """
    Set percentage-based slippage for the cerebro instance

    Args:
        cerebro: Cerebro instance
        slip_perc: Slippage percentage (default 0.002 = 0.2%)

    Note:
        This applies the same slippage to both buy and sell orders.
        For asymmetric slippage, a custom broker implementation is required.
    """
    cerebro.broker.set_slippage_perc(slip_perc)
