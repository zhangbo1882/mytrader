#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
价格突破策略

基于股票自身市场状态（牛熊市）自适应调整买入/止盈/止损阈值
"""
from .strategy import PriceBreakoutStrategyV2

__all__ = ['PriceBreakoutStrategyV2']
