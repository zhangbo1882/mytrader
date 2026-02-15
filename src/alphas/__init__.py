"""
101 Formulaic Alphas Factor Library

Public API for computing alpha factors based on Kakushadze (2015).
"""
from src.alphas.engine import AlphaEngine
from src.alphas.data_adapter import AlphaDataAdapter, AlphaDataPanel
from src.alphas.operators import (
    delay, delta, ts_sum, ts_min, ts_max, ts_argmax, ts_argmin,
    ts_rank, product, stddev, correlation, covariance, decay_linear,
    rank, scale, indneutralize, signedpower
)

__all__ = [
    'AlphaEngine',
    'AlphaDataAdapter',
    'AlphaDataPanel',
    'delay', 'delta', 'ts_sum', 'ts_min', 'ts_max',
    'ts_argmax', 'ts_argmin', 'ts_rank', 'product', 'stddev',
    'correlation', 'covariance', 'decay_linear',
    'rank', 'scale', 'indneutralize', 'signedpower',
]
