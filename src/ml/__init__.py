"""
Machine Learning Module for Stock Price Prediction

This module provides ML-based stock price prediction using:
- LightGBM for fast, interpretable baseline models
- LSTM for temporal pattern recognition
- Ensemble models for robust predictions
"""

__version__ = "1.0.0"

from .data_loader import MlDataLoader
from .preprocessor import FeatureEngineer

__all__ = ["MlDataLoader", "FeatureEngineer"]
