"""
测试 TechnicalIndicators 类
"""
import unittest
import pandas as pd
import numpy as np
from src.data_sources.query.technical import TechnicalIndicators


class TestTechnicalIndicators(unittest.TestCase):
    """测试 TechnicalIndicators 类"""

    def setUp(self):
        """创建测试数据"""
        # 创建包含 OHLCV 数据的测试 DataFrame
        np.random.seed(42)
        n = 100

        self.df = pd.DataFrame({
            'datetime': pd.date_range('2025-01-01', periods=n, freq='D'),
            'open': np.random.uniform(10, 50, n),
            'high': np.random.uniform(10, 50, n),
            'low': np.random.uniform(10, 50, n),
            'close': np.random.uniform(10, 50, n),
            'volume': np.random.uniform(1000000, 10000000, n),
        })

        # 确保 high >= close >= low
        self.df['high'] = self.df[['open', 'close', 'high']].max(axis=1)
        self.df['low'] = self.df[['open', 'close', 'low']].min(axis=1)

    def test_sma(self):
        """测试简单移动平均线"""
        sma = TechnicalIndicators.sma(self.df, period=20)
        self.assertIsInstance(sma, pd.Series)
        self.assertEqual(len(sma), len(self.df))
        # 前19个值应该是NaN
        self.assertTrue(pd.isna(sma.iloc[0:19]).all())
        # 第20个值不应该是NaN
        self.assertFalse(pd.isna(sma.iloc[19]))

    def test_ema(self):
        """测试指数移动平均线"""
        ema = TechnicalIndicators.ema(self.df, period=20)
        self.assertIsInstance(ema, pd.Series)
        self.assertEqual(len(ema), len(self.df))
        # EMA 第一个值应该是close，不是NaN
        self.assertEqual(ema.iloc[0], self.df['close'].iloc[0])

    def test_rsi(self):
        """测试相对强弱指标"""
        rsi = TechnicalIndicators.rsi(self.df, period=14)
        self.assertIsInstance(rsi, pd.Series)
        self.assertEqual(len(rsi), len(self.df))
        # 验证RSI值在0-100之间
        valid_rsi = rsi.dropna()
        if len(valid_rsi) > 0:
            self.assertTrue(all(valid_rsi >= 0))
            self.assertTrue(all(valid_rsi <= 100))

    def test_macd(self):
        """测试MACD指标"""
        df = TechnicalIndicators.macd(self.df, fast=12, slow=26, signal=9)
        self.assertIn('macd', df.columns)
        self.assertIn('macd_signal', df.columns)
        self.assertIn('macd_hist', df.columns)
        self.assertEqual(len(df), len(self.df))

    def test_bollinger_bands(self):
        """测试布林带"""
        df = TechnicalIndicators.bollinger_bands(self.df, period=20, std_dev=2.0)
        self.assertIn('bb_upper', df.columns)
        self.assertIn('bb_middle', df.columns)
        self.assertIn('bb_lower', df.columns)
        self.assertIn('bb_width', df.columns)
        self.assertIn('bb_pct', df.columns)

        # 验证上轨 > 中轨 > 下轨
        valid_data = df.dropna(subset=['bb_upper', 'bb_middle', 'bb_lower'])
        if len(valid_data) > 0:
            self.assertTrue(all(valid_data['bb_upper'] >= valid_data['bb_middle']))
            self.assertTrue(all(valid_data['bb_middle'] >= valid_data['bb_lower']))

    def test_atr(self):
        """测试平均真实波幅"""
        atr = TechnicalIndicators.atr(self.df, period=14)
        self.assertIsInstance(atr, pd.Series)
        self.assertEqual(len(atr), len(self.df))
        # ATR应该是非负数
        valid_atr = atr.dropna()
        if len(valid_atr) > 0:
            self.assertTrue(all(valid_atr >= 0))

    def test_stochastic(self):
        """测试随机指标"""
        df = TechnicalIndicators.stochastic(self.df, k_period=14, d_period=3)
        self.assertIn('stoch_k', df.columns)
        self.assertIn('stoch_d', df.columns)

        # 验证随机指标在0-100之间
        valid_k = df['stoch_k'].dropna()
        if len(valid_k) > 0:
            self.assertTrue(all(valid_k >= 0))
            self.assertTrue(all(valid_k <= 100))

    def test_obv(self):
        """测试能量潮指标"""
        obv = TechnicalIndicators.obv(self.df)
        self.assertIsInstance(obv, pd.Series)
        self.assertEqual(len(obv), len(self.df))

    def test_williams_r(self):
        """测试威廉指标"""
        williams_r = TechnicalIndicators.williams_r(self.df, period=14)
        self.assertIsInstance(williams_r, pd.Series)
        self.assertEqual(len(williams_r), len(self.df))

        # 验证Williams %R在-100到0之间
        valid_wr = williams_r.dropna()
        if len(valid_wr) > 0:
            self.assertTrue(all(valid_wr >= -100))
            self.assertTrue(all(valid_wr <= 0))

    def test_cci(self):
        """测试商品路径指标"""
        cci = TechnicalIndicators.cci(self.df, period=20)
        self.assertIsInstance(cci, pd.Series)
        self.assertEqual(len(cci), len(self.df))

    def test_momentum(self):
        """测试动量指标"""
        momentum = TechnicalIndicators.momentum(self.df, period=10)
        self.assertIsInstance(momentum, pd.Series)
        self.assertEqual(len(momentum), len(self.df))

    def test_roc(self):
        """测试变动率指标"""
        roc = TechnicalIndicators.roc(self.df, period=12)
        self.assertIsInstance(roc, pd.Series)
        self.assertEqual(len(roc), len(self.df))


if __name__ == '__main__':
    unittest.main()
