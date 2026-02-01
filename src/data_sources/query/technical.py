"""
技术指标计算工具
提供常用技术指标计算方法
"""
import pandas as pd
import numpy as np
from typing import Optional


class TechnicalIndicators:
    """
    技术指标计算类
    提供各种常用技术指标的计算方法
    """

    @staticmethod
    def sma(df: pd.DataFrame, period: int = 20, column: str = 'close') -> pd.Series:
        """
        简单移动平均线 (Simple Moving Average)

        Args:
            df: 包含价格数据的DataFrame
            period: 周期
            column: 计算列，默认close

        Returns:
            SMA值序列
        """
        return df[column].rolling(window=period).mean()

    @staticmethod
    def ema(df: pd.DataFrame, period: int = 20, column: str = 'close') -> pd.Series:
        """
        指数移动平均线 (Exponential Moving Average)

        Args:
            df: 包含价格数据的DataFrame
            period: 周期
            column: 计算列，默认close

        Returns:
            EMA值序列
        """
        return df[column].ewm(span=period, adjust=False).mean()

    @staticmethod
    def rsi(df: pd.DataFrame, period: int = 14, column: str = 'close') -> pd.Series:
        """
        相对强弱指标 (Relative Strength Index)

        Args:
            df: 包含价格数据的DataFrame
            period: 周期，默认14
            column: 计算列，默认close

        Returns:
            RSI值序列（0-100）
        """
        delta = df[column].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    @staticmethod
    def macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9,
             column: str = 'close') -> pd.DataFrame:
        """
        MACD指标 (Moving Average Convergence Divergence)

        Args:
            df: 包含价格数据的DataFrame
            fast: 快线周期，默认12
            slow: 慢线周期，默认26
            signal: 信号线周期，默认9
            column: 计算列，默认close

        Returns:
            添加了MACD列的原始DataFrame
            新增列：macd, macd_signal, macd_hist
        """
        df = df.copy()

        # 计算EMA
        ema_fast = df[column].ewm(span=fast, adjust=False).mean()
        ema_slow = df[column].ewm(span=slow, adjust=False).mean()

        # MACD线
        df['macd'] = ema_fast - ema_slow

        # 信号线
        df['macd_signal'] = df['macd'].ewm(span=signal, adjust=False).mean()

        # 柱状图
        df['macd_hist'] = df['macd'] - df['macd_signal']

        return df

    @staticmethod
    def bollinger_bands(df: pd.DataFrame, period: int = 20, std_dev: float = 2.0,
                        column: str = 'close') -> pd.DataFrame:
        """
        布林带 (Bollinger Bands)

        Args:
            df: 包含价格数据的DataFrame
            period: 周期，默认20
            std_dev: 标准差倍数，默认2
            column: 计算列，默认close

        Returns:
            添加了布林带列的原始DataFrame
            新增列：bb_upper, bb_middle, bb_lower, bb_width
        """
        df = df.copy()

        # 中轨 = SMA
        df['bb_middle'] = df[column].rolling(window=period).mean()

        # 标准差
        std = df[column].rolling(window=period).std()

        # 上轨和下轨
        df['bb_upper'] = df['bb_middle'] + (std * std_dev)
        df['bb_lower'] = df['bb_middle'] - (std * std_dev)

        # 带宽（上轨-下轨）/ 中轨
        df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_middle']

        # %B 位置指标 (价格 - 下轨) / (上轨 - 下轨)
        df['bb_pct'] = (df[column] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'])

        return df

    @staticmethod
    def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        平均真实波幅 (Average True Range)

        Args:
            df: 包含OHLC数据的DataFrame
            period: 周期，默认14

        Returns:
            ATR值序列
        """
        high = df['high']
        low = df['low']
        close = df['close']

        # 前一日收盘价
        prev_close = close.shift(1)

        # 真实波幅TR = max(H-L, abs(H-PC), abs(L-PC))
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        # ATR = TR的移动平均
        atr = tr.rolling(window=period).mean()

        return atr

    @staticmethod
    def stochastic(df: pd.DataFrame, k_period: int = 14, d_period: int = 3,
                   smooth_k: int = 1) -> pd.DataFrame:
        """
        随机指标 (Stochastic Oscillator)

        Args:
            df: 包含OHLC数据的DataFrame
            k_period: K线周期，默认14
            d_period: D线周期，默认3
            smooth_k: K线平滑周期，默认1

        Returns:
            添加了随机指标列的原始DataFrame
            新增列：stoch_k, stoch_d
        """
        df = df.copy()

        # 计算Raw %K
        low_min = df['low'].rolling(window=k_period).min()
        high_max = df['high'].rolling(window=k_period).max()

        raw_k = 100 * (df['close'] - low_min) / (high_max - low_min)

        # %K = Raw %K的移动平均
        df['stoch_k'] = raw_k.rolling(window=smooth_k).mean()

        # %D = %K的移动平均
        df['stoch_d'] = df['stoch_k'].rolling(window=d_period).mean()

        return df

    @staticmethod
    def obv(df: pd.DataFrame) -> pd.Series:
        """
        能量潮指标 (On Balance Volume)

        Args:
            df: 包含价格和成交量的DataFrame

        Returns:
            OBV值序列
        """
        df = df.copy()

        # 方向：涨为1，跌为-1，平为0
        direction = np.where(df['close'] > df['close'].shift(1), 1,
                            np.where(df['close'] < df['close'].shift(1), -1, 0))

        # OBV = 前一日OBV + 当日成交量 * 方向
        obv = (df['volume'] * direction).cumsum()

        return obv

    @staticmethod
    def williams_r(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        威廉指标 (Williams %R)

        Args:
            df: 包含OHLC数据的DataFrame
            period: 周期，默认14

        Returns:
            Williams %R值序列（-100到0）
        """
        high_max = df['high'].rolling(window=period).max()
        low_min = df['low'].rolling(window=period).min()

        williams_r = -100 * (high_max - df['close']) / (high_max - low_min)

        return williams_r

    @staticmethod
    def cci(df: pd.DataFrame, period: int = 20) -> pd.Series:
        """
        商品路径指标 (Commodity Channel Index)

        Args:
            df: 包含OHLC数据的DataFrame
            period: 周期，默认20

        Returns:
            CCI值序列
        """
        # 典型价格
        tp = (df['high'] + df['low'] + df['close']) / 3

        # SMA of typical price
        sma_tp = tp.rolling(window=period).mean()

        # Mean deviation
        mad = tp.rolling(window=period).apply(
            lambda x: np.abs(x - x.mean()).mean()
        )

        # CCI
        cci = (tp - sma_tp) / (0.015 * mad)

        return cci

    @staticmethod
    def momentum(df: pd.DataFrame, period: int = 10, column: str = 'close') -> pd.Series:
        """
        动量指标 (Momentum)

        Args:
            df: 包含价格数据的DataFrame
            period: 周期，默认10
            column: 计算列，默认close

        Returns:
            动量值序列
        """
        return df[column] - df[column].shift(period)

    @staticmethod
    def roc(df: pd.DataFrame, period: int = 12, column: str = 'close') -> pd.Series:
        """
        变动率指标 (Rate of Change)

        Args:
            df: 包含价格数据的DataFrame
            period: 周期，默认12
            column: 计算列，默认close

        Returns:
            ROC值序列（百分比）
        """
        return ((df[column] - df[column].shift(period)) / df[column].shift(period)) * 100
