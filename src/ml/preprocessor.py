"""
Feature Engineering Module

Handles technical indicator calculation, feature scaling, and data preparation
for ML model training.
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional, Any
from sklearn.preprocessing import StandardScaler, RobustScaler
import logging

from src.data_sources.query.technical import TechnicalIndicators

logger = logging.getLogger(__name__)


class FeatureEngineer:
    """
    特征工程类

    负责：
    - 计算技术指标
    - 特征标准化/归一化
    - 准备训练数据 (X, y)
    """

    def __init__(self, scaler_type: str = "standard"):
        """
        初始化特征工程器

        Args:
            scaler_type: 标准化类型 ('standard', 'robust', 'none')
        """
        self.scaler_type = scaler_type
        self.scaler = None
        self.feature_cols = None

        if scaler_type == "standard":
            self.scaler = StandardScaler()
        elif scaler_type == "robust":
            self.scaler = RobustScaler()

    def _check_feature_validity(
        self,
        df: pd.DataFrame,
        context: str = "特征",
        min_valid_ratio: float = 0.1
    ) -> None:
        """
        检查特征列的数据有效性，对问题特征打印警告

        Args:
            df: 包含特征的DataFrame
            context: 上下文描述（用于日志）
            min_valid_ratio: 最小有效值比例（默认10%）
        """
        # 定义需要检查的技术指标特征列表
        tech_features = [
            'sma_5', 'sma_10', 'sma_20', 'sma_60',
            'ema_12', 'ema_26',
            'rsi_14', 'rsi_6',
            'macd', 'macd_signal', 'macd_hist',
            'bb_upper', 'bb_middle', 'bb_lower', 'bb_width',
            'atr_14', 'atr_ratio',
            'stoch_k', 'stoch_d',
            'williams_r', 'cci',
            'momentum_10', 'roc_12',
            'obv', 'obv_ma_5',
            'close_to_sma20', 'close_to_sma60',
            'volume_change', 'volume_ma_ratio',
            'momentum_1w', 'momentum_1m', 'momentum_3m',
            'realized_vol_20', 'rolling_skew', 'rolling_kurt'
        ]

        # 只检查存在的特征
        features_to_check = [f for f in tech_features if f in df.columns]

        if not features_to_check:
            return

        # 检查每个特征的有效性
        invalid_features = []
        low_valid_features = []
        total_rows = len(df)

        for feature in features_to_check:
            if feature not in df.columns:
                continue

            # 计算有效值数量（非空且非NaN）
            valid_count = df[feature].notna().sum()
            valid_ratio = valid_count / total_rows if total_rows > 0 else 0

            # 没有任何有效值
            if valid_count == 0:
                invalid_features.append(feature)
            # 有效值比例过低
            elif valid_ratio < min_valid_ratio:
                low_valid_features.append((feature, valid_count, valid_ratio))

        # 打印警告汇总
        if invalid_features:
            logger.warning(
                f"[{context}] 以下特征完全没有有效数据（全NaN）: "
                f"{', '.join(invalid_features)}"
            )

        if low_valid_features:
            feature_details = [
                f"{f} ({valid_count}/{total_rows} = {ratio*100:.1f}%)"
                for f, valid_count, ratio in low_valid_features
            ]
            logger.warning(
                f"[{context}] 以下特征有效数据比例低于{min_valid_ratio*100}%: "
                f"{', '.join(feature_details)}"
            )

        # 汇总报告
        if invalid_features or low_valid_features:
            total_problem = len(invalid_features) + len(low_valid_features)
            logger.warning(
                f"[{context}] 共发现 {total_problem} 个特征存在数据问题，"
                f"总特征数: {len(features_to_check)}"
            )
        else:
            valid_count = sum(
                df[f].notna().sum() for f in features_to_check
            )
            total_values = len(features_to_check) * total_rows
            logger.info(
                f"[{context}] 所有 {len(features_to_check)} 个特征数据正常，"
                f"有效数据率: {valid_count}/{total_values} = "
                f"{100*valid_count/total_values if total_values > 0 else 0:.1f}%"
            )

    def add_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加技术指标特征

        Args:
            df: 包含OHLCV数据的DataFrame

        Returns:
            添加技术指标后的DataFrame
        """
        df = df.copy()

        # 移动平均线
        df['sma_5'] = TechnicalIndicators.sma(df, 5)
        df['sma_10'] = TechnicalIndicators.sma(df, 10)
        df['sma_20'] = TechnicalIndicators.sma(df, 20)
        df['sma_60'] = TechnicalIndicators.sma(df, 60)

        # 指数移动平均线
        df['ema_12'] = TechnicalIndicators.ema(df, 12)
        df['ema_26'] = TechnicalIndicators.ema(df, 26)

        # RSI
        df['rsi_14'] = TechnicalIndicators.rsi(df, 14)
        df['rsi_6'] = TechnicalIndicators.rsi(df, 6)

        # MACD
        df = TechnicalIndicators.macd(df, fast=12, slow=26, signal=9)

        # 布林带
        df = TechnicalIndicators.bollinger_bands(df, period=20, std_dev=2)
        df['bb_width'] = df['bb_upper'] - df['bb_lower']

        # ATR (波动率)
        df['atr_14'] = TechnicalIndicators.atr(df, 14)
        df['atr_ratio'] = df['atr_14'] / df['close']

        # 随机指标
        df = TechnicalIndicators.stochastic(df, k_period=14, d_period=3)

        # 威廉指标
        df['williams_r'] = TechnicalIndicators.williams_r(df, 14)

        # CCI
        df['cci'] = TechnicalIndicators.cci(df, 20)

        # 动量指标
        df['momentum_10'] = TechnicalIndicators.momentum(df, 10)
        df['roc_12'] = TechnicalIndicators.roc(df, 12)

        # OBV
        df['obv'] = TechnicalIndicators.obv(df)
        df['obv_ma_5'] = df['obv'].rolling(5).mean()

        # 价格相对位置
        df['close_to_sma20'] = df['close'] / df['sma_20'] - 1
        df['close_to_sma60'] = df['close'] / df['sma_60'] - 1

        # 成交量变化
        df['volume_change'] = df['volume'].pct_change()
        df['volume_ma_ratio'] = df['volume'] / df['volume'].rolling(20).mean()

        # 检查技术指标数据有效性
        self._check_feature_validity(df, context="技术指标")

        return df

    def add_cross_sectional_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加时间序列统计特征（原"横截面特征"，命名保留以兼容）

        注意：虽然函数名包含"cross_sectional"，但实际添加的是
        单股票的时间序列统计特征，而非跨股票的横截面特征。

        Args:
            df: 单只股票数据

        Returns:
            添加特征后的DataFrame
        """
        df = df.copy()

        # 价格动量
        df['momentum_1w'] = df['close'].pct_change(5)
        df['momentum_1m'] = df['close'].pct_change(20)
        df['momentum_3m'] = df['close'].pct_change(60)

        # 波动率
        df['realized_vol_20'] = df['returns_1d'].rolling(20).std() * np.sqrt(252)

        # 偏度和峰度
        df['rolling_skew'] = df['returns_1d'].rolling(20).skew()
        df['rolling_kurt'] = df['returns_1d'].rolling(20).kurt()

        # 检查时间序列特征数据有效性
        self._check_feature_validity(df, context="时间序列特征")

        return df

    def create_target(
        self,
        df: pd.DataFrame,
        target_type: str = "return_1d",
        threshold: float = 0.0
    ) -> pd.DataFrame:
        """
        创建目标变量

        Args:
            df: 数据DataFrame
            target_type: 目标类型 ('return_1d', 'return_5d', 'direction_1d')
            threshold: 分类阈值（收益率高于此值为1，否则为0）

        Returns:
            添加目标列后的DataFrame
        """
        df = df.copy()

        if target_type == "return_1d":
            # 预测下一天收益率：(close[t+1] - close[t]) / close[t]
            df['target'] = (df['close'].shift(-1) - df['close']) / df['close']

        elif target_type == "return_5d":
            # 预测未来5天收益率：(close[t+5] - close[t]) / close[t]
            df['target'] = (df['close'].shift(-5) - df['close']) / df['close']

        elif target_type == "direction_1d":
            # 预测下一天涨跌方向
            df['target'] = (df['close'].shift(-1) > df['close']).astype(int)

        elif target_type == "direction_5d":
            # 预测未来5天涨跌方向
            df['target'] = (df['close'].shift(-5) > df['close']).astype(int)

        elif target_type == "high_low_5d":
            # 预测未来5天的最高/最低价
            df['target_high_5d'] = df['high'].shift(-5)
            df['target_low_5d'] = df['low'].shift(-5)
            df['target'] = df['target_high_5d']  # 默认用高价

        else:
            raise ValueError(f"Unknown target_type: {target_type}")

        # 删除最后一行（没有目标值）
        df = df.dropna(subset=['target'])

        return df

    def prepare_training_data(
        self,
        df: pd.DataFrame,
        feature_cols: Optional[List[str]] = None,
        target_col: str = "target",
        handle_missing: str = "drop"
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        准备训练数据

        Args:
            df: 完整数据DataFrame
            feature_cols: 特征列名列表（None则自动推断）
            target_col: 目标列名
            handle_missing: 处理缺失值方式 ('drop', 'fill', 'none')

        Returns:
            (X, y) 特征矩阵和目标向量
        """
        df = df.copy()

        # 处理缺失值
        if handle_missing == "drop":
            df = df.dropna(subset=[target_col])
        elif handle_missing == "fill":
            df = df.fillna(df.mean())

        # 确定特征列
        if feature_cols is None:
            exclude_cols = [
                'datetime', 'symbol', 'interval', 'target',
                'target_high_5d', 'target_low_5d', 'report_date',
                'ann_date', 'end_date', 'ts_code', 'exchange',
                'open_qfq', 'high_qfq', 'low_qfq', 'close_qfq',  # 复权价格列
                'volume_ma_5', 'volume_ma_20',  # 移动平均成交量（会泄露未来信息）
                'turnover_ma_5', 'turnover_ma_20',  # 移动平均换手率
                'high_60', 'low_60', 'price_position',  # 价格位置特征
                'day_of_week', 'month', 'quarter',  # 时间特征（可能不需要）
            ]
            feature_cols = [c for c in df.columns
                           if c not in exclude_cols
                           and pd.api.types.is_numeric_dtype(df[c])]

        # 过滤掉包含非数值数据的列
        valid_features = []
        for col in feature_cols:
            try:
                # 测试是否可以转换为数值
                if df[col].notna().any():  # 至少有一个非空值
                    pd.to_numeric(df[col], errors='coerce')
                    valid_features.append(col)
            except:
                pass

        self.feature_cols = valid_features if valid_features else feature_cols

        # 提取特征和目标
        X = df[feature_cols].values
        y = df[target_col].values

        logger.info(f"Prepared training data: X shape={X.shape}, y shape={y.shape}")

        # 打印特征列表
        self._log_feature_list()

        return X, y

    def _log_feature_list(self) -> None:
        """打印特征列表（按类别分组）"""
        if not self.feature_cols:
            logger.warning("No features to display")
            return

        # 按类别分组
        features_by_category = {
            '原始OHLCV数据': ['open', 'high', 'low', 'close', 'volume', 'returns_1d'],
            '移动平均线类': ['sma_5', 'sma_10', 'sma_20', 'sma_60', 'ema_12', 'ema_26'],
            '相对强弱指标类': ['rsi_14', 'rsi_6'],
            'MACD指标类': ['macd', 'macd_signal', 'macd_hist'],
            '布林带指标类': ['bb_upper', 'bb_middle', 'bb_lower', 'bb_width', 'bb_pct'],
            '波动率指标类': ['atr_14', 'atr_ratio'],
            '随机指标类': ['stoch_k', 'stoch_d'],
            '威廉指标': ['williams_r'],
            '商品通道指标': ['cci'],
            '动量指标类': ['momentum_10', 'roc_12'],
            '成交量指标类': ['obv', 'obv_ma_5', 'volume_change', 'volume_ma_ratio'],
            '价格位置类': ['close_to_sma20', 'close_to_sma60'],
            '多周期动量类': ['momentum_1w', 'momentum_1m', 'momentum_3m'],
            '时间序列统计特征类': ['realized_vol_20', 'rolling_skew', 'rolling_kurt'],
            '精选核心特征类': [
                'momentum_5d', 'momentum_20d', 'momentum_60d',
                'close_position', 'high_low_range', 'close_vs_sma20', 'close_vs_sma60',
                'volume_price_corr', 'volatility_20', 'trend_strength',
                'up_rate_20d', 'up_rate_40d', 'drawdown_from_60d_high',
                'sma60_slope', 'sma20_below_sma60', 'bull_regime_uprate', 'bear_regime_momentum'
            ],
        }

        logger.info('=' * 80)
        logger.info(f'训练使用的特征列表（共 {len(self.feature_cols)} 个）')
        logger.info('=' * 80)

        # 打印每个类别
        category_num = 1
        for category, expected_features in features_by_category.items():
            # 找出实际存在的特征
            actual_features = [f for f in expected_features if f in self.feature_cols]

            if actual_features:
                logger.info(f'{category_num}. {category} ({len(actual_features)}个): {", ".join(actual_features)}')
                category_num += 1

        # 检查是否有未分类的特征
        all_categorized = set()
        for expected_features in features_by_category.values():
            all_categorized.update(expected_features)

        uncategorized = [f for f in self.feature_cols if f not in all_categorized]
        if uncategorized:
            logger.info(f'{category_num}. 未分类特征 ({len(uncategorized)}个): {", ".join(uncategorized)}')

        logger.info('=' * 80)

    def fit_scaler(self, X: np.ndarray) -> 'FeatureEngineer':
        """
        拟合标准化器

        Args:
            X: 特征矩阵

        Returns:
            self
        """
        if self.scaler is not None:
            self.scaler.fit(X)
            logger.info(f"Fitted {self.scaler_type} scaler")
        return self

    def transform(self, X: np.ndarray) -> np.ndarray:
        """
        应用标准化

        Args:
            X: 特征矩阵

        Returns:
            标准化后的特征矩阵
        """
        if self.scaler is not None and hasattr(self.scaler, 'scale_'):
            return self.scaler.transform(X)
        return X

    def fit_transform(self, X: np.ndarray) -> np.ndarray:
        """
        拟合并转换

        Args:
            X: 特征矩阵

        Returns:
            标准化后的特征矩阵
        """
        if self.scaler is not None:
            return self.scaler.fit_transform(X)
        return X

    def inverse_transform_predictions(
        self,
        predictions: np.ndarray,
        reference_idx: int = 0
    ) -> np.ndarray:
        """
        反标准化预测结果（如果需要）

        Args:
            predictions: 标准化后的预测值
            reference_idx: 参考索引（用于多输出情况）

        Returns:
            原始尺度的预测值
        """
        if self.scaler is None:
            return predictions

        # 对于单输出回归，可能不需要反标准化
        # 这里假设预测值是收益率，不需要反标准化
        return predictions

    def select_features_by_importance(
        self,
        df: pd.DataFrame,
        importance: Dict[str, float],
        top_k: int = 50
    ) -> List[str]:
        """
        根据特征重要性选择特征

        Args:
            df: 数据DataFrame
            importance: 特征重要性字典 {feature: importance}
            top_k: 选择的特征数量

        Returns:
            选择的特征列名列表
        """
        # 按重要性排序
        sorted_features = sorted(importance.items(), key=lambda x: x[1], reverse=True)

        # 选择top_k
        selected = [f[0] for f in sorted_features[:top_k] if f[0] in df.columns]

        logger.info(f"Selected {len(selected)} features out of {len(importance)}")

        return selected

    def add_essential_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加精选核心特征（约10个高价值特征）

        专为小样本设计：避免维度灾难，保持样本/特征比 ≈ 13:1

        Args:
            df: 包含OHLCV和基础技术指标的DataFrame

        Returns:
            添加核心特征后的DataFrame
        """
        df = df.copy()

        # 1. 多周期动量（3个）- 捕捉短中长三个时间尺度
        df['momentum_5d'] = df['close'].pct_change(5)
        df['momentum_20d'] = df['close'].pct_change(20)
        df['momentum_60d'] = df['close'].pct_change(60)

        # 2. 价格形态（3个）- K线结构和趋势位置
        hl_range = df['high'] - df['low']
        df['close_position'] = (df['close'] - df['low']) / (hl_range + 1e-8)
        df['high_low_range'] = hl_range / (df['close'] + 1e-8)
        if 'sma_20' in df.columns:
            df['close_vs_sma20'] = (df['close'] / (df['sma_20'] + 1e-8)) - 1
        if 'sma_60' in df.columns:
            df['close_vs_sma60'] = (df['close'] / (df['sma_60'] + 1e-8)) - 1

        # 3. 量价关系（2个）- 量价配合信号
        df['volume_price_corr'] = df['volume'].rolling(20, min_periods=10).corr(df['close'])
        vol_ma20 = df['volume'].rolling(20, min_periods=10).mean()
        df['volume_ma_ratio'] = df['volume'] / (vol_ma20 + 1e-8)

        # 4. 波动率（1个）- 年化已实现波动率（市场情绪）
        if 'returns_1d' in df.columns:
            df['volatility_20'] = df['returns_1d'].rolling(20, min_periods=10).std() * np.sqrt(252)

        # 5. ATR标准化趋势强度（1个）
        if 'atr_14' in df.columns:
            df['trend_strength'] = (
                (df['close'] - df['close'].shift(20)) / (df['atr_14'] + 1e-8)
            )

        # 6. 多空胜率（2个）- 直接反映市场多空状态，是判断牛熊切换的关键
        if 'returns_1d' in df.columns:
            # 最近20日上涨天数占比（0~1，>0.5为偏多市，<0.5为偏空市）
            df['up_rate_20d'] = (df['returns_1d'] > 0).rolling(20, min_periods=10).mean()
            # 最近40日上涨天数占比（中期多空信号）
            df['up_rate_40d'] = (df['returns_1d'] > 0).rolling(40, min_periods=20).mean()

        # 7. 相对高点回撤（1个）- 判断是否处于下跌趋势
        # (当前价 - 60日最高价) / 60日最高价，接近0=高位，大幅负值=下跌趋势
        high_60d = df['close'].rolling(60, min_periods=20).max()
        df['drawdown_from_60d_high'] = (df['close'] - high_60d) / (high_60d + 1e-8)

        # 8. 趋势方向（2个）- 区分"牛市中的临时回调"与"持续下跌趋势"
        # 关键洞察：up_rate低时，牛市会反弹，熊市会继续下跌
        # SMA60斜率（20日变化）：正=上升趋势，负=下降趋势
        if 'sma_60' in df.columns:
            sma60_20d_ago = df['sma_60'].shift(20)
            df['sma60_slope'] = (df['sma_60'] - sma60_20d_ago) / (sma60_20d_ago.abs() + 1e-8)
        # SMA20是否低于SMA60（死叉信号）：1=熊市排列，0=牛市排列
        if 'sma_20' in df.columns and 'sma_60' in df.columns:
            df['sma20_below_sma60'] = (df['sma_20'] < df['sma_60']).astype(float)

        # 9. 牛熊体制交互特征（2个）- 核心特征：均值回归仅在牛市体制下有效
        # 牛市体制多空胜率：牛市中低up_rate=反弹信号，熊市中不信此信号
        if 'up_rate_20d' in df.columns and 'sma20_below_sma60' in df.columns:
            # 牛市体制信号：仅在SMA20>SMA60时保留up_rate信号（熊市归零）
            df['bull_regime_uprate'] = df['up_rate_20d'] * (1 - df['sma20_below_sma60'])
        # 熊市体制动量延续：熊市中的负动量信号更可靠
        if 'momentum_3m' in df.columns and 'sma20_below_sma60' in df.columns:
            df['bear_regime_momentum'] = df['momentum_3m'] * df['sma20_below_sma60']

        logger.info(
            f"Added essential features: momentum_5d/20d/60d, "
            f"close_position, high_low_range, close_vs_sma20/60, "
            f"volume_price_corr, volume_ma_ratio, volatility_20, trend_strength, "
            f"up_rate_20d/40d, drawdown_from_60d_high, sma60_slope, sma20_below_sma60, "
            f"bull_regime_uprate, bear_regime_momentum"
        )

        return df

    def select_top_features(
        self,
        X: np.ndarray,
        feature_names: List[str],
        model_importance: Dict[str, float],
        top_k: int = 20,
        corr_threshold: float = 0.85,
        mandatory_features: Optional[List[str]] = None,
        excluded_features: Optional[List[str]] = None
    ) -> List[str]:
        """
        基于特征重要性和相关性去重，选择Top K特征

        策略：
        1. 先添加强制特征（mandatory_features），确保关键信号不被过滤
        2. 跳过excluded_features（被强制特征替代的原始特征）
        3. 按重要性降序排列，逐个添加，跳过与已选特征高相关（>corr_threshold）的特征
        4. 直到达到top_k个

        Args:
            X: 特征矩阵
            feature_names: 特征名称列表
            model_importance: 模型特征重要性字典 {feature: importance}
            top_k: 目标特征数量
            corr_threshold: 相关系数阈值（超过则认为冗余）
            mandatory_features: 必须包含的特征列表（无论重要性高低）
            excluded_features: 排除的特征列表（被强制特征替代的原始特征）

        Returns:
            选中的特征名称列表
        """
        if not model_importance:
            logger.warning("No feature importance available, returning all features")
            return feature_names[:top_k]

        # 构建特征名->列索引映射
        name_to_idx = {name: i for i, name in enumerate(feature_names)}

        selected = []

        # 先添加强制特征（跳过不在特征列表中的）
        if mandatory_features:
            for feat_name in mandatory_features:
                if feat_name in name_to_idx and feat_name not in selected:
                    selected.append(feat_name)
                    logger.info(f"Mandatory feature added: {feat_name}")

        # 按重要性降序排列
        sorted_features = sorted(
            model_importance.items(),
            key=lambda x: x[1],
            reverse=True
        )

        selected_set = set(selected)
        for feat_name, importance in sorted_features:
            if len(selected) >= top_k:
                break

            if feat_name not in name_to_idx:
                continue

            # 强制特征已添加，跳过
            if feat_name in selected_set:
                continue

            # 跳过被强制特征替代的原始特征
            if excluded_features and feat_name in excluded_features:
                continue
            feat_idx = name_to_idx[feat_name]

            # 检查与已选特征的最大相关系数
            is_redundant = False
            for sel_name in selected:
                sel_idx = name_to_idx[sel_name]
                corr = np.abs(np.corrcoef(X[:, feat_idx], X[:, sel_idx])[0, 1])
                if np.isnan(corr):
                    corr = 0.0
                if corr > corr_threshold:
                    is_redundant = True
                    break

            if not is_redundant:
                selected.append(feat_name)
                selected_set.add(feat_name)

        logger.info(
            f"Feature selection: {len(model_importance)} → {len(selected)} features "
            f"(top_k={top_k}, corr_threshold={corr_threshold})"
        )

        # 打印最终选中的特征列表
        logger.info(f"Selected features ({len(selected)}): {', '.join(selected)}")

        return selected

    def remove_correlated_features(
        self,
        df: pd.DataFrame,
        threshold: float = 0.95
    ) -> List[str]:
        """
        移除高度相关的特征

        Args:
            df: 数据DataFrame
            threshold: 相关系数阈值

        Returns:
            保留的特征列名列表
        """
        # 计算相关系数矩阵
        corr_matrix = df.corr().abs()

        # 找出高度相关的特征对
        upper_triangle = corr_matrix.where(
            np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
        )

        # 找出相关系数超过阈值的列
        to_drop = [column for column in upper_triangle.columns
                   if any(upper_triangle[column] > threshold)]

        # 保留的特征
        selected = [c for c in df.columns if c not in to_drop]

        logger.info(f"Dropped {len(to_drop)} correlated features, kept {len(selected)}")

        return selected


def prepare_data_for_training(
    df: pd.DataFrame,
    target_type: str = "return_1d",
    use_technical: bool = True,
    handle_missing: str = "drop"
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, List[str]]:
    """
    便捷函数：准备完整的训练数据

    Args:
        df: 原始数据DataFrame
        target_type: 目标类型
        use_technical: 是否使用技术指标
        handle_missing: 处理缺失值方式

    Returns:
        (X_train, X_val, X_test, y_train, y_val, y_test, feature_cols)
    """
    engineer = FeatureEngineer(scaler_type='standard')

    # 添加技术指标
    if use_technical:
        df = engineer.add_technical_indicators(df)
        df = engineer.add_cross_sectional_features(df)

    # 创建目标
    df = engineer.create_target(df, target_type=target_type)

    # 准备训练数据
    X, y = engineer.prepare_training_data(df, handle_missing=handle_missing)

    # 划分数据集
    n = len(df)
    train_end = int(n * 0.7)
    val_end = int(n * 0.85)

    X_train, y_train = X[:train_end], y[:train_end]
    X_val, y_val = X[train_end:val_end], y[train_end:val_end]
    X_test, y_test = X[val_end:], y[val_end:]

    # 标准化（只在训练集上fit）
    X_train = engineer.fit_transform(X_train)
    X_val = engineer.transform(X_val)
    X_test = engineer.transform(X_test)

    feature_cols = engineer.feature_cols

    return X_train, X_val, X_test, y_train, y_val, y_test, feature_cols


class FeatureSelector:
    """
    特征选择器

    使用多种方法选择最重要的特征
    """

    def __init__(self, method: str = "shap", top_k: int = 50):
        """
        初始化特征选择器

        Args:
            method: 选择方法 ('shap', 'importance', 'correlation')
            top_k: 保留的特征数量
        """
        self.method = method
        self.top_k = top_k
        self.selected_features = None
        self.scores = None

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        feature_names: List[str],
        model: Any = None
    ) -> 'FeatureSelector':
        """
        拟合特征选择器

        Args:
            X: 特征矩阵
            y: 目标向量
            feature_names: 特征名称列表
            model: 训练好的模型（用于SHAP）

        Returns:
            self
        """
        if self.method == "shap":
            self._fit_shap(X, y, feature_names, model)
        elif self.method == "importance":
            self._fit_importance(X, y, feature_names, model)
        elif self.method == "correlation":
            self._fit_correlation(X, y, feature_names)
        else:
            raise ValueError(f"Unknown method: {self.method}")

        return self

    def _fit_shap(
        self,
        X: np.ndarray,
        y: np.ndarray,
        feature_names: List[str],
        model: Any
    ) -> None:
        """使用SHAP值进行特征选择"""
        try:
            import shap

            # 计算SHAP值
            if hasattr(model, 'model') and hasattr(model.model, 'predict'):
                # LightGBM模型
                explainer = shap.TreeExplainer(model.model)
            else:
                # 使用通用explainer
                explainer = shap.Explainer(model, X[:100])

            shap_values = explainer.shap_values(X[:100])

            # 计算平均绝对SHAP值
            if isinstance(shap_values, list):
                shap_values = shap_values[0]

            mean_abs_shap = np.abs(shap_values).mean(axis=0)

            # 创建特征分数字典
            self.scores = dict(zip(feature_names, mean_abs_shap))

            # 选择top_k特征
            sorted_features = sorted(self.scores.items(), key=lambda x: x[1], reverse=True)
            self.selected_features = [f[0] for f in sorted_features[:self.top_k]]

            logger.info(f"Selected {len(self.selected_features)} features using SHAP")

        except ImportError:
            logger.warning("SHAP not installed, falling back to importance method")
            self._fit_importance(X, y, feature_names, model)

    def _fit_importance(
        self,
        X: np.ndarray,
        y: np.ndarray,
        feature_names: List[str],
        model: Any
    ) -> None:
        """使用模型特征重要性"""
        if hasattr(model, 'feature_importance') and model.feature_importance:
            self.scores = model.feature_importance.copy()
            sorted_features = sorted(self.scores.items(), key=lambda x: x[1], reverse=True)
            self.selected_features = [f[0] for f in sorted_features[:self.top_k]]
            logger.info(f"Selected {len(self.selected_features)} features using importance")
        else:
            logger.warning("Model doesn't have feature_importance, using all features")
            self.selected_features = feature_names

    def _fit_correlation(
        self,
        X: np.ndarray,
        y: np.ndarray,
        feature_names: List[str]
    ) -> None:
        """使用相关性进行特征选择"""
        # 计算每个特征与目标的相关系数
        correlations = {}
        for i, name in enumerate(feature_names):
            corr = np.corrcoef(X[:, i], y)[0, 1]
            if not np.isnan(corr):
                correlations[name] = abs(corr)

        self.scores = correlations
        sorted_features = sorted(correlations.items(), key=lambda x: x[1], reverse=True)
        self.selected_features = [f[0] for f in sorted_features[:self.top_k]]

        logger.info(f"Selected {len(self.selected_features)} features using correlation")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        应用特征选择

        Args:
            df: DataFrame

        Returns:
            只包含选中特征的DataFrame
        """
        if self.selected_features is None:
            return df

        return df[self.selected_features].copy()

    def get_feature_scores(self) -> Dict[str, float]:
        """
        获取特征分数

        Returns:
            特征分数字典
        """
        return self.scores or {}

    def get_selected_features(self) -> List[str]:
        """
        获取选中的特征列表

        Returns:
            特征名称列表
        """
        return self.selected_features or []


def select_features(
    df: pd.DataFrame,
    y: np.ndarray,
    feature_names: List[str],
    model: Any = None,
    method: str = "shap",
    top_k: int = 50
) -> Tuple[pd.DataFrame, List[str]]:
    """
    便捷函数：特征选择

    Args:
        df: 特征DataFrame
        y: 目标向量
        feature_names: 特征名称
        model: 训练好的模型
        method: 选择方法
        top_k: 保留的特征数量

    Returns:
        (df_selected, selected_features)
    """
    selector = FeatureSelector(method=method, top_k=top_k)
    X = df[feature_names].values

    selector.fit(X, y, feature_names, model)
    df_selected = selector.transform(df)
    selected_features = selector.get_selected_features()

    return df_selected, selected_features
