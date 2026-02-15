"""
101 Formulaic Alphas - 101个公式化Alpha因子

基于论文实现的全部101个量化交易因子：
Kakushadze, Z. (2015). "101 Formulaic Alphas"

论文链接: https://arxiv.org/abs/1601.00991
版权声明: 公式由 WorldQuant LLC 授权使用

## 基本信息

- 因子数量: 101个
- 平均持有期: 0.6-6.4天（短期交易因子）
- 平均相关性: 15.9%（低相关性，适合组合）
- 数据需求: OHLCV + VWAP + 市值 + 申万行业分类

## 因子分类概述

### 1. 动量反转类 (约25个因子)

捕捉价格、收益率的动量和反转信号，识别趋势转折点。

典型因子：
- Alpha#1: 波动率聚类反转因子 - 负收益期间的波动率聚类反转
- Alpha#4: 低价动量因子 - 持续创新低的超跌反弹
- Alpha#7: 成交量条件下的收盘价反转 - 放量反转信号
- Alpha#9, #10: 收盘价动量趋势切换 - 识别价格动量转换点
- Alpha#19: 短期反转与长期趋势交互 - 复合反转信号
- Alpha#46, #49, #51: 价格加速度状态切换 - 趋势加速度反转

特点: 基于价格时间序列的统计特征，简单有效

### 2. 量价关系类 (约30个因子)

考察成交量与价格的相关性、协方差，识别量价背离。

典型因子：
- Alpha#2: 成交量-价格背离因子 - 量价负相关预示反转
- Alpha#3, #6: 开盘价-成交量相关性 - 开盘异常交易行为
- Alpha#11-#16: VWAP偏离与成交量动量 - 多重量价信号
- Alpha#40: 高点波动率与高点-成交量相关性反转
- Alpha#44: 最高价与成交量排名相关性 - 追高行为过度
- Alpha#50: 成交量-VWAP排名相关性极值反转

特点: 利用量价关系的异常，捕捉市场情绪变化

### 3. VWAP偏离类 (约15个因子)

基于VWAP（成交量加权平均价）的价格偏离度分析。

典型因子：
- Alpha#5: VWAP偏离度与开盘价偏离度交叉 - 双重价格偏离
- Alpha#28: 平均成交量-低点相关性与中价偏离
- Alpha#41: 高低点几何平均与VWAP偏离 - 定价偏差信号
- Alpha#42: VWAP-收盘价偏离比率排名 - 相对偏离强度
- Alpha#57: VWAP偏离除以收盘价高点位置衰减

特点: VWAP作为"公允价格"的锚点，偏离后回归

### 4. 行业中性类 (约12个因子)

使用行业中性化处理，剔除行业因素，聚焦个股特异信号。

典型因子：
- Alpha#48: 行业中性化收益自相关强度 - 个股特异动量
- Alpha#58, #59: 行业中性VWAP-成交量相关性衰减时序排名
- Alpha#63: 行业中性收盘价动量衰减与混合价格-均量相关性差
- Alpha#67-#70: 行业板块/行业/细分行业三级中性化
- Alpha#80: 行业中性开盘-高点混合动量
- Alpha#100: 双重细分行业中性化复合因子

特点: 剔除行业Beta，适合多空组合策略

### 5. 复合因子类 (约8个因子)

多个子因子加权组合，多维度信号综合验证。

典型因子：
- Alpha#36: 五因子加权复合模型 - 综合日内振幅、缺口、收益、VWAP相关性、长期趋势
- Alpha#47: 价格倒数-成交量-高点上影线复合 - 低价股放量上影线
- Alpha#71: 收盘-均量相关性衰减与低开-VWAP偏离衰减的最大值
- Alpha#96: VWAP-成交量排名相关性与收盘-均量相关性峰值的双通道

特点: 多维度交叉验证，提高信号稳定性

### 6. 时序排名类 (约20个因子)

使用时序排名（ts_rank）计算价格、成交量在历史窗口中的相对位置。

典型因子：
- Alpha#17: 收盘价动量与成交量比率的三重时序排名交互
- Alpha#35: 成交量、价格位置、收益率三维时序排名交互
- Alpha#43: 相对成交量与价格动量双重时序排名
- Alpha#52: 低点动量与长期收益趋势叠加成交量时序排名
- Alpha#84: VWAP相对高点位置时序排名的收盘价动量符号幂次

特点: 时序百分位排名，消除绝对值影响

### 7. 非线性变换类 (约6个因子)

使用幂次、对数、符号幂等非线性变换，放大信号特征。

典型因子：
- Alpha#1: SignedPower变换 - 有向幂次变换
- Alpha#54: 低点-收盘价-开盘价-高点非线性幂次比率 - 5次幂放大
- Alpha#81: VWAP-均量相关性4次幂排名乘积
- Alpha#84: SignedPower(时序排名, 价格动量) - 动量符号调制
- Alpha#85, #94: 幂次交互 - rank^rank 非线性组合

特点: 非线性放大特定形态特征，提高敏感度

## 使用建议

### 因子选择策略

1. **高频交易**: 优先选择简单的量价关系类因子 (Alpha#2, #3, #6, #12)
2. **多空对冲**: 选择行业中性类因子 (Alpha#48, #58, #67-#70, #100)
3. **趋势跟随**: 选择动量反转类因子 (Alpha#1, #7, #19, #52)
4. **稳健策略**: 选择复合因子类 (Alpha#36, #71, #96)
5. **市值中性**: 结合市值因子 (Alpha#25, #56) 与行业中性因子

### 因子组合建议

- **低相关性**: 101个因子平均相关性15.9%，适合构建因子池
- **分散化**: 从不同分类中选择5-20个因子组合
- **动态调整**: 根据市场状态动态调整因子权重
- **回测验证**: 在A股市场上充分回测验证因子有效性

### 注意事项

1. **数据质量**: 需要高质量的OHLCV、VWAP、市值、行业分类数据
2. **计算成本**: 部分因子计算复杂度高，注意计算资源优化
3. **窗口期**: 因子需要450天历史数据作为lookback buffer
4. **NaN处理**: 计算结果中的NaN需要妥善处理
5. **本地化**: 原论文基于美股，A股市场需要调整和验证

## 函数接口

每个因子是一个函数：
    alpha_NNN(data: AlphaDataPanel) -> pd.DataFrame

输入：AlphaDataPanel 包含 open, high, low, close, volume, amount, returns, vwap, cap, industry_map
输出：DataFrame (index=dates, columns=symbols) 因子值，值越大表示预期收益越高

## 示例

```python
from src.alphas import AlphaEngine

# 初始化引擎
engine = AlphaEngine()

# 计算单个因子
df = engine.compute_alpha(101, ['600382', '000001'], '2025-01-01', '2025-01-31')

# 批量计算因子
results = engine.compute_alphas_batch([1, 6, 101], ['600382', '000001'],
                                       '2025-01-01', '2025-01-31')

# 获取截面快照（用于选股）
snapshot = engine.get_alpha_snapshot(101, ['600382', '000001'], '2025-01-27')
```
"""
import numpy as np
import pandas as pd
import math

from src.alphas.data_adapter import AlphaDataPanel
from src.alphas.operators import (
    delay, delta, ts_sum, ts_min, ts_max, ts_argmax, ts_argmin,
    ts_rank, product, stddev, correlation, covariance, decay_linear,
    rank, scale, indneutralize, signedpower, sign, log, abs_val
)


def _where(cond, a, b):
    """Ternary operator: where cond is True use a, else b.
    Works with DataFrames and scalars.
    """
    if isinstance(a, (pd.DataFrame, pd.Series)) or isinstance(b, (pd.DataFrame, pd.Series)):
        # Ensure all are aligned DataFrames
        if isinstance(cond, (pd.DataFrame, pd.Series)):
            return pd.DataFrame(
                np.where(cond, a, b),
                index=cond.index,
                columns=cond.columns if isinstance(cond, pd.DataFrame) else None
            )
    return np.where(cond, a, b)


# ============================================================================
# Alpha #1 - #10
# ============================================================================

def alpha_001(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#1: 波动率聚类反转因子

    当收益为负时使用波动率，否则使用收盘价进行平方处理，
    寻找5日内最大值出现的位置，然后对该位置进行排名。
    用途：捕捉负收益期间的波动率聚类效应，负收益伴随高波动往往预示反转。
    持有期：0.6-6.4天
    """
    cond = data.returns < 0
    inner = _where(cond, stddev(data.returns, 20), data.close)
    sp = signedpower(inner, 2.0)
    return rank(ts_argmax(sp, 5)) - 0.5


def alpha_002(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#2: 成交量-价格背离因子

    考察成交量变化率与开盘价涨跌幅之间的相关性。
    用途：当量价背离时（负相关），可能预示趋势反转。
    持有期：0.6-6.4天
    """
    return -1 * correlation(
        rank(delta(log(data.volume), 2)),
        rank((data.close - data.open) / data.open),
        6
    )


def alpha_003(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#3: 开盘价-成交量相关性反转

    开盘价与成交量的10日相关性，负相关表示低开高量或高开低量。
    用途：开盘价与成交量的异常关系可能预示市场情绪变化。
    持有期：0.6-6.4天
    """
    return -1 * correlation(rank(data.open), rank(data.volume), 10)


def alpha_004(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#4: 低价动量因子

    低价的9日时序排名，反映股价在近期的相对位置。
    用途：持续创新低的股票可能超跌反弹，反之亦然。
    持有期：0.6-6.4天
    """
    return -1 * ts_rank(rank(data.low), 9)


def alpha_005(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#5: VWAP偏离度与开盘价偏离度交叉因子

    结合开盘价相对10日均VWAP的偏离，以及收盘价与VWAP的偏离。
    用途：双重价格偏离信号，偏离越大反转动能越强。
    持有期：0.6-6.4天
    """
    return rank(data.open - ts_sum(data.vwap, 10) / 10) * (-1 * abs_val(rank(data.close - data.vwap)))


def alpha_006(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#6: 开盘价-成交量相关性（简化版）

    与Alpha#3类似，但使用原始值而非排名。
    用途：开盘价与成交量的负相关暗示异常交易行为。
    持有期：0.6-6.4天
    """
    return -1 * correlation(data.open, data.volume, 10)


def alpha_007(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#7: 成交量条件下的收盘价反转

    当成交量大于20日均值时，使用7日收盘价变化的时序排名与符号。
    用途：放量情况下的价格反转信号更可靠。
    持有期：0.6-6.4天
    """
    adv20 = data.adv(20)
    cond = adv20 < data.volume
    val = -1 * ts_rank(abs_val(delta(data.close, 7)), 60) * sign(delta(data.close, 7))
    return _where(cond, val, -1)


def alpha_008(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#8: 开盘价-收益动量交互因子

    5日开盘价之和与5日收益之和的乘积，考察其10日动量。
    用途：开盘价与收益的联合动量衰减预示反转。
    持有期：0.6-6.4天
    """
    inner = ts_sum(data.open, 5) * ts_sum(data.returns, 5)
    return -1 * rank(inner - delay(inner, 10))


def alpha_009(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#9: 收盘价动量趋势切换因子

    根据1日收盘价变化的5日最小值和最大值判断趋势状态。
    用途：识别价格动量的趋势转换点。
    持有期：0.6-6.4天
    """
    d1 = delta(data.close, 1)
    cond1 = ts_min(d1, 5) > 0
    cond2 = ts_max(d1, 5) < 0
    return _where(cond1, d1, _where(cond2, d1, -1 * d1))


def alpha_010(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#10: 收盘价动量趋势切换因子（排名版）

    与Alpha#9类似，但对结果进行排名处理。
    用途：排名后的趋势切换信号，减少异常值影响。
    持有期：0.6-6.4天
    """
    d1 = delta(data.close, 1)
    cond1 = ts_min(d1, 4) > 0
    cond2 = ts_max(d1, 4) < 0
    inner = _where(cond1, d1, _where(cond2, d1, -1 * d1))
    return rank(inner)


# ============================================================================
# Alpha #11 - #20
# ============================================================================

def alpha_011(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#11: VWAP偏离度与成交量动量交叉

    VWAP与收盘价3日偏离的极值与3日成交量变化的交互。
    用途：价格偏离与成交量变化的共振信号。
    持有期：0.6-6.4天
    """
    diff = data.vwap - data.close
    return (rank(ts_max(diff, 3)) + rank(ts_min(diff, 3))) * rank(delta(data.volume, 3))


def alpha_012(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#12: 成交量-价格动量因子

    成交量变化的符号乘以收盘价变化的反向。
    用途：量增价跌或量减价涨的背离信号。
    持有期：0.6-6.4天
    """
    return sign(delta(data.volume, 1)) * (-1 * delta(data.close, 1))


def alpha_013(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#13: 收盘价-成交量协方差反转

    收盘价与成交量排名的5日协方差。
    用途：量价协同性的反转，高协方差后可能反转。
    持有期：0.6-6.4天
    """
    return -1 * rank(covariance(rank(data.close), rank(data.volume), 5))


def alpha_014(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#14: 收益动量与开盘量相关性交互

    3日收益变化与10日开盘价-成交量相关性的交互。
    用途：收益动量衰减伴随开盘量相关性异常。
    持有期：0.6-6.4天
    """
    return -1 * rank(delta(data.returns, 3)) * correlation(data.open, data.volume, 10)


def alpha_015(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#15: 最高价-成交量相关性累计反转

    最高价与成交量排名的3日相关性，累计3日后反转。
    用途：高点追涨行为的反转信号。
    持有期：0.6-6.4天
    """
    return -1 * ts_sum(rank(correlation(rank(data.high), rank(data.volume), 3)), 3)


def alpha_016(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#16: 最高价-成交量协方差反转

    与Alpha#13类似，但使用最高价。
    用途：高点附近的量价协同性反转。
    持有期：0.6-6.4天
    """
    return -1 * rank(covariance(rank(data.high), rank(data.volume), 5))


def alpha_017(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#17: 收盘价动量与成交量比率的复合因子

    收盘价10日排名、价格二阶导数、相对成交量5日排名的三重交互。
    用途：多维度动量信号的复合，识别趋势转折点。
    持有期：0.6-6.4天
    """
    adv20 = data.adv(20)
    return ((-1 * rank(ts_rank(data.close, 10))) *
            rank(delta(delta(data.close, 1), 1)) *
            rank(ts_rank(data.volume / adv20, 5)))


def alpha_018(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#18: 日内波动与价格相关性综合因子

    结合日内振幅波动率、日内涨幅、收盘价与开盘价相关性。
    用途：日内行为模式的异常检测。
    持有期：0.6-6.4天
    """
    return -1 * rank(
        stddev(abs_val(data.close - data.open), 5) +
        (data.close - data.open) +
        correlation(data.close, data.open, 10)
    )


def alpha_019(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#19: 短期反转与长期趋势交互因子

    7日价格变化的符号反转，乘以250日累计收益排名。
    用途：短期反转叠加长期趋势的复合信号。
    持有期：0.6-6.4天
    """
    return (-1 * sign((data.close - delay(data.close, 7)) + delta(data.close, 7))) * \
           (1 + rank(1 + ts_sum(data.returns, 250)))


def alpha_020(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#20: 开盘价缺口三重反转因子

    开盘价相对前一日高点、收盘价、低点的缺口大小。
    用途：开盘缺口的综合评估，多重缺口共振。
    持有期：0.6-6.4天
    """
    return ((-1 * rank(data.open - delay(data.high, 1))) *
            rank(data.open - delay(data.close, 1)) *
            rank(data.open - delay(data.low, 1)))


# ============================================================================
# Alpha #21 - #30
# ============================================================================

def alpha_021(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#21: 均值波动带突破与放量信号综合因子

    根据价格在均值波动带中的位置和成交量状态进行三重判断。
    用途：短期价格突破上下波动带，结合放量条件判断趋势强度。
    持有期：0.6-6.4天
    """
    sma8 = ts_sum(data.close, 8) / 8
    std8 = stddev(data.close, 8)
    sma2 = ts_sum(data.close, 2) / 2
    adv20 = data.adv(20)
    vol_ratio = data.volume / adv20

    cond1 = (sma8 + std8) < sma2
    cond2 = sma2 < (sma8 - std8)
    cond3 = vol_ratio >= 1

    return _where(cond1, -1, _where(cond2, 1, _where(cond3, 1, -1)))


def alpha_022(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#22: 高点-成交量相关性变化与波动率交互因子

    最高价与成交量相关性的5日变化，乘以收盘价波动率排名。
    用途：高点追逐行为的相关性转变，结合波动率识别反转时机。
    持有期：0.6-6.4天
    """
    return -1 * delta(correlation(data.high, data.volume, 5), 5) * rank(stddev(data.close, 20))


def alpha_023(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#23: 高点突破均值反转因子

    当最高价突破20日均值时，使用2日高点变化的反向信号。
    用途：创新高后的短期回调预期，均值回归策略。
    持有期：0.6-6.4天
    """
    cond = ts_sum(data.high, 20) / 20 < data.high
    return _where(cond, -1 * delta(data.high, 2), 0)


def alpha_024(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#24: 长期趋势强度判断因子

    根据100日移动平均的变化率判断趋势强度，弱势时看相对低点位置，强势时看短期反转。
    用途：区分震荡市和趋势市，采用不同的反转策略。
    持有期：0.6-6.4天
    """
    sma100 = ts_sum(data.close, 100) / 100
    ratio = delta(sma100, 100) / delay(data.close, 100)
    cond = ratio <= 0.05
    return _where(cond, -1 * (data.close - ts_min(data.close, 100)), -1 * delta(data.close, 3))


def alpha_025(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#25: 负收益-成交量-价格偏离综合因子

    负收益率与平均成交量、VWAP、上影线的四重交互排名。
    用途：下跌伴随放量和上影线，可能预示空头衰竭反转。
    持有期：0.6-6.4天
    """
    adv20 = data.adv(20)
    return rank(-1 * data.returns * adv20 * data.vwap * (data.high - data.close))


def alpha_026(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#26: 成交量-高点时序排名相关性峰值反转

    成交量与最高价的时序排名相关性，取3日内最大值后反转。
    用途：量价时序排名高度一致时的反转信号。
    持有期：0.6-6.4天
    """
    return -1 * ts_max(correlation(ts_rank(data.volume, 5), ts_rank(data.high, 5), 5), 3)


def alpha_027(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#27: 成交量-VWAP排名相关性状态判断

    成交量与VWAP排名的6日相关性均值，按排名判断多空状态。
    用途：量价排名相关性过高时做空，过低时做多。
    持有期：0.6-6.4天
    """
    inner = ts_sum(correlation(rank(data.volume), rank(data.vwap), 6), 2) / 2.0
    cond = rank(inner) > 0.5
    return _where(cond, -1, 1)


def alpha_028(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#28: 平均成交量-低点相关性与中价偏离

    20日均量与低点的相关性加上中间价，减去收盘价后标准化。
    用途：成交量与低点关系异常时的价格偏离修正信号。
    持有期：0.6-6.4天
    """
    adv20 = data.adv(20)
    return scale(correlation(adv20, data.low, 5) + (data.high + data.low) / 2 - data.close)


def alpha_029(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#29: 复杂价格动量多层排名与延迟收益结合

    价格5日变化的多重排名变换取极小值，叠加延迟6日的负收益时序排名。
    用途：复杂动量信号过滤，识别深度反转机会。
    持有期：0.6-6.4天
    """
    inner = rank(rank(-1 * rank(delta(data.close - 1, 5))))
    inner2 = scale(log(ts_sum(ts_min(inner, 2), 1)))
    inner3 = product(rank(rank(inner2)), 1)
    return ts_min(inner3, 5) + ts_rank(delay(-1 * data.returns, 6), 5)


def alpha_030(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#30: 价格连续符号与成交量比率因子

    3日连续价格变化符号累加，反向排名后乘以成交量比率。
    用途：连续涨跌的持续性判断，结合成交量强度过滤。
    持有期：0.6-6.4天
    """
    s1 = sign(data.close - delay(data.close, 1))
    s2 = sign(delay(data.close, 1) - delay(data.close, 2))
    s3 = sign(delay(data.close, 2) - delay(data.close, 3))
    return (1.0 - rank(s1 + s2 + s3)) * ts_sum(data.volume, 5) / ts_sum(data.volume, 20)


# ============================================================================
# Alpha #31 - #40
# ============================================================================

def alpha_031(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#31: 价格动量线性衰减与均量-低点相关性综合

    10日价格动量的多重排名线性衰减，叠加3日价格动量和均量-低点相关性符号。
    用途：复杂动量信号叠加，多维度识别趋势转折点。
    持有期：0.6-6.4天
    """
    adv20 = data.adv(20)
    return (rank(rank(rank(decay_linear(-1 * rank(rank(delta(data.close, 10))), 10)))) +
            rank(-1 * delta(data.close, 3)) +
            sign(scale(correlation(adv20, data.low, 12))))


def alpha_032(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#32: 短期均值偏离与长期VWAP相关性

    7日均价与当前收盘价偏离标准化，加上230日VWAP与延迟收盘价相关性的放大。
    用途：短期偏离叠加长期相关性变化，捕捉趋势持续性。
    持有期：0.6-6.4天
    """
    return (scale(ts_sum(data.close, 7) / 7 - data.close) +
            20 * scale(correlation(data.vwap, delay(data.close, 5), 230)))


def alpha_033(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#33: 开盘价缺口比率反转因子

    开盘价相对收盘价的缺口比率（1 - open/close）排名反转。
    用途：开盘缺口的反向信号，缺口越大反转动能越强。
    持有期：0.6-6.4天
    """
    return rank(-1 * (1 - data.open / data.close))


def alpha_034(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#34: 收益率波动比与价格动量稳定性

    2日与5日收益波动率比值的反向排名，加上1日价格动量的反向排名。
    用途：波动率下降且动量稳定时的趋势延续信号。
    持有期：0.6-6.4天
    """
    return rank(
        (1 - rank(stddev(data.returns, 2) / stddev(data.returns, 5))) +
        (1 - rank(delta(data.close, 1)))
    )


def alpha_035(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#35: 成交量、价格位置、收益率三维时序排名交互

    32日成交量时序排名，乘以16日价格位置反向时序排名，再乘以32日收益反向时序排名。
    用途：量价收益三维时序动量的交叉验证信号。
    持有期：0.6-6.4天
    """
    return (ts_rank(data.volume, 32) *
            (1 - ts_rank(data.close + data.high - data.low, 16)) *
            (1 - ts_rank(data.returns, 32)))


def alpha_036(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#36: 五因子加权复合模型

    综合日内振幅-延迟成交量相关性、开盘缺口、延迟收益时序排名、VWAP-均量相关性、长期均价-开盘价关系五个因子。
    用途：多维度加权复合信号，提高预测稳定性。
    持有期：0.6-6.4天
    """
    adv20 = data.adv(20)
    return (2.21 * rank(correlation(data.close - data.open, delay(data.volume, 1), 15)) +
            0.7 * rank(data.open - data.close) +
            0.73 * rank(ts_rank(delay(-1 * data.returns, 6), 5)) +
            rank(abs_val(correlation(data.vwap, adv20, 6))) +
            0.6 * rank((ts_sum(data.close, 200) / 200 - data.open) * (data.close - data.open)))


def alpha_037(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#37: 延迟开盘缺口与收盘价相关性双重排名

    延迟1日的开盘缺口与收盘价的200日相关性排名，加上开盘缺口本身的排名。
    用途：开盘行为与收盘价长期关系的稳定性信号。
    持有期：0.6-6.4天
    """
    return rank(correlation(delay(data.open - data.close, 1), data.close, 200)) + rank(data.open - data.close)


def alpha_038(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#38: 收盘价时序排名与收盘开盘比交互反转

    10日收盘价时序排名反向，乘以收盘开盘比排名。
    用途：价格位置与日内强度的复合反转信号。
    持有期：0.6-6.4天
    """
    return -1 * rank(ts_rank(data.close, 10)) * rank(data.close / data.open)


def alpha_039(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#39: 价格动量与成交量衰减叠加长期收益

    7日价格变化乘以相对成交量线性衰减的反向排名，再乘以250日累计收益排名。
    用途：短期动量衰减与长期趋势共振的反转信号。
    持有期：0.6-6.4天
    """
    adv20 = data.adv(20)
    return (-1 * rank(delta(data.close, 7) * (1 - rank(decay_linear(data.volume / adv20, 9)))) *
            (1 + rank(ts_sum(data.returns, 250))))


def alpha_040(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#40: 高点波动率与高点-成交量相关性反转

    10日最高价波动率排名反向，乘以高点与成交量的相关性。
    用途：高波动伴随高点-量相关性增强时的反转信号。
    持有期：0.6-6.4天
    """
    return -1 * rank(stddev(data.high, 10)) * correlation(data.high, data.volume, 10)


# ============================================================================
# Alpha #41 - #50
# ============================================================================

def alpha_041(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#41: 高低点几何平均与VWAP偏离

    最高价和最低价几何平均值与VWAP的偏离度。
    用途：价格几何中心与成交量加权均价的背离，捕捉定价偏差。
    持有期：0.6-6.4天
    """
    return np.sqrt(data.high * data.low) - data.vwap


def alpha_042(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#42: VWAP-收盘价偏离比率排名

    VWAP与收盘价偏离的排名比率（差值排名/和值排名）。
    用途：相对偏离强度信号，排名归一化消除绝对值影响。
    持有期：0.6-6.4天
    """
    return rank(data.vwap - data.close) / rank(data.vwap + data.close)


def alpha_043(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#43: 相对成交量与价格动量双重时序排名

    20日相对成交量时序排名，乘以8日收盘价反向动量时序排名。
    用途：成交量放大伴随价格下跌的反转信号。
    持有期：0.6-6.4天
    """
    adv20 = data.adv(20)
    return ts_rank(data.volume / adv20, 20) * ts_rank(-1 * delta(data.close, 7), 8)


def alpha_044(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#44: 最高价与成交量排名相关性反转

    5日最高价与成交量排名的相关性反向。
    用途：高点与成交量排名正相关时做空，追高行为过度。
    持有期：0.6-6.4天
    """
    return -1 * correlation(data.high, rank(data.volume), 5)


def alpha_045(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#45: 延迟收盘价均值与量价相关性三重交互

    延迟5日收盘价20日均值排名，乘以收盘-成交量2日相关性，再乘以收盘价和相关性排名。
    用途：延迟价格水平、当前量价关系、价格累计的三重筛选。
    持有期：0.6-6.4天
    """
    return -1 * (rank(ts_sum(delay(data.close, 5), 20) / 20) *
                 correlation(data.close, data.volume, 2) *
                 rank(correlation(ts_sum(data.close, 5), ts_sum(data.close, 20), 2)))


def alpha_046(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#46: 价格加速度状态切换因子

    计算价格20-10日斜率与10日-当前斜率的差值，根据阈值切换多空信号。
    用途：价格加速度变化识别趋势转折，加速上升做空，加速下降做多。
    持有期：0.6-6.4天
    """
    d20_d10 = (delay(data.close, 20) - delay(data.close, 10)) / 10
    d10_c = (delay(data.close, 10) - data.close) / 10
    inner = d20_d10 - d10_c
    cond1 = inner > 0.25
    cond2 = inner < 0
    return _where(cond1, -1, _where(cond2, 1, -1 * delta(data.close, 1)))


def alpha_047(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#47: 价格倒数-成交量-高点上影线复合因子

    收盘价倒数排名乘以相对成交量，再乘以高点上影线归一化，减去VWAP动量排名。
    用途：低价股放量上影线减VWAP反弹的综合信号。
    持有期：0.6-6.4天
    """
    adv20 = data.adv(20)
    return ((rank(1 / data.close) * data.volume / adv20 *
             (data.high * rank(data.high - data.close) / (ts_sum(data.high, 5) / 5))) -
            rank(data.vwap - delay(data.vwap, 5)))


def alpha_048(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#48: 行业中性化收益自相关强度因子

    1日收益与延迟1日收益的250日相关性乘以收益率，行业中性化后归一化。
    用途：行业中性的收益自相关性，识别个股特异动量。
    持有期：0.6-6.4天
    """
    d1 = delta(data.close, 1)
    dd1 = delta(delay(data.close, 1), 1)
    inner = correlation(d1, dd1, 250) * d1 / data.close
    neutralized = indneutralize(inner, data.industry_map, level='subindustry')
    denom = ts_sum((d1 / delay(data.close, 1)) ** 2, 250)
    return neutralized / denom


def alpha_049(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#49: 价格加速度阈值反转因子（紧缩版）

    与Alpha#46类似，但阈值收紧至-0.1，更敏感的趋势加速判断。
    用途：快速识别价格加速下降转为减速的拐点。
    持有期：0.6-6.4天
    """
    d20_d10 = (delay(data.close, 20) - delay(data.close, 10)) / 10
    d10_c = (delay(data.close, 10) - data.close) / 10
    inner = d20_d10 - d10_c
    cond = inner < -0.1
    return _where(cond, 1, -1 * delta(data.close, 1))


def alpha_050(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#50: 成交量-VWAP排名相关性极值反转

    成交量与VWAP排名的5日相关性，取其排名后的5日最大值反向。
    用途：量价排名相关性达到极值时的反转信号。
    持有期：0.6-6.4天
    """
    return -1 * ts_max(rank(correlation(rank(data.volume), rank(data.vwap), 5)), 5)


# ============================================================================
# Alpha #51 - #60
# ============================================================================

def alpha_051(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#51: 价格加速度阈值反转因子（宽松版）

    与Alpha#49类似，但阈值放宽至-0.05，更保守的趋势判断。
    用途：识别价格加速下降幅度较大的反转机会。
    持有期：0.6-6.4天
    """
    d20_d10 = (delay(data.close, 20) - delay(data.close, 10)) / 10
    d10_c = (delay(data.close, 10) - data.close) / 10
    inner = d20_d10 - d10_c
    cond = inner < -0.05
    return _where(cond, 1, -1 * delta(data.close, 1))


def alpha_052(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#52: 低点动量与长期收益趋势叠加成交量

    5日最低点的5日变化，乘以长短期收益差排名，再乘以成交量时序排名。
    用途：低点抬升伴随长期收益改善和放量的趋势延续信号。
    持有期：0.6-6.4天
    """
    tmin5 = ts_min(data.low, 5)
    return ((-1 * tmin5 + delay(tmin5, 5)) *
            rank((ts_sum(data.returns, 240) - ts_sum(data.returns, 20)) / 220) *
            ts_rank(data.volume, 5))


def alpha_053(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#53: 收盘价在日内区间位置变化反转

    收盘价在高低点区间中的相对位置（类似威廉指标），9日变化反向。
    用途：区间位置快速变化的反转信号。
    持有期：0.6-6.4天
    """
    inner = ((data.close - data.low) - (data.high - data.close)) / (data.close - data.low).replace(0, np.nan)
    return -1 * delta(inner, 9)


def alpha_054(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#54: 低点-收盘价-开盘价-高点非线性幂次比率

    下影线乘以开盘价5次方，除以区间幅度乘以收盘价5次方，反向信号。
    用途：非线性放大下影线与开盘价关系，捕捉特殊形态。
    持有期：0.6-6.4天
    """
    num = -1 * (data.low - data.close) * (data.open ** 5)
    denom = (data.low - data.high).replace(0, np.nan) * (data.close ** 5)
    return num / denom


def alpha_055(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#55: 价格区间位置与成交量排名相关性反转

    12日收盘价在高低点区间的归一化位置排名，与成交量排名的6日相关性反向。
    用途：价格相对位置与成交量排名高度相关时反转。
    持有期：0.6-6.4天
    """
    tmin12 = ts_min(data.low, 12)
    tmax12 = ts_max(data.high, 12)
    rng = (tmax12 - tmin12).replace(0, np.nan)
    inner = (data.close - tmin12) / rng
    return -1 * correlation(rank(inner), rank(data.volume), 6)


def alpha_056(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#56: 收益率累计比与市值加权反转

    10日累计收益与2日滚动累计3次收益的比值排名，乘以收益-市值乘积排名，反向。
    用途：短期收益加速与市值效应的反转信号。
    持有期：0.6-6.4天
    """
    inner = ts_sum(data.returns, 10) / ts_sum(ts_sum(data.returns, 2), 3)
    return 0 - rank(inner) * rank(data.returns * data.cap)


def alpha_057(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#57: VWAP偏离除以收盘价高点位置衰减

    收盘价与VWAP偏离，除以30日收盘价最高点位置的线性衰减排名。
    用途：VWAP偏离归一化后的反转强度信号。
    持有期：0.6-6.4天
    """
    denom = decay_linear(rank(ts_argmax(data.close, 30)), 2)
    denom = denom.replace(0, np.nan)
    return 0 - (data.close - data.vwap) / denom


def alpha_058(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#58: 行业中性VWAP-成交量相关性线性衰减时序排名

    VWAP行业中性化后与成交量的相关性，经线性衰减和时序排名后反向。
    用途：行业中性的量价关系时序动量反转信号。
    持有期：0.6-6.4天
    """
    neutralized = indneutralize(data.vwap, data.industry_map, level='sector')
    d = int(math.floor(3.92795))
    return -1 * ts_rank(decay_linear(correlation(neutralized, data.volume, d), int(math.floor(7.89291))),
                         int(math.floor(5.50322)))


def alpha_059(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#59: 行业中性VWAP-成交量相关性衰减时序排名（简化版）

    VWAP行业中性化，与成交量4日相关性，16日线性衰减，8日时序排名反向。
    用途：与Alpha#58类似，但使用行业级别中性化。
    持有期：0.6-6.4天
    """
    # Note: vwap * 0.728317 + vwap * (1 - 0.728317) = vwap
    neutralized = indneutralize(data.vwap, data.industry_map, level='industry')
    return -1 * ts_rank(
        decay_linear(correlation(neutralized, data.volume, 4), 16),
        8
    )


def alpha_060(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#60: 成交量加权区间位置与收盘价高点位置双重标准化

    区间位置乘以成交量的双重标准化排名，减去收盘价最高点位置标准化排名。
    用途：量能加权的价格位置信号，对比单纯价格位置。
    持有期：0.6-6.4天
    """
    hl_range = (data.high - data.low).replace(0, np.nan)
    inner = ((data.close - data.low) - (data.high - data.close)) / hl_range * data.volume
    return 0 - (2 * scale(rank(inner)) - scale(rank(ts_argmax(data.close, 10))))


# ============================================================================
# Alpha #61 - #70
# ============================================================================

def alpha_061(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#61: VWAP水平位置与VWAP-均量相关性比较

    VWAP相对16日低点的排名，与VWAP-180日均量17日相关性排名的大小比较。
    用途：VWAP绝对位置与其流动性相关性的状态判断。
    持有期：0.6-6.4天
    """
    adv180 = data.adv(180)
    cond = rank(data.vwap - ts_min(data.vwap, 16)) < rank(correlation(data.vwap, adv180, 17))
    # Boolean to int
    return cond.astype(float)


def alpha_062(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#62: VWAP-均量累计相关性与价格排名关系比较

    VWAP与20日均量累计的相关性排名，对比开盘价与中间价排名关系，反向信号。
    用途：量价相关性与价格结构的双重验证。
    持有期：0.6-6.4天
    """
    adv20 = data.adv(20)
    left = rank(correlation(data.vwap, ts_sum(adv20, 22), 9))
    right = rank((rank(data.open) + rank(data.open)) < (rank((data.high + data.low) / 2) + rank(data.high)))
    return (left < right).astype(float) * -1


def alpha_063(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#63: 行业中性收盘价动量衰减与混合价格-均量相关性差

    行业中性收盘价2日变化的线性衰减排名，减去混合价格与均量累计相关性的衰减排名，反向。
    用途：行业中性价格动量与市场流动性关系的背离信号。
    持有期：0.6-6.4天
    """
    adv180 = data.adv(180)
    neutralized = indneutralize(data.close, data.industry_map, level='industry')
    part1 = rank(decay_linear(delta(neutralized, 2), 8))
    combo = data.vwap * 0.318108 + data.open * (1 - 0.318108)
    part2 = rank(decay_linear(correlation(combo, ts_sum(adv180, 37), 13), 12))
    return (part1 - part2) * -1


def alpha_064(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#64: 开盘-低点混合价格与均量相关性对比中间价-VWAP动量

    开盘低点加权与均量累计的相关性排名，对比中间价-VWAP加权的动量排名，反向。
    用途：开盘支撑强度与价格中枢动量的交叉验证。
    持有期：0.6-6.4天
    """
    adv120 = data.adv(120)
    combo1 = data.open * 0.178404 + data.low * (1 - 0.178404)
    combo2 = (data.high + data.low) / 2 * 0.178404 + data.vwap * (1 - 0.178404)
    left = rank(correlation(ts_sum(combo1, 12), ts_sum(adv120, 12), 16))
    right = rank(delta(combo2, 3))
    return (left < right).astype(float) * -1


def alpha_065(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#65: 开盘-VWAP混合与均量相关性对比开盘价水平位置

    开盘VWAP加权与60日均量累计的相关性排名，对比开盘价相对13日低点排名，反向。
    用途：开盘定价与流动性关系对比开盘绝对位置。
    持有期：0.6-6.4天
    """
    adv60 = data.adv(60)
    combo = data.open * 0.00817205 + data.vwap * (1 - 0.00817205)
    left = rank(correlation(combo, ts_sum(adv60, 8), 6))
    right = rank(data.open - ts_min(data.open, 13))
    return (left < right).astype(float) * -1


def alpha_066(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#66: VWAP动量衰减与低点-VWAP偏离比率衰减时序排名

    VWAP 3日变化的线性衰减排名，加上低点-VWAP偏离与开盘-中价差比率的衰减时序排名，反向。
    用途：VWAP短期动量与价格结构偏离的综合反转信号。
    持有期：0.6-6.4天
    """
    # low * 0.96633 + low * (1 - 0.96633) = low
    denom = (data.open - (data.high + data.low) / 2).replace(0, np.nan)
    inner = (data.low - data.vwap) / denom
    return (rank(decay_linear(delta(data.vwap, 3), 7)) +
            ts_rank(decay_linear(inner, 11), 6)) * -1


def alpha_067(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#67: 高点水平位置幂次乘以行业中性VWAP-均量相关性

    高点相对2日低点的排名，幂次为行业板块中性VWAP与细分行业中性均量的6日相关性排名，反向。
    用途：高点位置受行业中性流动性关系调制的反转信号。
    持有期：0.6-6.4天
    """
    adv20 = data.adv(20)
    nv = indneutralize(data.vwap, data.industry_map, level='sector')
    na = indneutralize(adv20, data.industry_map, level='subindustry')
    return (rank(data.high - ts_min(data.high, 2)) ** rank(correlation(nv, na, 6))) * -1


def alpha_068(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#68: 高点-均量排名相关性时序排名对比收盘-低点混合动量

    高点与15日均量排名的8日相关性时序排名，对比收盘低点加权1日动量排名，反向。
    用途：高点追涨行为与价格支撑动量的背离信号。
    持有期：0.6-6.4天
    """
    adv15 = data.adv(15)
    combo = data.close * 0.518371 + data.low * (1 - 0.518371)
    left = ts_rank(correlation(rank(data.high), rank(adv15), 8), 13)
    right = rank(delta(combo, 1))
    return (left < right).astype(float) * -1


def alpha_069(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#69: 行业中性VWAP动量极值幂次乘以收盘-VWAP混合与均量相关性

    行业中性VWAP 2日变化的4日最大值排名，幂次为收盘VWAP加权与均量4日相关性的9日时序排名，反向。
    用途：行业中性VWAP动量峰值受量价相关性调制的反转。
    持有期：0.6-6.4天
    """
    adv20 = data.adv(20)
    nv = indneutralize(data.vwap, data.industry_map, level='industry')
    combo = data.close * 0.490655 + data.vwap * (1 - 0.490655)
    return (rank(ts_max(delta(nv, 2), 4)) **
            ts_rank(correlation(combo, adv20, 4), 9)) * -1


def alpha_070(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#70: VWAP动量幂次乘以行业中性收盘价与均量相关性时序排名

    VWAP 1日变化排名，幂次为行业中性收盘价与50日均量17日相关性的17日时序排名，反向。
    用途：VWAP短期动量受行业中性流动性时序动量调制的反转。
    持有期：0.6-6.4天
    """
    adv50 = data.adv(50)
    nc = indneutralize(data.close, data.industry_map, level='industry')
    return (rank(delta(data.vwap, 1)) ** ts_rank(correlation(nc, adv50, 17), 17)) * -1


# ============================================================================
# Alpha #71 - #80
# ============================================================================

def alpha_071(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#71: 收盘-均量相关性衰减与低开-VWAP偏离衰减的最大值

    收盘价与均量时序排名相关性的衰减时序排名，与低开价格偏离VWAP平方的衰减时序排名，取最大值。
    用途：量价时序相关性与价格偏离结构的双通道信号。
    持有期：0.6-6.4天
    """
    adv180 = data.adv(180)
    p1 = ts_rank(decay_linear(correlation(ts_rank(data.close, 3), ts_rank(adv180, 12), 18), 4), 15)
    inner2 = rank((data.low + data.open) - 2 * data.vwap) ** 2
    p2 = ts_rank(decay_linear(inner2, 16), 4)
    return pd.DataFrame(np.maximum(p1, p2), index=p1.index, columns=p1.columns)


def alpha_072(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#72: 中间价-均量相关性衰减除以VWAP-成交量时序排名相关性衰减

    中间价与40日均量8日相关性的衰减排名，除以VWAP-成交量时序排名6日相关性的衰减排名。
    用途：价格中枢流动性与VWAP量价动量的比率信号。
    持有期：0.6-6.4天
    """
    adv40 = data.adv(40)
    num = rank(decay_linear(correlation((data.high + data.low) / 2, adv40, 8), 10))
    denom = rank(decay_linear(correlation(ts_rank(data.vwap, 3), ts_rank(data.volume, 18), 6), 2))
    return num / denom.replace(0, np.nan)


def alpha_073(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#73: VWAP动量衰减与开盘-低点混合变化率衰减的最大值反转

    VWAP 4日变化的衰减排名，与开盘低点加权变化率的衰减时序排名，取最大值后反向。
    用途：VWAP动量与开盘支撑变化率的双通道反转信号。
    持有期：0.6-6.4天
    """
    combo = data.open * 0.147155 + data.low * (1 - 0.147155)
    combo_nz = combo.replace(0, np.nan)
    inner = (delta(combo, 2) / combo_nz) * -1
    p1 = rank(decay_linear(delta(data.vwap, 4), 2))
    p2 = ts_rank(decay_linear(inner, 3), 16)
    return pd.DataFrame(np.maximum(p1, p2), index=p1.index, columns=p1.columns) * -1


def alpha_074(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#74: 收盘-均量累计相关性对比高点-VWAP混合与成交量排名相关性

    收盘价与30日均量累计的15日相关性排名，对比高点VWAP加权与成交量排名的相关性排名，反向。
    用途：价格与流动性累计关系对比高点量价排名相关性的背离。
    持有期：0.6-6.4天
    """
    adv30 = data.adv(30)
    combo = data.high * 0.0261661 + data.vwap * (1 - 0.0261661)
    left = rank(correlation(data.close, ts_sum(adv30, 37), 15))
    right = rank(correlation(rank(combo), rank(data.volume), 11))
    return (left < right).astype(float) * -1


def alpha_075(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#75: VWAP-成交量相关性对比低点-均量排名相关性

    VWAP与成交量4日相关性排名，对比低点与50日均量排名的12日相关性排名。
    用途：VWAP即时量价关系与低点流动性排名相关性的状态比较。
    持有期：0.6-6.4天
    """
    adv50 = data.adv(50)
    left = rank(correlation(data.vwap, data.volume, 4))
    right = rank(correlation(rank(data.low), rank(adv50), 12))
    return (left < right).astype(float)


def alpha_076(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#76: VWAP动量衰减与板块中性低点-均量相关性衰减时序排名的最大值反转

    VWAP 1日变化的衰减排名，与板块中性低点与均量相关性的衰减双重时序排名，取最大值后反向。
    用途：VWAP动量与板块中性支撑流动性的双通道反转。
    持有期：0.6-6.4天
    """
    adv81 = data.adv(81)
    nl = indneutralize(data.low, data.industry_map, level='sector')
    p1 = rank(decay_linear(delta(data.vwap, 1), 11))
    p2 = ts_rank(decay_linear(ts_rank(correlation(nl, adv81, 8), 19), 17), 19)
    return pd.DataFrame(np.maximum(p1, p2), index=p1.index, columns=p1.columns) * -1


def alpha_077(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#77: 中间价-VWAP偏离衰减与中间价-均量相关性衰减的最小值

    中间价与VWAP偏离的衰减排名，与中间价与40日均量3日相关性的衰减排名，取最小值。
    用途：价格中枢偏离与流动性相关性的双重约束信号。
    持有期：0.6-6.4天
    """
    adv40 = data.adv(40)
    inner1 = (data.high + data.low) / 2 + data.high - data.vwap - data.high
    # Simplifies to (high + low) / 2 - vwap
    p1 = rank(decay_linear(inner1, 20))
    p2 = rank(decay_linear(correlation((data.high + data.low) / 2, adv40, 3), 5))
    return pd.DataFrame(np.minimum(p1, p2), index=p1.index, columns=p1.columns)


def alpha_078(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#78: 低点-VWAP混合与均量累计相关性幂次乘以VWAP-成交量排名相关性

    低点VWAP加权累计与均量累计的相关性排名，幂次为VWAP与成交量排名的相关性排名。
    用途：支撑位流动性累计受量价排名相关性调制的信号。
    持有期：0.6-6.4天
    """
    adv40 = data.adv(40)
    combo = data.low * 0.352233 + data.vwap * (1 - 0.352233)
    return (rank(correlation(ts_sum(combo, 19), ts_sum(adv40, 19), 6)) **
            rank(correlation(rank(data.vwap), rank(data.volume), 5)))


def alpha_079(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#79: 板块中性收盘-开盘混合动量对比VWAP-均量时序排名相关性

    板块中性收盘开盘加权1日变化排名，对比VWAP与均量时序排名的14日相关性排名。
    用途：板块中性开盘强度与量价时序动量的状态比较。
    持有期：0.6-6.4天
    """
    adv150 = data.adv(150)
    combo = data.close * 0.60733 + data.open * (1 - 0.60733)
    nc = indneutralize(combo, data.industry_map, level='sector')
    left = rank(delta(nc, 1))
    right = rank(correlation(ts_rank(data.vwap, 3), ts_rank(adv150, 9), 14))
    return (left < right).astype(float)


def alpha_080(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#80: 行业中性开盘-高点混合动量符号幂次乘以高点-均量相关性时序排名

    行业中性开盘高点加权4日变化的符号排名，幂次为高点与均量5日相关性的5日时序排名，反向。
    用途：行业中性开盘冲高行为受高点流动性时序动量调制的反转。
    持有期：0.6-6.4天
    """
    adv10 = data.adv(10)
    combo = data.open * 0.868128 + data.high * (1 - 0.868128)
    nc = indneutralize(combo, data.industry_map, level='industry')
    return (rank(sign(delta(nc, 4))) ** ts_rank(correlation(data.high, adv10, 5), 5)) * -1


# ============================================================================
# Alpha #81 - #90
# ============================================================================

def alpha_081(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#81: VWAP-均量相关性幂次乘积对比VWAP-成交量排名相关性

    VWAP与均量累计8日相关性的4次幂排名，14日乘积后取对数排名，对比VWAP-成交量排名5日相关性排名，反向。
    用途：VWAP流动性相关性的非线性放大与即时量价相关性的背离信号。
    持有期：0.6-6.4天
    """
    adv10 = data.adv(10)
    inner = rank(correlation(data.vwap, ts_sum(adv10, 49), 8)) ** 4
    left = rank(log(product(rank(inner), 14)))
    right = rank(correlation(rank(data.vwap), rank(data.volume), 5))
    return (left < right).astype(float) * -1


def alpha_082(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#82: 开盘价动量衰减与板块中性成交量-开盘价相关性衰减时序排名的最小值反转

    开盘价1日变化的衰减排名，与板块中性成交量与开盘价17日相关性的衰减时序排名，取最小值后反向。
    用途：开盘动量与板块中性开盘量价关系的双重约束反转。
    持有期：0.6-6.4天
    """
    # open * 0.634196 + open * (1 - 0.634196) = open
    nv = indneutralize(data.volume, data.industry_map, level='sector')
    p1 = rank(decay_linear(delta(data.open, 1), 14))
    p2 = ts_rank(decay_linear(correlation(nv, data.open, 17), 6), 13)
    return pd.DataFrame(np.minimum(p1, p2), index=p1.index, columns=p1.columns) * -1


def alpha_083(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#83: 延迟振幅与成交量排名除以振幅-VWAP偏离比率

    2日延迟的高低幅与收盘均价比乘以成交量双重排名，除以当前振幅比与VWAP收盘偏离的比率。
    用途：延迟振幅与成交量强度归一化后的复杂形态信号。
    持有期：0.6-6.4天
    """
    hl_sma = (data.high - data.low) / (ts_sum(data.close, 5) / 5)
    num = rank(delay(hl_sma, 2)) * rank(rank(data.volume))
    denom = hl_sma / (data.vwap - data.close).replace(0, np.nan)
    return num / denom.replace(0, np.nan)


def alpha_084(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#84: VWAP相对高点位置时序排名的收盘价动量符号幂次

    VWAP相对15日高点的20日时序排名，以收盘价4日变化为幂次进行符号化幂运算。
    用途：VWAP位置强度受价格动量方向调制的非对称信号。
    持有期：0.6-6.4天
    """
    return signedpower(
        ts_rank(data.vwap - ts_max(data.vwap, 15), 20),
        delta(data.close, 4)
    )


def alpha_085(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#85: 高点-收盘价混合与均量相关性幂次乘以中间价-成交量时序排名相关性

    高点收盘加权与30日均量9日相关性排名，幂次为中间价与成交量时序排名的7日相关性排名。
    用途：高点定价流动性受中间价量价时序动量调制的信号。
    持有期：0.6-6.4天
    """
    adv30 = data.adv(30)
    combo = data.high * 0.876703 + data.close * (1 - 0.876703)
    return (rank(correlation(combo, adv30, 9)) **
            rank(correlation(ts_rank((data.high + data.low) / 2, 3),
                            ts_rank(data.volume, 10), 7)))


def alpha_086(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#86: 收盘-均量累计相关性时序排名对比收盘-VWAP偏离

    收盘价与20日均量累计的6日相关性时序排名，对比收盘价与VWAP偏离排名，反向。
    用途：价格流动性累积相关性时序动量与即时偏离的背离信号。
    持有期：0.6-6.4天
    """
    adv20 = data.adv(20)
    # (open + close) - (vwap + open) = close - vwap
    left = ts_rank(correlation(data.close, ts_sum(adv20, 14), 6), 20)
    right = rank(data.close - data.vwap)
    return (left < right).astype(float) * -1


def alpha_087(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#87: 收盘-VWAP混合动量衰减与行业中性均量-收盘价相关性衰减的最大值反转

    收盘VWAP加权1日变化的衰减排名，与行业中性均量与收盘价13日相关性绝对值的衰减时序排名，取最大值后反向。
    用途：价格动量与行业中性流动性相关性的双通道反转。
    持有期：0.6-6.4天
    """
    adv81 = data.adv(81)
    combo = data.close * 0.369701 + data.vwap * (1 - 0.369701)
    na = indneutralize(adv81, data.industry_map, level='industry')
    p1 = rank(decay_linear(delta(combo, 1), 2))
    p2 = ts_rank(decay_linear(abs_val(correlation(na, data.close, 13)), 4), 14)
    return pd.DataFrame(np.maximum(p1, p2), index=p1.index, columns=p1.columns) * -1


def alpha_088(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#88: 开低-高收排名差衰减与收盘-均量时序排名相关性衰减的最小值

    开盘低点排名和减高点收盘排名和的衰减排名，与收盘价与均量时序排名8日相关性的衰减时序排名，取最小值。
    用途：价格排名结构偏离与量价时序相关性的双重约束。
    持有期：0.6-6.4天
    """
    adv60 = data.adv(60)
    inner1 = rank(data.open) + rank(data.low) - rank(data.high) - rank(data.close)
    p1 = rank(decay_linear(inner1, 8))
    p2 = ts_rank(decay_linear(correlation(ts_rank(data.close, 8), ts_rank(adv60, 20), 8), 6), 2)
    return pd.DataFrame(np.minimum(p1, p2), index=p1.index, columns=p1.columns)


def alpha_089(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#89: 低点-均量相关性衰减时序排名减行业中性VWAP动量衰减时序排名

    低点与均量6日相关性的衰减3日时序排名，减去行业中性VWAP 3日变化的衰减15日时序排名。
    用途：支撑位流动性时序动量与行业中性VWAP动量的差异信号。
    持有期：0.6-6.4天
    """
    adv10 = data.adv(10)
    # low * 0.967285 + low * (1 - 0.967285) = low
    nv = indneutralize(data.vwap, data.industry_map, level='industry')
    p1 = ts_rank(decay_linear(correlation(data.low, adv10, 6), 5), 3)
    p2 = ts_rank(decay_linear(delta(nv, 3), 10), 15)
    return p1 - p2


def alpha_090(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#90: 收盘价相对高点位置幂次乘以细分行业中性均量-低点相关性时序排名

    收盘价相对4日高点的排名，幂次为细分行业中性均量与低点5日相关性的3日时序排名，反向。
    用途：价格下跌位置受细分行业中性支撑流动性时序动量调制的反转。
    持有期：0.6-6.4天
    """
    adv40 = data.adv(40)
    na = indneutralize(adv40, data.industry_map, level='subindustry')
    return (rank(data.close - ts_max(data.close, 4)) **
            ts_rank(correlation(na, data.low, 5), 3)) * -1


# ============================================================================
# Alpha #91 - #101
# ============================================================================

def alpha_091(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#91: 行业中性收盘-成交量相关性双重衰减时序排名减VWAP-均量相关性衰减

    行业中性收盘与成交量9日相关性的双重线性衰减4日时序排名，减去VWAP与均量4日相关性的衰减排名，反向。
    用途：行业中性量价关系双重衰减与VWAP流动性相关性的差异反转。
    持有期：0.6-6.4天
    """
    adv30 = data.adv(30)
    nc = indneutralize(data.close, data.industry_map, level='industry')
    p1 = ts_rank(decay_linear(decay_linear(correlation(nc, data.volume, 9), 16), 3), 4)
    p2 = rank(decay_linear(correlation(data.vwap, adv30, 4), 2))
    return (p1 - p2) * -1


def alpha_092(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#92: 中间收盘价低开价格比较衰减与低点-均量排名相关性衰减的最小值

    中间价与收盘价之和小于低点与开盘价之和的布尔值衰减时序排名，与低点与均量排名7日相关性的衰减时序排名，取最小值。
    用途：价格结构比较与支撑位流动性排名相关性的双重约束。
    持有期：0.6-6.4天
    """
    adv30 = data.adv(30)
    inner = ((data.high + data.low) / 2 + data.close) < (data.low + data.open)
    p1 = ts_rank(decay_linear(inner.astype(float), 14), 18)
    p2 = ts_rank(decay_linear(correlation(rank(data.low), rank(adv30), 7), 6), 6)
    return pd.DataFrame(np.minimum(p1, p2), index=p1.index, columns=p1.columns)


def alpha_093(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#93: 行业中性VWAP-均量相关性衰减时序排名除以收盘-VWAP混合动量衰减

    行业中性VWAP与均量17日相关性的衰减7日时序排名，除以收盘VWAP加权2日变化的衰减排名。
    用途：行业中性VWAP流动性时序动量与价格动量的比率信号。
    持有期：0.6-6.4天
    """
    adv81 = data.adv(81)
    nv = indneutralize(data.vwap, data.industry_map, level='industry')
    combo = data.close * 0.524434 + data.vwap * (1 - 0.524434)
    num = ts_rank(decay_linear(correlation(nv, adv81, 17), 19), 7)
    denom = rank(decay_linear(delta(combo, 2), 16))
    return num / denom.replace(0, np.nan)


def alpha_094(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#94: VWAP相对低点位置幂次乘以VWAP-均量时序排名相关性时序排名

    VWAP相对11日低点的排名，幂次为VWAP与均量时序排名的18日相关性的2日时序排名，反向。
    用途：VWAP上升位置受其与流动性时序相关性调制的反转信号。
    持有期：0.6-6.4天
    """
    adv60 = data.adv(60)
    return (rank(data.vwap - ts_min(data.vwap, 11)) **
            ts_rank(correlation(ts_rank(data.vwap, 19), ts_rank(adv60, 4), 18), 2)) * -1


def alpha_095(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#95: 开盘价相对低点位置对比中间价-均量累计相关性5次幂时序排名

    开盘价相对12日低点的排名，对比中间价与均量累计的12日相关性排名5次幂的11日时序排名。
    用途：开盘位置与中间价流动性相关性幂次放大的状态比较。
    持有期：0.6-6.4天
    """
    adv40 = data.adv(40)
    hl_mid = (data.high + data.low) / 2
    left = rank(data.open - ts_min(data.open, 12))
    right = ts_rank(rank(correlation(ts_sum(hl_mid, 19), ts_sum(adv40, 19), 12)) ** 5, 11)
    return (left < right).astype(float)


def alpha_096(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#96: VWAP-成交量排名相关性衰减与收盘-均量时序排名相关性最高点位置衰减的最大值反转

    VWAP与成交量排名3日相关性的衰减8日时序排名，与收盘价与均量时序排名3日相关性12日最高点位置的衰减13日时序排名，取最大值后反向。
    用途：即时量价排名相关性与历史量价相关性峰值位置的双通道反转。
    持有期：0.6-6.4天
    """
    adv60 = data.adv(60)
    p1 = ts_rank(decay_linear(correlation(rank(data.vwap), rank(data.volume), 3), 4), 8)
    inner = ts_argmax(correlation(ts_rank(data.close, 7), ts_rank(adv60, 4), 3), 12)
    p2 = ts_rank(decay_linear(inner, 14), 13)
    return pd.DataFrame(np.maximum(p1, p2), index=p1.index, columns=p1.columns) * -1


def alpha_097(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#97: 行业中性低点-VWAP混合动量衰减减低点-均量时序排名相关性衰减时序排名

    行业中性低点VWAP加权3日变化的衰减排名，减去低点与均量时序排名4日相关性的双重衰减时序排名，反向。
    用途：行业中性支撑位动量与支撑流动性时序相关性的差异反转。
    持有期：0.6-6.4天
    """
    adv60 = data.adv(60)
    combo = data.low * 0.721001 + data.vwap * (1 - 0.721001)
    nc = indneutralize(combo, data.industry_map, level='industry')
    p1 = rank(decay_linear(delta(nc, 3), 20))
    inner = ts_rank(correlation(ts_rank(data.low, 7), ts_rank(adv60, 17), 4), 18)
    p2 = ts_rank(decay_linear(inner, 15), 6)
    return (p1 - p2) * -1


def alpha_098(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#98: VWAP-均量累计相关性衰减减开盘-均量排名相关性最低点位置衰减

    VWAP与5日均量累计的4日相关性衰减排名，减去开盘与均量排名20日相关性8日最低点位置的衰减6日时序排名后再排名。
    用途：VWAP流动性累积与开盘定价流动性最低点位置的差异信号。
    持有期：0.6-6.4天
    """
    adv5 = data.adv(5)
    adv15 = data.adv(15)
    p1 = rank(decay_linear(correlation(data.vwap, ts_sum(adv5, 26), 4), 7))
    inner = ts_rank(ts_argmin(correlation(rank(data.open), rank(adv15), 20), 8), 6)
    p2 = rank(decay_linear(inner, 8))
    return p1 - p2


def alpha_099(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#99: 中间价-均量累计相关性对比低点-成交量相关性

    中间价与均量累计的8日相关性排名，对比低点与成交量6日相关性排名，反向。
    用途：中间价流动性累积与支撑位即时量价关系的背离信号。
    持有期：0.6-6.4天
    """
    adv60 = data.adv(60)
    hl_mid = (data.high + data.low) / 2
    left = rank(correlation(ts_sum(hl_mid, 19), ts_sum(adv60, 19), 8))
    right = rank(correlation(data.low, data.volume, 6))
    return (left < right).astype(float) * -1


def alpha_100(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#100: 双重细分行业中性化成交量加权区间位置与收盘-均量相关性中性化差值标准化

    区间位置乘以成交量的排名双重细分行业中性化标准化，减去收盘与均量相关性减收盘最低点位置排名的细分行业中性化标准化，乘以相对成交量，反向。
    用途：双重行业中性化的量价结构复杂组合信号。
    持有期：0.6-6.4天
    """
    adv20 = data.adv(20)
    hl_range = (data.high - data.low).replace(0, np.nan)
    inner1 = ((data.close - data.low) - (data.high - data.close)) / hl_range * data.volume
    r1 = rank(inner1)
    n1 = indneutralize(indneutralize(r1, data.industry_map, 'subindustry'), data.industry_map, 'subindustry')

    inner2 = correlation(data.close, rank(adv20), 5) - rank(ts_argmin(data.close, 30))
    n2 = indneutralize(inner2, data.industry_map, 'subindustry')

    return 0 - (1.5 * scale(n1) - scale(n2)) * (data.volume / adv20)


def alpha_101(data: AlphaDataPanel) -> pd.DataFrame:
    """Alpha#101: 收盘开盘价差归一化因子

    收盘价与开盘价之差除以高低价振幅（加0.001防止除零）。
    用途：日内收盘相对开盘的强度归一化，最简单的日内动量因子。
    持有期：0.6-6.4天
    """
    return (data.close - data.open) / ((data.high - data.low) + 0.001)


# ============================================================================
# Alpha registry - maps alpha_id to function
# ============================================================================

ALPHA_REGISTRY = {}
for i in range(1, 102):
    fname = f"alpha_{i:03d}"
    if fname in globals():
        ALPHA_REGISTRY[i] = globals()[fname]


def get_alpha_func(alpha_id: int):
    """Get alpha function by ID."""
    return ALPHA_REGISTRY.get(alpha_id)


def list_all_alphas():
    """Return list of all available alpha IDs."""
    return sorted(ALPHA_REGISTRY.keys())


# Alpha descriptions for metadata
ALPHA_DESCRIPTIONS = {
    1: "Reversal with volatility clustering",
    2: "Volume-price divergence",
    3: "Open-volume correlation",
    4: "Low price momentum",
    5: "VWAP deviation with open-close spread",
    6: "Open-volume correlation (simple)",
    7: "Volume-conditional close reversal",
    8: "Open-return momentum interaction",
    9: "Close momentum regime switch",
    10: "Close momentum regime (ranked)",
    11: "VWAP-close spread with volume momentum",
    12: "Volume-price momentum",
    13: "Close-volume covariance reversal",
    14: "Return momentum with open-volume correlation",
    15: "High-volume correlation sum",
    16: "High-volume covariance reversal",
    17: "Close momentum with volume ratio",
    18: "Close-open spread volatility",
    19: "Close reversal with long-term returns",
    20: "Open gap reversal",
    21: "Close SMA regime with volume",
    22: "High-volume correlation momentum",
    23: "High breakout reversal",
    24: "Long-term SMA trend with close reversal",
    25: "Return-volume-VWAP interaction",
    26: "Volume-high rank correlation",
    27: "Volume-VWAP correlation regime",
    28: "ADV-low correlation with mid-price",
    29: "Complex close momentum ranking",
    30: "Close sign persistence with volume ratio",
    31: "Complex decay close momentum",
    32: "Close SMA reversion with VWAP correlation",
    33: "Open-close ratio rank",
    34: "Return volatility ratio with close momentum",
    35: "Volume-price-return ranking interaction",
    36: "Multi-factor composite (5 factors)",
    37: "Open-close spread correlation with close level",
    38: "Close rank-ratio interaction",
    39: "Close momentum with volume-adjusted decay",
    40: "High volatility-volume correlation",
    41: "Geometric mean minus VWAP",
    42: "VWAP-close rank ratio (delay-0)",
    43: "Volume ratio rank with close momentum",
    44: "High-volume rank correlation",
    45: "Delayed close-volume-sum interaction",
    46: "Close trend regime switch",
    47: "Close rank-volume-high interaction",
    48: "Industry-neutralized return autocorrelation (delay-0)",
    49: "Close trend threshold reversal",
    50: "Volume-VWAP rank correlation max",
    51: "Close trend threshold reversal (tight)",
    52: "Low momentum with long-term return trend",
    53: "Close-low-high position change (delay-0)",
    54: "Low-close-high price power ratio (delay-0)",
    55: "Close position in range vs volume",
    56: "Return sum ratio with market cap",
    57: "VWAP deviation / close argmax decay",
    58: "Industry-neutralized VWAP-volume correlation decay",
    59: "Industry-neutralized VWAP-volume correlation",
    60: "Close position in range * volume (scaled)",
    61: "VWAP level vs VWAP-ADV correlation",
    62: "VWAP-ADV correlation vs open-price rank",
    63: "Industry-neutralized close delta vs VWAP correlation decay",
    64: "Open-low combo vs VWAP-high combo momentum",
    65: "Open-VWAP combo vs ADV correlation",
    66: "VWAP delta decay vs low-VWAP ratio decay",
    67: "High level vs industry VWAP-ADV correlation",
    68: "High-ADV correlation vs close-low combo momentum",
    69: "Industry VWAP delta vs close-VWAP-ADV correlation",
    70: "VWAP delta vs industry close-ADV correlation",
    71: "Max of close-ADV decay vs low-VWAP decay",
    72: "Mid-ADV correlation decay vs VWAP-volume rank decay",
    73: "Max of VWAP delta decay vs open-low combo change",
    74: "Close-ADV correlation vs high-VWAP rank correlation",
    75: "VWAP-volume correlation vs low-ADV rank correlation",
    76: "Max of VWAP delta decay vs industry low-ADV correlation",
    77: "Min of mid-VWAP decay vs mid-ADV correlation decay",
    78: "Low-VWAP sum-ADV correlation vs VWAP-volume rank correlation",
    79: "Industry-neutralized close-open combo vs VWAP-ADV rank correlation",
    80: "Industry-neutralized open-high momentum vs high-ADV correlation",
    81: "VWAP-ADV correlation product vs VWAP-volume rank correlation",
    82: "Min of open delta decay vs industry volume-open correlation",
    83: "High-low range delay with volume vs VWAP-close spread",
    84: "VWAP rank with close momentum power",
    85: "High-close combo ADV correlation vs mid-volume rank correlation",
    86: "Close-ADV correlation rank vs close-VWAP spread",
    87: "Max of close-VWAP delta decay vs industry ADV-close correlation",
    88: "Min of price rank decay vs close-ADV rank correlation decay",
    89: "Low-ADV correlation decay vs industry VWAP delta decay",
    90: "Close level vs industry ADV-low correlation",
    91: "Industry close-volume correlation double decay vs VWAP-ADV correlation",
    92: "Min of mid-price comparison decay vs low-ADV rank correlation decay",
    93: "Industry VWAP-ADV correlation vs close-VWAP delta decay",
    94: "VWAP level vs VWAP-ADV rank correlation",
    95: "Open level vs mid-ADV correlation rank power",
    96: "Max of VWAP-volume correlation decay vs close-ADV argmax decay",
    97: "Industry low-VWAP delta decay vs low-ADV rank correlation decay",
    98: "VWAP-ADV correlation decay vs open-ADV correlation argmin decay",
    99: "Mid-ADV correlation vs low-volume correlation",
    100: "Double industry-neutralized volume-adjusted close position",
    101: "Close-open spread / range (simplest)",
}
