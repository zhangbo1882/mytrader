"""
Stock State Detector (v2.0)

A股单只股票牛熊状态判断，五个正交维度：
1. 趋势分 (30分): EMA5/10/20 多空排列 + SMA锚方向确认
2. 动能分 (25分): 两期ROC之差（加速度），与趋势分正交
3. 量价分 (20分): 20日涨跌日量比 + 量价背离检测
4. 换手率分 (15分): 5日换手率 vs 60日历史均值（仅正分）
5. 波动分 (10分): ATR百分位，与趋势方向联动

总分阈值：≥70=牛市，≤40=熊市，中间=中性

设计文档参见 MARKET_REGIME_RULES.md
"""

import os
import logging
from typing import List, Tuple, Optional
from .market_regime import (
    MarketState,
    MarketRegime,
    MARKET_STATE_CONFIG,
    get_market_config,
    get_cycle_config
)

logger = logging.getLogger(__name__)


class StockStateDetector:
    """
    A股单只股票牛熊状态检测器（基于个股自身历史数据，自适应不同类型股票）

    周期配置（通过 cycle 参数选择）:
    - short:  EMA3/5/10 + SMA20锚，适合短线/题材股
    - medium: EMA5/10/20 + SMA60锚，适合波段/成长股（默认）
    - long:   EMA10/20/40 + SMA120锚，适合长线/蓝筹股
    """

    def __init__(self, lookback_days: int = None, cycle: str = None, config: dict = None):
        """
        初始化检测器

        Args:
            lookback_days: 回看天数（已废弃，请使用 cycle）
            cycle: 周期类型 ('short', 'medium', 'long')
            config: 自定义配置字典（最高优先级）

        优先级: config > cycle > lookback_days > default
        """
        if config is not None:
            self.config = config
        elif cycle is not None:
            self.config = get_cycle_config(cycle)
        else:
            self.config = get_market_config()
            if lookback_days is not None:
                self.config['lookback_days'] = lookback_days

        self.lookback_days = self.config['lookback_days']

    def detect_state(self, data, stock_code: str = "", verbose: bool = True) -> MarketState:
        """
        检测股票市场状态

        Args:
            data: backtrader data source（需包含 close/high/low/volume 字段）
            stock_code: 股票代码（用于日志）
            verbose: 是否输出详细日志

        Returns:
            MarketState 对象
        """
        if len(data) < self.lookback_days:
            if verbose:
                logger.warning(f"数据不足 {stock_code}: {len(data)} < {self.lookback_days}")
            return MarketState(MarketRegime.NEUTRAL, 0.5, 0, 0, 0, 0, 0)

        # 提取数据（倒序转正序列表）
        close = [data.close[-i] for i in range(self.lookback_days, 0, -1)]
        high = [data.high[-i] for i in range(self.lookback_days, 0, -1)]
        low = [data.low[-i] for i in range(self.lookback_days, 0, -1)]
        volume = [data.volume[-i] for i in range(self.lookback_days, 0, -1)]

        # 尝试获取换手率数据（backtrader data可能不含此字段）
        try:
            turnover_rate = [data.turnover_rate[-i] for i in range(self.lookback_days, 0, -1)]
        except AttributeError:
            turnover_rate = None

        current_date = data.datetime.date(0)

        # 计算各维度得分
        trend_score, trend_details = self._calculate_trend_score(close)
        momentum_score, momentum_details = self._calculate_momentum_score(close, high, low)
        volume_score, volume_details = self._calculate_volume_score(close, volume)
        turnover_score, turnover_details = self._calculate_turnover_score(turnover_rate)
        volatility_score, volatility_details = self._calculate_volatility_score(
            close, high, low, trend_score
        )

        total_score = trend_score + momentum_score + volume_score + turnover_score + volatility_score

        # 判断市场状态
        if total_score >= self.config['bull_threshold']:
            regime = MarketRegime.BULL
            confidence = min(1.0, (total_score - self.config['bull_threshold']) / 30)
        elif total_score <= self.config['bear_threshold']:
            regime = MarketRegime.BEAR
            confidence = min(1.0, (self.config['bear_threshold'] - total_score) / self.config['bear_threshold'])
        else:
            regime = MarketRegime.NEUTRAL
            confidence = 0.5

        if verbose:
            quiet_mode = os.environ.get('BACKTEST_QUIET_MODE', '0') == '1'
            if not quiet_mode:
                self._log_detection_details(
                    stock_code, current_date,
                    trend_score, trend_details,
                    momentum_score, momentum_details,
                    volume_score, volume_details,
                    turnover_score, turnover_details,
                    volatility_score, volatility_details,
                    total_score, regime, confidence
                )

        return MarketState(
            regime=regime,
            confidence=confidence,
            ma_trend=trend_score,
            momentum=momentum_score,
            position_ratio=0,  # deprecated
            volume_confirm=volume_score,
            volatility=volatility_score,
            turnover_score=turnover_score,
        )

    # -------------------------------------------------------------------------
    # EMA 计算工具
    # -------------------------------------------------------------------------

    def _calculate_ema(self, prices: List[float], period: int) -> float:
        """计算指数移动平均（EMA）"""
        if len(prices) < period:
            return sum(prices) / len(prices) if prices else 0
        k = 2.0 / (period + 1)
        ema = sum(prices[:period]) / period  # 用SMA初始化
        for price in prices[period:]:
            ema = price * k + ema * (1 - k)
        return ema

    # -------------------------------------------------------------------------
    # 维度1：趋势分（±30分）
    # -------------------------------------------------------------------------

    def _calculate_trend_score(self, close: List[float]) -> Tuple[int, dict]:
        """
        趋势分（30分）- EMA多空头排列 + SMA方向锚

        设计原理：
        - 用EMA代替SMA：A股散户普遍用MACD（基于EMA），EMA均线是自我实现的支撑/压力
        - SMA锚作为中期方向验证：区分真正趋势与熊市反弹
        - 完美多头+SMA锚上方: +30（趋势与大势共振）
        - 完美多头+SMA锚下方: +15（多头但在压制下，可能只是反弹）
        """
        ma_short = self.config['ma_short']
        ma_medium = self.config['ma_medium']
        ma_long = self.config['ma_long']
        ma_anchor = self.config.get('ma_anchor', 60)

        ema_s = self._calculate_ema(close, ma_short)
        ema_m = self._calculate_ema(close, ma_medium)
        ema_l = self._calculate_ema(close, ma_long)

        # SMA锚（用所有可用数据，最多取 ma_anchor 天）
        anchor_window = close[-ma_anchor:] if len(close) >= ma_anchor else close
        sma_anchor = sum(anchor_window) / len(anchor_window)
        above_anchor = close[-1] > sma_anchor

        if ema_s > ema_m > ema_l:
            score = 30 if above_anchor else 15
            arrangement = f"完美多头({'大势上方' if above_anchor else '大势下方，谨慎'})"
        elif ema_s > ema_m:
            score = 10 if above_anchor else 5
            arrangement = f"短期向上({'大势上方' if above_anchor else '大势下方'})"
        elif ema_s < ema_m < ema_l:
            score = -30 if not above_anchor else -15
            arrangement = f"完美空头({'大势下方' if not above_anchor else '大势上方仍压制'})"
        elif ema_s < ema_m:
            score = -10 if not above_anchor else -5
            arrangement = f"短期向下({'大势下方' if not above_anchor else '大势上方'})"
        else:
            score = 0
            arrangement = "混乱排列"

        details = {
            'ema_short': ema_s,
            'ema_medium': ema_m,
            'ema_long': ema_l,
            'sma_anchor': sma_anchor,
            'above_anchor': above_anchor,
            'arrangement': arrangement,
        }
        return score, details

    # -------------------------------------------------------------------------
    # 维度2：动能分（±25分）
    # -------------------------------------------------------------------------

    def _calculate_momentum_score(
        self, close: List[float], high: List[float], low: List[float]
    ) -> Tuple[int, dict]:
        """
        动能分（25分）- 四象限加速度评分

        设计原理：
        - 加速度 = 当期ROC - 前期ROC，测量"趋势力量是否在增强"
        - 关键改进：评分必须区分当前是涨还是跌
          * 纯加速度评分的缺陷：价格在涨但涨速比前期慢时，
            acceleration为负 → 错误给出-25熊市动能分
          * 正确逻辑：价格在涨（current_roc > 0）时，即使减速也应得正分；
            价格在跌（current_roc < 0）时，才可以给大幅负分
        - 四象限：涨+加速(+25), 涨+减速(+10), 跌+减速(+5), 跌+加速(-25)
        """
        roc_period = self.config['roc_period']
        atr_period = self.config.get('atr_period', roc_period)

        # 需要 2*roc_period + 1 个数据点
        min_required = 2 * roc_period + 1
        if len(close) < min_required:
            return 0, {'acceleration': 0, 'momentum_type': '数据不足'}

        # 当期ROC: 最近 roc_period 天的价格变化率（决定方向）
        current_roc = (close[-1] - close[-roc_period - 1]) / close[-roc_period - 1] * 100
        # 前期ROC: 再往前 roc_period 天（非重叠窗口，用于计算加速度）
        prev_roc = (close[-roc_period - 1] - close[-2 * roc_period - 1]) / close[-2 * roc_period - 1] * 100
        # 加速度 = 动能变化（正=加速，负=减速）
        acceleration = current_roc - prev_roc

        # ATR动态阈值（自适应不同波动率股票）
        atr_window = max(atr_period, 2)
        atr = self._calculate_atr(
            high[-atr_window:], low[-atr_window:], close[-atr_window:]
        )
        atr_pct = atr / close[-1] * 100 if close[-1] > 0 else 0
        dynamic_threshold = self.config['roc_dynamic_multiplier'] * atr_pct

        # 四象限评分：当前方向（current_roc）为主，加速度为修正
        if current_roc > 0:
            # 价格在涨 —— 以正分为基础，加速度只作修正
            if acceleration > dynamic_threshold:
                score = 25
                momentum_type = "涨+加速(主升浪增强)"
            elif acceleration >= 0:
                score = 15
                momentum_type = "涨+平稳(趋势持续)"
            elif acceleration >= -dynamic_threshold:
                score = 10
                momentum_type = "涨+轻微减速(仍在涨)"
            else:
                score = -5
                momentum_type = "涨+急速减速(警惕见顶)"
        else:
            # 价格在跌 —— 以负分为基础，减速给小幅正分
            if acceleration > dynamic_threshold:
                score = 5
                momentum_type = "跌+快速减速(底部信号)"
            elif acceleration >= 0:
                score = -5
                momentum_type = "跌+缓慢减速(观望)"
            elif acceleration >= -dynamic_threshold:
                score = -15
                momentum_type = "跌+轻微加速(趋势延续)"
            else:
                score = -25
                momentum_type = "跌+急速加速(深度下跌)"

        details = {
            'current_roc': current_roc,
            'prev_roc': prev_roc,
            'acceleration': acceleration,
            'atr_pct': atr_pct,
            'dynamic_threshold': dynamic_threshold,
            'momentum_type': momentum_type,
        }
        return score, details

    # -------------------------------------------------------------------------
    # 维度3：量价分（0~+20分）
    # -------------------------------------------------------------------------

    def _calculate_volume_score(
        self, close: List[float], volume: List[float]
    ) -> Tuple[int, dict]:
        """
        量价分（20分）- 20日涨跌日量比 + 量价背离检测

        设计原理：
        - 20日（一个月）样本更可靠，不易被游资短期操纵
        - 量价背离（价格创新高但成交量萎缩）是经典顶部信号，内嵌为惩罚项
        - 最高分提升至20分（原15分）
        """
        lookback = self.config.get('volume_lookback', 20)
        # 安全边界：如果数据不足，用可用的全部数据
        actual_lookback = min(lookback, len(close) - 1)

        up_days_volume = []
        down_days_volume = []
        for i in range(-actual_lookback, 0):
            if close[i] > close[i - 1]:
                up_days_volume.append(volume[i])
            elif close[i] < close[i - 1]:
                down_days_volume.append(volume[i])

        if up_days_volume and down_days_volume:
            avg_up_volume = sum(up_days_volume) / len(up_days_volume)
            avg_down_volume = sum(down_days_volume) / len(down_days_volume)
            volume_ratio = avg_up_volume / avg_down_volume if avg_down_volume > 0 else 2.0
        elif up_days_volume:
            avg_up_volume = sum(up_days_volume) / len(up_days_volume)
            avg_down_volume = 0
            volume_ratio = 2.0  # 只有上涨日，看多
        else:
            avg_up_volume = 0
            avg_down_volume = sum(down_days_volume) / len(down_days_volume) if down_days_volume else 0
            volume_ratio = 0.3  # 只有下跌日，看空

        # 基础分
        if volume_ratio > 2.0:
            base_score = 18
            volume_type = "强势量价配合"
        elif volume_ratio > 1.5:
            base_score = 14
            volume_type = "放量上涨"
        elif volume_ratio > 1.0:
            base_score = 10
            volume_type = "量增价涨"
        elif volume_ratio > 0.8:
            base_score = 5
            volume_type = "量价持平"
        else:
            base_score = 0
            volume_type = "缩量上涨(警惕)"

        # 量价背离检测（价格在5日高点，但近期量能萎缩）
        divergence_penalty = 0
        price_at_5d_high = False
        volume_shrinking = False
        if len(close) >= 5 and len(volume) >= actual_lookback:
            price_at_5d_high = close[-1] >= max(close[-5:])
            recent_avg_vol = sum(volume[-5:]) / 5
            baseline_avg_vol = sum(volume[-actual_lookback:]) / actual_lookback if actual_lookback > 0 else 1
            volume_shrinking = recent_avg_vol < baseline_avg_vol * 0.8
            if price_at_5d_high and volume_shrinking:
                divergence_penalty = 5
                base_score = max(0, base_score - divergence_penalty)
                volume_type += "(量价背离！)"

        # OBV加成（3日新高且无背离）
        obv_new_high = len(close) >= 4 and close[-1] > close[-4]
        obv_bonus = 2 if (obv_new_high and base_score > 0 and not (price_at_5d_high and volume_shrinking)) else 0

        final_score = min(base_score + obv_bonus, 20)

        details = {
            'avg_up_volume': avg_up_volume,
            'avg_down_volume': avg_down_volume,
            'volume_ratio': volume_ratio,
            'volume_type': volume_type,
            'price_at_5d_high': price_at_5d_high,
            'volume_shrinking': volume_shrinking,
            'divergence_penalty': divergence_penalty,
            'obv_bonus': obv_bonus,
        }
        return final_score, details

    # -------------------------------------------------------------------------
    # 维度4：换手率分（0~+15分，仅正分）
    # -------------------------------------------------------------------------

    def _calculate_turnover_score(
        self, turnover_rate: Optional[List[float]]
    ) -> Tuple[int, dict]:
        """
        换手率/情绪分（15分）- 捕捉散户参与热度（仅正分）

        设计原理：
        - 换手率高本身不是坏信号，高换手意味着市场活跃参与
        - "天量是顶部"的负面含义（价格推不动了）由动能分（加速度）负责
        - 用5日/历史均值比值，自动适配蓝筹低换手与题材股高换手
        - 最佳区间1.5~3.0倍：健康放量；>3倍爆量也给正分但较低
        """
        if not turnover_rate:
            return 0, {'turnover_ratio': None, 'turnover_type': '无换手率数据'}

        short_period = self.config.get('turnover_lookback_short', 5)
        long_period = self.config.get('turnover_lookback_long', 60)

        # 过滤无效值
        valid = [t for t in turnover_rate if t is not None and t > 0]
        if len(valid) < short_period:
            return 0, {'turnover_ratio': None, 'turnover_type': '数据不足'}

        short_avg = sum(valid[-short_period:]) / short_period
        actual_long = min(long_period, len(valid))
        long_avg = sum(valid[-actual_long:]) / actual_long

        if long_avg <= 0:
            return 0, {'turnover_ratio': None, 'turnover_type': '历史基准为0'}

        ratio = short_avg / long_avg

        if 1.5 <= ratio <= 3.0:
            score = 15
            turnover_type = "健康放量(最佳区间)"
        elif ratio > 3.0:
            score = 8
            turnover_type = "爆量(情绪过热，动能分另行判断)"
        elif 1.0 <= ratio < 1.5:
            score = 10
            turnover_type = "适度放量"
        elif 0.5 <= ratio < 1.0:
            score = 5
            turnover_type = "正常换手"
        elif 0.3 <= ratio < 0.5:
            score = 3
            turnover_type = "缩量等待"
        else:
            score = 0
            turnover_type = "极度缩量"

        details = {
            'short_avg': short_avg,
            'long_avg': long_avg,
            'turnover_ratio': ratio,
            'turnover_type': turnover_type,
        }
        return score, details

    # -------------------------------------------------------------------------
    # 维度5：波动分（±10分，与趋势方向联动）
    # -------------------------------------------------------------------------

    def _calculate_volatility_score(
        self, close: List[float], high: List[float], low: List[float], trend_score: int = 0
    ) -> Tuple[int, dict]:
        """
        波动分（10分）- ATR百分位，与趋势方向联动

        设计原理：
        - 旧逻辑"低波+10，高波-10"方向固定，存在缺陷：
          牛市末期往往低波动（温水煮青蛙），熊市底部往往高波动（恐慌抛售）
          固定方向会在顶部高估牛市评分、在底部低估底部确认度
        - 新逻辑与趋势方向联动：
          上升趋势中低波动 = 缩量整理蓄势（+10）
          下跌趋势中高波动 = 恐慌抛售，可能接近阶段底部（+5）
          下跌趋势中低波动 = 阴跌无量，最危险（-10）
        """
        atr_period = self.config.get('atr_period', 5)
        history_length = min(self.lookback_days, 40)

        atr_history = []
        for i in range(history_length, atr_period, -1):
            start_idx = max(0, len(high) - i)
            end_idx = max(atr_period, len(high) - i + atr_period)
            if end_idx <= len(high):
                atr = self._calculate_atr(
                    high[start_idx:end_idx], low[start_idx:end_idx], close[start_idx:end_idx]
                )
                if atr > 0:
                    atr_history.append(atr)

        if not atr_history:
            return 0, {'percentile': 50, 'volatility_type': '数据不足'}

        current_atr = self._calculate_atr(high[-atr_period:], low[-atr_period:], close[-atr_period:])
        percentile = (sum(x < current_atr for x in atr_history) / len(atr_history)) * 100

        low_threshold = self.config['volatility_percentile_low']    # 40%
        high_threshold = self.config['volatility_percentile_high']  # 80%

        if trend_score > 0:  # 上升趋势
            if percentile < low_threshold:
                score = 10
                volatility_type = "上升+低波(缩量蓄势)"
            elif percentile < 70:
                score = 5
                volatility_type = "上升+正常波动"
            else:
                score = -5
                volatility_type = "上升+高波(注意变盘)"
        elif trend_score < 0:  # 下降趋势
            if percentile > high_threshold:
                score = 5
                volatility_type = "下跌+恐慌(可能阶段见底)"
            elif percentile > low_threshold:
                score = -5
                volatility_type = "下跌+正常波动"
            else:
                score = -10
                volatility_type = "下跌+低波(阴跌，最危险)"
        else:  # 趋势不明
            score = 0
            volatility_type = "趋势不明"

        details = {
            'current_atr': current_atr,
            'atr_percentile': percentile,
            'volatility_type': volatility_type,
        }
        return score, details

    # -------------------------------------------------------------------------
    # ATR 计算工具
    # -------------------------------------------------------------------------

    def _calculate_atr(self, high: List[float], low: List[float], close: List[float]) -> float:
        """计算平均真实波幅（ATR）"""
        if len(high) < 2 or len(low) < 2 or len(close) < 2:
            return 0

        tr_list = []
        for i in range(1, len(high)):
            tr = max(
                high[i] - low[i],
                abs(high[i] - close[i - 1]),
                abs(low[i] - close[i - 1])
            )
            tr_list.append(tr)

        return sum(tr_list) / len(tr_list) if tr_list else 0

    # -------------------------------------------------------------------------
    # 日志输出
    # -------------------------------------------------------------------------

    def _log_detection_details(
        self,
        stock_code: str,
        current_date,
        trend_score: int,
        trend_details: dict,
        momentum_score: int,
        momentum_details: dict,
        volume_score: int,
        volume_details: dict,
        turnover_score: int,
        turnover_details: dict,
        volatility_score: int,
        volatility_details: dict,
        total_score: int,
        regime: MarketRegime,
        confidence: float
    ):
        """输出市场状态检测日志"""
        logger.info(
            f"[{stock_code}] 总分: {total_score}/100 → {regime.value} "
            f"(置信度:{confidence:.2f}) "
            f"趋势:{trend_score} 动能:{momentum_score} "
            f"量价:{volume_score} 换手:{turnover_score} 波动:{volatility_score}"
        )

        logger.debug("=" * 60)
        logger.debug(f"【市场状态检测详情 v2.1】{stock_code} - {current_date}")
        logger.debug("-" * 60)
        logger.debug(f"趋势分 ({trend_score}/±30): {trend_details['arrangement']}")
        logger.debug(
            f"  EMA短={trend_details['ema_short']:.2f}, "
            f"EMA中={trend_details['ema_medium']:.2f}, "
            f"EMA长={trend_details['ema_long']:.2f}, "
            f"SMA锚={trend_details['sma_anchor']:.2f}"
        )
        logger.debug(f"动能分 ({momentum_score}/±25): {momentum_details.get('momentum_type', '')}")
        logger.debug(
            f"  当期ROC={momentum_details.get('current_roc', 0):.2f}%, "
            f"前期ROC={momentum_details.get('prev_roc', 0):.2f}%, "
            f"加速度={momentum_details.get('acceleration', 0):.2f}%"
        )
        logger.debug(f"量价分 ({volume_score}/0~20): {volume_details['volume_type']}")
        logger.debug(
            f"  量比={volume_details['volume_ratio']:.2f}, "
            f"背离惩罚={volume_details['divergence_penalty']}, "
            f"OBV加成={volume_details['obv_bonus']}"
        )
        logger.debug(f"换手率分 ({turnover_score}/0~15): {turnover_details.get('turnover_type', '无数据')}")
        if turnover_details.get('turnover_ratio') is not None:
            logger.debug(f"  换手率比={turnover_details['turnover_ratio']:.2f}x历史均值")
        logger.debug(f"波动分 ({volatility_score}/±10): {volatility_details['volatility_type']}")
        logger.debug(
            f"  ATR分位数={volatility_details['atr_percentile']:.1f}%, "
            f"当前ATR={volatility_details['current_atr']:.2f}"
        )
        logger.debug("-" * 60)
        logger.debug(f"总分: {total_score}/100 → {regime.value} (置信度:{confidence:.2f})")
        logger.debug("=" * 60)
