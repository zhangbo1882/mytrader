"""
相对估值模型

支持多种相对估值方法：
- PE估值：市盈率估值
- PB估值：市净率估值
- PS估值：市销率估值
- PEG估值：PEG比率估值
"""

import sys
import os
from typing import Dict, List, Optional, Any
import numpy as np

# 添加项目路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.valuation.models.base_valuation_model import BaseValuationModel
from src.valuation.data.valuation_data_loader import ValuationDataLoader
from src.valuation.config.industry_params import (
    get_industry_params,
    calculate_industry_adjustment,
    get_market_cap_premium,
    get_growth_premium
)


class RelativeValuationModel(BaseValuationModel):
    """
    相对估值模型

    支持PE/PB/PS/PEG等多种相对估值方法
    """

    def __init__(self, method: str = 'pe', config: Optional[Dict] = None):
        """
        初始化相对估值模型

        Args:
            method: 估值方法 ('pe', 'pb', 'ps', 'peg', 'combined')
            config: 配置参数（可包含fiscal_date）
        """
        super().__init__(f'Relative_{method.upper()}', config)
        self.method = method.lower()
        db_path = config.get('db_path') if config else None
        self.data_loader = ValuationDataLoader(db_path if db_path else "data/tushare_data.db")

        # 如果指定了fiscal_date，设置到data_loader
        fiscal_date = config.get('fiscal_date') if config else None
        if fiscal_date:
            self.data_loader.set_fiscal_date(fiscal_date)

    def calculate(
        self,
        symbol: str,
        date: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        计算相对估值

        Args:
            symbol: 股票代码
            date: 估值日期
            **kwargs: 额外参数

        Returns:
            估值结果
        """
        # 验证输入
        is_valid, error = self.validate_input(symbol, date)
        if not is_valid:
            return {
                'symbol': symbol,
                'date': date,
                'error': error
            }

        # 加载数据
        data = self.data_loader.load_relative_valuation_data(symbol, date)

        # 检查数据完整性
        if not data.get('price'):
            return {
                'symbol': symbol,
                'date': date,
                'error': 'No price data available'
            }

        current_price = data['price']
        industry_info = data.get('industry_info', {})
        industry_stats = data.get('industry_stats', {})
        financial_indicators = data.get('financial_indicators', {})
        market_cap = data.get('market_cap', {})

        # 根据估值方法计算公允价值
        if self.method == 'combined':
            fair_value = self._calculate_combined_valuation(
                data, **kwargs
            )
        else:
            fair_value = self._calculate_single_method_valuation(
                data, self.method, **kwargs
            )

        # 获取详细指标
        metrics = self._get_detailed_metrics(data)

        # 获取关键假设
        assumptions = self._get_assumptions(data)

        # 获取警告信息
        warnings = self._get_warnings(data)

        # 标准化结果
        return self._standardize_result(
            symbol,
            self._format_date(date),
            fair_value,
            current_price,
            metrics,
            assumptions,
            warnings
        )

    def get_required_data(self, symbol: str, date: Optional[str] = None) -> Dict[str, Any]:
        """
        定义所需数据

        Args:
            symbol: 股票代码
            date: 估值日期

        Returns:
            所需数据描述
        """
        return {
            'price_data': ['close', 'pe_ttm', 'pb', 'ps_ttm'],
            'financial_indicators': ['roe', 'net_margin', 'revenue_growth'],
            'industry_info': ['sw_l1', 'sw_l2', 'sw_l3'],
            'industry_stats': ['pe_ttm_median', 'pb_median', 'ps_ttm_median']
        }

    def _calculate_single_method_valuation(
        self,
        data: Dict[str, Any],
        method: str,
        **kwargs
    ) -> float:
        """
        使用单一方法计算估值

        Args:
            data: 估值数据
            method: 估值方法

        Returns:
            公允价值
        """
        current_price = data['price']
        multiples = data.get('valuation_multiples', {})
        financial_indicators = data.get('financial_indicators', {})
        industry_info = data.get('industry_info', {})
        industry_stats = data.get('industry_stats', {})
        market_cap = data.get('market_cap', {})

        if method == 'pe':
            return self._calculate_pe_valuation(
                current_price, multiples, financial_indicators,
                industry_info, industry_stats, market_cap
            )
        elif method == 'pb':
            return self._calculate_pb_valuation(
                current_price, multiples, financial_indicators,
                industry_info, industry_stats, market_cap
            )
        elif method == 'ps':
            return self._calculate_ps_valuation(
                current_price, multiples, financial_indicators,
                industry_info, industry_stats, market_cap
            )
        elif method == 'peg':
            return self._calculate_peg_valuation(
                current_price, multiples, financial_indicators,
                industry_info, industry_stats, market_cap
            )
        else:
            # 默认使用PE
            return current_price

    def _calculate_pe_valuation(
        self,
        current_price: float,
        multiples: Dict,
        financial_indicators: Dict,
        industry_info: Dict,
        industry_stats: Dict,
        market_cap: Dict
    ) -> float:
        """
        PE估值方法

        合理PE = 行业PE × 增长率调整 × ROE调整 × 市值调整
        """
        # 获取当前PE
        current_pe = multiples.get('pe_ttm')
        if not current_pe or current_pe <= 0:
            return current_price

        # 获取行业PE中位数
        industry_pe = industry_stats.get('pe_ttm_median')
        if not industry_pe:
            industry_pe = 20  # 默认值

        # ROE调整
        roe = financial_indicators.get('roe', 0)
        roe_adjustment = self._adjust_pe_for_roe(roe)

        # 增长率调整
        # 简化处理：用ROE作为增长率代理，实际应使用收入/利润增长率
        growth_rate = roe * 0.5  # 假设可持续增长率 = ROE × 留存率
        growth_adjustment = self._adjust_pe_for_growth(growth_rate)

        # 市值调整
        total_mv = market_cap.get('total_mv', 0) / 100000000  # 转换为亿元
        cap_adjustment = get_market_cap_premium(total_mv)

        # 计算合理PE
        reasonable_pe = industry_pe * roe_adjustment * growth_adjustment * cap_adjustment

        # 获取EPS来计算目标价格
        # 简化：使用当前价格和当前PE反推EPS
        eps = current_price / current_pe
        fair_value = reasonable_pe * eps

        return fair_value

    def _calculate_pb_valuation(
        self,
        current_price: float,
        multiples: Dict,
        financial_indicators: Dict,
        industry_info: Dict,
        industry_stats: Dict,
        market_cap: Dict
    ) -> float:
        """
        PB估值方法

        合理PB = 行业PB × ROE调整
        """
        current_pb = multiples.get('pb')
        if not current_pb or current_pb <= 0:
            return current_price

        # 获取行业PB中位数
        industry_pb = industry_stats.get('pb_median')
        if not industry_pb:
            industry_pb = 2.0  # 默认值

        # ROE调整
        roe = financial_indicators.get('roe', 0)
        pb_adjustment = self._adjust_pb_for_roe(roe)

        # 计算合理PB
        reasonable_pb = industry_pb * pb_adjustment

        # 获取BVPS来计算目标价格
        # 简化：使用当前价格和当前PB反推BVPS
        bvps = current_price / current_pb
        fair_value = reasonable_pb * bvps

        return fair_value

    def _calculate_ps_valuation(
        self,
        current_price: float,
        multiples: Dict,
        financial_indicators: Dict,
        industry_info: Dict,
        industry_stats: Dict,
        market_cap: Dict
    ) -> float:
        """
        PS估值方法

        合理PS = 行业PS × 净利率调整
        """
        current_ps = multiples.get('ps_ttm')
        if not current_ps or current_ps <= 0:
            return current_price

        # 获取行业PS中位数
        industry_ps = industry_stats.get('ps_ttm_median')
        if not industry_ps:
            industry_ps = 3.0  # 默认值

        # 净利率调整
        net_margin = financial_indicators.get('net_margin', 0)
        margin_adjustment = self._adjust_ps_for_margin(net_margin)

        # 计算合理PS
        reasonable_ps = industry_ps * margin_adjustment

        # 获取SPS来计算目标价格
        # 简化：使用当前价格和当前PS反推SPS
        sps = current_price / current_ps
        fair_value = reasonable_ps * sps

        return fair_value

    def _calculate_peg_valuation(
        self,
        current_price: float,
        multiples: Dict,
        financial_indicators: Dict,
        industry_info: Dict,
        industry_stats: Dict,
        market_cap: Dict
    ) -> float:
        """
        PEG估值方法

        PEG = PE / 增长率
        合理PE = PEG × 增长率
        """
        current_pe = multiples.get('pe_ttm')
        if not current_pe or current_pe <= 0:
            return current_price

        # 简化：使用ROE作为增长率代理
        roe = financial_indicators.get('roe', 0)
        growth_rate = roe * 0.5  # 可持续增长率

        if growth_rate <= 0:
            # 没有增长，给予较低估值
            return current_price * 0.8

        # PEG合理范围：1.0-1.5
        # 成长性越高，PEG可以越高
        peg = 1.0 + min(growth_rate / 50, 0.5)  # 增长率50%以上，PEG=1.5

        # 计算合理PE
        reasonable_pe = peg * growth_rate

        # 获取EPS来计算目标价格
        eps = current_price / current_pe
        fair_value = reasonable_pe * eps

        return fair_value

    def _calculate_combined_valuation(
        self,
        data: Dict[str, Any],
        **kwargs
    ) -> float:
        """
        组合多种估值方法

        Args:
            data: 估值数据

        Returns:
            公允价值
        """
        # 分别计算PE、PB、PS估值
        pe_value = self._calculate_pe_valuation(data['price'], data['valuation_multiples'],
                                                data['financial_indicators'], data['industry_info'],
                                                data['industry_stats'], data['market_cap'])

        pb_value = self._calculate_pb_valuation(data['price'], data['valuation_multiples'],
                                                data['financial_indicators'], data['industry_info'],
                                                data['industry_stats'], data['market_cap'])

        ps_value = self._calculate_ps_valuation(data['price'], data['valuation_multiples'],
                                                data['financial_indicators'], data['industry_info'],
                                                data['industry_stats'], data['market_cap'])

        # 简单平均
        fair_value = (pe_value + pb_value + ps_value) / 3

        return fair_value

    def _adjust_pe_for_roe(self, roe: float) -> float:
        """
        根据ROE调整PE

        ROE 15%为基准，ROE每增加1%，PE增加3%
        """
        baseline_roe = 15.0
        adjustment = 1.0 + (roe - baseline_roe) * 0.03
        return max(0.5, min(adjustment, 2.0))  # 限制在0.5-2.0倍

    def _adjust_pe_for_growth(self, growth_rate: float) -> float:
        """
        根据增长率调整PE

        增长率15%为基准，每增加1%，PE增加2%
        """
        baseline_growth = 15.0
        adjustment = 1.0 + (growth_rate - baseline_growth) * 0.02
        return max(0.5, min(adjustment, 2.0))

    def _adjust_pb_for_roe(self, roe: float) -> float:
        """
        根据ROE调整PB

        PB与ROE正相关
        ROE 10%为基准，每增加1%，PB增加5%
        """
        baseline_roe = 10.0
        adjustment = 1.0 + (roe - baseline_roe) * 0.05
        return max(0.5, min(adjustment, 2.0))

    def _adjust_ps_for_margin(self, net_margin: float) -> float:
        """
        根据净利率调整PS

        净利率越高，PS越高
        净利率10%为基准
        """
        baseline_margin = 10.0
        adjustment = 1.0 + (net_margin - baseline_margin) * 0.02
        return max(0.5, min(adjustment, 2.0))

    def _calculate_confidence(self, metrics: Dict, assumptions: Dict) -> float:
        """
        计算估值置信度

        置信度影响因素：
        - 行业统计数据的股票数量
        - 财务指标的稳定性
        - 当前估值倍数与行业的偏离度
        """
        confidence = 0.5  # 基础置信度

        # 行业数据质量调整
        industry_count = metrics.get('industry_count', 0)
        if industry_count > 30:
            confidence += 0.15
        elif industry_count > 10:
            confidence += 0.10
        elif industry_count > 5:
            confidence += 0.05

        # ROE稳定性调整
        roe = metrics.get('roe', 0)
        if roe > 15:
            confidence += 0.10
        elif roe > 10:
            confidence += 0.05
        elif roe < 5:
            confidence -= 0.10

        return max(0.0, min(confidence, 1.0))

    def _get_detailed_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        获取详细指标
        """
        metrics = {
            'current_pe': data.get('valuation_multiples', {}).get('pe_ttm'),
            'current_pb': data.get('valuation_multiples', {}).get('pb'),
            'current_ps': data.get('valuation_multiples', {}).get('ps_ttm'),
            'industry_pe': data.get('industry_stats', {}).get('pe_ttm_median'),
            'industry_pb': data.get('industry_stats', {}).get('pb_median'),
            'industry_ps': data.get('industry_stats', {}).get('ps_ttm_median'),
        }

        # 添加财务指标
        financial_indicators = data.get('financial_indicators', {})
        metrics['roe'] = financial_indicators.get('roe')
        metrics['roa'] = financial_indicators.get('roa')
        metrics['net_margin'] = financial_indicators.get('net_margin')
        metrics['gross_margin'] = financial_indicators.get('gross_margin')

        # 添加行业信息
        industry_info = data.get('industry_info', {})
        metrics['sw_l1'] = industry_info.get('sw_l1')
        metrics['sw_l2'] = industry_info.get('sw_l2')
        metrics['sw_l3'] = industry_info.get('sw_l3')

        # 添加市值信息
        market_cap = data.get('market_cap', {})
        metrics['total_mv'] = market_cap.get('total_mv')
        metrics['circ_mv'] = market_cap.get('circ_mv')

        return metrics

    def _get_assumptions(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        获取关键假设
        """
        return {
            'valuation_method': self.method,
            'industry_pe_used': data.get('industry_stats', {}).get('pe_ttm_median'),
            'industry_pb_used': data.get('industry_stats', {}).get('pb_median'),
            'industry_ps_used': data.get('industry_stats', {}).get('ps_ttm_median'),
            'roe_adjustment': 'based on ROE vs industry baseline',
            'growth_adjustment': 'based on growth rate vs industry baseline',
            'market_cap_adjustment': 'small/mid/large cap premium applied'
        }

    def _get_warnings(self, data: Dict[str, Any]) -> List[str]:
        """
        获取警告信息
        """
        warnings = []

        # 检查数据完整性
        if not data.get('industry_stats'):
            warnings.append('No industry statistics available, using default values')

        if not data.get('financial_indicators'):
            warnings.append('No financial indicators available')

        multiples = data.get('valuation_multiples', {})
        if not multiples.get('pe_ttm') or multiples['pe_ttm'] <= 0:
            warnings.append('Invalid PE ratio')

        if not multiples.get('pb') or multiples['pb'] <= 0:
            warnings.append('Invalid PB ratio')

        # 检查估值合理性
        if multiples.get('pe_ttm', 0) > 100:
            warnings.append('PE ratio > 100, valuation may be unrealistic')

        return warnings
