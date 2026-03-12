"""
绝对估值模型

支持多种绝对估值方法：
- DCF: 自由现金流折现模型
- DDM: 股息折现模型
- RIM: 剩余收益模型
"""

import sys
import os
from typing import Dict, List, Optional, Any
import numpy as np

# 添加项目路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.valuation.models.base_valuation_model import BaseValuationModel
from src.valuation.data.valuation_data_loader import ValuationDataLoader


class DCFValuationModel(BaseValuationModel):
    """
    自由现金流折现模型 (DCF)

    核心步骤：
    1. 预测未来N年自由现金流
    2. 计算WACC
    3. 折现FCF得到现值
    4. 计算终值
    """

    def __init__(self, config: Optional[Dict] = None):
        """
        初始化DCF模型

        Args:
            config: 配置参数
                - forecast_years: 预测年数 (默认5)
                - terminal_growth: 终值增长率 (默认0.02，成熟公司2%)
                - risk_free_rate: 无风险利率 (默认0.025)
                - market_return: 市场回报率 (默认0.09，A股合理水平)
                - tax_rate: 企业所得税率 (默认0.25)
                - credit_spread: 债务信用利差 (默认0.025)
                - growth_rate_cap: 收入增长率上限 (默认0.06，成长公司6%)
                - wacc_min: WACC下限 (默认0.07)
                - wacc_max: WACC上限 (默认0.12)
                - beta: Beta系数 (可选，如果不提供则根据行业自动计算)
        """
        super().__init__('DCF', config)
        self.forecast_years = config.get('forecast_years', 5) if config else 5
        # 终值增长率2.5%（中国名义GDP增长约4-5%，保守取2.5%）
        self.terminal_growth = config.get('terminal_growth', 0.025) if config else 0.025
        # 无风险利率2.3%（当前10年国债约2.3%）
        self.risk_free_rate = config.get('risk_free_rate', 0.023) if config else 0.023
        # 市场回报率9%（A股合理水平）
        self.market_return = config.get('market_return', 0.09) if config else 0.09

        # 新增可配置参数
        self.tax_rate = config.get('tax_rate', 0.25) if config else 0.25
        # 信用利差2.5%
        self.credit_spread = config.get('credit_spread', 0.025) if config else 0.025
        # 增长率上限6%
        self.growth_rate_cap = config.get('growth_rate_cap', 0.06) if config else 0.06
        # WACC下限7%
        self.wacc_min = config.get('wacc_min', 0.07) if config else 0.07
        # WACC上限12%
        self.wacc_max = config.get('wacc_max', 0.12) if config else 0.12
        self.user_beta = config.get('beta', None) if config else None  # 用户指定的beta

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
        计算DCF估值

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

        # 加载DCF所需数据
        data = self.data_loader.load_dcf_valuation_data(symbol, date)

        # 严格检查数据完整性 - 任何关键数据缺失都应该报错

        # 1. 检查自由现金流数据
        fcf_data = data.get('free_cash_flow', [])
        if not fcf_data or len(fcf_data) == 0:
            return {
                'symbol': symbol,
                'date': date,
                'error': 'Insufficient data: free_cash_flow data is missing or empty. '
                       'Please ensure cashflow table has valid free_cashflow data.'
            }

        # 2. 检查股本信息
        shares_info = data.get('shares_info', {})
        total_shares = shares_info.get('total_share', 0)
        if not total_shares or total_shares <= 0:
            return {
                'symbol': symbol,
                'date': date,
                'error': 'Insufficient data: shares_info (total_share) is missing or invalid. '
                       'Please ensure daily_basic table exists and has share data.'
            }

        # 3. 检查资本结构
        capital_structure = data.get('capital_structure', {})
        if not capital_structure or not capital_structure.get('total_assets'):
            return {
                'symbol': symbol,
                'date': date,
                'error': 'Insufficient data: capital_structure data is missing. '
                       'Please ensure balancesheet table has data for this stock.'
            }

        # 4. 检查现金及现金等价物数据
        cash_date_used = data.get('cash_date_used')
        cash_equivalents = data.get('cash_equivalents')
        if cash_date_used is None or cash_equivalents is None:
            return {
                'symbol': symbol,
                'date': date,
                'error': 'Insufficient data: cash_equivalents data is missing. '
                       'Please ensure cashflow.end_bal_cash exists for the selected report period.'
            }

        # 获取当前价格
        price_data = self.data_loader._get_latest_price_data(
            self._standardize_code(symbol), date
        )
        current_price = price_data.get('close', 0)

        if not current_price:
            return {
                'symbol': symbol,
                'date': date,
                'error': 'No price data available'
            }

        # 计算WACC
        wacc = self._calculate_wacc(data)

        # 预测未来自由现金流
        fcf_forecast = self._forecast_fcf(data)

        # 折现FCF
        pv_fcf = self._discount_fcf(fcf_forecast, wacc)

        # 计算终值
        terminal_value = self._calculate_terminal_value(fcf_forecast[-1], wacc)

        # 折现终值
        pv_terminal = terminal_value / ((1 + wacc) ** self.forecast_years)

        # 计算企业价值
        enterprise_value = pv_fcf + pv_terminal

        # 获取现金及有息债务，计算净现金
        capital_structure = data.get('capital_structure', {})
        cash_equivalents = data.get('cash_equivalents', 0)

        # 计算有息债务
        interest_bearing_debt = capital_structure.get('interest_bearing_debt', 0) or 0

        # 计算净现金 = 现金及现金等价物 - 有息债务
        net_cash = cash_equivalents - interest_bearing_debt

        # 转换单位：元 -> 亿元
        net_cash_billions = net_cash / 100000000 if net_cash else 0

        # 计算股权价值 = 企业价值 - 净债务 + 净现金
        # 或者：股权价值 = 企业价值 + 净现金
        # 注意：净现金 = -净债务
        equity_value = enterprise_value + net_cash_billions

        # 计算每股价值
        # total_shares 单位是万股，转换为亿股
        total_shares_yi = total_shares / 10000  # 万 -> 亿股
        fair_value = equity_value / total_shares_yi

        # 获取详细指标
        metrics = self._get_detailed_metrics(data, wacc, fcf_forecast, terminal_value, net_cash_billions)

        # 获取关键假设
        assumptions = self._get_assumptions(data)

        # 获取警告
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
        """
        return {
            'cashflow_data': ['operating_cashflow', 'capex'],
            'capital_structure': ['total_assets', 'total_liability', 'total_owner_equities'],
            'shares_info': ['total_share', 'float_share']
        }

    def _calculate_wacc(self, data: Dict[str, Any]) -> float:
        """
        计算加权平均资本成本 (WACC)

        WACC = E/V * Re + D/V * Rd * (1 - T)

        Args:
            data: 估值数据

        Returns:
            WACC
        """
        # 获取资本结构
        capital_structure = data.get('capital_structure', {})
        total_assets = capital_structure.get('total_assets', 0)
        total_liability = capital_structure.get('total_liability', 0)
        total_equity = capital_structure.get('total_owner_equities', 0)

        if not total_assets or total_assets <= 0:
            return 0.10  # 默认10%

        # 计算债务和股权比例
        debt_ratio = total_liability / total_assets if total_liability else 0
        equity_ratio = total_equity / total_assets if total_equity else 1 - debt_ratio

        # 计算股权成本 (CAPM)
        # 优先使用用户指定的beta，否则使用计算出的beta
        beta = self.user_beta if self.user_beta is not None else data.get('beta', 1.0)
        re = self.risk_free_rate + beta * (self.market_return - self.risk_free_rate)

        # 债务成本 (使用可配置的信用利差)
        rd = self.risk_free_rate + self.credit_spread

        # 税率 (使用可配置的税率)
        tax_rate = self.tax_rate

        # 计算WACC
        wacc = equity_ratio * re + debt_ratio * rd * (1 - tax_rate)

        return max(self.wacc_min, min(wacc, self.wacc_max))  # 使用可配置的上下限

    def _forecast_fcf(self, data: Dict[str, Any]) -> List[float]:
        """
        预测未来自由现金流

        使用历史增长率预测，基准FCF使用多年平均值以平滑波动

        Args:
            data: 估值数据

        Returns:
            预测的FCF列表
        """
        fcf_history = data.get('free_cash_flow', [])

        if not fcf_history or len(fcf_history) < 2:
            # 没有历史数据，使用默认增长率
            base_fcf = 100  # 默认基准
            growth_rate = 0.10  # 默认10%增长
        else:
            # 数据库中 FCF 以元为单位，需要转换为亿元
            # 使用所有年份（含负值）的FCF计算平均值，避免系统性高估
            all_fcf = [record.get('free_cashflow', 0) for record in fcf_history if record.get('free_cashflow') is not None]

            if not all_fcf:
                base_fcf = 100
            else:
                avg_years = min(5, len(all_fcf))
                fcf_to_avg = all_fcf[:avg_years]
                base_fcf_raw = np.mean(fcf_to_avg)

                if base_fcf_raw <= 0:
                    # 平均FCF为负，DCF不适用
                    base_fcf = 0  # 后续会产生0估值，触发警告
                else:
                    base_fcf = base_fcf_raw / 100000000  # 元 -> 亿元

            # 计算历史增长率
            growth_history = data.get('revenue_growth', [])
            if growth_history:
                growth_rate = np.mean(growth_history)
                growth_rate = max(0.0, min(growth_rate, self.growth_rate_cap))
            else:
                growth_rate = self.growth_rate_cap

        # 预测未来FCF（增长率逐年递减）
        fcf_forecast = []
        for i in range(self.forecast_years):
            # 增长率逐年衰减到长期增长率
            decay_factor = 1 - (i / self.forecast_years)
            current_growth = growth_rate * decay_factor + self.terminal_growth * (1 - decay_factor)
            fcf_forecast.append(base_fcf * ((1 + current_growth) ** (i + 1)))

        return fcf_forecast

    def _discount_fcf(self, fcf_forecast: List[float], wacc: float) -> float:
        """
        折现未来自由现金流

        Args:
            fcf_forecast: 预测的FCF列表
            wacc: 折现率

        Returns:
            现值总和
        """
        pv_sum = 0
        for i, fcf in enumerate(fcf_forecast):
            pv = fcf / ((1 + wacc) ** (i + 1))
            pv_sum += pv

        return pv_sum

    def _calculate_terminal_value(self, last_fcf: float, wacc: float) -> float:
        """
        计算终值 (永续增长模型)

        TV = FCF_n * (1 + g) / (WACC - g)

        Args:
            last_fcf: 最后一期FCF
            wacc: WACC

        Returns:
            终值
        """
        if wacc <= self.terminal_growth:
            # WACC必须大于终值增长率
            wacc = self.terminal_growth + 0.01

        terminal_value = last_fcf * (1 + self.terminal_growth) / (wacc - self.terminal_growth)

        return terminal_value

    def _get_detailed_metrics(
        self,
        data: Dict[str, Any],
        wacc: float,
        fcf_forecast: List[float],
        terminal_value: float,
        net_cash: float = 0
    ) -> Dict[str, Any]:
        """
        获取详细指标（包含DCF置信度所需字段）
        """
        capital_structure = data.get('capital_structure', {})
        beta = self.user_beta if self.user_beta is not None else data.get('beta', 1.0)

        cash_equivalents = data.get('cash_equivalents', 0) / 100000000  # 元 -> 亿元
        interest_bearing_debt = capital_structure.get('interest_bearing_debt', 0) or 0

        # 计算FCF变异系数（用于置信度）
        fcf_history = data.get('free_cash_flow', [])
        fcf_cv = None
        fcf_years = 0
        if fcf_history:
            all_fcf = [r.get('free_cashflow', 0) for r in fcf_history if r.get('free_cashflow') is not None]
            fcf_years = len(all_fcf)
            if len(all_fcf) >= 2:
                fcf_mean = abs(np.mean(all_fcf))
                if fcf_mean > 0:
                    fcf_cv = np.std(all_fcf) / fcf_mean

        # FCF/净利润比率
        net_income = data.get('net_income', 0)
        fcf_to_ni_ratio = None
        if fcf_history and net_income and net_income > 0:
            latest_fcf = fcf_history[0].get('free_cashflow', 0) if fcf_history else 0
            if latest_fcf is not None:
                fcf_to_ni_ratio = latest_fcf / net_income

        return {
            'wacc': round(wacc, 4),
            'forecast_years': self.forecast_years,
            'terminal_growth': self.terminal_growth,
            'fcf_forecast': [round(fcf, 2) for fcf in fcf_forecast],
            'terminal_value': round(terminal_value, 2),
            'beta': beta,
            'debt_ratio': capital_structure.get('total_liability', 0) / capital_structure.get('total_assets', 1),
            'equity_ratio': capital_structure.get('total_owner_equities', 0) / capital_structure.get('total_assets', 1),
            'risk_free_rate': self.risk_free_rate,
            'market_return': self.market_return,
            'tax_rate': self.tax_rate,
            'growth_rate_cap': self.growth_rate_cap,
            'cash_equivalents_billions': round(cash_equivalents, 2),
            'interest_bearing_debt_billions': round(interest_bearing_debt / 100000000, 2),
            'net_cash_billions': round(net_cash, 2),
            'fcf_cv': round(fcf_cv, 3) if fcf_cv is not None else None,
            'fcf_years': fcf_years,
            'fcf_to_ni_ratio': round(fcf_to_ni_ratio, 3) if fcf_to_ni_ratio is not None else None,
        }

    def _get_assumptions(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        获取关键假设
        """
        beta = self.user_beta if self.user_beta is not None else data.get('beta', 1.0)

        # FCF 数据的报告期范围
        fcf_history = data.get('free_cash_flow', [])
        fcf_dates = sorted([r['end_date'] for r in fcf_history if r.get('end_date')])
        fcf_date_range = None
        if fcf_dates:
            def fmt(d):
                d = str(d)
                return f"{d[:4]}-{d[4:6]}-{d[6:]}"
            fcf_date_range = f"{fmt(fcf_dates[0])} ~ {fmt(fcf_dates[-1])}"

        # 资本结构（资产负债表）使用的报告期
        capital_structure = data.get('capital_structure', {})
        bs_date = capital_structure.get('end_date')
        bs_date_str = None
        if bs_date:
            bs_date = str(bs_date)
            bs_date_str = f"{bs_date[:4]}-{bs_date[4:6]}-{bs_date[6:]}"

        # 现金数据使用的报告期
        cash_date = data.get('cash_date_used')
        cash_date_str = None
        if cash_date:
            cash_date = str(cash_date)
            cash_date_str = f"{cash_date[:4]}-{cash_date[4:6]}-{cash_date[6:]}"

        result = {
            'forecast_years': self.forecast_years,
            'terminal_growth_rate': f'{self.terminal_growth * 100}%',
            'wacc': f'{self._calculate_wacc(data) * 100}%',
            'risk_free_rate': f'{self.risk_free_rate * 100}%',
            'market_return': f'{self.market_return * 100}%',
            'beta': beta,
            'growth_assumption': '基于历史营收增长率',
            'tax_rate': f'{self.tax_rate * 100}%',
            'credit_spread': f'{self.credit_spread * 100}%',
            'growth_rate_cap': f'{self.growth_rate_cap * 100}%',
            'fcf_date_range': fcf_date_range,
            'balance_sheet_date': bs_date_str,
            'cash_date': cash_date_str,
        }

        # 最新一年 FCFF 拆解，供用户理解数据来源
        if fcf_history:
            latest = fcf_history[0]
            interest = latest.get('interest_expense', 0) or 0
            tax = latest.get('tax_rate', 0.25) or 0.25
            result['fcf_latest_breakdown'] = {
                'operating_cf': latest.get('operating_cashflow'),
                'interest_add_back': round(interest * (1 - tax), 2),
                'capex': latest.get('capex'),
                'fcff': latest.get('free_cashflow'),
                'tax_rate_used': round(tax, 4),
            }

        return result

    def _get_warnings(self, data: Dict[str, Any]) -> List[str]:
        """
        获取警告信息
        """
        warnings = []

        if not data.get('free_cash_flow'):
            warnings.append('无历史FCFF数据，使用默认假设')

        capital_structure = data.get('capital_structure', {})
        if not capital_structure:
            warnings.append('无资本结构数据')

        fcf_history = data.get('free_cash_flow', [])
        if fcf_history and len(fcf_history) < 3:
            warnings.append('历史FCFF数据不足3年，预测结果可靠性较低')

        # 检查是否存在负值FCF
        if fcf_history:
            all_fcf = [r.get('free_cashflow', 0) for r in fcf_history if r.get('free_cashflow') is not None]
            neg_count = sum(1 for v in all_fcf if v < 0)
            if neg_count > 0:
                avg_fcf = np.mean(all_fcf) if all_fcf else 0
                if avg_fcf <= 0:
                    warnings.append(f'平均FCF为负（含{neg_count}年负值），DCF估值结果不可靠，建议参考PE/PB估值')
                else:
                    warnings.append(f'存在{neg_count}年负FCF，已含入均值计算')

        # 检查利息数据是否真正缺失（fin_exp_int_exp 和 fin_exp 均为 NULL）
        if fcf_history:
            missing_interest = sum(
                1 for r in fcf_history
                if r.get('interest_data_missing', 0)
            )
            if missing_interest > 0:
                warnings.append(f'利息数据缺失（{missing_interest}年），FCFF中利息加回=0（使用简化FCF）')

        # 检查FCF/净利润比率，识别高分红公司
        net_income = data.get('net_income', 0)
        if fcf_history and net_income and net_income > 0:
            latest_fcf = fcf_history[0].get('free_cashflow', 0) if fcf_history else 0
            if latest_fcf:
                fcf_to_net_income_ratio = latest_fcf / net_income
                if fcf_to_net_income_ratio < 0.75:
                    warnings.append(
                        f'警告: FCF/净利润比率仅为{fcf_to_net_income_ratio*100:.1f}%，'
                        f'可能是高分红公司。DCF估值基于FCF，可能低估此类公司价值，'
                        f'建议同时参考PE估值。'
                    )

        # 检查三个财报日期的一致性
        capital_structure = data.get('capital_structure', {})
        bs_date = str(capital_structure.get('end_date', '')) if capital_structure.get('end_date') else None
        cash_date_used = data.get('cash_date_used')
        fcf_latest_date = str(fcf_history[0]['end_date']) if fcf_history and fcf_history[0].get('end_date') else None

        if bs_date and cash_date_used and bs_date != str(cash_date_used):
            bs_fmt = f"{bs_date[:4]}-{bs_date[4:6]}-{bs_date[6:]}"
            cash_fmt = f"{str(cash_date_used)[:4]}-{str(cash_date_used)[4:6]}-{str(cash_date_used)[6:]}"
            warnings.append(f'资产负债表日期（{bs_fmt}）与现金数据日期（{cash_fmt}）不一致，资本结构与现金数据来自不同报告期')

        if bs_date and fcf_latest_date:
            bs_year = int(bs_date[:4])
            fcf_year = int(fcf_latest_date[:4])
            gap = bs_year - fcf_year
            if gap >= 2:
                bs_fmt = f"{bs_date[:4]}-{bs_date[4:6]}-{bs_date[6:]}"
                fcf_fmt = f"{fcf_latest_date[:4]}-{fcf_latest_date[4:6]}-{fcf_latest_date[6:]}"
                warnings.append(f'FCF历史数据最新至{fcf_fmt}，距资产负债表日期（{bs_fmt}）已超过{gap}年，现金流预测基准可能过时')

        return warnings

    def _calculate_confidence(self, metrics: Dict, assumptions: Dict) -> float:
        """
        DCF差异化置信度计算

        基础0.5 + FCF稳定性 + FCF/净利润质量 + 增长合理性
        """
        confidence = 0.5

        # FCF稳定性（变异系数）
        fcf_cv = metrics.get('fcf_cv')
        if fcf_cv is not None:
            if fcf_cv < 0.3:
                confidence += 0.15
            elif fcf_cv < 0.5:
                confidence += 0.08
            elif fcf_cv > 1.0:
                confidence -= 0.15

        # FCF/净利润质量
        fcf_to_ni_ratio = metrics.get('fcf_to_ni_ratio')
        if fcf_to_ni_ratio is not None:
            if fcf_to_ni_ratio > 0.8:
                confidence += 0.10
            elif fcf_to_ni_ratio < 0:
                confidence -= 0.15  # 负FCF/正净利润，质量差

        # 增长合理性：历史增长vs预测增长
        growth_rate_cap = metrics.get('growth_rate_cap', 0.06)
        if growth_rate_cap <= 0.04:
            confidence += 0.05  # 保守预测更可靠
        elif growth_rate_cap >= 0.08:
            confidence -= 0.05  # 激进预测风险高

        # 数据年数
        fcf_years = metrics.get('fcf_years', 0)
        if fcf_years >= 5:
            confidence += 0.08
        elif fcf_years >= 3:
            confidence += 0.04
        elif fcf_years < 2:
            confidence -= 0.10

        return max(0.3, min(confidence, 0.9))

    def _standardize_code(self, symbol: str) -> str:
        """标准化股票代码"""
        if '.' in symbol:
            return symbol.split('.')[0]
        return symbol


class DDMValuationModel(BaseValuationModel):
    """
    股息折现模型 (DDM)

    适用于稳定分红的公司
    """

    def __init__(self, config: Optional[Dict] = None):
        """
        初始化DDM模型

        Args:
            config: 配置参数
        """
        super().__init__('DDM', config)
        self.required_yield = config.get('required_yield', 0.08) if config else 0.08
        self.dividend_growth = config.get('dividend_growth', 0.03) if config else 0.03

    def calculate(
        self,
        symbol: str,
        date: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        计算DDM估值（简化实现）

        注：完整的DDM需要股息历史数据
        """
        # 简化实现：使用Gordon增长模型
        # P = D1 / (r - g)

        # 这里返回提示信息，因为需要股息数据
        return {
            'symbol': symbol,
            'date': date,
            'model': 'DDM',
            'error': 'DDM model requires dividend data. Not yet fully implemented.'
        }

    def get_required_data(self, symbol: str, date: Optional[str] = None) -> Dict[str, Any]:
        """
        定义所需数据
        """
        return {
            'dividend_history': ['dividend_per_share'],
            'required_yield': float,
            'dividend_growth_rate': float
        }


class RIMValuationModel(BaseValuationModel):
    """
    剩余收益模型 (RIM)

    基于账面价值和剩余收益
    """

    def __init__(self, config: Optional[Dict] = None):
        """
        初始化RIM模型

        Args:
            config: 配置参数
        """
        super().__init__('RIM', config)
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
        计算RIM估值

        V = B0 + sum(RI_t / (1 + r)^t)

        其中 RI_t = NI_t - r * B_{t-1}
        """
        # 简化实现，需要详细的财务数据
        return {
            'symbol': symbol,
            'date': date,
            'model': 'RIM',
            'error': 'RIM model requires detailed financial data. Not yet fully implemented.'
        }

    def get_required_data(self, symbol: str, date: Optional[str] = None) -> Dict[str, Any]:
        """
        定义所需数据
        """
        return {
            'book_value': ['total_owner_equities'],
            'net_income': ['n_income_attr_p'],
            'cost_of_equity': float
        }
