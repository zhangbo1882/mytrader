"""
相对估值模型

支持多种相对估值方法：
- PE估值：市盈率估值（使用财报EPS_TTM，行业ROE基准）
- PB估值：市净率估值（使用财报BVPS）
- PS估值：市销率估值（使用财报SPS_TTM）
- PEG估值：PEG比率估值（标准方法：合理PE=净利润增长率%）
"""

import sys
import os
from typing import Dict, List, Optional, Any
import numpy as np

# 添加项目路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from src.valuation.models.base_valuation_model import BaseValuationModel
from src.valuation.data.valuation_data_loader import ValuationDataLoader
from src.valuation.config.industry_params import (
    get_industry_params,
    get_industry_params_by_name,
    get_industry_method_weights,
    get_market_cap_premium,
)


class RelativeValuationModel(BaseValuationModel):
    """
    相对估值模型

    支持PE/PB/PS/PEG等多种相对估值方法
    """

    # PE估值的字段需求
    PE_REQUIRED_FIELDS = {
        "income": [
            "n_income_attr_p",  # 归属母公司净利润（用于计算eps_ttm）
            "revenue",  # 营业收入
        ],
        "balancesheet": [
            "total_share",  # 总股本
            "total_hldr_eqy_exc_min_int",  # 归属母公司净资产
        ],
        "fina_indicator": [
            "roe"  # 净资产收益率
        ],
        # 注意：PE不要求cashflow表
    }

    # PB估值的字段需求
    PB_REQUIRED_FIELDS = {
        "balancesheet": ["total_share", "total_hldr_eqy_exc_min_int"],
        "fina_indicator": ["roe"],
        # PB不要求income和cashflow表
    }

    # PS估值的字段需求
    PS_REQUIRED_FIELDS = {
        "income": [
            "revenue"  # 营业收入
        ],
        "balancesheet": ["total_share"],
        "fina_indicator": [
            "netprofit_margin"  # 净利率
        ],
        # PS不要求cashflow表
    }

    # PEG估值的字段需求（同PE，额外需要增长率）
    PEG_REQUIRED_FIELDS = {
        "income": ["n_income_attr_p", "revenue"],
        "balancesheet": ["total_share", "total_hldr_eqy_exc_min_int"],
        "fina_indicator": ["roe"],
        # PEG需要至少2-3年的数据来计算增长率
    }

    def __init__(self, method: str = "pe", config: Optional[Dict] = None):
        """
        初始化相对估值模型

        Args:
            method: 估值方法 ('pe', 'pb', 'ps', 'peg', 'combined')
            config: 配置参数（可包含fiscal_date）
        """
        super().__init__(f"Relative_{method.upper()}", config)
        self.method = method.lower()
        db_path = config.get("db_path") if config else None
        self.data_loader = ValuationDataLoader(
            db_path if db_path else "data/tushare_data.db"
        )

        # 如果指定了fiscal_date，设置到data_loader
        fiscal_date = config.get("fiscal_date") if config else None
        if fiscal_date:
            self.data_loader.set_fiscal_date(fiscal_date)

    def calculate(
        self, symbol: str, date: Optional[str] = None, **kwargs
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
            return {"symbol": symbol, "date": date, "error": error}

        # 加载数据
        data = self.data_loader.load_relative_valuation_data(symbol, date)

        # 检查数据完整性
        if not data.get("price"):
            return {"symbol": symbol, "date": date, "error": "No price data available"}

        # 港股检测：若无任何基本面数据，给出专门提示
        is_hk = symbol.upper().endswith(".HK") or (
            symbol.replace(".", "").isdigit() and len(symbol.replace(".", "")) in (4, 5)
        )
        if is_hk:
            fi = data.get("financial_indicators", {})
            psd = data.get("per_share_data", {})
            multiples = data.get("valuation_multiples", {})
            if not fi and not psd and not multiples.get("pe_ttm"):
                return {
                    "symbol": symbol,
                    "date": date,
                    "error": "港股基本面数据（财务指标/PE/PB/PS）不在数据库中，无法进行基本面估值。仅有行情价格数据。",
                }

        # 检查关键数据完整性
        missing_data = self._check_missing_data(data)
        if missing_data:
            return {
                "symbol": symbol,
                "date": date,
                "error": f"数据缺失，无法进行可靠估值: {', '.join(missing_data)}。请补充相关数据后再试。",
            }

        current_price = data["price"]

        # 根据估值方法计算公允价值
        if self.method == "combined":
            fair_value = self._calculate_combined_valuation(data, **kwargs)
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
            warnings,
        )

    def get_required_data(
        self, symbol: str, date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        定义所需数据
        """
        return {
            "price_data": ["close", "pe_ttm", "pb", "ps_ttm"],
            "financial_indicators": ["roe", "net_margin", "revenue_growth"],
            "per_share_data": ["eps_ttm", "bvps", "sps_ttm"],
            "industry_info": ["sw_l1", "sw_l2", "sw_l3"],
            "industry_stats": ["pe_ttm_median", "pb_median", "ps_ttm_median"],
        }

    def get_required_fields(self, method: str = None) -> Dict[str, List[str]]:
        """
        获取指定估值方法的字段需求

        Args:
            method: 估值方法（pe/pb/ps/peg），不传则使用当前方法

        Returns:
            字段需求字典，如：
            {
                'income': ['n_income_attr_p', 'revenue'],
                'balancesheet': ['total_share', 'total_hldr_eqy_exc_min_int'],
                'fina_indicator': ['roe']
            }
        """
        method = method or self.method

        field_mapping = {
            "pe": self.PE_REQUIRED_FIELDS,
            "pb": self.PB_REQUIRED_FIELDS,
            "ps": self.PS_REQUIRED_FIELDS,
            "peg": self.PEG_REQUIRED_FIELDS,
            "combined": self.PE_REQUIRED_FIELDS,  # combined使用最严格的PE字段
        }

        return field_mapping.get(method, self.PE_REQUIRED_FIELDS)

    def _check_missing_data(self, data: Dict[str, Any]) -> List[str]:
        """
        检查估值所需的关键数据是否缺失
        """
        missing = []

        # PE/PEG需要EPS
        if self.method in ("pe", "peg", "combined"):
            per_share = data.get("per_share_data", {})
            multiples = data.get("valuation_multiples", {})
            if not per_share.get("eps_ttm") and not multiples.get("pe_ttm"):
                missing.append("EPS(TTM)或PE数据")

        # PB需要BVPS
        if self.method in ("pb", "combined"):
            per_share = data.get("per_share_data", {})
            multiples = data.get("valuation_multiples", {})
            if not per_share.get("bvps") and not multiples.get("pb"):
                missing.append("BVPS或PB数据")

        # PS需要SPS
        if self.method in ("ps", "combined"):
            per_share = data.get("per_share_data", {})
            multiples = data.get("valuation_multiples", {})
            if not per_share.get("sps_ttm") and not multiples.get("ps_ttm"):
                missing.append("SPS(TTM)或PS数据")

        # 检查财务指标
        financial_indicators = data.get("financial_indicators", {})
        if not financial_indicators.get("roe"):
            missing.append("ROE(净资产收益率)")

        # 检查行业统计（仅在combined或pe/pb/ps单方法时）
        industry_stats = data.get("industry_stats", {})
        if not industry_stats.get("pe_ttm_median") and self.method in (
            "pe",
            "combined",
        ):
            missing.append("行业PE中位数")
        if not industry_stats.get("pb_median") and self.method in ("pb", "combined"):
            missing.append("行业PB中位数")

        return missing

    def _calculate_single_method_valuation(
        self, data: Dict[str, Any], method: str, **kwargs
    ) -> float:
        """
        使用单一方法计算估值
        """
        current_price = data["price"]
        multiples = data.get("valuation_multiples", {})
        financial_indicators = data.get("financial_indicators", {})
        industry_info = data.get("industry_info", {})
        industry_stats = data.get("industry_stats", {})
        market_cap = data.get("market_cap", {})
        per_share_data = data.get("per_share_data", {})

        if method == "pe":
            return self._calculate_pe_valuation(
                current_price,
                multiples,
                financial_indicators,
                industry_info,
                industry_stats,
                market_cap,
                per_share_data,
                data,
            )
        elif method == "pb":
            return self._calculate_pb_valuation(
                current_price,
                multiples,
                financial_indicators,
                industry_info,
                industry_stats,
                market_cap,
                per_share_data,
            )
        elif method == "ps":
            return self._calculate_ps_valuation(
                current_price,
                multiples,
                financial_indicators,
                industry_info,
                industry_stats,
                market_cap,
                per_share_data,
            )
        elif method == "peg":
            return self._calculate_peg_valuation(
                current_price,
                multiples,
                financial_indicators,
                industry_info,
                industry_stats,
                market_cap,
                per_share_data,
                data,
            )
        else:
            return current_price

    def _get_industry_roe_baseline(self, industry_info: Dict) -> float:
        """从行业参数获取ROE基准，若无则使用A股平均12%"""
        sw_l1_code = industry_info.get("sw_l1_code")
        sw_l1_name = industry_info.get("sw_l1")

        params = None
        if sw_l1_code:
            params = get_industry_params(sw_l1_code)
        if params is None and sw_l1_name:
            params = get_industry_params_by_name(sw_l1_name)

        if params:
            return params.get("roe_baseline", 12.0)
        return 12.0  # A股平均ROE约10-12%

    def _get_industry_growth_baseline(self, industry_info: Dict) -> float:
        """从行业参数获取增长率基准，若无则使用10%"""
        sw_l1_code = industry_info.get("sw_l1_code")
        sw_l1_name = industry_info.get("sw_l1")

        params = None
        if sw_l1_code:
            params = get_industry_params(sw_l1_code)
        if params is None and sw_l1_name:
            params = get_industry_params_by_name(sw_l1_name)

        if params:
            return params.get("growth_baseline", 10.0)
        return 10.0  # A股平均增长约8-10%

    def _calculate_pe_valuation(
        self,
        current_price: float,
        multiples: Dict,
        financial_indicators: Dict,
        industry_info: Dict,
        industry_stats: Dict,
        market_cap: Dict,
        per_share_data: Dict,
        data: Dict,
    ) -> float:
        """
        PE估值方法

        合理PE = 行业PE × ROE调整（行业基准）× 收入增长率调整 × 市值调整
        公允价值 = 合理PE × EPS_TTM（从财报直接获取）
        """
        # 优先使用财报EPS，避免循环依赖
        eps_ttm = per_share_data.get("eps_ttm") if per_share_data else None

        if not eps_ttm:
            # 无财报EPS时回退：从PE反推（但有循环依赖风险，仅作降级处理）
            current_pe = multiples.get("pe_ttm")
            if not current_pe or current_pe <= 0:
                return current_price
            eps_ttm = current_price / current_pe

        if not eps_ttm or eps_ttm <= 0:
            return current_price

        # 获取行业PE中位数
        industry_pe = (
            industry_stats.get("pe_ttm_median") or 16
        )  # 默认16（A股整体约15-18）

        # 使用行业特定ROE基准（而非全局15%）
        roe_baseline = self._get_industry_roe_baseline(industry_info)
        roe = financial_indicators.get("roe") or 0
        roe_adjustment = self._adjust_pe_for_roe(roe, roe_baseline)

        # 使用实际收入增长率（非ROE×0.5），消除双重ROE调整
        revenue_growth = data.get("revenue_growth")  # 已是百分比（如15.2）
        if revenue_growth is None:
            # 若无收入增长率，用行业增长基准
            revenue_growth = self._get_industry_growth_baseline(industry_info)
        growth_baseline = self._get_industry_growth_baseline(industry_info)
        growth_adjustment = self._adjust_pe_for_growth(revenue_growth, growth_baseline)

        # 市值调整（total_mv 单位为万元，除以10000转换为亿元）
        total_mv = (market_cap.get("total_mv") or 0) / 10000  # 万元→亿元
        cap_adjustment = get_market_cap_premium(total_mv)

        # 计算合理PE
        reasonable_pe = (
            industry_pe * roe_adjustment * growth_adjustment * cap_adjustment
        )
        # 合理范围：5~80
        reasonable_pe = max(5.0, min(reasonable_pe, 80.0))

        fair_value = reasonable_pe * eps_ttm

        return fair_value

    def _calculate_pb_valuation(
        self,
        current_price: float,
        multiples: Dict,
        financial_indicators: Dict,
        industry_info: Dict,
        industry_stats: Dict,
        market_cap: Dict,
        per_share_data: Dict,
    ) -> float:
        """
        PB估值方法

        合理PB = 行业PB × ROE调整
        公允价值 = 合理PB × BVPS（从财报直接获取）
        """
        # 优先使用财报BVPS
        bvps = per_share_data.get("bvps") if per_share_data else None

        if not bvps:
            current_pb = multiples.get("pb")
            if not current_pb or current_pb <= 0:
                return current_price
            bvps = current_price / current_pb

        if not bvps or bvps <= 0:
            return current_price

        # 获取行业PB中位数
        industry_pb = industry_stats.get("pb_median") or 1.5  # 默认1.5

        # 使用行业特定ROE基准
        roe_baseline = self._get_industry_roe_baseline(industry_info)
        roe = financial_indicators.get("roe") or 0
        pb_adjustment = self._adjust_pb_for_roe(roe, roe_baseline)

        # 计算合理PB
        reasonable_pb = industry_pb * pb_adjustment
        reasonable_pb = max(0.3, min(reasonable_pb, 10.0))

        fair_value = reasonable_pb * bvps

        return fair_value

    def _calculate_ps_valuation(
        self,
        current_price: float,
        multiples: Dict,
        financial_indicators: Dict,
        industry_info: Dict,
        industry_stats: Dict,
        market_cap: Dict,
        per_share_data: Dict,
    ) -> float:
        """
        PS估值方法

        合理PS = 行业PS × 净利率调整
        公允价值 = 合理PS × SPS_TTM（从财报直接获取）
        """
        # 优先使用财报SPS
        sps_ttm = per_share_data.get("sps_ttm") if per_share_data else None

        if not sps_ttm:
            current_ps = multiples.get("ps_ttm")
            if not current_ps or current_ps <= 0:
                return current_price
            sps_ttm = current_price / current_ps

        if not sps_ttm or sps_ttm <= 0:
            return current_price

        # 获取行业PS中位数
        industry_ps = (
            industry_stats.get("ps_ttm_median") or 2.0
        )  # 默认2.0（修正：原3.0偏高）

        # 净利率调整
        net_margin = financial_indicators.get("net_margin") or 0
        margin_adjustment = self._adjust_ps_for_margin(net_margin)

        # 计算合理PS
        reasonable_ps = industry_ps * margin_adjustment
        reasonable_ps = max(0.3, min(reasonable_ps, 15.0))

        fair_value = reasonable_ps * sps_ttm

        return fair_value

    def _calculate_peg_valuation(
        self,
        current_price: float,
        multiples: Dict,
        financial_indicators: Dict,
        industry_info: Dict,
        industry_stats: Dict,
        market_cap: Dict,
        per_share_data: Dict,
        data: Dict,
    ) -> float:
        """
        PEG估值方法（标准Peter Lynch方法）

        合理PE = PEG × 净利润增长率(%)
        标准PEG=1.0，高确定性成长股PEG=1.0-1.5，低确定性PEG=0.7-1.0

        增长率<=0时不适用PEG
        """
        # 优先使用财报EPS
        eps_ttm = per_share_data.get("eps_ttm") if per_share_data else None
        if not eps_ttm:
            current_pe = multiples.get("pe_ttm")
            if not current_pe or current_pe <= 0:
                return current_price
            eps_ttm = current_price / current_pe

        if not eps_ttm or eps_ttm <= 0:
            return current_price

        # 使用实际净利润增长率（非ROE×0.5）
        net_income_growth = data.get("net_income_growth")  # 百分比

        if net_income_growth is None or net_income_growth <= 0:
            # PEG不适用于负增长公司
            return current_price  # 返回当前价格（中性，不影响组合估值）

        # 标准PEG方法：合理PE = 净利润增长率(%)
        # ROE稳定（>15%）且增长确定性高允许PEG=1.2，否则PEG=0.8-1.0
        roe = financial_indicators.get("roe") or 0

        # 净利润增长率历史波动性
        net_income_growth_history = data.get("net_income_growth_history", [])
        if len(net_income_growth_history) >= 3:
            growth_arr = [
                g * 100 for g in net_income_growth_history[:3] if g is not None
            ]
            if growth_arr:
                growth_cv = np.std(growth_arr) / (abs(np.mean(growth_arr)) + 1e-6)
                # 波动系数越低，PEG越高（确定性越强）
                if growth_cv < 0.2 and roe > 15:
                    peg = 1.2  # 高确定性成长股
                elif growth_cv < 0.4 and roe > 10:
                    peg = 1.0  # 中等确定性
                else:
                    peg = 0.8  # 低确定性
            else:
                peg = 1.0
        else:
            peg = 1.0  # 数据不足，用标准值

        # 合理PE = PEG × 增长率(%)
        reasonable_pe = peg * net_income_growth
        reasonable_pe = max(5.0, min(reasonable_pe, 80.0))

        fair_value = reasonable_pe * eps_ttm

        return fair_value

    def _calculate_combined_valuation(self, data: Dict[str, Any], **kwargs) -> float:
        """
        组合多种估值方法（简单组合，仅供combined方法使用）
        真正的行业权重组合在 valuation_engine.py 实现
        """
        results = {}

        per_share_data = data.get("per_share_data", {})
        multiples = data.get("valuation_multiples", {})
        financial_indicators = data.get("financial_indicators", {})
        industry_info = data.get("industry_info", {})
        industry_stats = data.get("industry_stats", {})
        market_cap = data.get("market_cap", {})

        pe_val = self._calculate_pe_valuation(
            data["price"],
            multiples,
            financial_indicators,
            industry_info,
            industry_stats,
            market_cap,
            per_share_data,
            data,
        )
        pb_val = self._calculate_pb_valuation(
            data["price"],
            multiples,
            financial_indicators,
            industry_info,
            industry_stats,
            market_cap,
            per_share_data,
        )
        ps_val = self._calculate_ps_valuation(
            data["price"],
            multiples,
            financial_indicators,
            industry_info,
            industry_stats,
            market_cap,
            per_share_data,
        )

        # 简单平均（combined方法的简化实现）
        valid_vals = [v for v in [pe_val, pb_val, ps_val] if v != data["price"]]
        if not valid_vals:
            return data["price"]
        return float(np.mean(valid_vals))

    # ==================== 调整系数 ====================

    def _adjust_pe_for_roe(self, roe: float, roe_baseline: float = 12.0) -> float:
        """
        根据ROE调整PE（使用行业特定基准）

        ROE基准校正为12%（A股平均），灵敏度3%/每1%ROE
        """
        adjustment = 1.0 + (roe - roe_baseline) * 0.03
        return max(0.5, min(adjustment, 2.0))

    def _adjust_pe_for_growth(
        self, growth_rate: float, growth_baseline: float = 10.0
    ) -> float:
        """
        根据收入增长率调整PE（使用行业特定基准）

        基准校正为10%（A股平均），灵敏度2%/每1%增长率
        """
        adjustment = 1.0 + (growth_rate - growth_baseline) * 0.02
        return max(0.5, min(adjustment, 2.0))

    def _adjust_pb_for_roe(self, roe: float, roe_baseline: float = 12.0) -> float:
        """
        根据ROE调整PB（使用行业特定基准）

        灵敏度修正为3%/每1%ROE（原5%过于敏感）
        ROE从10%→20%时PB调整约1.3倍（原方案翻倍，过于敏感）
        """
        adjustment = 1.0 + (roe - roe_baseline) * 0.03
        return max(0.5, min(adjustment, 2.0))

    def _adjust_ps_for_margin(self, net_margin: float) -> float:
        """
        根据净利率调整PS

        净利率越高，PS越高（基准10%）
        """
        baseline_margin = 10.0
        adjustment = 1.0 + (net_margin - baseline_margin) * 0.02
        return max(0.5, min(adjustment, 2.0))

    # ==================== 差异化置信度计算 ====================

    def _calculate_confidence(self, metrics: Dict, assumptions: Dict) -> float:
        """
        差异化置信度计算（根据估值方法不同）

        PE置信度：盈利稳定性、行业样本量、PE偏离度
        PB置信度：ROE稳定性、资产质量
        PS置信度：收入增长稳定性、净利率
        PEG置信度：增长率可预测性
        """
        method = self.method

        if method == "pe":
            return self._calc_pe_confidence(metrics)
        elif method == "pb":
            return self._calc_pb_confidence(metrics)
        elif method == "ps":
            return self._calc_ps_confidence(metrics)
        elif method == "peg":
            return self._calc_peg_confidence(metrics)
        elif method == "combined":
            # 组合：取PE/PB/PS平均
            c_pe = self._calc_pe_confidence(metrics)
            c_pb = self._calc_pb_confidence(metrics)
            c_ps = self._calc_ps_confidence(metrics)
            return (c_pe + c_pb + c_ps) / 3
        else:
            return 0.5

    def _calc_pe_confidence(self, metrics: Dict) -> float:
        """PE估值置信度"""
        confidence = 0.5

        # 行业样本量（行业统计越多越可靠）
        industry_count = metrics.get("industry_count", 0)
        if industry_count > 30:
            confidence += 0.10
        elif industry_count > 10:
            confidence += 0.06
        elif industry_count > 5:
            confidence += 0.03

        # 盈利稳定性（EPS历史波动越低越可靠）
        eps_cv = metrics.get("eps_cv")  # 变异系数
        if eps_cv is not None:
            if eps_cv < 0.2:
                confidence += 0.15
            elif eps_cv < 0.4:
                confidence += 0.08
            elif eps_cv > 0.8:
                confidence -= 0.10

        # ROE水平（高ROE企业PE估值更可靠）
        roe = metrics.get("roe") or 0
        if roe > 15:
            confidence += 0.08
        elif roe < 5:
            confidence -= 0.10

        # PE偏离度（PE越接近行业中位数，估值越可靠）
        current_pe = metrics.get("current_pe") or 0
        industry_pe = metrics.get("industry_pe") or 1
        if current_pe and industry_pe:
            pe_deviation = abs(current_pe - industry_pe) / industry_pe
            if pe_deviation < 0.3:
                confidence += 0.05
            elif pe_deviation > 1.0:
                confidence -= 0.08

        return max(0.3, min(confidence, 0.9))

    def _calc_pb_confidence(self, metrics: Dict) -> float:
        """PB估值置信度"""
        confidence = 0.5

        # ROE稳定性
        roe = metrics.get("roe") or 0
        if roe > 12:
            confidence += 0.10
        elif roe < 5:
            confidence -= 0.10

        # 资产质量（资产负债率）
        debt_to_assets = metrics.get("debt_to_assets") or 0
        if debt_to_assets < 0.4:
            confidence += 0.10
        elif debt_to_assets < 0.6:
            confidence += 0.05
        elif debt_to_assets > 0.8:
            confidence -= 0.10

        # 行业样本量
        industry_count = metrics.get("industry_count", 0)
        if industry_count > 20:
            confidence += 0.08
        elif industry_count > 10:
            confidence += 0.05

        return max(0.3, min(confidence, 0.9))

    def _calc_ps_confidence(self, metrics: Dict) -> float:
        """PS估值置信度"""
        confidence = 0.5

        # 净利率为负时PS更适用（亏损但有收入的公司）
        net_margin = metrics.get("net_margin") or 0
        if net_margin < 0:
            confidence += 0.10  # 亏损公司PE无效，PS是主要方法
        elif net_margin > 20:
            confidence += 0.05

        # 收入增长稳定性
        revenue_growth = metrics.get("revenue_growth")
        if revenue_growth is not None and revenue_growth > 0:
            confidence += 0.08
        elif revenue_growth is not None and revenue_growth < -10:
            confidence -= 0.10

        # 行业样本量
        industry_count = metrics.get("industry_count", 0)
        if industry_count > 20:
            confidence += 0.08
        elif industry_count > 10:
            confidence += 0.05

        return max(0.3, min(confidence, 0.9))

    def _calc_peg_confidence(self, metrics: Dict) -> float:
        """PEG估值置信度"""
        confidence = 0.5

        # 增长率可预测性（波动系数）
        peg_growth_cv = metrics.get("peg_growth_cv")
        if peg_growth_cv is not None:
            if peg_growth_cv < 0.2:
                confidence += 0.15
            elif peg_growth_cv < 0.4:
                confidence += 0.08
            elif peg_growth_cv > 0.8:
                confidence -= 0.15

        # 增长率水平
        net_income_growth = metrics.get("net_income_growth")
        if net_income_growth is None or net_income_growth <= 0:
            confidence = 0.3  # 负增长不适用PEG
        elif net_income_growth > 50:
            confidence -= 0.05  # 超高增长不可持续

        # ROE水平
        roe = metrics.get("roe") or 0
        if roe > 15:
            confidence += 0.05

        return max(0.3, min(confidence, 0.9))

    def _get_detailed_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        获取详细指标（包含置信度计算所需数据）
        """
        multiples = data.get("valuation_multiples", {})
        per_share = data.get("per_share_data", {})
        financial = data.get("financial_indicators", {})
        industry_info = data.get("industry_info", {})
        industry_stats = data.get("industry_stats", {})
        market_cap = data.get("market_cap", {})

        metrics = {
            "current_pe": multiples.get("pe_ttm"),
            "current_pb": multiples.get("pb"),
            "current_ps": multiples.get("ps_ttm"),
            "industry_pe": industry_stats.get("pe_ttm_median"),
            "industry_pb": industry_stats.get("pb_median"),
            "industry_ps": industry_stats.get("ps_ttm_median"),
            "eps_ttm": per_share.get("eps_ttm"),
            "bvps": per_share.get("bvps"),
            "sps_ttm": per_share.get("sps_ttm"),
            "roe": financial.get("roe"),
            "roa": financial.get("roa"),
            "net_margin": financial.get("net_margin")
            or financial.get("netprofit_margin"),
            "gross_margin": financial.get("gross_margin")
            or financial.get("grossprofit_margin"),
            "debt_to_assets": financial.get("debt_to_assets"),
            "sw_l1": industry_info.get("sw_l1"),
            "sw_l1_code": industry_info.get("sw_l1_code"),
            "sw_l2": industry_info.get("sw_l2"),
            "sw_l2_code": industry_info.get("sw_l2_code"),
            "total_mv": market_cap.get("total_mv"),
            "revenue_growth": data.get("revenue_growth"),
            "net_income_growth": data.get("net_income_growth"),
        }

        # 计算EPS历史波动系数（用于PE置信度）
        # 这里使用收入增长历史作为代理
        revenue_growth_history = data.get("revenue_growth_history", [])
        if len(revenue_growth_history) >= 3:
            growth_pct = [g * 100 for g in revenue_growth_history[:3] if g is not None]
            if growth_pct and abs(np.mean(growth_pct)) > 0:
                metrics["eps_cv"] = np.std(growth_pct) / (
                    abs(np.mean(growth_pct)) + 1e-6
                )

        # PEG增长率波动系数
        net_income_growth_history = data.get("net_income_growth_history", [])
        if len(net_income_growth_history) >= 3:
            growth_pct = [
                g * 100 for g in net_income_growth_history[:3] if g is not None
            ]
            if growth_pct and abs(np.mean(growth_pct)) > 0:
                metrics["peg_growth_cv"] = np.std(growth_pct) / (
                    abs(np.mean(growth_pct)) + 1e-6
                )

        return metrics

    def _get_assumptions(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        获取关键假设（含行业参数信息）
        """
        industry_info = data.get("industry_info", {})
        roe_baseline = self._get_industry_roe_baseline(industry_info)
        growth_baseline = self._get_industry_growth_baseline(industry_info)
        per_share = data.get("per_share_data", {})
        eps_source = (
            "财报EPS_TTM（直接获取，非价格反推）"
            if per_share.get("eps_ttm")
            else "价格反推EPS（无财报数据，存在循环依赖）"
        )

        industry_stats = data.get("industry_stats", {})

        assumptions = {
            "valuation_method": self.method,
            "industry": industry_info.get("sw_l1"),
            "industry_code": industry_info.get("sw_l1_code"),
        }

        if self.method == "pe":
            assumptions.update(
                {
                    "industry_pe_used": industry_stats.get("pe_ttm_median"),
                    "roe_baseline_used": f"{roe_baseline}%（行业基准）",
                    "eps_source": eps_source,
                }
            )
        elif self.method == "pb":
            assumptions.update(
                {
                    "industry_pb_used": industry_stats.get("pb_median"),
                    "roe_baseline_used": f"{roe_baseline}%（行业基准）",
                }
            )
        elif self.method == "ps":
            assumptions.update(
                {
                    "industry_ps_used": industry_stats.get("ps_ttm_median"),
                    "net_margin_baseline": f"{data.get('net_margin', 0) * 100:.1f}%",
                }
            )
        elif self.method == "peg":
            assumptions.update(
                {
                    "industry_pe_used": industry_stats.get("pe_ttm_median"),
                    "growth_baseline_used": f"{growth_baseline}%（行业基准）",
                    "net_income_growth": data.get("net_income_growth"),
                    "eps_source": eps_source,
                }
            )
        else:
            assumptions.update(
                {
                    "industry_pe_used": industry_stats.get("pe_ttm_median"),
                    "industry_pb_used": industry_stats.get("pb_median"),
                    "industry_ps_used": industry_stats.get("ps_ttm_median"),
                    "roe_baseline_used": f"{roe_baseline}%（行业基准）",
                    "growth_baseline_used": f"{growth_baseline}%（行业基准）",
                    "eps_source": eps_source,
                    "revenue_growth": data.get("revenue_growth"),
                    "net_income_growth": data.get("net_income_growth"),
                }
            )

        return assumptions

    def _get_warnings(self, data: Dict[str, Any]) -> List[str]:
        """
        获取警告信息
        """
        warnings = []

        if not data.get("industry_stats"):
            warnings.append("无行业统计数据，使用默认值")

        per_share = data.get("per_share_data", {})
        if not per_share.get("eps_ttm"):
            warnings.append("无财报EPS数据，使用价格反推（可能有循环依赖）")

        multiples = data.get("valuation_multiples", {})
        if not multiples.get("pe_ttm") or (multiples.get("pe_ttm") or 0) <= 0:
            warnings.append("PE为负或无效（可能为亏损公司）")

        if (multiples.get("pe_ttm") or 0) > 100:
            warnings.append("PE > 100，估值参考意义有限")

        net_income_growth = data.get("net_income_growth")
        if (
            net_income_growth is not None
            and net_income_growth <= 0
            and self.method == "peg"
        ):
            warnings.append("净利润增长率≤0，PEG方法不适用")

        # 增长率数据缺失报警
        if self.method in ("pe", "combined"):
            if data.get("revenue_growth") is None:
                warnings.append("收入增长率数据缺失，PE调整使用行业基准值（估值可靠性降低）")
        if self.method in ("peg", "combined"):
            if data.get("net_income_growth") is None:
                warnings.append("净利润增长率数据缺失，PEG方法不可用")

        return warnings
