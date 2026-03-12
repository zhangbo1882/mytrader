"""
估值数据加载器

整合 FinancialQuery、IndustryStatisticsCalculator 等数据源，
为估值模型提供统一的数据接口
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from src.data_sources.query.financial_query import FinancialQuery
from src.screening.calculators.industry_statistics_calculator import (
    IndustryStatisticsCalculator,
)


class ValuationDataLoader:
    """
    估值专用数据加载器

    整合多种数据源，为估值模型提供统一的数据接口
    """

    def __init__(self, db_path: str = "data/tushare_data.db"):
        """
        初始化数据加载器

        Args:
            db_path: 数据库路径
        """
        self.db_path = db_path
        self.financial_query = FinancialQuery(db_path)
        self.industry_calc = IndustryStatisticsCalculator(db_path)

        # 创建数据库引擎用于直接查询
        from sqlalchemy import create_engine

        self.engine = create_engine(f"sqlite:///{db_path}")

        # 财务数据报告期（如果指定，则使用该报告期的数据）
        self.fiscal_date = None

    def set_fiscal_date(self, fiscal_date: str, valuation_date: Optional[str] = None):
        """
        设置财务数据报告期

        Args:
            fiscal_date: 财务数据报告期 (YYYY-MM-DD 或 YYYYMMDD)
            valuation_date: 估值日期 (YYYY-MM-DD，用于时间验证)

        Raises:
            ValueError: 如果fiscal_date > valuation_date（时间穿越）
        """
        import warnings
        from datetime import datetime

        # 标准化日期格式
        def normalize_date(d):
            if not d:
                return None
            d = d.replace("-", "")
            if len(d) == 8:
                return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            return d

        fiscal_date_norm = normalize_date(fiscal_date)
        self.fiscal_date = fiscal_date_norm

        # 时间验证
        if valuation_date:
            valuation_date_norm = normalize_date(valuation_date)
            if fiscal_date_norm and valuation_date_norm:
                try:
                    fiscal_dt = datetime.strptime(fiscal_date_norm, "%Y-%m-%d")
                    valuation_dt = datetime.strptime(valuation_date_norm, "%Y-%m-%d")

                    if fiscal_dt > valuation_dt:
                        warnings.warn(
                            f"⚠️  时间穿越警告: 财务数据报告期 ({fiscal_date_norm}) "
                            f"晚于估值日期 ({valuation_date_norm})。"
                            f"这是不合理的，因为估值时无法获得未来的财报数据。",
                            UserWarning,
                        )
                except ValueError as e:
                    print(f"Date parsing error: {e}")

    def _use_fiscal_date(self) -> bool:
        """是否使用指定的财务数据报告期"""
        return self.fiscal_date is not None

    def load_relative_valuation_data(
        self, symbol: str, date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        加载相对估值所需数据（PE/PB/PS/PEG）

        Args:
            symbol: 股票代码
            date: 估值日期

        Returns:
            相对估值数据字典
        """
        code = self._standardize_code(symbol)

        # 获取最新价格和估值倍数
        price_data = self._get_latest_price_data(code, date)

        # 获取财务指标（使用日期参数）
        financial_data = self._get_financial_indicators(code, date)

        # 从财报直接计算每股数据（EPS_TTM/BVPS/SPS_TTM）
        per_share_data = self._get_per_share_data(code, date)

        # 获取行业信息
        industry_info = self._get_industry_info(code, date)

        # 获取行业统计
        industry_stats = self._get_industry_stats(code, date, industry_info)

        # 获取市值信息
        market_cap = self._get_market_cap(code, date)

        # 获取收入增长率历史（转换为百分比，如 15.2 表示 15.2%）
        revenue_growth_history_raw = self._get_revenue_growth_history(code, date=date)
        revenue_growth_history = [
            r * 100 for r in revenue_growth_history_raw if r is not None
        ]
        revenue_growth = revenue_growth_history[0] if revenue_growth_history else None

        # 获取净利润增长率历史
        net_income_growth_history_raw = self._get_net_income_growth_history(
            code, date=date
        )
        net_income_growth_history = [
            r * 100 for r in net_income_growth_history_raw if r is not None
        ]
        net_income_growth = (
            net_income_growth_history[0] if net_income_growth_history else None
        )

        return {
            "symbol": code,
            "date": date,
            "price": price_data.get("close"),
            "valuation_multiples": {
                "pe_ttm": price_data.get("pe_ttm"),
                "pb": price_data.get("pb"),
                "ps_ttm": price_data.get("ps_ttm"),
            },
            "financial_indicators": financial_data,
            "per_share_data": per_share_data,
            "industry_info": industry_info,
            "industry_stats": industry_stats,
            "market_cap": market_cap,
            "revenue_growth": revenue_growth,
            "revenue_growth_history": revenue_growth_history,
            "net_income_growth": net_income_growth,
            "net_income_growth_history": net_income_growth_history,
        }

    def load_dcf_valuation_data(
        self, symbol: str, date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        加载DCF估值所需数据

        Args:
            symbol: 股票代码
            date: 估值日期

        Returns:
            DCF估值数据字典
        """
        code = self._standardize_code(symbol)

        # 获取现金流数据
        cashflow_data = self._get_cashflow_history(code)

        # 获取资本结构数据（使用日期参数）
        capital_structure = self._get_capital_structure(code, date)

        # 获取现金及现金等价物（使用日期参数）
        cash_equivalents, cash_date_used = self._get_cash_equivalents(code, date)

        # 计算净债务 = 有息债务 - 现金及现金等价物
        interest_bearing_debt = capital_structure.get("interest_bearing_debt", 0) or 0
        cash_equivalents_for_calc = cash_equivalents or 0
        net_debt = max(0, interest_bearing_debt - cash_equivalents_for_calc)  # 净债务不能为负

        # 获取收入增长历史（受截止日期约束）
        revenue_growth = self._get_revenue_growth_history(code, date=date)

        # 计算自由现金流（DCF 使用最新报告期数据，受截止日期约束）
        fcf_data = self._calculate_free_cash_flow(code, annual_only=False, date=date)

        # 获取Beta系数（简化版，使用行业Beta）
        beta = self._calculate_beta(code)

        # 获取股本信息（使用日期参数）
        shares_info = self._get_shares_info(code, date)

        # 获取最新净利润用于FCF/净利润比率检查
        net_income = self._get_latest_net_income(code, date)

        return {
            "symbol": code,
            "date": date,
            "cashflow_history": cashflow_data,
            "capital_structure": capital_structure,
            "cash_equivalents": cash_equivalents,
            "cash_date_used": cash_date_used,  # 实际使用的现金数据日期
            "net_debt": net_debt,
            "revenue_growth": revenue_growth,
            "free_cash_flow": fcf_data,
            "net_income": net_income,  # 添加净利润数据
            "beta": beta,
            "shares_info": shares_info,
        }

    def _get_latest_price_data(
        self, code: str, date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取最新价格数据

        Args:
            code: 股票代码
            date: 日期

        Returns:
            价格数据字典
        """
        try:
            from src.db.duckdb_manager import get_duckdb_manager

            duckdb_manager = get_duckdb_manager()
            with duckdb_manager.get_connection() as conn:
                # 标准化股票代码
                stock_code = code.split(".")[0]

                # 判断是 A 股还是港股
                if (
                    code.endswith(".HK")
                    or code.replace(".", "").isdigit()
                    and len(code.replace(".", "")) == 5
                ):
                    # 港股：使用 bars_1d 表
                    exchange = "HK"
                    table_name = "bars_1d"
                    # 港股没有 PE/PB 数据，设为 None
                    pe_ttm_col = "NULL as pe_ttm"
                    pb_col = "NULL as pb"
                    ps_ttm_col = "NULL as ps_ttm"
                else:
                    # A股：使用 bars_a_1d 表
                    exchange = (
                        "SH"
                        if code.endswith(".SH")
                        else "SZ"
                        if code.endswith(".SZ")
                        else None
                    )
                    table_name = "bars_a_1d"
                    pe_ttm_col = "pe_ttm"
                    pb_col = "pb"
                    ps_ttm_col = "ps_ttm"

                # 构建查询条件
                if date:
                    where_clause = f"AND datetime <= '{date}'"
                else:
                    where_clause = ""

                if exchange:
                    exchange_clause = f"AND exchange = '{exchange}'"
                else:
                    exchange_clause = ""

                query = f"""
                SELECT datetime, close, {pe_ttm_col}, {pb_col}, {ps_ttm_col}, total_mv, circ_mv
                FROM {table_name}
                WHERE stock_code = '{stock_code}'
                  {exchange_clause}
                  {where_clause}
                ORDER BY datetime DESC
                LIMIT 1
                """
                df = conn.execute(query).fetchdf()

            if not df.empty:
                return df.iloc[0].to_dict()

        except Exception as e:
            print(f"Error getting price data for {code}: {e}")

        return {}

    def _get_financial_indicators(
        self, code: str, date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取指定日期之前的最新财务指标，或指定报告期的财务指标

        Args:
            code: 股票代码
            date: 估值日期（YYYY-MM-DD格式），如果为None则使用最新数据

        Returns:
            财务指标字典
        """
        try:
            # 如果指定了财务数据报告期，则使用该报告期的数据
            if self._use_fiscal_date():
                # fiscal_date需要转换为数据库格式（YYYYMMDD）
                fiscal_date_db = self.fiscal_date.replace("-", "")
                query = f"""
                SELECT
                    roe, roa, grossprofit_margin, netprofit_margin, current_ratio, quick_ratio,
                    debt_to_assets, debt_to_eqt, assets_turn, inv_turn
                FROM fina_indicator
                WHERE ts_code LIKE :code
                  AND end_date = '{fiscal_date_db}'
                """
                df = pd.read_sql_query(query, self.engine, params={"code": f"{code}%"})
                # 精确匹配无数据时，回退到截止该日期的最新数据
                if df.empty:
                    query = f"""
                    SELECT
                        roe, roa, grossprofit_margin, netprofit_margin, current_ratio, quick_ratio,
                        debt_to_assets, debt_to_eqt, assets_turn, inv_turn
                    FROM fina_indicator
                    WHERE ts_code LIKE :code
                      AND end_date <= '{fiscal_date_db}'
                    ORDER BY end_date DESC
                    LIMIT 1
                    """
                    df = pd.read_sql_query(
                        query, self.engine, params={"code": f"{code}%"}
                    )
            elif date:
                # date需要转换为数据库格式（YYYYMMDD）进行比较
                date_db = date.replace("-", "")
                query = f"""
                SELECT
                    roe, roa, grossprofit_margin, netprofit_margin, current_ratio, quick_ratio,
                    debt_to_assets, debt_to_eqt, assets_turn, inv_turn
                FROM fina_indicator
                WHERE ts_code LIKE :code
                  AND end_date <= '{date_db}'
                ORDER BY end_date DESC
                LIMIT 1
                """
                df = pd.read_sql_query(query, self.engine, params={"code": f"{code}%"})
            else:
                query = """
                SELECT
                    roe, roa, grossprofit_margin, netprofit_margin, current_ratio, quick_ratio,
                    debt_to_assets, debt_to_eqt, assets_turn, inv_turn
                FROM fina_indicator
                WHERE ts_code LIKE :code
                ORDER BY end_date DESC
                LIMIT 1
                """
                df = pd.read_sql_query(query, self.engine, params={"code": f"{code}%"})

            if not df.empty:
                # Map database column names to expected names
                result = df.iloc[0].to_dict()
                # Create aliases for compatibility
                result["gross_margin"] = result.get("grossprofit_margin")
                result["net_margin"] = result.get("netprofit_margin")
                result["assets_turnover"] = result.get("assets_turn")
                result["inventory_turnover"] = result.get("inv_turn")
                result["debt_to_equity"] = result.get("debt_to_eqt")
                return result

        except Exception as e:
            print(f"Error getting financial indicators for {code}: {e}")

        return {}

    def _get_industry_info(
        self, code: str, date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取申万行业信息

        Args:
            code: 股票代码
            date: 日期

        Returns:
            行业信息字典
        """
        try:
            # Extract the 6-digit stock code (remove suffix like .SH, .SZ)
            code_6digit = code[:6] if len(code) > 6 else code

            if date:
                query = """
                SELECT
                    COALESCE(sc_l1.industry_name, sc_l1_from_l2.industry_name, sc_l1_direct.industry_name) as sw_l1,
                    COALESCE(sc_l1.industry_code, sc_l1_from_l2.industry_code, sc_l1_direct.industry_code) as sw_l1_code,
                    COALESCE(sc_l2.industry_name, sc_l2_direct.industry_name) as sw_l2,
                    COALESCE(sc_l2.industry_code, sc_l2_direct.industry_code) as sw_l2_code,
                    sc_l3.industry_name as sw_l3,
                    sc_l3.industry_code as sw_l3_code
                FROM sw_members swm
                -- Try to get L3 classification first (use index_code for JOIN)
                LEFT JOIN sw_classify sc_l3 ON swm.index_code = sc_l3.index_code AND sc_l3.level = 'L3'
                -- If index_code is L3, get its L2 parent
                LEFT JOIN sw_classify sc_l2 ON sc_l3.parent_code = sc_l2.industry_code
                -- If index_code is L2, get L2 directly (use index_code for JOIN)
                LEFT JOIN sw_classify sc_l2_direct ON swm.index_code = sc_l2_direct.index_code AND sc_l2_direct.level = 'L2'
                -- If index_code is L3, get its L1 parent via L2
                LEFT JOIN sw_classify sc_l1 ON sc_l2.parent_code = sc_l1.industry_code
                -- If index_code is L2, get its L1 parent
                LEFT JOIN sw_classify sc_l1_from_l2 ON sc_l2_direct.parent_code = sc_l1_from_l2.industry_code
                -- If index_code is L1, get L1 directly (use index_code for JOIN)
                LEFT JOIN sw_classify sc_l1_direct ON swm.index_code = sc_l1_direct.index_code AND sc_l1_direct.level = 'L1'
                WHERE SUBSTR(swm.ts_code, 1, 6) = :code
                  AND swm.in_date <= :date
                  AND (swm.out_date IS NULL OR swm.out_date > :date)
                ORDER BY
                    CASE
                        WHEN sc_l3.industry_name IS NOT NULL THEN 1
                        WHEN sc_l2.industry_name IS NOT NULL THEN 2
                        WHEN sc_l2_direct.industry_name IS NOT NULL THEN 2
                        WHEN sc_l1_direct.industry_name IS NOT NULL THEN 3
                        ELSE 4
                    END
                LIMIT 1
                """
                params = {"code": code_6digit, "date": date}
            else:
                query = """
                SELECT
                    COALESCE(sc_l1.industry_name, sc_l1_from_l2.industry_name, sc_l1_direct.industry_name) as sw_l1,
                    COALESCE(sc_l1.industry_code, sc_l1_from_l2.industry_code, sc_l1_direct.industry_code) as sw_l1_code,
                    COALESCE(sc_l2.industry_name, sc_l2_direct.industry_name) as sw_l2,
                    COALESCE(sc_l2.industry_code, sc_l2_direct.industry_code) as sw_l2_code,
                    sc_l3.industry_name as sw_l3,
                    sc_l3.industry_code as sw_l3_code
                FROM sw_members swm
                -- Try to get L3 classification first (use index_code for JOIN)
                LEFT JOIN sw_classify sc_l3 ON swm.index_code = sc_l3.index_code AND sc_l3.level = 'L3'
                -- If index_code is L3, get its L2 parent
                LEFT JOIN sw_classify sc_l2 ON sc_l3.parent_code = sc_l2.industry_code
                -- If index_code is L2, get L2 directly (use index_code for JOIN)
                LEFT JOIN sw_classify sc_l2_direct ON swm.index_code = sc_l2_direct.index_code AND sc_l2_direct.level = 'L2'
                -- If index_code is L3, get its L1 parent via L2
                LEFT JOIN sw_classify sc_l1 ON sc_l2.parent_code = sc_l1.industry_code
                -- If index_code is L2, get its L1 parent
                LEFT JOIN sw_classify sc_l1_from_l2 ON sc_l2_direct.parent_code = sc_l1_from_l2.industry_code
                -- If index_code is L1, get L1 directly (use index_code for JOIN)
                LEFT JOIN sw_classify sc_l1_direct ON swm.index_code = sc_l1_direct.index_code AND sc_l1_direct.level = 'L1'
                WHERE SUBSTR(swm.ts_code, 1, 6) = :code
                ORDER BY
                    CASE
                        WHEN sc_l3.industry_name IS NOT NULL THEN 1
                        WHEN sc_l2.industry_name IS NOT NULL THEN 2
                        WHEN sc_l2_direct.industry_name IS NOT NULL THEN 2
                        WHEN sc_l1_direct.industry_name IS NOT NULL THEN 3
                        ELSE 4
                    END
                LIMIT 1
                """
                params = {"code": code_6digit}

            df = pd.read_sql_query(query, self.engine, params=params)

            if not df.empty:
                return df.iloc[0].to_dict()

        except Exception as e:
            print(f"Error getting industry info for {code}: {e}")

        # Fallback: try to get industry from stock_basic_info table
        return self._get_industry_fallback(code)

    def _get_industry_fallback(self, code: str) -> Dict[str, Any]:
        """
        从stock_basic_info表获取行业信息作为回退

        Args:
            code: 股票代码

        Returns:
            行业信息字典
        """
        try:
            # Try with .SH suffix first
            for ts_code in [f"{code}.SH", f"{code}.SZ"]:
                query = """
                SELECT industry
                FROM stock_basic_info
                WHERE ts_code = :ts_code
                LIMIT 1
                """
                df = pd.read_sql_query(query, self.engine, params={"ts_code": ts_code})

                if not df.empty and df.iloc[0]["industry"]:
                    industry_name = df.iloc[0]["industry"]
                    return {
                        "sw_l1": industry_name,
                        "sw_l1_code": "000000",
                        "sw_l2": industry_name,
                        "sw_l2_code": "000000",
                        "sw_l3": industry_name,
                        "sw_l3_code": "000000",
                    }
        except Exception as e:
            print(f"Error in fallback for {code}: {e}")

        # Return empty if all methods fail
        return {}

    def _get_industry_stats(
        self,
        code: str,
        date: Optional[str] = None,
        industry_info: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        获取行业统计数据

        Args:
            code: 股票代码
            date: 日期
            industry_info: 行业信息（如果已获取）

        Returns:
            行业统计字典
        """
        if industry_info is None:
            industry_info = self._get_industry_info(code, date)

        if not industry_info or pd.isna(industry_info.get("sw_l1")):
            return {}

        sw_l1 = industry_info["sw_l1"]
        sw_l2 = industry_info.get("sw_l2")
        sw_l3 = industry_info.get("sw_l3")

        stats = {}
        required_metrics = ["pe_ttm", "pb", "ps_ttm"]

        # 尝试获取L3统计，如果没有则获取L2，再没有则获取L1
        # 只在所有三个指标都获取到后才停止，否则继续尝试更高级别
        for level, industry in [("L3", sw_l3), ("L2", sw_l2), ("L1", sw_l1)]:
            if industry and pd.notna(industry):
                try:
                    for metric in required_metrics:
                        # 如果该指标还未获取到，则尝试获取
                        metric_key_median = f"{metric}_median"
                        metric_key_p75 = f"{metric}_p75"

                        if metric_key_median not in stats:
                            # 获取中位数和75分位数
                            p50 = self.industry_calc.get_industry_percentile(
                                sw_l1,
                                metric,
                                0.50,
                                sw_l2 if level in ["L2", "L3"] else None,
                                sw_l3 if level == "L3" else None,
                            )
                            p75 = self.industry_calc.get_industry_percentile(
                                sw_l1,
                                metric,
                                0.75,
                                sw_l2 if level in ["L2", "L3"] else None,
                                sw_l3 if level == "L3" else None,
                            )

                            if p50 is not None:
                                stats[metric_key_median] = p50
                            if p75 is not None:
                                stats[metric_key_p75] = p75

                    # 检查是否所有指标都已获取到
                    all_metrics_found = all(
                        f"{metric}_median" in stats for metric in required_metrics
                    )
                    if all_metrics_found:
                        break
                except Exception as e:
                    print(f"Error getting industry stats for {industry}: {e}")
                    continue

        return stats

    def _get_market_cap(self, code: str, date: Optional[str] = None) -> Dict[str, Any]:
        """
        获取市值信息

        Args:
            code: 股票代码
            date: 日期

        Returns:
            市值信息字典
        """
        try:
            from src.db.duckdb_manager import get_duckdb_manager

            duckdb_manager = get_duckdb_manager()
            with duckdb_manager.get_connection() as conn:
                # 标准化股票代码
                stock_code = code.split(".")[0]

                # 判断是 A 股还是港股
                if (
                    code.endswith(".HK")
                    or code.replace(".", "").isdigit()
                    and len(code.replace(".", "")) == 5
                ):
                    # 港股：使用 bars_1d 表
                    exchange = "HK"
                    table_name = "bars_1d"
                else:
                    # A股：使用 bars_a_1d 表
                    exchange = (
                        "SH"
                        if code.endswith(".SH")
                        else "SZ"
                        if code.endswith(".SZ")
                        else None
                    )
                    table_name = "bars_a_1d"

                # 构建查询条件
                if date:
                    where_clause = f"AND datetime <= '{date}'"
                else:
                    where_clause = ""

                if exchange:
                    exchange_clause = f"AND exchange = '{exchange}'"
                else:
                    exchange_clause = ""

                query = f"""
                SELECT total_mv, circ_mv
                FROM {table_name}
                WHERE stock_code = '{stock_code}'
                  {exchange_clause}
                  {where_clause}
                ORDER BY datetime DESC
                LIMIT 1
                """
                df = conn.execute(query).fetchdf()

            if not df.empty:
                return df.iloc[0].to_dict()

        except Exception as e:
            print(f"Error getting market cap for {code}: {e}")

        return {}

    def _get_cashflow_history(self, code: str, years: int = 5) -> pd.DataFrame:
        """
        获取现金流历史数据

        Args:
            code: 股票代码
            years: 获取年数

        Returns:
            现金流数据
        """
        return self.financial_query.query_cashflow(code)

    def _get_capital_structure(
        self, code: str, date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取指定日期之前的最新资本结构数据，或指定报告期的资本结构数据

        Args:
            code: 股票代码
            date: 估值日期（YYYY-MM-DD格式）

        Returns:
            资本结构字典
        """
        try:
            # 如果指定了财务数据报告期，则使用该报告期的数据
            if self._use_fiscal_date():
                # fiscal_date需要转换为数据库格式（YYYYMMDD）
                fiscal_date_db = self.fiscal_date.replace("-", "")
                query = f"""
                SELECT
                    end_date, total_assets, total_liab, total_hldr_eqy_exc_min_int,
                    st_borr, lt_borr, bond_payable
                FROM balancesheet
                WHERE ts_code LIKE :code
                  AND report_type = '1'
                  AND end_date = '{fiscal_date_db}'
                """
                df = pd.read_sql_query(query, self.engine, params={"code": f"{code}%"})
                # 精确匹配无数据时，回退到截止该日期的最新年报
                if df.empty:
                    query = f"""
                    SELECT
                        end_date, total_assets, total_liab, total_hldr_eqy_exc_min_int,
                        st_borr, lt_borr, bond_payable
                    FROM balancesheet
                    WHERE ts_code LIKE :code
                      AND report_type = '1'
                      AND end_date <= '{fiscal_date_db}'
                      AND end_date LIKE '%1231'
                    ORDER BY end_date DESC
                    LIMIT 1
                    """
                    df = pd.read_sql_query(
                        query, self.engine, params={"code": f"{code}%"}
                    )
            elif date:
                # date需要转换为数据库格式（YYYYMMDD）进行比较
                date_db = date.replace("-", "")
                query = f"""
                SELECT
                    end_date, total_assets, total_liab, total_hldr_eqy_exc_min_int,
                    st_borr, lt_borr, bond_payable
                FROM balancesheet
                WHERE ts_code LIKE :code
                  AND report_type = '1'
                  AND end_date <= '{date_db}'
                ORDER BY end_date DESC
                LIMIT 1
                """
                df = pd.read_sql_query(query, self.engine, params={"code": f"{code}%"})
            else:
                query = """
                SELECT
                    end_date, total_assets, total_liab, total_hldr_eqy_exc_min_int,
                    st_borr, lt_borr, bond_payable
                FROM balancesheet
                WHERE ts_code LIKE :code
                  AND report_type = '1'
                ORDER BY end_date DESC
                LIMIT 1
                """
                df = pd.read_sql_query(query, self.engine, params={"code": f"{code}%"})

            if not df.empty:
                # Map to expected names
                result = df.iloc[0].to_dict()
                result["total_liability"] = result.get("total_liab")
                result["total_owner_equities"] = result.get(
                    "total_hldr_eqy_exc_min_int"
                )

                # 计算有息债务（短期借款 + 长期借款 + 应付债券）
                st_borr = result.get("st_borr", 0) or 0
                lt_borr = result.get("lt_borr", 0) or 0
                bond_payable = result.get("bond_payable", 0) or 0
                result["interest_bearing_debt"] = st_borr + lt_borr + bond_payable

                return result

        except Exception as e:
            print(f"Error getting capital structure for {code}: {e}")

        return {}

    def _get_cash_equivalents(
        self, code: str, date: Optional[str] = None
    ) -> tuple[Optional[float], Optional[str]]:
        """
        获取现金及现金等价物

        从现金流量表中获取现金及现金等价物期末余额。
        当指定 fiscal_date 时，必须优先使用该报告期自身的数据，不再回退到年报。

        Args:
            code: 股票代码
            date: 估值日期（YYYY-MM-DD格式）

        Returns:
            (现金及现金等价物（元）, 实际使用的数据日期)
            返回(None, None)表示未找到或数据缺失
        """
        try:
            cash_date_used = None  # 记录实际使用的现金数据日期

            # 构建查询条件
            if self._use_fiscal_date():
                fiscal_date_db = self.fiscal_date.replace("-", "")
                query = f"""
                SELECT end_date, end_bal_cash
                FROM cashflow
                WHERE ts_code LIKE :code
                  AND end_date = '{fiscal_date_db}'
                LIMIT 1
                """
                df = pd.read_sql_query(query, self.engine, params={"code": f"{code}%"})

                if not df.empty and not df.iloc[0].isna()["end_bal_cash"]:
                    cash_date_used = df.iloc[0]["end_date"]
                    return float(df.iloc[0]["end_bal_cash"]), cash_date_used

                return None, None

            elif date:
                # date需要转换为数据库格式（YYYYMMDD）进行比较
                date_db = date.replace("-", "")

                # 优先使用指定报告期的数据
                query = f"""
                SELECT end_date, end_bal_cash
                FROM cashflow
                WHERE ts_code LIKE :code
                  AND end_date <= '{date_db}'
                ORDER BY end_date DESC
                LIMIT 1
                """
                df = pd.read_sql_query(query, self.engine, params={"code": f"{code}%"})

                if not df.empty and not df.iloc[0].isna()["end_bal_cash"]:
                    cash_date_used = df.iloc[0]["end_date"]
                    return float(df.iloc[0]["end_bal_cash"]), cash_date_used

                # 如果数据不可用，返回None（不再把缺失数据当作0）
                return None, None

            else:
                # 获取最新的报告数据
                query = """
                SELECT end_date, end_bal_cash
                FROM cashflow
                WHERE ts_code LIKE :code
                ORDER BY end_date DESC
                LIMIT 1
                """
                df = pd.read_sql_query(query, self.engine, params={"code": f"{code}%"})

                if not df.empty and not df.iloc[0].isna()["end_bal_cash"]:
                    cash_date_used = df.iloc[0]["end_date"]
                    return float(df.iloc[0]["end_bal_cash"]), cash_date_used

            return None, None

        except Exception as e:
            print(f"Error getting cash equivalents for {code}: {e}")
            return None, None

    def _get_revenue_growth_history(
        self, code: str, years: int = 5, date: Optional[str] = None
    ) -> List[float]:
        """
        获取收入增长率历史（使用年度报告的同比增长率）

        Args:
            code: 股票代码
            years: 获取年数
            date: 截止日期（YYYY-MM-DD），不传则取最新

        Returns:
            年度同比增长率列表
        """
        try:
            # 确定截止日期：fiscal_date 优先，其次 date 参数
            if self._use_fiscal_date():
                cutoff = self.fiscal_date.replace("-", "")
            elif date:
                cutoff = date.replace("-", "")
            else:
                cutoff = None

            cutoff_clause = f"AND end_date <= '{cutoff}'" if cutoff else ""

            # 只查询年度报告（end_date 以 1231 结尾）
            query = f"""
            SELECT revenue, end_date
            FROM income
            WHERE ts_code LIKE :code
              AND report_type = '1'
              AND end_date LIKE '%1231'
              {cutoff_clause}
            ORDER BY end_date DESC
            LIMIT :years
            """
            df = pd.read_sql_query(
                query, self.engine, params={"code": f"{code}%", "years": years}
            )

            if len(df) >= 2:
                revenues = df["revenue"].tolist()
                growth_rates = []
                # 计算年度同比增长率（同比去年）
                for i in range(len(revenues) - 1):
                    if revenues[i] and revenues[i + 1] and revenues[i + 1] != 0:
                        # 当前年度 vs 去年同期
                        growth_rate = (revenues[i] - revenues[i + 1]) / revenues[i + 1]
                        growth_rates.append(growth_rate)
                return growth_rates

        except Exception as e:
            print(f"Error getting revenue growth for {code}: {e}")

        return []

    def _get_net_income_growth_history(
        self, code: str, years: int = 5, date: Optional[str] = None
    ) -> List[float]:
        """
        获取净利润增长率历史（年度同比增长率）

        Returns:
            年度同比增长率列表（分数形式，如 0.15 表示 15%）
        """
        try:
            if self._use_fiscal_date():
                cutoff = self.fiscal_date.replace("-", "")
            elif date:
                cutoff = date.replace("-", "")
            else:
                cutoff = None

            cutoff_clause = f"AND end_date <= '{cutoff}'" if cutoff else ""

            query = f"""
            SELECT n_income_attr_p, end_date
            FROM income
            WHERE ts_code LIKE :code
              AND report_type = '1'
              AND end_date LIKE '%1231'
              {cutoff_clause}
            ORDER BY end_date DESC
            LIMIT :years
            """
            df = pd.read_sql_query(
                query, self.engine, params={"code": f"{code}%", "years": years}
            )

            if len(df) >= 2:
                net_incomes = df["n_income_attr_p"].tolist()
                growth_rates = []
                for i in range(len(net_incomes) - 1):
                    curr = net_incomes[i]
                    prev = net_incomes[i + 1]
                    if curr is not None and prev and prev != 0 and prev > 0:
                        growth_rates.append((curr - prev) / prev)
                return growth_rates

        except Exception as e:
            print(f"Error getting net income growth for {code}: {e}")

        return []

    def _calculate_free_cash_flow(
        self, code: str, annual_only: bool = False, date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        计算自由现金流 (FCFF)

        FCFF = 经营活动净现金流 + 利息支出 × (1 - 有效税率) - 资本支出

        其中：
          利息支出 = fin_exp_int_exp（优先），否则 MAX(fin_exp, 0)
          有效税率 = CLAMP(income_tax / total_profit, 15%, 35%)；total_profit <= 0 时取 25%
          资本支出 = c_pay_acq_const_fiolta

        Args:
            code: 股票代码
            annual_only: 是否只返回年度报告（用于DCF估值）
            date: 截止日期（YYYY-MM-DD），不传则取最新

        Returns:
            自由现金流数据（只包含有效的非NaN记录）
        """
        # 确定截止日期：fiscal_date 优先，其次 date 参数
        if self._use_fiscal_date():
            cutoff = self.fiscal_date.replace("-", "")
        elif date:
            cutoff = date.replace("-", "")
        else:
            cutoff = None

        cutoff_clause = f"AND cf.end_date <= '{cutoff}'" if cutoff else ""
        try:
            if annual_only:
                # DCF 估值使用年度报告数据（report_type=1 年报，end_date 以1231结尾）
                query = f"""
                SELECT
                    cf.end_date,
                    cf.n_cashflow_act                          AS operating_cashflow,
                    COALESCE(cf.c_pay_acq_const_fiolta, 0)    AS capex,
                    COALESCE(
                        inc.fin_exp_int_exp,
                        CASE WHEN inc.fin_exp > 0 THEN inc.fin_exp ELSE 0 END,
                        0
                    )                                          AS interest_expense,
                    CASE
                        WHEN inc.total_profit > 0
                        THEN CASE
                            WHEN inc.income_tax / inc.total_profit < 0.15 THEN 0.15
                            WHEN inc.income_tax / inc.total_profit > 0.35 THEN 0.35
                            ELSE inc.income_tax / inc.total_profit
                        END
                        ELSE 0.25
                    END                                        AS tax_rate,
                    CASE
                        WHEN inc.fin_exp_int_exp IS NULL AND inc.fin_exp IS NULL THEN 1
                        ELSE 0
                    END                                        AS interest_data_missing
                FROM cashflow cf
                LEFT JOIN income inc
                       ON inc.ts_code = cf.ts_code
                      AND inc.end_date = cf.end_date
                      AND inc.report_type = 1
                WHERE cf.ts_code LIKE :code
                  AND cf.end_date LIKE '%1231'
                  AND cf.report_type = 1
                  AND cf.n_cashflow_act IS NOT NULL
                  {cutoff_clause}
                ORDER BY cf.end_date DESC
                LIMIT 5
                """
            else:
                # 其他用途：使用所有报告期，不限年报
                query = f"""
                SELECT
                    cf.end_date,
                    cf.n_cashflow_act                          AS operating_cashflow,
                    COALESCE(cf.c_pay_acq_const_fiolta, 0)    AS capex,
                    COALESCE(
                        inc.fin_exp_int_exp,
                        CASE WHEN inc.fin_exp > 0 THEN inc.fin_exp ELSE 0 END,
                        0
                    )                                          AS interest_expense,
                    CASE
                        WHEN inc.total_profit > 0
                        THEN CASE
                            WHEN inc.income_tax / inc.total_profit < 0.15 THEN 0.15
                            WHEN inc.income_tax / inc.total_profit > 0.35 THEN 0.35
                            ELSE inc.income_tax / inc.total_profit
                        END
                        ELSE 0.25
                    END                                        AS tax_rate,
                    CASE
                        WHEN inc.fin_exp_int_exp IS NULL AND inc.fin_exp IS NULL THEN 1
                        ELSE 0
                    END                                        AS interest_data_missing
                FROM cashflow cf
                LEFT JOIN income inc
                       ON inc.ts_code = cf.ts_code
                      AND inc.end_date = cf.end_date
                      AND inc.report_type = 1
                WHERE cf.ts_code LIKE :code
                  AND cf.n_cashflow_act IS NOT NULL
                  {cutoff_clause}
                ORDER BY cf.end_date DESC
                LIMIT 10
                """
            df = pd.read_sql_query(query, self.engine, params={"code": f"{code}%"})

            if not df.empty:
                # 在 Python 侧手算 FCFF，避免依赖 Tushare 预算的 free_cashflow 字段
                records = df.to_dict("records")
                for record in records:
                    op_cf = record.get("operating_cashflow") or 0
                    interest = record.get("interest_expense") or 0
                    tax = record.get("tax_rate") or 0.25
                    capex = record.get("capex") or 0
                    record["free_cashflow"] = op_cf + interest * (1 - tax) - capex
                return records
            else:
                return []

        except Exception as e:
            print(f"Error calculating FCF for {code}: {e}")

        return {}

    def _calculate_beta(self, code: str) -> float:
        """
        计算Beta系数（简化版，使用行业平均）

        Args:
            code: 股票代码

        Returns:
            Beta系数
        """
        # 简化实现：使用行业平均Beta
        # 实际应用中可以通过回归分析计算
        industry_info = self._get_industry_info(code)

        # 根据行业返回典型Beta值
        industry_beta_map = {
            "银行": 0.8,
            "医药生物": 1.1,
            "电子": 1.3,
            "房地产": 1.2,
            "食品饮料": 0.9,
            "汽车": 1.2,
            "化工": 1.1,
        }

        if industry_info:
            sw_l1 = industry_info.get("sw_l1", "")
            # 确保sw_l1不是None
            if sw_l1 and isinstance(sw_l1, str):
                for industry, beta in industry_beta_map.items():
                    if industry in sw_l1:
                        return beta

        # 默认Beta
        return 1.0

    def _get_shares_info(self, code: str, date: Optional[str] = None) -> Dict[str, Any]:
        """
        获取指定日期之前的最新股本信息

        Args:
            code: 股票代码
            date: 估值日期（YYYY-MM-DD格式）

        Returns:
            股本信息字典
        """
        try:
            from src.db.duckdb_manager import get_duckdb_manager

            duckdb_manager = get_duckdb_manager()
            with duckdb_manager.get_connection() as conn:
                # 标准化股票代码
                stock_code = code.split(".")[0]

                # 判断是 A 股还是港股
                if (
                    code.endswith(".HK")
                    or code.replace(".", "").isdigit()
                    and len(code.replace(".", "")) == 5
                ):
                    # 港股：使用 bars_1d 表
                    exchange = "HK"
                    table_name = "bars_1d"
                else:
                    # A股：使用 bars_a_1d 表
                    exchange = (
                        "SH"
                        if code.endswith(".SH")
                        else "SZ"
                        if code.endswith(".SZ")
                        else None
                    )
                    table_name = "bars_a_1d"

                # 构建查询条件
                if date:
                    where_clause = f"AND datetime <= '{date}'"
                else:
                    where_clause = ""

                if exchange:
                    exchange_clause = f"AND exchange = '{exchange}'"
                else:
                    exchange_clause = ""

                query = f"""
                SELECT total_share, float_share, free_share
                FROM {table_name}
                WHERE stock_code = '{stock_code}'
                  {exchange_clause}
                  {where_clause}
                ORDER BY datetime DESC
                LIMIT 1
                """
                df = conn.execute(query).fetchdf()

            if not df.empty:
                return df.iloc[0].to_dict()

        except Exception as e:
            print(f"Error getting shares info for {code}: {e}")

        return {}

    def _standardize_code(self, symbol: str) -> str:
        """
        标准化股票代码

        Args:
            symbol: 股票代码

        Returns:
            6位标准代码
        """
        if "." in symbol:
            return symbol.split(".")[0]
        return symbol

    def _get_latest_net_income(self, code: str, date: Optional[str] = None) -> float:
        """
        获取最新年度净利润

        Args:
            code: 股票代码
            date: 估值日期（YYYY-MM-DD格式）

        Returns:
            净利润（元），返回0表示未找到
        """
        try:
            # 构建查询条件
            if self._use_fiscal_date():
                # 使用fiscal_date获取最新的年度净利润
                fiscal_date_db = self.fiscal_date.replace("-", "")
                query = f"""
                SELECT n_income
                FROM income
                WHERE ts_code LIKE :code
                  AND end_date <= '{fiscal_date_db}'
                  AND end_date LIKE '%1231'
                ORDER BY end_date DESC
                LIMIT 1
                """
                df = pd.read_sql_query(query, self.engine, params={"code": f"{code}%"})

                if not df.empty and pd.notna(df.iloc[0]["n_income"]):
                    return float(df.iloc[0]["n_income"])

            elif date:
                # 使用date获取最新的年度净利润
                date_db = date.replace("-", "")
                query = f"""
                SELECT n_income
                FROM income
                WHERE ts_code LIKE :code
                  AND end_date <= '{date_db}'
                  AND end_date LIKE '%1231'
                ORDER BY end_date DESC
                LIMIT 1
                """
                df = pd.read_sql_query(query, self.engine, params={"code": f"{code}%"})

                if not df.empty and pd.notna(df.iloc[0]["n_income"]):
                    return float(df.iloc[0]["n_income"])

            else:
                # 获取最新的年度净利润
                query = """
                SELECT n_income
                FROM income
                WHERE ts_code LIKE :code
                  AND end_date LIKE '%1231'
                ORDER BY end_date DESC
                LIMIT 1
                """
                df = pd.read_sql_query(query, self.engine, params={"code": f"{code}%"})

                if not df.empty and pd.notna(df.iloc[0]["n_income"]):
                    return float(df.iloc[0]["n_income"])

            return 0.0

        except Exception as e:
            print(f"Error getting net income for {code}: {e}")
            return 0.0

    def _get_per_share_data(
        self, code: str, date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        从财报直接计算每股数据（EPS_TTM / BVPS / SPS_TTM），避免从价格反推。

        TTM 计算逻辑：
        - 找最新一期季报（Q1/Q2/Q3/Q4）
        - 若最新是Q4（年报），直接用年报数据
        - 若最新是Qn（n<4），则 TTM = Qn（本年累计）+ 上年年报 - 上年同期Qn

        Returns:
            {'eps_ttm': float|None, 'bvps': float|None, 'sps_ttm': float|None}
        """
        result = {"eps_ttm": None, "bvps": None, "sps_ttm": None}
        try:
            date_filter = ""
            if date:
                date_db = date.replace("-", "")
                date_filter = f"AND end_date <= '{date_db}'"

            # 取最近5条合并季报（report_type=1）
            query = f"""
                SELECT end_date, n_income_attr_p, revenue, report_type
                FROM income
                WHERE ts_code LIKE :code
                  AND report_type = 1
                  {date_filter}
                ORDER BY end_date DESC
                LIMIT 5
            """
            df_income = pd.read_sql_query(
                query, self.engine, params={"code": f"{code}%"}
            )

            # 取最新一期资产负债表（总股本、净资产）
            bs_query = f"""
                SELECT end_date, total_share, total_hldr_eqy_exc_min_int
                FROM balancesheet
                WHERE ts_code LIKE :code
                  AND report_type = 1
                  {date_filter}
                ORDER BY end_date DESC
                LIMIT 1
            """
            df_bs = pd.read_sql_query(
                bs_query, self.engine, params={"code": f"{code}%"}
            )

            if df_income.empty or df_bs.empty:
                return result

            total_share = df_bs.iloc[0]["total_share"]
            if not total_share or total_share <= 0:
                return result

            # 最新期财务数据
            latest = df_income.iloc[0]
            latest_end = str(latest["end_date"])
            month = latest_end[4:6]  # 03/06/09/12

            if month == "12":
                # 年报：直接用
                net_income_ttm = latest["n_income_attr_p"]
                revenue_ttm = latest["revenue"]
            else:
                # 季报：TTM = 本年累计 + 上年年报 - 上年同期
                # 找上年年报
                prev_year = str(int(latest_end[:4]) - 1) + "1231"
                prev_same = str(int(latest_end[:4]) - 1) + latest_end[4:]

                df_prev_annual = df_income[
                    df_income["end_date"].astype(str) == prev_year
                ]
                df_prev_same = df_income[df_income["end_date"].astype(str) == prev_same]

                if df_prev_annual.empty:
                    # 无上年年报，直接用最新累计（低精度）
                    net_income_ttm = latest["n_income_attr_p"]
                    revenue_ttm = latest["revenue"]
                else:
                    pa_net = df_prev_annual.iloc[0]["n_income_attr_p"] or 0
                    pa_rev = df_prev_annual.iloc[0]["revenue"] or 0
                    ps_net = (
                        df_prev_same.iloc[0]["n_income_attr_p"]
                        if not df_prev_same.empty
                        else 0
                    )
                    ps_rev = (
                        df_prev_same.iloc[0]["revenue"] if not df_prev_same.empty else 0
                    )
                    net_income_ttm = (
                        (latest["n_income_attr_p"] or 0) + pa_net - (ps_net or 0)
                    )
                    revenue_ttm = (latest["revenue"] or 0) + pa_rev - (ps_rev or 0)

            # 计算每股数据
            if net_income_ttm is not None:
                result["eps_ttm"] = round(net_income_ttm / total_share, 4)
            if revenue_ttm is not None:
                result["sps_ttm"] = round(revenue_ttm / total_share, 4)

            # BVPS 用最新净资产
            equity = df_bs.iloc[0]["total_hldr_eqy_exc_min_int"]
            if equity and equity > 0:
                result["bvps"] = round(equity / total_share, 4)

        except Exception as e:
            print(f"Error calculating per_share_data for {code}: {e}")

        return result
