"""
可用报告期查询服务

提供友好的API来查询股票的可用财务报告期
"""

from typing import Dict, List, Optional, Any
from src.valuation.data.financial_data_consistency_checker import (
    FinancialDataConsistencyChecker,
)


class AvailablePeriodsProvider:
    """
    可用报告期查询服务

    封装FinancialDataConsistencyChecker，提供更友好的API
    """

    def __init__(self, db_path: str = "data/tushare_data.db"):
        """
        初始化服务

        Args:
            db_path: 数据库路径
        """
        self.checker = FinancialDataConsistencyChecker(db_path)

    def get_available_periods(
        self,
        symbol: str,
        required_fields: Optional[Dict[str, List[str]]] = None,
        date: Optional[str] = None,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """
        获取可用的报告期列表

        Args:
            symbol: 股票代码
            required_fields: 必需字段（可选）
                例如：
                {
                    'balancesheet': ['total_assets', 'total_liab'],
                    'income': ['revenue', 'n_income'],
                    'cashflow': ['n_cashflow_act', 'free_cashflow']
                }
            date: 截止日期（YYYY-MM-DD格式）
            limit: 返回数量限制

        Returns:
            {
                'symbol': '601958',
                'available_periods': [
                    {
                        'end_date': '20241231',
                        'report_type': 'annual',
                        'is_available': True,
                        'data_quality': 'A',
                        'completeness': 1.0,
                        'missing_fields': []
                    }
                ],
                'recommended_period': '20241231',
                'total_periods': 20,
                'available_count': 5
            }
        """
        # 获取所有报告期及质量检查结果
        periods = self.checker.get_available_periods(
            symbol, required_fields=required_fields, date=date, limit=limit
        )

        # 筛选可用报告期
        available_periods = [p for p in periods if p["is_available"]]

        # 推荐报告期（优先年度报告，其次最新可用）
        recommended = None
        if available_periods:
            # 优先年度报告
            annual_periods = [
                p for p in available_periods if p["report_type"] == "annual"
            ]
            if annual_periods:
                recommended = annual_periods[0]
            else:
                recommended = available_periods[0]

        return {
            "symbol": symbol,
            "available_periods": periods,
            "recommended_period": recommended["end_date"] if recommended else None,
            "total_periods": len(periods),
            "available_count": len(available_periods),
        }

    def get_best_period(
        self,
        symbol: str,
        required_fields: Optional[Dict[str, List[str]]] = None,
        date: Optional[str] = None,
        prefer_annual: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        获取最佳可用报告期

        Args:
            symbol: 股票代码
            required_fields: 必需字段
            date: 截止日期
            prefer_annual: 是否优先年度报告

        Returns:
            最佳报告期信息，如果没有可用报告期返回None
        """
        result = self.get_available_periods(
            symbol, required_fields=required_fields, date=date, limit=20
        )

        available_periods = [
            p for p in result["available_periods"] if p["is_available"]
        ]

        if not available_periods:
            return None

        if prefer_annual:
            # 优先年度报告
            annual_periods = [
                p for p in available_periods if p["report_type"] == "annual"
            ]
            if annual_periods:
                return annual_periods[0]

        # 返回最新的可用报告期
        return available_periods[0]

    def check_period_availability(
        self,
        symbol: str,
        end_date: str,
        required_fields: Optional[Dict[str, List[str]]] = None,
    ) -> Dict[str, Any]:
        """
        检查指定报告期是否可用

        Args:
            symbol: 股票代码
            end_date: 报告期（YYYYMMDD格式）
            required_fields: 必需字段

        Returns:
            {
                'symbol': '601958',
                'end_date': '20241231',
                'is_available': True,
                'data_quality': 'A',
                'completeness': 1.0,
                'missing_fields': [],
                'errors': []
            }
        """
        check_result = self.checker.check_report_period(
            symbol, end_date, required_fields=required_fields
        )

        return {
            "symbol": symbol,
            "end_date": end_date,
            "is_available": check_result["is_available"],
            "data_quality": check_result["data_quality"],
            "completeness": check_result["completeness"],
            "missing_fields": check_result["missing_fields"],
            "null_fields": check_result["null_fields"],
            "errors": check_result["errors"],
            "warnings": check_result["warnings"],
        }

    @staticmethod
    def parse_required_fields_from_string(fields_str: str) -> Dict[str, List[str]]:
        """
        从字符串解析必需字段

        Args:
            fields_str: 字段字符串，格式：table1.field1,table1.field2,table2.field1
                例如："balancesheet.total_assets,income.revenue,cashflow.free_cashflow"

        Returns:
            {
                'balancesheet': ['total_assets'],
                'income': ['revenue'],
                'cashflow': ['free_cashflow']
            }
        """
        if not fields_str:
            return {}

        result = {}

        for field_spec in fields_str.split(","):
            field_spec = field_spec.strip()
            if "." not in field_spec:
                continue

            table, field = field_spec.split(".", 1)
            table = table.strip()
            field = field.strip()

            if table not in result:
                result[table] = []

            result[table].append(field)

        return result
