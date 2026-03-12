"""
财务数据一致性检查器（严格模式 + 历史数据检查）

职责：
- 检查财务数据的完整性、一致性、合理性
- 检查过去3年的历史数据完整性
- 提供客观的数据质量评分
- 不包含任何业务逻辑（估值、筛选等）
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from sqlalchemy import create_engine


class FinancialDataConsistencyChecker:
    """
    财务数据一致性检查器（严格模式 + 历史数据检查）

    核心原则：
    1. 数据必须来自同一报告期（end_date）
    2. 所有必需字段必须非NULL
    3. 数值必须合理（如total_assets > 0）
    4. 检查过去3年的历史数据完整性
    5. 至少需要6个完整报告期才可用
    """

    # 默认必需字段（用于一般财务数据检查）
    DEFAULT_REQUIRED_FIELDS = {
        "balancesheet": ["total_assets", "total_liab", "total_hldr_eqy_exc_min_int"],
        "income": ["revenue", "n_income", "n_income_attr_p"],
        "cashflow": ["n_cashflow_act", "free_cashflow", "end_bal_cash"],
    }

    # 数据质量等级定义
    QUALITY_GRADES = {
        "A": {"min_completeness": 1.0, "description": "数据完整，质量优秀"},
        "B": {"min_completeness": 0.9, "description": "次要字段缺失，不影响核心分析"},
        "C": {"min_completeness": 0.7, "description": "关键字段缺失，数据质量一般"},
        "D": {"min_completeness": 0.0, "description": "数据严重缺失或不可用"},
    }

    def __init__(self, db_path: str = "data/tushare_data.db"):
        """
        初始化检查器

        Args:
            db_path: 数据库路径
        """
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{db_path}")

    def check_report_period(
        self,
        code: str,
        end_date: str,
        required_fields: Optional[Dict[str, List[str]]] = None,
        check_historical: bool = True,
        historical_years: int = 3,
    ) -> Dict[str, Any]:
        """
        检查指定报告期的数据质量（严格模式 + 历史数据检查）

        Args:
            code: 股票代码
            end_date: 报告期（YYYYMMDD格式）
            required_fields: 必需字段字典
            check_historical: 是否检查历史数据（默认True）
            historical_years: 检查过去几年的历史数据（默认3年）

        Returns:
            {
                'is_available': bool,
                'end_date': str,
                'data_quality': str,
                'completeness': float,
                'missing_fields': [],
                'null_fields': [],
                'invalid_fields': [],
                'warnings': [],
                'errors': [],
                'historical_completeness': float,  # 历史数据完整度
                'available_periods_count': int,  # 可用报告期数量
                'total_periods_count': int  # 总报告期数量
            }
        """
        result = {
            "is_available": True,
            "end_date": end_date,
            "data_quality": "A",
            "completeness": 1.0,
            "missing_fields": [],
            "null_fields": [],
            "invalid_fields": [],
            "warnings": [],
            "errors": [],
            "historical_completeness": 0.0,
            "available_periods_count": 0,
            "total_periods_count": 0,
        }

        # 标准化股票代码
        if "." not in code:
            code = self._standardize_code(code)

        # 使用传入的required_fields或默认值
        fields_to_check = required_fields or self.DEFAULT_REQUIRED_FIELDS

        # 统计完整度
        total_fields = 0
        valid_fields = 0

        # 检查每个表的字段
        table_data = {}

        for table, fields in fields_to_check.items():
            total_fields += len(fields)

            # 查询该表该报告期的数据
            table_result = self._query_table_data(code, end_date, table, fields)

            if table_result is None:
                # 表数据不存在
                for field in fields:
                    result["missing_fields"].append(f"{table}.{field}")
                    result["errors"].append(f"{table}表无{end_date}报告期数据")
                result["is_available"] = False
                continue

            table_data[table] = table_result

            # 检查每个字段
            for field in fields:
                if field not in table_result:
                    result["missing_fields"].append(f"{table}.{field}")
                    result["is_available"] = False
                elif pd.isna(table_result[field]):
                    result["null_fields"].append(f"{table}.{field}")
                    result["is_available"] = False
                else:
                    # 字段存在且非NULL
                    valid_fields += 1

                    # 检查数值合理性
                    validation_result = self._validate_field_value(
                        table, field, table_result[field]
                    )
                    if not validation_result["is_valid"]:
                        result["invalid_fields"].append(
                            f"{table}.{field}: {validation_result['reason']}"
                        )
                        result["warnings"].append(
                            f"{table}.{field}值异常: {validation_result['reason']}"
                        )

        # 计算完整度
        if total_fields > 0:
            result["completeness"] = valid_fields / total_fields

        # 评估数据质量等级
        result["data_quality"] = self._calculate_quality_grade(result["completeness"])

        # 如果有必需字段缺失或为NULL，严格模式标记为不可用
        if result["missing_fields"] or result["null_fields"]:
            result["is_available"] = False
            result["errors"].append(
                f"严格模式：必需字段缺失或为NULL - "
                f"缺失: {len(result['missing_fields'])}, "
                f"NULL: {len(result['null_fields'])}"
            )

        # 检查历史数据（过去3年）
        if check_historical:
            historical_result = self._check_historical_data(
                code, end_date, required_fields, historical_years
            )

            result["historical_completeness"] = historical_result["completeness"]
            result["available_periods_count"] = historical_result["available_count"]
            result["total_periods_count"] = historical_result["total_count"]

            # 如果历史数据太少，降低可用性
            if historical_result["available_count"] < 6:  # 至少需要6个季度数据
                result["is_available"] = False
                result["errors"].append(
                    f"历史数据不足：过去{historical_years}年仅有{historical_result['available_count']}个完整报告期 "
                    f"(需要至少6个)"
                )
                result["data_quality"] = "D"

            # 添加警告信息
            if historical_result["completeness"] < 0.8:
                result["warnings"].append(
                    f"历史数据完整度较低: {historical_result['completeness'] * 100:.1f}% "
                    f"({historical_result['available_count']}/{historical_result['total_count']}个报告期)"
                )

        return result

    def _check_historical_data(
        self,
        code: str,
        end_date: str,
        required_fields: Optional[Dict[str, List[str]]],
        years: int = 3,
    ) -> Dict[str, Any]:
        """
        检查过去N年的历史数据完整性

        Args:
            code: 股票代码
            end_date: 基准报告期（YYYYMMDD格式）
            required_fields: 必需字段
            years: 检查年数（默认3年）

        Returns:
            {
                'completeness': float,  # 历史数据完整度 0.0-1.0
                'available_count': int,  # 可用报告期数量
                'total_count': int  # 总报告期数量
            }
        """
        # 计算起始日期（向前推N年）
        try:
            end_date_dt = datetime.strptime(end_date, "%Y%m%d")
            start_year = end_date_dt.year - years
            start_date = f"{start_year}0101"
        except Exception:
            start_date = "20200101"

        # 获取过去N年的所有报告期
        query = """
        SELECT DISTINCT end_date
        FROM balancesheet
        WHERE ts_code = :code
          AND end_date >= :start_date
          AND end_date <= :end_date
          AND report_type = '1'
        ORDER BY end_date DESC
        """

        try:
            df = pd.read_sql_query(
                query,
                self.engine,
                params={"code": code, "start_date": start_date, "end_date": end_date},
            )
        except Exception as e:
            print(f"Error querying historical periods for {code}: {e}")
            return {"completeness": 0.0, "available_count": 0, "total_count": 0}

        if df.empty:
            return {"completeness": 0.0, "available_count": 0, "total_count": 0}

        # 检查每个报告期的完整性
        available_count = 0
        total_count = len(df)
        periods_detail = []  # 添加详情列表

        for _, row in df.iterrows():
            period_end_date = str(row["end_date"])

            # 检查该报告期的必需字段（不递归检查历史)
            period_check = self._check_single_period_quick(
                code, period_end_date, required_fields
            )
            periods_detail.append(
                {
                    "end_date": period_end_date,
                    "is_complete": period_check["is_complete"],
                    "completeness": period_check["completeness"],
                    "missing_fields": period_check.get("missing_fields", []),
                }
            )

            if period_check["is_complete"]:
                available_count += 1

        # 计算历史数据完整度
        completeness = available_count / total_count if total_count > 0 else 0.0

        return {
            "completeness": completeness,
            "available_count": available_count,
            "total_count": total_count,
            "periods": [],  # 添加 periods 详情
        }

    def _check_single_period_quick(
        self, code: str, end_date: str, required_fields: Optional[Dict[str, List[str]]]
    ) -> Dict[str, Any]:
        """
        快速检查单个报告期的数据完整性（用于历史数据检查）

        Args:
            code: 股票代码
            end_date: 报告期（YYYYMMDD格式）
            required_fields: 必需字段

        Returns:
            {
                'is_complete': bool,
                'completeness': float,
                'missing_fields': List[str]
            }
        """
        if required_fields is None:
            required_fields = self.DEFAULT_REQUIRED_FIELDS

        # 检查每个表的必需字段
        missing_fields = []
        total_fields = 0
        valid_fields = 0

        for table, fields in required_fields.items():
            for field in fields:
                total_fields += 1

                # 查询字段值
                value = self._query_field_value(code, table, end_date, field)

                if value is None or pd.isna(value):
                    missing_fields.append(f"{table}.{field}")
                else:
                    valid_fields += 1

        completeness = valid_fields / total_fields if total_fields > 0 else 0.0
        is_complete = len(missing_fields) == 0

        return {
            "is_complete": is_complete,
            "completeness": completeness,
            "missing_fields": missing_fields,
        }

    def _query_field_value(
        self, code: str, table: str, end_date: str, field: str
    ) -> Optional[Any]:
        """
        查询单个字段的值

        Args:
            code: 股票代码
            table: 表名
            end_date: 报告期
            field: 字段名

        Returns:
            字段值，如果不存在返回None
        """
        try:
            query = f"""
            SELECT {field}
            FROM {table}
            WHERE ts_code = :code
              AND end_date = :end_date
              AND report_type = '1'
            LIMIT 1
            """

            df = pd.read_sql_query(
                query, self.engine, params={"code": code, "end_date": end_date}
            )

            if df.empty:
                return None

            return df.iloc[0][field]

        except Exception:
            return None

    def get_available_periods(
        self,
        code: str,
        required_fields: Optional[Dict[str, List[str]]] = None,
        date: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        获取所有可用的报告期列表（严格模式）

        Args:
            code: 股票代码
            required_fields: 必需字段（可选）
            date: 截止日期（YYYY-MM-DD格式）
            limit: 返回数量限制

        Returns:
            [
                {
                    'end_date': '20241231',
                    'report_type': 'annual',  # annual/quarterly
                    'is_available': True,
                    'data_quality': 'A',
                    'completeness': 1.0,
                    'missing_fields': []
                },
                ...
            ]
        """
        # 标准化股票代码
        if "." not in code:
            code = self._standardize_code(code)

        # 获取所有报告期
        query = """
        SELECT DISTINCT end_date
        FROM balancesheet
        WHERE ts_code = :code
          AND report_type = '1'
        """

        params = {"code": code}

        if date:
            date_db = date.replace("-", "")
            query += f" AND end_date <= '{date_db}'"

        query += " ORDER BY end_date DESC LIMIT :limit"
        params["limit"] = limit

        try:
            df = pd.read_sql_query(query, self.engine, params=params)
        except Exception as e:
            print(f"Error querying periods for {code}: {e}")
            return []

        if df.empty:
            return []

        # 检查每个报告期的数据质量
        results = []
        for _, row in df.iterrows():
            end_date = str(row["end_date"])

            # 检查该报告期（不检查历史数据，避免递归）
            check_result = self.check_report_period(
                code, end_date, required_fields, check_historical=False
            )

            # 判断报告类型
            report_type = "annual" if end_date.endswith("1231") else "quarterly"

            results.append(
                {
                    "end_date": end_date,
                    "report_type": report_type,
                    "is_available": check_result["is_available"],
                    "data_quality": check_result["data_quality"],
                    "completeness": check_result["completeness"],
                    "missing_fields": check_result["missing_fields"],
                    "null_fields": check_result["null_fields"],
                    "warnings": check_result["warnings"],
                }
            )

        return results

    def batch_check_consistency(
        self,
        codes: List[str],
        end_date: Optional[str] = None,
        required_fields: Optional[Dict[str, List[str]]] = None,
    ) -> Dict[str, Any]:
        """
        批量检查多只股票的数据一致性

        Args:
            codes: 股票代码列表
            end_date: 指定报告期（可选，不传则检查最新报告期）
            required_fields: 必需字段（可选）

        Returns:
            {
                'total_stocks': int,
                'passed': int,
                'failed': int,
                'pass_rate': float,
                'fail_rate': float,
                'quality_distribution': {'A': 0, 'B': 0, 'C': 0, 'D': 0},
                'failed_stocks': [
                    {
                        'code': str,
                        'issues': []
                    }
                ],
                'checked_at': str
            }
        """
        results = {
            "total_stocks": len(codes),
            "passed": 0,
            "failed": 0,
            "pass_rate": 0.0,
            "fail_rate": 0.0,
            "quality_distribution": {"A": 0, "B": 0, "C": 0, "D": 0},
            "failed_stocks": [],
            "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        for code in codes:
            try:
                # 如果没有指定end_date，检查最新报告期
                if end_date:
                    check_result = self.check_report_period(
                        code, end_date, required_fields
                    )
                else:
                    # 获取最新报告期
                    available = self.get_available_periods(
                        code, required_fields, limit=1
                    )
                    if not available:
                        results["failed"] += 1
                        results["quality_distribution"]["D"] += 1
                        results["failed_stocks"].append(
                            {"code": code, "issues": ["无任何报告期数据"]}
                        )
                        continue

                    check_result = available[0]

                # 统计结果
                if check_result["is_available"]:
                    results["passed"] += 1
                else:
                    results["failed"] += 1
                    results["failed_stocks"].append(
                        {
                            "code": code,
                            "issues": check_result.get("missing_fields", [])
                            + check_result.get("null_fields", []),
                        }
                    )

                # 统计质量分布
                quality = check_result["data_quality"]
                if quality in results["quality_distribution"]:
                    results["quality_distribution"][quality] += 1

            except Exception as e:
                results["failed"] += 1
                results["quality_distribution"]["D"] += 1
                results["failed_stocks"].append(
                    {"code": code, "issues": [f"检查失败: {str(e)}"]}
                )

        # 计算比率
        if results["total_stocks"] > 0:
            results["pass_rate"] = results["passed"] / results["total_stocks"]
            results["fail_rate"] = results["failed"] / results["total_stocks"]

        return results

    def _query_table_data(
        self, code: str, end_date: str, table: str, fields: List[str]
    ) -> Optional[Dict[str, Any]]:
        """
        查询指定表指定报告期的数据

        Args:
            code: 股票代码
            end_date: 报告期
            table: 表名
            fields: 字段列表

        Returns:
            字段值字典，如果查询失败返回None
        """
        try:
            fields_str = ", ".join(fields)
            query = f"""
            SELECT {fields_str}
            FROM {table}
            WHERE ts_code = :code
              AND end_date = :end_date
              AND report_type = '1'
            LIMIT 1
            """

            df = pd.read_sql_query(
                query, self.engine, params={"code": code, "end_date": end_date}
            )

            if df.empty:
                return None

            return df.iloc[0].to_dict()

        except Exception as e:
            print(f"Error querying {table} for {code} at {end_date}: {e}")
            return None

    def _validate_field_value(
        self, table: str, field: str, value: Any
    ) -> Dict[str, Any]:
        """
        验证字段值的合理性

        Returns:
            {'is_valid': bool, 'reason': str}
        """
        # 定义合理性规则
        validation_rules = {
            ("balancesheet", "total_assets"): lambda v: v > 0,
            ("balancesheet", "total_liab"): lambda v: v >= 0,
            ("balancesheet", "total_hldr_eqy_exc_min_int"): lambda v: v != 0,
            ("income", "revenue"): lambda v: True,  # 收入可以为0
            ("income", "n_income"): lambda v: True,  # 净利润可以为负
            ("cashflow", "n_cashflow_act"): lambda v: True,  # 现金流可以为负
        }

        validator = validation_rules.get((table, field))

        if validator is None:
            # 没有定义验证规则，默认通过
            return {"is_valid": True, "reason": ""}

        try:
            is_valid = validator(value)
            if is_valid:
                return {"is_valid": True, "reason": ""}
            else:
                return {"is_valid": False, "reason": f"值{value}不符合合理性要求"}
        except Exception as e:
            return {"is_valid": False, "reason": f"验证失败: {str(e)}"}

    def _calculate_quality_grade(self, completeness: float) -> str:
        """
        根据完整度计算质量等级

        Args:
            completeness: 完整度 0.0-1.0

        Returns:
            质量等级 A/B/C/D
        """
        if completeness >= 1.0:
            return "A"
        elif completeness >= 0.9:
            return "B"
        elif completeness >= 0.7:
            return "C"
        else:
            return "D"

    def _standardize_code(self, code: str) -> str:
        """
        标准化股票代码

        Args:
            code: 股票代码（可能不带后缀）

        Returns:
            标准化后的代码（带.SH/.SZ后缀）
        """
        code = code.upper()
        if "." in code:
            return code

        # 判断市场
        if code.startswith("6"):
            return f"{code}.SH"
        elif code.startswith(("0", "3")):
            return f"{code}.SZ"
        elif code.startswith("68"):
            return f"{code}.SH"
        else:
            return f"{code}.SH"  # 默认上海
