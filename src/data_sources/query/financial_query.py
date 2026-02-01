"""
财务数据查询模块

提供财务报表数据查询接口
"""

import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional, List


class FinancialQuery:
    """财务数据查询类"""

    def __init__(self, db_path: str = "data/tushare_data.db"):
        """
        初始化财务查询

        Args:
            db_path: 数据库路径
        """
        self.engine = create_engine(f"sqlite:///{db_path}")

    def _standardize_code(self, symbol: str) -> str:
        """
        标准化股票代码格式

        Args:
            symbol: 股票代码（600382 或 600382.SH）

        Returns:
            标准化的 6 位代码
        """
        if '.' in symbol:
            return symbol.split('.')[0]
        return symbol

    def query_income(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        report_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        查询利润表数据

        Args:
            symbol: 股票代码
            start_date: 开始公告日期（格式 YYYYMMDD）
            end_date: 结束公告日期（格式 YYYYMMDD）
            report_type: 报告类型（1合并报表、2单季合并、3调整单季合并表）

        Returns:
            利润表数据 DataFrame
        """
        code = self._standardize_code(symbol)
        table_name = f"income_{code}"

        # 构建查询条件
        conditions = []
        params = {}

        if start_date:
            conditions.append("ann_date >= :start_date")
            params["start_date"] = start_date

        if end_date:
            conditions.append("ann_date <= :end_date")
            params["end_date"] = end_date

        if report_type:
            conditions.append("report_type = :report_type")
            params["report_type"] = report_type

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
        SELECT * FROM {table_name}
        WHERE {where_clause}
        ORDER BY ann_date DESC
        """

        try:
            with self.engine.connect() as conn:
                df = pd.read_sql_query(query, conn, params=params)
                return df
        except Exception as e:
            print(f"查询利润表失败: {e}")
            return pd.DataFrame()

    def query_balancesheet(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        report_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        查询资产负债表数据

        Args:
            symbol: 股票代码
            start_date: 开始公告日期（格式 YYYYMMDD）
            end_date: 结束公告日期（格式 YYYYMMDD）
            report_type: 报告类型

        Returns:
            资产负债表数据 DataFrame
        """
        code = self._standardize_code(symbol)
        table_name = f"balancesheet_{code}"

        # 构建查询条件
        conditions = []
        params = {}

        if start_date:
            conditions.append("ann_date >= :start_date")
            params["start_date"] = start_date

        if end_date:
            conditions.append("ann_date <= :end_date")
            params["end_date"] = end_date

        if report_type:
            conditions.append("report_type = :report_type")
            params["report_type"] = report_type

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
        SELECT * FROM {table_name}
        WHERE {where_clause}
        ORDER BY ann_date DESC
        """

        try:
            with self.engine.connect() as conn:
                df = pd.read_sql_query(query, conn, params=params)
                return df
        except Exception as e:
            print(f"查询资产负债表失败: {e}")
            return pd.DataFrame()

    def query_cashflow(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        report_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        查询现金流量表数据

        Args:
            symbol: 股票代码
            start_date: 开始公告日期（格式 YYYYMMDD）
            end_date: 结束公告日期（格式 YYYYMMDD）
            report_type: 报告类型

        Returns:
            现金流量表数据 DataFrame
        """
        code = self._standardize_code(symbol)
        table_name = f"cashflow_{code}"

        # 构建查询条件
        conditions = []
        params = {}

        if start_date:
            conditions.append("ann_date >= :start_date")
            params["start_date"] = start_date

        if end_date:
            conditions.append("ann_date <= :end_date")
            params["end_date"] = end_date

        if report_type:
            conditions.append("report_type = :report_type")
            params["report_type"] = report_type

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
        SELECT * FROM {table_name}
        WHERE {where_clause}
        ORDER BY ann_date DESC
        """

        try:
            with self.engine.connect() as conn:
                df = pd.read_sql_query(query, conn, params=params)
                return df
        except Exception as e:
            print(f"查询现金流量表失败: {e}")
            return pd.DataFrame()

    def query_all_financial(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """
        查询所有财务数据

        Args:
            symbol: 股票代码
            start_date: 开始公告日期（格式 YYYYMMDD）
            end_date: 结束公告日期（格式 YYYYMMDD）

        Returns:
            包含三张报表的字典
            {
                'income': DataFrame,
                'balancesheet': DataFrame,
                'cashflow': DataFrame
            }
        """
        return {
            'income': self.query_income(symbol, start_date, end_date),
            'balancesheet': self.query_balancesheet(symbol, start_date, end_date),
            'cashflow': self.query_cashflow(symbol, start_date, end_date)
        }

    def get_latest_report_date(self, symbol: str, table_type: str = 'income') -> Optional[str]:
        """
        获取最新财报日期

        Args:
            symbol: 股票代码
            table_type: 报表类型（income/balancesheet/cashflow）

        Returns:
            最新公告日期（格式 YYYYMMDD），无数据返回 None
        """
        code = self._standardize_code(symbol)
        table_name = f"{table_type}_{code}"

        try:
            with self.engine.connect() as conn:
                # 检查表是否存在
                result = conn.execute(text(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                ))
                if not result.fetchone():
                    return None

                # 查询最新日期
                query = f"""
                SELECT ann_date, end_date FROM {table_name}
                ORDER BY ann_date DESC LIMIT 1
                """
                df = pd.read_sql_query(query, conn)

                if not df.empty:
                    return df[['ann_date', 'end_date']].iloc[0].to_dict()
                return None

        except Exception as e:
            print(f"查询最新财报日期失败: {e}")
            return None

    def compare_revenue(
        self,
        symbols: List[str],
        years: int = 5
    ) -> pd.DataFrame:
        """
        对比多家公司的营业收入

        Args:
            symbols: 股票代码列表
            years: 对比最近几年的数据

        Returns:
            对比结果 DataFrame
        """
        result_data = []

        for symbol in symbols:
            code = self._standardize_code(symbol)
            table_name = f"income_{code}"

            try:
                query = f"""
                SELECT ts_code, end_date, total_revenue, revenue
                FROM {table_name}
                WHERE report_type = '1'
                ORDER BY end_date DESC
                LIMIT {years}
                """

                with self.engine.connect() as conn:
                    df = pd.read_sql_query(query, conn)

                    if not df.empty:
                        for _, row in df.iterrows():
                            result_data.append({
                                'symbol': code,
                                'end_date': row['end_date'],
                                'total_revenue': row['total_revenue'],
                                'revenue': row['revenue']
                            })

            except Exception as e:
                print(f"查询 {symbol} 营业收入失败: {e}")

        if result_data:
            return pd.DataFrame(result_data)
        return pd.DataFrame()

    def get_financial_summary(
        self,
        symbol: str,
        year: Optional[str] = None
    ) -> dict:
        """
        获取财务摘要数据

        Args:
            symbol: 股票代码
            year: 年份（格式 YYYY），None 则返回最新年报

        Returns:
            财务摘要字典
        """
        code = self._standardize_code(symbol)

        # 构建查询条件
        income_table = f"income_{code}"
        balance_table = f"balancesheet_{code}"
        cashflow_table = f"cashflow_{code}"

        conditions = ["report_type = '1'"]  # 只查合并报表
        params = {}

        if year:
            conditions.append("end_date LIKE :year")
            params["year"] = f"{year}%"

        where_clause = " AND ".join(conditions)

        summary = {}

        try:
            with self.engine.connect() as conn:
                # 检查表是否存在
                for table in [income_table, balance_table, cashflow_table]:
                    result = conn.execute(text(
                        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
                    ))
                    if not result.fetchone():
                        return summary

                # 利润表关键指标
                income_query = f"""
                SELECT end_date, total_revenue, n_income, n_income_attr_p, basic_eps
                FROM {income_table}
                WHERE {where_clause}
                ORDER BY end_date DESC LIMIT 1
                """
                income_df = pd.read_sql_query(income_query, conn, params=params)

                if not income_df.empty:
                    summary['income'] = income_df.iloc[0].to_dict()

                # 资产负债表关键指标
                balance_query = f"""
                SELECT end_date, total_assets, total_liability, total_owner_equities
                FROM {balance_table}
                WHERE {where_clause}
                ORDER BY end_date DESC LIMIT 1
                """
                balance_df = pd.read_sql_query(balance_query, conn, params=params)

                if not balance_df.empty:
                    summary['balance'] = balance_df.iloc[0].to_dict()

                # 现金流量表关键指标
                cashflow_query = f"""
                SELECT end_date, net_cash_flows_oper_act, free_cashflow
                FROM {cashflow_table}
                WHERE {where_clause}
                ORDER BY end_date DESC LIMIT 1
                """
                cashflow_df = pd.read_sql_query(cashflow_query, conn, params=params)

                if not cashflow_df.empty:
                    summary['cashflow'] = cashflow_df.iloc[0].to_dict()

        except Exception as e:
            print(f"查询财务摘要失败: {e}")

        return summary

    def list_financial_tables(self) -> List[str]:
        """
        列出数据库中所有财务报表表

        Returns:
            表名列表
        """
        try:
            with self.engine.connect() as conn:
                query = """
                SELECT name FROM sqlite_master
                WHERE type='table'
                AND (name LIKE 'income_%' OR name LIKE 'balancesheet_%' OR name LIKE 'cashflow_%')
                ORDER BY name
                """
                df = pd.read_sql_query(query, conn)
                return df['name'].tolist()
        except Exception as e:
            print(f"查询财务表列表失败: {e}")
            return []

    def get_financial_stats(self) -> pd.DataFrame:
        """
        获取财务数据统计信息

        Returns:
            统计信息 DataFrame
        """
        tables = self.list_financial_tables()
        stats = []

        for table in tables:
            try:
                with self.engine.connect() as conn:
                    # 获取记录数
                    count_query = f"SELECT COUNT(*) as count FROM {table}"
                    count_df = pd.read_sql_query(count_query, conn)
                    count = count_df['count'].iloc[0]

                    # 获取最新日期
                    date_query = f"SELECT MAX(ann_date) as latest_date FROM {table}"
                    date_df = pd.read_sql_query(date_query, conn)
                    latest_date = date_df['latest_date'].iloc[0]

                    # 解析股票代码和报表类型
                    if table.startswith('income_'):
                        table_type = 'income'
                        code = table.replace('income_', '')
                    elif table.startswith('balancesheet_'):
                        table_type = 'balancesheet'
                        code = table.replace('balancesheet_', '')
                    elif table.startswith('cashflow_'):
                        table_type = 'cashflow'
                        code = table.replace('cashflow_', '')
                    else:
                        continue

                    stats.append({
                        'code': code,
                        'table_type': table_type,
                        'table_name': table,
                        'record_count': count,
                        'latest_date': latest_date
                    })

            except Exception as e:
                print(f"获取表 {table} 统计失败: {e}")

        return pd.DataFrame(stats)
