"""
股票筛选引擎

核心引擎功能：
1. 从数据库加载股票数据（多表JOIN）
2. 应用筛选条件
3. 返回符合条件的股票列表
"""
import pandas as pd
from sqlalchemy import create_engine
from typing import Optional, List
from src.screening.base_criteria import BaseCriteria


class ScreeningEngine:
    """
    股票筛选引擎

    功能：
    1. 从数据库加载股票数据
    2. 应用筛选条件
    3. 返回符合条件的股票列表
    """

    def __init__(self, db_path: str):
        """
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.engine = create_engine(f'sqlite:///{db_path}', echo=False)

    def screen(self, criteria: BaseCriteria, trade_date: Optional[str] = None,
               limit: Optional[int] = None) -> pd.DataFrame:
        """
        执行筛选

        Args:
            criteria: 筛选条件（BaseCriteria实例）
            trade_date: 筛选日期（YYYY-MM-DD），默认最新日期
            limit: 最大返回数量

        Returns:
            符合条件的股票DataFrame
        """
        # 加载数据
        df = self._load_data(trade_date)

        if df.empty:
            return pd.DataFrame()

        # 应用筛选条件
        result = criteria.filter(df)

        # 应用限制
        if limit and len(result) > limit:
            result = result.head(limit)

        return result

    def _load_data(self, trade_date: Optional[str] = None) -> pd.DataFrame:
        """
        从数据库加载股票数据（JOIN多表）

        Args:
            trade_date: 筛选日期（YYYY-MM-DD），默认最新日期

        Returns:
            包含所有数据的DataFrame
        """
        if trade_date is None:
            # 获取最新交易日
            query = """
            SELECT MAX(datetime) as latest_date
            FROM bars
            WHERE interval = '1d'
            """
            result = pd.read_sql_query(query, self.engine)
            if result.empty or result.iloc[0]['latest_date'] is None:
                return pd.DataFrame()
            trade_date = result.iloc[0]['latest_date']

        # 加载指定日期的数据（JOIN多表）
        # 优先使用L3级行业，如果没有则使用L2或L1
        # 使用ROW_NUMBER()获取最细粒度的行业分类
        query = """
        WITH ranked_industries AS (
            SELECT
                b.symbol,
                b.datetime,
                b.open, b.high, b.low, b.close,
                b.volume, b.amount, b.turnover, b.pct_chg,
                b.pe_ttm, b.pb, b.ps_ttm,
                b.total_mv, b.circ_mv,
                sc.industry_name,
                sc.level,
                sc.index_code,
                sc.parent_code,
                sn.name as stock_name,
                ROW_NUMBER() OVER (
                    PARTITION BY b.symbol
                    ORDER BY CASE sc.level WHEN 'L3' THEN 1 WHEN 'L2' THEN 2 ELSE 3 END
                ) as rn
            FROM bars b
            LEFT JOIN stock_names sn ON b.symbol = sn.code
            LEFT JOIN sw_members swm ON b.symbol = SUBSTR(swm.ts_code, 1, 6)
                AND swm.in_date <= b.datetime
                AND (swm.out_date IS NULL OR swm.out_date > b.datetime)
            LEFT JOIN sw_classify sc ON swm.index_code = sc.index_code
            WHERE b.datetime = :trade_date
              AND b.interval = '1d'
        ),
        base_data AS (
            SELECT
                symbol,
                datetime as trade_date,
                open, high, low, close,
                volume, amount, turnover, pct_chg,
                pe_ttm, pb, ps_ttm,
                total_mv, circ_mv,
                stock_name,
                industry_name as sw_l3,
                index_code
            FROM ranked_industries
            WHERE rn = 1
        )
        SELECT
            bd.*,
            sc2.industry_name as sw_l2,
            sc1.industry_name as sw_l1,
            f.roe as latest_roe,
            f.or_yoy as latest_or_yoy,
            f.netprofit_yoy as latest_gr_yoy,
            f.basic_eps,
            f.debt_to_assets
        FROM base_data bd
        LEFT JOIN sw_classify sc2 ON bd.sw_l3 IS NOT NULL
            AND sc2.industry_code = (SELECT parent_code FROM sw_classify WHERE industry_name = bd.sw_l3 LIMIT 1)
        LEFT JOIN sw_classify sc1 ON sc2.industry_code IS NOT NULL
            AND sc1.industry_code = sc2.parent_code
        LEFT JOIN fina_indicator f ON bd.symbol = SUBSTR(f.ts_code, 1, 6)
            AND f.end_date = (
                SELECT MAX(end_date) FROM fina_indicator
                WHERE SUBSTR(ts_code, 1, 6) = bd.symbol AND end_date <= bd.trade_date
            )
        """

        df = pd.read_sql_query(query, self.engine, params={'trade_date': trade_date})

        return df

    def get_available_dates(self, limit: int = 10) -> List[str]:
        """
        获取可用的交易日期列表

        Args:
            limit: 返回的日期数量

        Returns:
            日期列表（YYYY-MM-DD格式）
        """
        query = """
        SELECT DISTINCT datetime
        FROM bars
        WHERE interval = '1d'
        ORDER BY datetime DESC
        LIMIT :limit
        """
        result = pd.read_sql_query(query, self.engine, params={'limit': limit})
        return result['datetime'].tolist()

    def get_industries(self, level: int = 1) -> List[str]:
        """
        获取行业列表

        Args:
            level: 行业级别（1=一级，2=二级，3=三级）

        Returns:
            行业名称列表
        """
        if level == 1:
            query = "SELECT DISTINCT industry_name FROM sw_classify WHERE level = 'L1' ORDER BY industry_name"
        elif level == 2:
            query = "SELECT DISTINCT industry_name FROM sw_classify WHERE level = 'L2' ORDER BY industry_name"
        else:
            query = "SELECT DISTINCT industry_name FROM sw_classify WHERE level = 'L3' ORDER BY industry_name"

        result = pd.read_sql_query(query, self.engine)
        return result.iloc[:, 0].tolist()
