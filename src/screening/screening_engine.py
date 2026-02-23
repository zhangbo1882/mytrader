"""
股票筛选引擎

核心引擎功能：
1. 从数据库加载股票数据（多表JOIN）
2. 应用筛选条件
3. 返回符合条件的股票列表
"""
import logging
import time
import pandas as pd
from sqlalchemy import create_engine
from typing import Optional, List
from src.screening.base_criteria import BaseCriteria, AndCriteria, OrCriteria, NotCriteria
from src.screening.criteria.market_criteria import MarketFilter

logger = logging.getLogger(__name__)


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

        # 初始化 DuckDB 路径
        try:
            from config.settings import DUCKDB_PATH
            self.duckdb_path = str(DUCKDB_PATH)
        except Exception as e:
            logger.warning(f"[ScreeningEngine] Failed to get DuckDB path: {e}")
            self.duckdb_path = None

    def _get_duckdb_conn(self, max_retries: int = 5, retry_delay: float = 0.5):
        """
        获取 DuckDB 连接（每次创建新的只读连接）

        Args:
            max_retries: 最大重试次数
            retry_delay: 重试间隔（秒）
        """
        if not self.duckdb_path:
            return None

        import duckdb
        last_error = None

        for attempt in range(max_retries):
            try:
                conn = duckdb.connect(self.duckdb_path, read_only=True)
                logger.debug(f"[ScreeningEngine] DuckDB connection established (attempt {attempt + 1})")
                return conn
            except Exception as e:
                last_error = e
                if "lock" in str(e).lower() or "IO Error" in str(e):
                    # Lock conflict, retry after delay
                    if attempt < max_retries - 1:
                        logger.debug(f"[ScreeningEngine] DuckDB locked, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(retry_delay)
                else:
                    # Non-lock error, don't retry
                    logger.error(f"[ScreeningEngine] Failed to connect to DuckDB: {e}")
                    break

        logger.error(f"[ScreeningEngine] Failed to connect to DuckDB after {max_retries} attempts: {last_error}")
        return None

    def _close_duckdb_conn(self, conn):
        """关闭 DuckDB 连接"""
        if conn:
            try:
                conn.close()
            except Exception as e:
                logger.warning(f"[ScreeningEngine] Failed to close DuckDB connection: {e}")

    def _extract_market_config(self, criteria: BaseCriteria) -> dict:
        """
        从筛选条件中提取市场配置

        Returns:
            dict: {
                'include_hk': bool,  # 是否包含港股
                'include_a': bool,   # 是否包含A股
            }
        """
        result = {'include_hk': False, 'include_a': False}

        def extract_from_criteria(crit):
            if isinstance(crit, MarketFilter):
                # 检查是否包含港股
                if '港股' in crit.markets and crit.mode == 'whitelist':
                    result['include_hk'] = True

                # 检查是否包含A股市场
                a_stock_markets = [m for m in crit.markets if m in ['主板', '创业板', '科创板', '北交所']]
                if a_stock_markets and crit.mode == 'whitelist':
                    result['include_a'] = True

                # 如果是黑名单模式
                if crit.mode == 'blacklist':
                    # 黑名单且没有排除港股，则包含港股
                    if '港股' not in crit.markets:
                        result['include_hk'] = True

                    # 黑名单且没有排除所有A股市场，则包含A股
                    if set(crit.markets) < set(['主板', '创业板', '科创板', '北交所']):
                        result['include_a'] = True

            elif isinstance(crit, (AndCriteria, OrCriteria)):
                for sub_crit in crit.criteria:
                    extract_from_criteria(sub_crit)
            elif isinstance(crit, NotCriteria):
                extract_from_criteria(crit.criteria)

        extract_from_criteria(criteria)

        # 如果没有明确指定市场，默认包含A股
        if not result['include_hk'] and not result['include_a']:
            result['include_a'] = True

        return result

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
        logger.info(f"[ScreeningEngine] Starting screening, criteria type: {type(criteria).__name__}, date: {trade_date}")

        # 提取市场配置
        market_config = self._extract_market_config(criteria)
        logger.info(f"[ScreeningEngine] Market config: {market_config}")

        # 加载数据
        logger.info(f"[ScreeningEngine] Loading data from database...")
        df = self._load_data(trade_date, market_config)

        if df.empty:
            logger.warning(f"[ScreeningEngine] Database returned empty result")
            return pd.DataFrame()

        logger.info(f"[ScreeningEngine] Data loaded from database, stock count: {len(df)}")

        # 应用筛选条件
        logger.info(f"[ScreeningEngine] Applying filter criteria...")
        result = criteria.filter(df)
        logger.info(f"[ScreeningEngine] Filter applied, remaining stocks: {len(result)}")

        # 应用限制
        if limit and len(result) > limit:
            logger.info(f"[ScreeningEngine] Applying limit: {limit}")
            result = result.head(limit)

        logger.info(f"[ScreeningEngine] Screening completed, final result: {len(result)} stocks")
        return result

    def _load_data(self, trade_date: Optional[str] = None, market_config: dict = None) -> pd.DataFrame:
        """
        从数据库加载股票数据（支持DuckDB和港股）

        Args:
            trade_date: 筛选日期（YYYY-MM-DD），默认最新日期
            market_config: 市场配置 {'include_hk': bool, 'include_a': bool}

        Returns:
            包含所有数据的DataFrame
        """
        logger.debug(f"[ScreeningEngine] _load_data start, specified date: {trade_date}, market_config: {market_config}")

        if market_config is None:
            market_config = {'include_hk': False, 'include_a': True}

        # 使用 DuckDB 加载数据
        duckdb_conn = self._get_duckdb_conn()
        if duckdb_conn is not None:
            try:
                result = self._load_data_from_duckdb(duckdb_conn, trade_date, market_config)
                return result
            except Exception as e:
                logger.error(f"[ScreeningEngine] DuckDB load failed: {e}")
                raise
            finally:
                self._close_duckdb_conn(duckdb_conn)

        # DuckDB 不可用
        raise RuntimeError("DuckDB connection not available for screening")

    def _load_data_from_duckdb(self, duckdb_conn, trade_date: Optional[str] = None, market_config: dict = None) -> pd.DataFrame:
        """
        从 DuckDB 加载A股和/或港股数据

        Args:
            duckdb_conn: DuckDB 连接
            trade_date: 筛选日期（YYYY-MM-DD），默认最新日期
            market_config: 市场配置 {'include_hk': bool, 'include_a': bool}

        Returns:
            包含所有数据的DataFrame
        """
        logger.debug(f"[ScreeningEngine] _load_data_from_duckdb start, market_config: {market_config}")

        if market_config is None:
            market_config = {'include_hk': False, 'include_a': True}

        # 确定需要查询的表
        tables_to_query = []
        if market_config['include_a']:
            tables_to_query.append(('bars_a_1d', 'A股'))
        if market_config['include_hk']:
            tables_to_query.append(('bars_1d', '港股'))

        if not tables_to_query:
            logger.warning(f"[ScreeningEngine] No tables to query")
            return pd.DataFrame()

        # 获取交易日
        if trade_date is None:
            try:
                table_name = tables_to_query[0][0]
                result = duckdb_conn.execute(f"""
                    SELECT MAX(datetime) as latest_date
                    FROM {table_name}
                """).fetchdf()
                if result.empty or result.iloc[0]['latest_date'] is None:
                    logger.error(f"[ScreeningEngine] Cannot get latest trade date from DuckDB")
                    return pd.DataFrame()
                trade_date = result.iloc[0]['latest_date']
                logger.debug(f"[ScreeningEngine] Using latest trade date: {trade_date}")
            except Exception as e:
                logger.error(f"[ScreeningEngine] Error getting latest date from DuckDB: {e}")
                return pd.DataFrame()

        # 从所有需要的表加载数据
        all_dfs = []
        for table_name, market_type in tables_to_query:
            logger.debug(f"[ScreeningEngine] Loading {market_type} data from {table_name} for date: {trade_date}")

            try:
                # 港股和A股的列名略有不同，需要统一
                if table_name == 'bars_1d':  # 港股表
                    query = """
                    SELECT
                        stock_code as symbol,
                        datetime as trade_date,
                        open, high, low, close,
                        volume, amount,
                        turnover_rate_f as turnover,
                        pct_chg,
                        pe_ttm, pb, ps_ttm,
                        total_mv,
                        circ_mv
                    FROM bars_1d
                    WHERE datetime = ?::DATE
                    """
                else:  # A股表
                    query = """
                    SELECT
                        stock_code as symbol,
                        datetime as trade_date,
                        open, high, low, close,
                        volume, amount, turnover_rate_f as turnover, pct_chg,
                        pe_ttm, pb, ps_ttm,
                        total_mv, circ_mv
                    FROM bars_a_1d
                    WHERE datetime = ?::DATE
                    """

                df = duckdb_conn.execute(query, [trade_date]).fetchdf()
                logger.debug(f"[ScreeningEngine] {market_type} query returned {len(df)} rows")

                if not df.empty:
                    # 添加市场标记
                    df['data_source'] = market_type
                    all_dfs.append(df)

            except Exception as e:
                logger.error(f"[ScreeningEngine] Error loading {market_type} data: {e}")
                import traceback
                logger.error(traceback.format_exc())

        if not all_dfs:
            logger.warning(f"[ScreeningEngine] No data found in DuckDB for date: {trade_date}")
            return pd.DataFrame()

        # 合并所有数据
        df = pd.concat(all_dfs, ignore_index=True)
        logger.debug(f"[ScreeningEngine] Combined data shape: {df.shape}")

        # 从 SQLite 获取补充信息（股票名称和市场信息）
        symbols = df['symbol'].tolist()
        placeholders = ','.join([f"'{s}'" for s in symbols])

        # 获取股票名称（分别从A股和港股表获取）
        try:
            # A股名称
            a_mask = df['data_source'] == 'A股'
            if a_mask.any():
                a_symbols = df[a_mask]['symbol'].tolist()
                a_placeholders = ','.join([f"'{s}'" for s in a_symbols])
                names_query = f"""
                SELECT code, name
                FROM stock_names
                WHERE code IN ({a_placeholders})
                """
                names_df = pd.read_sql_query(names_query, self.engine)
                df = df.merge(names_df, left_on='symbol', right_on='code', how='left', suffixes=('', '_a'))

            # 港股名称 - 从DuckDB获取
            hk_mask = df['data_source'] == '港股'
            if hk_mask.any():
                try:
                    hk_symbols = df[hk_mask]['symbol'].tolist()
                    # 港股代码需要加上.HK后缀才能在hk_stock_list中匹配
                    hk_symbols_with_suffix = [f"{s}.HK" for s in hk_symbols]
                    hk_placeholders = ','.join([f"'{s}'" for s in hk_symbols_with_suffix])

                    # 使用DuckDB查询港股名称
                    hk_names_df = duckdb_conn.execute(f"""
                        SELECT ts_code, name
                        FROM hk_stock_list
                        WHERE ts_code IN ({hk_placeholders})
                    """).fetchdf()

                    if not hk_names_df.empty:
                        # 去掉.HK后缀，只保留5位代码
                        hk_names_df['code'] = hk_names_df['ts_code'].str.replace('.HK', '')
                        # 创建映射字典
                        hk_name_map = dict(zip(hk_names_df['code'], hk_names_df['name']))
                        # 为港股设置名称
                        df.loc[hk_mask, 'name'] = df.loc[hk_mask, 'symbol'].map(hk_name_map)
                except Exception as e:
                    logger.warning(f"[ScreeningEngine] Failed to get HK stock names from DuckDB: {e}")

        except Exception as e:
            logger.warning(f"[ScreeningEngine] Failed to get stock names: {e}")
            df['name'] = df['symbol']

        # 重命名列为 stock_name
        if 'name' in df.columns:
            df['stock_name'] = df['name'].fillna(df['symbol'])
        else:
            df['stock_name'] = df['symbol']

        # 为A股数据添加market字段
        if 'market' not in df.columns:
            try:
                a_symbols = df[df['data_source'] == 'A股']['symbol'].tolist()
                if a_symbols:
                    a_placeholders = ','.join([f"'{s}'" for s in a_symbols])
                    market_query = f"""
                    SELECT code, market
                    FROM stock_basic_info
                    WHERE code IN ({a_placeholders})
                    """
                    market_df = pd.read_sql_query(market_query, self.engine)
                    df = df.merge(market_df, left_on='symbol', right_on='code', how='left', suffixes=('', '_y'))
                    # 只为A股更新market字段
                    a_mask = df['data_source'] == 'A股'
                    if 'market_y' in df.columns:
                        df.loc[a_mask, 'market'] = df.loc[a_mask, 'market_y']
            except Exception as e:
                logger.warning(f"[ScreeningEngine] Failed to get market info from SQLite: {e}")
                df.loc[df['data_source'] == 'A股', 'market'] = None

        # 为港股设置market字段
        df.loc[df['data_source'] == '港股', 'market'] = '港股'

        # 获取A股的行业信息（申万行业）
        a_mask = df['data_source'] == 'A股'
        if a_mask.any():
            try:
                a_symbols = df[a_mask]['symbol'].tolist()
                a_placeholders = ','.join([f"'{s}'" for s in a_symbols])

                # 获取每个股票对应的行业（优先 L3，其次 L2，最后 L1）
                industry_query = f"""
                WITH ranked_industries AS (
                    SELECT
                        SUBSTR(swm.ts_code, 1, 6) as symbol,
                        sc.industry_name,
                        sc.level,
                        sc.index_code,
                        ROW_NUMBER() OVER (
                            PARTITION BY SUBSTR(swm.ts_code, 1, 6)
                            ORDER BY CASE sc.level WHEN 'L3' THEN 1 WHEN 'L2' THEN 2 ELSE 3 END
                        ) as rn
                    FROM sw_members swm
                    LEFT JOIN sw_classify sc ON swm.index_code = sc.index_code
                    WHERE SUBSTR(swm.ts_code, 1, 6) IN ({a_placeholders})
                        AND swm.in_date <= '{trade_date}'
                        AND (swm.out_date IS NULL OR swm.out_date > '{trade_date}')
                )
                SELECT
                    symbol,
                    industry_name as sw_l3,
                    index_code
                FROM ranked_industries
                WHERE rn = 1
                """
                industry_df = pd.read_sql_query(industry_query, self.engine)
                df = df.merge(industry_df, on='symbol', how='left')
            except Exception as e:
                logger.warning(f"[ScreeningEngine] Failed to get industry info from SQLite: {e}")
                df['sw_l3'] = None
                df['index_code'] = None

        # 获取一级行业
        if not df.empty and 'index_code' in df.columns:
            try:
                a_symbols = df[a_mask]['symbol'].tolist()
                if a_symbols:
                    a_placeholders = ','.join([f"'{s}'" for s in a_symbols])
                    l1_query = f"""
                    SELECT DISTINCT
                        swm.ts_code,
                        sc.industry_name as sw_l1
                    FROM sw_members swm
                    JOIN sw_classify sc ON swm.index_code = sc.index_code
                    WHERE SUBSTR(swm.ts_code, 1, 6) IN ({a_placeholders})
                        AND sc.level = 'L1'
                    """
                    l1_df = pd.read_sql_query(l1_query, self.engine)
                    l1_df['symbol'] = l1_df['ts_code'].str[:6]
                    df = df.merge(l1_df[['symbol', 'sw_l1']].drop_duplicates(), on='symbol', how='left')
            except Exception as e:
                logger.warning(f"[ScreeningEngine] Failed to get L1 industries: {e}")

        # 调整列顺序
        cols = ['symbol', 'stock_name', 'trade_date', 'open', 'high', 'low', 'close',
                'volume', 'amount', 'turnover', 'pct_chg', 'pe_ttm', 'pb', 'ps_ttm',
                'total_mv', 'circ_mv', 'market', 'sw_l3', 'sw_l1', 'data_source']
        df = df[[c for c in cols if c in df.columns]]

        # 添加市值单位转换
        if 'circ_mv' in df.columns:
            # 流通市值转换：A股（万元 -> 亿元）、港股（元 -> 亿元）
            if 'data_source' in df.columns:
                # A股转换：万元 -> 亿元（除以10000）
                a_mask = df['data_source'] == 'A股'
                df.loc[a_mask, 'circ_mv_yi'] = df.loc[a_mask, 'circ_mv'] / 10000

                # 港股转换：元 -> 亿元（除以100000000）
                hk_mask = df['data_source'] == '港股'
                df.loc[hk_mask, 'circ_mv_yi'] = df.loc[hk_mask, 'circ_mv'] / 100000000
            else:
                # 没有 data_source 字段，假设是A股（万元）
                df['circ_mv_yi'] = df['circ_mv'] / 10000

        return df

        # 加载指定日期的数据（JOIN多表）
        # 优先使用L3级行业，如果没有则使用L2或L1
        # 使用ROW_NUMBER()获取最细粒度的行业分类
        logger.debug(f"[ScreeningEngine] Executing multi-table JOIN query, date: {trade_date}")
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
                sbi.market,
                ROW_NUMBER() OVER (
                    PARTITION BY b.symbol
                    ORDER BY CASE sc.level WHEN 'L3' THEN 1 WHEN 'L2' THEN 2 ELSE 3 END
                ) as rn
            FROM bars b
            LEFT JOIN stock_names sn ON b.symbol = sn.code
            LEFT JOIN stock_basic_info sbi ON b.symbol = sbi.code
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
                index_code,
                market
            FROM ranked_industries
            WHERE rn = 1
        )
        SELECT
            bd.symbol,
            bd.trade_date,
            bd.open, bd.high, bd.low, bd.close,
            bd.volume, bd.amount, bd.turnover, bd.pct_chg,
            bd.pe_ttm, bd.pb, bd.ps_ttm,
            bd.total_mv, bd.circ_mv,
            bd.stock_name,
            bd.sw_l3,
            bd.index_code,
            bd.market,
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
        logger.debug(f"[ScreeningEngine] Database query returned, data shape: {df.shape}")

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
