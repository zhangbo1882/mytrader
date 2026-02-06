"""
行业统计计算器

功能：
- 基于当前所有可用数据计算行业内各指标的百分位统计
- 缓存到industry_statistics表
- 支持三级行业统计（sw_l1/sw_l2/sw_l3）
"""
import pandas as pd
from typing import List, Optional
from sqlalchemy import create_engine, text
from datetime import datetime


def groupby_two_levels(df, columns):
    """Helper function to group by two levels, handling NaN values"""
    if columns[0] not in df.columns or columns[1] not in df.columns:
        return []

    grouped = df.groupby(columns, dropna=False)
    for keys, _ in grouped:
        yield keys[0], keys[1]


class IndustryStatisticsCalculator:
    """
    行业统计计算器：动态生成行业参数

    基于当前所有可用数据计算行业内各指标的百分位统计，缓存到industry_statistics表
    支持三级行业统计（sw_l1/sw_l2/sw_l3）
    """

    def __init__(self, db_path: str):
        """
        Args:
            db_path: 数据库路径
        """
        self.db_path = db_path
        self.engine = create_engine(f'sqlite:///{db_path}', echo=False)

    def calculate_industry_statistics(self, calculated_at: str = None,
                                       metrics: List[str] = None) -> pd.DataFrame:
        """
        计算行业统计（使用所有可用数据，无滚动窗口）

        为每个行业级别（L1/L2/L3）生成独立的统计记录

        Args:
            calculated_at: 计算时间标识（YYYY-MM-DD HH:MM:SS），默认当前时间
            metrics: 要计算的指标列表

        Returns:
            DataFrame with columns: [calculated_at, sw_l1, sw_l2, sw_l3, metric_name,
                                     p10, p25, p50, p75, p90, mean, std, min, max, count]
        """
        if calculated_at is None:
            calculated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if metrics is None:
            # 从bars表可直接获取的指标
            metrics = ['pe_ttm', 'pb', 'ps_ttm', 'total_mv', 'circ_mv']

        results = []

        # 获取最新交易日期
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT MAX(datetime) as latest_date
                FROM bars
                WHERE interval = '1d'
            """))
            latest_date = result.fetchone()[0]

        if not latest_date:
            print("Error: No trading data found")
            return pd.DataFrame()

        print(f"  Using latest trading date: {latest_date}")

        for metric in metrics:
            # 只读取最新一天的数据，通过多次JOIN获取l1/l2/l3行业名称
            # 对于每个股票，获取其最细级行业（优先L3 > L2 > L1）
            query = f"""
            WITH ranked_industries AS (
                SELECT
                    b.symbol,
                    b.{metric},
                    sc.industry_name as l3_name,
                    sc.industry_code as l3_code,
                    sc.parent_code as l2_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY b.symbol
                        ORDER BY
                            CASE sc.level
                                WHEN 'L3' THEN 1
                                WHEN 'L2' THEN 2
                                WHEN 'L1' THEN 3
                                ELSE 4
                            END
                    ) as rn
                FROM bars b
                LEFT JOIN sw_members swm ON b.symbol = SUBSTR(swm.ts_code, 1, 6)
                    AND swm.in_date <= b.datetime
                    AND (swm.out_date IS NULL OR swm.out_date > b.datetime)
                LEFT JOIN sw_classify sc ON swm.index_code = sc.index_code
                WHERE b.interval = '1d'
                  AND b.datetime = :latest_date
                  AND b.{metric} IS NOT NULL
            )
            SELECT
                sc_l1.industry_name as sw_l1,
                sc_l2.industry_name as sw_l2,
                ri.l3_name as sw_l3,
                ri.{metric}
            FROM ranked_industries ri
            LEFT JOIN sw_classify sc_l2 ON ri.l2_code = sc_l2.industry_code
            LEFT JOIN sw_classify sc_l1 ON sc_l2.parent_code = sc_l1.industry_code
            WHERE ri.rn = 1
            """

            try:
                df = pd.read_sql_query(query, self.engine, params={'latest_date': latest_date})
            except Exception as e:
                print(f"Error loading metric {metric}: {e}")
                import traceback
                traceback.print_exc()
                continue

            if df.empty:
                print(f"  No data for metric {metric}")
                continue

            print(f"  Processing {metric}: {len(df)} records")

            # 为每个行业级别生成统计（L1, L2, L3都要保存）
            # L1: 一级行业（所有该一级行业下的股票）
            if 'sw_l1' in df.columns:
                for l1_name, group in df.groupby(['sw_l1']):
                    if pd.notna(l1_name):
                        values = group[metric].dropna()
                        if len(values) >= 5:
                            stats = {
                                'calculated_at': calculated_at,
                                'sw_l1': l1_name,
                                'sw_l2': None,
                                'sw_l3': None,
                                'metric_name': metric,
                                'p10': values.quantile(0.10),
                                'p25': values.quantile(0.25),
                                'p50': values.quantile(0.50),
                                'p75': values.quantile(0.75),
                                'p90': values.quantile(0.90),
                                'mean': values.mean(),
                                'std': values.std(),
                                'min': values.min(),
                                'max': values.max(),
                                'count': len(values)
                            }
                            results.append(stats)

            # L2: 二级行业（该二级行业下的股票）
            if 'sw_l2' in df.columns:
                for (l1_name, l2_name), group in df.groupby(['sw_l1', 'sw_l2'], dropna=False):
                    if pd.notna(l2_name):
                        values = group[metric].dropna()
                        if len(values) >= 5:
                            stats = {
                                'calculated_at': calculated_at,
                                'sw_l1': l1_name,
                                'sw_l2': l2_name,
                                'sw_l3': None,
                                'metric_name': metric,
                                'p10': values.quantile(0.10),
                                'p25': values.quantile(0.25),
                                'p50': values.quantile(0.50),
                                'p75': values.quantile(0.75),
                                'p90': values.quantile(0.90),
                                'mean': values.mean(),
                                'std': values.std(),
                                'min': values.min(),
                                'max': values.max(),
                                'count': len(values)
                            }
                            results.append(stats)

            # L3: 三级行业（最细粒度）
            if 'sw_l3' in df.columns:
                for (l1, l2, l3), group in df.groupby(['sw_l1', 'sw_l2', 'sw_l3'], dropna=False):
                    if pd.notna(l3):
                        values = group[metric].dropna()
                        if len(values) >= 5:
                            stats = {
                                'calculated_at': calculated_at,
                                'sw_l1': l1,
                                'sw_l2': l2,
                                'sw_l3': l3,
                                'metric_name': metric,
                                'p10': values.quantile(0.10),
                                'p25': values.quantile(0.25),
                                'p50': values.quantile(0.50),
                                'p75': values.quantile(0.75),
                                'p90': values.quantile(0.90),
                                'mean': values.mean(),
                                'std': values.std(),
                                'min': values.min(),
                                'max': values.max(),
                                'count': len(values)
                            }
                            results.append(stats)

        return pd.DataFrame(results)

    def save_industry_statistics(self, stats_df: pd.DataFrame) -> bool:
        """
        保存行业统计到数据库

        Args:
            stats_df: 行业统计DataFrame

        Returns:
            是否保存成功
        """
        try:
            stats_df.to_sql('industry_statistics', self.engine, if_exists='append', index=False)
            return True
        except Exception as e:
            print(f"Error saving industry statistics: {e}")
            return False

    def get_industry_percentile(self, sw_l1: str, metric_name: str,
                                 percentile: float = 0.75,
                                 sw_l2: str = None, sw_l3: str = None) -> Optional[float]:
        """
        获取指定行业、指标的百分位值（最新版本）

        Args:
            sw_l1: 申万一级行业
            metric_name: 指标名称
            percentile: 百分位（0.75 = 75th percentile）
            sw_l2: 申万二级行业（可选）
            sw_l3: 申万三级行业（可选）

        Returns:
            百分位值，如果不存在则返回None
        """
        percentile_column = f"p{int(percentile * 100)}"

        # 构建查询条件
        where_conditions = ["sw_l1 = :sw_l1", "metric_name = :metric_name"]
        params = {'sw_l1': sw_l1, 'metric_name': metric_name}

        if sw_l2 is not None:
            where_conditions.append("sw_l2 = :sw_l2")
            params['sw_l2'] = sw_l2

        if sw_l3 is not None:
            where_conditions.append("sw_l3 = :sw_l3")
            params['sw_l3'] = sw_l3

        where_clause = " AND ".join(where_conditions)

        query = f"""
        SELECT {percentile_column}
        FROM industry_statistics
        WHERE {where_clause}
        ORDER BY calculated_at DESC
        LIMIT 1
        """

        try:
            result = pd.read_sql_query(query, self.engine, params=params)

            if result.empty:
                return None

            return result.iloc[0][percentile_column]
        except Exception as e:
            print(f"Error getting industry percentile: {e}")
            return None

    def clear_old_statistics(self, keep_latest: int = 1) -> int:
        """
        清除旧的行业统计数据

        Args:
            keep_latest: 保留最新几个版本的统计

        Returns:
            删除的行数
        """
        try:
            # 获取所有不同的calculated_at版本
            query = "SELECT DISTINCT calculated_at FROM industry_statistics ORDER BY calculated_at DESC"
            versions = pd.read_sql_query(query, self.engine)

            if len(versions) <= keep_latest:
                return 0

            # 删除旧版本
            old_versions = versions.iloc[keep_latest:]['calculated_at'].tolist()

            delete_query = "DELETE FROM industry_statistics WHERE calculated_at IN :old_versions"
            with self.engine.connect() as conn:
                result = conn.execute(text(delete_query), {'old_versions': tuple(old_versions)})
                conn.commit()
                return result.rowcount
        except Exception as e:
            print(f"Error clearing old statistics: {e}")
            return 0
