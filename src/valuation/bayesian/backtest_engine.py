"""
贝叶斯回测引擎

对历史季报期运行估值，对比3个月后的10日均价，计算各方法的方向准确率。
使用10日均价代替单日价格，平滑短期波动噪音。
用于生成贝叶斯先验矩阵的历史准确率数据。

支持 L1/L2 两个粒度：
- L1 (31个大行业): 样本量充足，稳定性高
- L2 (~100个细分行业): 更精确，样本量适中（建议 min_stocks≥15）
"""

import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class BayesianBacktestEngine:
    """
    回测引擎：对历史季报期运行估值，对比3个月后的10日均价，计算方向准确率

    使用10日均价代替单日价格，避免单日波动噪音影响方向判断。

    数据来源：
    - bars_a_1d（duckdb）：股价、PE/PB/PS
    - income/balancesheet/cashflow（sqlite）：财务数据
    """

    def __init__(
        self,
        db_path: str = "data/tushare_data.db",
        duck_path: Optional[str] = None
    ):
        """
        初始化回测引擎

        Args:
            db_path: SQLite 数据库路径
            duck_path: DuckDB 数据库路径（None则使用默认配置）
        """
        self.db_path = db_path
        self.duck_path = duck_path

    def _get_sqlite_conn(self) -> sqlite3.Connection:
        """获取 SQLite 连接"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _get_industry_stocks(
        self,
        index_code: str,
        level: str = 'L1'
    ) -> List[str]:
        """
        从数据库获取行业内的全部股票代码列表

        Args:
            index_code: 行业指数代码（如 '801780.SI'，'801125.SI'）
            level: 行业层级 ('L1' 或 'L2')

        Returns:
            有价格数据的股票代码列表（如 ['601398.SH', ...]）
        """
        try:
            with self._get_sqlite_conn() as conn:
                cursor = conn.execute(
                    """
                    SELECT DISTINCT swm.ts_code
                    FROM sw_members swm
                    JOIN sw_classify sc ON swm.index_code = sc.index_code
                    WHERE swm.index_code = ?
                      AND sc.level = ?
                      AND (swm.out_date IS NULL OR swm.out_date > '2020-01-01')
                    ORDER BY swm.ts_code
                    """,
                    (index_code, level)
                )
                stocks = [row['ts_code'] for row in cursor.fetchall()]

                # 过滤出有收盘价数据的股票
                return self._filter_stocks_with_data(stocks)

        except Exception as e:
            logger.error(f"获取行业股票失败 ({index_code}): {e}")
            return []

    def _get_l2_industries(
        self,
        min_stocks: int = 15
    ) -> List[Tuple[str, str, str]]:
        """
        获取有足够股票数的L2行业列表

        Args:
            min_stocks: 最少股票数量（过滤掉样本太少的行业）

        Returns:
            [(index_code, industry_code, industry_name), ...] 列表
            例如：[('801125.SI', '340500', '白酒Ⅱ'), ...]
        """
        try:
            with self._get_sqlite_conn() as conn:
                cursor = conn.execute(
                    """
                    SELECT sc.index_code, sc.industry_code, sc.industry_name,
                           COUNT(DISTINCT swm.ts_code) as stock_count
                    FROM sw_classify sc
                    JOIN sw_members swm ON sc.index_code = swm.index_code
                    WHERE sc.level = 'L2'
                      AND (swm.out_date IS NULL OR swm.out_date > '2020-01-01')
                    GROUP BY sc.index_code, sc.industry_code, sc.industry_name
                    HAVING COUNT(DISTINCT swm.ts_code) >= ?
                    ORDER BY sc.industry_code
                    """,
                    (min_stocks,)
                )
                result = [
                    (row['index_code'], row['industry_code'], row['industry_name'])
                    for row in cursor.fetchall()
                ]
                logger.info(f"找到 {len(result)} 个有效L2行业（min_stocks={min_stocks}）")
                return result
        except Exception as e:
            logger.error(f"获取L2行业列表失败: {e}")
            return []

    def _filter_stocks_with_data(
        self,
        stocks: List[str],
    ) -> List[str]:
        """
        过滤出有价格数据的股票

        Args:
            stocks: 股票代码列表（如 '000001.SZ' 格式）

        Returns:
            有效股票列表（保留原始格式）
        """
        try:
            from src.db.duckdb_manager import get_duckdb_manager
            duckdb_manager = get_duckdb_manager(read_only=True)
            valid = []
            with duckdb_manager.get_connection() as conn:
                for ts_code in stocks:
                    # bars_a_1d 使用 6 位纯数字代码（去掉 .SH/.SZ 后缀）
                    code_6 = ts_code[:6] if '.' in ts_code else ts_code
                    try:
                        result = conn.execute(
                            "SELECT COUNT(*) FROM bars_a_1d WHERE stock_code = ?",
                            [code_6]
                        ).fetchone()
                        if result and result[0] > 0:
                            valid.append(ts_code)
                    except Exception:
                        pass
            return valid
        except Exception as e:
            logger.warning(f"过滤股票数据时出错: {e}，返回原始列表")
            return stocks

    def _get_price_on_date(
        self,
        ts_code: str,
        date: str
    ) -> Optional[float]:
        """
        从 DuckDB bars_a_1d 获取指定日期（或最近交易日）的收盘价

        Args:
            ts_code: 股票代码（如 '601398.SH' 或 '601398'）
            date: 日期字符串（YYYY-MM-DD）

        Returns:
            收盘价，若无数据则返回 None
        """
        try:
            from src.db.duckdb_manager import get_duckdb_manager
            duckdb_manager = get_duckdb_manager(read_only=True)
            # bars_a_1d 使用 6 位纯数字代码
            code_6 = ts_code[:6] if '.' in ts_code else ts_code
            with duckdb_manager.get_connection() as conn:
                result = conn.execute(
                    """
                    SELECT close FROM bars_a_1d
                    WHERE stock_code = ?
                      AND datetime >= ? AND datetime <= ?
                    ORDER BY ABS(datetime - ?::DATE)
                    LIMIT 1
                    """,
                    [code_6, date, _add_days(date, 10), date]
                ).fetchone()

                if result and result[0]:
                    return float(result[0])
                return None
        except Exception as e:
            logger.debug(f"获取价格失败 ({ts_code} @ {date}): {e}")
            return None

    def _get_avg_price_around_date(
        self,
        ts_code: str,
        date: str,
        window: int = 10
    ) -> Optional[float]:
        """
        获取指定日期前后的均价（平滑单日波动）

        取 date 前 window/2 天到后 window/2 天的交易日的收盘价均值。
        例如 window=10，取 date 前5天到后4天（含当天约10个交易日）

        Args:
            ts_code: 股票代码（如 '601398.SH' 或 '601398'）
            date: 日期字符串（YYYY-MM-DD）
            window: 均价窗口大小（交易日数），默认10

        Returns:
            均价，若有效数据少于 window/2 则返回 None
        """
        try:
            from src.db.duckdb_manager import get_duckdb_manager
            duckdb_manager = get_duckdb_manager(read_only=True)
            # bars_a_1d 使用 6 位纯数字代码
            code_6 = ts_code[:6] if '.' in ts_code else ts_code

            # 计算日期范围：前后各 window/2 + 2 天（预留缓冲）
            half_window = window // 2 + 2
            start_date = _add_days(date, -half_window)
            end_date = _add_days(date, half_window)

            with duckdb_manager.get_connection() as conn:
                result = conn.execute(
                    """
                    SELECT AVG(close) as avg_close, COUNT(*) as cnt
                    FROM bars_a_1d
                    WHERE stock_code = ?
                      AND datetime >= ? AND datetime <= ?
                      AND close > 0
                    """,
                    [code_6, start_date, end_date]
                ).fetchone()

                # 至少需要一半的数据点才算有效
                min_required = window // 2
                if result and result[0] and result[1] >= min_required:
                    return float(result[0])
                return None
        except Exception as e:
            logger.debug(f"获取均价失败 ({ts_code} @ {date}): {e}")
            return None

    def _run_valuation_for_stock(
        self,
        ts_code: str,
        eval_date: str
    ) -> Dict[str, Optional[float]]:
        """
        对单只股票在指定日期运行 PE/PB/PS/DCF 估值

        Args:
            ts_code: 股票代码
            eval_date: 估值日期

        Returns:
            {method: fair_value} 字典，估值失败时为 None
        """
        results = {}
        # method -> model_key 映射（model.name 格式）
        method_to_model_key = {
            'pe': 'Relative_PE',
            'pb': 'Relative_PB',
            'ps': 'Relative_PS',
            'dcf': 'DCF',
        }
        methods = list(method_to_model_key.keys())

        try:
            import sys
            import os
            # 确保路径正确
            project_root = os.path.abspath(
                os.path.join(os.path.dirname(__file__), '../../..')
            )
            if project_root not in sys.path:
                sys.path.insert(0, project_root)

            from src.valuation.engine.valuation_engine import ValuationEngine
            from src.valuation.models.relative_valuation import RelativeValuationModel

            engine = ValuationEngine(db_path=self.db_path)

            # 注册相对估值模型
            for m in ['pe', 'pb', 'ps', 'peg']:
                model = RelativeValuationModel(method=m)
                engine.register_model(model)

            # 注册 DCF 模型
            try:
                from src.valuation.models.absolute_valuation import DCFValuationModel
                dcf_model = DCFValuationModel()
                engine.register_model(dcf_model)
            except Exception:
                pass

            # 逐方法估值
            for method in methods:
                model_key = method_to_model_key[method]
                if model_key not in engine.models:
                    results[method] = None
                    continue
                try:
                    model = engine.models[model_key]
                    result = model.calculate(ts_code, eval_date)
                    if not result.get('error') and result.get('fair_value'):
                        fair_value = result['fair_value']
                        current_price_val = result.get('current_price')
                        # 排除明显异常（公允价值==当前价格，说明估值失败退化）
                        if (current_price_val and
                                abs(fair_value - current_price_val) > 1e-6 * current_price_val):
                            results[method] = fair_value
                        else:
                            results[method] = None
                    else:
                        results[method] = None
                except Exception as e:
                    logger.debug(f"  {method} 估值失败 ({ts_code} @ {eval_date}): {e}")
                    results[method] = None

        except Exception as e:
            logger.error(f"估值引擎初始化失败: {e}")
            for m in methods:
                results[m] = None

        return results

    def run_industry_backtest(
        self,
        industry_code: str,
        industry_name: str,
        years: int = 3,
        index_code: Optional[str] = None,
        level: str = 'L1'
    ) -> tuple:
        """
        对指定行业全量回测，返回各方法的历史方向准确率

        流程：
        1. 获取行业全部有数据的股票
        2. 生成评估日期：过去 years 年的每个季末
        3. 对每只股票×每个评估日期，运行估值
        4. 对比 91 天后的10日均价，计算方向准确率（使用均价平滑单日波动）
        5. 加权准确率：weight = 1 / (1 + error_rate)

        Args:
            industry_code: 申万行业代码（L1: '801780'，L2: '340500'）
            industry_name: 行业名称（如 '银行'，'白酒Ⅱ'）
            years: 回测年数
            index_code: 行业指数代码（如 '801780.SI'），None时从industry_code构造
            level: 行业层级 ('L1' 或 'L2')

        Returns:
            (accuracies, counts, stock_count) 元组
            - accuracies: {method: accuracy} 各方法加权准确率
            - counts: {method: sample_count} 各方法实际样本数
            - stock_count: 参与回测的股票数量
        """
        logger.info(
            f"开始回测行业 {industry_name}({industry_code}) [{level}], years={years}"
        )

        # 构建 index_code
        if index_code is None:
            index_code = f"{industry_code}.SI"

        # 获取行业全部有数据的股票
        stocks = self._get_industry_stocks(index_code, level)
        if not stocks:
            logger.warning(f"行业 {industry_name} 未找到股票，跳过")
            return {}, {}, 0

        logger.info(f"  行业 {industry_name} 找到 {len(stocks)} 只股票")

        # 生成评估日期
        eval_dates = _generate_quarter_ends(years)
        logger.info(f"  评估日期数量: {len(eval_dates)}, 范围: {eval_dates[0]} ~ {eval_dates[-1]}")

        # 汇总各方法的评估结果
        method_stats: Dict[str, Dict] = {
            'pe': {'weighted_correct': 0.0, 'total_weight': 0.0, 'count': 0},
            'pb': {'weighted_correct': 0.0, 'total_weight': 0.0, 'count': 0},
            'ps': {'weighted_correct': 0.0, 'total_weight': 0.0, 'count': 0},
            'dcf': {'weighted_correct': 0.0, 'total_weight': 0.0, 'count': 0},
        }

        total_evaluations = len(stocks) * len(eval_dates)
        completed = 0

        for ts_code in stocks:
            for eval_date in eval_dates:
                completed += 1
                if completed % 20 == 0:
                    logger.info(
                        f"  进度: {completed}/{total_evaluations} "
                        f"({100*completed//total_evaluations}%)"
                    )

                # 获取当前10日均价（平滑单日波动）
                current_price = self._get_avg_price_around_date(ts_code, eval_date, window=10)
                if not current_price or current_price <= 0:
                    continue

                # 获取 91 天后的10日均价
                future_date = _add_days(eval_date, 91)
                future_price = self._get_avg_price_around_date(ts_code, future_date, window=10)
                if not future_price or future_price <= 0:
                    continue

                # 实际涨跌方向
                actual_direction = 1 if future_price > current_price else -1

                # 运行各方法估值
                fair_values = self._run_valuation_for_stock(ts_code, eval_date)

                for method, fair_value in fair_values.items():
                    if fair_value is None or fair_value <= 0:
                        continue

                    # 预测方向（公允价值 > 当前价格 → 看涨）
                    predicted_direction = 1 if fair_value > current_price else -1

                    # 误差率（用于加权，误差越大权重越小）
                    error_rate = abs(fair_value - current_price) / current_price
                    weight = 1.0 / (1.0 + error_rate)

                    # 是否预测正确
                    is_correct = 1 if predicted_direction == actual_direction else 0

                    stats = method_stats[method]
                    stats['weighted_correct'] += is_correct * weight
                    stats['total_weight'] += weight
                    stats['count'] += 1

        # 计算各方法的加权准确率
        accuracies = {}
        counts = {}
        for method, stats in method_stats.items():
            if stats['total_weight'] > 0 and stats['count'] >= 5:
                accuracy = stats['weighted_correct'] / stats['total_weight']
                # 截断到合理范围
                accuracy = max(0.3, min(accuracy, 0.9))
                accuracies[method] = round(accuracy, 4)
                counts[method] = stats['count']
                logger.info(
                    f"  {method}: accuracy={accuracy:.3f}, "
                    f"count={stats['count']}, weight={stats['total_weight']:.1f}"
                )
            else:
                logger.info(
                    f"  {method}: 样本不足 (count={stats['count']})，跳过"
                )

        return accuracies, counts, len(stocks)

    def run_all_industries(
        self,
        years: int = 3,
        industry_codes: Optional[List[str]] = None,
        progress_callback=None
    ) -> Dict[str, Dict[str, float]]:
        """
        对所有行业运行全量回测

        Args:
            years: 回测年数
            industry_codes: 指定行业代码列表，None 表示全部
            progress_callback: 进度回调函数

        Returns:
            {industry_code: {method: accuracy}} 字典
        """
        from src.valuation.config.industry_params import INDUSTRY_PARAMS

        if industry_codes:
            industries = {
                code: params
                for code, params in INDUSTRY_PARAMS.items()
                if code in industry_codes
            }
        else:
            industries = INDUSTRY_PARAMS

        all_results = {}
        total = len(industries)
        for i, (ind_code, ind_params) in enumerate(industries.items()):
            ind_name = ind_params['name']
            if progress_callback:
                progress_callback(f"回测行业 {i+1}/{total}: {ind_name}({ind_code})")

            try:
                result, _, _ = self.run_industry_backtest(
                    ind_code, ind_name, years=years
                )
                all_results[ind_code] = result
            except Exception as e:
                logger.error(f"回测行业 {ind_name} 失败: {e}")
                all_results[ind_code] = {}

        return all_results

    def run_all_l2_industries(
        self,
        years: int = 3,
        top_n: int = 15,
        min_stocks: int = 15,
        progress_callback=None
    ) -> Dict[str, Dict[str, float]]:
        """
        对所有有效L2行业运行回测

        仅处理股票数量 >= min_stocks 的L2行业，确保样本充足。

        Args:
            years: 回测年数
            min_stocks: L2行业最少股票数量（过滤小行业）
            progress_callback: 进度回调函数（接收消息字符串）

        Returns:
            {industry_code: {method: accuracy}} 字典
            industry_code 为 L2 的 industry_code（如 '340500'）
        """
        industries = self._get_l2_industries(min_stocks=min_stocks)
        if not industries:
            logger.warning("未找到满足条件的L2行业")
            return {}

        total = len(industries)
        logger.info(f"开始L2行业全量回测，共 {total} 个行业，years={years}")

        all_results = {}
        for i, (idx_code, ind_code, ind_name) in enumerate(industries):
            if progress_callback:
                progress_callback(
                    f"[L2] 回测行业 {i+1}/{total}: {ind_name}({ind_code})"
                )

            try:
                result, _, _ = self.run_industry_backtest(
                    industry_code=ind_code,
                    industry_name=ind_name,
                    years=years,
                    index_code=idx_code,
                    level='L2'
                )
                all_results[ind_code] = result
            except Exception as e:
                logger.error(f"回测L2行业 {ind_name}({ind_code}) 失败: {e}")
                all_results[ind_code] = {}

        return all_results


def _add_days(date_str: str, days: int) -> str:
    """
    给日期字符串加上指定天数

    Args:
        date_str: 日期字符串 YYYY-MM-DD
        days: 天数

    Returns:
        新日期字符串
    """
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        new_dt = dt + timedelta(days=days)
        return new_dt.strftime('%Y-%m-%d')
    except Exception:
        return date_str


def _generate_quarter_ends(years: int) -> List[str]:
    """
    生成过去 years 年的季末日期列表

    Args:
        years: 年数

    Returns:
        季末日期列表（YYYY-MM-DD）
    """
    today = datetime.now()
    dates = []
    quarter_months = [3, 6, 9, 12]

    for year_offset in range(years + 1):
        year = today.year - year_offset
        for month in quarter_months:
            if month == 3:
                day = 31
            elif month == 6:
                day = 30
            elif month == 9:
                day = 30
            else:
                day = 31

            date_str = f"{year}-{month:02d}-{day:02d}"
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
                # 只取过去的日期（已过季末 + 30天确保财报可用）
                if dt + timedelta(days=30) < today:
                    dates.append(date_str)
            except ValueError:
                pass

    dates.sort()
    return dates
