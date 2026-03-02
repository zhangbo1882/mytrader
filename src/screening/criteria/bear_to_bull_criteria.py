"""
熊牛交替信号筛选条件

筛选当日为牛市，且之前连续N个交易日持续为熊市或震荡的股票
"""
import logging
import time
import pandas as pd
import duckdb
from typing import Dict, List
from src.screening.base_criteria import BaseCriteria

logger = logging.getLogger(__name__)


class BearToBullTransitionCriteria(BaseCriteria):
    """
    熊牛交替信号筛选条件

    筛选当日为牛市（bull），且之前连续 N 个交易日一直为熊市（bear）或震荡（neutral）的股票
    """

    def __init__(self, period: int = 10, cycle: str = 'medium'):
        """
        Args:
            period: 之前需要检查的天数（默认10）
            cycle: 判断用的周期配置 ('short', 'medium', 'long')
        """
        self.period = period
        self.cycle = cycle

        try:
            from config.settings import DUCKDB_PATH
            self.duckdb_path = str(DUCKDB_PATH)
        except Exception:
            self.duckdb_path = None

    @property
    def cost(self) -> int:
        return 20

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        应用筛选条件

        策略：从 df 提取股票列表，查询 DuckDB 历史 OHLCV 数据，
        对每支股票计算牛熊历史，筛选满足熊牛交替条件的股票。
        """
        logger.debug(
            f"[BearToBullTransitionCriteria] Starting filter, "
            f"period: {self.period}, cycle: {self.cycle}, input size: {len(df)}"
        )

        if df.empty:
            return df

        if 'symbol' not in df.columns:
            logger.warning("[BearToBullTransitionCriteria] 'symbol' column not found in DataFrame")
            return df

        if not self.duckdb_path:
            logger.warning("[BearToBullTransitionCriteria] DuckDB path not configured")
            return df

        symbols = df['symbol'].unique().tolist()
        if not symbols:
            return pd.DataFrame()

        # 从 df 获取 trade_date（用于限制查询范围）
        trade_date = df.iloc[0].get('trade_date') if 'trade_date' in df.columns else None
        trade_date_str = self.format_date_for_db(trade_date) if trade_date else None

        # 计算总共需要的历史天数：lookback_days（指标计算所需）+ period + 5（余量）
        from web.services.market_regime_service import CYCLE_CONFIGS
        cycle_cfg = CYCLE_CONFIGS.get(self.cycle, CYCLE_CONFIGS['medium'])
        lookback_days = cycle_cfg['lookback_days']
        total_days = lookback_days + self.period + 5

        # 区分A股和港股
        if 'data_source' in df.columns:
            data_sources = df.groupby('symbol')['data_source'].first().to_dict()
            a_symbols = [s for s in symbols if data_sources.get(s) != '港股']
            hk_symbols = [s for s in symbols if data_sources.get(s) == '港股']
        else:
            a_symbols = symbols
            hk_symbols = []

        # 带重试的 DuckDB 连接
        conn = None
        max_retries = 5
        retry_delay = 0.3
        last_error = None

        for attempt in range(max_retries):
            try:
                conn = duckdb.connect(self.duckdb_path, read_only=True)
                break
            except Exception as e:
                last_error = e
                if "lock" in str(e).lower() or "IO Error" in str(e):
                    if attempt < max_retries - 1:
                        logger.debug(
                            f"[BearToBullTransitionCriteria] DuckDB locked, "
                            f"retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(retry_delay)
                else:
                    logger.error(f"[BearToBullTransitionCriteria] Failed to connect to DuckDB: {e}")
                    break

        if conn is None:
            logger.error(
                f"[BearToBullTransitionCriteria] Failed to connect to DuckDB "
                f"after {max_retries} attempts: {last_error}"
            )
            return df

        try:
            all_qualified = []

            if a_symbols:
                qualified = self._process_symbols(conn, a_symbols, 'bars_a_1d', trade_date_str, total_days)
                all_qualified.extend(qualified)
                logger.debug(
                    f"[BearToBullTransitionCriteria] A股: {len(a_symbols)} symbols, {len(qualified)} qualified"
                )

            if hk_symbols:
                qualified = self._process_symbols(conn, hk_symbols, 'bars_1d', trade_date_str, total_days)
                all_qualified.extend(qualified)
                logger.debug(
                    f"[BearToBullTransitionCriteria] 港股: {len(hk_symbols)} symbols, {len(qualified)} qualified"
                )

            if not all_qualified:
                logger.info("[BearToBullTransitionCriteria] No symbols meet the bear-to-bull condition")
                return pd.DataFrame()

            result = df[df['symbol'].isin(all_qualified)].copy()
            logger.info(
                f"[BearToBullTransitionCriteria] Filter completed, "
                f"input: {len(df)}, qualified: {len(all_qualified)}, output: {len(result)}"
            )
            return result

        finally:
            conn.close()

    def _process_symbols(
        self, conn, symbols: List[str], table_name: str,
        trade_date_str: str, total_days: int
    ) -> List[str]:
        """查询历史数据并计算符合熊牛交替条件的股票列表"""
        if not symbols:
            return []

        symbols_placeholder = ', '.join([f"'{s}'" for s in symbols])
        date_filter = f"AND datetime <= '{trade_date_str}'" if trade_date_str else ""

        query = f"""
        SELECT stock_code, datetime, open, high, low, close, volume
        FROM {table_name}
        WHERE stock_code IN ({symbols_placeholder})
          {date_filter}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY datetime DESC) <= {total_days}
        ORDER BY stock_code, datetime
        """

        try:
            result_df = conn.execute(query).fetchdf()
        except Exception as e:
            logger.error(f"[BearToBullTransitionCriteria] DuckDB query error: {e}")
            return []

        if result_df.empty:
            return []

        from web.services.market_regime_service import MarketRegimeService

        qualified = []
        for symbol, group_df in result_df.groupby('stock_code'):
            try:
                data = group_df[['datetime', 'open', 'high', 'low', 'close', 'volume']].to_dict('records')
                service = MarketRegimeService(cycle=self.cycle)
                regime_history = service.calculate_regime_history(data)

                # 需要足够的历史 regime 记录：当日 + 前 period 天
                if len(regime_history) < self.period + 1:
                    continue

                # 最后一条为当日，须为牛市
                if regime_history[-1]['regime'] != 'bull':
                    continue

                # 前 period 条记录全部为 bear 或 neutral
                prev_regimes = regime_history[-self.period - 1:-1]
                if len(prev_regimes) < self.period:
                    continue

                if all(r['regime'] in ('bear', 'neutral') for r in prev_regimes):
                    qualified.append(symbol)

            except Exception as e:
                logger.error(f"[BearToBullTransitionCriteria] Error processing symbol {symbol}: {e}")
                continue

        return qualified

    def to_config(self) -> Dict:
        return {
            'type': 'BearToBull',
            'period': self.period,
            'cycle': self.cycle,
        }

    @classmethod
    def from_config(cls, config: Dict):
        return cls(
            period=config.get('period', 10),
            cycle=config.get('cycle', 'medium'),
        )
