# tushare.py
import tushare as ts
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import os
import time
import json
import logging
from pathlib import Path
from typing import Optional, Tuple
from src.data_sources.base import BaseStockDB

logger = logging.getLogger(__name__)


class TushareDB(BaseStockDB):
    def __init__(self, token: str, db_path: str = "data/tushare_data.db"):
        """
        初始化 Tushare 数据库

        Args:
            token: Tushare API token
            db_path: 数据库文件路径
        """
        # 调用父类初始化
        super().__init__(db_path)

        # 初始化 Tushare API
        ts.set_token(token)
        self.pro = ts.pro_api()

        # API调用速率限制追踪
        self._api_call_times = []  # 记录最近API调用的时间
        self._rate_limit_delay = 1.3  # 每次API调用的最小间隔（秒），保守设置为1.3秒
        self._max_calls_per_minute = 50  # 每分钟最大调用次数

        # 最近一次现金流回填统计
        self._last_cashflow_backfill_stats = None

    def _standardize_code(self, symbol: str) -> str:
        """
        标准化股票代码格式
        输入: 600382 或 600382.SH 或 00941
        输出: 600382.SH 或 00941.HK
        """
        # Ensure symbol is a string
        if not isinstance(symbol, str):
            symbol = str(symbol)

        if "." in symbol:
            return symbol.upper()

        # 自动判断交易所
        if symbol.startswith(("600", "601", "603", "604", "605", "688", "689")):
            return f"{symbol}.SH"  # 上交所
        elif symbol.startswith(("000", "001", "002", "003", "300", "301")):
            # 检查是否是港股（4-5位且不在A股范围内）
            if len(symbol) in [4, 5] and not symbol.startswith(("300", "301")):
                return f"{symbol}.HK"  # 港股
            return f"{symbol}.SZ"  # 深交所
        elif len(symbol) in [4, 5]:
            # 4-5位数字代码，判断为港股
            return f"{symbol}.HK"  # 港股
        else:
            raise ValueError(f"无法识别股票代码: {symbol}")

    def _detect_exchange(self, symbol: str) -> str:
        """自动识别交易所"""
        # 如果包含交易所后缀，直接使用
        if "." in symbol:
            suffix = symbol.split(".")[1].upper()
            if suffix == "SH":
                return "SSE"
            elif suffix == "SZ":
                return "SZSE"
            elif suffix == "HK":
                return "HKEX"

        # 否则根据代码前缀判断
        code = symbol.split(".")[0] if "." in symbol else symbol
        if code.startswith(("600", "601", "603", "604", "605", "688", "689")):
            return "SSE"
        elif code.startswith(("000", "001", "002", "003", "300", "301")):
            # 检查是否是港股
            if len(code) in [4, 5] and not code.startswith(("300", "301")):
                return "HKEX"
            return "SZSE"
        elif len(code) in [4, 5]:
            return "HKEX"
        else:
            return "UNKNOWN"

    def _wait_for_rate_limit(self):
        """
        确保不超过API频率限制（每分钟50次）

        计算逻辑：
        - 如果最近50次调用都在1分钟内，需要等待
        - 每次调用间隔至少1.3秒（保守值，60/50=1.2秒）
        """
        if self._api_call_times:
            # 获取最后一次调用时间
            last_call_time = self._api_call_times[-1]
            time_since_last_call = (datetime.now() - last_call_time).total_seconds()

            # 如果距离上次调用时间不足最小间隔，等待
            if time_since_last_call < self._rate_limit_delay:
                wait_time = self._rate_limit_delay - time_since_last_call
                time.sleep(wait_time)

        # 记录本次调用时间
        self._api_call_times.append(datetime.now())

        # 清理超过1分钟的旧记录（保留最近1分钟的记录即可）
        one_minute_ago = datetime.now() - timedelta(minutes=1)
        self._api_call_times = [t for t in self._api_call_times if t > one_minute_ago]

        # 额外检查：如果最近1分钟内已经有50次调用，等待到下一次可用时间
        if len(self._api_call_times) >= self._max_calls_per_minute:
            # 等待到最早的调用时间超过1分钟
            oldest_call = self._api_call_times[0]
            wait_until = oldest_call + timedelta(minutes=1)
            wait_seconds = (wait_until - datetime.now()).total_seconds()
            if wait_seconds > 0:
                print(f"  ⏸️  API频率限制，等待 {wait_seconds:.1f} 秒...")
                time.sleep(wait_seconds)
                # 清空记录，重新开始计数
                self._api_call_times = []

    def _retry_api_call(self, func, *args, max_retries=3, **kwargs):
        """
        带重试机制和速率限制的API调用

        Args:
            func: 要调用的函数
            max_retries: 最大重试次数

        Returns:
            函数返回值，失败返回None
        """
        for attempt in range(max_retries):
            try:
                # 等待以满足速率限制
                self._wait_for_rate_limit()

                # 执行API调用
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2**attempt  # 指数退避：1秒、2秒、4秒
                    print(
                        f"  ⚠️  第 {attempt + 1} 次调用失败: {e}，{wait_time}秒后重试..."
                    )
                    time.sleep(wait_time)
                else:
                    print(f"  ❌ 重试 {max_retries} 次后仍然失败")
                    return None

    def save_daily(
        self,
        symbol: str,
        start_date: str = "20200101",
        end_date: str = None,
        adjust: str = None,
    ):
        """
        保存 A 股日线数据（先检查本地数据库，避免重复调用API）

        Args:
            symbol: 股票代码，可以是 600382 或 600382.SH
            start_date: 开始日期，格式 YYYYMMDD
            end_date: 结束日期，格式 YYYYMMDD，None则使用配置文件默认值
            adjust: 复权类型，qfq=前复权, hfq=后复权, ''=不复权。None则使用配置文件默认值
        """
        # 如果未指定复权类型，从配置文件读取
        if adjust is None:
            from config.settings import DEFAULT_ADJUST

            adjust = DEFAULT_ADJUST

        # 如果未指定结束日期，使用当前日期
        if end_date is None:
            end_date = datetime.today().strftime("%Y%m%d")

        # 第零步：使用基类方法检查是否应该跳过下载
        should_skip, reason = self.should_skip_download(symbol, start_date, end_date)
        if should_skip:
            print(f"⏭️  {symbol} {reason}")
            return
        else:
            print(f"📥 {symbol} {reason}，开始下载...")

        # 标准化代码
        ts_code = self._standardize_code(symbol)

        # 第一步：获取数据（始终获取不复权数据 + 复权因子）
        try:
            # 获取日线数据（不复权，获取所有字段）- 使用重试机制
            df = self._retry_api_call(
                self.pro.daily,
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
            )

            if df is None or df.empty:
                print(f"⚠️ {symbol} 无数据")
                return

            # 保存不复权价格
            df["open_orig"] = df["open"]
            df["high_orig"] = df["high"]
            df["low_orig"] = df["low"]
            df["close_orig"] = df["close"]

            # 获取复权因子并计算前复权价格
            try:
                adj_df = self.pro.adj_factor(
                    ts_code=ts_code, start_date=start_date, end_date=end_date
                )
                df = df.merge(adj_df, on=["ts_code", "trade_date"], how="left")

                # 计算前复权价格
                df["open_qfq"] = df["open"] * df["adj_factor"]
                df["high_qfq"] = df["high"] * df["adj_factor"]
                df["low_qfq"] = df["low"] * df["adj_factor"]
                df["close_qfq"] = df["close"] * df["adj_factor"]
            except:
                # 如果获取复权因子失败，前复权价格为 None
                print(f"  ⚠️  无法获取复权因子，前复权价格将为空")
                df["open_qfq"] = None
                df["high_qfq"] = None
                df["low_qfq"] = None
                df["close_qfq"] = None

            # 根据配置决定使用哪种价格作为主价格（兼容旧代码）
            if adjust == "qfq":
                df["open"] = df["open_qfq"]
                df["high"] = df["high_qfq"]
                df["low"] = df["low_qfq"]
                df["close"] = df["close_qfq"]
            elif adjust == "hfq":
                # 后复权 = 当前价 / 复权因子
                df["open"] = df["open"] / df["adj_factor"]
                df["high"] = df["high"] / df["adj_factor"]
                df["low"] = df["low"] / df["adj_factor"]
                df["close"] = df["close"] / df["adj_factor"]
            else:
                # 不复权，使用原始价格
                df["open"] = df["open_orig"]
                df["high"] = df["high_orig"]
                df["low"] = df["low_orig"]
                df["close"] = df["close_orig"]

            # 获取每日基本面指标（daily_basic），如果无权限则跳过
            basic_data_available = False
            try:
                basic = self._retry_api_call(
                    self.pro.daily_basic,
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields="ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv",
                )

                if basic is not None and not basic.empty:
                    # 合并所有 daily_basic 字段
                    df = df.merge(basic, on=["ts_code", "trade_date"], how="left")
                    basic_data_available = True
                    print(f"  ✓ 获取到 daily_basic 数据 {len(basic)} 条")
                else:
                    print(
                        f"  ⚠️  daily_basic 数据暂未生成（API更新延迟），稍后可重试更新换手率"
                    )
                    # 设置所有新字段为 None
                    for field in [
                        "turnover_rate_f",
                        "volume_ratio",
                        "pe",
                        "pe_ttm",
                        "pb",
                        "ps",
                        "ps_ttm",
                        "dv_ratio",
                        "dv_ttm",
                        "total_share",
                        "float_share",
                        "free_share",
                        "total_mv",
                        "circ_mv",
                    ]:
                        df[field] = None

            except Exception as e:
                # 优雅处理权限错误
                if "无权限" in str(e) or "权限" in str(e) or "403" in str(e):
                    print(f"  ⚠️  无权限获取 daily_basic 数据（需要2000+积分）")
                else:
                    print(f"  ⚠️  获取 daily_basic 数据失败: {e}")

                # 设置所有字段为 None
                for field in [
                    "turnover",
                    "turnover_rate_f",
                    "volume_ratio",
                    "pe",
                    "pe_ttm",
                    "pb",
                    "ps",
                    "ps_ttm",
                    "dv_ratio",
                    "dv_ttm",
                    "total_share",
                    "float_share",
                    "free_share",
                    "total_mv",
                    "circ_mv",
                ]:
                    df[field] = None

            # 重命名列
            df = df.rename(
                columns={
                    "trade_date": "datetime",
                    "vol": "volume",
                    "turnover_rate": "turnover",
                }
            )

            # 添加元数据
            df["symbol"] = ts_code.split(".")[0]
            df["exchange"] = self._detect_exchange(ts_code)
            df["interval"] = "1d"
            df["datetime"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d")

            # 添加 amount 列（如果不存在）
            if "amount" not in df.columns:
                df["amount"] = None

            # 选择要保存的列（包含所有 Tushare daily 字段 + 前复权价格 + daily_basic 指标）
            # Tushare daily 字段：ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
            columns = [
                "symbol",
                "exchange",
                "interval",
                "datetime",
                "open",
                "high",
                "low",
                "close",  # 不复权价格（主价格列）
                "open_qfq",
                "high_qfq",
                "low_qfq",
                "close_qfq",  # 前复权价格
                "pre_close",
                "change",
                "pct_chg",  # Tushare 额外字段
                "volume",
                "turnover",
                "amount",
                # Daily basic 指标
                "turnover_rate_f",
                "volume_ratio",
                "pe",
                "pe_ttm",
                "pb",
                "ps",
                "ps_ttm",
                "total_mv",
                "circ_mv",
                "total_share",
                "float_share",
                "free_share",
                "dv_ratio",
                "dv_ttm",
            ]

            # 确保所有列都存在（某些字段可能在旧数据中不存在）
            for col in columns:
                if col not in df.columns:
                    df[col] = None

        except Exception as e:
            # 数据获取失败
            print(f"❌ {symbol} 下载失败: {e}")
            return

        # 第二步：保存到数据库
        try:
            df[columns].to_sql(
                "bars", self.engine, if_exists="append", index=False, method="multi"
            )
            print(f"✅ 已保存 {symbol} 共 {len(df)} 条记录")
        except Exception as e:
            # 数据库操作失败（比如重复数据），不显示为"下载失败"
            if "UNIQUE constraint" in str(e) or "duplicate" in str(e).lower():
                # 数据已存在，跳过
                print(f"⏭️  {symbol} 数据已存在，跳过")
            else:
                # 其他数据库错误
                print(f"⚠️  {symbol} 数据库操作失败: {e}")

    def save_multiple_stocks(
        self,
        symbols: list,
        start_date: str = "20200101",
        end_date: str = None,
        adjust: str = None,
    ):
        """
        批量保存多只股票数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            adjust: 复权类型，None则使用配置文件默认值
        """
        # 如果未指定复权类型，从配置文件读取
        if adjust is None:
            from config.settings import DEFAULT_ADJUST

            adjust = DEFAULT_ADJUST

        for symbol in symbols:
            self.save_daily(symbol, start_date, end_date, adjust)

    def update_turnover_only(
        self, symbols: list = None, start_date: str = None, end_date: str = None
    ):
        """
        单独更新换手率等基本面数据（用于补充之前未获取到的 daily_basic 数据）

        Args:
            symbols: 股票代码列表，None 则更新全部
            start_date: 开始日期，None 则使用最近7天
            end_date: 结束日期，None 则使用当前日期

        Returns:
            更新的记录数
        """
        from datetime import timedelta
        import pandas as pd

        # 如果未指定结束日期，使用当前日期
        if end_date is None:
            end_date = datetime.today().strftime("%Y%m%d")

        # 如果未指定开始日期，使用最近7天
        if start_date is None:
            start_date = (datetime.today() - timedelta(days=7)).strftime("%Y%m%d")

        # 获取股票列表
        if symbols is None:
            # 从DuckDB获取所有A股股票代码
            from src.db.duckdb_manager import get_duckdb_manager

            duckdb_manager = get_duckdb_manager()
            with duckdb_manager.get_connection() as conn:
                # 检查A股表是否存在
                if duckdb_manager.table_exists("bars_a_1d"):
                    df = conn.execute(
                        "SELECT DISTINCT stock_code as symbol FROM bars_a_1d"
                    ).fetchdf()
                    symbols = df["symbol"].tolist() if not df.empty else []
                else:
                    # 回退到SQLite（兼容旧逻辑）
                    query = """
                    SELECT DISTINCT symbol FROM bars
                    WHERE interval = '1d'
                    """
                    with self.engine.connect() as conn:
                        df = pd.read_sql_query(query, conn)
                    symbols = df["symbol"].tolist() if not df.empty else []

        if not symbols:
            print("❌ 没有找到股票")
            return 0

        print(f"📊 开始更新换手率数据（{start_date} - {end_date}）")
        print(f"📋 共 {len(symbols)} 只股票")

        updated_count = 0
        skipped_count = 0

        for symbol in symbols:
            try:
                ts_code = self._standardize_code(symbol)

                # 获取 daily_basic 数据
                basic = self._retry_api_call(
                    self.pro.daily_basic,
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields="ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv",
                )

                if basic is None or basic.empty:
                    skipped_count += 1
                    continue

                # 准备更新数据
                basic = basic.rename(columns={"trade_date": "datetime"})
                basic["datetime"] = pd.to_datetime(basic["datetime"]).dt.strftime(
                    "%Y-%m-%d"
                )
                basic["symbol"] = symbol
                basic["turnover"] = basic["turnover_rate"]

                # 只更新 turnover 等字段为 NULL 的记录
                # 使用 SQL UPDATE 语句逐条更新
                with self.engine.connect() as conn:
                    for _, row in basic.iterrows():
                        update_sql = """
                        UPDATE bars
                        SET turnover = :turnover,
                            turnover_rate_f = :turnover_rate_f,
                            volume_ratio = :volume_ratio,
                            pe = :pe,
                            pe_ttm = :pe_ttm,
                            pb = :pb,
                            ps = :ps,
                            ps_ttm = :ps_ttm,
                            total_mv = :total_mv,
                            circ_mv = :circ_mv,
                            total_share = :total_share,
                            float_share = :float_share,
                            free_share = :free_share,
                            dv_ratio = :dv_ratio,
                            dv_ttm = :dv_ttm
                        WHERE symbol = :symbol
                          AND datetime = :datetime
                          AND turnover IS NULL
                        """
                        result = conn.execute(
                            text(update_sql),
                            {
                                "turnover": row.get("turnover"),
                                "turnover_rate_f": row.get("turnover_rate_f"),
                                "volume_ratio": row.get("volume_ratio"),
                                "pe": row.get("pe"),
                                "pe_ttm": row.get("pe_ttm"),
                                "pb": row.get("pb"),
                                "ps": row.get("ps"),
                                "ps_ttm": row.get("ps_ttm"),
                                "total_mv": row.get("total_mv"),
                                "circ_mv": row.get("circ_mv"),
                                "total_share": row.get("total_share"),
                                "float_share": row.get("float_share"),
                                "free_share": row.get("free_share"),
                                "dv_ratio": row.get("dv_ratio"),
                                "dv_ttm": row.get("dv_ttm"),
                                "symbol": row["symbol"],
                                "datetime": row["datetime"],
                            },
                        )
                        if result.rowcount > 0:
                            updated_count += 1
                    conn.commit()

                print(f"  ✓ {symbol} 更新了 {len(basic)} 条记录")

            except Exception as e:
                print(f"  ❌ {symbol} 更新失败: {e}")

        print(f"\n{'=' * 60}")
        print(f"换手率更新完成:")
        print(f"  成功: {updated_count} 条记录")
        print(f"  跳过: {skipped_count} 只股票")
        print(f"{'=' * 60}")

        return updated_count

    def _get_stock_name_from_api(self, symbol: str) -> str:
        """
        从 Tushare API 获取股票名称

        Args:
            symbol: 股票代码

        Returns:
            股票名称，失败则返回 None
        """
        try:
            ts_code = self._standardize_code(symbol)
            basic = self.pro.stock_basic(ts_code=ts_code, fields="ts_code,name")
            if not basic.empty:
                return basic["name"].values[0]
        except:
            pass
        return None

    def get_stock_list(self, exchange: str = None) -> pd.DataFrame:
        """
        获取股票列表

        Args:
            exchange: 交易所 SSE/SZSE，None 表示全部
        """
        try:
            df = self.pro.stock_list(exchange=exchange)
            return df
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return pd.DataFrame()

    def check_permissions(self):
        """检查当前 token 的权限和积分"""
        try:
            print("=" * 60)
            print("Tushare 接口权限测试:")
            print("=" * 60)

            # 测试各个接口
            print("\n接口权限测试:")

            # 测试股票列表接口
            try:
                stocks = self.pro.stock_list(exchange="SSE")
                print(f"  ✅ stock_list - 可用")
            except Exception as e:
                print(f"  ❌ stock_list - 无权限")

            # 测试日线数据接口
            try:
                df = self.pro.daily(
                    ts_code="000001.SZ", start_date="20250101", end_date="20250102"
                )
                print(f"  ✅ daily - 可用")
            except Exception as e:
                print(f"  ❌ daily - 无权限")

            # 测试股票基本信息接口（stock_basic）
            try:
                df = self.pro.stock_basic(
                    exchange="", list_status="L", fields="ts_code,symbol,name", limit=1
                )
                print(f"  ✅ stock_basic - 可用（获取股票名称）")
            except Exception as e:
                print(f"  ❌ stock_basic - 无权限（无法获取股票名称）")

            # 测试日线基本信息接口
            try:
                df = self.pro.daily_basic(
                    ts_code="000001.SZ", start_date="20250101", end_date="20250102"
                )
                print(f"  ✅ daily_basic - 可用")
            except Exception as e:
                print(f"  ❌ daily_basic - 无权限")

            # 测试复权因子接口
            try:
                df = self.pro.adj_factor(
                    ts_code="000001.SZ", start_date="20250101", end_date="20250102"
                )
                print(f"  ✅ adj_factor - 可用")
            except Exception as e:
                print(f"  ❌ adj_factor - 无权限")

            print("\n提示:")
            print("  - 如果显示无权限，需要升级 Tushare 积分")
            print("  - 日线数据通常需要 2000+ 积分")
            print("  - 访问 https://tushare.pro 查看积分规则")
            print("=" * 60)

        except Exception as e:
            print(f"❌ 获取用户信息失败: {e}")

    def _save_checkpoint(self, checkpoint_path: str, data: dict):
        """
        保存下载进度检查点

        Args:
            checkpoint_path: 检查点文件路径
            data: 要保存的数据
        """
        try:
            with open(checkpoint_path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"  ⚠️  保存检查点失败: {e}")

    def _load_checkpoint(self, checkpoint_path: str) -> dict:
        """
        加载下载进度检查点

        Args:
            checkpoint_path: 检查点文件路径

        Returns:
            检查点数据，如果不存在则返回空字典
        """
        try:
            if Path(checkpoint_path).exists():
                with open(checkpoint_path, "r") as f:
                    return json.load(f)
        except Exception as e:
            print(f"  ⚠️  加载检查点失败: {e}")
        return {}

    def save_all_stocks_by_code(
        self,
        start_date: str = "20240101",
        end_date: str = None,
        adjust: str = None,
        checkpoint_path: str = None,
        resume: bool = True,
    ):
        """
        按股票代码循环获取全部A股数据

        Args:
            start_date: 开始日期
            end_date: 结束日期
            adjust: 复权类型
            checkpoint_path: 检查点文件路径
            resume: 是否从检查点恢复

        Returns:
            统计信息字典
        """
        # 设置默认检查点路径
        if checkpoint_path is None:
            try:
                from config.settings import CHECKPOINT_FILE

                checkpoint_path = str(CHECKPOINT_FILE)
            except ImportError:
                checkpoint_path = "data/download_checkpoint.json"

        # 1. 获取全部A股列表
        print("📋 正在获取股票列表...")
        try:
            stock_list = self._retry_api_call(
                self.pro.stock_basic,
                exchange="",
                list_status="L",  # 只获取上市股票
                fields="ts_code,name,area,industry,list_date",
            )
            if stock_list is None or stock_list.empty:
                print("❌ 获取股票列表失败")
                return None
            all_stocks = stock_list["ts_code"].tolist()
            print(f"📋 共 {len(all_stocks)} 只股票")
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return None

        # 2. 尝试从检查点恢复
        start_index = 0
        stats = {"success": 0, "failed": 0, "skipped": 0, "total": len(all_stocks)}

        if resume:
            checkpoint = self._load_checkpoint(checkpoint_path)
            if checkpoint:
                # 验证检查点是否匹配当前下载任务
                if (
                    checkpoint.get("start_date") == start_date
                    and checkpoint.get("end_date") == end_date
                    and checkpoint.get("adjust") == adjust
                    and checkpoint.get("total") == len(all_stocks)
                ):
                    last_index = checkpoint.get("last_index", 0)
                    start_index = last_index + 1
                    stats = checkpoint.get("stats", stats)
                    print(f"🔄 从检查点恢复: 第 {start_index + 1} 只股票开始")
                else:
                    print("⚠️  检查点参数不匹配，从头开始下载")
                    Path(checkpoint_path).unlink(missing_ok=True)

        # 3. 遍历每只股票
        for i in range(start_index, len(all_stocks)):
            ts_code = all_stocks[i]

            # 定期显示进度（每50只股票）
            if (i + 1) % 50 == 1 or i == len(all_stocks) - 1:
                print(f"\n{'=' * 60}")
                print(f"进度: [{i + 1}/{stats['total']}]")
                print(
                    f"成功: {stats['success']} | 失败: {stats['failed']} | 跳过: {stats['skipped']}"
                )
                print(f"{'=' * 60}")

            try:
                # 调用现有的 save_daily 方法
                self.save_daily(ts_code, start_date, end_date, adjust)
                stats["success"] += 1
            except Exception as e:
                print(f"❌ {ts_code} 处理失败: {e}")
                stats["failed"] += 1

            # 每10只股票保存一次检查点
            if (i + 1) % 10 == 0:
                checkpoint_data = {
                    "start_date": start_date,
                    "end_date": end_date,
                    "adjust": adjust,
                    "total": len(all_stocks),
                    "last_index": i,
                    "stats": stats,
                    "timestamp": datetime.now().isoformat(),
                }
                self._save_checkpoint(checkpoint_path, checkpoint_data)

        # 4. 删除检查点文件（下载完成）
        if Path(checkpoint_path).exists():
            Path(checkpoint_path).unlink()
            print("🗑️  已删除检查点文件")

        # 5. 输出统计信息
        print(f"\n{'=' * 60}")
        print(f"数据下载完成:")
        print(f"  总计: {stats['total']} 只股票")
        print(f"  成功: {stats['success']}")
        print(f"  失败: {stats['failed']}")
        print(f"  跳过: {stats['skipped']}")
        print(f"{'=' * 60}")

        return stats

    def save_all_stocks_by_code_incremental(
        self,
        default_start_date: str = "20240101",
        end_date: str = None,
        adjust: str = None,
        checkpoint_path: str = None,
        resume: bool = True,
        stock_list: list = None,
    ):
        """
        按股票代码增量更新A股数据（只下载每只股票的最新缺失数据）

        Args:
            default_start_date: 默认开始日期（用于没有数据的股票）
            end_date: 结束日期
            adjust: 复权类型
            checkpoint_path: 检查点文件路径
            resume: 是否从检查点恢复
            stock_list: 指定股票列表，None则获取全部A股

        Returns:
            统计信息字典
        """
        # 设置默认检查点路径
        if checkpoint_path is None:
            try:
                from config.settings import CHECKPOINT_FILE

                checkpoint_path = str(CHECKPOINT_FILE)
            except ImportError:
                checkpoint_path = "data/download_checkpoint.json"

        # 如果未指定结束日期，使用当前日期
        if end_date is None:
            end_date = datetime.today().strftime("%Y%m%d")

        # 1. 获取股票列表
        if stock_list is None:
            # 获取全部A股列表
            print("📋 正在获取股票列表...")
            try:
                stock_list_df = self._retry_api_call(
                    self.pro.stock_basic,
                    exchange="",
                    list_status="L",  # 只获取上市股票
                    fields="ts_code,name,area,industry,list_date",
                )
                if stock_list_df is None or stock_list_df.empty:
                    print("❌ 获取股票列表失败")
                    return None
                all_stocks = stock_list_df["ts_code"].tolist()
                print(f"📋 共 {len(all_stocks)} 只股票")
            except Exception as e:
                print(f"❌ 获取股票列表失败: {e}")
                return None
        else:
            # 使用指定的股票列表
            all_stocks = []
            print(f"📋 使用指定的股票列表...")

            for code in stock_list:
                # 标准化代码格式
                try:
                    ts_code = self._standardize_code(code)
                    all_stocks.append(ts_code)
                except Exception as e:
                    print(f"  ⚠️  股票代码 {code} 格式不正确: {e}")

            if not all_stocks:
                print("❌ 没有有效的股票代码")
                return None

            print(f"📋 共 {len(all_stocks)} 只股票")

        # 2. 检查每只股票的最新数据日期
        import pandas as pd
        from datetime import timedelta

        incremental_dates = {}
        need_update_stocks = []
        no_data_stocks = []

        print("🔍 检查本地数据最新日期...")

        for ts_code in all_stocks:
            try:
                code = ts_code.split(".")[0]
                exchange = (
                    "SH"
                    if ts_code.endswith(".SH")
                    else "SZ"
                    if ts_code.endswith(".SZ")
                    else None
                )

                # 优先从DuckDB查询该股票的最新数据日期
                from src.db.duckdb_manager import get_duckdb_manager

                duckdb_manager = get_duckdb_manager()
                latest_date = None

                with duckdb_manager.get_connection() as conn:
                    # 检查A股表
                    if duckdb_manager.table_exists("bars_a_1d"):
                        query = f"""
                        SELECT datetime FROM bars_a_1d
                        WHERE stock_code = '{code}'
                        """
                        if exchange:
                            query += f" AND exchange = '{exchange}'"
                        query += " ORDER BY datetime DESC LIMIT 1"
                        df = conn.execute(query).fetchdf()
                        if not df.empty and df["datetime"][0] is not None:
                            latest_date = pd.to_datetime(df["datetime"][0])

                # 如果DuckDB没有数据，回退到SQLite（兼容旧逻辑）
                if latest_date is None:
                    query = """
                    SELECT datetime FROM bars
                    WHERE symbol = :symbol AND interval = '1d'
                    ORDER BY datetime DESC LIMIT 1
                    """
                    with self.engine.connect() as conn:
                        df = pd.read_sql_query(query, conn, params={"symbol": code})
                    if not df.empty:
                        latest_date = pd.to_datetime(df["datetime"].iloc[0])

                if latest_date is not None:
                    end_date_dt = pd.to_datetime(end_date)

                    # 如果最新数据已经是今天或之后，跳过
                    if latest_date >= end_date_dt:
                        continue

                    # 从最新日期的下一天开始
                    start_date_dt = latest_date + timedelta(days=1)
                    start_date = start_date_dt.strftime("%Y%m%d")

                    incremental_dates[ts_code] = start_date
                    need_update_stocks.append(ts_code)
                else:
                    # 无数据，需要从头下载
                    no_data_stocks.append(ts_code)
                    incremental_dates[ts_code] = default_start_date
                    need_update_stocks.append(ts_code)

            except Exception as e:
                print(f"  ⚠️  {ts_code} 检查失败: {e}")
                # 失败时也添加到需要更新的列表
                incremental_dates[ts_code] = default_start_date
                need_update_stocks.append(ts_code)

        print(
            f"✅ 检查完成: {len(need_update_stocks)} 只需要更新, {len(all_stocks) - len(need_update_stocks)} 只已是最新"
        )

        if len(need_update_stocks) == 0:
            print("🎉 所有股票数据已是最新，无需更新！")
            return {
                "total": len(all_stocks),
                "success": 0,
                "failed": 0,
                "skipped": len(all_stocks),
            }

        # 3. 尝试从检查点恢复
        start_index = 0
        stats = {
            "success": 0,
            "failed": 0,
            "skipped": len(all_stocks) - len(need_update_stocks),
            "total": len(all_stocks),
        }

        if resume:
            checkpoint = self._load_checkpoint(checkpoint_path)
            if checkpoint:
                # 验证检查点是否匹配当前下载任务
                if (
                    checkpoint.get("default_start_date") == default_start_date
                    and checkpoint.get("end_date") == end_date
                    and checkpoint.get("adjust") == adjust
                    and checkpoint.get("total") == len(all_stocks)
                ):
                    last_index = checkpoint.get("last_index", 0)
                    start_index = last_index + 1
                    stats = checkpoint.get("stats", stats)
                    print(f"🔄 从检查点恢复: 第 {start_index + 1} 只股票开始")
                else:
                    print("⚠️  检查点参数不匹配，从头开始下载")
                    Path(checkpoint_path).unlink(missing_ok=True)

        # 4. 遍历每只股票
        for i in range(start_index, len(all_stocks)):
            ts_code = all_stocks[i]

            # 跳过不需要更新的股票
            if ts_code not in incremental_dates:
                continue

            # 定期显示进度（每50只股票）
            if (i + 1) % 50 == 1 or i == len(all_stocks) - 1:
                print(f"\n{'=' * 60}")
                print(f"进度: [{i + 1}/{stats['total']}]")
                print(
                    f"成功: {stats['success']} | 失败: {stats['failed']} | 跳过: {stats['skipped']}"
                )
                print(f"{'=' * 60}")

            try:
                # 使用增量开始日期
                start_date = incremental_dates[ts_code]
                # 调用现有的 save_daily 方法
                self.save_daily(ts_code, start_date, end_date, adjust)
                stats["success"] += 1
            except Exception as e:
                print(f"❌ {ts_code} 处理失败: {e}")
                stats["failed"] += 1

            # 每10只股票保存一次检查点
            if (i + 1) % 10 == 0:
                checkpoint_data = {
                    "default_start_date": default_start_date,
                    "end_date": end_date,
                    "adjust": adjust,
                    "total": len(all_stocks),
                    "last_index": i,
                    "stats": stats,
                    "timestamp": datetime.now().isoformat(),
                }
                self._save_checkpoint(checkpoint_path, checkpoint_data)

        # 5. 删除检查点文件（下载完成）
        if Path(checkpoint_path).exists():
            Path(checkpoint_path).unlink()
            print("🗑️  已删除检查点文件")

        # 6. 输出统计信息
        print(f"\n{'=' * 60}")
        print(f"数据下载完成:")
        print(f"  总计: {stats['total']} 只股票")
        print(f"  成功: {stats['success']}")
        print(f"  失败: {stats['failed']}")
        print(f"  跳过: {stats['skipped']}")
        print(f"{'=' * 60}")

        return stats

    def save_all_stocks_by_date(
        self, start_date: str = "20240101", end_date: str = None
    ):
        """
        按交易日批量获取全部A股数据（不含复权价格）

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            统计信息字典
        """
        # 如果未指定结束日期，使用当前日期
        if end_date is None:
            end_date = datetime.today().strftime("%Y%m%d")

        # 1. 获取交易日历
        print("📅 正在获取交易日历...")
        try:
            df_cal = self._retry_api_call(
                self.pro.trade_cal,
                exchange="SSE",
                is_open="1",
                start_date=start_date,
                end_date=end_date,
                fields="cal_date",
            )
            if df_cal is None or df_cal.empty:
                print("❌ 获取交易日历失败")
                return None
            trade_dates = df_cal["cal_date"].tolist()
            print(f"📅 共 {len(trade_dates)} 个交易日")
        except Exception as e:
            print(f"❌ 获取交易日历失败: {e}")
            return None

        # 2. 遍历每个交易日
        stats = {"success": 0, "failed": 0, "total": len(trade_dates)}
        total_records = 0

        for i, date in enumerate(trade_dates, 1):
            # 定期显示进度（每20个交易日）
            if i % 20 == 1 or i == len(trade_dates):
                print(f"\n{'=' * 60}")
                print(f"进度: [{i}/{stats['total']}]")
                print(
                    f"成功: {stats['success']} | 失败: {stats['failed']} | 总记录: {total_records}"
                )
                print(f"{'=' * 60}")

            # 使用重试机制获取数据
            df = self._retry_api_call(self.pro.daily, trade_date=date)

            if df is not None and not df.empty:
                # 数据转换和保存
                saved_count = self._save_daily_batch(df, date)
                total_records += saved_count
                stats["success"] += 1
                print(f"✅ {date} 保存了 {saved_count} 条记录")
            else:
                stats["failed"] += 1
                print(f"⚠️ {date} 获取数据失败")

        # 3. 输出统计信息
        print(f"\n{'=' * 60}")
        print(f"数据下载完成:")
        print(f"  总计: {stats['total']} 个交易日")
        print(f"  成功: {stats['success']}")
        print(f"  失败: {stats['failed']}")
        print(f"  总记录: {total_records} 条")
        print(f"{'=' * 60}")

        return stats

    def _save_daily_batch(self, df: pd.DataFrame, trade_date: str) -> int:
        """
        保存批量获取的日线数据

        Args:
            df: Tushare daily 接口返回的DataFrame
            trade_date: 交易日期

        Returns:
            保存的记录数
        """
        # 保存不复权价格
        df["open_orig"] = df["open"]
        df["high_orig"] = df["high"]
        df["low_orig"] = df["low"]
        df["close_orig"] = df["close"]
        df["open_qfq"] = None
        df["high_qfq"] = None
        df["low_qfq"] = None
        df["close_qfq"] = None
        df["turnover"] = None

        # 重命名列
        df = df.rename(columns={"trade_date": "datetime", "vol": "volume"})

        # 添加元数据
        df["symbol"] = df["ts_code"].str.split(".").str[0]
        df["exchange"] = df["ts_code"].apply(self._detect_exchange)
        df["interval"] = "1d"
        df["datetime"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d")

        # 添加 amount 列（如果不存在）
        if "amount" not in df.columns:
            df["amount"] = None

        # 选择要保存的列
        columns = [
            "symbol",
            "exchange",
            "interval",
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "open_qfq",
            "high_qfq",
            "low_qfq",
            "close_qfq",
            "pre_close",
            "change",
            "pct_chg",
            "volume",
            "turnover",
            "amount",
        ]

        # 确保所有列都存在
        for col in columns:
            if col not in df.columns:
                df[col] = None

        # 保存到数据库
        try:
            df[columns].to_sql(
                "bars", self.engine, if_exists="append", index=False, method="multi"
            )
            return len(df)
        except Exception as e:
            if "UNIQUE constraint" in str(e) or "duplicate" in str(e).lower():
                # 数据已存在，返回已保存数量
                return 0
            else:
                print(f"  ⚠️  数据库操作失败: {e}")
                return 0

    # ==================== 财务数据相关方法 ====================

    def _create_unified_financial_table(self, table_name: str):
        """
        创建统一财务报表表（如果不存在）

        统一表包含所有股票的数据，使用 ts_code 字段区分

        Args:
            table_name: 表名（income, balancesheet, cashflow）
        """
        # 定义每张表的字段列表
        table_columns = {
            "income": [
                # 主键字段
                "ts_code",
                "ann_date",
                "end_date",
                # 利润表字段
                "f_ann_date",
                "report_type",
                "comp_type",
                "end_type",
                "basic_eps",
                "diluted_eps",
                "total_revenue",
                "revenue",
                "int_income",
                "prem_earned",
                "comm_income",
                "n_commis_income",
                "n_oth_income",
                "n_oth_b_income",
                "prem_income",
                "out_prem",
                "une_prem_reser",
                "reins_income",
                "n_sec_tb_income",
                "n_sec_uw_income",
                "n_asset_mg_income",
                "oth_b_income",
                "fv_value_chg_gain",
                "invest_income",
                "ass_invest_income",
                "forex_gain",
                "total_cogs",
                "oper_cost",
                "int_exp",
                "comm_exp",
                "biz_tax_surchg",
                "sell_exp",
                "admin_exp",
                "fin_exp",
                "assets_impair_loss",
                "prem_refund",
                "compens_payout",
                "reser_insur_liab",
                "div_payt",
                "reins_exp",
                "oper_exp",
                "compens_payout_refu",
                "insur_reser_refu",
                "reins_cost_refund",
                "other_bus_cost",
                "operate_profit",
                "non_oper_income",
                "non_oper_exp",
                "nca_disploss",
                "total_profit",
                "income_tax",
                "n_income",
                "n_income_attr_p",
                "minority_gain",
                "oth_compr_income",
                "t_compr_income",
                "compr_inc_attr_p",
                "compr_inc_attr_m_s",
                "ebit",
                "ebitda",
                "insurance_exp",
                "undist_profit",
                "distable_profit",
                "rd_exp",
                "fin_exp_int_exp",
                "fin_exp_int_inc",
                "transfer_surplus_rese",
                "transfer_housing_imprest",
                "transfer_oth",
                "adj_lossgain",
                "withdra_legal_surplus",
                "withdra_legal_pubfund",
                "withdra_biz_devfund",
                "withdra_rese_fund",
                "withdra_oth_ersu",
                "workers_welfare",
                "distr_profit_shrhder",
                "prfshare_payable_dvd",
                "comshare_payable_dvd",
                "capit_comstock_div",
                "continued_net_profit",
                "update_flag",
            ],
            "balancesheet": [
                # 主键字段
                "ts_code",
                "ann_date",
                "end_date",
                # 资产负债表字段
                "f_ann_date",
                "report_type",
                "comp_type",
                "end_type",
                "total_share",
                "cap_rese",
                "undistr_porfit",
                "surplus_rese",
                "special_rese",
                "money_cap",
                "trad_asset",
                "notes_receiv",
                "accounts_receiv",
                "oth_receiv",
                "prepayment",
                "div_receiv",
                "int_receiv",
                "inventories",
                "amor_exp",
                "nca_within_1y",
                "sett_rsrv",
                "loanto_oth_bank_fi",
                "premium_receiv",
                "reinsur_receiv",
                "reinsur_res_receiv",
                "pur_resale_fa",
                "oth_cur_assets",
                "total_cur_assets",
                "fa_avail_for_sale",
                "htm_invest",
                "lt_eqt_invest",
                "invest_real_estate",
                "time_deposits",
                "oth_assets",
                "lt_rec",
                "fix_assets",
                "cip",
                "const_materials",
                "fixed_assets_disp",
                "produc_bio_assets",
                "oil_and_gas_assets",
                "intan_assets",
                "r_and_d",
                "goodwill",
                "lt_amor_exp",
                "defer_tax_assets",
                "decr_in_disbur",
                "oth_nca",
                "total_nca",
                "cash_reser_cb",
                "depos_in_oth_bfi",
                "prec_metals",
                "deriv_assets",
                "rr_reins_une_prem",
                "rr_reins_outstd_cla",
                "rr_reins_lins_liab",
                "rr_reins_lthins_liab",
                "refund_depos",
                "ph_pledge_loans",
                "refund_cap_depos",
                "indep_acct_assets",
                "client_depos",
                "client_prov",
                "transac_seat_fee",
                "invest_as_receiv",
                "total_assets",
                "lt_borr",
                "st_borr",
                "cb_borr",
                "depos_ib_deposits",
                "loan_oth_bank",
                "trading_fl",
                "notes_payable",
                "acct_payable",
                "adv_receipts",
                "sold_for_repur_fa",
                "comm_payable",
                "payroll_payable",
                "taxes_payable",
                "int_payable",
                "div_payable",
                "oth_payable",
                "acc_exp",
                "deferred_inc",
                "st_bonds_payable",
                "payable_to_reinsurer",
                "rsrv_insur_cont",
                "acting_trading_sec",
                "acting_uw_sec",
                "non_cur_liab_due_1y",
                "oth_cur_liab",
                "total_cur_liab",
                "bond_payable",
                "lt_payable",
                "specific_payables",
                "estimated_liab",
                "defer_tax_liab",
                "defer_inc_non_cur_liab",
                "oth_ncl",
                "total_ncl",
                "depos_oth_bfi",
                "deriv_liab",
                "depos",
                "agency_bus_liab",
                "oth_liab",
                "prem_receiv_adva",
                "depos_received",
                "ph_invest",
                "reser_une_prem",
                "reser_outstd_claims",
                "reser_lins_liab",
                "reser_lthins_liab",
                "indept_acc_liab",
                "pledge_borr",
                "indem_payable",
                "policy_div_payable",
                "total_liab",
                "treasury_share",
                "ordin_risk_reser",
                "forex_differ",
                "invest_loss_unconf",
                "minority_int",
                "total_hldr_eqy_exc_min_int",
                "total_hldr_eqy_inc_min_int",
                "total_liab_hldr_eqy",
                "lt_payroll_payable",
                "oth_comp_income",
                "oth_eqt_tools",
                "oth_eqt_tools_p_shr",
                "lending_funds",
                "acc_receivable",
                "st_fin_payable",
                "payables",
                "hfs_assets",
                "hfs_sales",
                "cost_fin_assets",
                "fair_value_fin_assets",
                "contract_assets",
                "contract_liab",
                "accounts_receiv_bill",
                "accounts_pay",
                "oth_rcv_total",
                "fix_assets_total",
                "cip_total",
                "oth_pay_total",
                "long_pay_total",
                "debt_invest",
                "oth_debt_invest",
                "update_flag",
            ],
            "cashflow": [
                # 主键字段
                "ts_code",
                "ann_date",
                "end_date",
                # 现金流量表字段
                "f_ann_date",
                "comp_type",
                "report_type",
                "end_type",
                "net_profit",
                "finan_exp",
                "c_fr_sale_sg",
                "recp_tax_rends",
                "n_depos_incr_fi",
                "n_incr_loans_cb",
                "n_inc_borr_oth_fi",
                "prem_fr_orig_contr",
                "n_incr_insured_dep",
                "n_reinsur_prem",
                "n_incr_disp_tfa",
                "ifc_cash_incr",
                "n_incr_disp_faas",
                "n_incr_loans_oth_bank",
                "n_cap_incr_repur",
                "c_fr_oth_operate_a",
                "c_inf_fr_operate_a",
                "c_paid_goods_s",
                "c_paid_to_for_empl",
                "c_paid_for_taxes",
                "n_incr_clt_loan_adv",
                "n_incr_dep_cbob",
                "c_pay_claims_orig_inco",
                "pay_handling_chrg",
                "pay_comm_insur_plcy",
                "oth_cash_pay_oper_act",
                "st_cash_out_act",
                "n_cashflow_act",
                "oth_recp_ral_inv_act",
                "c_disp_withdrwl_invest",
                "c_recp_return_invest",
                "n_recp_disp_fiolta",
                "n_recp_disp_sobu",
                "stot_inflows_inv_act",
                "c_pay_acq_const_fiolta",
                "c_paid_invest",
                "n_disp_subs_oth_biz",
                "oth_pay_ral_inv_act",
                "n_incr_pledge_loan",
                "stot_out_inv_act",
                "n_cashflow_inv_act",
                "c_recp_borrow",
                "proc_issue_bonds",
                "oth_cash_recp_ral_fnc_act",
                "stot_cash_in_fnc_act",
                "free_cashflow",
                "c_prepay_amt_borr",
                "c_pay_dist_dpcp_int_exp",
                "incl_dvd_profit_paid_sc_ms",
                "oth_cashpay_ral_fnc_act",
                "stot_cashout_fnc_act",
                "n_cash_flows_fnc_act",
                "eff_fx_flu_cash",
                "n_incr_cash_cash_equ",
                "c_cash_equ_beg_period",
                "c_cash_equ_end_period",
                "c_recp_cap_contrib",
                "incl_cash_rec_saims",
                "uncon_invest_loss",
                "prov_depr_assets",
                "depr_fa_coga_dpba",
                "amort_intang_assets",
                "lt_amort_deferred_exp",
                "decr_deferred_exp",
                "incr_acc_exp",
                "loss_disp_fiolta",
                "loss_scr_fa",
                "loss_fv_chg",
                "invest_loss",
                "decr_def_inc_tax_assets",
                "incr_def_inc_tax_liab",
                "decr_inventories",
                "decr_oper_payable",
                "incr_oper_payable",
                "others",
                "im_net_cashflow_oper_act",
                "conv_debt_into_cap",
                "conv_copbonds_due_within_1y",
                "fa_fnc_leases",
                "im_n_incr_cash_equ",
                "net_dism_capital_add",
                "net_cash_rece_sec",
                "credit_impa_loss",
                "use_right_asset_dep",
                "oth_loss_asset",
                "end_bal_cash",
                "beg_bal_cash",
                "end_bal_cash_equ",
                "beg_bal_cash_equ",
                "update_flag",
            ],
        }

        if table_name not in table_columns:
            raise ValueError(f"未知的表类型: {table_name}")

        columns = table_columns[table_name]

        # 构建列定义
        columns_def = []
        for col in columns:
            if col in ["ts_code", "ann_date", "end_date"]:
                columns_def.append(f"{col} TEXT NOT NULL")
            else:
                columns_def.append(f"{col} REAL")

        # 主键定义
        primary_key = "PRIMARY KEY (ts_code, ann_date, end_date)"

        # 创建表SQL
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {", ".join(columns_def)},
            {primary_key}
        );
        """

        # 执行建表
        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
                # 创建索引
                conn.execute(
                    text(
                        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol_date ON {table_name}(ts_code, end_date)"
                    )
                )
                conn.execute(
                    text(
                        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name}(end_date)"
                    )
                )
                conn.commit()
        except Exception as e:
            print(f"⚠️  创建表 {table_name} 失败: {e}")
            raise

    def _extract_stock_code(self, ts_code: str) -> str:
        """
        从 ts_code 中提取纯股票代码

        Args:
            ts_code: 股票代码（如 000001.SZ）

        Returns:
            纯股票代码（如 000001）
        """
        return ts_code.split(".")[0]

    def _convert_ts_code_to_akshare_symbol(self, ts_code: str) -> Optional[str]:
        """将 A 股 ts_code 转换为 AKShare 需要的 symbol 格式。"""
        ts_code_std = self._standardize_code(ts_code)
        if ts_code_std.endswith(".SH"):
            return f"SH{ts_code_std.split('.')[0]}"
        if ts_code_std.endswith(".SZ"):
            return f"SZ{ts_code_std.split('.')[0]}"
        return None

    def _normalize_financial_date(self, value) -> Optional[str]:
        """将日期值标准化为 YYYYMMDD。"""
        if pd.isna(value):
            return None

        value_str = str(value).strip()
        if not value_str:
            return None

        digits = "".join(ch for ch in value_str if ch.isdigit())
        if len(digits) >= 8:
            return digits[:8]

        return None

    def _load_akshare_cashflow_end_bal_cash(self, ts_code: str) -> pd.DataFrame:
        """获取 AKShare 现金流中的期末现金及现金等价物余额。"""
        ak_symbol = self._convert_ts_code_to_akshare_symbol(ts_code)
        if not ak_symbol:
            return pd.DataFrame(columns=["ann_date", "end_date", "end_bal_cash"])

        try:
            import akshare as ak
        except ImportError:
            logger.warning("akshare 未安装，跳过 cashflow.end_bal_cash 回填")
            return pd.DataFrame(columns=["ann_date", "end_date", "end_bal_cash"])

        try:
            df = ak.stock_cash_flow_sheet_by_report_em(symbol=ak_symbol)
        except Exception as exc:
            logger.warning("获取 AKShare 现金流失败 %s: %s", ak_symbol, exc)
            return pd.DataFrame(columns=["ann_date", "end_date", "end_bal_cash"])

        if df is None or df.empty:
            return pd.DataFrame(columns=["ann_date", "end_date", "end_bal_cash"])

        required_columns = {"NOTICE_DATE", "REPORT_DATE", "END_CCE"}
        if not required_columns.issubset(df.columns):
            logger.warning(
                "AKShare 现金流缺少必要列 %s，实际列: %s",
                sorted(required_columns),
                list(df.columns),
            )
            return pd.DataFrame(columns=["ann_date", "end_date", "end_bal_cash"])

        result = df[["NOTICE_DATE", "REPORT_DATE", "END_CCE"]].copy()
        result["ann_date"] = result["NOTICE_DATE"].apply(self._normalize_financial_date)
        result["end_date"] = result["REPORT_DATE"].apply(self._normalize_financial_date)
        result["end_bal_cash"] = pd.to_numeric(result["END_CCE"], errors="coerce")
        result = result[["ann_date", "end_date", "end_bal_cash"]]
        result = result.dropna(subset=["ann_date", "end_date", "end_bal_cash"])
        return result

    def _backfill_cashflow_end_bal_cash(
        self, df: pd.DataFrame, ts_code: str
    ) -> Tuple[pd.DataFrame, dict]:
        """使用 AKShare END_CCE 严格回填现金流 end_bal_cash。"""
        stats = {
            "symbol": self._standardize_code(ts_code),
            "missing_candidates": 0,
            "filled": 0,
            "unmatched": 0,
            "ambiguous": 0,
            "invalid_dates": 0,
        }

        if df.empty or "end_bal_cash" not in df.columns:
            return df, stats

        candidate_mask = df["end_bal_cash"].isna()
        if "report_type" in df.columns:
            report_type_series = pd.to_numeric(df["report_type"], errors="coerce")
            candidate_mask = candidate_mask & report_type_series.eq(1)

        stats["missing_candidates"] = int(candidate_mask.sum())
        if stats["missing_candidates"] == 0:
            return df, stats

        ak_df = self._load_akshare_cashflow_end_bal_cash(ts_code)
        if ak_df.empty:
            stats["unmatched"] = stats["missing_candidates"]
            logger.warning(
                "%s cashflow.end_bal_cash 缺失 %s 条，AKShare 无可用匹配数据",
                stats["symbol"],
                stats["missing_candidates"],
            )
            return df, stats

        duplicate_mask = ak_df.duplicated(subset=["ann_date", "end_date"], keep=False)
        ambiguous_keys = {
            (row["ann_date"], row["end_date"])
            for _, row in ak_df.loc[duplicate_mask, ["ann_date", "end_date"]].iterrows()
        }
        if ambiguous_keys:
            logger.warning(
                "%s AKShare 现金流存在 %s 个重复公告日+报告期组合，已跳过这些组合",
                stats["symbol"],
                len(ambiguous_keys),
            )

        ak_df = ak_df.loc[~duplicate_mask].drop_duplicates(subset=["ann_date", "end_date"])
        ak_lookup = {
            (row["ann_date"], row["end_date"]): row["end_bal_cash"]
            for _, row in ak_df.iterrows()
        }

        for idx in df.index[candidate_mask]:
            ann_date = self._normalize_financial_date(df.at[idx, "ann_date"])
            end_date = self._normalize_financial_date(df.at[idx, "end_date"])
            if not ann_date or not end_date:
                stats["invalid_dates"] += 1
                logger.warning(
                    "%s cashflow 记录日期无法标准化，ann_date=%s end_date=%s",
                    stats["symbol"],
                    df.at[idx, "ann_date"],
                    df.at[idx, "end_date"],
                )
                continue

            key = (ann_date, end_date)
            if key in ambiguous_keys:
                stats["ambiguous"] += 1
                continue

            matched_value = ak_lookup.get(key)
            if pd.isna(matched_value) or matched_value is None:
                stats["unmatched"] += 1
                continue

            df.at[idx, "end_bal_cash"] = float(matched_value)
            stats["filled"] += 1

        if stats["filled"] > 0 or stats["missing_candidates"] > 0:
            print(
                f"  💰 end_bal_cash 回填: 缺失 {stats['missing_candidates']} 条, "
                f"成功 {stats['filled']} 条, 未匹配 {stats['unmatched']} 条, "
                f"重复匹配 {stats['ambiguous']} 条"
            )

        return df, stats

    def _smart_dedup_financial_data(
        self, df: pd.DataFrame, table_type: str
    ) -> pd.DataFrame:
        """
        智能去重：优先保留关键字段非NULL的记录

        Args:
            df: 原始数据框
            table_type: 表类型（income/balancesheet/cashflow/fina_indicator）

        Returns:
            去重后的数据框
        """
        if df.empty:
            return df

        # 定义每个表的关键字段（优先保留这些字段非NULL的记录）
        key_columns_map = {
            "income": ["revenue", "n_income", "n_income_attr_p", "oper_cost"],
            "balancesheet": [
                "total_assets",
                "total_liab",
                "total_hldr_eqy_exc_min_int",
            ],
            "cashflow": ["n_cashflow_act", "free_cashflow", "n_cashflow_inv_act", "end_bal_cash"],
            "fina_indicator": ["roe", "roa", "netprofit_margin"],
        }

        key_columns = key_columns_map.get(table_type, [])

        # 定义去重的关键字段
        dedup_columns = ["ts_code", "ann_date", "end_date"]
        if table_type == "fina_indicator":
            dedup_columns.append("report_type")

        def select_best_record(group):
            """选择最佳记录：优先保留update_flag=1且关键字段非NULL的记录"""
            if len(group) == 1:
                return group.iloc[0]

            # 策略1：优先选择update_flag=1的记录
            if "update_flag" in group.columns:
                updated = group[group["update_flag"] == 1]
                if not updated.empty:
                    # 在update_flag=1的记录中，选择关键字段最完整的
                    if key_columns:
                        best_record = None
                        max_non_null = -1

                        for idx, row in updated.iterrows():
                            non_null_count = sum(
                                [
                                    1
                                    for col in key_columns
                                    if col in row and pd.notna(row[col])
                                ]
                            )
                            if non_null_count > max_non_null:
                                max_non_null = non_null_count
                                best_record = row

                        if best_record is not None:
                            return best_record

                    # 如果没有关键字段，返回第一条update_flag=1的记录
                    return updated.iloc[0]

            # 策略2：选择关键字段最完整的记录
            if key_columns:
                best_record = None
                max_non_null = -1

                for idx, row in group.iterrows():
                    non_null_count = sum(
                        [1 for col in key_columns if col in row and pd.notna(row[col])]
                    )
                    if non_null_count > max_non_null:
                        max_non_null = non_null_count
                        best_record = row

                if best_record is not None:
                    return best_record

            # 策略3：如果没有关键字段，返回第一条记录
            return group.iloc[0]

        # 按优先级排序后去重：先看 update_flag=1，再看关键字段完整度，最后保留原始顺序靠前的记录
        df_dedup = df.copy()
        df_dedup["_row_order"] = range(len(df_dedup))

        if key_columns:
            existing_key_columns = [col for col in key_columns if col in df_dedup.columns]
            if existing_key_columns:
                df_dedup["_non_null_score"] = df_dedup[existing_key_columns].notna().sum(axis=1)
            else:
                df_dedup["_non_null_score"] = 0
        else:
            df_dedup["_non_null_score"] = 0

        if "update_flag" in df_dedup.columns:
            df_dedup["_update_priority"] = (
                pd.to_numeric(df_dedup["update_flag"], errors="coerce").fillna(0).eq(1).astype(int)
            )
        else:
            df_dedup["_update_priority"] = 0

        sort_columns = dedup_columns + ["_update_priority", "_non_null_score", "_row_order"]
        ascending = [True] * len(dedup_columns) + [False, False, True]
        df_dedup = df_dedup.sort_values(sort_columns, ascending=ascending)
        df_dedup = df_dedup.drop_duplicates(subset=dedup_columns, keep="first")

        return df_dedup.drop(columns=["_row_order", "_non_null_score", "_update_priority"])

    def save_income(self, ts_code: str, start_date: str = None, end_date: str = None):
        """
        获取并保存利润表数据到统一表

        Args:
            ts_code: 股票代码（如 000001.SZ 或 000001）
            start_date: 公告开始日期（格式 YYYYMMDD）
            end_date: 公告结束日期（格式 YYYYMMDD）

        Returns:
            保存的记录数，失败返回 0
        """
        try:
            # 标准化代码
            ts_code_std = self._standardize_code(ts_code)
            table_name = "income"

            # 获取数据
            print(f"  📥 获取利润表数据 {ts_code_std}...")
            df = self._retry_api_call(
                self.pro.income,
                ts_code=ts_code_std,
                start_date=start_date,
                end_date=end_date,
            )

            if df is None or df.empty:
                print(f"  ⚠️  {ts_code_std} 无利润表数据")
                return 0

            # 智能去重：优先保留关键字段非NULL的记录
            df_before = len(df)
            df = self._smart_dedup_financial_data(df, "income")
            if len(df) < df_before:
                print(f"  🔄 去除重复数据: {df_before} -> {len(df)} 条")

            # 确保统一表存在
            self._create_unified_financial_table(table_name)

            # 删除该股票的旧数据（避免主键冲突）
            with self.engine.connect() as conn:
                delete_sql = "DELETE FROM income WHERE ts_code = :ts_code"
                conn.execute(text(delete_sql), {"ts_code": ts_code_std})
                conn.commit()

            # 保存到统一表
            df.to_sql(
                table_name, self.engine, if_exists="append", index=False, method="multi"
            )
            print(f"  ✅ 已保存利润表 {len(df)} 条记录")

            return len(df)

        except Exception as e:
            error_msg = str(e)
            if "无权限" in error_msg or "权限" in error_msg or "403" in error_msg:
                print(f"  ⚠️  无权限获取利润表数据（需要2000+积分）")
            else:
                print(f"  ❌ 保存利润表失败: {e}")
            return 0

    def save_balancesheet(
        self, ts_code: str, start_date: str = None, end_date: str = None
    ):
        """
        获取并保存资产负债表数据到统一表

        Args:
            ts_code: 股票代码（如 000001.SZ 或 000001）
            start_date: 公告开始日期（格式 YYYYMMDD）
            end_date: 公告结束日期（格式 YYYYMMDD）

        Returns:
            保存的记录数，失败返回 0
        """
        try:
            # 标准化代码
            ts_code_std = self._standardize_code(ts_code)
            table_name = "balancesheet"

            # 获取数据
            print(f"  📥 获取资产负债表数据 {ts_code_std}...")
            df = self._retry_api_call(
                self.pro.balancesheet,
                ts_code=ts_code_std,
                start_date=start_date,
                end_date=end_date,
            )

            if df is None or df.empty:
                print(f"  ⚠️  {ts_code_std} 无资产负债表数据")
                return 0

            # 智能去重：优先保留关键字段非NULL的记录
            df_before = len(df)
            df = self._smart_dedup_financial_data(df, "balancesheet")
            if len(df) < df_before:
                print(f"  🔄 去除重复数据: {df_before} -> {len(df)} 条")

            # 确保统一表存在
            self._create_unified_financial_table(table_name)

            # 删除该股票的旧数据（避免主键冲突）
            with self.engine.connect() as conn:
                delete_sql = "DELETE FROM balancesheet WHERE ts_code = :ts_code"
                conn.execute(text(delete_sql), {"ts_code": ts_code_std})
                conn.commit()

            # 保存到统一表
            df.to_sql(
                table_name, self.engine, if_exists="append", index=False, method="multi"
            )
            print(f"  ✅ 已保存资产负债表 {len(df)} 条记录")

            return len(df)

        except Exception as e:
            error_msg = str(e)
            if "无权限" in error_msg or "权限" in error_msg or "403" in error_msg:
                print(f"  ⚠️  无权限获取资产负债表数据（需要2000+积分）")
            else:
                print(f"  ❌ 保存资产负债表失败: {e}")
            return 0

    def save_cashflow(self, ts_code: str, start_date: str = None, end_date: str = None):
        """
        获取并保存现金流量表数据到统一表

        Args:
            ts_code: 股票代码（如 000001.SZ 或 000001）
            start_date: 公告开始日期（格式 YYYYMMDD）
            end_date: 公告结束日期（格式 YYYYMMDD）

        Returns:
            保存的记录数，失败返回 0
        """
        try:
            # 标准化代码
            ts_code_std = self._standardize_code(ts_code)
            table_name = "cashflow"
            self._last_cashflow_backfill_stats = None

            # 获取数据
            print(f"  📥 获取现金流量表数据 {ts_code_std}...")
            df = self._retry_api_call(
                self.pro.cashflow,
                ts_code=ts_code_std,
                start_date=start_date,
                end_date=end_date,
            )

            if df is None or df.empty:
                print(f"  ⚠️  {ts_code_std} 无现金流量表数据")
                return 0

            df, backfill_stats = self._backfill_cashflow_end_bal_cash(df, ts_code_std)
            self._last_cashflow_backfill_stats = backfill_stats

            # 智能去重：优先保留关键字段非NULL的记录
            df_before = len(df)
            df = self._smart_dedup_financial_data(df, "cashflow")
            if len(df) < df_before:
                print(f"  🔄 去除重复数据: {df_before} -> {len(df)} 条")

            # 确保统一表存在
            self._create_unified_financial_table(table_name)

            # 删除该股票的旧数据（避免主键冲突）
            with self.engine.connect() as conn:
                delete_sql = "DELETE FROM cashflow WHERE ts_code = :ts_code"
                conn.execute(text(delete_sql), {"ts_code": ts_code_std})
                conn.commit()

            # 保存到统一表
            df.to_sql(
                table_name, self.engine, if_exists="append", index=False, method="multi"
            )
            print(f"  ✅ 已保存现金流量表 {len(df)} 条记录")

            return len(df)

        except Exception as e:
            error_msg = str(e)
            if "无权限" in error_msg or "权限" in error_msg or "403" in error_msg:
                print(f"  ⚠️  无权限获取现金流量表数据（需要2000+积分）")
            else:
                print(f"  ❌ 保存现金流量表失败: {e}")
            return 0

    def save_fina_indicator(
        self, ts_code: str, start_date: str = None, end_date: str = None
    ) -> int:
        """
        获取并保存财务指标数据

        Args:
            ts_code: 股票代码（如 000001.SZ 或 000001）
            start_date: 公告开始日期（格式 YYYYMMDD）
            end_date: 公告结束日期（格式 YYYYMMDD）

        Returns:
            保存的记录数，失败返回 0
        """
        try:
            # 标准化代码
            ts_code_std = self._standardize_code(ts_code)
            table_name = "fina_indicator"

            # 核心指标列（50个）
            core_columns = [
                # 基础字段
                "ts_code",
                "ann_date",
                "end_date",
                "report_type",
                # 盈利能力 (12个指标)
                "eps",
                "basic_eps",
                "diluted_eps",
                "roe",
                "roa",
                "roic",
                "netprofit_margin",
                "grossprofit_margin",
                "operateprofit_margin",
                "core_roe",
                "core_roa",
                "q_eps",
                # 成长能力 (10个指标)
                "or_yoy",
                "tr_yoy",
                "netprofit_yoy",
                "assets_yoy",
                "ebt_yoy",
                "ocf_yoy",
                "roe_yoy",
                "q_or_yoy",
                "q_tr_yoy",
                "q_netprofit_yoy",
                # 营运能力 (8个指标)
                "assets_turn",
                "ar_turn",
                "inv_turn",
                "ca_turn",
                "fa_turn",
                "current_assets_turn",
                "equity_turn",
                "op_npta",
                # 偿债能力 (8个指标)
                "current_ratio",
                "quick_ratio",
                "cash_ratio",
                "debt_to_assets",
                "debt_to_eqt",
                "equity_multiplier",
                "ebit_to_interest",
                "op_to_ebit",
                # 现金流指标 (7个指标)
                "ocfps",
                "ocf_to_debt",
                "ocf_to_shortdebt",
                "ocf_to_liability",
                "ocf_to_interest",
                "cf_to_debt",
                "free_cf",
                # 每股指标 (3个指标)
                "bps",
                "tangible_asset_to_share",
                "capital_reserv_to_share",
            ]

            # 获取数据
            print(f"  📥 获取财务指标数据 {ts_code_std}...")
            df = self._retry_api_call(
                self.pro.fina_indicator,
                ts_code=ts_code_std,
                start_date=start_date,
                end_date=end_date,
            )

            if df is None or df.empty:
                print(f"  ⚠️  {ts_code_std} 无财务指标数据")
                return 0

            # 添加 report_type 字段（API返回的数据中没有此字段）
            # 默认为 1 表示合并报表
            if "report_type" not in df.columns:
                df["report_type"] = 1

            # 去重处理
            df_before = len(df)
            df = self._smart_dedup_financial_data(df, "fina_indicator")
            if len(df) < df_before:
                print(f"  🔄 去除重复数据: {df_before} -> {len(df)} 条")

            # 选择核心指标列（只保留存在的列）
            available_columns = [col for col in core_columns if col in df.columns]
            df = df[available_columns]

            # 先删除该股票的所有旧数据（避免主键冲突）
            with self.engine.connect() as conn:
                delete_sql = "DELETE FROM fina_indicator WHERE ts_code = :ts_code"
                conn.execute(text(delete_sql), {"ts_code": ts_code_std})
                conn.commit()

            # 保存到数据库
            df.to_sql(
                table_name, self.engine, if_exists="append", index=False, method="multi"
            )
            print(f"  ✅ 已保存财务指标 {len(df)} 条记录")
            return len(df)

        except Exception as e:
            error_msg = str(e)
            # 权限不足时优雅降级
            if (
                "无权限" in error_msg
                or "权限" in error_msg
                or "403" in error_msg
                or "权限不足" in error_msg
            ):
                print(f"  ⚠️  无权限获取财务指标数据（需要2000+积分）")
            else:
                print(f"  ❌ 保存财务指标失败: {e}")
            return 0

    def check_fina_indicator_access(self) -> bool:
        """
        检查是否有财务指标接口访问权限

        Returns:
            True 表示有权限，False 表示无权限
        """
        try:
            test_df = self.pro.fina_indicator(ts_code="000001.SZ", limit=1)
            return test_df is not None and not test_df.empty
        except:
            return False

    def save_all_financial(
        self,
        ts_code: str,
        start_date: str = None,
        end_date: str = None,
        include_indicators: bool = True,
    ) -> dict:
        """
        获取并保存所有财务报表数据（利润表、资产负债表、现金流量表、财务指标）

        Args:
            ts_code: 股票代码（如 000001.SZ 或 000001）
            start_date: 公告开始日期（格式 YYYYMMDD）
            end_date: 公告结束日期（格式 YYYYMMDD）
            include_indicators: 是否包含财务指标（默认 True）

        Returns:
            包含各报表保存数量与现金回填统计的结果字典
        """
        result = {
            "ts_code": None,
            "total_records": 0,
            "income_count": 0,
            "balance_count": 0,
            "cashflow_count": 0,
            "indicator_count": 0,
            "cashflow_backfill": None,
        }

        try:
            # 标准化代码
            ts_code_std = self._standardize_code(ts_code)
            result["ts_code"] = ts_code_std
            print(f"\n{'=' * 60}")
            print(f"开始下载财务数据: {ts_code_std}")
            print(f"{'=' * 60}")

            # 1. 利润表
            income_count = self.save_income(ts_code_std, start_date, end_date)
            result["income_count"] = income_count
            result["total_records"] += income_count

            # 2. 资产负债表
            balance_count = self.save_balancesheet(ts_code_std, start_date, end_date)
            result["balance_count"] = balance_count
            result["total_records"] += balance_count

            # 3. 现金流量表
            cashflow_count = self.save_cashflow(ts_code_std, start_date, end_date)
            result["cashflow_count"] = cashflow_count
            result["cashflow_backfill"] = self._last_cashflow_backfill_stats
            result["total_records"] += cashflow_count

            # 4. 财务指标（可选）
            indicator_count = 0
            if include_indicators:
                indicator_count = self.save_fina_indicator(
                    ts_code_std, start_date, end_date
                )
                result["indicator_count"] = indicator_count
                result["total_records"] += indicator_count

            print(f"\n{'=' * 60}")
            print(f"✅ {ts_code_std} 财务数据下载完成")
            print(f"  利润表: {income_count} 条")
            print(f"  资产负债表: {balance_count} 条")
            print(f"  现金流量表: {cashflow_count} 条")
            if result["cashflow_backfill"]:
                backfill = result["cashflow_backfill"]
                print(
                    f"  end_bal_cash 回填: 成功 {backfill['filled']} / 缺失 {backfill['missing_candidates']}"
                )
            if include_indicators:
                print(f"  财务指标: {indicator_count} 条")
            print(f"  总计: {result['total_records']} 条")
            print(f"{'=' * 60}")

        except Exception as e:
            print(f"❌ {ts_code} 下载财务数据失败: {e}")

        return result

    def get_latest_financial_date(self, ts_code: str, table_type: str) -> str:
        """
        查询指定股票的最新财报日期（从统一表查询）

        Args:
            ts_code: 股票代码（如 000001.SZ 或 000001）
            table_type: 报表类型（income/balancesheet/cashflow/fina_indicator）

        Returns:
            最新公告日期（格式 YYYYMMDD），无数据则返回 None
        """
        try:
            # 标准化代码
            ts_code_std = self._standardize_code(ts_code)

            # 所有财务表都使用统一表名
            table_name = table_type

            # 检查表是否存在
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(
                        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                    )
                )
                if not result.fetchone():
                    return None

                # 查询最新日期
                query = f"""
                SELECT ann_date FROM {table_name}
                WHERE ts_code = :ts_code
                ORDER BY ann_date DESC LIMIT 1
                """
                df = pd.read_sql_query(query, conn, params={"ts_code": ts_code_std})

                if not df.empty:
                    return df["ann_date"].iloc[0]
                return None

        except Exception as e:
            print(f"  ⚠️  查询最新财报日期失败: {e}")
            return None

    # ==================== 指数数据相关方法 ====================

    def save_index_basic(self, market: str = None):
        """
        获取并保存指数基本信息

        Args:
            market: 市场代码 ('SSE' 上交所, 'SZSE' 深交所)，None 表示全部

        Returns:
            保存的指数数量（总是返回正数，表示数据库中的指数数量）
        """
        # 先获取数据
        print(f"  📥 获取指数基本信息 (market={market or '全部'})...")
        df = self._retry_api_call(self.pro.index_basic, market=market or "")

        if df is None or df.empty:
            print(f"  ⚠️  无指数基本信息")
            # 即使 API 返回空，也检查数据库中是否已有数据
            with self.engine.connect() as conn:
                query = "SELECT COUNT(*) FROM index_names"
                if market == "SSE":
                    query += " WHERE ts_code LIKE '%.SH'"
                elif market == "SZSE":
                    query += " WHERE ts_code LIKE '%.SZ'"
                result = conn.execute(text(query))
                count = result.fetchone()[0]
                return count

        # 准备数据
        df = df.copy()
        df["updated_at"] = datetime.now().isoformat()

        # 尝试保存到数据库
        try:
            df.to_sql(
                "index_names",
                self.engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            print(f"  ✅ 已保存 {len(df)} 条指数基本信息")
            return len(df)
        except Exception as e:
            error_msg = str(e)
            if "UNIQUE constraint" in error_msg or "duplicate" in error_msg.lower():
                # 数据已存在，不需要更新（基本信息通常不变）
                # 直接返回数据库中的数量
                with self.engine.connect() as conn:
                    query = "SELECT COUNT(*) FROM index_names"
                    if market == "SSE":
                        query += " WHERE ts_code LIKE '%.SH'"
                    elif market == "SZSE":
                        query += " WHERE ts_code LIKE '%.SZ'"
                    result = conn.execute(text(query))
                    count = result.fetchone()[0]
                print(f"  ℹ️  指数基本信息已存在，数据库中共有 {count} 条")
                return count
            else:
                print(f"  ❌ 保存指数基本信息失败: {e}")
                return 0

    def save_index_daily(
        self, ts_code: str, start_date: str = "20200101", end_date: str = None
    ):
        """
        保存指数日线数据

        Args:
            ts_code: 指数代码（如 000001.SH）
            start_date: 开始日期，格式 YYYYMMDD
            end_date: 结束日期，格式 YYYYMMDD，None则使用今天

        Returns:
            保存的记录数，失败返回 0
        """
        try:
            # 如果未指定结束日期，使用当前日期
            if end_date is None:
                end_date = datetime.today().strftime("%Y%m%d")

            # 标准化代码
            if "." not in ts_code:
                raise ValueError(f"指数代码格式错误: {ts_code}，应为 000001.SH 格式")

            # 获取指数日线数据
            print(f"  📥 获取指数日线数据 {ts_code} ({start_date} - {end_date})...")
            df = self._retry_api_call(
                self.pro.index_daily,
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
            )

            if df is None or df.empty:
                print(f"  ⚠️  {ts_code} 无指数日线数据")
                return 0

            # 重命名列以匹配 bars 表结构
            df = df.rename(columns={"trade_date": "datetime", "vol": "volume"})

            # 添加元数据
            # 指数使用完整的 ts_code 作为 symbol（如 000001.SH），避免与股票代码冲突
            df["symbol"] = ts_code
            df["exchange"] = self._detect_exchange(ts_code)
            df["interval"] = "1d"
            df["datetime"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d")

            # 指数数据没有的股票字段，设为 None
            stock_only_fields = [
                "open_qfq",
                "high_qfq",
                "low_qfq",
                "close_qfq",  # 前复权价格
                "turnover",  # 换手率
                # 估值指标
                "pe",
                "pe_ttm",
                "pb",
                "ps",
                "ps_ttm",
                # 市值指标
                "total_mv",
                "circ_mv",
                # 股本结构
                "total_share",
                "float_share",
                "free_share",
                # 流动性指标
                "volume_ratio",
                "turnover_rate_f",
                # 分红指标
                "dv_ratio",
                "dv_ttm",
            ]
            for field in stock_only_fields:
                df[field] = None

            # 选择要保存的列
            columns = [
                "symbol",
                "exchange",
                "interval",
                "datetime",
                "open",
                "high",
                "low",
                "close",
                "open_qfq",
                "high_qfq",
                "low_qfq",
                "close_qfq",
                "pre_close",
                "change",
                "pct_chg",
                "volume",
                "turnover",
                "amount",
                # Daily basic 指标
                "turnover_rate_f",
                "volume_ratio",
                "pe",
                "pe_ttm",
                "pb",
                "ps",
                "ps_ttm",
                "total_mv",
                "circ_mv",
                "total_share",
                "float_share",
                "free_share",
                "dv_ratio",
                "dv_ttm",
            ]

            # 确保所有列都存在
            for col in columns:
                if col not in df.columns:
                    df[col] = None

            # 保存到数据库
            df[columns].to_sql(
                "bars", self.engine, if_exists="append", index=False, method="multi"
            )
            print(f"  ✅ 已保存 {ts_code} 共 {len(df)} 条记录")
            return len(df)

        except Exception as e:
            # 数据库操作失败
            if "UNIQUE constraint" in str(e) or "duplicate" in str(e).lower():
                # 数据已存在，跳过
                print(f"  ⏭️  {ts_code} 指数数据已存在，跳过")
                return 0
            else:
                print(f"  ❌ {ts_code} 指数数据保存失败: {e}")
                return 0

    def save_all_indices(
        self, start_date: str = "20240101", end_date: str = None, markets: list = None
    ):
        """
        批量下载所有指数数据

        Args:
            start_date: 开始日期，格式 YYYYMMDD
            end_date: 结束日期，格式 YYYYMMDD，None则使用今天
            markets: 市场列表 ['SSE', 'SZSE']，None则表示全部

        Returns:
            统计信息字典
        """
        # 如果未指定结束日期，使用当前日期
        if end_date is None:
            end_date = datetime.today().strftime("%Y%m%d")

        # 默认市场
        if markets is None:
            markets = ["SSE", "SZSE"]

        # 第一步：获取指数基本信息
        print("📋 正在获取指数列表...")
        all_indices = []

        for market in markets:
            try:
                count = self.save_index_basic(market=market)
                if count > 0:
                    # 从数据库读取指数代码
                    query = "SELECT ts_code FROM index_names"
                    if market == "SSE":
                        query += " WHERE ts_code LIKE '%.SH'"
                    elif market == "SZSE":
                        query += " WHERE ts_code LIKE '%.SZ'"

                    with self.engine.connect() as conn:
                        df = pd.read_sql_query(query, conn)
                        all_indices.extend(df["ts_code"].tolist())
            except Exception as e:
                print(f"  ❌ 获取 {market} 指数列表失败: {e}")

        if not all_indices:
            print("❌ 没有找到指数")
            return {"total": 0, "success": 0, "failed": 0}

        # 去重
        all_indices = list(set(all_indices))
        print(f"📋 共 {len(all_indices)} 个指数")

        # 第二步：逐个下载指数行情数据
        stats = {"total": len(all_indices), "success": 0, "failed": 0, "skipped": 0}

        for i, ts_code in enumerate(all_indices):
            # 定期显示进度
            if (i + 1) % 10 == 1 or i == len(all_indices) - 1:
                print(f"\n{'=' * 60}")
                print(f"进度: [{i + 1}/{stats['total']}]")
                print(
                    f"成功: {stats['success']} | 失败: {stats['failed']} | 跳过: {stats['skipped']}"
                )
                print(f"{'=' * 60}")

            try:
                result = self.save_index_daily(ts_code, start_date, end_date)
                if result > 0:
                    stats["success"] += 1
                elif result == 0:
                    stats["skipped"] += 1
                else:
                    stats["failed"] += 1
            except Exception as e:
                print(f"  ❌ {ts_code} 处理失败: {e}")
                stats["failed"] += 1

        # 输出统计信息
        print(f"\n{'=' * 60}")
        print(f"指数数据下载完成:")
        print(f"  总计: {stats['total']} 个指数")
        print(f"  成功: {stats['success']}")
        print(f"  失败: {stats['failed']}")
        print(f"  跳过: {stats['skipped']}")
        print(f"{'=' * 60}")

        return stats

    # ==================== 申万行业分类相关方法 ====================

    def save_sw_classify(
        self, src: str = "SW2021", level: str = None, update_timestamp: bool = True
    ) -> int:
        """
        获取并保存申万行业分类数据

        Args:
            src: 行业分类来源，SW2014=申万2014版本，SW2021=申万2021版本（默认）
            level: 行业分级，L1=一级，L2=二级，L3=三级，None=全部
            update_timestamp: 是否更新时间戳，False时保留旧时间戳用于增量更新

        Returns:
            保存的记录数
        """
        try:
            print(f"  📥 获取申万行业分类数据 (src={src}, level={level or '全部'})...")

            # 获取数据
            params = {"src": src}
            if level:
                params["level"] = level

            df = self._retry_api_call(self.pro.index_classify, **params)

            if df is None or df.empty:
                print(f"  ⚠️  无申万行业分类数据")
                return 0

            # 准备数据
            df = df.copy()
            df["src"] = src
            df["updated_at"] = datetime.now().isoformat()

            # 选择列并保存
            columns = [
                "index_code",
                "industry_name",
                "parent_code",
                "level",
                "industry_code",
                "is_pub",
                "src",
                "updated_at",
            ]

            # 确保所有列都存在
            for col in columns:
                if col not in df.columns:
                    df[col] = None

            df = df[columns]

            if update_timestamp:
                # 删除旧数据并重新插入
                delete_sql = "DELETE FROM sw_classify WHERE src = :src"
                with self.engine.connect() as conn:
                    conn.execute(text(delete_sql), {"src": src})
                    conn.commit()

                df.to_sql(
                    "sw_classify",
                    self.engine,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
            else:
                # 使用 upsert 保留旧时间戳
                upsert_sql = """
                INSERT INTO sw_classify (index_code, industry_name, parent_code, level, industry_code, is_pub, src, updated_at)
                VALUES (:index_code, :industry_name, :parent_code, :level, :industry_code, :is_pub, :src, :updated_at)
                ON CONFLICT(index_code) DO UPDATE SET
                    industry_name = :industry_name,
                    parent_code = :parent_code,
                    level = :level,
                    industry_code = :industry_code,
                    is_pub = :is_pub,
                    src = :src
                """

                with self.engine.connect() as conn:
                    for _, row in df.iterrows():
                        conn.execute(
                            text(upsert_sql),
                            {
                                "index_code": row["index_code"],
                                "industry_name": row["industry_name"],
                                "parent_code": row["parent_code"],
                                "level": row["level"],
                                "industry_code": row["industry_code"],
                                "is_pub": row["is_pub"],
                                "src": row["src"],
                                "updated_at": row["updated_at"],
                            },
                        )
                    conn.commit()

            print(f"  ✅ 已保存申万行业分类 {len(df)} 条记录")
            return len(df)

        except Exception as e:
            error_msg = str(e)
            if "无权限" in error_msg or "权限" in error_msg or "403" in error_msg:
                print(f"  ⚠️  无权限获取申万行业分类数据（需要2000+积分）")
            else:
                print(f"  ❌ 保存申万行业分类失败: {e}")
            return 0

    def save_sw_members(
        self,
        index_code: str = None,
        ts_code: str = None,
        is_new: str = "Y",
        force_update: bool = False,
    ) -> int:
        """
        获取并保存申万行业成分股数据

        Args:
            index_code: 行业指数代码，None表示获取所有
            ts_code: 股票代码，与index_code二选一
            is_new: 是否最新成分，Y=是（默认），N=否
            force_update: 是否强制更新（删除旧数据）

        Returns:
            保存的记录数

        注意：Tushare的index_member_all接口返回的是每只股票及其所属行业信息
              每行包含l1_code, l2_code, l3_code，需要展开为多条记录
        """
        try:
            # 构建查询参数 - 只传is_new，不传index_code（API不会按index_code过滤）
            params = {"is_new": is_new}
            if ts_code:
                params["ts_code"] = ts_code

            desc = (
                f"index_code={index_code}"
                if index_code
                else f"ts_code={ts_code}"
                if ts_code
                else "全部"
            )
            print(f"  📥 获取申万行业成分股数据 ({desc}, is_new={is_new})...")

            # 获取数据（一次性获取所有股票的行业信息）
            df = self._retry_api_call(self.pro.index_member_all, **params)

            if df is None or df.empty:
                print(f"  ⚠️  无申万行业成分股数据")
                return 0

            # 将宽格式转换为长格式（每个股票-行业对一条记录）
            records = []
            for _, row in df.iterrows():
                ts_code = row["ts_code"]
                name = row["name"]
                in_date = row["in_date"]
                out_date = row["out_date"]

                # 为每个非空行业代码创建一条记录
                for level in ["l3", "l2", "l1"]:  # 优先三级行业
                    code_col = f"{level}_code"
                    name_col = f"{level}_name"

                    if pd.notna(row.get(code_col)):
                        records.append(
                            {
                                "index_code": row[code_col],
                                "ts_code": ts_code,
                                "name": name,
                                "in_date": in_date,
                                "out_date": out_date,
                                "is_new": is_new,
                            }
                        )
                        # 如果指定了index_code，找到匹配后就不再处理更低级别的行业
                        if index_code and row[code_col] == index_code:
                            break

            # 如果指定了index_code，过滤出该行业的记录
            if index_code:
                records = [r for r in records if r["index_code"] == index_code]

                # 删除该行业的旧数据
                if force_update and records:
                    delete_sql = "DELETE FROM sw_members WHERE index_code = :index_code"
                    with self.engine.connect() as conn:
                        conn.execute(text(delete_sql), {"index_code": index_code})
                        conn.commit()

            if not records:
                print(f"  ⚠️  无符合条件的申万行业成分股数据")
                return 0

            # 保存到数据库 - 使用 upsert 避免重复插入
            upsert_sql = """
            INSERT INTO sw_members (index_code, ts_code, name, in_date, out_date, is_new)
            VALUES (:index_code, :ts_code, :name, :in_date, :out_date, :is_new)
            ON CONFLICT(index_code, ts_code) DO UPDATE SET
                name = :name,
                in_date = :in_date,
                out_date = :out_date,
                is_new = :is_new
            """

            with self.engine.connect() as conn:
                for record in records:
                    conn.execute(text(upsert_sql), record)
                conn.commit()

            print(f"  ✅ 已保存申万行业成分股 {len(records)} 条记录")
            return len(records)

        except Exception as e:
            error_msg = str(e)
            if "无权限" in error_msg or "权限" in error_msg or "403" in error_msg:
                print(f"  ⚠️  无权限获取申万行业成分股数据（需要2000+积分）")
            else:
                print(f"  ❌ 保存申万行业成分股失败: {e}")
            import traceback

            traceback.print_exc()
            return 0

    def save_sw_members_all(self, is_new: str = "Y", src: str = "SW2021") -> int:
        """
        获取并保存所有股票的申万行业成分股数据（完整版）

        通过遍历一级行业代码来分批获取数据。
        使用 l1_code 参数获取每个一级行业的成分股。

        Args:
            is_new: 是否最新成分，Y=是（默认），N=否
            src: 行业分类来源，SW2021 或 SW2014

        Returns:
            保存的记录数
        """
        import pandas as pd
        from sqlalchemy import text

        try:
            # 获取所有一级行业代码
            print(f"  📋 获取一级行业列表...")
            l1_industries = pd.read_sql_query(
                "SELECT index_code, industry_name FROM sw_classify WHERE level = 'L1' AND src = :src ORDER BY index_code",
                self.engine,
                params={"src": src},
            )

            if l1_industries.empty:
                print(f"  ⚠️  无行业分类数据")
                return 0

            l1_codes = l1_industries["index_code"].tolist()
            print(f"  📊 共 {len(l1_codes)} 个一级行业需要查询")

            # 遍历每个一级行业获取成分股
            all_records = []
            failed_industries = []
            seen_stocks = set()  # 用于跟踪已处理的股票

            for i, index_code in enumerate(l1_codes):
                industry_name = l1_industries[
                    l1_industries["index_code"] == index_code
                ]["industry_name"].values[0]
                try:
                    if (i + 1) % 5 == 0 or i == 0:
                        print(
                            f"  🔄 正在查询 [{i + 1}/{len(l1_codes)}] {industry_name} ({index_code})"
                        )

                    # 使用 l1_code 参数查询该一级行业的所有成分股
                    df = self._retry_api_call(
                        self.pro.index_member_all,
                        l1_code=index_code,  # 注意：使用 l1_code 而不是 index_code
                        is_new=is_new,
                    )

                    if df is None or df.empty:
                        # 该行业可能没有成分股数据
                        continue

                    # 将宽格式转换为长格式（每个股票-行业对一条记录）
                    for _, row in df.iterrows():
                        ts_code = row["ts_code"]
                        name = row["name"]
                        in_date = row["in_date"]
                        out_date = row["out_date"]

                        # 跳过已处理的股票（避免重复）
                        if ts_code in seen_stocks:
                            continue
                        seen_stocks.add(ts_code)

                        # 为每个非空行业代码创建一条记录（L1, L2, L3）
                        for level in ["l3", "l2", "l1"]:
                            code_col = f"{level}_code"
                            if pd.notna(row.get(code_col)):
                                all_records.append(
                                    {
                                        "index_code": row[code_col],
                                        "ts_code": ts_code,
                                        "name": name,
                                        "in_date": in_date,
                                        "out_date": out_date,
                                        "is_new": is_new,
                                    }
                                )

                except Exception as e:
                    failed_industries.append((index_code, industry_name, str(e)))
                    print(f"  ⚠️  查询 {industry_name} ({index_code}) 失败: {e}")

            if failed_industries:
                print(f"  ⚠️  共 {len(failed_industries)} 个行业查询失败")

            if not all_records:
                print(f"  ⚠️  无申万行业成分股数据")
                return 0

            # 保存到数据库
            print(f"  💾 正在保存 {len(all_records)} 条记录...")

            upsert_sql = """
            INSERT INTO sw_members (index_code, ts_code, name, in_date, out_date, is_new)
            VALUES (:index_code, :ts_code, :name, :in_date, :out_date, :is_new)
            ON CONFLICT(index_code, ts_code) DO UPDATE SET
                name = :name,
                in_date = :in_date,
                out_date = :out_date,
                is_new = :is_new
            """

            with self.engine.connect() as conn:
                for record in all_records:
                    conn.execute(text(upsert_sql), record)
                conn.commit()

            unique_stocks = len(seen_stocks)
            print(f"  ✅ 已保存申万行业成分股 {len(all_records)} 条记录")
            print(f"  📈 覆盖 {unique_stocks} 只股票")
            return len(all_records)

        except Exception as e:
            error_msg = str(e)
            if "无权限" in error_msg or "权限" in error_msg or "403" in error_msg:
                print(f"  ⚠️  无权限获取申万行业成分股数据（需要2000+积分）")
            else:
                print(f"  ❌ 保存申万行业成分股失败: {e}")
            import traceback

            traceback.print_exc()
            return 0

    def get_outdated_indices(self, src: str = "SW2021", days: int = 7) -> list:
        """
        获取需要更新的行业代码列表（根据 updated_at 判断）

        Args:
            src: 行业分类来源
            days: 超过多少天未更新则需要更新

        Returns:
            需要更新的行业代码列表
        """
        import pandas as pd

        cutoff_date = (datetime.now() - pd.Timedelta(days=days)).isoformat()

        query = """
        SELECT index_code FROM sw_classify
        WHERE src = :src
        AND (updated_at IS NULL OR updated_at < :cutoff_date)
        """

        with self.engine.connect() as conn:
            df = pd.read_sql_query(
                query, conn, params={"src": src, "cutoff_date": cutoff_date}
            )

        return df["index_code"].tolist() if not df.empty else []

    def update_indices_timestamp(self, index_codes: list, src: str = "SW2021"):
        """
        更新指定行业的 updated_at 时间戳

        Args:
            index_codes: 行业代码列表
            src: 行业分类来源
        """
        if not index_codes:
            return

        now = datetime.now().isoformat()
        placeholders = ",".join([f":code{i}" for i in range(len(index_codes))])
        params = {f"code{i}": code for i, code in enumerate(index_codes)}
        params["src"] = src
        params["now"] = now

        update_sql = f"""
        UPDATE sw_classify
        SET updated_at = :now
        WHERE index_code IN ({placeholders})
        AND src = :src
        """

        with self.engine.connect() as conn:
            conn.execute(text(update_sql), params)
            conn.commit()

    def save_all_sw_industry(
        self,
        src: str = "SW2021",
        is_new: str = "Y",
        force_update: bool = False,
        incremental: bool = False,
        incremental_days: int = 7,
    ) -> dict:
        """
        获取并保存所有申万行业分类和成分股数据

        Args:
            src: 行业分类来源，SW2014=申万2014版本，SW2021=申万2021版本（默认）
            is_new: 是否最新成分，Y=是（默认），N=否
            force_update: 是否强制更新
            incremental: 是否增量更新（只更新超过指定天数的行业）
            incremental_days: 增量更新时，超过多少天未更新则需要更新

        Returns:
            统计信息字典
        """
        print(f"\n{'=' * 60}")
        if incremental:
            print(f"开始增量更新申万行业数据 (src={src}, days={incremental_days})")
        else:
            print(f"开始获取申万行业数据 (src={src})")
        print(f"{'=' * 60}")

        stats = {
            "classify_count": 0,
            "members_count": 0,
            "total_indices": 0,
            "skipped_indices": 0,
            "failed_indices": [],
        }

        # 1. 获取行业分类（增量模式下不更新时间戳）
        print("\n1. 获取申万行业分类...")
        update_ts = not incremental  # 增量模式下不更新时间戳
        classify_count = self.save_sw_classify(src=src, update_timestamp=update_ts)
        stats["classify_count"] = classify_count

        if classify_count == 0:
            print("❌ 获取行业分类失败")
            return stats

        # 2. 获取需要更新的行业代码
        if incremental:
            outdated_indices = self.get_outdated_indices(src=src, days=incremental_days)
            if not outdated_indices:
                print(f"\n✅ 所有行业数据都是最新的（{incremental_days}天内已更新）")
                return stats

            all_indices = outdated_indices
            print(
                f"\n2. 增量更新行业成分股（{len(all_indices)}/{classify_count} 个行业需要更新）..."
            )
        else:
            query = "SELECT index_code FROM sw_classify WHERE src = :src"
            with self.engine.connect() as conn:
                df_indices = pd.read_sql_query(query, conn, params={"src": src})

            if df_indices.empty:
                print("❌ 没有找到行业分类")
                return stats

            all_indices = df_indices["index_code"].tolist()
            print(f"\n2. 获取行业成分股（共 {len(all_indices)} 个行业）...")

        stats["total_indices"] = len(all_indices)

        # 3. 遍历每个行业获取成分股
        updated_indices = []  # 记录成功更新的行业
        for i, index_code in enumerate(all_indices):
            # 定期显示进度
            if (i + 1) % 20 == 1 or i == len(all_indices) - 1:
                print(f"\n{'=' * 60}")
                print(f"进度: [{i + 1}/{stats['total_indices']}]")
                print(
                    f"成功: {stats['members_count']} | 失败: {len(stats['failed_indices'])}"
                )
                print(f"{'=' * 60}")

            try:
                count = self.save_sw_members(
                    index_code=index_code, is_new=is_new, force_update=force_update
                )
                if count > 0:
                    stats["members_count"] += count
                    updated_indices.append(index_code)
                else:
                    stats["failed_indices"].append(index_code)

            except Exception as e:
                print(f"  ❌ {index_code} 处理失败: {e}")
                stats["failed_indices"].append(index_code)

        # 4. 更新成功更新的行业的时间戳
        if incremental and updated_indices:
            self.update_indices_timestamp(updated_indices, src=src)

        # 5. 输出统计信息
        print(f"\n{'=' * 60}")
        if incremental:
            print(f"申万行业数据增量更新完成:")
            print(f"  总行业数: {classify_count} 个")
            print(f"  需要更新: {stats['total_indices']} 个")
            print(f"  跳过: {classify_count - stats['total_indices']} 个（已最新）")
        else:
            print(f"申万行业数据获取完成:")
            print(f"  行业分类: {stats['classify_count']} 条")
        print(f"  成分股: {stats['members_count']} 条")
        if stats["failed_indices"]:
            print(f"  失败行业: {len(stats['failed_indices'])} 个")
        print(f"{'=' * 60}")

        return stats

    def get_sw_industry_members(self, index_code: str) -> pd.DataFrame:
        """
        从数据库获取指定申万行业的成分股

        Args:
            index_code: 行业指数代码

        Returns:
            成分股DataFrame
        """
        query = """
        SELECT m.index_code, c.industry_name, c.level, m.ts_code, m.name, m.in_date, m.out_date, m.is_new
        FROM sw_members m
        JOIN sw_classify c ON m.index_code = c.index_code
        WHERE m.index_code = :index_code
        ORDER BY m.ts_code
        """
        return pd.read_sql_query(query, self.engine, params={"index_code": index_code})

    def get_stock_sw_industry(self, ts_code: str) -> pd.DataFrame:
        """
        从数据库获取指定股票所属的申万行业

        Args:
            ts_code: 股票代码（如 000001.SZ）

        Returns:
            行业信息DataFrame
        """
        query = """
        SELECT m.index_code, c.industry_name, c.level, c.parent_code, m.ts_code, m.name, m.in_date, m.out_date, m.is_new
        FROM sw_members m
        JOIN sw_classify c ON m.index_code = c.index_code
        WHERE m.ts_code = :ts_code AND m.is_new = 'Y'
        ORDER BY c.level
        """
        return pd.read_sql_query(query, self.engine, params={"ts_code": ts_code})

    def get_sw_classify(self, src: str = "SW2021", level: str = None) -> pd.DataFrame:
        """
        从数据库获取申万行业分类

        Args:
            src: 行业分类来源
            level: 行业级别，None=全部

        Returns:
            行业分类DataFrame
        """
        query = "SELECT * FROM sw_classify WHERE src = :src"
        params = {"src": src}

        if level:
            query += " AND level = :level"
            params["level"] = level

        query += " ORDER BY industry_code"

        return pd.read_sql_query(query, self.engine, params=params)

    # ==================== 资金流向相关方法 ====================

    def _create_moneyflow_tables(self):
        """创建资金流向相关表"""
        # 创建个股资金流向表
        stock_moneyflow_sql = """
        CREATE TABLE IF NOT EXISTS stock_moneyflow (
            ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,

            -- 小单
            buy_sm_vol INTEGER,
            buy_sm_amount REAL,
            sell_sm_vol INTEGER,
            sell_sm_amount REAL,

            -- 中单
            buy_md_vol INTEGER,
            buy_md_amount REAL,
            sell_md_vol INTEGER,
            sell_md_amount REAL,

            -- 大单
            buy_lg_vol INTEGER,
            buy_lg_amount REAL,
            sell_lg_vol INTEGER,
            sell_lg_amount REAL,

            -- 特大单
            buy_elg_vol INTEGER,
            buy_elg_amount REAL,
            sell_elg_vol INTEGER,
            sell_elg_amount REAL,

            -- 净流向
            net_mf_vol INTEGER,
            net_mf_amount REAL,

            -- 计算字段（方便查询）
            net_lg_amount REAL,
            net_elg_amount REAL,

            PRIMARY KEY (ts_code, trade_date)
        );
        """
        # 创建行业资金流向汇总表
        industry_moneyflow_sql = """
        CREATE TABLE IF NOT EXISTS industry_moneyflow (
            trade_date TEXT NOT NULL,
            level TEXT NOT NULL,
            sw_l1 TEXT,
            sw_l2 TEXT,
            sw_l3 TEXT,
            index_code TEXT,

            -- 成分股统计
            stock_count INTEGER,
            up_count INTEGER,
            down_count INTEGER,
            limit_up_count INTEGER,
            limit_down_count INTEGER,

            -- 资金流向汇总（万元）
            net_mf_amount REAL,
            net_lg_amount REAL,
            net_elg_amount REAL,
            buy_elg_amount REAL,
            sell_elg_amount REAL,
            buy_lg_amount REAL,
            sell_lg_amount REAL,

            -- 平均值（万元/股）
            avg_net_amount REAL,
            avg_net_lg_amount REAL,
            avg_net_elg_amount REAL,

            updated_at TEXT,

            PRIMARY KEY (trade_date, level, sw_l1, sw_l2, sw_l3)
        );
        """

        # 创建索引
        index_sql_1 = "CREATE INDEX IF NOT EXISTS idx_moneyflow_date ON stock_moneyflow(trade_date);"
        index_sql_2 = "CREATE INDEX IF NOT EXISTS idx_ind_moneyflow_date ON industry_moneyflow(trade_date);"
        index_sql_3 = "CREATE INDEX IF NOT EXISTS idx_ind_moneyflow_level ON industry_moneyflow(level);"

        with self.engine.connect() as conn:
            from sqlalchemy import text

            conn.execute(text(stock_moneyflow_sql))
            conn.execute(text(industry_moneyflow_sql))
            conn.execute(text(index_sql_1))
            conn.execute(text(index_sql_2))
            conn.execute(text(index_sql_3))
            conn.commit()

    def save_moneyflow(
        self, ts_code: str, start_date: str = None, end_date: str = None
    ) -> int:
        """
        获取单只股票的资金流向数据

        Args:
            ts_code: 股票代码 (如 600382.SH)
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            保存的记录数
        """
        try:
            # 确保表存在
            self._create_moneyflow_tables()

            # 标准化股票代码
            ts_code = self._standardize_code(ts_code)

            # 获取数据
            params = {"ts_code": ts_code}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date

            df = self._retry_api_call(self.pro.moneyflow, **params)

            if df is None or df.empty:
                return 0

            # 计算净流向字段
            df["net_lg_amount"] = df["buy_lg_amount"] - df["sell_lg_amount"]
            df["net_elg_amount"] = df["buy_elg_amount"] - df["sell_elg_amount"]

            # 删除该股票在指定日期范围内的已有数据（避免主键冲突）
            if start_date or end_date:
                with self.engine.connect() as conn:
                    delete_query = text("""
                        DELETE FROM stock_moneyflow
                        WHERE ts_code = :ts_code
                        AND (:start_date IS NULL OR trade_date >= :start_date)
                        AND (:end_date IS NULL OR trade_date <= :end_date)
                    """)
                    result = conn.execute(
                        delete_query,
                        {
                            "ts_code": ts_code,
                            "start_date": start_date,
                            "end_date": end_date,
                        },
                    )
                    deleted_count = result.rowcount
                    conn.commit()

            # 保存数据
            df.to_sql(
                "stock_moneyflow",
                self.engine,
                if_exists="append",
                index=False,
                method="multi",
            )

            return len(df)

        except Exception as e:
            print(f"获取 {ts_code} 资金流向数据失败: {e}")
            return 0

    def save_moneyflow_by_date(self, trade_date: str, exclude_st: bool = True) -> int:
        """
        获取指定日期的所有股票资金流向数据

        Args:
            trade_date: 交易日期 YYYYMMDD
            exclude_st: 是否排除ST股（默认True）

        Returns:
            保存的记录数
        """
        try:
            # 确保表存在
            self._create_moneyflow_tables()

            # 获取数据
            df = self._retry_api_call(self.pro.moneyflow, trade_date=trade_date)

            if df is None or df.empty:
                return 0

            # 排除ST股（从 st_stocks 表获取 ST 股票列表）
            if exclude_st:
                with self.engine.connect() as conn:
                    st_codes_result = conn.execute(
                        text("SELECT ts_code FROM st_stocks")
                    ).fetchall()
                    st_codes = [row[0] for row in st_codes_result]
                    if st_codes:
                        before_count = len(df)
                        df = df[~df["ts_code"].isin(st_codes)]
                        after_count = len(df)
                        if before_count > after_count:
                            print(f"  排除 ST 股票: {before_count - after_count} 只")

            # 计算净流向字段
            df["net_lg_amount"] = df["buy_lg_amount"] - df["sell_lg_amount"]
            df["net_elg_amount"] = df["buy_elg_amount"] - df["sell_elg_amount"]

            # 删除该日期的已有数据（避免主键冲突）
            with self.engine.connect() as conn:
                delete_query = text("""
                    DELETE FROM stock_moneyflow
                    WHERE trade_date = :trade_date
                """)
                result = conn.execute(delete_query, {"trade_date": trade_date})
                deleted_count = result.rowcount
                conn.commit()

            # 保存数据
            df.to_sql(
                "stock_moneyflow",
                self.engine,
                if_exists="append",
                index=False,
                method="multi",
            )

            return len(df)

        except Exception as e:
            print(f"获取 {trade_date} 资金流向数据失败: {e}")
            return 0

    def save_all_moneyflow_incremental(
        self, start_date: str = None, exclude_st: bool = True
    ) -> dict:
        """
        增量更新所有股票资金流向数据

        Args:
            start_date: 起始日期，默认从数据库最新日期的下一天开始
                      如果数据库为空，则从1年前开始
            exclude_st: 是否排除ST股（默认True）

        Returns:
            统计信息字典
        """
        stats = {"success": 0, "failed": 0, "skipped": 0}

        try:
            # 确保表存在
            self._create_moneyflow_tables()

            # 获取数据库最新日期
            if start_date is None:
                query = text("SELECT MAX(trade_date) as max_date FROM stock_moneyflow")
                with self.engine.connect() as conn:
                    result = conn.execute(query).fetchone()
                    if result and result[0]:
                        # 从最新日期的下一天开始
                        from datetime import datetime, timedelta

                        last_date = datetime.strptime(result[0], "%Y%m%d")
                        start_date = (last_date + timedelta(days=1)).strftime("%Y%m%d")

            # 如果没有指定开始日期且数据库为空，使用1年前作为默认值
            if start_date is None:
                from datetime import datetime, timedelta

                one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
                start_date = one_year_ago

            # 获取交易日期列表
            from datetime import datetime, timedelta

            start = datetime.strptime(start_date, "%Y%m%d")
            end = datetime.now()

            # 获取交易日历
            trade_dates = self.pro.trade_cal(
                exchange="SSE", start_date=start_date, end_date=end.strftime("%Y%m%d")
            )
            trade_dates = trade_dates[trade_dates["is_open"] == 1]["cal_date"].tolist()

            print(
                f"增量更新资金流向数据: {start_date} 至今，共 {len(trade_dates)} 个交易日"
            )

            # 遍历每个交易日
            for trade_date in trade_dates:
                try:
                    count = self.save_moneyflow_by_date(
                        trade_date, exclude_st=exclude_st
                    )
                    if count > 0:
                        stats["success"] += count
                        print(f"  ✓ {trade_date}: {count} 条记录")
                    else:
                        stats["skipped"] += 1
                        print(f"  - {trade_date}: 无数据")
                    self._wait_for_rate_limit()
                except Exception as e:
                    stats["failed"] += 1
                    print(f"  ✗ {trade_date}: 失败 - {e}")

        except Exception as e:
            print(f"增量更新资金流向数据失败: {e}")

        return stats

    def save_moneyflow_by_stocks(
        self,
        stock_list: list,
        start_date: str = None,
        end_date: str = None,
        exclude_st: bool = True,
    ) -> dict:
        """
        按股票列表获取资金流向数据（适用于收藏列表或自定义股票）

        Args:
            stock_list: 股票代码列表
            start_date: 开始日期 YYYYMMDD（默认从1年前开始）
            end_date: 结束日期 YYYYMMDD（默认今天）
            exclude_st: 是否排除ST股（默认True）

        Returns:
            统计信息字典
        """
        stats = {"success": 0, "failed": 0, "skipped": 0}

        try:
            from datetime import datetime, timedelta

            # 确保表存在
            self._create_moneyflow_tables()

            # 设置默认日期范围
            if end_date is None:
                end_date = datetime.now().strftime("%Y%m%d")

            if start_date is None:
                # 默认从1年前开始
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")

            print(
                f"按股票列表获取资金流向: {len(stock_list)} 只股票, {start_date} - {end_date}"
            )

            # 遍历股票列表
            for i, stock_code in enumerate(stock_list, 1):
                try:
                    # 标准化股票代码
                    ts_code = self._standardize_code(stock_code)

                    # 获取资金流向数据
                    df = self._retry_api_call(
                        self.pro.moneyflow,
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date,
                    )

                    if df is None or df.empty:
                        stats["skipped"] += 1
                        print(f"  [{i}/{len(stock_list)}] {stock_code}: 无数据")
                        continue

                    # 排除ST股
                    if exclude_st and "name" in df.columns:
                        df = df[~df["name"].str.startswith("ST")]
                        df = df[~df["name"].str.startswith("*ST")]

                    # 计算净流向字段
                    df["net_lg_amount"] = df["buy_lg_amount"] - df["sell_lg_amount"]
                    df["net_elg_amount"] = df["buy_elg_amount"] - df["sell_elg_amount"]

                    # 删除该股票在指定日期范围内的已有数据（避免主键冲突）
                    with self.engine.connect() as conn:
                        delete_query = text("""
                            DELETE FROM stock_moneyflow
                            WHERE ts_code = :ts_code
                            AND trade_date BETWEEN :start_date AND :end_date
                        """)
                        result = conn.execute(
                            delete_query,
                            {
                                "ts_code": ts_code,
                                "start_date": start_date,
                                "end_date": end_date,
                            },
                        )
                        deleted_count = result.rowcount
                        conn.commit()

                    # 保存数据
                    df.to_sql(
                        "stock_moneyflow",
                        self.engine,
                        if_exists="append",
                        index=False,
                        method="multi",
                    )

                    stats["success"] += len(df)
                    print(
                        f"  ✓ [{i}/{len(stock_list)}] {stock_code}: {len(df)} 条记录 (删除了 {deleted_count} 条旧数据)"
                    )

                    # 等待限流
                    self._wait_for_rate_limit()

                except Exception as e:
                    stats["failed"] += 1
                    print(f"  ✗ [{i}/{len(stock_list)}] {stock_code}: 失败 - {e}")

        except Exception as e:
            print(f"按股票列表获取资金流向数据失败: {e}")

        return stats

    def calculate_industry_moneyflow(self, trade_date: str, level: str = "L1") -> int:
        """
        计算指定日期的行业资金流向汇总

        Args:
            trade_date: 交易日期 YYYYMMDD
            level: 行业级别 (L1/L2/L3)

        Returns:
            保存的记录数
        """
        try:
            from sqlalchemy import text
            from datetime import datetime

            # 确保表存在
            self._create_moneyflow_tables()

            # 查询个股资金流向并关联行业分类（使用 industry_name）
            # sw_classify 表只有 industry_name 字段，需要在 Python 中填充 sw_l1/sw_l2/sw_l3
            query = """
            SELECT
                m.trade_date,
                c.level,
                c.industry_name,
                c.index_code,
                COUNT(DISTINCT m.ts_code) as stock_count,
                SUM(CASE WHEN m.net_mf_amount > 0 THEN 1 ELSE 0 END) as up_count,
                SUM(CASE WHEN m.net_mf_amount < 0 THEN 1 ELSE 0 END) as down_count,
                SUM(m.net_mf_amount) as net_mf_amount,
                SUM(m.net_lg_amount) as net_lg_amount,
                SUM(m.net_elg_amount) as net_elg_amount,
                SUM(m.buy_elg_amount) as buy_elg_amount,
                SUM(m.sell_elg_amount) as sell_elg_amount,
                SUM(m.buy_lg_amount) as buy_lg_amount,
                SUM(m.sell_lg_amount) as sell_lg_amount,
                AVG(m.net_mf_amount) as avg_net_amount,
                AVG(m.net_lg_amount) as avg_net_lg_amount,
                AVG(m.net_elg_amount) as avg_net_elg_amount
            FROM stock_moneyflow m
            JOIN sw_members mem ON m.ts_code = mem.ts_code AND mem.is_new = 'Y'
            JOIN sw_classify c ON mem.index_code = c.index_code
            WHERE m.trade_date = :trade_date AND c.level = :level
            GROUP BY c.industry_name, c.index_code, c.level, m.trade_date
            """

            with self.engine.connect() as conn:
                df = pd.read_sql_query(
                    query, conn, params={"trade_date": trade_date, "level": level}
                )

            if df.empty:
                return 0

            # 根据 level 填充 sw_l1, sw_l2, sw_l3 字段
            df["sw_l1"] = None
            df["sw_l2"] = None
            df["sw_l3"] = None

            if level == "L1":
                df["sw_l1"] = df["industry_name"]
            elif level == "L2":
                df["sw_l2"] = df["industry_name"]
                # 需要关联 parent_code 获取 L1 名称
                for idx, row in df.iterrows():
                    with self.engine.connect() as conn:
                        parent_result = conn.execute(
                            text(
                                "SELECT parent_code FROM sw_classify WHERE index_code = :code"
                            ),
                            {"code": row["index_code"]},
                        ).fetchone()
                        if (
                            parent_result
                            and parent_result[0]
                            and parent_result[0] != "0"
                        ):
                            l1_result = conn.execute(
                                text(
                                    "SELECT industry_name FROM sw_classify WHERE index_code = :code"
                                ),
                                {"code": parent_result[0]},
                            ).fetchone()
                            if l1_result:
                                df.at[idx, "sw_l1"] = l1_result[0]
            elif level == "L3":
                df["sw_l3"] = df["industry_name"]
                # 对于 L3 行业，通过 sw_members 表获取对应的 L1 和 L2 行业名称
                for idx, row in df.iterrows():
                    with self.engine.connect() as conn:
                        # 找一只属于该 L3 行业的股票
                        stock_result = conn.execute(
                            text("""
                                SELECT mem.ts_code FROM sw_members mem
                                JOIN sw_classify c ON mem.index_code = c.index_code
                                WHERE c.industry_name = :industry_name AND c.level = 'L3' AND mem.is_new = 'Y'
                                LIMIT 1
                            """),
                            {"industry_name": row["industry_name"]},
                        ).fetchone()

                        if stock_result:
                            # 通过这只股票查找其所属的 L1 和 L2 行业
                            l1_l2_result = conn.execute(
                                text("""
                                    SELECT
                                        (SELECT c1.industry_name FROM sw_members mem3
                                         JOIN sw_classify c1 ON mem3.index_code = c1.index_code
                                         WHERE mem3.ts_code = :stock_code AND c1.level = 'L1' AND mem3.is_new = 'Y'
                                         LIMIT 1) as l1_name,
                                        (SELECT c2.industry_name FROM sw_members mem3
                                         JOIN sw_classify c2 ON mem3.index_code = c2.index_code
                                         WHERE mem3.ts_code = :stock_code AND c2.level = 'L2' AND mem3.is_new = 'Y'
                                         LIMIT 1) as l2_name
                                """),
                                {"stock_code": stock_result[0]},
                            ).fetchone()

                            if l1_l2_result:
                                df.at[idx, "sw_l1"] = l1_l2_result[0]
                                df.at[idx, "sw_l2"] = l1_l2_result[1]

            # 删除临时列
            if "industry_name" in df.columns:
                df = df.drop(columns=["industry_name"])

            # 添加更新时间
            df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df["limit_up_count"] = 0
            df["limit_down_count"] = 0

            # 删除旧数据
            delete_query = """
            DELETE FROM industry_moneyflow
            WHERE trade_date = :trade_date AND level = :level
            """
            with self.engine.connect() as conn:
                conn.execute(
                    text(delete_query), {"trade_date": trade_date, "level": level}
                )
                conn.commit()

            # 保存新数据
            df.to_sql(
                "industry_moneyflow",
                self.engine,
                if_exists="append",
                index=False,
                method="multi",
            )

            return len(df)

        except Exception as e:
            print(f"计算 {trade_date} 行业资金流向汇总失败: {e}")
            import traceback

            traceback.print_exc()
            return 0

    def save_industry_moneyflow_batch(
        self, start_date: str = None, end_date: str = None
    ) -> dict:
        """
        批量计算并保存行业资金流向汇总

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            统计信息字典
        """
        stats = {"success": 0, "failed": 0}

        try:
            from datetime import datetime

            # 获取日期范围
            if end_date is None:
                end_date = datetime.now().strftime("%Y%m%d")

            if start_date is None:
                # 查询 industry_moneyflow 最新日期
                query = text(
                    "SELECT MAX(trade_date) as max_date FROM industry_moneyflow"
                )
                with self.engine.connect() as conn:
                    result = conn.execute(query).fetchone()
                    if result and result[0]:
                        start_date = result[0]
                    else:
                        start_date = "20240101"

            # 获取有资金流向数据的日期列表
            query = """
            SELECT DISTINCT trade_date
            FROM stock_moneyflow
            WHERE trade_date >= :start_date AND trade_date <= :end_date
            ORDER BY trade_date
            """

            with self.engine.connect() as conn:
                df_dates = pd.read_sql_query(
                    query, conn, params={"start_date": start_date, "end_date": end_date}
                )

            if df_dates.empty:
                print(f"没有找到 {start_date} 至 {end_date} 的资金流向数据")
                return stats

            trade_dates = df_dates["trade_date"].tolist()
            print(f"计算行业资金流向汇总: {len(trade_dates)} 个交易日")

            # 遍历每个交易日和行业级别
            levels = ["L1", "L2", "L3"]
            for trade_date in trade_dates:
                for level in levels:
                    try:
                        count = self.calculate_industry_moneyflow(trade_date, level)
                        if count > 0:
                            stats["success"] += count
                            print(f"  ✓ {trade_date} {level}: {count} 条记录")
                    except Exception as e:
                        stats["failed"] += 1
                        print(f"  ✗ {trade_date} {level}: 失败 - {e}")

        except Exception as e:
            print(f"批量计算行业资金流向汇总失败: {e}")

        return stats

    # ========================================================================
    # Dragon List (龙虎榜) 相关方法
    # ========================================================================

    def _create_dragon_list_tables(self):
        """创建龙虎榜数据表"""
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS dragon_list (
                    ts_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    name TEXT,
                    close REAL,
                    pct_change REAL,
                    turnover_rate REAL,
                    amount REAL,
                    l_sell REAL,
                    l_buy REAL,
                    l_amount REAL,
                    net_amount REAL,
                    net_rate REAL,
                    amount_rate REAL,
                    float_values REAL,
                    reason TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (ts_code, trade_date)
                );
            """)
            )

            # 创建索引
            conn.execute(
                text("""
                CREATE INDEX IF NOT EXISTS idx_dragon_list_trade_date
                ON dragon_list(trade_date);
            """)
            )
            conn.execute(
                text("""
                CREATE INDEX IF NOT EXISTS idx_dragon_list_ts_code
                ON dragon_list(ts_code);
            """)
            )
            conn.execute(
                text("""
                CREATE INDEX IF NOT EXISTS idx_dragon_list_reason
                ON dragon_list(reason);
            """)
            )
            conn.execute(
                text("""
                CREATE INDEX IF NOT EXISTS idx_dragon_list_net_amount
                ON dragon_list(net_amount);
            """)
            )

    def save_dragon_list(self, trade_date: str = None, ts_code: str = None):
        """
        保存龙虎榜数据

        Args:
            trade_date: 交易日期 YYYYMMDD，默认为最近交易日
            ts_code: 股票代码（可选）

        Returns:
            保存的记录数
        """
        try:
            # 确保表存在
            self._create_dragon_list_tables()

            self._wait_for_rate_limit()

            # 如果没有提供trade_date，尝试获取最近交易日
            if not trade_date:
                from datetime import datetime, timedelta

                # 尝试获取最近5个工作日的数据
                for days_back in range(1, 8):  # 尝试前1-7天
                    test_date = (datetime.now() - timedelta(days=days_back)).strftime(
                        "%Y%m%d"
                    )
                    try:
                        # 测试是否能获取数据（不实际保存，只是测试API调用）
                        test_df = self._retry_api_call(
                            self.pro.top_list, trade_date=test_date
                        )
                        if test_df is not None and not test_df.empty:
                            trade_date = test_date
                            print(f"找到最近交易日: {trade_date}")
                            break
                    except Exception as e:
                        # 如果API调用失败，继续尝试下一天
                        continue

                # 如果仍然没有找到交易日，使用昨天的日期
                if not trade_date:
                    trade_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
                    print(f"使用默认日期（昨天）: {trade_date}")

            params = {"trade_date": trade_date}
            if ts_code:
                params["ts_code"] = ts_code

            print(f"[save_dragon_list] 调用Tushare API参数: {params}")
            df = self._retry_api_call(self.pro.top_list, **params)

            if df is None:
                print(f"[save_dragon_list] API返回 None")
                return 0
            elif df.empty:
                print(
                    f"[save_dragon_list] API返回空DataFrame, 形状: {df.shape}, 列名: {list(df.columns)}"
                )
                return 0
            else:
                print(
                    f"[save_dragon_list] 成功获取数据, 形状: {df.shape}, 示例列: {list(df.columns)[:5] if len(df.columns) > 5 else list(df.columns)}"
                )

            # 删除旧数据（支持更新）
            print(
                f"[save_dragon_list] 准备保存数据到数据库，trade_date={trade_date}, ts_code={ts_code}"
            )

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    with self.engine.begin() as conn:
                        if trade_date and ts_code:
                            print(
                                f"[save_dragon_list] 删除指定日期和股票的数据: {trade_date}, {ts_code}"
                            )
                            result = conn.execute(
                                text("""
                                DELETE FROM dragon_list
                                WHERE trade_date = :trade_date AND ts_code = :ts_code
                            """),
                                {"trade_date": trade_date, "ts_code": ts_code},
                            )
                            print(f"[save_dragon_list] 删除了 {result.rowcount} 行")
                        elif trade_date:
                            print(
                                f"[save_dragon_list] 删除指定日期的所有数据: {trade_date}"
                            )
                            result = conn.execute(
                                text("""
                                DELETE FROM dragon_list WHERE trade_date = :trade_date
                            """),
                                {"trade_date": trade_date},
                            )
                            print(f"[save_dragon_list] 删除了 {result.rowcount} 行")

                        # 删除重复数据（同一股票同一天可能因不同原因多次上榜）
                        original_len = len(df)
                        df = df.drop_duplicates(
                            subset=["ts_code", "trade_date"], keep="first"
                        )
                        if len(df) < original_len:
                            print(
                                f"[save_dragon_list] 删除了 {original_len - len(df)} 条重复记录"
                            )

                        # 插入新数据 - 使用逐行插入减少锁定冲突
                        print(
                            f"[save_dragon_list] 插入 {len(df)} 条新记录，尝试 {attempt + 1}/{max_retries}"
                        )
                        df.to_sql(
                            "dragon_list",
                            self.engine,
                            if_exists="append",
                            index=False,
                            method=None,
                            chunksize=10,
                        )
                        print(f"[save_dragon_list] 数据插入完成")

                    print(f"[save_dragon_list] 成功保存 {len(df)} 条记录")
                    return len(df)

                except Exception as e:
                    if "database is locked" in str(e) and attempt < max_retries - 1:
                        wait_time = 2**attempt  # 指数退避：1秒、2秒、4秒
                        print(
                            f"[save_dragon_list] 数据库锁定，{wait_time}秒后重试... (尝试 {attempt + 1}/{max_retries})"
                        )
                        import time

                        time.sleep(wait_time)
                    else:
                        print(f"[save_dragon_list] 数据库操作失败: {e}")
                        raise

        except Exception as e:
            print(f"获取龙虎榜数据失败: {e}")
            return 0

    def save_dragon_list_batch(self, start_date: str, end_date: str = None):
        """
        批量保存龙虎榜数据（用于历史数据回填）

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD，默认为今天

        Returns:
            总保存记录数
        """
        from datetime import datetime, timedelta

        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        total_count = 0
        current_date = start_date
        dates_to_fetch = []

        # 生成日期列表（只考虑交易日，跳过周末）
        while current_date <= end_date:
            dt = datetime.strptime(current_date, "%Y%m%d")
            if dt.weekday() < 5:  # 周一到周五
                dates_to_fetch.append(current_date)
            current_date = (dt + timedelta(days=1)).strftime("%Y%m%d")

        print(
            f"[save_dragon_list_batch] 开始处理 {len(dates_to_fetch)} 个交易日: {dates_to_fetch[:5]}{'...' if len(dates_to_fetch) > 5 else ''}"
        )

        for i, date in enumerate(dates_to_fetch):
            print(
                f"[save_dragon_list_batch] 处理第 {i + 1}/{len(dates_to_fetch)} 个日期: {date}"
            )
            count = self.save_dragon_list(trade_date=date)
            total_count += count
            if count > 0:
                print(f"[save_dragon_list_batch] 已保存 {date} 龙虎榜数据: {count} 条")
            else:
                print(f"[save_dragon_list_batch] {date} 无数据或保存失败")

        print(f"[save_dragon_list_batch] 批量处理完成，总共保存 {total_count} 条记录")
        return total_count

    # ========================================================================
    # HK Stock Data (港股数据) 相关方法
    # ========================================================================

    def save_hk_daily_to_duckdb(
        self, symbol: str, start_date: str = None, end_date: str = None
    ):
        """
        保存港股日线数据到DuckDB

        Uses Tushare APIs:
        - pro.hk_daily() for OHLCV data
        - pro.hk_adjfactor() for adjustment factors
        - NO daily_basic data (not available for HK stocks)

        Data is written directly to DuckDB, NOT to SQLite.

        Args:
            symbol: Stock code (e.g., "00700" or "00700.HK")
            start_date: Start date in YYYYMMDD format (None = check existing data)
            end_date: End date in YYYYMMDD format (None = today)

        Returns:
            Dictionary with statistics (rows_saved, start_date, end_date)
        """
        from src.db.duckdb_manager import get_duckdb_writer

        # 1. Standardize code to XXXXX.HK format
        ts_code = self._standardize_code(symbol)
        stock_code = ts_code.split(".")[0]

        # 2. Check existing data in DuckDB to determine start_date if not provided
        db_writer = get_duckdb_writer()
        try:
            with db_writer.get_connection() as conn:
                # Check if table exists
                if not db_writer.table_exists("bars_1d"):
                    db_writer.create_table("bars_1d", "1d")

                # Get latest date for this stock
                check_query = f"""
                    SELECT MAX(datetime) as max_date
                    FROM bars_1d
                    WHERE stock_code = '{stock_code}' AND exchange = 'HK'
                """
                result_df = conn.execute(check_query).fetchdf()

                if start_date is None:
                    if not result_df.empty and result_df["max_date"][0] is not None:
                        # Incremental update: start from next day
                        max_date = result_df["max_date"][0]
                        # Check for NaT (pandas Not a Time)
                        if pd.isna(max_date):
                            # No valid date, use default
                            start_date = "20200101"
                            logger.info(
                                f"  [HK] 首次下载: 从 {start_date} 开始 (日期无效)"
                            )
                        else:
                            # Handle both DATE and TIMESTAMP types
                            if isinstance(max_date, str):
                                # Remove time part if present
                                max_date = max_date.split(" ")[0]
                            else:
                                # Convert to string first
                                max_date = str(max_date).split(" ")[0]
                            start_date = (
                                datetime.strptime(max_date, "%Y-%m-%d")
                                + timedelta(days=1)
                            ).strftime("%Y%m%d")
                            logger.info(
                                f"  [HK] 增量更新: 从 {start_date} 开始 (最新数据: {max_date})"
                            )
                    else:
                        # No existing data, use default
                        start_date = "20200101"
                        logger.info(
                            f"  [HK] 首次下载: 从 {start_date} 开始 (无历史数据)"
                        )
                else:
                    logger.info(
                        f"  [HK] 指定日期范围: {start_date} ~ {end_date or 'today'}"
                    )

        except Exception as e:
            logger.error(f"  [HK] 检查现有数据失败: {e}")
            if start_date is None:
                start_date = "20200101"

        # 3. Set end_date to today if not provided
        if end_date is None:
            end_date = datetime.today().strftime("%Y%m%d")

        # 4. Fetch data from both APIs:
        #    - pro.hk_daily: 原始价格 + 真实涨跌幅 (pct_chg)
        #    - pro.hk_daily_adj: 复权因子 + 市值数据
        logger.info(f"  [HK] 正在获取 {ts_code} 数据: {start_date} ~ {end_date}")

        # 获取原始价格和涨跌幅
        df_daily = self._retry_api_call(
            self.pro.hk_daily, ts_code=ts_code, start_date=start_date, end_date=end_date
        )

        if df_daily is None or df_daily.empty:
            logger.warning(f"  [HK] {ts_code} 无数据 (API返回空)")
            return {"rows_saved": 0, "start_date": start_date, "end_date": end_date}

        logger.info(f"  [HK] 获取到 {len(df_daily)} 条原始数据")

        # 获取复权因子和市值数据
        df_adj = self._retry_api_call(
            self.pro.hk_daily_adj,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )

        if df_adj is not None and not df_adj.empty:
            logger.info(f"  [HK] 获取到 {len(df_adj)} 条复权数据")

            # 合并两个数据集
            df = df_daily.merge(
                df_adj[
                    [
                        "trade_date",
                        "adj_factor",
                        "turnover_ratio",
                        "total_share",
                        "free_share",
                        "total_mv",
                        "free_mv",
                    ]
                ],
                on="trade_date",
                how="left",
            )
        else:
            logger.warning(f"  [HK] 未能获取复权因子和市值数据")
            df = df_daily

        # 5. Calculate 前复权 prices using adj_factor (如果有的话)
        if "adj_factor" in df.columns:
            df["open_qfq"] = df["open"] * df["adj_factor"]
            df["high_qfq"] = df["high"] * df["adj_factor"]
            df["low_qfq"] = df["low"] * df["adj_factor"]
            df["close_qfq"] = df["close"] * df["adj_factor"]

            # Log adj_factor range
            unique_factors = df["adj_factor"].dropna().unique()
            if len(unique_factors) > 0:
                logger.info(
                    f"  [HK] 复权因子范围: {unique_factors.min():.6f} ~ {unique_factors.max():.6f}"
                )

        # 6. Prepare DataFrame for DuckDB
        df_to_save = pd.DataFrame(
            {
                "stock_code": stock_code,
                "exchange": "HK",
                "datetime": pd.to_datetime(
                    df["trade_date"], format="%Y%m%d", errors="coerce"
                ),
                "open": df["open"],
                "high": df["high"],
                "low": df["low"],
                "close": df["close"],
                "pre_close": df.get("pre_close"),
                "change": df.get("change"),
                "pct_chg": df.get("pct_chg"),  # 使用 hk_daily 的真实涨跌幅
                "volume": df.get("vol", df.get("volume")),
                "amount": df.get("amount"),
                "open_qfq": df.get("open_qfq"),
                "high_qfq": df.get("high_qfq"),
                "low_qfq": df.get("low_qfq"),
                "close_qfq": df.get("close_qfq"),
                # 添加换手率和市值字段
                "turnover_rate_f": df.get("turnover_ratio"),  # 换手率
                "total_share": df.get("total_share"),  # 总股本
                "free_share": df.get("free_share"),  # 流通股本
                "total_mv": df.get("total_mv"),  # 总市值
                "circ_mv": df.get("free_mv"),  # 流通市值
            }
        )

        # Remove rows with NaT datetime
        before_count = len(df_to_save)
        df_to_save = df_to_save.dropna(subset=["datetime"])
        after_count = len(df_to_save)
        if before_count != after_count:
            logger.info(f"  [HK] 过滤掉 {before_count - after_count} 条无效日期的记录")

        # 7. Write directly to DuckDB bars_1d table
        try:
            with db_writer.get_connection() as conn:
                # Register as temp table and insert
                conn.register("temp_hk_import", df_to_save)

                columns_str = ", ".join(df_to_save.columns)
                conn.execute(f"""
                    INSERT OR REPLACE INTO bars_1d ({columns_str})
                    SELECT {columns_str} FROM temp_hk_import
                """)
                conn.unregister("temp_hk_import")

                rows_saved = len(df_to_save)
                if df_to_save["datetime"].min() != df_to_save["datetime"].max():
                    date_range = f"{df_to_save['datetime'].min().date()} ~ {df_to_save['datetime'].max().date()}"
                else:
                    date_range = f"{df_to_save['datetime'].min().date()}"

                logger.info(
                    f"  [HK] ✓ {ts_code} 成功保存 {rows_saved} 条记录 ({date_range})"
                )

                return {
                    "rows_saved": rows_saved,
                    "start_date": start_date,
                    "end_date": end_date,
                }

        except Exception as e:
            logger.error(f"  [HK] ✗ 保存到 DuckDB 失败: {e}")
            import traceback

            traceback.print_exc()
            return {"rows_saved": 0, "start_date": start_date, "end_date": end_date}

        finally:
            db_writer.close()

    def save_a_daily_to_duckdb(
        self, symbol: str, start_date: str = None, end_date: str = None
    ):
        """
        保存A股日线数据到DuckDB（不写SQLite）

        Uses Tushare APIs:
        - pro.daily() for OHLCV data
        - pro.adj_factor() for adjustment factors
        - pro.daily_basic() for valuation metrics (if available)

        Data is written directly to DuckDB bars_a_1d table (A-share specific table),
        NOT to SQLite and NOT to bars_1d (which is for HK stocks).

        Args:
            symbol: Stock code (e.g. "600382" or "600382.SH")
            start_date: Start date in YYYYMMDD format (None = check existing data)
            end_date: End date in YYYYMMDD format (None = today)

        Returns:
            Dictionary with statistics (rows_saved, start_date, end_date)
        """
        import time

        func_start_time = time.time()

        from src.db.duckdb_manager import get_duckdb_writer
        from config.settings import A_SHARE_TABLE_MAP

        # 1. Standardize code to XXXXX.SH/SZ format
        ts_code = self._standardize_code(symbol)
        stock_code = ts_code.split(".")[0]
        exchange = "SH" if ts_code.endswith(".SH") else "SZ"

        # A股专用表名
        a_share_table = A_SHARE_TABLE_MAP.get("1d", "bars_a_1d")

        # 2. Check existing data in DuckDB to determine start_date if not provided
        db_writer = get_duckdb_writer()
        try:
            with db_writer.get_connection() as conn:
                # Check if A-share table exists, create if not
                if not db_writer.table_exists(a_share_table):
                    # Create A-share specific table
                    conn.execute(f"""
                        CREATE TABLE {a_share_table} (
                            stock_code VARCHAR NOT NULL,
                            exchange VARCHAR,
                            datetime DATE NOT NULL,
                            open FLOAT,
                            high FLOAT,
                            low FLOAT,
                            close FLOAT,
                            open_qfq FLOAT,
                            high_qfq FLOAT,
                            low_qfq FLOAT,
                            close_qfq FLOAT,
                            pre_close FLOAT,
                            change FLOAT,
                            pct_chg FLOAT,
                            volume DOUBLE,
                            turnover FLOAT,
                            amount DOUBLE,
                            pe FLOAT,
                            pe_ttm FLOAT,
                            pb FLOAT,
                            ps FLOAT,
                            ps_ttm FLOAT,
                            total_mv FLOAT,
                            circ_mv FLOAT,
                            total_share FLOAT,
                            float_share FLOAT,
                            free_share FLOAT,
                            volume_ratio FLOAT,
                            turnover_rate_f FLOAT,
                            dv_ratio FLOAT,
                            dv_ttm FLOAT,
                            PRIMARY KEY (stock_code, datetime)
                        )
                    """)
                    logger.info(f"  [A股] 创建表 {a_share_table}")

                # Get latest date for this stock
                check_query = f"""
                    SELECT MAX(datetime) as max_date
                    FROM {a_share_table}
                    WHERE stock_code = '{stock_code}' AND exchange = '{exchange}'
                """
                result_df = conn.execute(check_query).fetchdf()

                if start_date is None:
                    if not result_df.empty and result_df["max_date"][0] is not None:
                        # Incremental update: start from next day
                        max_date = result_df["max_date"][0]
                        # Check for NaT (pandas Not a Time)
                        if pd.isna(max_date):
                            # No valid date, use default
                            start_date = "20240101"
                            logger.info(
                                f"  [A股] 首次下载: 从 {start_date} 开始 (日期无效)"
                            )
                        else:
                            # Handle both DATE and TIMESTAMP types
                            if isinstance(max_date, str):
                                # Remove time part if present
                                max_date = max_date.split(" ")[0]
                            else:
                                # Convert to string first
                                max_date = str(max_date).split(" ")[0]
                            start_date = (
                                datetime.strptime(max_date, "%Y-%m-%d")
                                + timedelta(days=1)
                            ).strftime("%Y%m%d")
                            logger.info(
                                f"  [A股] 增量更新: 从 {start_date} 开始 (最新数据: {max_date})"
                            )
                    else:
                        # No existing data, use default
                        start_date = "20240101"
                        logger.info(
                            f"  [A股] 首次下载: 从 {start_date} 开始 (无历史数据)"
                        )
                else:
                    logger.info(
                        f"  [A股] 指定日期范围: {start_date} ~ {end_date or 'today'}"
                    )

        except Exception as e:
            logger.error(f"  [A股] 检查现有数据失败: {e}")
            if start_date is None:
                start_date = "20240101"

        # 3. Set end_date to today if not provided
        if end_date is None:
            end_date = datetime.today().strftime("%Y%m%d")

        # 4. Fetch original OHLCV data using pro.daily()
        logger.info(
            f"  [A股] 正在获取 {ts_code} 原始价格数据: {start_date} ~ {end_date}"
        )
        df = self._retry_api_call(
            self.pro.daily, ts_code=ts_code, start_date=start_date, end_date=end_date
        )

        if df is None or df.empty:
            logger.warning(f"  [A股] {ts_code} 无数据 (API返回空)")
            return {"rows_saved": 0, "start_date": start_date, "end_date": end_date}

        logger.info(f"  [A股] 获取到 {len(df)} 条原始数据")

        # 5. Fetch adjustment factors using pro.adj_factor()
        # Then calculate: 前复权价格 = 原始价格 × adj_factor
        logger.info(f"  [A股] 正在获取复权因子...")
        adj_df = self._retry_api_call(
            self.pro.adj_factor,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )

        if adj_df is None or adj_df.empty:
            logger.warning(f"  [A股] 无法获取复权因子，将使用原始价格")
            df["open_qfq"] = df["open"]
            df["high_qfq"] = df["high"]
            df["low_qfq"] = df["low"]
            df["close_qfq"] = df["close"]
            df["adj_factor"] = None
        else:
            logger.info(f"  [A股] 获取到 {len(adj_df)} 条复权因子数据")
            # Merge and calculate: 前复权价格 = 原始价格 × adj_factor
            df = df.merge(
                adj_df[["ts_code", "trade_date", "adj_factor"]],
                on=["ts_code", "trade_date"],
                how="left",
            )

            # Calculate 前复权 prices
            df["open_qfq"] = df["open"] * df["adj_factor"]
            df["high_qfq"] = df["high"] * df["adj_factor"]
            df["low_qfq"] = df["low"] * df["adj_factor"]
            df["close_qfq"] = df["close"] * df["adj_factor"]

            # Log adj_factor range
            unique_factors = df["adj_factor"].dropna().unique()
            if len(unique_factors) > 0:
                logger.info(
                    f"  [A股] 复权因子范围: {unique_factors.min():.6f} ~ {unique_factors.max():.6f}"
                )

        # 6. (Optional) Try to fetch daily_basic for valuation metrics
        # This may fail due to permission limits
        try:
            logger.info(f"  [A股] 正在获取每日指标（估值数据）...")
            basic_df = self._retry_api_call(
                self.pro.daily_basic,
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
            )
            if basic_df is not None and not basic_df.empty:
                logger.info(f"  [A股] 获取到 {len(basic_df)} 条估值数据")
                # Merge valuation metrics
                basic_cols = [
                    "ts_code",
                    "trade_date",
                    "pe",
                    "pe_ttm",
                    "pb",
                    "ps",
                    "ps_ttm",
                    "total_mv",
                    "circ_mv",
                    "total_share",
                    "float_share",
                    "free_share",
                    "turnover_rate_f",
                    "volume_ratio",
                    "dv_ratio",
                    "dv_ttm",
                ]
                # 只保留存在的列
                basic_cols_to_merge = [
                    col for col in basic_cols if col in basic_df.columns
                ]
                df = df.merge(
                    basic_df[basic_cols_to_merge],
                    on=["ts_code", "trade_date"],
                    how="left",
                )
        except Exception as e:
            logger.warning(f"  [A股] 获取每日指标失败（可能需要更高权限）: {e}")

        # 记录API获取耗时
        api_time = time.time() - func_start_time

        # 7. Prepare DataFrame for DuckDB
        df_to_save = pd.DataFrame(
            {
                "stock_code": stock_code,
                "exchange": exchange,
                "datetime": pd.to_datetime(
                    df["trade_date"], format="%Y%m%d", errors="coerce"
                ),
                "open": df["open"],
                "high": df["high"],
                "low": df["low"],
                "close": df["close"],
                "pre_close": df.get("pre_close"),
                "change": df.get("change"),
                "pct_chg": df.get("pct_chg"),  # A股 daily 接口返回的是 pct_chg
                "volume": df.get("vol", df.get("volume")),
                "amount": df.get("amount"),
                "open_qfq": df.get("open_qfq"),
                "high_qfq": df.get("high_qfq"),
                "low_qfq": df.get("low_qfq"),
                "close_qfq": df.get("close_qfq"),
                # Valuation fields (from daily_basic)
                "turnover_rate_f": df.get("turnover_rate_f"),
                "total_share": df.get("total_share"),
                "float_share": df.get("float_share"),
                "free_share": df.get("free_share"),
                "total_mv": df.get("total_mv"),
                "circ_mv": df.get("circ_mv"),
                "volume_ratio": df.get("volume_ratio"),
                "pe": df.get("pe"),
                "pe_ttm": df.get("pe_ttm"),
                "pb": df.get("pb"),
                "ps": df.get("ps"),
                "ps_ttm": df.get("ps_ttm"),
                "dv_ratio": df.get("dv_ratio"),
                "dv_ttm": df.get("dv_ttm"),
            }
        )

        # Remove rows with NaT datetime
        before_count = len(df_to_save)
        df_to_save = df_to_save.dropna(subset=["datetime"])
        after_count = len(df_to_save)
        if before_count != after_count:
            logger.info(f"  [A股] 过滤掉 {before_count - after_count} 条无效日期的记录")

        # 8. Write directly to DuckDB bars_a_1d table
        db_write_start = time.time()
        try:
            with db_writer.get_connection() as conn:
                # Register as temp table and insert
                conn.register("temp_a_import", df_to_save)

                # Get columns that exist in the table
                table_columns = (
                    conn.execute(
                        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{a_share_table}'"
                    )
                    .fetchdf()["column_name"]
                    .tolist()
                )

                # Only insert columns that exist in both the DataFrame and the table
                available_columns = [
                    col for col in df_to_save.columns if col in table_columns
                ]
                columns_str = ", ".join(available_columns)

                conn.execute(f"""
                    INSERT OR REPLACE INTO {a_share_table} ({columns_str})
                    SELECT {columns_str} FROM temp_a_import
                """)
                conn.unregister("temp_a_import")

                db_write_time = time.time() - db_write_start
                total_time = time.time() - func_start_time
                rows_saved = len(df_to_save)
                if df_to_save["datetime"].min() != df_to_save["datetime"].max():
                    date_range = f"{df_to_save['datetime'].min().date()} ~ {df_to_save['datetime'].max().date()}"
                else:
                    date_range = f"{df_to_save['datetime'].min().date()}"

                logger.info(
                    f"  [A股] ✓ {ts_code} 成功保存 {rows_saved} 条记录 ({date_range}) | 耗时: API={api_time:.2f}s, DB={db_write_time:.2f}s, 总计={total_time:.2f}s"
                )

                return {
                    "rows_saved": rows_saved,
                    "start_date": start_date,
                    "end_date": end_date,
                }

        except Exception as e:
            logger.error(f"  [A股] ✗ 保存到 DuckDB 失败: {e}")
            import traceback

            traceback.print_exc()
            return {"rows_saved": 0, "start_date": start_date, "end_date": end_date}

        finally:
            db_writer.close()

    def save_all_a_daily_by_date(self, trade_date: str = None):
        """
        按日期批量保存A股日线数据到DuckDB（高效方式）

        一次API调用获取当天所有A股数据，比逐股票获取快1000倍以上。

        Args:
            trade_date: 交易日期 YYYYMMDD 格式（None = 今天）

        Returns:
            Dictionary with statistics
        """
        import time
        from src.db.duckdb_manager import get_duckdb_writer
        from config.settings import A_SHARE_TABLE_MAP
        from datetime import datetime

        func_start_time = time.time()

        # 1. 确定交易日期
        if trade_date is None:
            trade_date = datetime.today().strftime("%Y%m%d")

        logger.info(f"[A股批量] 开始获取 {trade_date} 所有A股数据...")

        a_share_table = A_SHARE_TABLE_MAP.get("1d", "bars_a_1d")
        db_writer = get_duckdb_writer()

        try:
            # 2. 批量获取日线数据（一次调用获取所有股票）
            api_start = time.time()
            logger.info(f"  [A股批量] 正在获取日线数据...")
            df = self._retry_api_call(self.pro.daily, trade_date=trade_date)
            if df is None or df.empty:
                logger.warning(f"  [A股批量] {trade_date} 无日线数据（可能非交易日）")
                return {"rows_saved": 0, "trade_date": trade_date}
            logger.info(
                f"  [A股批量] 获取到 {len(df)} 条日线数据，耗时: {time.time() - api_start:.2f}s"
            )

            # 3. 批量获取复权因子
            adj_start = time.time()
            logger.info(f"  [A股批量] 正在获取复权因子...")
            adj_df = self._retry_api_call(self.pro.adj_factor, trade_date=trade_date)
            if adj_df is not None and not adj_df.empty:
                logger.info(
                    f"  [A股批量] 获取到 {len(adj_df)} 条复权因子，耗时: {time.time() - adj_start:.2f}s"
                )
                df = df.merge(
                    adj_df[["ts_code", "trade_date", "adj_factor"]],
                    on=["ts_code", "trade_date"],
                    how="left",
                )
                # 计算前复权价格
                df["open_qfq"] = df["open"] * df["adj_factor"]
                df["high_qfq"] = df["high"] * df["adj_factor"]
                df["low_qfq"] = df["low"] * df["adj_factor"]
                df["close_qfq"] = df["close"] * df["adj_factor"]
            else:
                logger.warning(f"  [A股批量] 无复权因子数据")
                df["adj_factor"] = None
                df["open_qfq"] = df["open"]
                df["high_qfq"] = df["high"]
                df["low_qfq"] = df["low"]
                df["close_qfq"] = df["close"]

            # 4. 批量获取估值数据
            basic_start = time.time()
            logger.info(f"  [A股批量] 正在获取估值数据...")
            basic_df = self._retry_api_call(self.pro.daily_basic, trade_date=trade_date)
            if basic_df is not None and not basic_df.empty:
                logger.info(
                    f"  [A股批量] 获取到 {len(basic_df)} 条估值数据，耗时: {time.time() - basic_start:.2f}s"
                )
                basic_cols = [
                    "ts_code",
                    "trade_date",
                    "pe",
                    "pe_ttm",
                    "pb",
                    "ps",
                    "ps_ttm",
                    "total_mv",
                    "circ_mv",
                    "total_share",
                    "float_share",
                    "free_share",
                    "turnover_rate_f",
                    "volume_ratio",
                    "dv_ratio",
                    "dv_ttm",
                ]
                basic_cols_to_merge = [
                    col for col in basic_cols if col in basic_df.columns
                ]
                df = df.merge(
                    basic_df[basic_cols_to_merge],
                    on=["ts_code", "trade_date"],
                    how="left",
                )
            else:
                logger.warning(f"  [A股批量] 无估值数据")

            api_total_time = time.time() - api_start

            # 5. 准备保存数据
            df["stock_code"] = df["ts_code"].str.split(".").str[0]
            df["exchange"] = df["ts_code"].str.split(".").str[1]
            df["datetime"] = pd.to_datetime(
                df["trade_date"], format="%Y%m%d", errors="coerce"
            )
            df["turnover"] = df.get("amount")  # 成交额
            df["pct_chg"] = df.get("pct_chg")
            df["volume"] = df.get("vol")  # 成交量：API返回的是vol，数据库需要volume

            df_to_save = df[~df["datetime"].isna()].copy()

            # 6. 写入DuckDB
            db_start = time.time()
            with db_writer.get_connection() as conn:
                # 确保表存在
                if not db_writer.table_exists(a_share_table):
                    conn.execute(f"""
                        CREATE TABLE {a_share_table} (
                            stock_code VARCHAR NOT NULL,
                            exchange VARCHAR,
                            datetime DATE NOT NULL,
                            open FLOAT, high FLOAT, low FLOAT, close FLOAT,
                            open_qfq FLOAT, high_qfq FLOAT, low_qfq FLOAT, close_qfq FLOAT,
                            pre_close FLOAT, change FLOAT, pct_chg FLOAT,
                            volume DOUBLE, turnover FLOAT, amount DOUBLE,
                            pe FLOAT, pe_ttm FLOAT, pb FLOAT, ps FLOAT, ps_ttm FLOAT,
                            total_mv FLOAT, circ_mv FLOAT,
                            total_share FLOAT, float_share FLOAT, free_share FLOAT,
                            volume_ratio FLOAT, turnover_rate_f FLOAT,
                            dv_ratio FLOAT, dv_ttm FLOAT,
                            PRIMARY KEY (stock_code, datetime)
                        )
                    """)

                # 获取表的列
                table_columns = (
                    conn.execute(
                        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{a_share_table}'"
                    )
                    .fetchdf()["column_name"]
                    .tolist()
                )

                # 只保留表中存在的列
                available_columns = [
                    col for col in df_to_save.columns if col in table_columns
                ]
                df_save = df_to_save[available_columns].copy()

                conn.register("temp_batch_import", df_save)
                columns_str = ", ".join(available_columns)
                conn.execute(f"""
                    INSERT OR REPLACE INTO {a_share_table} ({columns_str})
                    SELECT {columns_str} FROM temp_batch_import
                """)
                conn.unregister("temp_batch_import")

            db_time = time.time() - db_start
            total_time = time.time() - func_start_time
            rows_saved = len(df_to_save)

            logger.info(
                f"  [A股批量] ✓ 成功保存 {rows_saved} 条记录 | 耗时: API={api_total_time:.2f}s, DB={db_time:.2f}s, 总计={total_time:.2f}s"
            )

            return {
                "rows_saved": rows_saved,
                "trade_date": trade_date,
                "api_time": round(api_total_time, 2),
                "db_time": round(db_time, 2),
                "total_time": round(total_time, 2),
            }

        except Exception as e:
            logger.error(f"  [A股批量] ✗ 保存失败: {e}")
            import traceback

            traceback.print_exc()
            return {"rows_saved": 0, "trade_date": trade_date}

        finally:
            db_writer.close()

    def save_all_hk_daily_by_date(self, trade_date: str = None):
        """
        按日期批量保存港股日线数据到DuckDB（高效方式）

        一次API调用获取当天所有港股数据。

        Args:
            trade_date: 交易日期 YYYYMMDD 格式（None = 今天）

        Returns:
            Dictionary with statistics
        """
        import time
        from src.db.duckdb_manager import get_duckdb_writer
        from datetime import datetime

        func_start_time = time.time()

        # 1. 确定交易日期
        if trade_date is None:
            trade_date = datetime.today().strftime("%Y%m%d")

        logger.info(f"[港股批量] 开始获取 {trade_date} 所有港股数据...")

        hk_table = "bars_1d"
        db_writer = get_duckdb_writer()

        try:
            # 2. 批量获取港股日线数据
            api_start = time.time()
            logger.info(f"  [港股批量] 正在获取日线数据...")
            df = self._retry_api_call(self.pro.hk_daily, trade_date=trade_date)
            if df is None or df.empty:
                logger.warning(f"  [港股批量] {trade_date} 无日线数据（可能非交易日）")
                return {"rows_saved": 0, "trade_date": trade_date}
            logger.info(
                f"  [港股批量] 获取到 {len(df)} 条日线数据，耗时: {time.time() - api_start:.2f}s"
            )

            # 3. 准备保存数据
            df["stock_code"] = df["ts_code"].str.split(".").str[0]
            df["exchange"] = "HK"
            df["datetime"] = pd.to_datetime(
                df["trade_date"], format="%Y%m%d", errors="coerce"
            )
            df["turnover"] = df.get("amount")

            df_to_save = df[~df["datetime"].isna()].copy()

            # 4. 写入DuckDB
            db_start = time.time()
            with db_writer.get_connection() as conn:
                # 确保表存在
                if not db_writer.table_exists(hk_table):
                    conn.execute(f"""
                        CREATE TABLE {hk_table} (
                            stock_code VARCHAR NOT NULL,
                            exchange VARCHAR,
                            datetime DATE NOT NULL,
                            open FLOAT, high FLOAT, low FLOAT, close FLOAT,
                            pre_close FLOAT, change FLOAT, pct_chg FLOAT,
                            volume DOUBLE, turnover FLOAT, amount DOUBLE,
                            total_mv FLOAT,
                            PRIMARY KEY (stock_code, datetime)
                        )
                    """)

                # 获取表的列
                table_columns = (
                    conn.execute(
                        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{hk_table}'"
                    )
                    .fetchdf()["column_name"]
                    .tolist()
                )

                # 只保留表中存在的列
                available_columns = [
                    col for col in df_to_save.columns if col in table_columns
                ]
                df_save = df_to_save[available_columns].copy()

                conn.register("temp_hk_batch_import", df_save)
                columns_str = ", ".join(available_columns)
                conn.execute(f"""
                    INSERT OR REPLACE INTO {hk_table} ({columns_str})
                    SELECT {columns_str} FROM temp_hk_batch_import
                """)
                conn.unregister("temp_hk_batch_import")

            api_total_time = time.time() - api_start
            db_time = time.time() - db_start
            total_time = time.time() - func_start_time
            rows_saved = len(df_to_save)

            logger.info(
                f"  [港股批量] ✓ 成功保存 {rows_saved} 条记录 | 耗时: API={api_total_time:.2f}s, DB={db_time:.2f}s, 总计={total_time:.2f}s"
            )

            return {
                "rows_saved": rows_saved,
                "trade_date": trade_date,
                "api_time": round(api_total_time, 2),
                "db_time": round(db_time, 2),
                "total_time": round(total_time, 2),
            }

        except Exception as e:
            logger.error(f"  [港股批量] ✗ 保存失败: {e}")
            import traceback

            traceback.print_exc()
            return {"rows_saved": 0, "trade_date": trade_date}

        finally:
            db_writer.close()
