# tushare.py
import tushare as ts
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import os
import time
import json
from pathlib import Path
from src.data_sources.base import BaseStockDB


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

    def _standardize_code(self, symbol: str) -> str:
        """
        标准化股票代码格式
        输入: 600382 或 600382.SH
        输出: 600382.SH
        """
        # Ensure symbol is a string
        if not isinstance(symbol, str):
            symbol = str(symbol)

        if '.' in symbol:
            return symbol.upper()

        # 自动判断交易所
        if symbol.startswith(('600', '601', '603', '604', '605', '688', '689')):
            return f"{symbol}.SH"  # 上交所
        elif symbol.startswith(('000', '001', '002', '003', '300', '301')):
            return f"{symbol}.SZ"  # 深交所
        else:
            raise ValueError(f"无法识别股票代码: {symbol}")

    def _detect_exchange(self, symbol: str) -> str:
        """自动识别交易所"""
        # 如果包含交易所后缀，直接使用
        if '.' in symbol:
            suffix = symbol.split('.')[1].upper()
            if suffix == 'SH':
                return 'SSE'
            elif suffix == 'SZ':
                return 'SZSE'

        # 否则根据代码前缀判断
        code = symbol.split('.')[0] if '.' in symbol else symbol
        if code.startswith(('600', '601', '603', '604', '605', '688', '689')):
            return 'SSE'
        elif code.startswith(('000', '001', '002', '003', '300', '301')):
            return 'SZSE'
        else:
            return 'UNKNOWN'

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
        self._api_call_times = [
            t for t in self._api_call_times if t > one_minute_ago
        ]

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
                    wait_time = 2 ** attempt  # 指数退避：1秒、2秒、4秒
                    print(f"  ⚠️  第 {attempt + 1} 次调用失败: {e}，{wait_time}秒后重试...")
                    time.sleep(wait_time)
                else:
                    print(f"  ❌ 重试 {max_retries} 次后仍然失败")
                    return None

    def save_daily(self, symbol: str, start_date: str = "20200101",
                   end_date: str = None, adjust: str = None):
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
                end_date=end_date
            )

            if df is None or df.empty:
                print(f"⚠️ {symbol} 无数据")
                return

            # 保存不复权价格
            df['open_orig'] = df['open']
            df['high_orig'] = df['high']
            df['low_orig'] = df['low']
            df['close_orig'] = df['close']

            # 获取复权因子并计算前复权价格
            try:
                adj_df = self.pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
                df = df.merge(adj_df, on=['ts_code', 'trade_date'], how='left')

                # 计算前复权价格
                df['open_qfq'] = df['open'] * df['adj_factor']
                df['high_qfq'] = df['high'] * df['adj_factor']
                df['low_qfq'] = df['low'] * df['adj_factor']
                df['close_qfq'] = df['close'] * df['adj_factor']
            except:
                # 如果获取复权因子失败，前复权价格为 None
                print(f"  ⚠️  无法获取复权因子，前复权价格将为空")
                df['open_qfq'] = None
                df['high_qfq'] = None
                df['low_qfq'] = None
                df['close_qfq'] = None

            # 根据配置决定使用哪种价格作为主价格（兼容旧代码）
            if adjust == 'qfq':
                df['open'] = df['open_qfq']
                df['high'] = df['high_qfq']
                df['low'] = df['low_qfq']
                df['close'] = df['close_qfq']
            elif adjust == 'hfq':
                # 后复权 = 当前价 / 复权因子
                df['open'] = df['open'] / df['adj_factor']
                df['high'] = df['high'] / df['adj_factor']
                df['low'] = df['low'] / df['adj_factor']
                df['close'] = df['close'] / df['adj_factor']
            else:
                # 不复权，使用原始价格
                df['open'] = df['open_orig']
                df['high'] = df['high_orig']
                df['low'] = df['low_orig']
                df['close'] = df['close_orig']

            # 获取每日基本面指标（daily_basic），如果无权限则跳过
            basic_data_available = False
            try:
                basic = self._retry_api_call(
                    self.pro.daily_basic,
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields='ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv'
                )

                if basic is not None and not basic.empty:
                    # 合并所有 daily_basic 字段
                    df = df.merge(basic, on=['ts_code', 'trade_date'], how='left')
                    basic_data_available = True
                    print(f"  ✓ 获取到 daily_basic 数据 {len(basic)} 条")
                else:
                    print(f"  ⚠️  daily_basic 数据暂未生成（API更新延迟），稍后可重试更新换手率")
                    # 设置所有新字段为 None
                    for field in ['turnover_rate_f', 'volume_ratio', 'pe', 'pe_ttm', 'pb',
                                  'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm', 'total_share',
                                  'float_share', 'free_share', 'total_mv', 'circ_mv']:
                        df[field] = None

            except Exception as e:
                # 优雅处理权限错误
                if "无权限" in str(e) or "权限" in str(e) or "403" in str(e):
                    print(f"  ⚠️  无权限获取 daily_basic 数据（需要2000+积分）")
                else:
                    print(f"  ⚠️  获取 daily_basic 数据失败: {e}")

                # 设置所有字段为 None
                for field in ['turnover', 'turnover_rate_f', 'volume_ratio', 'pe', 'pe_ttm', 'pb',
                              'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm', 'total_share',
                              'float_share', 'free_share', 'total_mv', 'circ_mv']:
                    df[field] = None

            # 重命名列
            df = df.rename(columns={
                "trade_date": "datetime",
                "vol": "volume",
                "turnover_rate": "turnover"
            })

            # 添加元数据
            df["symbol"] = ts_code.split('.')[0]
            df["exchange"] = self._detect_exchange(ts_code)
            df["interval"] = "1d"
            df["datetime"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d")

            # 添加 amount 列（如果不存在）
            if 'amount' not in df.columns:
                df['amount'] = None

            # 选择要保存的列（包含所有 Tushare daily 字段 + 前复权价格 + daily_basic 指标）
            # Tushare daily 字段：ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
            columns = ["symbol", "exchange", "interval", "datetime",
                      "open", "high", "low", "close",  # 不复权价格（主价格列）
                      "open_qfq", "high_qfq", "low_qfq", "close_qfq",  # 前复权价格
                      "pre_close", "change", "pct_chg",  # Tushare 额外字段
                      "volume", "turnover", "amount",
                      # Daily basic 指标
                      "turnover_rate_f", "volume_ratio",
                      "pe", "pe_ttm", "pb", "ps", "ps_ttm",
                      "total_mv", "circ_mv",
                      "total_share", "float_share", "free_share",
                      "dv_ratio", "dv_ttm"]

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

    def save_multiple_stocks(self, symbols: list, start_date: str = "20200101",
                            end_date: str = None, adjust: str = None):
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

    def update_turnover_only(self, symbols: list = None, start_date: str = None, end_date: str = None):
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
            # 从数据库获取所有股票代码
            query = """
            SELECT DISTINCT symbol FROM bars
            WHERE interval = '1d'
            """
            with self.engine.connect() as conn:
                df = pd.read_sql_query(query, conn)
            symbols = df['symbol'].tolist() if not df.empty else []

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
                    fields='ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv'
                )

                if basic is None or basic.empty:
                    skipped_count += 1
                    continue

                # 准备更新数据
                basic = basic.rename(columns={"trade_date": "datetime"})
                basic["datetime"] = pd.to_datetime(basic["datetime"]).dt.strftime("%Y-%m-%d")
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
                                "datetime": row["datetime"]
                            }
                        )
                        if result.rowcount > 0:
                            updated_count += 1
                    conn.commit()

                print(f"  ✓ {symbol} 更新了 {len(basic)} 条记录")

            except Exception as e:
                print(f"  ❌ {symbol} 更新失败: {e}")

        print(f"\n{'='*60}")
        print(f"换手率更新完成:")
        print(f"  成功: {updated_count} 条记录")
        print(f"  跳过: {skipped_count} 只股票")
        print(f"{'='*60}")

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
            basic = self.pro.stock_basic(ts_code=ts_code, fields='ts_code,name')
            if not basic.empty:
                return basic['name'].values[0]
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
                stocks = self.pro.stock_list(exchange='SSE')
                print(f"  ✅ stock_list - 可用")
            except Exception as e:
                print(f"  ❌ stock_list - 无权限")

            # 测试日线数据接口
            try:
                df = self.pro.daily(ts_code='000001.SZ', start_date='20250101', end_date='20250102')
                print(f"  ✅ daily - 可用")
            except Exception as e:
                print(f"  ❌ daily - 无权限")

            # 测试股票基本信息接口（stock_basic）
            try:
                df = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name', limit=1)
                print(f"  ✅ stock_basic - 可用（获取股票名称）")
            except Exception as e:
                print(f"  ❌ stock_basic - 无权限（无法获取股票名称）")

            # 测试日线基本信息接口
            try:
                df = self.pro.daily_basic(ts_code='000001.SZ', start_date='20250101', end_date='20250102')
                print(f"  ✅ daily_basic - 可用")
            except Exception as e:
                print(f"  ❌ daily_basic - 无权限")

            # 测试复权因子接口
            try:
                df = self.pro.adj_factor(ts_code='000001.SZ', start_date='20250101', end_date='20250102')
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
            with open(checkpoint_path, 'w') as f:
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
                with open(checkpoint_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"  ⚠️  加载检查点失败: {e}")
        return {}

    def save_all_stocks_by_code(self, start_date: str = "20240101",
                                end_date: str = None,
                                adjust: str = None,
                                checkpoint_path: str = None,
                                resume: bool = True):
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
                exchange='',
                list_status='L',  # 只获取上市股票
                fields='ts_code,name,area,industry,list_date'
            )
            if stock_list is None or stock_list.empty:
                print("❌ 获取股票列表失败")
                return None
            all_stocks = stock_list['ts_code'].tolist()
            print(f"📋 共 {len(all_stocks)} 只股票")
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return None

        # 2. 尝试从检查点恢复
        start_index = 0
        stats = {'success': 0, 'failed': 0, 'skipped': 0, 'total': len(all_stocks)}

        if resume:
            checkpoint = self._load_checkpoint(checkpoint_path)
            if checkpoint:
                # 验证检查点是否匹配当前下载任务
                if (checkpoint.get('start_date') == start_date and
                    checkpoint.get('end_date') == end_date and
                    checkpoint.get('adjust') == adjust and
                    checkpoint.get('total') == len(all_stocks)):

                    last_index = checkpoint.get('last_index', 0)
                    start_index = last_index + 1
                    stats = checkpoint.get('stats', stats)
                    print(f"🔄 从检查点恢复: 第 {start_index + 1} 只股票开始")
                else:
                    print("⚠️  检查点参数不匹配，从头开始下载")
                    Path(checkpoint_path).unlink(missing_ok=True)

        # 3. 遍历每只股票
        for i in range(start_index, len(all_stocks)):
            ts_code = all_stocks[i]

            # 定期显示进度（每50只股票）
            if (i + 1) % 50 == 1 or i == len(all_stocks) - 1:
                print(f"\n{'='*60}")
                print(f"进度: [{i + 1}/{stats['total']}]")
                print(f"成功: {stats['success']} | 失败: {stats['failed']} | 跳过: {stats['skipped']}")
                print(f"{'='*60}")

            try:
                # 调用现有的 save_daily 方法
                self.save_daily(ts_code, start_date, end_date, adjust)
                stats['success'] += 1
            except Exception as e:
                print(f"❌ {ts_code} 处理失败: {e}")
                stats['failed'] += 1

            # 每10只股票保存一次检查点
            if (i + 1) % 10 == 0:
                checkpoint_data = {
                    'start_date': start_date,
                    'end_date': end_date,
                    'adjust': adjust,
                    'total': len(all_stocks),
                    'last_index': i,
                    'stats': stats,
                    'timestamp': datetime.now().isoformat()
                }
                self._save_checkpoint(checkpoint_path, checkpoint_data)

        # 4. 删除检查点文件（下载完成）
        if Path(checkpoint_path).exists():
            Path(checkpoint_path).unlink()
            print("🗑️  已删除检查点文件")

        # 5. 输出统计信息
        print(f"\n{'='*60}")
        print(f"数据下载完成:")
        print(f"  总计: {stats['total']} 只股票")
        print(f"  成功: {stats['success']}")
        print(f"  失败: {stats['failed']}")
        print(f"  跳过: {stats['skipped']}")
        print(f"{'='*60}")

        return stats

    def save_all_stocks_by_code_incremental(self, default_start_date: str = "20240101",
                                           end_date: str = None,
                                           adjust: str = None,
                                           checkpoint_path: str = None,
                                           resume: bool = True,
                                           stock_list: list = None):
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
                    exchange='',
                    list_status='L',  # 只获取上市股票
                    fields='ts_code,name,area,industry,list_date'
                )
                if stock_list_df is None or stock_list_df.empty:
                    print("❌ 获取股票列表失败")
                    return None
                all_stocks = stock_list_df['ts_code'].tolist()
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
                code = ts_code.split('.')[0]

                # 查询该股票的最新数据日期
                query = """
                SELECT datetime FROM bars
                WHERE symbol = :symbol AND interval = '1d'
                ORDER BY datetime DESC LIMIT 1
                """
                with self.engine.connect() as conn:
                    df = pd.read_sql_query(
                        query,
                        conn,
                        params={"symbol": code}
                    )

                if not df.empty:
                    latest_date = pd.to_datetime(df['datetime'].iloc[0])
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

        print(f"✅ 检查完成: {len(need_update_stocks)} 只需要更新, {len(all_stocks) - len(need_update_stocks)} 只已是最新")

        if len(need_update_stocks) == 0:
            print("🎉 所有股票数据已是最新，无需更新！")
            return {
                'total': len(all_stocks),
                'success': 0,
                'failed': 0,
                'skipped': len(all_stocks)
            }

        # 3. 尝试从检查点恢复
        start_index = 0
        stats = {'success': 0, 'failed': 0, 'skipped': len(all_stocks) - len(need_update_stocks), 'total': len(all_stocks)}

        if resume:
            checkpoint = self._load_checkpoint(checkpoint_path)
            if checkpoint:
                # 验证检查点是否匹配当前下载任务
                if (checkpoint.get('default_start_date') == default_start_date and
                    checkpoint.get('end_date') == end_date and
                    checkpoint.get('adjust') == adjust and
                    checkpoint.get('total') == len(all_stocks)):

                    last_index = checkpoint.get('last_index', 0)
                    start_index = last_index + 1
                    stats = checkpoint.get('stats', stats)
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
                print(f"\n{'='*60}")
                print(f"进度: [{i + 1}/{stats['total']}]")
                print(f"成功: {stats['success']} | 失败: {stats['failed']} | 跳过: {stats['skipped']}")
                print(f"{'='*60}")

            try:
                # 使用增量开始日期
                start_date = incremental_dates[ts_code]
                # 调用现有的 save_daily 方法
                self.save_daily(ts_code, start_date, end_date, adjust)
                stats['success'] += 1
            except Exception as e:
                print(f"❌ {ts_code} 处理失败: {e}")
                stats['failed'] += 1

            # 每10只股票保存一次检查点
            if (i + 1) % 10 == 0:
                checkpoint_data = {
                    'default_start_date': default_start_date,
                    'end_date': end_date,
                    'adjust': adjust,
                    'total': len(all_stocks),
                    'last_index': i,
                    'stats': stats,
                    'timestamp': datetime.now().isoformat()
                }
                self._save_checkpoint(checkpoint_path, checkpoint_data)

        # 5. 删除检查点文件（下载完成）
        if Path(checkpoint_path).exists():
            Path(checkpoint_path).unlink()
            print("🗑️  已删除检查点文件")

        # 6. 输出统计信息
        print(f"\n{'='*60}")
        print(f"数据下载完成:")
        print(f"  总计: {stats['total']} 只股票")
        print(f"  成功: {stats['success']}")
        print(f"  失败: {stats['failed']}")
        print(f"  跳过: {stats['skipped']}")
        print(f"{'='*60}")

        return stats

    def save_all_stocks_by_date(self, start_date: str = "20240101",
                                end_date: str = None):
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
                exchange='SSE',
                is_open='1',
                start_date=start_date,
                end_date=end_date,
                fields='cal_date'
            )
            if df_cal is None or df_cal.empty:
                print("❌ 获取交易日历失败")
                return None
            trade_dates = df_cal['cal_date'].tolist()
            print(f"📅 共 {len(trade_dates)} 个交易日")
        except Exception as e:
            print(f"❌ 获取交易日历失败: {e}")
            return None

        # 2. 遍历每个交易日
        stats = {'success': 0, 'failed': 0, 'total': len(trade_dates)}
        total_records = 0

        for i, date in enumerate(trade_dates, 1):
            # 定期显示进度（每20个交易日）
            if i % 20 == 1 or i == len(trade_dates):
                print(f"\n{'='*60}")
                print(f"进度: [{i}/{stats['total']}]")
                print(f"成功: {stats['success']} | 失败: {stats['failed']} | 总记录: {total_records}")
                print(f"{'='*60}")

            # 使用重试机制获取数据
            df = self._retry_api_call(
                self.pro.daily,
                trade_date=date
            )

            if df is not None and not df.empty:
                # 数据转换和保存
                saved_count = self._save_daily_batch(df, date)
                total_records += saved_count
                stats['success'] += 1
                print(f"✅ {date} 保存了 {saved_count} 条记录")
            else:
                stats['failed'] += 1
                print(f"⚠️ {date} 获取数据失败")

        # 3. 输出统计信息
        print(f"\n{'='*60}")
        print(f"数据下载完成:")
        print(f"  总计: {stats['total']} 个交易日")
        print(f"  成功: {stats['success']}")
        print(f"  失败: {stats['failed']}")
        print(f"  总记录: {total_records} 条")
        print(f"{'='*60}")

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
        df['open_orig'] = df['open']
        df['high_orig'] = df['high']
        df['low_orig'] = df['low']
        df['close_orig'] = df['close']
        df['open_qfq'] = None
        df['high_qfq'] = None
        df['low_qfq'] = None
        df['close_qfq'] = None
        df['turnover'] = None

        # 重命名列
        df = df.rename(columns={
            "trade_date": "datetime",
            "vol": "volume"
        })

        # 添加元数据
        df["symbol"] = df['ts_code'].str.split('.').str[0]
        df["exchange"] = df['ts_code'].apply(self._detect_exchange)
        df["interval"] = "1d"
        df["datetime"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d")

        # 添加 amount 列（如果不存在）
        if 'amount' not in df.columns:
            df['amount'] = None

        # 选择要保存的列
        columns = ["symbol", "exchange", "interval", "datetime",
                  "open", "high", "low", "close",
                  "open_qfq", "high_qfq", "low_qfq", "close_qfq",
                  "pre_close", "change", "pct_chg",
                  "volume", "turnover", "amount"]

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

    def _create_financial_table_from_df(self, table_name: str, df: pd.DataFrame):
        """
        根据DataFrame动态创建财务报表表

        Args:
            table_name: 表名（如 income_000001）
            df: 包含列信息的DataFrame
        """
        if df.empty:
            raise ValueError("DataFrame为空，无法创建表")

        # 构建列定义
        columns_def = []
        for col in df.columns:
            if col in ['ts_code', 'ann_date', 'end_date']:
                columns_def.append(f"{col} TEXT NOT NULL")
            else:
                columns_def.append(f"{col} REAL")

        # 主键定义
        primary_key = "PRIMARY KEY (ts_code, ann_date, end_date)"

        # 创建表SQL
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns_def)},
            {primary_key}
        );
        """

        # 执行建表
        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
            print(f"✅ 表 {table_name} 已创建")
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
        return ts_code.split('.')[0]

    def save_income(self, ts_code: str, start_date: str = None, end_date: str = None):
        """
        获取并保存利润表数据

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
            code = self._extract_stock_code(ts_code_std)
            table_name = f"income_{code}"

            # 获取数据
            print(f"  📥 获取利润表数据 {ts_code_std}...")
            df = self._retry_api_call(
                self.pro.income,
                ts_code=ts_code_std,
                start_date=start_date,
                end_date=end_date
            )

            if df is None or df.empty:
                print(f"  ⚠️  {ts_code_std} 无利润表数据")
                return 0

            # 先对API返回的数据去重
            df_before = len(df)
            df = df.drop_duplicates(subset=['ts_code', 'ann_date', 'end_date'], keep='last')
            if len(df) < df_before:
                print(f"  🔄 去除重复数据: {df_before} -> {len(df)} 条")

            # 检查表是否存在，不存在则创建
            table_exists = False
            with self.engine.connect() as conn:
                result = conn.execute(text(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                ))
                table_exists = result.fetchone() is not None

            # 保存到数据库
            if not table_exists:
                # 表不存在，创建并插入
                self._create_financial_table_from_df(table_name, df)
                df.to_sql(table_name, self.engine, if_exists="append", index=False, method="multi")
                print(f"  ✅ 已保存利润表 {len(df)} 条记录")
            else:
                # 表已存在，检查是否有重复
                existing_df = pd.read_sql_query(f"SELECT ts_code, ann_date, end_date FROM {table_name}", self.engine)
                if existing_df.empty:
                    # 表为空，直接插入
                    df.to_sql(table_name, self.engine, if_exists="append", index=False, method="multi")
                    print(f"  ✅ 已保存利润表 {len(df)} 条记录")
                else:
                    # 合并并去重
                    merged_df = pd.concat([existing_df, df], ignore_index=True)
                    merged_df = merged_df.drop_duplicates(subset=['ts_code', 'ann_date', 'end_date'], keep='last')

                    # 删除旧表并重新创建
                    with self.engine.connect() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                        conn.commit()
                    self._create_financial_table_from_df(table_name, merged_df)
                    merged_df.to_sql(table_name, self.engine, if_exists="append", index=False, method="multi")
                    print(f"  ✅ 已保存利润表 {len(merged_df)} 条记录（含历史数据）")

            return len(df)

        except Exception as e:
            error_msg = str(e)
            if "无权限" in error_msg or "权限" in error_msg or "403" in error_msg:
                print(f"  ⚠️  无权限获取利润表数据（需要2000+积分）")
            else:
                print(f"  ❌ 保存利润表失败: {e}")
            return 0

    def save_balancesheet(self, ts_code: str, start_date: str = None, end_date: str = None):
        """
        获取并保存资产负债表数据

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
            code = self._extract_stock_code(ts_code_std)
            table_name = f"balancesheet_{code}"

            # 获取数据
            print(f"  📥 获取资产负债表数据 {ts_code_std}...")
            df = self._retry_api_call(
                self.pro.balancesheet,
                ts_code=ts_code_std,
                start_date=start_date,
                end_date=end_date
            )

            if df is None or df.empty:
                print(f"  ⚠️  {ts_code_std} 无资产负债表数据")
                return 0

            # 先对API返回的数据去重
            df_before = len(df)
            df = df.drop_duplicates(subset=['ts_code', 'ann_date', 'end_date'], keep='last')
            if len(df) < df_before:
                print(f"  🔄 去除重复数据: {df_before} -> {len(df)} 条")

            # 检查表是否存在，不存在则创建
            table_exists = False
            with self.engine.connect() as conn:
                result = conn.execute(text(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                ))
                table_exists = result.fetchone() is not None

            # 保存到数据库
            if not table_exists:
                self._create_financial_table_from_df(table_name, df)
                df.to_sql(table_name, self.engine, if_exists="append", index=False, method="multi")
                print(f"  ✅ 已保存资产负债表 {len(df)} 条记录")
            else:
                existing_df = pd.read_sql_query(f"SELECT ts_code, ann_date, end_date FROM {table_name}", self.engine)
                if existing_df.empty:
                    df.to_sql(table_name, self.engine, if_exists="append", index=False, method="multi")
                    print(f"  ✅ 已保存资产负债表 {len(df)} 条记录")
                else:
                    merged_df = pd.concat([existing_df, df], ignore_index=True)
                    merged_df = merged_df.drop_duplicates(subset=['ts_code', 'ann_date', 'end_date'], keep='last')
                    with self.engine.connect() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                        conn.commit()
                    self._create_financial_table_from_df(table_name, merged_df)
                    merged_df.to_sql(table_name, self.engine, if_exists="append", index=False, method="multi")
                    print(f"  ✅ 已保存资产负债表 {len(merged_df)} 条记录（含历史数据）")

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
        获取并保存现金流量表数据

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
            code = self._extract_stock_code(ts_code_std)
            table_name = f"cashflow_{code}"

            # 获取数据
            print(f"  📥 获取现金流量表数据 {ts_code_std}...")
            df = self._retry_api_call(
                self.pro.cashflow,
                ts_code=ts_code_std,
                start_date=start_date,
                end_date=end_date
            )

            if df is None or df.empty:
                print(f"  ⚠️  {ts_code_std} 无现金流量表数据")
                return 0

            # 先对API返回的数据去重
            df_before = len(df)
            df = df.drop_duplicates(subset=['ts_code', 'ann_date', 'end_date'], keep='last')
            if len(df) < df_before:
                print(f"  🔄 去除重复数据: {df_before} -> {len(df)} 条")

            # 检查表是否存在，不存在则创建
            table_exists = False
            with self.engine.connect() as conn:
                result = conn.execute(text(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                ))
                table_exists = result.fetchone() is not None

            # 保存到数据库
            if not table_exists:
                self._create_financial_table_from_df(table_name, df)
                df.to_sql(table_name, self.engine, if_exists="append", index=False, method="multi")
                print(f"  ✅ 已保存现金流量表 {len(df)} 条记录")
            else:
                existing_df = pd.read_sql_query(f"SELECT ts_code, ann_date, end_date FROM {table_name}", self.engine)
                if existing_df.empty:
                    df.to_sql(table_name, self.engine, if_exists="append", index=False, method="multi")
                    print(f"  ✅ 已保存现金流量表 {len(df)} 条记录")
                else:
                    merged_df = pd.concat([existing_df, df], ignore_index=True)
                    merged_df = merged_df.drop_duplicates(subset=['ts_code', 'ann_date', 'end_date'], keep='last')
                    with self.engine.connect() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                        conn.commit()
                    self._create_financial_table_from_df(table_name, merged_df)
                    merged_df.to_sql(table_name, self.engine, if_exists="append", index=False, method="multi")
                    print(f"  ✅ 已保存现金流量表 {len(merged_df)} 条记录（含历史数据）")

            return len(df)

        except Exception as e:
            error_msg = str(e)
            if "无权限" in error_msg or "权限" in error_msg or "403" in error_msg:
                print(f"  ⚠️  无权限获取现金流量表数据（需要2000+积分）")
            else:
                print(f"  ❌ 保存现金流量表失败: {e}")
            return 0

    def save_fina_indicator(self, ts_code: str, start_date: str = None, end_date: str = None) -> int:
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
                'ts_code', 'ann_date', 'end_date', 'report_type',
                # 盈利能力 (12个指标)
                'eps', 'basic_eps', 'diluted_eps',
                'roe', 'roa', 'roic',
                'netprofit_margin', 'grossprofit_margin', 'operateprofit_margin',
                'core_roe', 'core_roa', 'q_eps',
                # 成长能力 (10个指标)
                'or_yoy', 'tr_yoy', 'netprofit_yoy', 'assets_yoy',
                'ebt_yoy', 'ocf_yoy', 'roe_yoy',
                'q_or_yoy', 'q_tr_yoy', 'q_netprofit_yoy',
                # 营运能力 (8个指标)
                'assets_turn', 'ar_turn', 'inv_turn',
                'ca_turn', 'fa_turn', 'current_assets_turn',
                'equity_turn', 'op_npta',
                # 偿债能力 (8个指标)
                'current_ratio', 'quick_ratio', 'cash_ratio',
                'debt_to_assets', 'debt_to_eqt', 'equity_multiplier',
                'ebit_to_interest', 'op_to_ebit',
                # 现金流指标 (7个指标)
                'ocfps', 'ocf_to_debt', 'ocf_to_shortdebt',
                'ocf_to_liability', 'ocf_to_interest',
                'cf_to_debt', 'free_cf',
                # 每股指标 (3个指标)
                'bps', 'tangible_asset_to_share', 'capital_reserv_to_share'
            ]

            # 获取数据
            print(f"  📥 获取财务指标数据 {ts_code_std}...")
            df = self._retry_api_call(
                self.pro.fina_indicator,
                ts_code=ts_code_std,
                start_date=start_date,
                end_date=end_date
            )

            if df is None or df.empty:
                print(f"  ⚠️  {ts_code_std} 无财务指标数据")
                return 0

            # 去重处理
            df_before = len(df)
            df = df.drop_duplicates(subset=['ts_code', 'ann_date', 'end_date', 'report_type'], keep='last')
            if len(df) < df_before:
                print(f"  🔄 去除重复数据: {df_before} -> {len(df)} 条")

            # 选择核心指标列（只保留存在的列）
            available_columns = [col for col in core_columns if col in df.columns]
            df = df[available_columns]

            # 先删除重复数据（如果存在）
            with self.engine.connect() as conn:
                # 获取API返回数据的公告日期列表
                ann_dates = df['ann_date'].tolist()
                placeholders = ','.join([':ann_date_' + str(i) for i in range(len(ann_dates))])
                params = {'ts_code': ts_code_std}
                params.update({f'ann_date_{i}': date for i, date in enumerate(ann_dates)})

                delete_sql = f"""
                DELETE FROM fina_indicator
                WHERE ts_code = :ts_code AND ann_date IN ({placeholders})
                """
                conn.execute(text(delete_sql), params)
                conn.commit()

            # 保存到数据库
            df.to_sql(table_name, self.engine, if_exists="append", index=False, method="multi")
            print(f"  ✅ 已保存财务指标 {len(df)} 条记录")
            return len(df)

        except Exception as e:
            error_msg = str(e)
            # 权限不足时优雅降级
            if "无权限" in error_msg or "权限" in error_msg or "403" in error_msg or "权限不足" in error_msg:
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
            test_df = self.pro.fina_indicator(ts_code='000001.SZ', limit=1)
            return test_df is not None and not test_df.empty
        except:
            return False

    def save_all_financial(self, ts_code: str, start_date: str = None, end_date: str = None, include_indicators: bool = True):
        """
        获取并保存所有财务报表数据（利润表、资产负债表、现金流量表、财务指标）

        Args:
            ts_code: 股票代码（如 000001.SZ 或 000001）
            start_date: 公告开始日期（格式 YYYYMMDD）
            end_date: 公告结束日期（格式 YYYYMMDD）
            include_indicators: 是否包含财务指标（默认 True）

        Returns:
            保存的记录总数
        """
        total_records = 0

        try:
            # 标准化代码
            ts_code_std = self._standardize_code(ts_code)
            print(f"\n{'='*60}")
            print(f"开始下载财务数据: {ts_code_std}")
            print(f"{'='*60}")

            # 1. 利润表
            income_count = self.save_income(ts_code_std, start_date, end_date)
            total_records += income_count

            # 2. 资产负债表
            balance_count = self.save_balancesheet(ts_code_std, start_date, end_date)
            total_records += balance_count

            # 3. 现金流量表
            cashflow_count = self.save_cashflow(ts_code_std, start_date, end_date)
            total_records += cashflow_count

            # 4. 财务指标（可选）
            indicator_count = 0
            if include_indicators:
                indicator_count = self.save_fina_indicator(ts_code_std, start_date, end_date)
                total_records += indicator_count

            print(f"\n{'='*60}")
            print(f"✅ {ts_code_std} 财务数据下载完成")
            print(f"  利润表: {income_count} 条")
            print(f"  资产负债表: {balance_count} 条")
            print(f"  现金流量表: {cashflow_count} 条")
            if include_indicators:
                print(f"  财务指标: {indicator_count} 条")
            print(f"  总计: {total_records} 条")
            print(f"{'='*60}")

        except Exception as e:
            print(f"❌ {ts_code} 下载财务数据失败: {e}")

        return total_records

    def get_latest_financial_date(self, ts_code: str, table_type: str) -> str:
        """
        查询指定股票的最新财报日期

        Args:
            ts_code: 股票代码（如 000001.SZ 或 000001）
            table_type: 报表类型（income/balancesheet/cashflow/fina_indicator）

        Returns:
            最新公告日期（格式 YYYYMMDD），无数据则返回 None
        """
        try:
            # 标准化代码
            ts_code_std = self._standardize_code(ts_code)

            # fina_indicator 使用统一表名
            if table_type == 'fina_indicator':
                table_name = 'fina_indicator'
            else:
                code = self._extract_stock_code(ts_code_std)
                table_name = f"{table_type}_{code}"

            # 检查表是否存在
            with self.engine.connect() as conn:
                result = conn.execute(text(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                ))
                if not result.fetchone():
                    return None

                # 查询最新日期
                query = f"""
                SELECT ann_date FROM {table_name}
                WHERE ts_code = :ts_code
                ORDER BY ann_date DESC LIMIT 1
                """
                df = pd.read_sql_query(
                    query,
                    conn,
                    params={"ts_code": ts_code_std}
                )

                if not df.empty:
                    return df['ann_date'].iloc[0]
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
        df = self._retry_api_call(
            self.pro.index_basic,
            market=market or ''
        )

        if df is None or df.empty:
            print(f"  ⚠️  无指数基本信息")
            # 即使 API 返回空，也检查数据库中是否已有数据
            with self.engine.connect() as conn:
                query = "SELECT COUNT(*) FROM index_names"
                if market == 'SSE':
                    query += " WHERE ts_code LIKE '%.SH'"
                elif market == 'SZSE':
                    query += " WHERE ts_code LIKE '%.SZ'"
                result = conn.execute(text(query))
                count = result.fetchone()[0]
                return count

        # 准备数据
        df = df.copy()
        df['updated_at'] = datetime.now().isoformat()

        # 尝试保存到数据库
        try:
            df.to_sql('index_names', self.engine, if_exists='append', index=False, method='multi')
            print(f"  ✅ 已保存 {len(df)} 条指数基本信息")
            return len(df)
        except Exception as e:
            error_msg = str(e)
            if "UNIQUE constraint" in error_msg or "duplicate" in error_msg.lower():
                # 数据已存在，不需要更新（基本信息通常不变）
                # 直接返回数据库中的数量
                with self.engine.connect() as conn:
                    query = "SELECT COUNT(*) FROM index_names"
                    if market == 'SSE':
                        query += " WHERE ts_code LIKE '%.SH'"
                    elif market == 'SZSE':
                        query += " WHERE ts_code LIKE '%.SZ'"
                    result = conn.execute(text(query))
                    count = result.fetchone()[0]
                print(f"  ℹ️  指数基本信息已存在，数据库中共有 {count} 条")
                return count
            else:
                print(f"  ❌ 保存指数基本信息失败: {e}")
                return 0

    def save_index_daily(self, ts_code: str, start_date: str = "20200101", end_date: str = None):
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
            if '.' not in ts_code:
                raise ValueError(f"指数代码格式错误: {ts_code}，应为 000001.SH 格式")

            # 获取指数日线数据
            print(f"  📥 获取指数日线数据 {ts_code} ({start_date} - {end_date})...")
            df = self._retry_api_call(
                self.pro.index_daily,
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )

            if df is None or df.empty:
                print(f"  ⚠️  {ts_code} 无指数日线数据")
                return 0

            # 重命名列以匹配 bars 表结构
            df = df.rename(columns={
                "trade_date": "datetime",
                "vol": "volume"
            })

            # 添加元数据
            # 指数使用完整的 ts_code 作为 symbol（如 000001.SH），避免与股票代码冲突
            df["symbol"] = ts_code
            df["exchange"] = self._detect_exchange(ts_code)
            df["interval"] = "1d"
            df["datetime"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d")

            # 指数数据没有的股票字段，设为 None
            stock_only_fields = [
                'open_qfq', 'high_qfq', 'low_qfq', 'close_qfq',  # 前复权价格
                'turnover',  # 换手率
                # 估值指标
                'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm',
                # 市值指标
                'total_mv', 'circ_mv',
                # 股本结构
                'total_share', 'float_share', 'free_share',
                # 流动性指标
                'volume_ratio', 'turnover_rate_f',
                # 分红指标
                'dv_ratio', 'dv_ttm'
            ]
            for field in stock_only_fields:
                df[field] = None

            # 选择要保存的列
            columns = ["symbol", "exchange", "interval", "datetime",
                      "open", "high", "low", "close",
                      "open_qfq", "high_qfq", "low_qfq", "close_qfq",
                      "pre_close", "change", "pct_chg",
                      "volume", "turnover", "amount",
                      # Daily basic 指标
                      "turnover_rate_f", "volume_ratio",
                      "pe", "pe_ttm", "pb", "ps", "ps_ttm",
                      "total_mv", "circ_mv",
                      "total_share", "float_share", "free_share",
                      "dv_ratio", "dv_ttm"]

            # 确保所有列都存在
            for col in columns:
                if col not in df.columns:
                    df[col] = None

            # 保存到数据库
            df[columns].to_sql("bars", self.engine, if_exists="append", index=False, method="multi")
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

    def save_all_indices(self, start_date: str = "20240101", end_date: str = None, markets: list = None):
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
            markets = ['SSE', 'SZSE']

        # 第一步：获取指数基本信息
        print("📋 正在获取指数列表...")
        all_indices = []

        for market in markets:
            try:
                count = self.save_index_basic(market=market)
                if count > 0:
                    # 从数据库读取指数代码
                    query = "SELECT ts_code FROM index_names"
                    if market == 'SSE':
                        query += " WHERE ts_code LIKE '%.SH'"
                    elif market == 'SZSE':
                        query += " WHERE ts_code LIKE '%.SZ'"

                    with self.engine.connect() as conn:
                        df = pd.read_sql_query(query, conn)
                        all_indices.extend(df['ts_code'].tolist())
            except Exception as e:
                print(f"  ❌ 获取 {market} 指数列表失败: {e}")

        if not all_indices:
            print("❌ 没有找到指数")
            return {'total': 0, 'success': 0, 'failed': 0}

        # 去重
        all_indices = list(set(all_indices))
        print(f"📋 共 {len(all_indices)} 个指数")

        # 第二步：逐个下载指数行情数据
        stats = {'total': len(all_indices), 'success': 0, 'failed': 0, 'skipped': 0}

        for i, ts_code in enumerate(all_indices):
            # 定期显示进度
            if (i + 1) % 10 == 1 or i == len(all_indices) - 1:
                print(f"\n{'='*60}")
                print(f"进度: [{i + 1}/{stats['total']}]")
                print(f"成功: {stats['success']} | 失败: {stats['failed']} | 跳过: {stats['skipped']}")
                print(f"{'='*60}")

            try:
                result = self.save_index_daily(ts_code, start_date, end_date)
                if result > 0:
                    stats['success'] += 1
                elif result == 0:
                    stats['skipped'] += 1
                else:
                    stats['failed'] += 1
            except Exception as e:
                print(f"  ❌ {ts_code} 处理失败: {e}")
                stats['failed'] += 1

        # 输出统计信息
        print(f"\n{'='*60}")
        print(f"指数数据下载完成:")
        print(f"  总计: {stats['total']} 个指数")
        print(f"  成功: {stats['success']}")
        print(f"  失败: {stats['failed']}")
        print(f"  跳过: {stats['skipped']}")
        print(f"{'='*60}")

        return stats

    # ==================== 申万行业分类相关方法 ====================

    def save_sw_classify(self, src: str = 'SW2021', level: str = None,
                         update_timestamp: bool = True) -> int:
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
            params = {'src': src}
            if level:
                params['level'] = level

            df = self._retry_api_call(
                self.pro.index_classify,
                **params
            )

            if df is None or df.empty:
                print(f"  ⚠️  无申万行业分类数据")
                return 0

            # 准备数据
            df = df.copy()
            df['src'] = src
            df['updated_at'] = datetime.now().isoformat()

            # 选择列并保存
            columns = ['index_code', 'industry_name', 'parent_code', 'level', 'industry_code', 'is_pub', 'src', 'updated_at']

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

                df.to_sql('sw_classify', self.engine, if_exists='append', index=False, method='multi')
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
                        conn.execute(text(upsert_sql), {
                            "index_code": row['index_code'],
                            "industry_name": row['industry_name'],
                            "parent_code": row['parent_code'],
                            "level": row['level'],
                            "industry_code": row['industry_code'],
                            "is_pub": row['is_pub'],
                            "src": row['src'],
                            "updated_at": row['updated_at']
                        })
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

    def save_sw_members(self, index_code: str = None, ts_code: str = None,
                       is_new: str = 'Y', force_update: bool = False) -> int:
        """
        获取并保存申万行业成分股数据

        Args:
            index_code: 行业指数代码，None表示获取所有
            ts_code: 股票代码，与index_code二选一
            is_new: 是否最新成分，Y=是（默认），N=否
            force_update: 是否强制更新（删除旧数据）

        Returns:
            保存的记录数
        """
        try:
            # 构建查询参数
            params = {'is_new': is_new}
            if index_code:
                params['index_code'] = index_code
            if ts_code:
                params['ts_code'] = ts_code

            desc = f"index_code={index_code}" if index_code else f"ts_code={ts_code}" if ts_code else "全部"
            print(f"  📥 获取申万行业成分股数据 ({desc}, is_new={is_new})...")

            # 获取数据
            df = self._retry_api_call(
                self.pro.index_member_all,
                **params
            )

            if df is None or df.empty:
                print(f"  ⚠️  无申万行业成分股数据")
                return 0

            # 如果指定了 index_code，删除旧数据
            if index_code and force_update:
                delete_sql = "DELETE FROM sw_members WHERE index_code = :index_code"
                with self.engine.connect() as conn:
                    conn.execute(text(delete_sql), {"index_code": index_code})
                    conn.commit()

            # 准备数据
            df = df.copy()
            df['is_new'] = is_new

            # API 返回的数据中没有 index_code 字段，需要手动添加
            if index_code and 'index_code' not in df.columns:
                df['index_code'] = index_code

            # 选择列
            columns = ['index_code', 'ts_code', 'name', 'in_date', 'out_date', 'is_new']

            # 确保所有列都存在
            for col in columns:
                if col not in df.columns:
                    df[col] = None

            df = df[columns]

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
                for _, row in df.iterrows():
                    conn.execute(text(upsert_sql), {
                        "index_code": row['index_code'],
                        "ts_code": row['ts_code'],
                        "name": row['name'],
                        "in_date": row['in_date'],
                        "out_date": row['out_date'],
                        "is_new": row['is_new']
                    })
                conn.commit()

            print(f"  ✅ 已保存申万行业成分股 {len(df)} 条记录")
            return len(df)

        except Exception as e:
            error_msg = str(e)
            if "无权限" in error_msg or "权限" in error_msg or "403" in error_msg:
                print(f"  ⚠️  无权限获取申万行业成分股数据（需要2000+积分）")
            else:
                print(f"  ❌ 保存申万行业成分股失败: {e}")
            return 0

    def get_outdated_indices(self, src: str = 'SW2021', days: int = 7) -> list:
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
            df = pd.read_sql_query(query, conn, params={"src": src, "cutoff_date": cutoff_date})

        return df['index_code'].tolist() if not df.empty else []

    def update_indices_timestamp(self, index_codes: list, src: str = 'SW2021'):
        """
        更新指定行业的 updated_at 时间戳

        Args:
            index_codes: 行业代码列表
            src: 行业分类来源
        """
        if not index_codes:
            return

        now = datetime.now().isoformat()
        placeholders = ','.join([f':code{i}' for i in range(len(index_codes))])
        params = {f'code{i}': code for i, code in enumerate(index_codes)}
        params['src'] = src
        params['now'] = now

        update_sql = f"""
        UPDATE sw_classify
        SET updated_at = :now
        WHERE index_code IN ({placeholders})
        AND src = :src
        """

        with self.engine.connect() as conn:
            conn.execute(text(update_sql), params)
            conn.commit()

    def save_all_sw_industry(self, src: str = 'SW2021', is_new: str = 'Y',
                            force_update: bool = False, incremental: bool = False,
                            incremental_days: int = 7) -> dict:
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
        print(f"\n{'='*60}")
        if incremental:
            print(f"开始增量更新申万行业数据 (src={src}, days={incremental_days})")
        else:
            print(f"开始获取申万行业数据 (src={src})")
        print(f"{'='*60}")

        stats = {
            'classify_count': 0,
            'members_count': 0,
            'total_indices': 0,
            'skipped_indices': 0,
            'failed_indices': []
        }

        # 1. 获取行业分类（增量模式下不更新时间戳）
        print("\n1. 获取申万行业分类...")
        update_ts = not incremental  # 增量模式下不更新时间戳
        classify_count = self.save_sw_classify(src=src, update_timestamp=update_ts)
        stats['classify_count'] = classify_count

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
            print(f"\n2. 增量更新行业成分股（{len(all_indices)}/{classify_count} 个行业需要更新）...")
        else:
            query = "SELECT index_code FROM sw_classify WHERE src = :src"
            with self.engine.connect() as conn:
                df_indices = pd.read_sql_query(query, conn, params={"src": src})

            if df_indices.empty:
                print("❌ 没有找到行业分类")
                return stats

            all_indices = df_indices['index_code'].tolist()
            print(f"\n2. 获取行业成分股（共 {len(all_indices)} 个行业）...")

        stats['total_indices'] = len(all_indices)

        # 3. 遍历每个行业获取成分股
        updated_indices = []  # 记录成功更新的行业
        for i, index_code in enumerate(all_indices):
            # 定期显示进度
            if (i + 1) % 20 == 1 or i == len(all_indices) - 1:
                print(f"\n{'='*60}")
                print(f"进度: [{i + 1}/{stats['total_indices']}]")
                print(f"成功: {stats['members_count']} | 失败: {len(stats['failed_indices'])}")
                print(f"{'='*60}")

            try:
                count = self.save_sw_members(index_code=index_code, is_new=is_new, force_update=force_update)
                if count > 0:
                    stats['members_count'] += count
                    updated_indices.append(index_code)
                else:
                    stats['failed_indices'].append(index_code)

            except Exception as e:
                print(f"  ❌ {index_code} 处理失败: {e}")
                stats['failed_indices'].append(index_code)

        # 4. 更新成功更新的行业的时间戳
        if incremental and updated_indices:
            self.update_indices_timestamp(updated_indices, src=src)

        # 5. 输出统计信息
        print(f"\n{'='*60}")
        if incremental:
            print(f"申万行业数据增量更新完成:")
            print(f"  总行业数: {classify_count} 个")
            print(f"  需要更新: {stats['total_indices']} 个")
            print(f"  跳过: {classify_count - stats['total_indices']} 个（已最新）")
        else:
            print(f"申万行业数据获取完成:")
            print(f"  行业分类: {stats['classify_count']} 条")
        print(f"  成分股: {stats['members_count']} 条")
        if stats['failed_indices']:
            print(f"  失败行业: {len(stats['failed_indices'])} 个")
        print(f"{'='*60}")

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

    def get_sw_classify(self, src: str = 'SW2021', level: str = None) -> pd.DataFrame:
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
