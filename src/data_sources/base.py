# base.py
"""
股票数据库基类
抽象 Tushare 和 AKShare 的共同功能
"""
import pandas as pd
from sqlalchemy import create_engine, text
from abc import ABC, abstractmethod
from datetime import datetime


class BaseStockDB(ABC):
    """
    股票数据库基类
    定义了所有数据源都需要实现的接口和共同功能
    """

    def __init__(self, db_path: str):
        """
        初始化数据库连接

        Args:
            db_path: 数据库文件路径
        """
        self.engine = create_engine(f"sqlite:///{db_path}", echo=False)
        self._create_tables()
        self._stock_name_cache = {}  # 股票名称缓存

    def _create_tables(self):
        """创建 K 线表（通用结构，同时存储不复权和前复权价格）"""
        create_sql = """
        CREATE TABLE IF NOT EXISTS bars (
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            interval TEXT NOT NULL,
            datetime TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            open_qfq REAL,
            high_qfq REAL,
            low_qfq REAL,
            close_qfq REAL,
            pre_close REAL,
            change REAL,
            pct_chg REAL,
            volume REAL,
            turnover REAL,
            amount REAL,
            -- 估值指标
            pe REAL,
            pe_ttm REAL,
            pb REAL,
            ps REAL,
            ps_ttm REAL,
            -- 市值指标
            total_mv REAL,
            circ_mv REAL,
            -- 股本结构
            total_share REAL,
            float_share REAL,
            free_share REAL,
            -- 流动性指标
            volume_ratio REAL,
            turnover_rate_f REAL,
            -- 分红指标
            dv_ratio REAL,
            dv_ttm REAL,
            PRIMARY KEY (symbol, interval, datetime)
        );
        """
        with self.engine.connect() as conn:
            conn.execute(text(create_sql))
            conn.commit()

        # 创建股票名称表
        stock_names_sql = """
        CREATE TABLE IF NOT EXISTS stock_names (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            updated_at TEXT
        );
        """
        with self.engine.connect() as conn:
            conn.execute(text(stock_names_sql))
            conn.commit()

        # 创建指数名称表
        index_names_sql = """
        CREATE TABLE IF NOT EXISTS index_names (
            ts_code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            market TEXT,
            publisher TEXT,
            index_type TEXT,
            category TEXT,
            base_date TEXT,
            base_point REAL,
            list_date TEXT,
            weight_rule TEXT,
            desc TEXT,
            updated_at TEXT
        );
        """
        with self.engine.connect() as conn:
            conn.execute(text(index_names_sql))
            conn.commit()

        # 创建财务指标表（统一表结构，非动态表）
        fina_indicator_sql = """
        CREATE TABLE IF NOT EXISTS fina_indicator (
            ts_code TEXT NOT NULL,
            ann_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            report_type TEXT,

            -- 盈利能力 (12个指标)
            eps REAL, basic_eps REAL, diluted_eps REAL,
            roe REAL, roa REAL, roic REAL,
            netprofit_margin REAL, grossprofit_margin REAL, operateprofit_margin REAL,
            core_roe REAL, core_roa REAL, q_eps REAL,

            -- 成长能力 (10个指标)
            or_yoy REAL, tr_yoy REAL, netprofit_yoy REAL, assets_yoy REAL,
            ebt_yoy REAL, ocf_yoy REAL, roe_yoy REAL,
            q_or_yoy REAL, q_tr_yoy REAL, q_netprofit_yoy REAL,

            -- 营运能力 (8个指标)
            assets_turn REAL, ar_turn REAL, inv_turn REAL,
            ca_turn REAL, fa_turn REAL, current_assets_turn REAL,
            equity_turn REAL, op_npta REAL,

            -- 偿债能力 (8个指标)
            current_ratio REAL, quick_ratio REAL, cash_ratio REAL,
            debt_to_assets REAL, debt_to_eqt REAL, equity_multiplier REAL,
            ebit_to_interest REAL, op_to_ebit REAL,

            -- 现金流指标 (7个指标)
            ocfps REAL, ocf_to_debt REAL, ocf_to_shortdebt REAL,
            ocf_to_liability REAL, ocf_to_interest REAL,
            cf_to_debt REAL, free_cf REAL,

            -- 每股指标 (3个指标)
            bps REAL, tangible_asset_to_share REAL, capital_reserv_to_share REAL,

            PRIMARY KEY (ts_code, ann_date, end_date, report_type)
        );
        """
        with self.engine.connect() as conn:
            conn.execute(text(fina_indicator_sql))
            conn.commit()

        # 创建申万行业分类表
        sw_classify_sql = """
        CREATE TABLE IF NOT EXISTS sw_classify (
            index_code TEXT PRIMARY KEY,
            industry_name TEXT NOT NULL,
            parent_code TEXT,
            level TEXT NOT NULL,
            industry_code TEXT NOT NULL,
            is_pub TEXT,
            src TEXT DEFAULT 'SW2021',
            updated_at TEXT
        );
        """
        with self.engine.connect() as conn:
            conn.execute(text(sw_classify_sql))
            conn.commit()

        # 创建申万行业成分股表
        sw_members_sql = """
        CREATE TABLE IF NOT EXISTS sw_members (
            index_code TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            name TEXT,
            in_date TEXT,
            out_date TEXT,
            is_new TEXT DEFAULT 'Y',
            PRIMARY KEY (index_code, ts_code),
            FOREIGN KEY (index_code) REFERENCES sw_classify(index_code) ON DELETE CASCADE
        );
        """
        with self.engine.connect() as conn:
            conn.execute(text(sw_members_sql))
            conn.commit()

        # 创建索引以提升查询性能
        index_sqls = [
            "CREATE INDEX IF NOT EXISTS idx_bars_symbol ON bars(symbol);",
            "CREATE INDEX IF NOT EXISTS idx_bars_datetime ON bars(datetime);",
            "CREATE INDEX IF NOT EXISTS idx_bars_symbol_datetime ON bars(symbol, datetime);",
            "CREATE INDEX IF NOT EXISTS idx_bars_turnover ON bars(turnover);",
            "CREATE INDEX IF NOT EXISTS idx_stock_names_name ON stock_names(name);",
            "CREATE INDEX IF NOT EXISTS idx_index_names_name ON index_names(name);",
            # Daily basic 指标索引
            "CREATE INDEX IF NOT EXISTS idx_bars_pe ON bars(pe);",
            "CREATE INDEX IF NOT EXISTS idx_bars_pb ON bars(pb);",
            "CREATE INDEX IF NOT EXISTS idx_bars_total_mv ON bars(total_mv);",
            "CREATE INDEX IF NOT EXISTS idx_bars_circ_mv ON bars(circ_mv);",
            "CREATE INDEX IF NOT EXISTS idx_bars_volume_ratio ON bars(volume_ratio);",
            "CREATE INDEX IF NOT EXISTS idx_bars_turnover_rate_f ON bars(turnover_rate_f);",
            # 财务指标表索引
            "CREATE INDEX IF NOT EXISTS idx_fina_indicator_ts_code ON fina_indicator(ts_code);",
            "CREATE INDEX IF NOT EXISTS idx_fina_indicator_end_date ON fina_indicator(end_date);",
            "CREATE INDEX IF NOT EXISTS idx_fina_indicator_ann_date ON fina_indicator(ann_date);",
            "CREATE INDEX IF NOT EXISTS idx_fina_indicator_roe ON fina_indicator(roe);",
            # 申万行业分类表索引
            "CREATE INDEX IF NOT EXISTS idx_sw_classify_level ON sw_classify(level);",
            "CREATE INDEX IF NOT EXISTS idx_sw_classify_parent_code ON sw_classify(parent_code);",
            "CREATE INDEX IF NOT EXISTS idx_sw_classify_src ON sw_classify(src);",
            # 申万行业成分股表索引
            "CREATE INDEX IF NOT EXISTS idx_sw_members_index_code ON sw_members(index_code);",
            "CREATE INDEX IF NOT EXISTS idx_sw_members_ts_code ON sw_members(ts_code);",
            "CREATE INDEX IF NOT EXISTS idx_sw_members_is_new ON sw_members(is_new);",
        ]
        with self.engine.connect() as conn:
            for index_sql in index_sqls:
                try:
                    conn.execute(text(index_sql))
                    conn.commit()
                except Exception as e:
                    pass  # 索引可能已存在

        # 如果表已存在，尝试添加新列（用于已存在的数据库）
        try:
            alter_sqls = [
                "ALTER TABLE bars ADD COLUMN open_qfq REAL;",
                "ALTER TABLE bars ADD COLUMN high_qfq REAL;",
                "ALTER TABLE bars ADD COLUMN low_qfq REAL;",
                "ALTER TABLE bars ADD COLUMN close_qfq REAL;",
                "ALTER TABLE bars ADD COLUMN pre_close REAL;",
                "ALTER TABLE bars ADD COLUMN change REAL;",
                "ALTER TABLE bars ADD COLUMN pct_chg REAL;",
                # Daily basic 指标列
                "ALTER TABLE bars ADD COLUMN pe REAL;",
                "ALTER TABLE bars ADD COLUMN pe_ttm REAL;",
                "ALTER TABLE bars ADD COLUMN pb REAL;",
                "ALTER TABLE bars ADD COLUMN ps REAL;",
                "ALTER TABLE bars ADD COLUMN ps_ttm REAL;",
                "ALTER TABLE bars ADD COLUMN total_mv REAL;",
                "ALTER TABLE bars ADD COLUMN circ_mv REAL;",
                "ALTER TABLE bars ADD COLUMN total_share REAL;",
                "ALTER TABLE bars ADD COLUMN float_share REAL;",
                "ALTER TABLE bars ADD COLUMN free_share REAL;",
                "ALTER TABLE bars ADD COLUMN volume_ratio REAL;",
                "ALTER TABLE bars ADD COLUMN turnover_rate_f REAL;",
                "ALTER TABLE bars ADD COLUMN dv_ratio REAL;",
                "ALTER TABLE bars ADD COLUMN dv_ttm REAL;"
            ]
            with self.engine.connect() as conn:
                for alter_sql in alter_sqls:
                    try:
                        conn.execute(text(alter_sql))
                        conn.commit()
                    except:
                        pass  # 列可能已存在
        except:
            pass

    def load_bars(self, symbol: str, start: str, end: str,
                  interval: str = "1d", price_type: str = "") -> pd.DataFrame:
        """
        从本地加载数据

        Args:
            symbol: 股票代码
            start: 开始日期 YYYY-MM-DD
            end: 结束日期 YYYY-MM-DD
            interval: 时间周期，默认 1d
            price_type: 价格类型，''=不复权（默认）, 'qfq'=前复权

        Returns:
            包含OHLCV数据的DataFrame
        """
        query = """
        SELECT * FROM bars
        WHERE symbol = :symbol
          AND interval = :interval
          AND datetime >= :start
          AND datetime <= :end
        ORDER BY datetime
        """
        df = pd.read_sql_query(
            query,
            self.engine,
            params={"symbol": symbol, "interval": interval, "start": start, "end": end}
        )
        if not df.empty:
            df["datetime"] = pd.to_datetime(df["datetime"])

            # 根据价格类型选择对应的列
            if price_type == 'qfq' and 'close_qfq' in df.columns:
                # 使用前复权价格
                df['open'] = df['open_qfq']
                df['high'] = df['high_qfq']
                df['low'] = df['low_qfq']
                df['close'] = df['close_qfq']
            # else: 默认使用主价格列（不复权）

        return df

    def get_stock_name(self, symbol: str) -> str:
        """
        获取股票名称（优先从数据库读取）

        Args:
            symbol: 股票代码

        Returns:
            股票名称，如果获取失败则返回 "未知"
        """
        # 标准化代码
        code = symbol.split('.')[0] if '.' in symbol else symbol

        # 先从缓存查找
        if code in self._stock_name_cache:
            return self._stock_name_cache[code]

        # 从数据库读取
        try:
            query = "SELECT name FROM stock_names WHERE code = :code"
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"code": code}).fetchone()
                if result and result[0]:
                    self._stock_name_cache[code] = result[0]
                    return result[0]
        except Exception as e:
            pass

        # 尝试从子类实现获取
        try:
            name = self._get_stock_name_from_api(symbol)
            if name:
                self._stock_name_cache[code] = name
                # 保存到数据库
                self.save_stock_name(code, name)
                return name
        except:
            pass

        return "未知"

    def save_stock_name(self, code: str, name: str) -> bool:
        """
        保存股票名称到数据库

        Args:
            code: 股票代码（6位）
            name: 股票名称

        Returns:
            是否保存成功
        """
        try:
            upsert_sql = """
            INSERT INTO stock_names (code, name, updated_at)
            VALUES (:code, :name, :updated_at)
            ON CONFLICT(code) DO UPDATE SET
                name = :name,
                updated_at = :updated_at
            """
            with self.engine.connect() as conn:
                conn.execute(text(upsert_sql), {
                    "code": code,
                    "name": name,
                    "updated_at": datetime.now().isoformat()
                })
                conn.commit()
            return True
        except Exception as e:
            print(f"保存股票名称失败 {code}: {e}")
            return False

    def save_stock_names_batch(self, names_dict: dict) -> int:
        """
        批量保存股票名称到数据库

        Args:
            names_dict: 股票代码到名称的映射 {code: name}

        Returns:
            成功保存的数量
        """
        success_count = 0
        for code, name in names_dict.items():
            if self.save_stock_name(code, name):
                success_count += 1
        return success_count

    def _get_stock_name_from_api(self, symbol: str) -> str:
        """
        从 API 获取股票名称（子类可选实现）

        Args:
            symbol: 股票代码

        Returns:
            股票名称，如果未实现或失败则返回 None
        """
        # 默认实现：子类可以覆盖此方法
        return None

    def check_local_data(self, symbol: str, start_date: str, end_date: str) -> dict:
        """
        检查本地数据库中的数据情况

        Args:
            symbol: 股票代码
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            包含数据信息的字典:
            {
                'has_data': bool,        # 是否有数据
                'latest_date': str,      # 最新数据日期
                'days_diff': int,        # 距离今天的天数
                'need_update': bool      # 是否需要更新
            }
        """
        code = symbol.split('.')[0] if '.' in symbol else symbol
        start_date_readable = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        end_date_readable = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"

        result = {
            'has_data': False,
            'latest_date': None,
            'days_diff': None,
            'need_update': True
        }

        try:
            existing_df = pd.read_sql_query(
                "SELECT datetime FROM bars WHERE symbol = :symbol AND interval = '1d' "
                "AND datetime >= :start AND datetime <= :end "
                "ORDER BY datetime DESC LIMIT 1",
                self.engine,
                params={"symbol": code, "start": start_date_readable, "end": end_date_readable}
            )

            if not existing_df.empty:
                latest_date = existing_df['datetime'].max()
                latest_date_dt = pd.to_datetime(latest_date)
                end_date_dt = pd.to_datetime(end_date_readable)

                # 计算数据日期距离今天的天数
                today = pd.Timestamp.now().date()
                days_diff = (today - latest_date_dt.date()).days

                result['has_data'] = True
                result['latest_date'] = latest_date
                result['days_diff'] = days_diff

                # 判断是否需要更新
                if latest_date_dt >= end_date_dt:
                    result['need_update'] = False
                elif days_diff <= 2:
                    result['need_update'] = False

        except Exception as e:
            pass

        return result

    def should_skip_download(self, symbol: str, start_date: str, end_date: str) -> tuple:
        """
        判断是否应该跳过下载

        Args:
            symbol: 股票代码
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            (should_skip: bool, reason: str)
        """
        info = self.check_local_data(symbol, start_date, end_date)

        if not info['has_data']:
            return False, "本地无数据"

        if not info['need_update']:
            if info['days_diff'] is not None and info['days_diff'] <= 2:
                return True, f"本地数据已是最新（最新: {info['latest_date']}）"
            else:
                return True, f"本地数据已完整（最新: {info['latest_date']}）"

        return False, f"本地数据较旧（最新: {info['latest_date']}）"

    def query(self):
        """
        获取查询器实例

        Returns:
            StockQuery 查询器实例
        """
        from .query.stock_query import StockQuery

        # 从 engine.url 提取数据库路径
        db_path = self.engine.url.database
        return StockQuery(db_path)

    @abstractmethod
    def save_daily(self, symbol: str, start_date: str = "20200101",
                   end_date: str = None, adjust: str = "qfq"):
        """
        保存日线数据（子类必须实现）

        Args:
            symbol: 股票代码
            start_date: 开始日期，格式 YYYYMMDD
            end_date: 结束日期，格式 YYYYMMDD，默认为今天
            adjust: 复权类型
        """
        pass
