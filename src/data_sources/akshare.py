# akshare.py
import akshare as ak
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os
from src.data_sources.base import BaseStockDB


class AKShareDB(BaseStockDB):
    def __init__(self, db_path: str = "data/akshare_data.db"):
        """
        初始化 AKShare 数据库

        Args:
            db_path: 数据库文件路径
        """
        # 调用父类初始化
        super().__init__(db_path)

    def _detect_exchange(self, symbol: str) -> str:
        """自动识别交易所"""
        if symbol.startswith(('600', '601', '603', '688')):
            return 'SSE'
        elif symbol.startswith(('000', '001', '002', '300', '301')):
            return 'SZSE'
        else:
            return 'HK'  # 默认港股

    def save_daily(self, symbol: str, start_date: str = "20200101",
                   end_date: str = None, adjust: str = None):
        """
        保存日线数据（实现抽象基类方法）

        Args:
            symbol: 股票代码
            start_date: 开始日期，格式 YYYYMMDD
            end_date: 结束日期，格式 YYYYMMDD
            adjust: 复权类型（AKShare 不使用此参数，忽略）
        """
        # AKShare 使用 save_a_stock 和 save_hk_stock
        # 这里简单调用 save_a_stock（适用于A股）
        return self.save_a_stock(symbol, start_date, end_date)

    def save_a_stock(self, symbol: str, start_date: str = "20100101", end_date: str = None):
        """保存 A 股日线数据（同时保存不复权和前复权价格）"""
        if end_date is None:
            end_date = datetime.today().strftime("%Y%m%d")

        # 使用基类方法检查是否应该跳过下载
        should_skip, reason = self.should_skip_download(symbol, start_date, end_date)
        if should_skip:
            print(f"⏭️  {symbol} {reason}")
            return
        else:
            print(f"📥 {symbol} {reason}，开始下载...")

        try:
            # 获取不复权数据
            df_orig = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=""  # 不复权
            )
            if df_orig.empty:
                print(f"⚠️ {symbol} 无数据")
                return

            # 获取前复权数据
            df_qfq = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"  # 前复权
            )

            # 合并数据
            df_orig = df_orig.rename(columns={
                "日期": "datetime",
                "开盘": "open",
                "最高": "high",
                "最低": "low",
                "收盘": "close",
                "成交量": "volume",
                "换手率": "turnover"
            })

            df_qfq = df_qfq.rename(columns={
                "开盘": "open_qfq",
                "最高": "high_qfq",
                "最低": "low_qfq",
                "收盘": "close_qfq"
            })

            # 合并不复权和前复权数据
            df = df_orig.merge(df_qfq[['datetime', 'open_qfq', 'high_qfq', 'low_qfq', 'close_qfq']],
                              on='datetime', how='left')

            df["symbol"] = symbol
            df["exchange"] = self._detect_exchange(symbol)
            df["interval"] = "1d"
            df["datetime"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d")
            df["amount"] = None  # AKShare 暂时没有成交额

            # 存入数据库
            try:
                df[["symbol", "exchange", "interval", "datetime",
                    "open", "high", "low", "close",  # 不复权价格
                    "open_qfq", "high_qfq", "low_qfq", "close_qfq",  # 前复权价格
                    "volume", "turnover", "amount"]].to_sql(
                    "bars", self.engine, if_exists="append", index=False, method="multi"
                )
                print(f"✅ 已保存 {symbol} 共 {len(df)} 条记录")
            except Exception as e:
                if "UNIQUE constraint" in str(e) or "duplicate" in str(e).lower():
                    print(f"⏭️  {symbol} 数据已存在，跳过")
                else:
                    print(f"⚠️  {symbol} 数据库操作失败: {e}")
        except Exception as e:
            print(f"❌ {symbol} 下载失败: {e}")

    def save_hk_stock(self, symbol: str, start_date: str = "20100101", end_date: str = None):
        """保存港股日线数据"""
        if end_date is None:
            end_date = datetime.today().strftime("%Y%m%d")

        # 从配置文件读取复权类型
        from config.settings import DEFAULT_ADJUST
        adjust = DEFAULT_ADJUST

        # 使用基类方法检查是否应该跳过下载
        should_skip, reason = self.should_skip_download(symbol, start_date, end_date)
        if should_skip:
            print(f"⏭️  {symbol} {reason}")
            return
        else:
            print(f"📥 {symbol} {reason}，开始下载...")

        try:
            df = ak.stock_hk_daily(symbol=symbol, adjust=adjust)
            # AKShare 港股返回的是完整历史，需手动切片
            df["date"] = pd.to_datetime(df["date"])
            mask = (df["date"] >= start_date) & (df["date"] <= end_date)
            df = df[mask]
            if df.empty:
                print(f"⚠️ {symbol} 无数据")
                return

            df = df.rename(columns={
                "date": "datetime",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume"
            })
            df["symbol"] = symbol
            df["exchange"] = "HK"
            df["interval"] = "1d"
            df["datetime"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d")
            df["turnover"] = None  # 港股数据无换手率
            df["amount"] = None  # 港股数据无成交额

            # 存入数据库
            try:
                df[["symbol", "exchange", "interval", "datetime", "open", "high", "low", "close", "volume", "turnover", "amount"]].to_sql(
                    "bars", self.engine, if_exists="append", index=False, method="multi"
                )
                print(f"✅ 已保存 {symbol} 共 {len(df)} 条记录")
            except Exception as e:
                if "UNIQUE constraint" in str(e) or "duplicate" in str(e).lower():
                    print(f"⏭️  {symbol} 数据已存在，跳过")
                else:
                    print(f"⚠️  {symbol} 数据库操作失败: {e}")
        except Exception as e:
            print(f"❌ {symbol} 下载失败: {e}")