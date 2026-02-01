"""
测试 StockQuery 类
"""
import unittest
import os
import pandas as pd
from src.data_sources.query.stock_query import StockQuery
from src.data_sources.tushare import TushareDB


class TestStockQuery(unittest.TestCase):
    """测试 StockQuery 类"""

    @classmethod
    def setUpClass(cls):
        """测试类初始化：创建测试数据库"""
        # 使用测试数据库路径
        cls.test_db_path = "data/test_data.db"
        cls.test_token = os.getenv("TUSHARE_TOKEN", "test_token")

    def setUp(self):
        """每个测试方法执行前的设置"""
        # 如果测试数据库不存在，跳过测试
        if not os.path.exists(self.test_db_path):
            self.skipTest(f"测试数据库不存在: {self.test_db_path}")

        self.query = StockQuery(self.test_db_path)

    def test_query_bars(self):
        """测试基础K线查询"""
        df = self.query.query_bars("600382", "2025-01-01", "2025-01-31")
        self.assertIsInstance(df, pd.DataFrame)
        # 如果有数据，验证列存在
        if not df.empty:
            self.assertIn('close', df.columns)
            self.assertIn('datetime', df.columns)

    def test_query_by_price_range(self):
        """测试价格范围查询"""
        # 查询价格在 10-50 元之间的数据
        df = self.query.query_by_price_range("600382", "2025-01-01", "2025-12-31",
                                              min_price=10, max_price=50)
        self.assertIsInstance(df, pd.DataFrame)
        # 验证结果
        if not df.empty:
            self.assertTrue(all(df['close'] >= 10))
            self.assertTrue(all(df['close'] <= 50))

    def test_query_by_turnover(self):
        """测试换手率范围查询"""
        df = self.query.query_by_turnover("600382", "2025-01-01", "2025-12-31", min_turnover=1.0)
        self.assertIsInstance(df, pd.DataFrame)
        # 验证结果
        if not df.empty and 'turnover' in df.columns:
            self.assertTrue(all(df['turnover'] >= 1.0))

    def test_query_by_change(self):
        """测试涨跌幅范围查询"""
        # 查询涨跌幅 > 5% 的数据
        df = self.query.query_by_change("600382", "2025-01-01", "2025-12-31", min_change=5.0)
        self.assertIsInstance(df, pd.DataFrame)
        # 验证结果
        if not df.empty and 'pct_chg' in df.columns:
            self.assertTrue(all(df['pct_chg'] >= 5.0))

    def test_query_with_filters(self):
        """测试复合条件查询"""
        # 多个条件组合
        df = self.query.query_with_filters(
            "600382", "2025-01-01", "2025-12-31",
            filters={
                'close': (10, 50),          # 价格 10-50
                'volume': (1000000, None),  # 成交量 > 100万
                'pct_chg': (-5, 5)          # 涨跌幅 -5% 到 5%
            }
        )
        self.assertIsInstance(df, pd.DataFrame)
        # 验证结果
        if not df.empty:
            if 'close' in df.columns:
                self.assertTrue(all(df['close'] >= 10))
                self.assertTrue(all(df['close'] <= 50))
            if 'volume' in df.columns:
                self.assertTrue(all(df['volume'] >= 1000000))
            if 'pct_chg' in df.columns:
                self.assertTrue(all(df['pct_chg'] >= -5))
                self.assertTrue(all(df['pct_chg'] <= 5))

    def test_check_data_completeness(self):
        """测试数据完整性检查"""
        info = self.query.check_data_completeness("600382", "2025-01-01", "2025-01-31")
        self.assertIsInstance(info, dict)
        self.assertIn('completeness_rate', info)
        self.assertIn('missing_count', info)
        self.assertIn('missing_days', info)
        self.assertIn('total_days', info)
        self.assertIn('actual_days', info)
        # 验证完整率在 0-1 之间
        self.assertGreaterEqual(info['completeness_rate'], 0.0)
        self.assertLessEqual(info['completeness_rate'], 1.0)

    def test_find_missing_dates(self):
        """测试查找缺失日期"""
        missing_dates = self.query.find_missing_dates("600382", "2025-01-01", "2025-01-31")
        self.assertIsInstance(missing_dates, list)

    def test_calculate_returns(self):
        """测试收益率计算"""
        df = self.query.calculate_returns("600382", "2025-01-01", "2025-12-31", period=1)
        self.assertIsInstance(df, pd.DataFrame)
        if not df.empty:
            self.assertIn('return', df.columns)

    def test_calculate_volatility(self):
        """测试波动率计算"""
        df = self.query.calculate_volatility("600382", "2025-01-01", "2025-12-31", window=20)
        self.assertIsInstance(df, pd.DataFrame)
        if not df.empty:
            self.assertIn('volatility', df.columns)

    def test_get_summary_stats(self):
        """测试汇总统计"""
        stats = self.query.get_summary_stats("600382", "2025-01-01", "2025-12-31")
        self.assertIsInstance(stats, dict)
        self.assertIn('total_days', stats)
        self.assertIn('avg_volume', stats)
        self.assertIn('avg_turnover', stats)
        self.assertIn('total_return', stats)
        self.assertIn('annual_return', stats)
        self.assertIn('volatility', stats)
        self.assertIn('max_drawdown', stats)

    def test_query_multiple_symbols(self):
        """测试批量查询"""
        symbols = ["600382", "600711"]
        results = self.query.query_multiple_symbols(symbols, "2025-01-01", "2025-01-31")
        self.assertIsInstance(results, dict)
        self.assertEqual(len(results), 2)


class TestTushareDBQuery(unittest.TestCase):
    """测试 TushareDB 的 query() 工厂方法"""

    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        cls.test_db_path = "data/test_data.db"
        cls.test_token = os.getenv("TUSHARE_TOKEN", "test_token")

    def setUp(self):
        """每个测试方法执行前的设置"""
        if not os.path.exists(self.test_db_path):
            self.skipTest(f"测试数据库不存在: {self.test_db_path}")

    def test_query_factory_method(self):
        """测试 query() 工厂方法"""
        db = TushareDB(token=self.test_token, db_path=self.test_db_path)
        query = db.query()
        self.assertIsInstance(query, StockQuery)

    def test_query_integration(self):
        """测试 query() 方法集成"""
        db = TushareDB(token=self.test_token, db_path=self.test_db_path)
        query = db.query()
        df = query.query_bars("600382", "2025-01-01", "2025-01-31")
        self.assertIsInstance(df, pd.DataFrame)


if __name__ == '__main__':
    unittest.main()
