"""
预定义筛选策略

提供常用的预定义筛选策略，可直接使用或作为示例
"""
from src.screening.base_criteria import AndCriteria
from src.screening.criteria.basic_criteria import RangeCriteria, GreaterThanCriteria, PercentileCriteria
from src.screening.criteria.industry_criteria import (
    IndustryFilter, IndustryRelativeCriteria
)


class PredefinedStrategies:
    """预定义筛选策略集合"""

    @staticmethod
    def liquidity_strategy():
        """
        流动性策略：按成本排序优化

        执行顺序（从低成本到高成本）：
        1. 成本1: 价格范围过滤（直接从数据库读取）
        2. 成本5: 市值过滤（简单比较）
        3. 成本10: 换手率过滤（聚合计算）

        Returns:
            AndCriteria: 筛选条件
        """
        # 注意：条件顺序会被AndCriteria自动优化（按cost排序）
        return AndCriteria(
            RangeCriteria('close', 2, 500),           # cost=1, 快速排除低价股
            RangeCriteria('circ_mv', 50000, None),     # cost=5, 排除小盘股
            RangeCriteria('turnover', 0.3, None)       # cost=10, 活跃度过滤
        )

    @staticmethod
    def value_strategy():
        """
        价值投资策略

        特点：
        1. 低估值：PE < 20, PB < 3
        2. 高质量：ROE > 15%
        3. 流动性保证：成交额 > 5000万

        Returns:
            AndCriteria: 筛选条件
        """
        return AndCriteria(
            RangeCriteria('pe_ttm', 0, 20),
            RangeCriteria('pb', 0, 3),
            GreaterThanCriteria('latest_roe', 15),
            GreaterThanCriteria('amount', 5000)
        )

    @staticmethod
    def growth_strategy():
        """
        成长股策略

        特点：
        1. 高成长：营收增长率 > 20%
        2. 盈利增长：净利润增长率 > 15%
        3. 合理估值：PE < 50（容忍度高）
        4. 基本质量：ROE > 8%

        Returns:
            AndCriteria: 筛选条件
        """
        return AndCriteria(
            GreaterThanCriteria('latest_or_yoy', 20),
            GreaterThanCriteria('latest_gr_yoy', 15),
            RangeCriteria('pe_ttm', 0, 50),
            GreaterThanCriteria('latest_roe', 8),
            GreaterThanCriteria('amount', 5000)
        )

    @staticmethod
    def tech_growth_strategy():
        """
        科技成长策略（行业白名单）

        只筛选科技相关行业：
        - 计算机、通信、电子、传媒
        - 营收增长 > 20%
        - PE < 60（科技股容忍度）
        - ROE > 8%

        Returns:
            AndCriteria: 筛选条件
        """
        return AndCriteria(
            # 行业白名单
            IndustryFilter(['计算机', '通信', '电子', '传媒'], mode='whitelist'),
            # 成长性
            GreaterThanCriteria('latest_or_yoy', 20),
            # 估值（科技股标准）
            RangeCriteria('pe_ttm', 0, 60),
            GreaterThanCriteria('latest_roe', 8),
            # 流动性
            GreaterThanCriteria('amount', 5000)
        )

    @staticmethod
    def quality_strategy():
        """
        质量策略（行业内相对筛选）

        特点：
        1. 行业内ROE前30%的股票
        2. 基础流动性保证
        3. 财务健康：负债率 < 60%

        Returns:
            AndCriteria: 筛选条件
        """
        return AndCriteria(
            # 按行业内相对质量筛选
            IndustryRelativeCriteria('latest_roe', percentile=0.3, min_stocks=5),
            # 财务健康
            RangeCriteria('debt_to_assets', 0, 60),
            # 流动性
            GreaterThanCriteria('amount', 3000)
        )

    @staticmethod
    def dividend_strategy():
        """
        股息策略

        特点：
        1. 高股息率：使用前25%的股票
        2. 稳定性：市值 > 100亿
        3. 合理估值：PE < 30

        Returns:
            AndCriteria: 筛选条件
        """
        # 注意：股息率需要从其他数据源获取，这里使用percentile作为示例
        return AndCriteria(
            PercentileCriteria('pe_ttm', 0.25),  # 低估值前25%
            GreaterThanCriteria('total_mv', 1000000),  # 市值>100亿 (单位万元)
            GreaterThanCriteria('amount', 5000)
        )

    @staticmethod
    def low_volatility_strategy():
        """
        低波动策略

        特点：
        1. 大盘股：市值 > 200亿
        2. 低估值：PE < 30
        3. 行业内质量前30%

        Returns:
            AndCriteria: 筛选条件
        """
        return AndCriteria(
            GreaterThanCriteria('total_mv', 2000000),  # 市值>200亿
            RangeCriteria('pe_ttm', 0, 30),
            IndustryRelativeCriteria('latest_roe', percentile=0.3, min_stocks=5),
            GreaterThanCriteria('amount', 10000)
        )

    @staticmethod
    def turnaround_strategy():
        """
        困境反转策略

        特点：
        1. 低估值：PE < 10 或 PB < 1
        2. 行业相对质量前40%（放宽标准）
        3. 排除极端小盘股

        Returns:
            AndCriteria: 筛选条件
        """
        return AndCriteria(
            RangeCriteria('pb', 0, 1.5),
            IndustryRelativeCriteria('latest_roe', percentile=0.4, min_stocks=3),
            GreaterThanCriteria('circ_mv', 30000),  # 流通市值>30亿
            GreaterThanCriteria('amount', 2000)
        )

    @staticmethod
    def momentum_quality_strategy():
        """
        动量质量策略

        特点：
        1. 高质量：行业内ROE前30%
        2. 合理估值：PE < 行业中位数
        3. 流动性活跃

        Returns:
            AndCriteria: 筛选条件
        """
        return AndCriteria(
            IndustryRelativeCriteria('latest_roe', percentile=0.3, min_stocks=5),
            PercentileCriteria('pe_ttm', 0.5),  # PE低于中位数
            GreaterThanCriteria('turnover', 2),  # 换手率>2%
            GreaterThanCriteria('amount', 5000)
        )

    @staticmethod
    def exclude_financials_strategy():
        """
        排除金融策略

        特点：
        1. 排除银行、非银金融
        2. 价值投资标准
        3. 流动性保证

        Returns:
            AndCriteria: 筛选条件
        """
        return AndCriteria(
            # 行业黑名单
            IndustryFilter(['银行', '非银金融'], mode='blacklist'),
            # 价值条件
            RangeCriteria('pe_ttm', 0, 30),
            RangeCriteria('pb', 0, 5),
            GreaterThanCriteria('latest_roe', 10),
            # 流动性
            GreaterThanCriteria('amount', 3000)
        )

    @staticmethod
    def list_strategies():
        """
        列出所有预定义策略

        Returns:
            dict: 策略名称到方法的映射
        """
        return {
            'liquidity': '流动性策略',
            'value': '价值投资策略',
            'growth': '成长股策略',
            'tech_growth': '科技成长策略',
            'quality': '质量策略（行业内相对）',
            'dividend': '股息策略',
            'low_volatility': '低波动策略',
            'turnaround': '困境反转策略',
            'momentum_quality': '动量质量策略',
            'exclude_financials': '排除金融策略'
        }
