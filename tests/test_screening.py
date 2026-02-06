"""
股票筛选系统测试脚本

测试基础筛选功能
"""
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.screening.screening_engine import ScreeningEngine
from src.screening.criteria.basic_criteria import RangeCriteria, GreaterThanCriteria, PercentileCriteria
from src.screening.criteria.industry_criteria import IndustryFilter, IndustryRelativeCriteria
from src.screening.base_criteria import AndCriteria, OrCriteria, NotCriteria
from src.screening.strategies.predefined_strategies import PredefinedStrategies
from src.screening.rule_engine import RuleEngine


def test_basic_screening():
    """测试基础筛选"""
    print("=" * 50)
    print("测试基础筛选功能")
    print("=" * 50)

    # 创建引擎
    db_path = 'data/tushare_data.db'
    if not os.path.exists(db_path):
        print(f"数据库不存在: {db_path}")
        return

    engine = ScreeningEngine(db_path)

    # 测试1：简单的PE筛选
    print("\n测试1: PE筛选 (0 < PE < 30)")
    criteria = RangeCriteria('pe_ttm', 0, 30)
    result = engine.screen(criteria, limit=10)
    print(f"筛选结果: {len(result)} 只股票")
    if not result.empty:
        print(result[['symbol', 'stock_name', 'pe_ttm', 'sw_l1']].head())

    # 测试2：AND组合
    print("\n测试2: AND组合 (PE < 20 AND ROE > 10)")
    criteria = AndCriteria(
        RangeCriteria('pe_ttm', 0, 20),
        GreaterThanCriteria('latest_roe', 10)
    )
    result = engine.screen(criteria, limit=10)
    print(f"筛选结果: {len(result)} 只股票")
    if not result.empty:
        print(result[['symbol', 'stock_name', 'pe_ttm', 'latest_roe']].head())

    # 测试3：行业过滤
    print("\n测试3: 排除金融行业")
    criteria = AndCriteria(
        RangeCriteria('pe_ttm', 0, 50),
        IndustryFilter(['银行', '非银金融'], mode='blacklist')
    )
    result = engine.screen(criteria, limit=10)
    print(f"筛选结果: {len(result)} 只股票")
    if not result.empty:
        print(result[['symbol', 'stock_name', 'pe_ttm', 'sw_l1']].head())

    # 测试4：行业内相对筛选
    print("\n测试4: 行业内ROE前30%")
    criteria = AndCriteria(
        IndustryRelativeCriteria('latest_roe', percentile=0.3, min_stocks=5),
        GreaterThanCriteria('amount', 3000)
    )
    result = engine.screen(criteria, limit=20)
    print(f"筛选结果: {len(result)} 只股票")
    if not result.empty:
        print(result[['symbol', 'stock_name', 'latest_roe', 'sw_l1']].head())

    # 测试5：预定义策略
    print("\n测试5: 预定义策略 - 价值投资")
    criteria = PredefinedStrategies.value_strategy()
    result = engine.screen(criteria, limit=10)
    print(f"筛选结果: {len(result)} 只股票")
    if not result.empty:
        print(result[['symbol', 'stock_name', 'pe_ttm', 'pb', 'latest_roe']].head())

    # 测试6：RuleEngine
    print("\n测试6: RuleEngine - JSON配置")
    config = {
        'type': 'AND',
        'criteria': [
            {'type': 'Range', 'column': 'pe_ttm', 'min_val': 0, 'max_val': 30},
            {'type': 'IndustryFilter', 'industries': ['银行', '非银金融'], 'mode': 'blacklist'},
            {'type': 'GreaterThan', 'column': 'latest_roe', 'threshold': 8}
        ]
    }
    criteria = RuleEngine.build_from_config(config)
    result = engine.screen(criteria, limit=10)
    print(f"筛选结果: {len(result)} 只股票")
    if not result.empty:
        print(result[['symbol', 'stock_name', 'pe_ttm', 'latest_roe', 'sw_l1']].head())

    # 测试7：OR组合
    print("\n测试7: OR组合 (PE < 10 OR ROE > 20)")
    criteria = OrCriteria(
        RangeCriteria('pe_ttm', 0, 10),
        GreaterThanCriteria('latest_roe', 20)
    )
    result = engine.screen(criteria, limit=10)
    print(f"筛选结果: {len(result)} 只股票")
    if not result.empty:
        print(result[['symbol', 'stock_name', 'pe_ttm', 'latest_roe']].head())


def test_industry_statistics():
    """测试行业统计"""
    print("\n" + "=" * 50)
    print("测试行业统计计算")
    print("=" * 50)

    from src.screening.calculators.industry_statistics_calculator import IndustryStatisticsCalculator

    db_path = 'data/tushare_data.db'
    if not os.path.exists(db_path):
        print(f"数据库不存在: {db_path}")
        return

    calc = IndustryStatisticsCalculator(db_path)

    # 计算行业统计
    print("\n计算行业统计中...")
    stats_df = calc.calculate_industry_statistics(metrics=['pe_ttm', 'pb', 'total_mv'])

    if not stats_df.empty:
        print(f"计算完成: {len(stats_df)} 条统计记录")
        print(f"覆盖行业数: {stats_df['sw_l1'].nunique()} 个一级，"
              f"{stats_df['sw_l2'].nunique()} 个二级，"
              f"{stats_df['sw_l3'].nunique()} 个三级")

        # 显示部分统计结果
        print("\n示例统计数据（银行行业的PE统计）:")
        bank_stats = stats_df[(stats_df['sw_l1'] == '银行') & (stats_df['metric_name'] == 'pe_ttm')]
        if not bank_stats.empty:
            print(bank_stats[['sw_l1', 'sw_l2', 'metric_name', 'p25', 'p50', 'p75']].head())

        # 保存到数据库
        print("\n保存行业统计到数据库...")
        success = calc.save_industry_statistics(stats_df)
        if success:
            print("保存成功！")
        else:
            print("保存失败！")
    else:
        print("没有计算到统计数据")


def test_list_strategies():
    """列出所有预定义策略"""
    print("\n" + "=" * 50)
    print("预定义策略列表")
    print("=" * 50)

    strategies = PredefinedStrategies.list_strategies()
    for key, name in strategies.items():
        print(f"  {key}: {name}")


def main():
    """主函数"""
    print("股票筛选系统测试")
    print("=" * 50)

    # 检查数据库
    db_path = 'data/tushare_data.db'
    if not os.path.exists(db_path):
        print(f"错误: 数据库不存在 - {db_path}")
        print("请先运行数据更新脚本获取数据")
        return

    # 运行测试
    test_list_strategies()
    test_basic_screening()
    # test_industry_statistics()  # 可选：测试行业统计计算

    print("\n" + "=" * 50)
    print("测试完成!")
    print("=" * 50)


if __name__ == '__main__':
    main()
