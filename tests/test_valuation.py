"""
估值模型系统测试

测试各个估值模型的基本功能
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.valuation.models.relative_valuation import RelativeValuationModel
from src.valuation.models.absolute_valuation import DCFValuationModel
from src.valuation.engine.valuation_engine import ValuationEngine


def test_pe_valuation():
    """测试PE估值模型"""
    print("\n=== 测试PE估值模型 ===")
    model = RelativeValuationModel(method='pe')

    # 测试一个有数据的股票（需要数据库中有对应数据）
    # 使用茅台股票代码 600519 作为测试
    result = model.calculate('600519')

    if 'error' in result:
        print(f"错误: {result['error']}")
        return False

    print(f"股票: {result['symbol']}")
    print(f"模型: {result['model']}")
    print(f"当前价格: {result['current_price']}")
    print(f"公允价值: {result['fair_value']}")
    print(f"涨跌幅空间: {result['upside_downside']}%")
    print(f"评级: {result['rating']}")
    print(f"置信度: {result['confidence']}")
    return True


def test_combined_valuation():
    """测试组合估值模型"""
    print("\n=== 测试组合估值模型 ===")
    model = RelativeValuationModel(method='combined')

    result = model.calculate('600519')

    if 'error' in result:
        print(f"错误: {result['error']}")
        return False

    print(f"股票: {result['symbol']}")
    print(f"模型: {result['model']}")
    print(f"当前价格: {result['current_price']}")
    print(f"公允价值: {result['fair_value']}")
    print(f"涨跌幅空间: {result['upside_downside']}%")
    print(f"评级: {result['rating']}")
    print(f"置信度: {result['confidence']}")
    return True


def test_valuation_engine():
    """测试估值引擎"""
    print("\n=== 测试估值引擎 ===")

    engine = ValuationEngine()

    # 注册模型
    for method in ['pe', 'pb', 'ps']:
        model = RelativeValuationModel(method=method)
        engine.register_model(model)

    # 列出模型
    models = engine.list_models()
    print(f"可用模型: {models}")

    # 单股票估值
    result = engine.value_stock('600519', methods=['Relative_PE'])

    if 'error' in result:
        print(f"错误: {result['error']}")
        return False

    print(f"股票: {result['symbol']}")
    print(f"模型: {result['model']}")
    print(f"公允价值: {result['fair_value']}")
    print(f"涨跌幅空间: {result['upside_downside']}%")
    print(f"评级: {result['rating']}")
    return True


def test_industry_params():
    """测试行业参数"""
    print("\n=== 测试行业参数 ===")

    from src.valuation.config.industry_params import (
        get_industry_params,
        get_market_cap_premium,
        get_growth_premium
    )

    # 测试获取行业参数
    params = get_industry_params('801010')  # 银行
    if params:
        print(f"银行行业: {params['name']}")
        print(f"主要估值方法: {params['primary_method']}")
        print(f"ROE基准: {params['roe_baseline']}%")
    else:
        print("未找到银行行业参数")

    # 测试市值溢价
    premium = get_market_cap_premium(100)  # 100亿
    print(f"市值溢价(100亿): {premium}")

    # 测试成长性溢价
    growth_premium = get_growth_premium(25)  # 25%增长率
    print(f"成长性溢价(25%): {growth_premium}")

    return True


def main():
    """运行所有测试"""
    print("=" * 50)
    print("估值模型系统测试")
    print("=" * 50)

    results = []

    # 运行测试
    results.append(("行业参数", test_industry_params()))
    results.append(("PE估值模型", test_pe_valuation()))
    results.append(("组合估值模型", test_combined_valuation()))
    results.append(("估值引擎", test_valuation_engine()))

    # 打印结果
    print("\n" + "=" * 50)
    print("测试结果")
    print("=" * 50)

    for name, success in results:
        status = "✓ 通过" if success else "✗ 失败"
        print(f"{name}: {status}")

    # 统计
    passed = sum(1 for _, success in results if success)
    total = len(results)
    print(f"\n总计: {passed}/{total} 通过")

    return passed == total


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
