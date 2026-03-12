"""
估值服务

提供股票估值的业务逻辑
"""
from flask import jsonify
from typing import Dict, List, Optional, Any
import traceback


def valuation_summary(
    symbol: str,
    methods: Optional[str] = None,
    date: Optional[str] = None,
    fiscal_date: Optional[str] = None,
    combine_method: str = 'weighted',
    dcf_config: Optional[Dict[str, Any]] = None
) -> tuple[Dict[str, Any], int]:
    """
    获取股票估值摘要

    Args:
        symbol: 股票代码
        methods: 估值方法，逗号分隔 (pe, pb, ps, peg, dcf, combined)，默认全部
        date: 估值日期/股价日期 (YYYY-MM-DD 或 YYYYMMDD)
        fiscal_date: 财务数据截止报告期 (YYYY-MM-DD 或 YYYYMMDD)，不传则取最新
        combine_method: 组合方式 (weighted, average, median, max_confidence, bayesian, min_fair_value)
        dcf_config: DCF估值配置参数（可选）
            - risk_profile: 风险偏好 ('conservative', 'balanced', 'aggressive')
            - 或直接指定参数:
            - forecast_years: 预测年数
            - terminal_growth: 终值增长率
            - risk_free_rate: 无风险利率
            - market_return: 市场回报率
            - growth_rate_cap: 增长率上限
            - wacc_min/max: WACC范围
            - beta: Beta系数

    Returns:
        (响应数据, HTTP状态码)
    """
    try:
        from src.valuation.engine.valuation_engine import ValuationEngine

        # 初始化估值引擎
        engine = ValuationEngine()

        # 注册相对估值模型
        from src.valuation.models.relative_valuation import RelativeValuationModel

        # 注册所有相对估值方法
        for method in ['pe', 'pb', 'ps', 'peg']:
            model = RelativeValuationModel(method=method)
            engine.register_model(model)

        # 注册combined模型
        combined_model = RelativeValuationModel(method='combined')
        engine.register_model(combined_model)

        # 注册DCF模型（可选）
        try:
            from src.valuation.models.absolute_valuation import DCFValuationModel
            # 处理DCF配置
            if dcf_config:
                # 如果指定了risk_profile，使用预设参数
                risk_profile = dcf_config.pop('risk_profile', None)
                if risk_profile:
                    preset_config = get_dcf_preset_config(risk_profile)
                    dcf_config = {**preset_config, **dcf_config}

            dcf_model = DCFValuationModel(config=dcf_config if dcf_config else None)
            engine.register_model(dcf_model)
        except Exception as e:
            # DCF模型可能因为数据不足而注册失败
            import logging
            logging.getLogger(__name__).warning(f"Failed to register DCF model: {e}")

        # 模型注册完毕后，统一设置财务数据截止报告期（会传播到所有已注册模型）
        if fiscal_date:
            engine.set_fiscal_date(fiscal_date, valuation_date=date)
        # 解析估值方法
        if methods:
            method_list = [m.strip().lower() for m in methods.split(',')]
            # 转换方法名到模型名
            model_names = []

            for m in method_list:
                if m == 'combined':
                    # combined 表示 PE/PB/PS 三种相对估值方法，使用用户选择的组合方式
                    model_names.extend(['Relative_PE', 'Relative_PB', 'Relative_PS'])
                elif m == 'peg':
                    model_names.append('Relative_PEG')
                elif m == 'dcf':
                    model_names.append('DCF')
                else:
                    model_names.append(f'Relative_{m.upper()}')

            # 去重
            model_names = list(dict.fromkeys(model_names))
        else:
            model_names = None  # 使用所有模型

        # 执行估值
        result = engine.value_stock(
            symbol,
            date=date,
            methods=model_names,
            combine_method=combine_method
        )

        # 检查是否有错误
        if 'error' in result:
            return {
                'success': False,
                'error': result['error']
            }, 404

        # 格式化响应
        return {
            'success': True,
            'symbol': symbol,
            'valuation': _format_valuation_result(result)
        }, 200

    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Valuation failed for {symbol}: {e}")
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }, 500


def batch_valuation(
    symbols: str,
    methods: Optional[str] = None,
    date: Optional[str] = None,
    fiscal_date: Optional[str] = None,
    combine_method: str = 'weighted',
    dcf_config: Optional[Dict[str, Any]] = None
) -> tuple[Dict[str, Any], int]:
    """
    批量获取股票估值

    Args:
        symbols: 股票代码，逗号分隔
        methods: 估值方法，逗号分隔
        date: 估值日期/股价日期
        fiscal_date: 财务数据报告期（可选）
        combine_method: 组合方式
        dcf_config: DCF估值配置参数（可选）

    Returns:
        (响应数据, HTTP状态码)
    """
    try:
        from src.valuation.engine.valuation_engine import ValuationEngine

        # 初始化估值引擎
        engine = ValuationEngine()

        # 设置财务数据日期（如果指定），并进行时间验证
        # 注意：必须在注册模型之前设置，这样每个模型的数据加载器都会使用正确的fiscal_date
        if fiscal_date:
            engine.set_fiscal_date(fiscal_date, valuation_date=date)

        # 解析估值方法
        if methods:
            method_list = [m.strip().lower() for m in methods.split(',')]
        else:
            method_list = ['pe', 'pb', 'ps', 'peg']  # 默认使用相对估值

        # 根据请求的方法注册相应的模型
        from src.valuation.models.relative_valuation import RelativeValuationModel

        # 准备配置参数（包括fiscal_date）
        base_config = {}
        if fiscal_date:
            base_config['fiscal_date'] = fiscal_date

        # 注册相对估值模型
        for method in ['pe', 'pb', 'ps', 'peg']:
            if method in method_list or 'combined' in method_list:
                model = RelativeValuationModel(method=method, config=base_config if base_config else None)
                engine.register_model(model)

        # 注册DCF模型（如果请求了）
        if 'dcf' in method_list:
            try:
                from src.valuation.models.absolute_valuation import DCFValuationModel
                # 合并配置参数
                config = {**base_config, **(dcf_config or {})}  # 合并fiscal_date和dcf_config
                # 传递 DCF 配置参数
                dcf_model = DCFValuationModel(config=config if config else None)
                engine.register_model(dcf_model)
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"Failed to register DCF model: {e}")

        # 转换方法名到模型名
        model_names = []
        for m in method_list:
            if m == 'combined':
                # combined 表示 PE/PB/PS 三种相对估值方法
                model_names.extend(['Relative_PE', 'Relative_PB', 'Relative_PS'])
            elif m == 'peg':
                model_names.append('Relative_PEG')
            elif m == 'dcf':
                model_names.append('DCF')
            else:
                model_names.append(f'Relative_{m.upper()}')

        # 解析股票代码
        symbol_list = [s.strip() for s in symbols.split(',')]

        # 执行批量估值
        results = engine.batch_value_stocks(
            symbol_list,
            date=date,
            methods=model_names,
            combine_method=combine_method
        )

        # 格式化响应
        valuations = []
        for result in results:
            if 'error' not in result:
                valuations.append(_format_valuation_result(result))
            else:
                valuations.append({
                    'symbol': result.get('symbol'),
                    'error': result.get('error')
                })

        return {
            'success': True,
            'valuations': valuations
        }, 200

    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Batch valuation failed: {e}")
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }, 500


def compare_valuation(
    symbols: str,
    method: Optional[str] = None,
    date: Optional[str] = None
) -> tuple[Dict[str, Any], int]:
    """
    对比多只股票的估值

    Args:
        symbols: 股票代码，逗号分隔
        method: 估值方法
        date: 估值日期

    Returns:
        (响应数据, HTTP状态码)
    """
    try:
        from src.valuation.engine.valuation_engine import ValuationEngine

        # 初始化估值引擎
        engine = ValuationEngine()

        # 注册模型
        from src.valuation.models.relative_valuation import RelativeValuationModel

        for m in ['pe', 'pb', 'ps', 'peg']:
            model = RelativeValuationModel(method=m)
            engine.register_model(model)

        combined_model = RelativeValuationModel(method='combined')
        engine.register_model(combined_model)

        # 解析股票代码
        symbol_list = [s.strip() for s in symbols.split(',')]

        # 执行对比估值
        result = engine.compare_stocks(
            symbol_list,
            date=date,
            method=method
        )

        # 格式化响应
        return {
            'success': True,
            'comparison': {
                'date': result.get('date'),
                'method': result.get('method'),
                'summary': result.get('summary'),
                'stocks': [_format_valuation_result(r) if 'error' not in r else r
                          for r in result.get('stocks', [])]
            }
        }, 200

    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Compare valuation failed: {e}")
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }, 500


def list_models() -> tuple[Dict[str, Any], int]:
    """
    列出所有可用的估值模型

    Returns:
        (响应数据, HTTP状态码)
    """
    try:
        from src.valuation.engine.valuation_engine import ValuationEngine

        engine = ValuationEngine()

        # 注册相对估值模型
        from src.valuation.models.relative_valuation import RelativeValuationModel

        for method in ['pe', 'pb', 'ps', 'peg']:
            model = RelativeValuationModel(method=method)
            engine.register_model(model)

        # 注册combined模型
        combined_model = RelativeValuationModel(method='combined')
        engine.register_model(combined_model)

        # 注册DCF模型（可选）
        try:
            from src.valuation.models.absolute_valuation import DCFValuationModel
            dcf_model = DCFValuationModel()
            engine.register_model(dcf_model)
        except Exception:
            # DCF模型可能因为数据不足而注册失败，忽略
            pass

        return {
            'success': True,
            'models': engine.list_models()
        }, 200

    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Failed to list models: {e}")
        return {
            'success': False,
            'error': str(e)
        }, 500


def _sanitize_nan(obj):
    """
    递归地将 NaN/Infinity 值替换为 None（JSON null），以确保合法的 JSON 输出。
    标准 JSON 不支持 NaN 和 Infinity，浏览器无法解析含有这些值的响应。
    """
    import math
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, dict):
        return {k: _sanitize_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_sanitize_nan(item) for item in obj]
    return obj


def _format_valuation_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    格式化估值结果

    Args:
        result: 原始估值结果

    Returns:
        格式化后的结果
    """
    formatted = {
        'symbol': result.get('symbol'),
        'date': result.get('date'),
        'model': result.get('model'),
        'fair_value': result.get('fair_value'),
        'current_price': result.get('current_price'),
        'upside_downside': result.get('upside_downside'),
        'rating': result.get('rating'),
        'confidence': result.get('confidence'),
        'metrics': result.get('metrics', {}),
        'assumptions': result.get('assumptions', {}),
        'warnings': result.get('warnings', [])
    }

    # 如果有组合结果，添加各个模型的结果
    if 'individual_results' in result:
        formatted['individual_results'] = [
            _format_valuation_result(r) for r in result['individual_results']
        ]

    # 清理 NaN/Infinity 值，确保合法的 JSON 输出
    return _sanitize_nan(formatted)


def get_dcf_preset_config(risk_profile: str) -> Dict[str, Any]:
    """
    获取DCF估值预设配置

    Args:
        risk_profile: 风险偏好类型
            - 'conservative': 保守型（高折现率、低增长假设）
            - 'balanced': 平衡型（中等参数）
            - 'aggressive': 积极型（低折现率、高增长假设）

    Returns:
        DCF配置字典
    """
    presets = {
        'conservative': {
            'forecast_years': 5,
            'terminal_growth': 0.01,  # 1%终值增长
            'risk_free_rate': 0.03,   # 3%无风险利率
            'market_return': 0.10,    # 10%市场回报
            'credit_spread': 0.035,   # 3.5%信用利差
            'growth_rate_cap': 0.04,  # 4%增长率上限
            'wacc_min': 0.09,        # 9% WACC下限
            'wacc_max': 0.14,        # 14% WACC上限
        },
        'balanced': {
            'forecast_years': 5,
            'terminal_growth': 0.02,  # 2%终值增长
            'risk_free_rate': 0.025,  # 2.5%无风险利率
            'market_return': 0.09,    # 9%市场回报
            'credit_spread': 0.025,   # 2.5%信用利差
            'growth_rate_cap': 0.06,  # 6%增长率上限
            'wacc_min': 0.07,        # 7% WACC下限
            'wacc_max': 0.12,        # 12% WACC上限
        },
        'aggressive': {
            'forecast_years': 5,
            'terminal_growth': 0.025, # 2.5%终值增长
            'risk_free_rate': 0.02,   # 2%无风险利率
            'market_return': 0.08,    # 8%市场回报
            'credit_spread': 0.02,    # 2%信用利差
            'growth_rate_cap': 0.08,  # 8%增长率上限
            'wacc_min': 0.06,        # 6% WACC下限
            'wacc_max': 0.11,        # 11% WACC上限
        },
    }

    return presets.get(risk_profile, presets['balanced'])


def get_dcf_config_presets() -> tuple[Dict[str, Any], int]:
    """
    获取所有DCF预设配置

    Returns:
        (响应数据, HTTP状态码)
    """
    try:
        presets = {
            'conservative': {
                'name': '保守型',
                'description': '适用于不确定性高、风险厌恶的投资场景',
                '适用场景': [
                    '周期性公司',
                    '高负债公司',
                    '新兴市场公司',
                    '业绩波动大'
                ],
                '参数建议': get_dcf_preset_config('conservative'),
                '预期结果': '估值偏低，安全边际高'
            },
            'balanced': {
                'name': '平衡型',
                'description': '适用于大多数成熟稳定公司',
                '适用场景': [
                    '行业龙头',
                    '业绩稳定',
                    '适度增长',
                    '中国市场主流'
                ],
                '参数建议': get_dcf_preset_config('balanced'),
                '预期结果': '估值适中，符合市场共识'
            },
            'aggressive': {
                'name': '积极型',
                'description': '适用于确定性高、有护城河的优质公司',
                '适用场景': [
                    '强势品牌（如茅台）',
                    '垄断性业务',
                    '持续高ROE',
                    '长期增长确定性'
                ],
                '参数建议': get_dcf_preset_config('aggressive'),
                '预期结果': '估值偏高，反映成长性和品牌溢价'
            }
        }

        return {
            'success': True,
            'presets': presets,
            '推荐': '平衡型适用于大多数场景，特殊公司可根据特征选择'
        }, 200

    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Failed to get DCF presets: {e}")
        return {
            'success': False,
            'error': str(e)
        }, 500
