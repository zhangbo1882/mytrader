"""
估值引擎

统一调度多个估值模型，支持模型组合和结果聚合
"""

import sys
import os
from typing import Dict, List, Optional, Any, Union
import numpy as np

# 添加项目路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.valuation.models.base_valuation_model import BaseValuationModel


class ValuationEngine:
    """
    估值引擎

    统一调度多个估值模型，支持：
    - 单模型估值
    - 多模型组合估值
    - 批量估值
    - 模型结果聚合
    """

    def __init__(self, db_path: str = "data/tushare_data.db"):
        """
        初始化估值引擎

        Args:
            db_path: 数据库路径
        """
        self.db_path = db_path
        self.models: Dict[str, BaseValuationModel] = {}
        self.fiscal_date = None  # 财务数据报告期
        self._init_models()

    def _init_models(self):
        """
        初始化所有估值模型

        子类可以覆盖此方法来添加自定义模型
        """
        # 模型将在子模块实现后注册到这里
        pass

    def set_fiscal_date(self, fiscal_date: str, valuation_date: Optional[str] = None):
        """
        设置财务数据报告期，并传递给所有已注册的模型

        Args:
            fiscal_date: 财务数据报告期 (YYYY-MM-DD 或 YYYYMMDD)
            valuation_date: 估值日期 (用于时间验证)
        """
        import warnings
        from datetime import datetime

        # 标准化日期格式
        def normalize_date(d):
            if not d:
                return None
            d = str(d).replace('-', '')
            if len(d) == 8:
                return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            return d

        self.fiscal_date = normalize_date(fiscal_date)

        # 时间验证
        if valuation_date:
            valuation_date_norm = normalize_date(valuation_date)
            if self.fiscal_date and valuation_date_norm:
                try:
                    fiscal_dt = datetime.strptime(self.fiscal_date, '%Y-%m-%d')
                    valuation_dt = datetime.strptime(valuation_date_norm, '%Y-%m-%d')

                    if fiscal_dt > valuation_dt:
                        warnings.warn(
                            f"⚠️  时间穿越警告: 财务数据报告期 ({self.fiscal_date}) "
                            f"晚于估值日期 ({valuation_date_norm})。"
                            f"这是不合理的，因为估值时无法获得未来的财报数据。",
                            UserWarning
                        )
                except ValueError as e:
                    print(f"Date parsing error: {e}")

        # 传递给所有已注册的模型
        for model in self.models.values():
            if hasattr(model, 'data_loader') and hasattr(model.data_loader, 'set_fiscal_date'):
                model.data_loader.set_fiscal_date(self.fiscal_date, valuation_date)

    def register_model(self, model: BaseValuationModel):
        """
        注册估值模型

        Args:
            model: 估值模型实例
        """
        self.models[model.name] = model

    def value_stock(
        self,
        symbol: str,
        date: Optional[str] = None,
        methods: Optional[List[str]] = None,
        combine_method: str = 'weighted',
        **kwargs
    ) -> Dict[str, Any]:
        """
        对单只股票进行估值

        Args:
            symbol: 股票代码
            date: 估值日期
            methods: 估值方法列表，None表示使用所有模型
            combine_method: 结果组合方式 ('weighted', 'average', 'median', 'max_confidence')
            **kwargs: 额外参数

        Returns:
            估值结果字典
        """
        # 确定使用的模型
        if methods is None:
            models_to_use = list(self.models.values())
        else:
            models_to_use = [self.models[m] for m in methods if m in self.models]

        if not models_to_use:
            return {
                'symbol': symbol,
                'date': date,
                'error': 'No valuation models available'
            }

        # 单个模型直接返回结果
        if len(models_to_use) == 1:
            return models_to_use[0].calculate(symbol, date, **kwargs)

        # 多模型：分别计算后组合
        results = []
        for model in models_to_use:
            try:
                result = model.calculate(symbol, date, **kwargs)
                results.append(result)
            except Exception as e:
                # 记录错误但继续处理其他模型
                results.append({
                    'model': model.name,
                    'symbol': symbol,
                    'error': str(e)
                })

        # 组合结果
        combined_result = self._combine_results(
            results,
            method=combine_method
        )

        return combined_result

    def batch_value_stocks(
        self,
        symbols: List[str],
        date: Optional[str] = None,
        methods: Optional[List[str]] = None,
        combine_method: str = 'weighted',
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        批量对多只股票进行估值

        Args:
            symbols: 股票代码列表
            date: 估值日期
            methods: 估值方法列表
            combine_method: 结果组合方式
            **kwargs: 额外参数

        Returns:
            估值结果列表
        """
        results = []
        for symbol in symbols:
            result = self.value_stock(
                symbol, date, methods, combine_method, **kwargs
            )
            results.append(result)

        return results

    def compare_stocks(
        self,
        symbols: List[str],
        date: Optional[str] = None,
        method: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        对比多只股票的估值结果

        Args:
            symbols: 股票代码列表
            date: 估值日期
            method: 估值方法，None表示使用默认方法
            **kwargs: 额外参数

        Returns:
            对比结果
        """
        results = []
        for symbol in symbols:
            if method:
                model = self.models.get(method)
                if model:
                    try:
                        result = model.calculate(symbol, date, **kwargs)
                        results.append(result)
                    except Exception as e:
                        results.append({
                            'symbol': symbol,
                            'error': str(e)
                        })
            else:
                # 使用所有可用模型
                result = self.value_stock(symbol, date, **kwargs)
                results.append(result)

        # 排序：按涨跌幅空间降序
        sorted_results = sorted(
            results,
            key=lambda x: x.get('upside_downside', 0),
            reverse=True
        )

        return {
            'date': date,
            'method': method,
            'stocks': sorted_results,
            'summary': self._generate_comparison_summary(sorted_results)
        }

    def _combine_results(
        self,
        results: List[Dict[str, Any]],
        method: str = 'weighted'
    ) -> Dict[str, Any]:
        """
        组合多个估值模型的结果

        Args:
            results: 多个模型的结果列表
            method: 组合方式

        Returns:
            组合后的估值结果
        """
        # 过滤掉有错误的结果
        valid_results = [r for r in results if 'error' not in r]

        if not valid_results:
            return results[0] if results else {'error': 'All models failed'}

        if len(valid_results) == 1:
            return valid_results[0]

        # 提取关键数据
        symbol = valid_results[0]['symbol']
        date = valid_results[0]['date']
        current_price = valid_results[0]['current_price']

        # 计算公允价值
        fair_values = [r['fair_value'] for r in valid_results]
        confidences = [r.get('confidence', 0.5) for r in valid_results]

        if method == 'weighted':
            # 加权平均（基于置信度）
            total_confidence = sum(confidences)
            if total_confidence > 0:
                weights = [c / total_confidence for c in confidences]
                combined_fair_value = sum(fv * w for fv, w in zip(fair_values, weights))
            else:
                combined_fair_value = np.mean(fair_values)

        elif method == 'average':
            # 简单平均
            combined_fair_value = np.mean(fair_values)

        elif method == 'median':
            # 中位数
            combined_fair_value = np.median(fair_values)

        elif method == 'max_confidence':
            # 使用置信度最高的模型结果
            max_idx = np.argmax(confidences)
            combined_fair_value = fair_values[max_idx]

        else:
            # 默认使用加权平均
            total_confidence = sum(confidences)
            if total_confidence > 0:
                weights = [c / total_confidence for c in confidences]
                combined_fair_value = sum(fv * w for fv, w in zip(fair_values, weights))
            else:
                combined_fair_value = np.mean(fair_values)

        # 计算涨跌幅空间
        if current_price > 0:
            upside_downside = (combined_fair_value - current_price) / current_price * 100
        else:
            upside_downside = 0

        # 组合置信度（平均）
        combined_confidence = np.mean(confidences)

        # 计算评级
        rating = self._calculate_rating(upside_downside, combined_confidence)

        # 聚合详细指标
        all_metrics = {}
        for r in valid_results:
            model_name = r['model']
            if 'metrics' in r:
                all_metrics[model_name] = r['metrics']

        # 聚合假设
        all_assumptions = {}
        for r in valid_results:
            model_name = r['model']
            if 'assumptions' in r:
                all_assumptions[model_name] = r['assumptions']

        # 收集警告
        all_warnings = []
        for r in valid_results:
            if 'warnings' in r:
                all_warnings.extend(r['warnings'])

        return {
            'symbol': symbol,
            'date': date,
            'model': f'Combined_{method}',
            'fair_value': round(combined_fair_value, 2),
            'current_price': round(current_price, 2),
            'upside_downside': round(upside_downside, 2),
            'rating': rating,
            'confidence': round(combined_confidence, 3),
            'metrics': all_metrics,
            'assumptions': all_assumptions,
            'warnings': all_warnings,
            'individual_results': valid_results,
            'combination_method': method
        }

    def _calculate_rating(self, upside_downside: float, confidence: float) -> str:
        """
        计算评级

        Args:
            upside_downside: 涨跌幅空间
            confidence: 置信度

        Returns:
            评级
        """
        if confidence < 0.3:
            return '观望'

        if upside_downside > 30:
            return '强烈买入'
        elif upside_downside > 15:
            return '买入'
        elif upside_downside > -15:
            return '持有'
        elif upside_downside > -30:
            return '卖出'
        else:
            return '强烈卖出'

    def _generate_comparison_summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        生成对比摘要

        Args:
            results: 估值结果列表

        Returns:
            摘要字典
        """
        valid_results = [r for r in results if 'error' not in r]

        if not valid_results:
            return {}

        upside_downsides = [r.get('upside_downside', 0) for r in valid_results]

        return {
            'total_stocks': len(results),
            'valid_stocks': len(valid_results),
            'avg_upside': round(np.mean(upside_downsides), 2),
            'max_upside': round(np.max(upside_downsides), 2),
            'min_upside': round(np.min(upside_downsides), 2),
            'buy_count': sum(1 for r in valid_results if '买入' in r.get('rating', '')),
            'hold_count': sum(1 for r in valid_results if r.get('rating') == '持有'),
            'sell_count': sum(1 for r in valid_results if '卖出' in r.get('rating', ''))
        }

    def list_models(self) -> List[str]:
        """
        列出所有可用的估值模型

        Returns:
            模型名称列表
        """
        return list(self.models.keys())

    def get_model_info(self, model_name: str) -> Optional[Dict[str, Any]]:
        """
        获取指定模型的信息

        Args:
            model_name: 模型名称

        Returns:
            模型信息字典
        """
        if model_name in self.models:
            return self.models[model_name].get_model_info()
        return None
