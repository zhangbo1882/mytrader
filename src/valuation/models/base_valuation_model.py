"""
估值模型基类

定义所有估值模型的统一接口和标准返回格式
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import numpy as np


class BaseValuationModel(ABC):
    """
    估值模型抽象基类

    所有估值模型必须继承此类并实现核心方法
    """

    def __init__(self, name: str, config: Optional[Dict] = None):
        """
        初始化估值模型

        Args:
            name: 模型名称
            config: 模型配置参数
        """
        self.name = name
        self.config = config or {}

    @abstractmethod
    def calculate(
        self,
        symbol: str,
        date: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        计算股票估值

        Args:
            symbol: 股票代码
            date: 估值日期 (YYYY-MM-DD 或 YYYYMMDD), None表示最新
            **kwargs: 额外参数

        Returns:
            标准估值结果字典:
            {
                'symbol': str,              # 股票代码
                'date': str,                # 估值日期
                'model': str,               # 估值模型名称
                'fair_value': float,        # 公允价值
                'current_price': float,     # 当前价格
                'upside_downside': float,   # 涨跌幅空间 (%)
                'rating': str,              # 评级: '买入'/'持有'/'卖出'
                'confidence': float,        # 置信度 0-1
                'metrics': Dict,            # 详细指标
                'assumptions': Dict,        # 关键假设
                'warnings': List[str]       # 警告信息
            }
        """
        pass

    @abstractmethod
    def get_required_data(self, symbol: str, date: Optional[str] = None) -> Dict[str, Any]:
        """
        定义估值所需的数据

        Args:
            symbol: 股票代码
            date: 估值日期

        Returns:
            所需数据类型的字典描述
        """
        pass

    def batch_calculate(
        self,
        symbols: List[str],
        date: Optional[str] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        批量计算估值

        Args:
            symbols: 股票代码列表
            date: 估值日期
            **kwargs: 额外参数

        Returns:
            估值结果列表
        """
        results = []
        for symbol in symbols:
            try:
                result = self.calculate(symbol, date, **kwargs)
                results.append(result)
            except Exception as e:
                # 记录错误但继续处理其他股票
                results.append({
                    'symbol': symbol,
                    'date': date,
                    'model': self.name,
                    'error': str(e)
                })
        return results

    def compare_with_industry(
        self,
        symbol: str,
        date: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        与同行业公司对比估值

        Args:
            symbol: 股票代码
            date: 估值日期
            **kwargs: 额外参数

        Returns:
            行业对比结果
        """
        # 默认实现，子类可覆盖
        return {
            'symbol': symbol,
            'date': date,
            'note': 'Industry comparison not implemented for this model'
        }

    def _calculate_rating(
        self,
        upside_downside: float,
        confidence: float
    ) -> str:
        """
        根据涨跌幅空间和置信度计算评级

        Args:
            upside_downside: 涨跌幅空间 (%)
            confidence: 置信度

        Returns:
            评级: '买入'/'持有'/'卖出'
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

    def _standardize_result(
        self,
        symbol: str,
        date: str,
        fair_value: float,
        current_price: float,
        metrics: Dict,
        assumptions: Dict,
        warnings: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        标准化估值结果格式

        Args:
            symbol: 股票代码
            date: 估值日期
            fair_value: 公允价值
            current_price: 当前价格
            metrics: 详细指标
            assumptions: 关键假设
            warnings: 警告信息

        Returns:
            标准格式的估值结果
        """
        # 计算涨跌幅空间
        if current_price > 0:
            upside_downside = (fair_value - current_price) / current_price * 100
        else:
            upside_downside = 0

        # 计算置信度 (子类可以覆盖此方法)
        confidence = self._calculate_confidence(metrics, assumptions)

        # 计算评级
        rating = self._calculate_rating(upside_downside, confidence)

        return {
            'symbol': symbol,
            'date': date,
            'model': self.name,
            'fair_value': round(fair_value, 2),
            'current_price': round(current_price, 2),
            'upside_downside': round(upside_downside, 2),
            'rating': rating,
            'confidence': round(confidence, 3),
            'metrics': metrics,
            'assumptions': assumptions,
            'warnings': warnings or []
        }

    def _calculate_confidence(
        self,
        metrics: Dict,
        assumptions: Dict
    ) -> float:
        """
        计算估值置信度

        子类可以覆盖此方法实现特定的置信度计算逻辑

        Args:
            metrics: 详细指标
            assumptions: 关键假设

        Returns:
            置信度 0-1
        """
        # 默认实现，子类应覆盖
        return 0.5

    def _format_date(self, date: Optional[str]) -> str:
        """
        格式化日期

        Args:
            date: 日期字符串

        Returns:
            格式化后的日期 (YYYY-MM-DD)
        """
        if date is None:
            return datetime.now().strftime('%Y-%m-%d')

        # 移除分隔符并标准化
        date_clean = date.replace('-', '').replace('/', '')

        if len(date_clean) == 8:
            return f'{date_clean[:4]}-{date_clean[4:6]}-{date_clean[6:8]}'
        else:
            return date

    def get_model_info(self) -> Dict[str, Any]:
        """
        获取模型信息

        Returns:
            模型信息字典
        """
        return {
            'name': self.name,
            'config': self.config,
            'description': self.__doc__ or 'No description available'
        }

    def validate_input(self, symbol: str, date: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        """
        验证输入参数

        Args:
            symbol: 股票代码
            date: 估值日期

        Returns:
            (是否有效, 错误信息)
        """
        if not symbol:
            return False, 'Symbol cannot be empty'

        # 标准化股票代码
        symbol_clean = symbol.replace('.SH', '').replace('.SZ', '')
        if not symbol_clean.isdigit() or len(symbol_clean) != 6:
            return False, f'Invalid symbol format: {symbol}'

        return True, None
