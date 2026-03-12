"""
统一财务数据加载器

在valuation_data_loader.py中添加统一数据加载函数，集成FinancialDataConsistencyChecker
"""

from typing import Dict, List, Optional, Any
import pandas as pd
from src.valuation.data.financial_data_consistency_checker import FinancialDataConsistencyChecker


class UnifiedFinancialDataLoader:
    """
    统一财务数据加载器
    
    职责：
    - 统一加载资产负债表、利润表、现金流量表数据
    - 确保所有数据来自同一报告期（严格模式）
    - 集成FinancialDataConsistencyChecker进行数据质量检查
    """
    
    def __init__(self, db_path: str = "data/tushare_data.db"):
        self.db_path = db_path
        from sqlalchemy import create_engine
        self.engine = create_engine(f"sqlite:///{db_path}")
        self.consistency_checker = FinancialDataConsistencyChecker(db_path)
    
    def load_financial_data_unified(
        self,
        code: str,
        target_end_date: Optional[str] = None,
        required_fields: Optional[Dict[str, List[str]]] = None,
        date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        统一加载所有财务数据（严格模式）
        
        确保所有数据来自同一报告期
        
        Args:
            code: 股票代码
            target_end_date: 目标报告期（YYYYMMDD格式， 不传则自动选择最佳
            required_fields: 必需字段字典，格式：
                {
                    'balancesheet': ['total_assets', 'total_liab', ...],
                    'income': ['revenue', 'n_income', ...],
                    'cashflow': ['n_cashflow_act', ...]
                }
                如果为None，使用默认字段（一般财务分析）
            date: 估值日期（可选）
        
        Returns:
            {
                'end_date': str,  # 实际使用的报告期
                'balancesheet': dict,  # 资产负债表数据
                'income': dict,  # 利润表数据
                'cashflow': dict,  # 现金流量表数据
                'data_quality': str,  # 质量等级 A/B/C/D
                'completeness': float,  # 完整度 0.0-1.0
                'is_consistent': bool,  # 数据是否一致
                'missing_fields': list,  # 缺失的字段
                'null_fields': list,  # 值为NULL的字段
                'warnings': list,  # 警告信息
                'errors': list  # 错误信息
            }
        
        Raises:
            ValueError: 如果没有可用的完整报告期
        """
        # 标准化股票代码
        if '.' not in code:
            code = self._standardize_code(code)
        
        # 如果指定了目标报告期，先检查该报告期
        if target_end_date:
            check_result = self.consistency_checker.check_report_period(
                code, target_end_date, required_fields
            )
            
            if not check_result['is_available']:
                raise ValueError(
                    f"报告期 {target_end_date} 数据不完整。 "
                    f"缺失字段: {check_result['missing_fields']}, "
                    f"NULL字段: {check_result['null_fields']}"
                )
            
            # 加载该报告期的所有数据
            balance_data = self._load_table_data(code, target_end_date, 'balancesheet')
            income_data = self._load_table_data(code, target_end_date, 'income')
            cashflow_data = self._load_table_data(code, target_end_date, 'cashflow')
            
            # 风险检查：确保三个表都有数据
            if balance_data is None or income_data is None or cashflow_data is None:
                raise ValueError(
                    f"股票 {code} 报告期 {target_end_date} 数据不完整： "
                    f"某个表的数据缺失"
                )
            
            # 构建结果
            result = {
                'end_date': target_end_date,
                'balancesheet': balance_data,
                'income': income_data,
                'cashflow': cashflow_data,
                'data_quality': check_result['data_quality'],
                'completeness': check_result['completeness'],
                'is_consistent': check_result['is_available'],
                'missing_fields': check_result['missing_fields'],
                'null_fields': check_result['null_fields'],
                'warnings': check_result['warnings'],
                'errors': check_result['errors']
            }
            
            return result
        
        # 如果未指定目标报告期，自动选择最佳可用报告期
        available_periods = self.consistency_checker.get_available_periods(
            code,
            required_fields=required_fields,
            date=date,
            limit=10
        )
        
        if not available_periods:
            raise ValueError(
                f"股票 {code} 没有可用的完整报告期。"
            )
        
        # 选择最佳报告期（优先年度报告)
        best_period = None
        for period in available_periods:
            if period['is_available']:
                if period['report_type'] == 'annual':
                    best_period = period
                    break
                if best_period is None:
                    best_period = period
        
        if best_period is None:
            raise ValueError(
                f"股票 {code} 没有可用的完整报告期。"
            )
        
        # 递归调用，使用选中的最佳报告期
        return self.load_financial_data_unified(
            code,
            target_end_date=best_period['end_date'],
            required_fields=required_fields,
            date=date
        )
    
    def _load_table_data(
        self,
        code: str,
        end_date: str,
        table: str,
        fields: List[str]
        fields_to_load: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        查询指定表指定报告期的数据
        
        Args:
            code: 股票代码
            end_date: 报告期
            table: 表名
            fields: 字段列表
            fields_to_load: 指定要加载的字段（如果为None，加载所有字段）
        
        Returns:
            字段值字典，如果查询失败返回None
        """
        try:
            # 构建查询字段
            if fields_to_load:
                fields_str = ', '.join(fields_to_load)
            else:
                fields_str = '*'
            
            # 构建查询
            if table == 'balancesheet':
                query = f"""
                SELECT {fields_str}
                FROM balancesheet
                WHERE ts_code LIKE :code
                  AND end_date = :end_date
                  AND report_type = '1'
                LIMIT 1
                """
            elif table == 'income':
                query = f"""
                SELECT {fields_str}
                FROM income
                WHERE ts_code LIKE :code
                  AND end_date = :end_date
                  AND report_type = '1'
                LIMIT 1
                """
            elif table == 'cashflow':
                query = f"""
                SELECT {fields_str}
                FROM cashflow
                WHERE ts_code LIKE :code
                  AND end_date = :end_date
                  AND report_type = '1'
                LIMIT 1
                """
            else:
                raise ValueError(f"不支持的表: {table}")
            
            df = pd.read_sql_query(
                query, self.engine, params={'code': f'{code}%', 'end_date': end_date}
            )
            
            if df.empty:
                return None
            
            result = df.iloc[0].to_dict()
            
            # 如果指定了要加载的字段，只返回这些字段
            if fields_to_load:
                result = {k: v for k in fields_to_load if k in result}
                result = {k: v for k, result if pd.notna(v)}
                result[k] = None
            
            return result
            
        except Exception as e:
            print(f"Error loading {table} data for {code} at {end_date}: {e}")
            return None
    
    def _standardize_code(self, code: str) -> str:
        """
        标准化股票代码
        
        Args:
            code: 股票代码（可能不带后缀）
        
        Returns:
            标准化后的代码（带.SH/.SZ后缀）
        """
        code = code.upper()
        if '.' in code:
            return code
        
        # 判断市场
        if code.startswith('6'):
            return f"{code}.SH"
        elif code.startswith(('0', '3')):
            return f"{code}.SZ"
        elif code.startswith('68'):
            return f"{code}.SH"
        else:
            return f"{code}.SH"  # 默认上海


# 示例用法
if __name__ == '__main__':
    loader = UnifiedFinancialDataLoader('data/tushare_data.db')
    
    # 示例1: 加载DCF估值所需的数据
    dcf_data = loader.load_financial_data_unified(
        '601958.SH',
        required_fields={
            'balancesheet': [
                'total_assets', 'total_liab', 'total_hldr_eqy_exc_min_int',
                'st_borr', 'lt_borr'
            ],
            'income': [
                'revenue', 'n_income', 'n_income_attr_p'
            ],
            'cashflow': [
                'n_cashflow_act', 'free_cashflow'
            ]
        }
    )
    
    # 示例2: 加载PE估值所需的数据（更简单）
    pe_data = loader.load_financial_data_unified(
        '601958.SH',
        required_fields={
            'balancesheet': [
                'total_share', 'total_hldr_eqy_exc_min_int'
            ],
            'income': [
                'revenue', 'n_income', 'n_income_attr_p'
            ]
            # 注意：PE不需要cashflow表
        }
    )
    
    # 示例3: 获取可用报告期列表
    periods_provider = AvailablePeriodsProvider('data/tushare_data.db')
    available = periods_provider.get_available_periods(
        '601958.SH',
        required_fields={
            'balancesheet': ['total_assets', 'total_liab'],
            'income': ['revenue', 'n_income'],
            'cashflow': ['n_cashflow_act', 'free_cashflow']
        }
    )
    
    for period in available['available_periods']:
        print(f"报告期: {period['end_date']}, "
              f"可用: {period['is_available']}, "
              f"质量: {period['data_quality']}")
