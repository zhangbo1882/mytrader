"""
Financial data business logic services
"""
from flask import jsonify
from src.utils.stock_lookup import get_stock_name_from_code
import pandas as pd
import numpy as np


def financial_summary(symbol):
    """获取股票财务摘要数据（最新期核心指标，包括财务指标和估值指标）"""
    try:
        from src.data_sources.query.financial_query import FinancialQuery
        fq = FinancialQuery('data/tushare_data.db')

        # 获取完整财务数据（包含财务指标）
        data = fq.query_all_financial(symbol, include_indicators=True)
        if not data or data['income'].empty:
            return {'success': False, 'error': '暂无财务数据'}, 404

        # 获取最新一期数据（合并报表，report_type=1）
        income_filtered = data['income'][data['income']['report_type'] == 1] if not data['income'].empty else pd.DataFrame()
        balance_filtered = data['balancesheet'][data['balancesheet']['report_type'] == 1] if not data['balancesheet'].empty else pd.DataFrame()
        cashflow_filtered = data['cashflow'][data['cashflow']['report_type'] == 1] if not data['cashflow'].empty else pd.DataFrame()

        # 处理财务指标数据（如果存在）
        indicator_df = data.get('fina_indicator', pd.DataFrame())
        indicator_filtered = indicator_df[indicator_df['report_type'] == 1] if not indicator_df.empty else pd.DataFrame()
        indicator = indicator_filtered.iloc[0] if not indicator_filtered.empty else None

        income = income_filtered.iloc[0] if not income_filtered.empty else None
        balance = balance_filtered.iloc[0] if not balance_filtered.empty else None
        cashflow = cashflow_filtered.iloc[0] if not cashflow_filtered.empty else None

        # 构建扁平化的摘要数据
        summary = {}

        # 基本信息
        if income is not None:
            summary['end_date'] = income.get('end_date')
            summary['ann_date'] = income.get('ann_date')

        # 利润指标
        if income is not None:
            summary['total_operate_revenue'] = income.get('total_revenue') or income.get('revenue')
            summary['operate_profit'] = income.get('operate_profit')
            summary['net_profit'] = income.get('n_income')
            summary['basic_eps'] = income.get('basic_eps')

        # 资产负债
        if balance is not None:
            summary['total_assets'] = balance.get('total_assets')
            summary['total_liability'] = balance.get('total_liab')
            summary['total_hldr_eqy_exc_min_int'] = balance.get('total_hldr_eqy_exc_min_int')

        # 现金流
        if cashflow is not None:
            summary['n_cashflow_act'] = cashflow.get('n_cashflow_act')
            summary['free_cashflow'] = cashflow.get('free_cashflow')
            summary['sales_cash'] = cashflow.get('c_fr_sale_sg')  # 销售收现

        # 财务指标（8个核心指标）
        if indicator is not None:
            # 盈利能力
            summary['roe'] = indicator.get('roe')  # 净资产收益率
            summary['roa'] = indicator.get('roa')  # 总资产报酬率
            summary['netprofit_margin'] = indicator.get('netprofit_margin')  # 销售净利率
            summary['grossprofit_margin'] = indicator.get('grossprofit_margin')  # 销售毛利率

            # 成长能力
            summary['or_yoy'] = indicator.get('or_yoy')  # 营业收入同比增长率
            summary['netprofit_yoy'] = indicator.get('netprofit_yoy')  # 净利润同比增长率

            # 偿债能力
            summary['current_ratio'] = indicator.get('current_ratio')  # 流动比率

            # 营运能力
            summary['assets_turn'] = indicator.get('assets_turn')  # 总资产周转率

        # 获取估值指标（PE、PB、市值等）
        try:
            code = fq._standardize_code(symbol)
            valuation_query = """
            SELECT datetime, close, pe, pe_ttm, pb, ps, ps_ttm, total_mv, circ_mv
            FROM bars
            WHERE symbol LIKE :symbol
            AND pe IS NOT NULL
            ORDER BY datetime DESC
            LIMIT 1
            """
            with fq.engine.connect() as conn:
                valuation_df = pd.read_sql_query(valuation_query, conn, params={'symbol': f'{code}%'})

            if not valuation_df.empty:
                valuation = valuation_df.iloc[0]
                summary['valuation_date'] = valuation.get('datetime')
                summary['close'] = valuation.get('close')
                summary['pe'] = valuation.get('pe')
                summary['pe_ttm'] = valuation.get('pe_ttm')
                summary['pb'] = valuation.get('pb')
                summary['ps'] = valuation.get('ps')
                summary['ps_ttm'] = valuation.get('ps_ttm')
                summary['total_mv'] = valuation.get('total_mv')
                summary['circ_mv'] = valuation.get('circ_mv')
        except Exception as e:
            # 估值指标获取失败不影响整体结果
            import logging
            logging.getLogger(__name__).warning(f"Failed to get valuation for {symbol}: {e}")

        # 清理NaN值
        for key, value in summary.items():
            if isinstance(value, float) and (np.isnan(value) or str(value) == 'nan'):
                summary[key] = None

        # 获取股票名称
        stock_name = get_stock_name_from_code(symbol)

        # 转换为前端期望的格式
        from datetime import datetime
        indicators = []

        # 定义指标映射（中文名称 -> 字段名 + 单位）
        indicator_map = {
            # 盈利能力
            '净资产收益率': ('roe', '%'),
            '总资产报酬率': ('roa', '%'),
            '销售净利率': ('netprofit_margin', '%'),
            '销售毛利率': ('grossprofit_margin', '%'),
            # 成长能力
            '营业收入增长率': ('or_yoy', '%'),
            '净利润增长率': ('netprofit_yoy', '%'),
            # 偿债能力
            '流动比率': ('current_ratio', ''),
            # 营运能力
            '总资产周转率': ('assets_turn', ''),
            # 每股指标
            '基本每股收益': ('basic_eps', '元'),
            # 利润表
            '营业总收入': ('total_operate_revenue', '元'),
            '营业利润': ('operate_profit', '元'),
            '净利润': ('net_profit', '元'),
            # 资产负债表
            '总资产': ('total_assets', '元'),
            '总负债': ('total_liability', '元'),
            '股东权益': ('total_hldr_eqy_exc_min_int', '元'),
            # 现金流
            '经营活动现金流': ('n_cashflow_act', '元'),
            '自由现金流': ('free_cashflow', '元'),
            '销售收现': ('sales_cash', '元'),
            # 估值指标
            '市盈率': ('pe', ''),
            '市盈率TTM': ('pe_ttm', ''),
            '市净率': ('pb', ''),
            '市销率': ('ps', ''),
            '市销率TTM': ('ps_ttm', ''),
            '总市值': ('total_mv', '万元'),
            '流通市值': ('circ_mv', '万元'),
            '收盘价': ('close', '元'),
        }

        # 构建indicators数组
        end_date = summary.get('end_date', '')
        ann_date = summary.get('ann_date', '')

        for name, (field, unit) in indicator_map.items():
            value = summary.get(field)
            if value is not None:
                # 格式化数值
                if unit == '元' or unit == '万元':
                    # 大数值用万/亿表示
                    if abs(value) >= 100000000:
                        display_value = f"{value / 100000000:.2f}亿"
                    elif abs(value) >= 10000:
                        display_value = f"{value / 10000:.2f}万"
                    else:
                        display_value = f"{value:.2f}"
                elif unit == '%':
                    display_value = f"{value * 100:.2f}%" if abs(value) < 1 else f"{value:.2f}%"
                else:
                    display_value = f"{value:.2f}" if isinstance(value, float) else str(value)

                indicators.append({
                    'key': field,
                    'name': name,
                    'value': display_value,
                    'unit': unit,
                    'date': ann_date
                })

        return {
            'stockCode': symbol,
            'stockName': stock_name or symbol,
            'updateTime': datetime.now().isoformat(),
            'indicators': indicators
        }, 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}, 500


def financial_report(symbol):
    """获取完整财务报表（前端专用）"""
    try:
        from src.data_sources.query.financial_query import FinancialQuery
        fq = FinancialQuery('data/tushare_data.db')

        data = fq.query_all_financial(symbol)
        if not data or data['income'].empty:
            return {'error': '暂无财务数据'}, 404

        # 字段名映射：中文项目名 -> 数据库字段
        INCOME_FIELDS = {
            '营业收入': 'total_revenue',
            '营业成本': 'oper_cost',
            '营业利润': 'operate_profit',
            '利润总额': 'total_profit',
            '净利润': 'n_income',
            '基本每股收益': 'basic_eps',
            '稀释每股收益': 'diluted_eps',
        }

        BALANCE_FIELDS = {
            '资产总计': 'total_assets',
            '负债合计': 'total_liab',
            '所有者权益合计': 'equity_total',
            '流动资产合计': 'total_cur_assets',
            '流动负债合计': 'total_cur_liab',
        }

        CASHFLOW_FIELDS = {
            '经营活动现金流量小计': 'n_cashflow_act',
            '投资活动现金流量小计': 'n_cash_flows_fnc_act',
            '筹资活动现金流量小计': 'n_cash_flows_fnc_act',
            '现金及现金等价物增加额': 'n_cash_incr',
        }

        # 单位映射
        INDICATOR_UNITS = {
            'roe': 'percent',
            'roa': 'percent',
            'netprofit_margin': 'percent',
            'grossprofit_margin': 'percent',
            'or_yoy': 'percent',
            'netprofit_yoy': 'percent',
            'current_ratio': 'number',
            'assets_turn': 'number',
        }

        # 转换DataFrame为列表，限制最近8条，并处理NaN值
        result = {
            'income': [],
            'balance': [],
            'cashflow': [],
            'indicators': []
        }

        # 处理利润表
        if not data['income'].empty:
            df_sorted = data['income'].sort_values('end_date', ascending=False).head(8)
            for _, row in df_sorted.iterrows():
                for name_cn, field in INCOME_FIELDS.items():
                    value = row.get(field)
                    if value is not None and pd.notna(value):
                        result['income'].append({
                            'item': name_cn,
                            'value': float(value),
                            'date': str(row.get('end_date', ''))
                        })

        # 处理资产负债表
        if not data.get('balancesheet', pd.DataFrame()).empty:
            df_sorted = data['balancesheet'].sort_values('end_date', ascending=False).head(8)
            for _, row in df_sorted.iterrows():
                for name_cn, field in BALANCE_FIELDS.items():
                    value = row.get(field)
                    if value is not None and pd.notna(value):
                        result['balance'].append({
                            'item': name_cn,
                            'value': float(value),
                            'date': str(row.get('end_date', ''))
                        })

        # 处理现金流量表
        if not data['cashflow'].empty:
            df_sorted = data['cashflow'].sort_values('end_date', ascending=False).head(8)
            for _, row in df_sorted.iterrows():
                for name_cn, field in CASHFLOW_FIELDS.items():
                    value = row.get(field)
                    if value is not None and pd.notna(value):
                        result['cashflow'].append({
                            'item': name_cn,
                            'value': float(value),
                            'date': str(row.get('end_date', ''))
                        })

        # 处理财务指标
        if 'fina_indicator' in data and not data['fina_indicator'].empty:
            df_sorted = data['fina_indicator'].sort_values('end_date', ascending=False).head(8)
            field_names = {
                'roe': '净资产收益率',
                'roa': '总资产报酬率',
                'netprofit_margin': '销售净利率',
                'grossprofit_margin': '销售毛利率',
                'or_yoy': '营业收入增长率',
                'netprofit_yoy': '净利润增长率',
                'current_ratio': '流动比率',
                'assets_turn': '总资产周转率',
            }
            for _, row in df_sorted.iterrows():
                for field, name_cn in field_names.items():
                    value = row.get(field)
                    if value is not None and pd.notna(value):
                        result['indicators'].append({
                            'item': name_cn,
                            'value': float(value),
                            'unit': INDICATOR_UNITS.get(field, 'number'),
                            'date': str(row.get('end_date', ''))
                        })

        return result, 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': str(e)}, 500


def financial_full(symbol):
    """获取完整财务报表（最近8个季度）和估值指标（最近30天）"""
    try:
        from src.data_sources.query.financial_query import FinancialQuery
        fq = FinancialQuery('data/tushare_data.db')

        data = fq.query_all_financial(symbol)
        if not data or data['income'].empty:
            return {'success': False, 'error': '暂无财务数据'}, 404

        # 转换DataFrame为列表，限制最近8条，并处理NaN值
        result = {}
        for table_type, df in data.items():
            df_sorted = df.sort_values('end_date', ascending=False).head(8)
            # 将NaN替换为None，避免JSON解析错误
            df_clean = df_sorted.replace({np.nan: None})
            result[table_type] = df_clean.to_dict('records')

        # 获取估值指标（最近30天的数据）
        try:
            code = fq._standardize_code(symbol)
            valuation_query = """
            SELECT datetime, close, pe, pe_ttm, pb, ps, ps_ttm,
                   total_mv / 10000 as total_mv_yi,
                   circ_mv / 10000 as circ_mv_yi
            FROM bars
            WHERE symbol LIKE :symbol
            AND pe IS NOT NULL
            ORDER BY datetime DESC
            LIMIT 30
            """
            with fq.engine.connect() as conn:
                valuation_df = pd.read_sql_query(valuation_query, conn, params={'symbol': f'{code}%'})

            if not valuation_df.empty:
                # 将NaN替换为None
                valuation_df = valuation_df.replace({np.nan: None})
                result['valuation'] = valuation_df.to_dict('records')
        except Exception as e:
            # 估值指标获取失败不影响整体结果
            import logging
            logging.getLogger(__name__).warning(f"Failed to get valuation for {symbol}: {e}")
            result['valuation'] = []

        return {
            'success': True,
            'symbol': symbol,
            'data': result
        }, 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}, 500


def financial_check(symbol):
    """检查股票是否有财务数据"""
    try:
        from src.data_sources.query.financial_query import FinancialQuery
        fq = FinancialQuery('data/tushare_data.db')

        latest_date = fq.get_latest_report_date(symbol, 'income')
        has_data = latest_date is not None

        return {
            'success': True,
            'symbol': symbol,
            'has_data': has_data,
            'latest_date': latest_date
        }, 200
    except Exception as e:
        return {'success': True, 'has_data': False}, 200


def financial_indicators(symbol):
    """获取股票财务指标数据（最近8个季度）"""
    try:
        from src.data_sources.query.financial_query import FinancialQuery
        fq = FinancialQuery('data/tushare_data.db')

        # 查询财务指标数据
        df = fq.query_fina_indicator(symbol)
        if df.empty:
            return {'success': False, 'error': '暂无财务指标数据'}, 404

        # 限制最近8条，并处理NaN值
        df_sorted = df.sort_values('end_date', ascending=False).head(8)
        df_clean = df_sorted.replace({np.nan: None})

        return {
            'success': True,
            'symbol': symbol,
            'data': df_clean.to_dict('records')
        }, 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}, 500


def financial_valuation(symbol):
    """获取股票最新估值指标（PE、PB、市值等）"""
    try:
        from src.data_sources.tushare import TushareDB
        from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH

        db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

        # 标准化代码
        code = db._extract_stock_code(symbol)
        ts_code_std = db._standardize_code(symbol)

        # 查询最新的估值指标（日线数据）
        query = """
        SELECT datetime, close, pe, pe_ttm, pb, ps, ps_ttm, total_mv, circ_mv
        FROM bars
        WHERE symbol LIKE :symbol
        AND pe IS NOT NULL
        ORDER BY datetime DESC
        LIMIT 1
        """

        with db.engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params={'symbol': f'{code}%'})

        if df.empty:
            return {'success': False, 'error': '暂无估值数据'}, 404

        # 转换为字典并处理NaN值
        result = df.iloc[0].to_dict()
        for key, value in result.items():
            if isinstance(value, float) and (np.isnan(value) or str(value) == 'nan'):
                result[key] = None

        return {
            'success': True,
            'symbol': symbol,
            'valuation': result
        }, 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}, 500
