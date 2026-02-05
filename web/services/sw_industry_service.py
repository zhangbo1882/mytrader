"""
Shenwan (SW) Industry business logic services
"""
from flask import request
from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH
from src.data_sources.tushare import TushareDB
import pandas as pd


def get_sw_industries():
    """
    获取申万行业分类列表，支持层级结构

    Query params:
        src: 行业分类来源 (SW2014/SW2021)，默认 SW2021
        level: 行业级别 (L1/L2/L3)，不传则返回所有级别
        parent_code: 父级行业代码，用于筛选子行业

    Returns:
        {
            'data': [...],    # 行业列表（层级结构）
            'total': 511       # 总数
        }
    """
    src = request.args.get('src', 'SW2021')
    level = request.args.get('level')
    parent_code = request.args.get('parent_code')

    try:
        db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

        # 获取行业分类数据
        df = db.get_sw_classify(src=src, level=level)

        if df.empty:
            return {'data': [], 'total': 0}, 200

        # 如果指定了 parent_code，筛选子行业
        if parent_code:
            df = df[df['parent_code'] == parent_code]

        # 如果没有指定 level，构建层级结构
        if not level:
            # 构建 L1 -> L2 -> L3 的层级结构
            data = build_hierarchy(df)
        else:
            # 指定了 level，直接返回列表
            data = df.to_dict('records')

        return {
            'data': data,
            'total': len(df)
        }, 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'获取申万行业列表失败: {str(e)}'}, 500


def build_hierarchy(df):
    """
    构建申万行业的层级结构

    Args:
        df: sw_classify DataFrame

    Returns:
        层级结构的行业列表
    """
    # 分离不同级别
    l1_df = df[df['level'] == 'L1'] if not df.empty else pd.DataFrame()
    l2_df = df[df['level'] == 'L2'] if not df.empty else pd.DataFrame()
    l3_df = df[df['level'] == 'L3'] if not df.empty else pd.DataFrame()

    result = []

    # 构建 L1 行业
    for _, l1_row in l1_df.iterrows():
        l1_item = {
            'index_code': l1_row['index_code'],
            'industry_name': l1_row['industry_name'],
            'industry_code': l1_row['industry_code'],
            'level': l1_row['level'],
            'parent_code': l1_row.get('parent_code'),
            'children': []
        }

        # 查找 L2 子行业
        l2_children = l2_df[l2_df['parent_code'] == l1_row['index_code']]
        for _, l2_row in l2_children.iterrows():
            l2_item = {
                'index_code': l2_row['index_code'],
                'industry_name': l2_row['industry_name'],
                'industry_code': l2_row['industry_code'],
                'level': l2_row['level'],
                'parent_code': l2_row.get('parent_code'),
                'children': []
            }

            # 查找 L3 子行业
            l3_children = l3_df[l3_df['parent_code'] == l2_row['index_code']]
            for _, l3_row in l3_children.iterrows():
                l3_item = {
                    'index_code': l3_row['index_code'],
                    'industry_name': l3_row['industry_name'],
                    'industry_code': l3_row['industry_code'],
                    'level': l3_row['level'],
                    'parent_code': l3_row.get('parent_code'),
                    'children': []
                }
                l2_item['children'].append(l3_item)

            l1_item['children'].append(l2_item)

        result.append(l1_item)

    return result


def get_sw_industry_members(index_code):
    """
    获取指定申万行业的成分股列表

    Args:
        index_code: 行业代码 (如: 801010.SI)

    Returns:
        {
            'index_code': '801010.SI',
            'industry_name': '农林牧渔',
            'members': [
                {
                    'ts_code': '000001.SZ',
                    'name': '平安银行'
                }
            ],
            'total': 35
        }
    """
    try:
        db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

        # 获取成分股数据
        df = db.get_sw_industry_members(index_code)

        if df.empty:
            return {
                'index_code': index_code,
                'industry_name': None,
                'members': [],
                'total': 0
            }, 200

        # 获取行业名称
        industry_name = df['industry_name'].iloc[0] if not df.empty else None

        # 只返回当前成分股（is_new = 'Y' 且没有 out_date）
        current_members = df[
            (df['is_new'] == 'Y') &
            (pd.isna(df['out_date']) | (df['out_date'] == ''))
        ]

        # 构建返回数据
        members = []
        for _, row in current_members.iterrows():
            members.append({
                'ts_code': row['ts_code'],
                'name': row['name']
            })

        return {
            'index_code': index_code,
            'industry_name': industry_name,
            'members': members,
            'total': len(members)
        }, 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'获取成分股列表失败: {str(e)}'}, 500
