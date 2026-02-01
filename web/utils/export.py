"""
数据导出工具
"""
import pandas as pd
from flask import send_file
import io
from datetime import datetime


def export_to_csv(data, filename=None):
    """
    导出为 CSV

    Args:
        data: 数据字典 {symbol: [records]}
        filename: 文件名

    Returns:
        Flask send_file response
    """
    if not filename:
        filename = f"stock_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    # 将数据转换为 DataFrame
    dfs = []
    for symbol, records in data.items():
        df = pd.DataFrame(records)
        df['股票代码'] = symbol
        dfs.append(df)

    if not dfs:
        # 如果没有数据，返回空文件
        output = io.BytesIO()
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )

    result_df = pd.concat(dfs, ignore_index=True)

    # 选择和重命名列
    column_map = {
        'datetime': '日期',
        'open': '开盘价',
        'high': '最高价',
        'low': '最低价',
        'close': '收盘价',
        'volume': '成交量',
        'turnover': '换手率(%)',
        'pct_chg': '涨跌幅(%)',
        'amount': '成交额'
    }

    # 重命名存在的列
    result_df = result_df.rename(columns=column_map)

    # 选择需要的列（股票代码总是在最前）
    columns = ['股票代码', '日期', '开盘价', '最高价', '最低价', '收盘价', '成交量', '换手率(%)', '涨跌幅(%)']
    available_columns = [col for col in columns if col in result_df.columns]
    result_df = result_df[available_columns]

    output = io.StringIO()
    result_df.to_csv(output, index=False, encoding='utf-8-sig')
    output.seek(0)

    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8-sig')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )


def export_to_excel(data, filename=None):
    """
    导出为 Excel

    Args:
        data: 数据字典 {symbol: [records]}
        filename: 文件名

    Returns:
        Flask send_file response
    """
    if not filename:
        filename = f"stock_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    output = io.BytesIO()

    column_map = {
        'datetime': '日期',
        'open': '开盘价',
        'high': '最高价',
        'low': '最低价',
        'close': '收盘价',
        'volume': '成交量',
        'turnover': '换手率(%)',
        'pct_chg': '涨跌幅(%)',
        'amount': '成交额'
    }

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        if not data:
            # 如果没有数据，创建一个空的工作表
            pd.DataFrame().to_excel(writer, sheet_name='无数据', index=False)
        else:
            for symbol, records in data.items():
                if not records:
                    continue

                df = pd.DataFrame(records)
                df['股票代码'] = symbol

                # 重命名列
                df = df.rename(columns=column_map)

                # 选择需要的列
                columns = ['股票代码', '日期', '开盘价', '最高价', '最低价', '收盘价', '成交量', '换手率(%)', '涨跌幅(%)']
                available_columns = [col for col in columns if col in df.columns]
                df = df[available_columns]

                # Excel sheet name 最多31个字符
                sheet_name = symbol[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    output.seek(0)

    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename
    )
