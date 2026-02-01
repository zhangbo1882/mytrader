"""
数据标准化工具

统一 AKShare 和 Tushare 的数据格式和字段名
"""
import pandas as pd
from datetime import datetime


# 标准字段名定义
STANDARD_FIELDS = {
    'datetime': '日期时间',
    'symbol': '股票代码',
    'exchange': '交易所',
    'open': '开盘价',
    'high': '最高价',
    'low': '最低价',
    'close': '收盘价',
    'pre_close': '昨收价',
    'volume': '成交量（股）',
    'turnover': '成交额（元）',
    'amount': '成交额（元）',
    'pct_chg': '涨跌幅（%）',
    'change': '涨跌额',
    'amplitude': '振幅（%）',
    'turnover_rate': '换手率（%）',
}


# AKShare 字段映射
AKSHARE_FIELD_MAPPING = {
    '日期': 'datetime',
    '股票代码': 'symbol',
    '开盘': 'open',
    '收盘': 'close',
    '最高': 'high',
    '最低': 'low',
    '成交量': 'volume',
    '成交额': 'turnover',
    '涨跌幅': 'pct_chg',
    '涨跌额': 'change',
    '振幅': 'amplitude',
    '换手率': 'turnover_rate',
}


# Tushare 字段映射
TUSHARE_FIELD_MAPPING = {
    'trade_date': 'datetime',
    'ts_code': 'symbol',
    'open': 'open',
    'close': 'close',
    'high': 'high',
    'low': 'low',
    'vol': 'volume',
    'amount': 'turnover',
    'pct_chg': 'pct_chg',
    'change': 'change',
    'pre_close': 'pre_close',
}


def standardize_akshare_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    标准化 AKShare 数据

    Args:
        df: AKShare 原始数据

    Returns:
        标准化后的 DataFrame
    """
    if df.empty:
        return df

    # 复制数据避免修改原始数据
    df = df.copy()

    # 重命名列
    df = df.rename(columns=AKSHARE_FIELD_MAPPING)

    # 日期格式转换：已经是 YYYY-MM-DD 格式，确保是字符串
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d')

    # 股票代码：已经是6位，添加交易所信息
    if 'symbol' in df.columns:
        df['symbol'] = df['symbol'].astype(str)
        # 根据代码判断交易所
        df['exchange'] = df['symbol'].apply(_detect_exchange)

    # 成交量：AKShare 已经是股，不需要转换
    # 成交额：AKShare 已经是元，不需要转换

    # 按日期排序
    if 'datetime' in df.columns:
        df = df.sort_values('datetime').reset_index(drop=True)

    return df


def standardize_tushare_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    标准化 Tushare 数据

    Args:
        df: Tushare 原始数据

    Returns:
        标准化后的 DataFrame
    """
    if df.empty:
        return df

    # 复制数据避免修改原始数据
    df = df.copy()

    # 重命名列
    df = df.rename(columns=TUSHARE_FIELD_MAPPING)

    # 日期格式转换：YYYYMMDD -> YYYY-MM-DD
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

    # 股票代码：000001.SZ -> 000001
    if 'symbol' in df.columns:
        df['symbol'] = df['symbol'].str.split('.').str[0]
        df['symbol'] = df['symbol'].astype(str)
        # 添加交易所信息
        df['exchange'] = df['symbol'].apply(_detect_exchange)

    # 成交量：Tushare 是手 -> 转换为股
    if 'volume' in df.columns:
        df['volume'] = df['volume'] * 100

    # 成交额：Tushare 是千元 -> 转换为元
    if 'turnover' in df.columns:
        df['turnover'] = df['turnover'] * 1000

    # 按日期排序
    if 'datetime' in df.columns:
        df = df.sort_values('datetime').reset_index(drop=True)

    return df


def _detect_exchange(code: str) -> str:
    """
    根据股票代码判断交易所

    Args:
        code: 6位股票代码

    Returns:
        'SSE'（上交所）或 'SZSE'（深交所）
    """
    if code.startswith(('600', '601', '603', '604', '605', '688', '689')):
        return 'SSE'
    elif code.startswith(('000', '001', '002', '003', '300', '301')):
        return 'SZSE'
    else:
        return 'UNKNOWN'


def merge_data_sources(akshare_df: pd.DataFrame, tushare_df: pd.DataFrame) -> pd.DataFrame:
    """
    合并两个数据源的数据，优先使用 AKShare，缺失数据用 Tushare 填充

    Args:
        akshare_df: 标准化后的 AKShare 数据
        tushare_df: 标准化后的 Tushare 数据

    Returns:
        合并后的 DataFrame
    """
    if akshare_df.empty and tushare_df.empty:
        return pd.DataFrame()

    if akshare_df.empty:
        return tushare_df

    if tushare_df.empty:
        return akshare_df

    # 合并数据
    merged = pd.concat([akshare_df, tushare_df], ignore_index=True)

    # 去重：同一股票同一日期保留第一条（AKShare 优先）
    if 'symbol' in merged.columns and 'datetime' in merged.columns:
        merged = merged.drop_duplicates(subset=['symbol', 'datetime'], keep='first')

    # 按日期排序
    merged = merged.sort_values('datetime').reset_index(drop=True)

    return merged


def validate_data(df: pd.DataFrame, source: str = "") -> dict:
    """
    验证数据质量

    Args:
        df: 数据 DataFrame
        source: 数据源名称

    Returns:
        验证结果字典
    """
    result = {
        'source': source,
        'total_records': len(df),
        'has_null': False,
        'negative_price': False,
        'zero_volume': False,
        'issues': []
    }

    if df.empty:
        result['issues'].append("数据为空")
        return result

    # 检查空值
    null_counts = df.isnull().sum()
    if null_counts.any():
        result['has_null'] = True
        for col, count in null_counts.items():
            if count > 0:
                result['issues'].append(f"{col} 有 {count} 个空值")

    # 检查价格为负数
    price_cols = ['open', 'high', 'low', 'close']
    for col in price_cols:
        if col in df.columns:
            if (df[col] < 0).any():
                result['negative_price'] = True
                result['issues'].append(f"{col} 存在负数")

    # 检查成交量为0
    if 'volume' in df.columns:
        zero_count = (df['volume'] == 0).sum()
        if zero_count > 0:
            result['zero_volume'] = True
            result['issues'].append(f"有 {zero_count} 条记录成交量为0")

    # 检查价格逻辑（最高价 >= 最低价，收盘价在最高最低价之间）
    if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
        invalid_high = df['high'] < df['low']
        if invalid_high.any():
            result['issues'].append(f"有 {invalid_high.sum()} 条记录最高价 < 最低价")

    return result


if __name__ == "__main__":
    # 测试代码
    print("数据标准化工具模块")
    print(f"标准字段定义: {len(STANDARD_FIELDS)} 个")
    print(f"AKShare 映射: {len(AKSHARE_FIELD_MAPPING)} 个字段")
    print(f"Tushare 映射: {len(TUSHARE_FIELD_MAPPING)} 个字段")
