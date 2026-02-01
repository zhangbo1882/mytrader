#!/usr/bin/env python3
"""
初始化股票名称到数据库

从 akshare 免费接口获取所有 A 股股票名称并保存到数据库
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_sources.tushare import TushareDB
from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH
import akshare as ak


def init_stock_names():
    """从 akshare 获取股票名称并保存到数据库"""
    print("=" * 60)
    print("初始化股票名称到数据库")
    print("=" * 60)

    # 初始化数据库
    db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

    # 从 akshare 获取所有 A 股股票信息
    print("\n正在从 akshare 获取股票列表...")
    try:
        stock_info = ak.stock_info_a_code_name()
        print(f"✅ 成功获取 {len(stock_info)} 支股票的信息")
    except Exception as e:
        print(f"❌ 获取股票列表失败: {e}")
        return

    # 构建股票代码到名称的映射
    names_dict = {}
    for _, row in stock_info.iterrows():
        code = row['code']
        name = row['name']
        names_dict[code] = name

    # 批量保存到数据库
    print(f"\n正在保存 {len(names_dict)} 支股票名称到数据库...")
    success_count = db.save_stock_names_batch(names_dict)

    print(f"\n✅ 成功保存 {success_count} 支股票名称")
    print("=" * 60)


if __name__ == "__main__":
    init_stock_names()
