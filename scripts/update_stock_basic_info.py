#!/usr/bin/env python3
"""
更新股票基本信息表，添加上市日期等完整信息
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from src.data_sources.tushare import TushareDB
from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH


def update_stock_basic_info():
    """
    从Tushare获取股票基本信息并更新到数据库
    """
    print("=" * 100)
    print("开始更新股票基本信息")
    print("=" * 100)

    # 初始化 Tushare 数据库连接
    db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

    print("\n正在从Tushare获取股票基本信息...")
    try:
        # 获取所有A股基本信息
        stock_basic = db._retry_api_call(
            db.pro.stock_basic,
            exchange='',
            list_status='L',  # 上市股票
            fields='ts_code,symbol,name,area,industry,market,list_date,delist_date,is_hs'
        )

        if stock_basic is None or stock_basic.empty:
            print("❌ 获取股票基本信息失败")
            return False

        print(f"✅ 获取到 {len(stock_basic)} 只股票的基本信息")

        # 添加更新时间戳
        stock_basic['updated_at'] = datetime.now().isoformat()

        # 重新排列列
        columns_order = [
            'code', 'ts_code', 'name', 'area', 'industry',
            'market', 'list_date', 'delist_date', 'is_hs', 'updated_at'
        ]

        # 重命名symbol为code（与stock_names保持一致）
        stock_basic = stock_basic.rename(columns={'symbol': 'code'})

        # 确保所有列都存在
        for col in columns_order:
            if col not in stock_basic.columns:
                stock_basic[col] = None

        stock_basic = stock_basic[columns_order]

        # 连接数据库
        engine = create_engine(f"sqlite:///{TUSHARE_DB_PATH}")

        # 检查表是否存在，不存在则创建
        check_table_sql = """
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='stock_basic_info'
        """

        with engine.connect() as conn:
            result = conn.execute(text(check_table_sql))
            table_exists = result.fetchone() is not None

            if not table_exists:
                print("\n创建 stock_basic_info 表...")
                create_table_sql = """
                CREATE TABLE stock_basic_info (
                    code TEXT PRIMARY KEY,
                    ts_code TEXT NOT NULL,
                    name TEXT NOT NULL,
                    area TEXT,
                    industry TEXT,
                    market TEXT,
                    list_date TEXT,
                    delist_date TEXT,
                    is_hs TEXT,
                    updated_at TEXT
                );
                """
                conn.execute(text(create_table_sql))
                conn.commit()
                print("✅ 表创建成功")

        # 保存数据（使用replace策略）
        print("\n保存数据到数据库...")
        stock_basic.to_sql('stock_basic_info', engine, if_exists='replace', index=False, method='multi')
        print(f"✅ 已保存 {len(stock_basic)} 只股票的基本信息")

        # 同时更新 stock_names 表（添加缺失的字段）
        print("\n更新 stock_names 表结构...")
        with engine.connect() as conn:
            # 检查并添加新字段
            alter_sqls = [
                "ALTER TABLE stock_names ADD COLUMN area TEXT",
                "ALTER TABLE stock_names ADD COLUMN industry TEXT",
                "ALTER TABLE stock_names ADD COLUMN market TEXT",
                "ALTER TABLE stock_names ADD COLUMN list_date TEXT",
            ]

            for sql in alter_sqls:
                try:
                    conn.execute(text(sql))
                    conn.commit()
                    print(f"  ✅ 添加字段成功")
                except Exception as e:
                    if "duplicate column" in str(e).lower():
                        pass  # 字段已存在，忽略
                    else:
                        print(f"  ⚠️  {e}")

            # 更新数据
            print("\n同步数据到 stock_names 表...")
            for _, row in stock_basic.iterrows():
                update_sql = """
                UPDATE stock_names
                SET area = :area,
                    industry = :industry,
                    market = :market,
                    list_date = :list_date,
                    updated_at = :updated_at
                WHERE code = :code
                """
                conn.execute(text(update_sql), {
                    'area': row['area'],
                    'industry': row['industry'],
                    'market': row['market'],
                    'list_date': row['list_date'],
                    'updated_at': row['updated_at'],
                    'code': row['code']
                })
            conn.commit()
            print("✅ stock_names 表更新完成")

        return True

    except Exception as e:
        print(f"❌ 更新股票基本信息失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def display_stock_basic_info(limit=20):
    """
    显示股票基本信息

    Args:
        limit: 显示数量限制
    """
    engine = create_engine(f"sqlite:///{TUSHARE_DB_PATH}")

    query = f"""
    SELECT * FROM stock_basic_info
    ORDER BY list_date DESC
    LIMIT {limit}
    """

    try:
        df = pd.read_sql_query(query, engine)

        if df.empty:
            print("⚠️  没有数据")
            return

        print("\n" + "=" * 120)
        print(f"最新上市的股票（前{limit}只）")
        print("=" * 120)

        # 格式化显示
        display_df = df[['code', 'name', 'list_date', 'area', 'industry', 'market']].copy()
        display_df.columns = ['代码', '名称', '上市日期', '地区', '行业', '市场']

        print(display_df.to_string(index=False))

    except Exception as e:
        print(f"❌ 查询失败: {e}")


def analyze_by_list_date():
    """
    按上市日期统计分析
    """
    engine = create_engine(f"sqlite:///{TUSHARE_DB_PATH}")

    query = """
    SELECT
        substr(list_date, 1, 4) as year,
        COUNT(*) as count
    FROM stock_basic_info
    WHERE list_date IS NOT NULL AND list_date != ''
    GROUP BY year
    ORDER BY year DESC
    """

    try:
        df = pd.read_sql_query(query, engine)

        if df.empty:
            print("⚠️  没有数据")
            return

        print("\n" + "=" * 80)
        print("按上市年份统计")
        print("=" * 80)
        print(df.to_string(index=False))

        # 统计信息
        total = df['count'].sum()
        print(f"\n总计: {total} 只股票")

    except Exception as e:
        print(f"❌ 查询失败: {e}")


if __name__ == "__main__":
    print(f"\n{'='*100}")
    print("股票基本信息更新脚本")
    print(f"{'='*100}\n")

    # 更新基本信息
    success = update_stock_basic_info()

    if success:
        # 显示最新上市的股票
        display_stock_basic_info(limit=20)

        # 按年份统计
        analyze_by_list_date()

        # 导出到CSV
        engine = create_engine(f"sqlite:///{TUSHARE_DB_PATH}")
        df = pd.read_sql_query("SELECT * FROM stock_basic_info", engine)

        csv_path = Path(__file__).parent.parent / "data" / "stock_basic_info.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"\n✅ 已导出CSV文件到: {csv_path}")

    print(f"\n{'='*100}")
    print("完成")
    print(f"{'='*100}")
