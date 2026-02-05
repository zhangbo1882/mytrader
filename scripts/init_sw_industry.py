#!/usr/bin/env python3
"""
初始化申万行业数据到数据库

从 Tushare Pro 接口获取申万行业分类及其成分股并保存到数据库

Usage:
    # 获取所有申万行业数据（511个行业，耗时较长）
    python scripts/init_sw_industry.py

    # 增量更新（只更新超过7天未更新的行业）
    python scripts/init_sw_industry.py --incremental

    # 增量更新（自定义天数）
    python scripts/init_sw_industry.py --incremental --days 3

    # 获取特定行业的成分股（推荐用于测试）
    python scripts/init_sw_industry.py --index-code 801010.SI

    # 获取申万2014版本
    python scripts/init_sw_industry.py --src SW2014

    # 强制更新（删除旧数据重新获取）
    python scripts/init_sw_industry.py --index-code 801010.SI --force

常用行业代码：
    801010.SI - 农林牧渔
    801020.SI - 采掘
    801030.SI - 化工
    801040.SI - 钢铁
    801050.SI - 有色金属
    801080.SI - 电子
    801120.SI - 计算机
    801140.SI - 传媒
    801150.SI - 通信
    801160.SI - 银行
    801200.SI - 非银金融
    801710.SI - 食品饮料
    801720.SI - 纺织服饰
    801730.SI - 轻工制造
    801740.SI - 医药生物
    801750.SI - 公用事业
    801760.SI - 交通运输
    801770.SI - 房地产
    801780.SI - 商业贸易
    801790.SI - 休闲服务
    801880.SI - 建筑装饰
    801890.SI - 电气设备

查看所有行业代码：
    SELECT * FROM sw_classify WHERE src='SW2021' ORDER BY industry_code;
"""
import sys
import argparse
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_sources.tushare import TushareDB
from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH


def init_sw_industry(src: str = 'SW2021', force_update: bool = False,
                     incremental: bool = False, incremental_days: int = 7,
                     index_code: str = None):
    """
    从 Tushare Pro 获取申万行业数据并保存到数据库

    Args:
        src: 申万行业分类版本
        force_update: 是否强制更新
        incremental: 是否增量更新
        incremental_days: 增量更新天数阈值
        index_code: 指定行业代码（如 801010.SI），None 表示获取所有行业
    """
    print("=" * 60)
    if index_code:
        print(f"初始化申万行业数据到数据库 ({src}, 行业: {index_code})")
    elif incremental:
        print(f"增量更新申万行业数据到数据库 ({src}, days={incremental_days})")
    else:
        print(f"初始化申万行业数据到数据库 ({src})")
    print("=" * 60)

    db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

    if index_code:
        # 获取单个行业的成分股
        print(f"\n获取指定行业 {index_code} 的成分股...")
        count = db.save_sw_members(index_code=index_code, is_new='Y', force_update=force_update)
        stats = {'members_count': count}
        print(f"\n{'='*60}")
        print(f"申万行业成分股获取完成:")
        print(f"  成分股: {stats['members_count']} 条")
        print(f"{'='*60}")
    else:
        # 获取所有行业数据
        stats = db.save_all_sw_industry(
            src=src,
            is_new='Y',
            force_update=force_update,
            incremental=incremental,
            incremental_days=incremental_days
        )

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='初始化申万行业数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s                              获取所有申万行业数据
  %(prog)s --incremental                增量更新（7天内未更新的行业）
  %(prog)s --incremental --days 3       增量更新（3天内未更新的行业）
  %(prog)s --index-code 801010.SI       获取特定行业的成分股
  %(prog)s --force                      强制更新所有数据
        """
    )
    parser.add_argument('--src', type=str, default='SW2021',
                        choices=['SW2014', 'SW2021'],
                        help='申万行业分类版本：SW2014=2014版本，SW2021=2021版本（默认）')
    parser.add_argument('--force', action='store_true',
                        help='强制更新所有数据（忽略已存在的数据）')
    parser.add_argument('--incremental', action='store_true',
                        help='增量更新模式（只更新超过指定天数的行业）')
    parser.add_argument('--days', type=int, default=7,
                        help='增量更新天数阈值（默认7天）')
    parser.add_argument('--index-code', type=str, default=None,
                        help='指定行业代码（如 801010.SI），只获取该行业的成分股')
    args = parser.parse_args()

    init_sw_industry(
        src=args.src,
        force_update=args.force,
        incremental=args.incremental,
        incremental_days=args.days,
        index_code=args.index_code
    )
