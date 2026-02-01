#!/usr/bin/env python3
"""
回填 Tushare Daily Basic 每日指标数据

为现有历史数据回填 daily_basic 指标（PE、PB、市值等），只获取 daily_basic 数据，
不重复获取 daily 和 adj_factor 数据，使用 UPDATE 语句更新现有记录。

使用方法:
    # 回填所有股票
    python scripts/backfill_daily_basic.py

    # 回填指定日期范围
    python scripts/backfill_daily_basic.py --start-date 20230101 --end-date 20240101

    # 回填指定股票
    python scripts/backfill_daily_basic.py --stocks 000001,600000

    # 从文件读取股票列表
    python scripts/backfill_daily_basic.py --stocks stocks.txt

    # 继续上次的回填
    python scripts/backfill_daily_basic.py --resume

    # 查看帮助
    python scripts/backfill_daily_basic.py --help
"""
import argparse
import sys
import os
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import text

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_sources.tushare import TushareDB


def get_stocks_need_backfill(db):
    """
    获取需要回填 daily_basic 数据的股票列表

    Returns:
        list: 需要回填的股票代码列表
    """
    query = """
    SELECT DISTINCT symbol
    FROM bars
    WHERE pe IS NULL
      AND pb IS NULL
      AND total_mv IS NULL
      AND interval = '1d'
    ORDER BY symbol
    """
    try:
        with db.engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
        if not df.empty:
            return df['symbol'].tolist()
        return []
    except Exception as e:
        print(f"❌ 查询需要回填的股票失败: {e}")
        return []


def get_stock_date_range(db, symbol):
    """
    获取股票的数据日期范围

    Args:
        db: TushareDB 实例
        symbol: 股票代码

    Returns:
        tuple: (start_date, end_date) 格式为 YYYYMMDD，如果没有数据返回 (None, None)
    """
    query = """
    SELECT MIN(datetime) as min_date, MAX(datetime) as max_date
    FROM bars
    WHERE symbol = :symbol
      AND interval = '1d'
    """
    try:
        with db.engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params={"symbol": symbol})
        if not df.empty and df['min_date'].iloc[0] is not None:
            start_date = pd.to_datetime(df['min_date'].iloc[0]).strftime("%Y%m%d")
            end_date = pd.to_datetime(df['max_date'].iloc[0]).strftime("%Y%m%d")
            return start_date, end_date
        return None, None
    except Exception as e:
        print(f"  ⚠️  {symbol} 获取日期范围失败: {e}")
        return None, None


def get_existing_dates_with_basic(db, symbol):
    """
    获取已有 daily_basic 数据的日期列表

    Args:
        db: TushareDB 实例
        symbol: 股票代码

    Returns:
        set: 已有数据的日期集合
    """
    query = """
    SELECT datetime
    FROM bars
    WHERE symbol = :symbol
      AND interval = '1d'
      AND pe IS NOT NULL
    """
    try:
        with db.engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params={"symbol": symbol})
        if not df.empty:
            return set(pd.to_datetime(df['datetime']).dt.strftime("%Y-%m-%d"))
        return set()
    except Exception as e:
        print(f"  ⚠️  {symbol} 查询已有数据失败: {e}")
        return set()


def backfill_stock(db, symbol, start_date=None, end_date=None, skip_existing=True):
    """
    回填单只股票的 daily_basic 数据

    Args:
        db: TushareDB 实例
        symbol: 股票代码
        start_date: 开始日期 YYYYMMDD，None 则自动检测
        end_date: 结束日期 YYYYMMDD，None 则自动检测
        skip_existing: 是否跳过已有数据的日期

    Returns:
        dict: {'success': bool, 'updated': int, 'skipped': int}
    """
    # 标准化代码
    try:
        ts_code = db._standardize_code(symbol)
    except Exception as e:
        return {'success': False, 'updated': 0, 'skipped': 0, 'error': str(e)}

    # 自动检测日期范围
    if start_date is None or end_date is None:
        detected_start, detected_end = get_stock_date_range(db, ts_code.split('.')[0])
        if start_date is None:
            start_date = detected_start
        if end_date is None:
            end_date = detected_end

    if start_date is None or end_date is None:
        return {'success': False, 'updated': 0, 'skipped': 0, 'error': '无法检测日期范围'}

    # 获取已有数据的日期
    existing_dates = set()
    if skip_existing:
        existing_dates = get_existing_dates_with_basic(db, ts_code.split('.')[0])

    try:
        # 获取 daily_basic 数据
        basic = db._retry_api_call(
            db.pro.daily_basic,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields='ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv'
        )

        if basic is None or basic.empty:
            return {'success': True, 'updated': 0, 'skipped': 0}

        # 过滤已有数据
        if skip_existing and existing_dates:
            basic['trade_date_formatted'] = pd.to_datetime(basic['trade_date']).dt.strftime("%Y-%m-%d")
            basic = basic[~basic['trade_date_formatted'].isin(existing_dates)]
            existing_count = len(existing_dates)
            basic = basic.drop(columns=['trade_date_formatted'])
        else:
            existing_count = 0

        if basic.empty:
            return {'success': True, 'updated': 0, 'skipped': len(existing_dates)}

        # 准备更新数据
        updates = []
        code = ts_code.split('.')[0]

        for _, row in basic.iterrows():
            trade_date = pd.to_datetime(row['trade_date']).strftime("%Y-%m-%d")
            update_data = {
                'symbol': code,
                'datetime': trade_date,
                'turnover': row.get('turnover_rate'),
                'turnover_rate_f': row.get('turnover_rate_f'),
                'volume_ratio': row.get('volume_ratio'),
                'pe': row.get('pe'),
                'pe_ttm': row.get('pe_ttm'),
                'pb': row.get('pb'),
                'ps': row.get('ps'),
                'ps_ttm': row.get('ps_ttm'),
                'dv_ratio': row.get('dv_ratio'),
                'dv_ttm': row.get('dv_ttm'),
                'total_mv': row.get('total_mv'),
                'circ_mv': row.get('circ_mv'),
                'total_share': row.get('total_share'),
                'float_share': row.get('float_share'),
                'free_share': row.get('free_share'),
            }
            updates.append(update_data)

        # 批量更新数据库
        updated_count = 0
        with db.engine.connect() as conn:
            for update_data in updates:
                try:
                    # 构建 UPDATE 语句
                    set_clauses = []
                    params = {'symbol': update_data['symbol'], 'datetime': update_data['datetime']}

                    for key, value in update_data.items():
                        if key not in ['symbol', 'datetime'] and value is not None:
                            set_clauses.append(f"{key} = :{key}")
                            params[key] = value

                    if set_clauses:
                        sql = f"""
                        UPDATE bars
                        SET {', '.join(set_clauses)}
                        WHERE symbol = :symbol AND datetime = :datetime AND interval = '1d'
                        """
                        result = conn.execute(text(sql), params)
                        updated_count += result.rowcount
                except Exception as e:
                    pass  # 跳过更新失败的记录

            conn.commit()

        return {
            'success': True,
            'updated': updated_count,
            'skipped': existing_count
        }

    except Exception as e:
        error_msg = str(e)
        if "无权限" in error_msg or "权限" in error_msg or "403" in error_msg:
            return {'success': False, 'updated': 0, 'skipped': 0, 'error': '无权限（需要2000+积分）'}
        return {'success': False, 'updated': 0, 'skipped': 0, 'error': error_msg}


def save_checkpoint(checkpoint_path: str, data: dict):
    """保存检查点"""
    try:
        with open(checkpoint_path, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"  ⚠️  保存检查点失败: {e}")


def load_checkpoint(checkpoint_path: str) -> dict:
    """加载检查点"""
    try:
        if Path(checkpoint_path).exists():
            with open(checkpoint_path, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"  ⚠️  加载检查点失败: {e}")
    return {}


def main():
    parser = argparse.ArgumentParser(
        description='回填 Tushare Daily Basic 每日指标数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s                                    # 回填所有股票
  %(prog)s --start-date 20230101             # 指定开始日期
  %(prog)s --start-date 20230101 --end-date 20240101  # 指定日期范围
  %(prog)s --stocks 000001,600000            # 回填指定股票
  %(prog)s --stocks stocks.txt               # 从文件读取股票列表
  %(prog)s --resume                          # 继续上次的回填
        """
    )
    parser.add_argument('--start-date', default=None,
                       help='开始日期 (YYYYMMDD)，默认自动检测')
    parser.add_argument('--end-date', default=None,
                       help='结束日期 (YYYYMMDD)，默认自动检测')
    parser.add_argument('--stocks', default=None,
                       help='股票列表：逗号分隔的代码(如000001,600000)或文件路径(每行一个代码)')
    parser.add_argument('--resume', action='store_true',
                       help='从检查点恢复')
    parser.add_argument('--checkpoint', default='data/backfill_daily_basic_checkpoint.json',
                       help='检查点文件路径')
    parser.add_argument('--skip-existing', action='store_true', default=True,
                       help='跳过已有数据的日期（默认启用）')

    args = parser.parse_args()

    # 从环境变量或配置文件获取token
    try:
        from config.settings import TUSHARE_TOKEN, TUSHARE_DB_PATH
    except ImportError:
        print("❌ 无法导入配置文件，请确保 config/settings.py 存在并包含 TUSHARE_TOKEN")
        return 1

    if not TUSHARE_TOKEN:
        print("❌ TUSHARE_TOKEN 未设置，请在 config/settings.py 中配置")
        return 1

    # 初始化数据库连接
    db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

    # 获取需要回填的股票列表
    if args.stocks:
        if ',' in args.stocks:
            stock_list = [s.strip() for s in args.stocks.split(',') if s.strip()]
            print(f"📋 使用指定的股票列表: {len(stock_list)} 只")
        elif Path(args.stocks).exists():
            # 是文件路径
            try:
                with open(args.stocks, 'r') as f:
                    stock_list = [line.strip() for line in f if line.strip()]
                print(f"📋 从文件读取了 {len(stock_list)} 只股票")
            except Exception as e:
                print(f"❌ 读取文件失败: {e}")
                return 1
        else:
            # 是单个股票代码
            stock_list = [args.stocks.strip()]
            print(f"📋 使用指定的股票: {stock_list[0]}")
    else:
        print("🔍 查询需要回填的股票...")
        stock_list = get_stocks_need_backfill(db)
        print(f"📋 共 {len(stock_list)} 只股票需要回填 daily_basic 数据")

    if not stock_list:
        print("✅ 没有需要回填的股票")
        return 0

    # 尝试从检查点恢复
    start_index = 0
    stats = {'success': 0, 'failed': 0, 'updated': 0, 'total': len(stock_list)}

    if args.resume:
        checkpoint = load_checkpoint(args.checkpoint)
        if checkpoint:
            if checkpoint.get('total') == len(stock_list):
                last_index = checkpoint.get('last_index', 0)
                start_index = last_index + 1
                stats = checkpoint.get('stats', stats)
                print(f"🔄 从检查点恢复: 第 {start_index + 1} 只股票开始")
            else:
                print("⚠️  检查点不匹配，从头开始")
                Path(args.checkpoint).unlink(missing_ok=True)

    # 显示回填信息
    print("=" * 60)
    print("Daily Basic 数据回填")
    print("=" * 60)
    print(f"总股票数: {len(stock_list)}")
    if args.start_date:
        print(f"开始日期: {args.start_date}")
    if args.end_date:
        print(f"结束日期: {args.end_date}")
    print(f"跳过已有数据: {args.skip_existing}")
    print("⚠️  API频率限制：每分钟50次，每次间隔1.3秒")
    print("=" * 60)
    print()

    # 遍历每只股票
    for i in range(start_index, len(stock_list)):
        symbol = stock_list[i]

        # 定期显示进度（每50只股票）
        if (i + 1) % 50 == 1 or i == len(stock_list) - 1:
            print(f"\n{'='*60}")
            print(f"进度: [{i + 1}/{stats['total']}]")
            print(f"成功: {stats['success']} | 失败: {stats['failed']} | 已更新记录: {stats['updated']}")
            print(f"{'='*60}")

        try:
            result = backfill_stock(
                db, symbol,
                start_date=args.start_date,
                end_date=args.end_date,
                skip_existing=args.skip_existing
            )

            if result['success']:
                stats['success'] += 1
                stats['updated'] += result['updated']
                if result['updated'] > 0:
                    print(f"✅ {symbol} 更新了 {result['updated']} 条记录")
                else:
                    print(f"⏭️  {symbol} 无需更新（已有数据或无新数据）")
            else:
                stats['failed'] += 1
                error = result.get('error', '未知错误')
                print(f"❌ {symbol} 失败: {error}")

        except Exception as e:
            stats['failed'] += 1
            print(f"❌ {symbol} 处理失败: {e}")

        # 每10只股票保存一次检查点
        if (i + 1) % 10 == 0:
            checkpoint_data = {
                'total': len(stock_list),
                'last_index': i,
                'stats': stats,
                'timestamp': datetime.now().isoformat()
            }
            save_checkpoint(args.checkpoint, checkpoint_data)

    # 删除检查点文件（回填完成）
    if Path(args.checkpoint).exists():
        Path(args.checkpoint).unlink()
        print("🗑️  已删除检查点文件")

    # 输出统计信息
    print(f"\n{'='*60}")
    print(f"Daily Basic 数据回填完成:")
    print(f"  总计: {stats['total']} 只股票")
    print(f"  成功: {stats['success']}")
    print(f"  失败: {stats['failed']}")
    print(f"  更新记录: {stats['updated']} 条")
    print(f"{'='*60}")

    return 0


if __name__ == '__main__':
    exit(main())
