#!/usr/bin/env python3
"""
下载全部A股历史数据脚本（保存到DuckDB的bars_a_1d表）

使用方法:
    # 增量更新（只下载最新数据，推荐日常使用）
    python scripts/download_all_stocks.py --incremental

    # 下载最近2年数据（默认，从2024-01-01开始）
    python scripts/download_all_stocks.py

    # 指定日期范围
    python scripts/download_all_stocks.py --start-date 20230101 --end-date 20240101

    # 指定股票列表
    python scripts/download_all_stocks.py --stocks 000001,600000

    # 按市场类型下载
    python scripts/download_all_stocks.py --market main          # 只下载主板
    python scripts/download_all_stocks.py --market gem,star      # 下载创业板和科创板

    # 排除ST股票
    python scripts/download_all_stocks.py --exclude-st

    # 排除特定股票
    python scripts/download_all_stocks.py --exclude 000001,600000

    # 组合使用
    python scripts/download_all_stocks.py --market main,gem --exclude-st --incremental

    # 查看帮助
    python scripts/download_all_stocks.py --help
"""
import argparse
from datetime import datetime, timedelta
import sys
import os
import pandas as pd
import time

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db.duckdb_manager import DuckDBManager
import tushare as ts


# 市场类型映射
MARKET_TYPE_MAP = {
    'main': ['上海主板', '深圳主板', '中小板'],      # 主板
    'gem': ['创业板'],                               # 创业板
    'star': ['科创板'],                              # 科创板
    'bse': ['北交所'],                               # 北交所
}


def get_market_type(code: str) -> str:
    """
    根据股票代码判断市场类型

    Args:
        code: 6位股票代码

    Returns:
        市场类型: 'main', 'gem', 'star', 'bse', 'unknown'
    """
    code = str(code).strip()
    # 上海主板
    if code.startswith(('600', '601', '603', '604', '605')):
        return 'main'
    # 科创板
    elif code.startswith(('688', '689')):
        return 'star'
    # 深圳主板
    elif code.startswith(('000', '001')):
        return 'main'
    # 中小板（已合并到主板）
    elif code.startswith(('002', '003')):
        return 'main'
    # 创业板
    elif code.startswith(('300', '301')):
        return 'gem'
    # 北交所
    elif code.startswith(('8', '4')):
        return 'bse'
    else:
        return 'unknown'


# DuckDB bars_a_1d 表的列
BARS_A_1D_COLUMNS = [
    "stock_code", "exchange", "datetime",
    "open", "high", "low", "close",
    "open_qfq", "high_qfq", "low_qfq", "close_qfq",
    "pre_close", "change", "pct_chg",
    "volume", "turnover", "amount",
    "pe", "pe_ttm", "pb", "ps", "ps_ttm",
    "total_mv", "circ_mv",
    "total_share", "float_share", "free_share",
    "volume_ratio", "turnover_rate_f",
    "dv_ratio", "dv_ttm"
]


def get_stock_list(pro, duckdb, stock_list=None, markets=None, exclude_st=False, exclude_codes=None):
    """
    获取股票列表

    Args:
        pro: Tushare pro API
        duckdb: DuckDBManager 实例
        stock_list: 指定的股票列表，None则获取全部A股
        markets: 市场类型列表，如 ['main', 'gem']，None表示不过滤
        exclude_st: 是否排除ST股票
        exclude_codes: 要排除的股票代码列表

    Returns:
        股票代码列表 (带后缀，如 000001.SZ)
    """
    if stock_list:
        # 标准化股票代码（添加后缀）
        result = []
        for code in stock_list:
            if '.' in code:
                result.append(code)
            else:
                # 根据代码判断交易所
                if code.startswith('6'):
                    result.append(f"{code}.SH")
                else:
                    result.append(f"{code}.SZ")
        return result

    # 获取全部A股列表（包含股票名称用于ST判断）
    print("📋 正在获取股票列表...")
    df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,area,industry,list_date')
    if df is None or df.empty:
        print("❌ 获取股票列表失败")
        return []

    original_count = len(df)

    # 按市场类型过滤
    if markets:
        df['market_type'] = df['ts_code'].apply(lambda x: get_market_type(x.split('.')[0]))
        df = df[df['market_type'].isin(markets)]
        print(f"📍 市场过滤: {original_count} -> {len(df)} 只股票 (市场: {', '.join(markets)})")

    # 排除ST股票
    if exclude_st:
        # 先从股票名称判断
        st_mask = df['name'].str.contains('ST', case=False, na=False)
        st_from_name = df[st_mask]['ts_code'].tolist()

        # 再从数据库st_stocks表查询
        st_from_db = set()
        try:
            if duckdb.table_exists('st_stocks'):
                query = "SELECT DISTINCT stock_code FROM st_stocks"
                st_df = duckdb.query(query).fetchdf()
                if not st_df.empty:
                    st_from_db = set(st_df['stock_code'].tolist())
        except Exception as e:
            print(f"  ⚠️ 查询st_stocks表失败: {e}")

        # 合并ST股票列表
        all_st_codes = set(st_from_name) | st_from_db
        if all_st_codes:
            df = df[~df['ts_code'].isin(all_st_codes)]
            print(f"🚫 排除ST股票: {len(all_st_codes)} 只")

    # 排除指定股票
    if exclude_codes:
        exclude_set = set()
        for code in exclude_codes:
            if '.' in code:
                exclude_set.add(code)
            else:
                # 添加SH/SZ后缀
                if code.startswith('6'):
                    exclude_set.add(f"{code}.SH")
                else:
                    exclude_set.add(f"{code}.SZ")
        df = df[~df['ts_code'].isin(exclude_set)]
        print(f"❌ 排除指定股票: {len(exclude_set)} 只")

    stock_codes = df['ts_code'].tolist()
    print(f"📋 最终股票数量: {len(stock_codes)} 只")
    return stock_codes


def get_incremental_start_dates(duckdb, stock_codes, end_date):
    """
    获取增量更新的开始日期

    Args:
        duckdb: DuckDBManager 实例
        stock_codes: 股票代码列表
        end_date: 结束日期

    Returns:
        dict: {股票代码: 开始日期}，None表示需要从头下载
    """
    result = {}
    end_date_dt = pd.to_datetime(end_date)

    print("🔍 检查本地数据最新日期...")

    for ts_code in stock_codes:
        try:
            code = ts_code.split('.')[0]

            # 查询该股票的最新数据日期
            query = f"""
            SELECT MAX(datetime) as latest_date FROM bars_a_1d
            WHERE stock_code = '{code}'
            """
            df = duckdb.query(query).fetchdf()

            if not df.empty and df['latest_date'].iloc[0] is not None:
                latest_date = pd.to_datetime(df['latest_date'].iloc[0])
                # 从最新日期的下一天开始
                start_date_dt = latest_date + timedelta(days=1)
                # 转换为 YYYYMMDD 格式
                start_date = start_date_dt.strftime("%Y%m%d")

                # 只在需要更新时才添加到结果中
                if start_date_dt <= end_date_dt:
                    result[ts_code] = start_date
                else:
                    result[ts_code] = None  # 已是最新
            else:
                # 无数据，需要从头下载
                result[ts_code] = None

        except Exception as e:
            print(f"  ⚠️  {ts_code} 检查失败: {e}")
            result[ts_code] = None

    # 统计
    need_update = sum(1 for v in result.values() if v is not None)
    no_data = sum(1 for v in result.values() if v is None)

    print(f"✅ 检查完成: {need_update} 只需要增量更新, {no_data} 只需要全量下载")

    return result


def download_stock_data(pro, ts_code, start_date, end_date):
    """
    下载单只股票的数据

    Args:
        pro: Tushare pro API
        ts_code: 股票代码（带后缀）
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)

    Returns:
        DataFrame or None
    """
    try:
        # 获取日线数据
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

        if df is None or df.empty:
            return None

        # 获取复权因子
        try:
            adj_df = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if adj_df is not None and not adj_df.empty:
                df = df.merge(adj_df, on=['ts_code', 'trade_date'], how='left')
                # 计算前复权价格
                df['open_qfq'] = df['open'] * df['adj_factor']
                df['high_qfq'] = df['high'] * df['adj_factor']
                df['low_qfq'] = df['low'] * df['adj_factor']
                df['close_qfq'] = df['close'] * df['adj_factor']
            else:
                df['open_qfq'] = None
                df['high_qfq'] = None
                df['low_qfq'] = None
                df['close_qfq'] = None
        except Exception:
            df['open_qfq'] = None
            df['high_qfq'] = None
            df['low_qfq'] = None
            df['close_qfq'] = None

        # 获取 daily_basic 数据
        try:
            basic = pro.daily_basic(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields='ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv'
            )

            if basic is not None and not basic.empty:
                df = df.merge(basic, on=['ts_code', 'trade_date'], how='left')
        except Exception:
            pass

        # 设置缺失的列
        for col in ['turnover_rate_f', 'volume_ratio', 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm',
                    'total_mv', 'circ_mv', 'total_share', 'float_share', 'free_share',
                    'dv_ratio', 'dv_ttm']:
            if col not in df.columns:
                df[col] = None

        # 重命名列以匹配 DuckDB 表结构
        df = df.rename(columns={
            "trade_date": "datetime",
            "vol": "volume",
            "turnover_rate": "turnover"
        })

        # 添加 stock_code 和 exchange
        code, exchange_suffix = ts_code.split('.')
        df["stock_code"] = code
        df["exchange"] = exchange_suffix

        # 格式化日期
        df["datetime"] = pd.to_datetime(df["datetime"])

        # 确保 amount 列存在
        if 'amount' not in df.columns:
            df['amount'] = None

        # 选择需要的列
        return df[BARS_A_1D_COLUMNS]

    except Exception as e:
        print(f"  ❌ {ts_code} 下载失败: {e}")
        return None


def save_to_duckdb(duckdb, df):
    """
    保存数据到 DuckDB

    Args:
        duckdb: DuckDBManager 实例
        df: DataFrame

    Returns:
        是否成功
    """
    try:
        # 使用 INSERT ON CONFLICT DO NOTHING 避免重复
        duckdb.insert_dataframe(df, 'bars_a_1d', on_conflict='DO NOTHING')
        return True
    except Exception as e:
        if "Constraint" in str(e) or "duplicate" in str(e).lower():
            # 数据已存在，不算失败
            return True
        print(f"  ⚠️ 保存失败: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='下载全部A股历史数据（保存到DuckDB的bars_a_1d表）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s --incremental                      # 增量更新（只下载最新数据）
  %(prog)s --incremental --stocks 000001,600000  # 更新指定股票
  %(prog)s                                    # 下载最近2年数据（默认）
  %(prog)s --start-date 20230101             # 指定开始日期
  %(prog)s --start-date 20230101 --end-date 20240101  # 指定日期范围
  %(prog)s --market main                      # 只下载主板股票
  %(prog)s --market main,gem --exclude-st     # 下载主板和创业板，排除ST股
  %(prog)s --market star --incremental        # 只下载科创板，增量更新
  %(prog)s --exclude-st --exclude 000001,600000  # 排除ST股和特定股票
        """
    )
    parser.add_argument('--start-date', default='20240101',
                       help='开始日期 (YYYYMMDD)，默认为20240101。使用--incremental时自动检测')
    parser.add_argument('--end-date', default=None,
                       help='结束日期 (YYYYMMDD)，默认为今天')
    parser.add_argument('--incremental', action='store_true',
                       help='增量更新模式：只下载每只股票最新缺失的数据（推荐日常使用）')
    parser.add_argument('--stocks', default=None,
                       help='股票列表：逗号分隔的代码(如000001,600000)')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='批量保存的行数阈值（默认100）')
    parser.add_argument('--market', default=None,
                       help='市场类型：逗号分隔(如main,gem,star,bse)。main=主板,gem=创业板,star=科创板,bse=北交所')
    parser.add_argument('--exclude-st', action='store_true',
                       help='排除ST股票')
    parser.add_argument('--exclude', default=None,
                       help='排除指定股票：逗号分隔的代码(如000001,600000)')

    args = parser.parse_args()

    # 处理股票列表参数
    stock_list = None
    if args.stocks:
        stock_list = [s.strip() for s in args.stocks.split(',') if s.strip()]

    # 处理市场类型参数
    markets = None
    if args.market:
        markets = [m.strip().lower() for m in args.market.split(',') if m.strip()]
        # 验证市场类型
        valid_markets = set(MARKET_TYPE_MAP.keys())
        invalid = set(markets) - valid_markets
        if invalid:
            print(f"❌ 无效的市场类型: {', '.join(invalid)}")
            print(f"   有效类型: {', '.join(valid_markets)}")
            return 1

    # 处理排除股票参数
    exclude_codes = None
    if args.exclude:
        exclude_codes = [s.strip() for s in args.exclude.split(',') if s.strip()]

    # 如果没有指定结束日期，使用今天
    if args.end_date is None:
        args.end_date = datetime.today().strftime("%Y%m%d")

    # 从环境变量或配置文件获取token
    try:
        from config.settings import TUSHARE_TOKEN
    except ImportError:
        print("❌ 无法导入配置文件，请确保 config/settings.py 存在并包含 TUSHARE_TOKEN")
        return 1

    if not TUSHARE_TOKEN:
        print("❌ TUSHARE_TOKEN 未设置，请在 config/settings.py 中配置")
        return 1

    # 初始化 Tushare API
    pro = ts.pro_api(TUSHARE_TOKEN)

    # 初始化 DuckDB
    duckdb = DuckDBManager()

    # 检查表是否存在
    if not duckdb.table_exists('bars_a_1d'):
        print("❌ bars_a_1d 表不存在，请先初始化数据库")
        return 1

    # 获取股票列表
    all_stocks = get_stock_list(
        pro, duckdb,
        stock_list=stock_list,
        markets=markets,
        exclude_st=args.exclude_st,
        exclude_codes=exclude_codes
    )
    if not all_stocks:
        print("❌ 没有可下载的股票")
        return 1

    # 显示下载信息
    print("=" * 60)
    if args.incremental:
        print("全部A股数据增量更新（保存到DuckDB bars_a_1d表）")
    else:
        print("全部A股数据下载（保存到DuckDB bars_a_1d表）")
    print("=" * 60)

    if args.incremental:
        print(f"更新模式: 增量更新（只下载最新数据）")
        print(f"默认开始日期: {args.start_date}（用于无数据的股票）")
    else:
        print(f"开始日期: {args.start_date}")

    print(f"结束日期: {args.end_date}")

    # 显示过滤条件
    if markets:
        market_names = {
            'main': '主板',
            'gem': '创业板',
            'star': '科创板',
            'bse': '北交所'
        }
        market_display = [market_names.get(m, m) for m in markets]
        print(f"市场类型: {', '.join(market_display)}")

    if args.exclude_st:
        print(f"排除ST股票: 是")

    if exclude_codes:
        print(f"排除指定股票: {', '.join(exclude_codes)}")

    print(f"股票数量: {len(all_stocks)}")
    print("=" * 60)

    # 增量更新时获取每只股票的开始日期
    start_dates = {}
    if args.incremental:
        start_dates = get_incremental_start_dates(duckdb, all_stocks, args.end_date)

    # 统计
    stats = {'success': 0, 'failed': 0, 'skipped': 0, 'total': len(all_stocks)}

    # 遍历每只股票
    for i, ts_code in enumerate(all_stocks):
        # 显示进度
        if (i + 1) % 50 == 1 or i == len(all_stocks) - 1:
            print(f"\n{'='*60}")
            print(f"进度: [{i + 1}/{stats['total']}]")
            print(f"成功: {stats['success']} | 失败: {stats['failed']} | 跳过: {stats['skipped']}")
            print(f"{'='*60}")

        # 确定开始日期
        if args.incremental:
            if ts_code in start_dates:
                start_date = start_dates[ts_code]
                if start_date is None:
                    # 检查是否已经是最新
                    code = ts_code.split('.')[0]
                    query = f"SELECT MAX(datetime) as latest FROM bars_a_1d WHERE stock_code = '{code}'"
                    df = duckdb.query(query).fetchdf()
                    if not df.empty and df['latest'].iloc[0] is not None:
                        print(f"⏭️  {ts_code} 已是最新，跳过")
                        stats['skipped'] += 1
                        continue
                    # 无数据，使用默认开始日期
                    start_date = args.start_date
            else:
                start_date = args.start_date
        else:
            start_date = args.start_date

        # 下载股票数据
        print(f"📥 {ts_code} ({start_date} - {args.end_date})...")
        df = download_stock_data(pro, ts_code, start_date, args.end_date)

        if df is None or df.empty:
            print(f"  ⚠️ {ts_code} 无数据")
            stats['skipped'] += 1
        else:
            # 保存到 DuckDB
            if save_to_duckdb(duckdb, df):
                print(f"  ✅ {ts_code} 已保存 {len(df)} 条记录")
                stats['success'] += 1
            else:
                stats['failed'] += 1

        # API频率限制：每分钟50次，间隔1.2秒
        time.sleep(1.2)

    # 输出统计信息
    print(f"\n{'='*60}")
    print(f"数据下载完成:")
    print(f"  总计: {stats['total']} 只股票")
    print(f"  成功: {stats['success']}")
    print(f"  失败: {stats['failed']}")
    print(f"  跳过: {stats['skipped']}")
    print(f"{'='*60}")

    return 0


if __name__ == '__main__':
    exit(main())
