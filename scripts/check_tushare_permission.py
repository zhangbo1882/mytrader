#!/usr/bin/env python3
"""
检查 Tushare API 权限
"""
import sys
sys.path.insert(0, '/Users/zhangbo/Public/go/github.com/mytrader')

def check_permission():
    try:
        import tushare as ts
        from config.settings import TUSHARE_TOKEN

        print("=" * 60)
        print("检查 Tushare API 权限")
        print("=" * 60)

        pro = ts.pro_api(TUSHARE_TOKEN)

        # 1. 测试 basic 接口（免费）
        print("\n1. 测试 stock_basic 接口（免费）...")
        try:
            basic = pro.stock_basic(ts_code='600382.SH', fields='ts_code,name')
            if basic is not None and not basic.empty:
                print(f"   ✓ stock_basic 接口正常")
                print(f"   股票名称: {basic['name'].iloc[0]}")
            else:
                print(f"   ✗ stock_basic 接口返回空数据")
        except Exception as e:
            print(f"   ✗ stock_basic 接口失败: {e}")

        # 2. 测试 daily 接口（免费）
        print("\n2. 测试 daily 接口（免费）...")
        try:
            daily = pro.daily(ts_code='600382.SH', start_date='20260128', end_date='20260128')
            if daily is not None and not daily.empty:
                print(f"   ✓ daily 接口正常")
                print(f"   收盘价: {daily['close'].iloc[0]}")
            else:
                print(f"   ✗ daily 接口返回空数据")
        except Exception as e:
            print(f"   ✗ daily 接口失败: {e}")

        # 3. 测试 daily_basic 接口（需要积分）
        print("\n3. 测试 daily_basic 接口（需要2000+积分）...")
        try:
            basic_data = pro.daily_basic(
                ts_code='600382.SH',
                start_date='20260128',
                end_date='20260128',
                fields='ts_code,trade_date,turnover_rate'
            )

            if basic_data is not None and not basic_data.empty:
                print(f"   ✓ daily_basic 接口正常")
                print(f"   返回列: {list(basic_data.columns)}")
                print(f"   数据行数: {len(basic_data)}")
                if 'ts_code' in basic_data.columns:
                    print(f"   ✓ 有 ts_code 列")
                else:
                    print(f"   ✗ 缺少 ts_code 列！")
                    print(f"   返回的数据: {basic_data.head()}")
            else:
                print(f"   ✗ daily_basic 接口返回空数据")
                print(f"   这可能意味着:")
                print(f"     - 积分不足（需要2000+积分）")
                print(f"     - 没有该接口的权限")

        except Exception as e:
            error_msg = str(e)
            print(f"   ✗ daily_basic 接口失败: {error_msg}")

            if "无权限" in error_msg or "权限" in error_msg:
                print(f"   原因: 账户权限不足")
            elif "403" in error_msg:
                print(f"   原因: 403 Forbidden - 积分或权限不足")
            elif "积分" in error_msg:
                print(f"   原因: 积分不足")

        print("\n" + "=" * 60)
        print("检查完成")
        print("=" * 60)
        print("\n建议:")
        print("- 如果 daily_basic 失败，请访问 https://tushare.pro")
        print("- 登录后查看您的积分和接口权限")
        print("- daily_basic 接口需要 2000+ 积分")

    except ImportError as e:
        print(f"错误: 无法导入 tushare 模块")
        print(f"请安装: pip install tushare")
        print(f"错误详情: {e}")

if __name__ == '__main__':
    check_permission()
