"""
股票名称查找工具（从数据库读取）
"""
from sqlalchemy import create_engine, text
from config.settings import TUSHARE_DB_PATH
from pathlib import Path
import pandas as pd

# 缓存所有股票代码和名称
_stock_names_cache = None

# 缓存所有指数代码和名称
_index_names_cache = None

# 缓存 DuckDB 中的股票代码
_duckdb_symbols_cache = None


def _load_duckdb_symbols():
    """从 DuckDB 加载所有已导入的股票代码和名称"""
    global _duckdb_symbols_cache

    if _duckdb_symbols_cache is not None:
        return _duckdb_symbols_cache

    try:
        from src.db.duckdb_manager import get_duckdb_manager

        db_manager = get_duckdb_manager()

        # 使用 context manager 避免持久连接占用锁
        with db_manager.get_connection() as conn:
            # 从所有时间周期表中获取股票代码
            all_symbols = set()  # 存储 (stock_code, exchange) 元组
            # 包含A股表和港股表
            tables = ['bars_a_1d', 'bars_1d', 'bars_5m', 'bars_15m', 'bars_30m', 'bars_60m']

            for table in tables:
                try:
                    result = conn.execute(
                        f"SELECT DISTINCT stock_code, exchange FROM {table}"
                    ).fetchdf()
                    if not result.empty:
                        for _, row in result.iterrows():
                            stock_code = row['stock_code']
                            exchange = row['exchange']
                            # 组合成 ts_code 格式
                            if pd.notna(exchange):
                                ts_code = f"{stock_code}.{exchange}"
                            else:
                                ts_code = stock_code
                            all_symbols.add(ts_code)
                except Exception:
                    pass  # 表可能不存在

            # 尝试从 hk_stock_list 表获取港股名称
            symbol_names = {}
            try:
                # 检查 hk_stock_list 表是否存在
                tables_result = conn.execute("SHOW TABLES").fetchdf()
                if 'hk_stock_list' in tables_result['name'].values:
                    # 获取所有港股的代码和名称
                    hk_result = conn.execute(
                        "SELECT ts_code, name FROM hk_stock_list"
                    ).fetchdf()
                    if not hk_result.empty:
                        symbol_names = dict(zip(hk_result['ts_code'], hk_result['name']))
                        print(f"✅ 从 hk_stock_list 加载了 {len(symbol_names)} 个港股名称")
            except Exception as e:
                print(f"⚠️  从 hk_stock_list 加载港股名称失败: {e}")

            # 构建最终字典：优先使用hk_stock_list中的名称，否则使用代码本身
            _duckdb_symbols_cache = {}
            for symbol in all_symbols:
                _duckdb_symbols_cache[symbol] = symbol_names.get(symbol, symbol)

            if _duckdb_symbols_cache:
                print(f"✅ 从 DuckDB 加载了 {len(_duckdb_symbols_cache)} 个股票代码")

    except Exception as e:
        print(f"⚠️  从 DuckDB 加载股票代码失败: {e}")
        _duckdb_symbols_cache = {}

    return _duckdb_symbols_cache


def _load_stock_names():
    """从数据库加载所有股票代码和名称"""
    global _stock_names_cache

    if _stock_names_cache is not None:
        return _stock_names_cache

    db_path = TUSHARE_DB_PATH
    if not db_path.exists():
        print("❌ 数据库文件不存在")
        _stock_names_cache = {}
        return _stock_names_cache

    try:
        engine = create_engine(f"sqlite:///{db_path}", echo=False)

        # 从数据库读取所有股票名称
        query = "SELECT code, name FROM stock_names ORDER BY code"
        with engine.connect() as conn:
            result = conn.execute(text(query))
            _stock_names_cache = {row[0]: row[1] for row in result}

        print(f"✅ 从数据库加载了 {len(_stock_names_cache)} 支股票名称")

    except Exception as e:
        print(f"❌ 从数据库加载股票名称失败: {e}")
        _stock_names_cache = {}

    return _stock_names_cache


def _load_index_names():
    """从数据库加载所有指数代码和名称"""
    global _index_names_cache

    if _index_names_cache is not None:
        return _index_names_cache

    db_path = TUSHARE_DB_PATH
    if not db_path.exists():
        _index_names_cache = {}
        return _index_names_cache

    try:
        engine = create_engine(f"sqlite:///{db_path}", echo=False)

        # 从数据库读取所有指数名称
        query = "SELECT ts_code, name FROM index_names ORDER BY ts_code"
        with engine.connect() as conn:
            result = conn.execute(text(query))
            _index_names_cache = {row[0]: row[1] for row in result}

        print(f"✅ 从数据库加载了 {len(_index_names_cache)} 个指数名称")

    except Exception as e:
        # 指数表可能不存在，返回空字典
        _index_names_cache = {}

    return _index_names_cache


def search_stocks(query: str, limit: int = 10, asset_type: str = 'all') -> list:
    """
    搜索股票和指数，支持代码和名称

    Args:
        query: 搜索关键词（代码或名称）
        limit: 返回结果数量
        asset_type: 资产类型 ('stock', 'index', 'all')

    Returns:
        资产列表 [{"code": "600382", "name": "广东明珠", "type": "stock"}, ...]
    """
    results = []
    matched_codes = set()  # 用于去重
    exact_matches = []  # 精确匹配的结果（优先显示）

    if not query:
        return results

    query = query.strip()
    # 只对纯代码进行大写转换（保留中文原样）
    if query and query[0].isdigit():  # 以数字开头，说明是股票代码
        query = query.upper()

    # 精确匹配的代码变体（支持前导零）
    exact_variants = [query]
    if query.isdigit():
        query_6digit = query.zfill(6)  # 补齐到6位
        if query_6digit != query:
            exact_variants.append(query_6digit)

    # 模糊匹配：数字输入时也允许匹配代码子串，但使用原始输入（不生成变体）
    # 这样 0031 可以匹配 00316.HK，但不会匹配 000531（因为 0031 不是 000531 的子串）
    fuzzy_query = query

    # 搜索股票（从 TushareDB）
    if asset_type in ['stock', 'all']:
        stock_names = _load_stock_names()
        for code, name in stock_names.items():
            # 先检查精确匹配（使用变体）
            is_exact = any(qv == code for qv in exact_variants)
            match_found = False

            if not is_exact:
                # 模糊匹配：名称和代码都支持子串匹配
                if fuzzy_query in name or fuzzy_query in code:
                    match_found = True

            if (is_exact or match_found) and code not in matched_codes:
                result = {"code": code, "name": name, "type": "stock"}
                if is_exact:
                    exact_matches.append(result)
                else:
                    results.append(result)
                matched_codes.add(code)

                if len(results) + len(exact_matches) >= limit:
                    break

    # 搜索从 DuckDB 导入的股票代码
    if asset_type in ['stock', 'all'] and len(results) + len(exact_matches) < limit:
        duckdb_symbols = _load_duckdb_symbols()
        for code, name in duckdb_symbols.items():
            # 避免重复添加
            if code not in matched_codes:
                # 提取不含交易所后缀的代码（00316.HK -> 00316）
                clean_code = code.split('.')[0] if '.' in code else code

                # 先检查精确匹配（使用变体）
                is_exact = any(qv == clean_code for qv in exact_variants)
                match_found = False

                if not is_exact:
                    # 模糊匹配：名称和代码都支持子串匹配
                    if fuzzy_query in name or fuzzy_query in clean_code:
                        match_found = True

                if (is_exact or match_found):
                    result = {"code": code, "name": name, "type": "stock"}
                    if is_exact:
                        exact_matches.append(result)
                    else:
                        results.append(result)
                    matched_codes.add(code)

                if len(results) + len(exact_matches) >= limit:
                    break

    # 精确匹配优先，然后是其他匹配
    results = exact_matches + results

    # 搜索指数
    if asset_type in ['index', 'all'] and len(results) < limit:
        index_names = _load_index_names()
        for code, name in index_names.items():
            if code in matched_codes:
                continue

            # 转换 ts_code 格式 (000001.SH -> 000001)
            clean_code = code.split('.')[0] if '.' in code else code

            # 先检查精确匹配（使用变体）
            is_exact = any(qv == clean_code for qv in exact_variants)
            match_found = False

            if not is_exact:
                # 模糊匹配：名称和代码都支持子串匹配
                if fuzzy_query in name or fuzzy_query in code or fuzzy_query in clean_code:
                    match_found = True

            if match_found:
                # 对指数保留完整的 ts_code 格式 (如 000001.SH)，以避免与股票代码冲突
                results.append({"code": code, "name": name, "type": "index"})
                matched_codes.add(code)

            if len(results) >= limit:
                break

    return results[:limit]


def get_stock_name_from_code(code: str) -> str:
    """
    根据代码获取股票或指数名称

    Args:
        code: 股票代码或指数代码（指数应使用完整 ts_code 格式，如 000001.SH）

    Returns:
        股票/指数名称
    """
    try:
        # 确保code是字符串
        if not isinstance(code, str):
            code = str(code)

        clean_code = code.split('.')[0] if '.' in code else code

        # 如果代码包含交易所后缀（如 .SH 或 .SZ），优先从指数查找
        if '.' in code:
            index_names = _load_index_names()
            if isinstance(index_names, dict) and code in index_names:
                return index_names[code]

        # 再尝试从股票名称获取
        stock_names = _load_stock_names()
        if isinstance(stock_names, dict) and clean_code in stock_names:
            return stock_names[clean_code]

        # 最后尝试从指数名称获取（处理不带后缀的指数代码）
        index_names = _load_index_names()
        if isinstance(index_names, dict):
            if clean_code in index_names:
                return index_names[clean_code]

        # 最后尝试从 DuckDB 获取
        duckdb_symbols = _load_duckdb_symbols()
        if isinstance(duckdb_symbols, dict):
            # 先尝试精确匹配
            if code in duckdb_symbols:
                return duckdb_symbols[code]
            # 对于不带后缀的代码，尝试匹配带后缀的港股代码
            # 例如: 00857 -> 00857.HK
            if '.' not in code:
                possible_codes = [f"{code}.HK", f"{code}.SH", f"{code}.SZ"]
                for possible_code in possible_codes:
                    if possible_code in duckdb_symbols:
                        return duckdb_symbols[possible_code]

        return code  # 如果找不到，返回代码本身
    except Exception as e:
        print(f"⚠️  获取股票名称失败 ({code}): {e}")
        return code
