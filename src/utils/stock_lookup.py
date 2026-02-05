"""
股票名称查找工具（从数据库读取）
"""
from sqlalchemy import create_engine, text
from config.settings import TUSHARE_DB_PATH
from pathlib import Path

# 缓存所有股票代码和名称
_stock_names_cache = None

# 缓存所有指数代码和名称
_index_names_cache = None


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

    if not query:
        return results

    query = query.strip()
    # 只对纯代码进行大写转换（保留中文原样）
    if query and query[0].isdigit():  # 以数字开头，说明是股票代码
        query = query.upper()

    # 搜索股票
    if asset_type in ['stock', 'all']:
        stock_names = _load_stock_names()
        for code, name in stock_names.items():
            if query in code or query in name:
                results.append({"code": code, "name": name, "type": "stock"})

            if len(results) >= limit:
                break

    # 搜索指数
    if asset_type in ['index', 'all']:
        index_names = _load_index_names()
        for code, name in index_names.items():
            # 转换 ts_code 格式 (000001.SH -> 000001)
            clean_code = code.split('.')[0] if '.' in code else code
            if query in clean_code or query in name or query in code:
                # 对指数保留完整的 ts_code 格式 (如 000001.SH)，以避免与股票代码冲突
                results.append({"code": code, "name": name, "type": "index"})

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

        return code  # 如果找不到，返回代码本身
    except Exception as e:
        print(f"⚠️  获取股票名称失败 ({code}): {e}")
        return code
