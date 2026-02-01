"""
股票名称查找工具（从数据库读取）
"""
from sqlalchemy import create_engine, text
from config.settings import TUSHARE_DB_PATH
from pathlib import Path

# 缓存所有股票代码和名称
_stock_names_cache = None


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


def search_stocks(query: str, limit: int = 10) -> list:
    """
    搜索股票，支持代码和名称

    Args:
        query: 搜索关键词（代码或名称）
        limit: 返回结果数量

    Returns:
        股票列表 [{"code": "600382", "name": "广东明珠"}, ...]
    """
    results = []

    if not query:
        return results

    query = query.strip().upper()

    # 从数据库加载的股票中搜索
    stock_names = _load_stock_names()

    for code, name in stock_names.items():
        if query in code or query in name:
            results.append({"code": code, "name": name})

        if len(results) >= limit:
            break

    return results[:limit]


def get_stock_name_from_code(code: str) -> str:
    """
    根据代码获取股票名称

    Args:
        code: 股票代码

    Returns:
        股票名称
    """
    try:
        # 确保code是字符串
        if not isinstance(code, str):
            code = str(code)

        # 从数据库缓存中获取
        stock_names = _load_stock_names()

        # 确保返回的是字典类型
        if not isinstance(stock_names, dict):
            print(f"⚠️  股票名称缓存类型错误: {type(stock_names)}，重新加载...")
            global _stock_names_cache
            _stock_names_cache = None
            stock_names = _load_stock_names()

        # 再次检查
        if not isinstance(stock_names, dict):
            print(f"⚠️  股票名称缓存仍然不是字典，返回代码本身")
            return code

        clean_code = code.split('.')[0] if '.' in code else code

        if clean_code in stock_names:
            return stock_names[clean_code]

        return code  # 如果找不到，返回代码本身
    except Exception as e:
        print(f"⚠️  获取股票名称失败 ({code}): {e}")
        return code
