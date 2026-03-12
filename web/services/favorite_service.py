"""
Favorites service - 收藏功能服务层
"""

from flask import request
from sqlalchemy import text
from src.utils.stock_lookup import get_stock_name_from_code
from config.settings import TUSHARE_DB_PATH
from src.data_sources.tushare import TushareDB
import sqlite3

# 数据库连接（懒加载）
_db = None


def get_db():
    """获取数据库连接（懒加载）"""
    global _db
    if _db is None:
        try:
            _db = TushareDB(token="", db_path=str(TUSHARE_DB_PATH))
            # 初始化表结构（添加新字段）
            init_favorites_table(_db)
        except Exception as e:
            print(f"Warning: Failed to initialize database: {e}")
            _db = False
    return _db


def init_favorites_table(db):
    """确保 favorites 表有所有需要的字段"""
    if not db:
        return

    try:
        with db.engine.connect() as conn:
            # 检查并添加新字段
            columns = [
                "safety_rating TEXT",
                "fundamental_rating TEXT",
                "entry_price REAL",
                "urgency INTEGER",
                "fair_value REAL",
                "upside_downside REAL",
                "valuation_date TEXT",
                "valuation_confidence REAL",
            ]
            for col_def in columns:
                col_name = col_def.split()[0]
                try:
                    conn.execute(text(f"ALTER TABLE favorites ADD COLUMN {col_def}"))
                    conn.commit()
                    print(f"Added column {col_name} to favorites table")
                except Exception:
                    pass  # 字段已存在
    except Exception as e:
        print(f"Warning: Failed to init favorites table: {e}")


def _get_stock_industry_info(conn, stock_code: str) -> dict:
    """
    获取股票的申万行业信息

    Args:
        conn: 数据库连接
        stock_code: 股票代码（如 600382 或 00762.HK）

    Returns:
        dict: {'sw_l1': str, 'sw_l2': str, 'sw_l3': str}
    """
    code_6digit = stock_code[:6] if len(stock_code) >= 6 else stock_code

    try:
        query = """
        SELECT
            COALESCE(sc_l1.industry_name, sc_l1_from_l2.industry_name, sc_l1_direct.industry_name) as sw_l1,
            COALESCE(sc_l2.industry_name, sc_l2_direct.industry_name) as sw_l2,
            sc_l3.industry_name as sw_l3
        FROM sw_members swm
        LEFT JOIN sw_classify sc_l3 
            ON swm.index_code = sc_l3.index_code AND sc_l3.level = 'L3'
        LEFT JOIN sw_classify sc_l2 
            ON sc_l3.parent_code = sc_l2.industry_code
        LEFT JOIN sw_classify sc_l2_direct 
            ON swm.index_code = sc_l2_direct.index_code AND sc_l2_direct.level = 'L2'
        LEFT JOIN sw_classify sc_l1 
            ON sc_l2.parent_code = sc_l1.industry_code
        LEFT JOIN sw_classify sc_l1_from_l2 
            ON sc_l2_direct.parent_code = sc_l1_from_l2.industry_code
        LEFT JOIN sw_classify sc_l1_direct 
            ON swm.index_code = sc_l1_direct.index_code AND sc_l1_direct.level = 'L1'
        WHERE SUBSTR(swm.ts_code, 1, 6) = :code
          AND (swm.out_date IS NULL OR swm.out_date > date('now'))
        ORDER BY swm.in_date DESC
        LIMIT 1
        """

        result = conn.execute(text(query), {"code": code_6digit})
        row = result.fetchone()

        if row:
            return {
                "sw_l1": row[0] if row[0] else None,
                "sw_l2": row[1] if row[1] else None,
                "sw_l3": row[2] if row[2] else None,
            }
    except Exception as e:
        print(f"Warning: Failed to get industry info for {stock_code}: {e}")

    return {"sw_l1": None, "sw_l2": None, "sw_l3": None}


def list_favorites():
    """
    获取收藏列表

    Query params:
        user_id: 用户ID（可选，默认 'default'）
    """
    db = get_db()
    if not db:
        return {"error": "数据库连接失败"}, 500

    try:
        user_id = request.args.get("user_id", "default")

        with db.engine.connect() as conn:
            result = conn.execute(
                text("""
                SELECT id, stock_code, stock_name, added_at, notes,
                       safety_rating, fundamental_rating, entry_price, urgency,
                       fair_value, upside_downside, valuation_date, valuation_confidence
                FROM favorites
                WHERE user_id = :user_id
                ORDER BY added_at DESC
            """),
                {"user_id": user_id},
            )

            favorites = []
            for row in result:
                stock_code = row[1]
                industry_info = _get_stock_industry_info(conn, stock_code)

                favorites.append(
                    {
                        "id": row[0],
                        "stock_code": stock_code,
                        "stock_name": row[2],
                        "added_at": row[3],
                        "notes": row[4],
                        "safety_rating": row[5],
                        "fundamental_rating": row[6],
                        "entry_price": row[7],
                        "urgency": row[8],
                        "fair_value": row[9],
                        "upside_downside": row[10],
                        "valuation_date": row[11],
                        "valuation_confidence": row[12],
                        "sw_l1": industry_info["sw_l1"],
                        "sw_l2": industry_info["sw_l2"],
                        "sw_l3": industry_info["sw_l3"],
                    }
                )

            return {"favorites": favorites, "total": len(favorites)}, 200

    except Exception as e:
        return {"error": f"查询收藏失败: {str(e)}"}, 500


def add_favorite():
    """
    添加收藏

    Body (JSON):
        stock_code: 股票代码
        notes: 备注（可选）
        safety_rating: 安全性评级（可选）
        fundamental_rating: 基本面评级（可选）
        entry_price: 进场价格（可选）
        urgency: 紧急程度1-5（可选）
    """
    db = get_db()
    if not db:
        return {"error": "数据库连接失败"}, 500

    try:
        data = request.json
        if not data:
            return {"error": "请求数据为空"}, 400

        stock_code = data.get("stock_code", "").strip()
        if not stock_code:
            return {"error": "股票代码不能为空"}, 400

        # 验证股票代码并获取名称
        stock_name = get_stock_name_from_code(stock_code)
        if not stock_name:
            return {"error": "无效的股票代码"}, 400

        user_id = request.args.get("user_id", "default")
        notes = data.get("notes", "")
        safety_rating = data.get("safety_rating")
        fundamental_rating = data.get("fundamental_rating")
        entry_price = data.get("entry_price")
        urgency = data.get("urgency")

        from datetime import datetime

        added_at = datetime.now().isoformat()

        try:
            with db.engine.connect() as conn:
                result = conn.execute(
                    text("""
                    INSERT INTO favorites (user_id, stock_code, stock_name, added_at, notes,
                                           safety_rating, fundamental_rating, entry_price, urgency)
                    VALUES (:user_id, :stock_code, :stock_name, :added_at, :notes,
                            :safety_rating, :fundamental_rating, :entry_price, :urgency)
                """),
                    {
                        "user_id": user_id,
                        "stock_code": stock_code,
                        "stock_name": stock_name,
                        "added_at": added_at,
                        "notes": notes,
                        "safety_rating": safety_rating,
                        "fundamental_rating": fundamental_rating,
                        "entry_price": entry_price,
                        "urgency": urgency,
                    },
                )
                conn.commit()

                return {
                    "id": result.lastrowid,
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "added_at": added_at,
                    "notes": notes,
                    "safety_rating": safety_rating,
                    "fundamental_rating": fundamental_rating,
                    "entry_price": entry_price,
                    "urgency": urgency,
                }, 201

        except sqlite3.IntegrityError:
            return {"error": "该股票已在收藏列表中"}, 409

    except Exception as e:
        return {"error": f"添加收藏失败: {str(e)}"}, 500


def remove_favorite(stock_code):
    """
    删除收藏

    Path params:
        stock_code: 股票代码
    """
    db = get_db()
    if not db:
        return {"error": "数据库连接失败"}, 500

    try:
        user_id = request.args.get("user_id", "default")

        with db.engine.connect() as conn:
            result = conn.execute(
                text("""
                DELETE FROM favorites
                WHERE user_id = :user_id AND stock_code = :stock_code
            """),
                {"user_id": user_id, "stock_code": stock_code},
            )
            conn.commit()

            if result.rowcount == 0:
                return {"error": "收藏不存在"}, 404

            return {"message": "删除成功"}, 200

    except Exception as e:
        return {"error": f"删除收藏失败: {str(e)}"}, 500


def check_favorite(stock_code):
    """
    检查是否已收藏

    Path params:
        stock_code: 股票代码
    """
    db = get_db()
    if not db:
        return {"error": "数据库连接失败"}, 500

    try:
        user_id = request.args.get("user_id", "default")

        with db.engine.connect() as conn:
            result = conn.execute(
                text("""
                SELECT id, stock_code, stock_name, added_at, notes,
                       safety_rating, fundamental_rating, entry_price, urgency
                FROM favorites
                WHERE user_id = :user_id AND stock_code = :stock_code
            """),
                {"user_id": user_id, "stock_code": stock_code},
            )

            row = result.fetchone()
            if row:
                return {
                    "is_favorite": True,
                    "favorite": {
                        "id": row[0],
                        "stock_code": row[1],
                        "stock_name": row[2],
                        "added_at": row[3],
                        "notes": row[4],
                        "safety_rating": row[5],
                        "fundamental_rating": row[6],
                        "entry_price": row[7],
                        "urgency": row[8],
                    },
                }, 200
            else:
                return {"is_favorite": False, "favorite": None}, 200

    except Exception as e:
        return {"error": f"检查收藏失败: {str(e)}"}, 500


def clear_favorites():
    """
    清空收藏

    Query params:
        user_id: 用户ID（可选，默认 'default'）
    """
    db = get_db()
    if not db:
        return {"error": "数据库连接失败"}, 500

    try:
        user_id = request.args.get("user_id", "default")

        with db.engine.connect() as conn:
            # 先获取数量
            count_result = conn.execute(
                text("""
                SELECT COUNT(*) FROM favorites WHERE user_id = :user_id
            """),
                {"user_id": user_id},
            )
            count = count_result.fetchone()[0]

            # 删除
            conn.execute(
                text("""
                DELETE FROM favorites WHERE user_id = :user_id
            """),
                {"user_id": user_id},
            )
            conn.commit()

            return {"message": f"已清空 {count} 条收藏"}, 200

    except Exception as e:
        return {"error": f"清空收藏失败: {str(e)}"}, 500


def batch_add_favorites():
    """
    批量添加收藏

    Body (JSON):
        stock_codes: 股票代码列表（兼容旧格式）
        stocks_data: 股票数据列表（新格式，包含额外字段）
            [{code, safety_rating, fundamental_rating, entry_price, urgency}, ...]
        notes: 备注（可选）
    """
    db = get_db()
    if not db:
        return {"error": "数据库连接失败"}, 500

    try:
        data = request.json
        if not data:
            return {"error": "请求数据为空"}, 400

        # 支持两种格式：旧格式 stock_codes 列表，新格式 stocks_data 列表
        stocks_data = data.get("stocks_data", [])
        stock_codes = data.get("stock_codes", [])

        # 如果没有 stocks_data，从 stock_codes 构建
        if not stocks_data and stock_codes:
            stocks_data = [{"code": code} for code in stock_codes]

        if not stocks_data:
            return {"error": "股票数据不能为空"}, 400

        user_id = request.args.get("user_id", "default")
        notes = data.get("notes", "")

        from datetime import datetime

        added_at = datetime.now().isoformat()

        # 先查询已存在的股票代码
        with db.engine.connect() as conn:
            existing_result = conn.execute(
                text("""
                SELECT stock_code FROM favorites WHERE user_id = :user_id
            """),
                {"user_id": user_id},
            )
            existing_codes = {row[0] for row in existing_result}

        results = []
        success = 0
        failed = 0
        updated = 0

        for stock_item in stocks_data:
            # 支持字符串格式（旧格式）和对象格式（新格式）
            if isinstance(stock_item, str):
                stock_code = stock_item.strip()
                safety_rating = None
                fundamental_rating = None
                entry_price = None
                urgency = None
            else:
                stock_code = stock_item.get("code", "").strip()
                if not stock_code:
                    stock_code = stock_item.get("stock_code", "").strip()
                safety_rating = stock_item.get("safety_rating")
                fundamental_rating = stock_item.get("fundamental_rating")
                entry_price = stock_item.get("entry_price")
                urgency = stock_item.get("urgency")

            if not stock_code:
                failed += 1
                results.append(
                    {
                        "stock_code": stock_code,
                        "success": False,
                        "error": "股票代码为空",
                    }
                )
                continue

            # 验证股票代码并获取名称
            stock_name = get_stock_name_from_code(stock_code)
            if not stock_name:
                failed += 1
                results.append(
                    {
                        "stock_code": stock_code,
                        "success": False,
                        "error": "无效的股票代码",
                    }
                )
                continue

            try:
                with db.engine.connect() as conn:
                    # 检查是否已存在
                    if stock_code in existing_codes:
                        # 已存在，更新记录
                        conn.execute(
                            text("""
                            UPDATE favorites
                            SET safety_rating = :safety_rating,
                                fundamental_rating = :fundamental_rating,
                                entry_price = :entry_price,
                                urgency = :urgency
                            WHERE user_id = :user_id AND stock_code = :stock_code
                        """),
                            {
                                "user_id": user_id,
                                "stock_code": stock_code,
                                "safety_rating": safety_rating,
                                "fundamental_rating": fundamental_rating,
                                "entry_price": entry_price,
                                "urgency": urgency,
                            },
                        )
                        conn.commit()

                        updated += 1
                        results.append(
                            {
                                "stock_code": stock_code,
                                "stock_name": stock_name,
                                "success": True,
                                "updated": True,
                            }
                        )
                    else:
                        # 不存在，插入新记录
                        conn.execute(
                            text("""
                            INSERT INTO favorites (user_id, stock_code, stock_name, added_at, notes,
                                                   safety_rating, fundamental_rating, entry_price, urgency)
                            VALUES (:user_id, :stock_code, :stock_name, :added_at, :notes,
                                    :safety_rating, :fundamental_rating, :entry_price, :urgency)
                        """),
                            {
                                "user_id": user_id,
                                "stock_code": stock_code,
                                "stock_name": stock_name,
                                "added_at": added_at,
                                "notes": notes,
                                "safety_rating": safety_rating,
                                "fundamental_rating": fundamental_rating,
                                "entry_price": entry_price,
                                "urgency": urgency,
                            },
                        )
                        conn.commit()

                        success += 1
                        existing_codes.add(
                            stock_code
                        )  # 添加到已存在集合，防止同一批次重复
                        results.append(
                            {
                                "stock_code": stock_code,
                                "stock_name": stock_name,
                                "success": True,
                            }
                        )

            except sqlite3.IntegrityError:
                # 并发插入导致的唯一约束冲突，尝试更新
                with db.engine.connect() as conn:
                    conn.execute(
                        text("""
                        UPDATE favorites
                        SET safety_rating = :safety_rating,
                            fundamental_rating = :fundamental_rating,
                            entry_price = :entry_price,
                            urgency = :urgency
                        WHERE user_id = :user_id AND stock_code = :stock_code
                    """),
                        {
                            "user_id": user_id,
                            "stock_code": stock_code,
                            "safety_rating": safety_rating,
                            "fundamental_rating": fundamental_rating,
                            "entry_price": entry_price,
                            "urgency": urgency,
                        },
                    )
                    conn.commit()

                updated += 1
                results.append(
                    {
                        "stock_code": stock_code,
                        "stock_name": stock_name,
                        "success": True,
                        "updated": True,
                    }
                )

        return {
            "success": success,
            "updated": updated,
            "failed": failed,
            "total": len(stocks_data),
            "results": results,
        }, 200

    except Exception as e:
        return {"error": f"批量添加收藏失败: {str(e)}"}, 500


def update_favorite(stock_code):
    """
    更新收藏的附加字段

    Path params:
        stock_code: 股票代码

    Body (JSON):
        safety_rating: 安全性评级（可选）
        fundamental_rating: 基本面评级（可选）
        entry_price: 进场价格（可选）
        urgency: 紧急程度1-5（可选）
        notes: 备注（可选）
    """
    db = get_db()
    if not db:
        return {"error": "数据库连接失败"}, 500

    try:
        data = request.json
        if not data:
            return {"error": "请求数据为空"}, 400

        user_id = request.args.get("user_id", "default")

        # 构建更新字段
        update_fields = []
        params = {"user_id": user_id, "stock_code": stock_code}

        if "safety_rating" in data:
            update_fields.append("safety_rating = :safety_rating")
            params["safety_rating"] = data["safety_rating"]

        if "fundamental_rating" in data:
            update_fields.append("fundamental_rating = :fundamental_rating")
            params["fundamental_rating"] = data["fundamental_rating"]

        if "entry_price" in data:
            update_fields.append("entry_price = :entry_price")
            params["entry_price"] = data["entry_price"]

        if "urgency" in data:
            update_fields.append("urgency = :urgency")
            params["urgency"] = data["urgency"]

        if "notes" in data:
            update_fields.append("notes = :notes")
            params["notes"] = data["notes"]

        if "fair_value" in data:
            update_fields.append("fair_value = :fair_value")
            params["fair_value"] = data["fair_value"]

        if "upside_downside" in data:
            update_fields.append("upside_downside = :upside_downside")
            params["upside_downside"] = data["upside_downside"]

        if "valuation_date" in data:
            update_fields.append("valuation_date = :valuation_date")
            params["valuation_date"] = data["valuation_date"]

        if "valuation_confidence" in data:
            update_fields.append("valuation_confidence = :valuation_confidence")
            params["valuation_confidence"] = data["valuation_confidence"]

        if not update_fields:
            return {"error": "没有要更新的字段"}, 400

        with db.engine.connect() as conn:
            result = conn.execute(
                text(f"""
                UPDATE favorites
                SET {", ".join(update_fields)}
                WHERE user_id = :user_id AND stock_code = :stock_code
            """),
                params,
            )
            conn.commit()

            if result.rowcount == 0:
                return {"error": "收藏不存在"}, 404

            # 返回更新后的数据
            select_result = conn.execute(
                text("""
                SELECT id, stock_code, stock_name, added_at, notes,
                       safety_rating, fundamental_rating, entry_price, urgency
                FROM favorites
                WHERE user_id = :user_id AND stock_code = :stock_code
            """),
                {"user_id": user_id, "stock_code": stock_code},
            )

            row = select_result.fetchone()
            return {
                "id": row[0],
                "stock_code": row[1],
                "stock_name": row[2],
                "added_at": row[3],
                "notes": row[4],
                "safety_rating": row[5],
                "fundamental_rating": row[6],
                "entry_price": row[7],
                "urgency": row[8],
            }, 200

    except Exception as e:
        return {"error": f"更新收藏失败: {str(e)}"}, 500


