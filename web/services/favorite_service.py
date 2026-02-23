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
                'safety_rating TEXT',
                'fundamental_rating TEXT',
                'entry_price REAL'
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


def list_favorites():
    """
    获取收藏列表

    Query params:
        user_id: 用户ID（可选，默认 'default'）
    """
    db = get_db()
    if not db:
        return {'error': '数据库连接失败'}, 500

    try:
        user_id = request.args.get('user_id', 'default')

        with db.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT id, stock_code, stock_name, added_at, notes,
                       safety_rating, fundamental_rating, entry_price
                FROM favorites
                WHERE user_id = :user_id
                ORDER BY added_at DESC
            """), {"user_id": user_id})

            favorites = []
            for row in result:
                favorites.append({
                    'id': row[0],
                    'stock_code': row[1],
                    'stock_name': row[2],
                    'added_at': row[3],
                    'notes': row[4],
                    'safety_rating': row[5],
                    'fundamental_rating': row[6],
                    'entry_price': row[7]
                })

            return {
                'favorites': favorites,
                'total': len(favorites)
            }, 200

    except Exception as e:
        return {'error': f'查询收藏失败: {str(e)}'}, 500


def add_favorite():
    """
    添加收藏

    Body (JSON):
        stock_code: 股票代码
        notes: 备注（可选）
        safety_rating: 安全性评级（可选）
        fundamental_rating: 基本面评级（可选）
        entry_price: 进场价格（可选）
    """
    db = get_db()
    if not db:
        return {'error': '数据库连接失败'}, 500

    try:
        data = request.json
        if not data:
            return {'error': '请求数据为空'}, 400

        stock_code = data.get('stock_code', '').strip()
        if not stock_code:
            return {'error': '股票代码不能为空'}, 400

        # 验证股票代码并获取名称
        stock_name = get_stock_name_from_code(stock_code)
        if not stock_name:
            return {'error': '无效的股票代码'}, 400

        user_id = request.args.get('user_id', 'default')
        notes = data.get('notes', '')
        safety_rating = data.get('safety_rating')
        fundamental_rating = data.get('fundamental_rating')
        entry_price = data.get('entry_price')

        from datetime import datetime
        added_at = datetime.now().isoformat()

        try:
            with db.engine.connect() as conn:
                result = conn.execute(text("""
                    INSERT INTO favorites (user_id, stock_code, stock_name, added_at, notes,
                                           safety_rating, fundamental_rating, entry_price)
                    VALUES (:user_id, :stock_code, :stock_name, :added_at, :notes,
                            :safety_rating, :fundamental_rating, :entry_price)
                """), {
                    "user_id": user_id,
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "added_at": added_at,
                    "notes": notes,
                    "safety_rating": safety_rating,
                    "fundamental_rating": fundamental_rating,
                    "entry_price": entry_price
                })
                conn.commit()

                return {
                    'id': result.lastrowid,
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'added_at': added_at,
                    'notes': notes,
                    'safety_rating': safety_rating,
                    'fundamental_rating': fundamental_rating,
                    'entry_price': entry_price
                }, 201

        except sqlite3.IntegrityError:
            return {'error': '该股票已在收藏列表中'}, 409

    except Exception as e:
        return {'error': f'添加收藏失败: {str(e)}'}, 500


def remove_favorite(stock_code):
    """
    删除收藏

    Path params:
        stock_code: 股票代码
    """
    db = get_db()
    if not db:
        return {'error': '数据库连接失败'}, 500

    try:
        user_id = request.args.get('user_id', 'default')

        with db.engine.connect() as conn:
            result = conn.execute(text("""
                DELETE FROM favorites
                WHERE user_id = :user_id AND stock_code = :stock_code
            """), {
                "user_id": user_id,
                "stock_code": stock_code
            })
            conn.commit()

            if result.rowcount == 0:
                return {'error': '收藏不存在'}, 404

            return {'message': '删除成功'}, 200

    except Exception as e:
        return {'error': f'删除收藏失败: {str(e)}'}, 500


def check_favorite(stock_code):
    """
    检查是否已收藏

    Path params:
        stock_code: 股票代码
    """
    db = get_db()
    if not db:
        return {'error': '数据库连接失败'}, 500

    try:
        user_id = request.args.get('user_id', 'default')

        with db.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT id, stock_code, stock_name, added_at, notes,
                       safety_rating, fundamental_rating, entry_price
                FROM favorites
                WHERE user_id = :user_id AND stock_code = :stock_code
            """), {
                "user_id": user_id,
                "stock_code": stock_code
            })

            row = result.fetchone()
            if row:
                return {
                    'is_favorite': True,
                    'favorite': {
                        'id': row[0],
                        'stock_code': row[1],
                        'stock_name': row[2],
                        'added_at': row[3],
                        'notes': row[4],
                        'safety_rating': row[5],
                        'fundamental_rating': row[6],
                        'entry_price': row[7]
                    }
                }, 200
            else:
                return {'is_favorite': False, 'favorite': None}, 200

    except Exception as e:
        return {'error': f'检查收藏失败: {str(e)}'}, 500


def clear_favorites():
    """
    清空收藏

    Query params:
        user_id: 用户ID（可选，默认 'default'）
    """
    db = get_db()
    if not db:
        return {'error': '数据库连接失败'}, 500

    try:
        user_id = request.args.get('user_id', 'default')

        with db.engine.connect() as conn:
            # 先获取数量
            count_result = conn.execute(text("""
                SELECT COUNT(*) FROM favorites WHERE user_id = :user_id
            """), {"user_id": user_id})
            count = count_result.fetchone()[0]

            # 删除
            conn.execute(text("""
                DELETE FROM favorites WHERE user_id = :user_id
            """), {"user_id": user_id})
            conn.commit()

            return {'message': f'已清空 {count} 条收藏'}, 200

    except Exception as e:
        return {'error': f'清空收藏失败: {str(e)}'}, 500


def batch_add_favorites():
    """
    批量添加收藏

    Body (JSON):
        stock_codes: 股票代码列表（兼容旧格式）
        stocks_data: 股票数据列表（新格式，包含额外字段）
            [{code, safety_rating, fundamental_rating, entry_price}, ...]
        notes: 备注（可选）
    """
    db = get_db()
    if not db:
        return {'error': '数据库连接失败'}, 500

    try:
        data = request.json
        if not data:
            return {'error': '请求数据为空'}, 400

        # 支持两种格式：旧格式 stock_codes 列表，新格式 stocks_data 列表
        stocks_data = data.get('stocks_data', [])
        stock_codes = data.get('stock_codes', [])

        # 如果没有 stocks_data，从 stock_codes 构建
        if not stocks_data and stock_codes:
            stocks_data = [{'code': code} for code in stock_codes]

        if not stocks_data:
            return {'error': '股票数据不能为空'}, 400

        user_id = request.args.get('user_id', 'default')
        notes = data.get('notes', '')

        from datetime import datetime
        added_at = datetime.now().isoformat()

        # 先查询已存在的股票代码
        with db.engine.connect() as conn:
            existing_result = conn.execute(text("""
                SELECT stock_code FROM favorites WHERE user_id = :user_id
            """), {"user_id": user_id})
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
            else:
                stock_code = stock_item.get('code', '').strip()
                if not stock_code:
                    stock_code = stock_item.get('stock_code', '').strip()
                safety_rating = stock_item.get('safety_rating')
                fundamental_rating = stock_item.get('fundamental_rating')
                entry_price = stock_item.get('entry_price')

            if not stock_code:
                failed += 1
                results.append({
                    'stock_code': stock_code,
                    'success': False,
                    'error': '股票代码为空'
                })
                continue

            # 验证股票代码并获取名称
            stock_name = get_stock_name_from_code(stock_code)
            if not stock_name:
                failed += 1
                results.append({
                    'stock_code': stock_code,
                    'success': False,
                    'error': '无效的股票代码'
                })
                continue

            try:
                with db.engine.connect() as conn:
                    # 检查是否已存在
                    if stock_code in existing_codes:
                        # 已存在，更新记录
                        conn.execute(text("""
                            UPDATE favorites
                            SET safety_rating = :safety_rating,
                                fundamental_rating = :fundamental_rating,
                                entry_price = :entry_price
                            WHERE user_id = :user_id AND stock_code = :stock_code
                        """), {
                            "user_id": user_id,
                            "stock_code": stock_code,
                            "safety_rating": safety_rating,
                            "fundamental_rating": fundamental_rating,
                            "entry_price": entry_price
                        })
                        conn.commit()

                        updated += 1
                        results.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'success': True,
                            'updated': True
                        })
                    else:
                        # 不存在，插入新记录
                        conn.execute(text("""
                            INSERT INTO favorites (user_id, stock_code, stock_name, added_at, notes,
                                                   safety_rating, fundamental_rating, entry_price)
                            VALUES (:user_id, :stock_code, :stock_name, :added_at, :notes,
                                    :safety_rating, :fundamental_rating, :entry_price)
                        """), {
                            "user_id": user_id,
                            "stock_code": stock_code,
                            "stock_name": stock_name,
                            "added_at": added_at,
                            "notes": notes,
                            "safety_rating": safety_rating,
                            "fundamental_rating": fundamental_rating,
                            "entry_price": entry_price
                        })
                        conn.commit()

                        success += 1
                        existing_codes.add(stock_code)  # 添加到已存在集合，防止同一批次重复
                        results.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'success': True
                        })

            except sqlite3.IntegrityError:
                # 并发插入导致的唯一约束冲突，尝试更新
                with db.engine.connect() as conn:
                    conn.execute(text("""
                        UPDATE favorites
                        SET safety_rating = :safety_rating,
                            fundamental_rating = :fundamental_rating,
                            entry_price = :entry_price
                        WHERE user_id = :user_id AND stock_code = :stock_code
                    """), {
                        "user_id": user_id,
                        "stock_code": stock_code,
                        "safety_rating": safety_rating,
                        "fundamental_rating": fundamental_rating,
                        "entry_price": entry_price
                    })
                    conn.commit()

                updated += 1
                results.append({
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'success': True,
                    'updated': True
                })

        return {
            'success': success,
            'updated': updated,
            'failed': failed,
            'total': len(stocks_data),
            'results': results
        }, 200

    except Exception as e:
        return {'error': f'批量添加收藏失败: {str(e)}'}, 500


def update_favorite(stock_code):
    """
    更新收藏的附加字段

    Path params:
        stock_code: 股票代码

    Body (JSON):
        safety_rating: 安全性评级（可选）
        fundamental_rating: 基本面评级（可选）
        entry_price: 进场价格（可选）
        notes: 备注（可选）
    """
    db = get_db()
    if not db:
        return {'error': '数据库连接失败'}, 500

    try:
        data = request.json
        if not data:
            return {'error': '请求数据为空'}, 400

        user_id = request.args.get('user_id', 'default')

        # 构建更新字段
        update_fields = []
        params = {"user_id": user_id, "stock_code": stock_code}

        if 'safety_rating' in data:
            update_fields.append("safety_rating = :safety_rating")
            params['safety_rating'] = data['safety_rating']

        if 'fundamental_rating' in data:
            update_fields.append("fundamental_rating = :fundamental_rating")
            params['fundamental_rating'] = data['fundamental_rating']

        if 'entry_price' in data:
            update_fields.append("entry_price = :entry_price")
            params['entry_price'] = data['entry_price']

        if 'notes' in data:
            update_fields.append("notes = :notes")
            params['notes'] = data['notes']

        if not update_fields:
            return {'error': '没有要更新的字段'}, 400

        with db.engine.connect() as conn:
            result = conn.execute(text(f"""
                UPDATE favorites
                SET {', '.join(update_fields)}
                WHERE user_id = :user_id AND stock_code = :stock_code
            """), params)
            conn.commit()

            if result.rowcount == 0:
                return {'error': '收藏不存在'}, 404

            # 返回更新后的数据
            select_result = conn.execute(text("""
                SELECT id, stock_code, stock_name, added_at, notes,
                       safety_rating, fundamental_rating, entry_price
                FROM favorites
                WHERE user_id = :user_id AND stock_code = :stock_code
            """), {"user_id": user_id, "stock_code": stock_code})

            row = select_result.fetchone()
            return {
                'id': row[0],
                'stock_code': row[1],
                'stock_name': row[2],
                'added_at': row[3],
                'notes': row[4],
                'safety_rating': row[5],
                'fundamental_rating': row[6],
                'entry_price': row[7]
            }, 200

    except Exception as e:
        return {'error': f'更新收藏失败: {str(e)}'}, 500
