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
        except Exception as e:
            print(f"Warning: Failed to initialize database: {e}")
            _db = False
    return _db


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
                SELECT id, stock_code, stock_name, added_at, notes
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
                    'notes': row[4]
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

        from datetime import datetime
        added_at = datetime.now().isoformat()

        try:
            with db.engine.connect() as conn:
                result = conn.execute(text("""
                    INSERT INTO favorites (user_id, stock_code, stock_name, added_at, notes)
                    VALUES (:user_id, :stock_code, :stock_name, :added_at, :notes)
                """), {
                    "user_id": user_id,
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "added_at": added_at,
                    "notes": notes
                })
                conn.commit()

                return {
                    'id': result.lastrowid,
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'added_at': added_at,
                    'notes': notes
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
                SELECT id, stock_code, stock_name, added_at, notes
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
                        'notes': row[4]
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
        stock_codes: 股票代码列表
        notes: 备注（可选）
    """
    db = get_db()
    if not db:
        return {'error': '数据库连接失败'}, 500

    try:
        data = request.json
        if not data:
            return {'error': '请求数据为空'}, 400

        stock_codes = data.get('stock_codes', [])
        if not stock_codes:
            return {'error': '股票代码列表不能为空'}, 400

        user_id = request.args.get('user_id', 'default')
        notes = data.get('notes', '')

        from datetime import datetime
        added_at = datetime.now().isoformat()

        results = []
        success = 0
        failed = 0

        for stock_code in stock_codes:
            stock_code = stock_code.strip()
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
                    conn.execute(text("""
                        INSERT INTO favorites (user_id, stock_code, stock_name, added_at, notes)
                        VALUES (:user_id, :stock_code, :stock_name, :added_at, :notes)
                    """), {
                        "user_id": user_id,
                        "stock_code": stock_code,
                        "stock_name": stock_name,
                        "added_at": added_at,
                        "notes": notes
                    })
                    conn.commit()

                    success += 1
                    results.append({
                        'stock_code': stock_code,
                        'stock_name': stock_name,
                        'success': True
                    })

            except sqlite3.IntegrityError:
                failed += 1
                results.append({
                    'stock_code': stock_code,
                    'success': False,
                    'error': '已存在'
                })

        return {
            'success': success,
            'failed': failed,
            'total': len(stock_codes),
            'results': results
        }, 200

    except Exception as e:
        return {'error': f'批量添加收藏失败: {str(e)}'}, 500
