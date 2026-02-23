"""
Board data business logic services

NOTE: Board functionality has been removed as it depended on AKShare.
These endpoints return empty responses to avoid breaking the frontend.
"""
from flask import jsonify


def get_boards_list():
    """获取所有板块列表"""
    # Board functionality removed - return empty list
    return [], 200


def get_board_constituents(board_code):
    """获取指定板块的成分股"""
    # Board functionality removed - return empty list
    return [], 200


def get_stock_boards(stock_code):
    """获取指定股票所属的所有板块"""
    # Board functionality removed - return empty list
    return [], 200
