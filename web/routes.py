"""
Flask routes for the stock query web server

NOTE: All API endpoints have been moved to Flask-RESTX namespaces.
See /api/docs for interactive API documentation.

NOTE: Old UI (Bootstrap + vanilla JS) has been removed.
New React frontend is in /frontend directory.
"""
from flask import Blueprint, jsonify

# 创建 Blueprint
bp = Blueprint('routes', __name__)

# 导入 ML Blueprint 并注册
from web import ml_routes
bp.register_blueprint(ml_routes.ml_bp)


@bp.route('/')
def index():
    """主页 - 已迁移到 React 前端"""
    return jsonify({
        'message': 'MyTrader API Server',
        'note': 'Web UI has been migrated to React frontend. See /frontend directory.',
        'api_docs': '/api/docs',
        'health': '/api/health',
        'frontend': 'Run `cd frontend && npm run dev` to start the React development server'
    }), 200


@bp.route('/api')
def api_info():
    """API 信息"""
    return jsonify({
        'message': 'MyTrader API Server',
        'version': '1.0.0',
        'swagger_ui': '/api/docs',
        'health': '/api/health'
    }), 200
