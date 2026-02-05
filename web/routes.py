"""
Flask routes for the stock query web server

NOTE: All API endpoints have been moved to Flask-RESTX namespaces.
See /api/docs for interactive API documentation.
"""
from flask import Blueprint, jsonify, render_template

# 创建 Blueprint
bp = Blueprint('routes', __name__)

# 导入 ML Blueprint 并注册
from web import ml_routes
bp.register_blueprint(ml_routes.ml_bp)


@bp.route('/')
def index():
    """主页 - 股票查询界面"""
    return render_template('index.html')


@bp.route('/api')
def api_info():
    """API 信息"""
    return jsonify({
        'message': 'MyTrader API Server',
        'version': '1.0.0',
        'swagger_ui': '/api/docs',
        'health': '/api/health'
    }), 200
