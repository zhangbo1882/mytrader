"""
Flask-RESTX API configuration and endpoints with Swagger UI
"""
from flask import Blueprint
from flask_restx import Api
from web.restx_namespaces import (
    stock_ns,
    task_ns,
    schedule_ns,
    financial_ns,
    health_ns,
    sw_industry_ns,
    boards_ns,
    favorites_ns,
    liquidity_ns
)

# Create Blueprint
restx_bp = Blueprint('api', __name__, url_prefix='/api')

# Initialize Flask-RESTX
api = Api(
    restx_bp,
    version='1.0',
    title='MyTrader API',
    description='量化交易系统 API 文档',
    doc='/docs',  # Swagger UI available at /api/docs
    prefix='',  # Empty prefix since blueprint already has url_prefix='/api'
    default='MyTrader',
    default_label='MyTrader API Operations',
    validate=True,
    ordered=True,
    terms_url=None,
    contact='support',
    contact_email='zhangbo1882@gmail.com',
    license='MIT',
    license_url='https://opensource.org/licenses/MIT'
)

# Register namespaces
api.add_namespace(health_ns, path='/health')
api.add_namespace(stock_ns, path='/stock')
api.add_namespace(task_ns, path='/tasks')
api.add_namespace(schedule_ns, path='/schedule')
api.add_namespace(financial_ns, path='/financial')
api.add_namespace(sw_industry_ns, path='/sw-industry')
api.add_namespace(boards_ns, path='/boards')
api.add_namespace(favorites_ns, path='/favorites')
api.add_namespace(liquidity_ns, path='/liquidity')
