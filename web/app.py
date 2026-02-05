"""
Flask Web Application for Stock Query System
"""
from flask import Flask
from flask_cors import CORS
from flask_socketio import SocketIO
import sys
import os
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 创建 Flask 应用
app = Flask(__name__,
            template_folder='templates',
            static_folder='static')
app.config['JSON_AS_ASCII'] = False  # 支持中文
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.secret_key = 'your-secret-key-here'  # Required for session

# 启用 CORS
CORS(app)

# Initialize SocketIO
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# 注册路由 Blueprint
from web.routes import bp
app.register_blueprint(bp)

# 注册 RESTX API Blueprint (Swagger UI at /api/docs)
from web.restx_api import restx_bp
app.register_blueprint(restx_bp)

# 初始化任务管理器和调度器
from web.tasks import init_task_manager
from web.scheduler import init_scheduler
from config.settings import CHECKPOINT_DIR, SCHEDULE_DB_PATH, SCHEDULER_TIMEZONE, TASKS_DB_PATH

# Initialize on application startup
_initialized = False


@app.before_request
def initialize():
    """Initialize task manager and scheduler before first request"""
    global _initialized
    if not _initialized:
        print("[Init] Initializing task manager...")
        # Initialize task manager WITHOUT initializing database schema
        # Database schema is initialized by worker process only
        # This avoids SQLite lock conflicts between web and worker
        tm = init_task_manager(
            db_path=str(TASKS_DB_PATH),
            checkpoint_dir=CHECKPOINT_DIR,
            init_db=False  # Web service should NOT init database
        )
        print(f"[Init] Task manager initialized: {tm}")

        # 注意: 不在这里清理陈旧任务，应该由 worker 进程来处理
        # 避免多个进程同时访问 SQLite 数据库导致锁定

        # Initialize scheduler
        print("[Init] Initializing scheduler...")
        init_scheduler(str(SCHEDULE_DB_PATH), timezone=SCHEDULER_TIMEZONE)

        # Initialize WebSocket AI Screen service
        print("[Init] Initializing WebSocket AI Screen service...")
        from web.websocket.ai_screen_ws import init_claude_cli, register_socketio_events
        init_claude_cli()
        register_socketio_events(socketio)
        print("[Init] WebSocket AI Screen service initialized")

        # Note: Task execution is now handled by Worker Service
        # Worker will recover and execute unfinished tasks
        print("[Init] Task execution handled by Worker Service")
        print("[Init] Start worker with: python scripts/start_worker.py")

        _initialized = True
        print("[Init] Initialization complete")


if __name__ == '__main__':
    import os
    # 从环境变量读取配置，支持开发和生产环境
    # 默认开发环境: 端口5001, debug模式
    # 生产环境: 端口8000, 非debug模式
    env = os.getenv('FLASK_ENV', 'development')
    if env == 'production':
        port = int(os.getenv('PORT', 8000))
        debug = False
    else:
        port = int(os.getenv('PORT', 5001))
        debug = True

    socketio.run(app, host='0.0.0.0', port=port, debug=debug, allow_unsafe_werkzeug=True)
