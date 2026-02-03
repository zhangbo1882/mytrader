#!/bin/bash
# 启动股票查询系统的 Web 服务
# 支持开发环境和生产环境

# 切换到项目根目录
cd "$(dirname "$0")/.."

# 检查虚拟环境是否存在
VENV_DIR=".venv"
if [ ! -d "$VENV_DIR" ]; then
    echo "错误: 虚拟环境不存在于 $VENV_DIR"
    echo "请先创建虚拟环境: python -m venv .venv"
    exit 1
fi

# 激活虚拟环境
source "$VENV_DIR/bin/activate"

# 设置环境变量，默认为开发环境
FLASK_ENV="${FLASK_ENV:-development}"

# 根据环境设置端口
if [ "$FLASK_ENV" = "production" ]; then
    # 生产环境: 端口 8000
    export PORT="${PORT:-8000}"
    echo "启动生产环境服务，端口: $PORT"
else
    # 开发环境: 端口 5001
    export PORT="${PORT:-5001}"
    echo "启动开发环境服务，端口: $PORT (支持调试和热重载)"
fi

# 启动 Flask 应用
python -m web.app
