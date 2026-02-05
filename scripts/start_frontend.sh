#!/bin/bash
# 启动前端开发服务器
# 使用 Vite 开发服务器

# 切换到项目根目录
cd "$(dirname "$0")/.."

# 检查前端目录是否存在
FRONTEND_DIR="frontend"
if [ ! -d "$FRONTEND_DIR" ]; then
    echo "错误: 前端目录不存在于 $FRONTEND_DIR"
    exit 1
fi

# 切换到前端目录
cd "$FRONTEND_DIR"

# 检查 node_modules 是否存在
if [ ! -d "node_modules" ]; then
    echo "node_modules 不存在，正在安装依赖..."
    npm install
    if [ $? -ne 0 ]; then
        echo "错误: npm install 失败"
        exit 1
    fi
fi

# 获取环境变量，默认为开发环境
NODE_ENV="${NODE_ENV:-development}"

# 根据环境设置端口和命令
if [ "$NODE_ENV" = "production" ]; then
    # 生产环境: 先构建再预览
    echo "构建生产版本..."
    npm run build
    if [ $? -ne 0 ]; then
        echo "错误: 构建失败"
        exit 1
    fi
    echo "启动生产环境预览服务器，端口: 4173"
    npm run preview
else
    # 开发环境: 使用 Vite 开发服务器
    export PORT="${PORT:-5173}"
    echo "启动前端开发服务器，端口: $PORT (支持热更新)"
    npm run dev
fi
