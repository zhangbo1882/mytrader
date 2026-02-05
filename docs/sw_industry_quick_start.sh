#!/bin/bash
# 申万行业成分股API快速测试脚本

echo "================================"
echo "申万行业成分股API测试"
echo "================================"
echo ""

# 检查虚拟环境
if [ ! -d ".venv" ]; then
    echo "❌ 虚拟环境不存在"
    echo "   请先创建: python3 -m venv .venv"
    exit 1
fi

# 激活虚拟环境
echo "📦 激活虚拟环境..."
source .venv/bin/activate

# 检查Flask应用是否运行
echo "🔍 检查API服务状态..."
if curl -s http://localhost:5555/health | grep -q "ok"; then
    echo "✅ API服务已运行"
    echo ""

    # 运行测试脚本（获取申万2021数据）
    echo "🚀 开始测试..."
    python scripts/test_sw_industry_api.py --src SW2021
else
    echo "❌ API服务未启动"
    echo ""
    echo "请先启动Flask应用:"
    echo "  source .venv/bin/activate"
    echo "  python web/app.py"
    echo ""
    echo "然后在另一个终端运行此脚本"
    exit 1
fi
