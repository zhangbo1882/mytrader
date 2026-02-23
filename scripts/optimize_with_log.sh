#!/bin/bash
# 优化脚本 - 完整日志输出到文件
#
# 使用方法:
#   ./scripts/optimize_with_log.sh 00941 2021-01-01 2025-12-31
#   ./scripts/optimize_with_log.sh 01810 2021-01-01 2025-12-31

# 检查参数
if [ $# -lt 1 ]; then
    echo "使用方法: $0 <股票代码> [开始日期] [结束日期]"
    echo "示例: $0 00941 2021-01-01 2025-12-31"
    exit 1
fi

STOCK_CODE=$1
START_DATE=${2:-"2021-01-01"}
END_DATE=${3:-"2025-12-31"}

# 创建日志目录
LOG_DIR="logs/optimization"
mkdir -p "$LOG_DIR"

# 生成日志文件名
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/${STOCK_CODE}_FULL_${TIMESTAMP}.log"

echo "======================================================================"
echo "优化任务开始"
echo "======================================================================"
echo "股票代码: $STOCK_CODE"
echo "开始日期: $START_DATE"
echo "结束日期: $END_DATE"
echo "完整日志: $LOG_FILE"
echo "======================================================================"
echo ""

# 激活虚拟环境并运行优化，同时输出到控制台和日志文件
source .venv/bin/activate

# 使用 tee 命令同时输出到控制台和文件
python -m src.optimization.standalone "$STOCK_CODE" "$START_DATE" "$END_DATE" 2>&1 | tee "$LOG_FILE"

# 检查退出状态
EXIT_CODE=${PIPESTATUS[0]}

echo ""
echo "======================================================================"
echo "优化任务完成"
echo "退出代码: $EXIT_CODE"
echo "完整日志已保存到: $LOG_FILE"
echo "======================================================================"

# 显示日志文件大小
if [ -f "$LOG_FILE" ]; then
    FILE_SIZE=$(du -h "$LOG_FILE" | cut -f1)
    echo "日志文件大小: $FILE_SIZE"
fi

exit $EXIT_CODE
