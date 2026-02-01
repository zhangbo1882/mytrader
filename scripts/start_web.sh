#!/bin/bash
# 启动股票查询系统的 Web 服务

cd "$(dirname "$0")/.."
python -m web.app
