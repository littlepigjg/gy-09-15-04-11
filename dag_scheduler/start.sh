#!/bin/bash

# DAG工作流调度引擎启动脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=========================================="
echo "   DAG工作流调度引擎"
echo "=========================================="

# 检查Python依赖
echo "检查Python依赖..."
pip install -r "$SCRIPT_DIR/requirements.txt" -q

# 创建数据目录
mkdir -p "$SCRIPT_DIR/data/workflows"
mkdir -p "$SCRIPT_DIR/data/states"
mkdir -p "$SCRIPT_DIR/data/logs"

echo ""
echo "启动服务..."
echo "前端地址: http://localhost:5000"
echo "WebSocket: ws://localhost:8765"
echo ""

# 启动应用
cd "$SCRIPT_DIR/backend"
python app.py
