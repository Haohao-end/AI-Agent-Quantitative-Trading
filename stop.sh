#!/bin/bash

# 确保脚本在错误时退出
set -e

echo "正在停止所有服务..."

# 停止并移除所有容器
docker compose down

echo "所有服务已停止！"

# 显示当前运行的容器状态，确认服务是否已经完全停止
docker compose ps 