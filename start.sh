#!/bin/bash

# 确保脚本在错误时退出
set -e

# 创建必要的目录
mkdir -p logs vector_db

# 启动所有定义在docker-compose.yml中的服务
docker compose up -d

# 等待服务启动
echo "等待服务启动..."
sleep 5

# 查看容器是否正常运行
docker compose ps

echo "服务已启动！"
echo "访问 http://localhost:8888 使用RAG智能问答系统" 