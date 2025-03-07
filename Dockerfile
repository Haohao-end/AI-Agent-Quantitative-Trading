# 使用Python 3.12-slim作为基础镜像
# slim版本比完整版体积小，但包含了运行Python所需的最小依赖
FROM python:3.12-slim

# 设置工作目录为/app
WORKDIR /app

# 设置Python环境变量
# PYTHONUNBUFFERED=1 : 确保Python输出不被缓存，实时显示日志
# PYTHONDONTWRITEBYTECODE=1 : 不生成.pyc文件
ENV PYTHONUNBUFFERED=1 \    
    PYTHONDONTWRITEBYTECODE=1

# 安装系统依赖
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \    
        build-essential \   
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装项目依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY . .

# 创建日志与向量存储的目录
RUN mkdir -p logs vector_db && \
    # 设置目录权限
    chmod -R 755 logs vector_db

# 暴露服务端口
EXPOSE 8888

# 使用非root用户运行应用
RUN useradd -m -s /bin/bash appuser && \
    chown -R appuser:appuser /app
USER appuser

# 容器启动命令
CMD ["python", "main.py"] 