from pydantic_settings import BaseSettings
import os

# 使用pydantic_settings库实现配置管理
class Settings(BaseSettings):
    """应用配置类"""
    
    # 服务配置
    HOST: str = os.getenv("HOST", "127.0.0.1")  # 服务监听地址
    PORT: int = int(os.getenv("PORT", "8888"))  # 服务端口
    WORKERS: int = int(os.getenv("WORKERS", "4"))  # 工作进程数
    
    # Ollama配置
    OLLAMA_MODEL: str = os.getenv("OLLAMA_MODEL", "deepseek-r1:8b")
    
    # 向量存储配置
    VECTOR_DB_PATH: str = os.getenv("VECTOR_DB_PATH", "./vector_db")  # 向量数据库
    
    # 存储路径
    CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", "500"))  # 文档分块大小 
    CHUNK_OVERLAP: int = int(os.getenv("CHUNK_OVERLAP", "100"))  # 分块重叠大小
    VECTOR_DIMENSIONS: int = 4096  # 向量维度，根据实际使用的模型调整
    
    # 日志配置
    LOG_DIR: str = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")  # 日志目录
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")  # 日志级别
    LOG_FORMAT: str = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"  # 日志格式
    LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"  # 日期格式
    LOG_MAX_BYTES: int = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))  # 单个日志文件最大大小
    
    class Config:
        env_file = ".env"

settings = Settings() 