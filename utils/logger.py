import logging
import sys
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import datetime
from app_config.config import settings

def setup_logging(
    log_dir: str = "logs",
    log_level: int = logging.INFO,
) -> logging.Logger:
    """
    设置日志配置
    
    Args:
        log_dir: 日志文件目录
        log_level: 日志级别
    
    Returns:
        logging.Logger: 配置好的日志记录器
    """
    try:
        print(f"正在初始化日志系统，目录: {log_dir}")
        
        # os.access() 是一个标准库函数，用于测试指定路径的访问权限。
        # os.W_OK 是一个常量，表示写入权限。
        if os.path.exists(log_dir) and not os.access(log_dir, os.W_OK):
            raise PermissionError(f"没有写入权限: {log_dir}")
            
        # 创建主日志目录，exist_ok=True 表示如果目录已存在则不报错
        os.makedirs(log_dir, exist_ok=True)
        
        # 获取当前日期时间
        today = datetime.now()
        
        # 按年月创建子目录，格式为 YYYY-MM
        month_dir = os.path.join(log_dir, today.strftime('%Y-%m'))
        
        # 创建年月子目录，exist_ok=True 表示如果目录已存在则不报错
        os.makedirs(month_dir, exist_ok=True)
        
        # 验证目录是否成功创建
        if not os.path.exists(month_dir):
            raise OSError(f"无法创建日志目录: {month_dir}")
        
        # 创建logger对象
        logger = logging.getLogger('RAGChat')
        logger.setLevel(log_level)
        
        # 避免重复日志输出
        if logger.handlers:
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
        
        # 日志格式
        formatter = logging.Formatter(
            settings.LOG_FORMAT,
            datefmt=settings.LOG_DATE_FORMAT
        )

        # 创建一个新的日志处理器，将日志消息输出到标准输出控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        # 为控制台处理器设置一个格式化器
        console_handler.setFormatter(formatter)
        # 添加处理器到日志记录器
        logger.addHandler(console_handler)
        
        # 按天切割的文件处理器
        log_file = os.path.join(month_dir, f'ragchat_{today.strftime("%Y-%m-%d")}.log')

        # 创建一个按天切割的文件处理器
        file_handler = TimedRotatingFileHandler(
            log_file,
            when='midnight',
            interval=1,
            encoding='utf-8'
        )
        # 为文件处理器设置一个格式化器
        file_handler.setFormatter(formatter)
        # 添加处理器到日志记录器
        logger.addHandler(file_handler)
        
        print(f"日志文件路径: {log_file}")  # 添加调试信息
        
        # 记录日志系统初始化成功
        logger.info(f"日志系统初始化成功，日志文件: {log_file}")
        return logger
        
    except Exception as e:
        print(f"日志系统初始化失败: {str(e)}")
        
        # 创建一个后备的日志记录器，仅输出到控制台
        # 当主日志系统初始化失败时，确保程序仍然可以记录日志
        fallback_logger = logging.getLogger('RAGChat')
        # 设置日志级别与主日志系统相同
        fallback_logger.setLevel(log_level)
        # 创建一个控制台处理器用于输出日志
        console_handler = logging.StreamHandler(sys.stdout)
        # 使用与主日志系统相同的格式
        console_handler.setFormatter(logging.Formatter(settings.LOG_FORMAT))
        # 将处理器添加到后备日志记录器
        fallback_logger.addHandler(console_handler)
        # 返回后备日志记录器
        return fallback_logger

# 创建全局logger实例
logger = setup_logging(
    log_dir=settings.LOG_DIR,
    log_level=getattr(logging, settings.LOG_LEVEL)
)

def get_logger() -> logging.Logger:
    """获取logger实例"""
    return logger 