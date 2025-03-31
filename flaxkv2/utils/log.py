"""
FlaxKV2 日志模块
"""

import os
import sys
from loguru import logger

# 配置默认日志级别
DEFAULT_LOG_LEVEL = os.environ.get("FLAXKV_LOG_LEVEL", "INFO")

# 默认配置
logger.remove()  # 移除默认配置
logger.add(
    sys.stderr,
    level=DEFAULT_LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
)

def get_logger(name):
    """
    获取指定名称的logger
    
    Args:
        name: 日志名称，通常为模块名
        
    Returns:
        logger: 配置好的logger实例
    """
    return logger.bind(name=name)

def set_log_level(level):
    """
    设置日志级别
    
    Args:
        level: 日志级别，如 'DEBUG', 'INFO', 'WARNING', 'ERROR'
    """
    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    
def add_file_log(filepath, level="DEBUG", rotation="10 MB", retention="7 days"):
    """
    添加文件日志记录
    
    Args:
        filepath: 日志文件路径
        level: 日志级别
        rotation: 日志轮转条件
        retention: 日志保留时间
    """
    logger.add(
        filepath,
        level=level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        rotation=rotation,
        retention=retention
    ) 