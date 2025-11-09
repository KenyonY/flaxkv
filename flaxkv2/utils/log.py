"""
FlaxKV2 日志模块

作为基础库，遵循以下日志最佳实践：
1. 默认禁用所有日志输出，避免污染应用程序的终端
2. 通过环境变量 FLAXKV_ENABLE_LOGGING=1 可以启用日志
3. 应用程序可以调用 enable_logging() 来启用日志
4. 通过控制 handler 来实现启用/禁用，简单可靠
"""

import os
import sys
from loguru import logger

# 移除 loguru 的默认 handler
logger.remove()

# 检查是否通过环境变量启用日志
_ENABLE_LOGGING = os.environ.get("FLAXKV_ENABLE_LOGGING", "0") == "1"
_DEFAULT_LOG_LEVEL = os.environ.get("FLAXKV_LOG_LEVEL", "WARNING")

# 存储当前的 handler ID
_current_handler_id = None

# 如果环境变量要求启用，则添加 handler
if _ENABLE_LOGGING:
    _current_handler_id = logger.add(
        sys.stderr,
        level=_DEFAULT_LOG_LEVEL,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )

def enable_logging(level="INFO", format_str=None):
    """
    启用 FlaxKV2 日志输出（供应用程序调用）

    Args:
        level: 日志级别，默认 'INFO'
        format_str: 自定义格式字符串，None 则使用默认格式

    Example:
        >>> from flaxkv2.utils.log import enable_logging
        >>> enable_logging(level="DEBUG")
    """
    global _current_handler_id

    # 清除所有现有的 handler
    logger.remove()
    _current_handler_id = None

    if format_str is None:
        format_str = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"

    _current_handler_id = logger.add(
        sys.stderr,
        level=level,
        format=format_str
    )

def disable_logging():
    """
    禁用 FlaxKV2 日志输出

    Example:
        >>> from flaxkv2.utils.log import disable_logging
        >>> disable_logging()
    """
    global _current_handler_id

    # 移除所有 handler
    logger.remove()
    _current_handler_id = None

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

    Note:
        如果日志未启用，需要先调用 enable_logging()
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