"""
FlaxFile - 高性能文件传输工具

基于ZMQ优化的跨网络文件传输系统
性能: 3800+ MB/s (本地), 1000+ MB/s (10Gbps网络)
"""

__version__ = "1.0.0"
__author__ = "FlaxKV Team"

from .client import FlaxFileClient
from .server import FlaxFileServer

__all__ = ["FlaxFileClient", "FlaxFileServer"]
