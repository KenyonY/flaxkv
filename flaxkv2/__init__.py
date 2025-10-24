"""
FlaxKV2 - 高性能、多功能键值存储
"""

__version__ = '0.1.0'

# 导入自动关闭模块，确保它被初始化
from flaxkv2 import auto_close

# 主接口导出
from flaxkv2.core.base import FlaxKV
from flaxkv2.core.leveldb_dict import LevelDBDict
from flaxkv2.core.nested_dict import NestedDBDict

__all__ = ["FlaxKV", "LevelDBDict", "NestedDBDict"] 