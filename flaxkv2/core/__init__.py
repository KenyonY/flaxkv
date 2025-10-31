"""
FlaxKV2 核心模块
"""

from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict
from flaxkv2.core.cached_leveldb_dict import CachedLevelDBDict

__all__ = ["RawLevelDBDict", "CachedLevelDBDict"]

# 注意: LevelDBDict 已弃用，不再从 core 模块导出
# 如仍需使用，请直接导入: from flaxkv2.core.leveldb_dict import LevelDBDict
# 强烈建议迁移到 RawLevelDBDict，详见 DEPRECATION_NOTICE.md 