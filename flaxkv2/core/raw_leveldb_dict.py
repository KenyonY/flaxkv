"""
FlaxKV2 原始LevelDB后端实现（无缓冲）
用于性能对比测试
"""

import os
import threading
from typing import Any, Dict, List, Tuple, Optional

import plyvel

from flaxkv2.serialization import encoder, decoder
from flaxkv2.core.nested_dict import NestedDBDict
from flaxkv2.utils.log import get_logger
from flaxkv2.utils.ttl import TTLManager

logger = get_logger(__name__)


class RawLevelDBDict:
    """
    原始LevelDB字典实现（无缓冲机制）

    直接写入LevelDB，不使用任何缓冲区、缓存、索引等高级功能。
    用于性能基准测试，作为最基础的性能参考。
    """

    def __init__(
        self,
        name: str,
        path: str = ".",
        rebuild: bool = False,
        create_if_missing: bool = True,
        raw: bool = False,
        default_ttl: Optional[int] = None,
        auto_nested: bool = False,
        **kwargs
    ):
        """
        初始化原始LevelDB字典

        Args:
            name: 数据库名称
            path: 数据库路径
            rebuild: 是否重建数据库
            create_if_missing: 如果数据库不存在是否创建
            raw: 是否使用原始模式（不进行序列化）
            default_ttl: 默认TTL（秒），为None表示不使用TTL
            auto_nested: 是否自动将字典类型转换为嵌套存储（默认False，保持性能基准纯粹）
        """
        self.name = name
        self.path = os.path.abspath(path)
        self.db_path = os.path.join(self.path, self.name)
        self._raw = raw
        self._db = None
        self._closed = False
        self._db_lock = threading.RLock()
        self._default_ttl = default_ttl
        self._auto_nested = auto_nested

        # 准备数据库
        if rebuild and os.path.exists(self.db_path):
            import shutil
            shutil.rmtree(self.db_path)

        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        # 设置选项
        self._leveldb_options = {
            'create_if_missing': create_if_missing,
            'write_buffer_size': 64 * 1024 * 1024,  # 64MB
            'max_open_files': 100,
            'compression': 'snappy',
        }

        # 初始化TTL管理器
        self._ttl_manager = TTLManager()

        # 初始化数据库
        self._init_db()

        # 设置TTL管理器的数据库引用
        self._ttl_manager.set_db(self)

    def _init_db(self):
        """初始化数据库连接"""
        try:
            self._db = plyvel.DB(self.db_path, **self._leveldb_options)
            logger.info(f"Opened raw LevelDB at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to open raw LevelDB at {self.db_path}: {e}")
            raise

    def _encode_key(self, key):
        """编码键"""
        if self._raw:
            if isinstance(key, str):
                return key.encode('utf-8')
            elif isinstance(key, bytes):
                return key
            else:
                return str(key).encode('utf-8')
        else:
            return encoder.encode_key(key)

    def _decode_key(self, key_bytes):
        """解码键"""
        if self._raw:
            try:
                return key_bytes.decode('utf-8')
            except UnicodeDecodeError:
                return key_bytes
        else:
            return decoder.decode_key(key_bytes)

    def _encode_value(self, value):
        """编码值"""
        if self._raw:
            if isinstance(value, str):
                return value.encode('utf-8')
            elif isinstance(value, bytes):
                return value
            else:
                return str(value).encode('utf-8')
        else:
            return encoder.encode(value)

    def _decode_value(self, value_bytes):
        """解码值"""
        if self._raw:
            try:
                return value_bytes.decode('utf-8')
            except UnicodeDecodeError:
                return value_bytes
        else:
            return decoder.decode(value_bytes)

    def __getitem__(self, key):
        """获取键值"""
        # 特殊键：直接处理
        if isinstance(key, str) and key.startswith('__nested__:'):
            key_bytes = self._encode_key(key)
            with self._db_lock:
                value_bytes = self._db.get(key_bytes)
            if value_bytes is None:
                raise KeyError(key)
            return self._decode_value(value_bytes)

        # 检查TTL
        if self._ttl_manager.is_expired(key):
            # 删除过期键
            try:
                del self[key]
            except:
                pass
            raise KeyError(key)

        # 如果启用了自动嵌套，检查是否是嵌套字典
        if self._auto_nested:
            try:
                marker_key = f'__nested__:{key}'
                marker_bytes = self._encode_key(marker_key)
                with self._db_lock:
                    marker_value = self._db.get(marker_bytes)
                if marker_value is not None:
                    # 是嵌套字典，返回 NestedDBDict
                    return self.nested(key)
            except:
                pass

        # 普通值
        key_bytes = self._encode_key(key)

        with self._db_lock:
            value_bytes = self._db.get(key_bytes)

        if value_bytes is None:
            raise KeyError(key)

        return self._decode_value(value_bytes)

    def __setitem__(self, key, value):
        """设置键值 - 直接写入数据库"""
        # 特殊键：直接处理
        if isinstance(key, str) and key.startswith('__nested__:'):
            key_bytes = self._encode_key(key)
            value_bytes = self._encode_value(value)
            with self._db_lock:
                self._db.put(key_bytes, value_bytes)
            return

        # 如果启用了自动嵌套且值是字典
        if self._auto_nested and isinstance(value, dict):
            # 1. 标记为嵌套字典
            marker_key = f'__nested__:{key}'
            marker_bytes = self._encode_key(marker_key)
            marker_value = self._encode_value(True)
            with self._db_lock:
                self._db.put(marker_bytes, marker_value)

            # 2. 创建 nested，递归写入
            nested = self.nested(key)
            nested.clear()
            for k, v in value.items():
                nested[k] = v  # 递归
            return

        # 非字典或未启用自动嵌套：取消标记（如果有）
        if self._auto_nested:
            try:
                marker_key = f'__nested__:{key}'
                marker_bytes = self._encode_key(marker_key)
                with self._db_lock:
                    self._db.delete(marker_bytes)
            except:
                pass

        # 传统存储
        key_bytes = self._encode_key(key)
        value_bytes = self._encode_value(value)

        with self._db_lock:
            self._db.put(key_bytes, value_bytes)

        # 如果设置了默认TTL，应用到新键
        if self._default_ttl is not None:
            self._ttl_manager.set(key, self._default_ttl)

    def __delitem__(self, key):
        """删除键"""
        # 特殊键：直接处理
        if isinstance(key, str) and key.startswith('__nested__:'):
            key_bytes = self._encode_key(key)
            with self._db_lock:
                self._db.delete(key_bytes)
            return

        # 如果启用了自动嵌套，检查是否是嵌套字典
        if self._auto_nested:
            marker_key = f'__nested__:{key}'
            marker_bytes = self._encode_key(marker_key)

            with self._db_lock:
                marker_value = self._db.get(marker_bytes)

            if marker_value is not None:
                # 是嵌套字典，递归删除所有子键
                nested = self.nested(key)
                nested.clear()
                # 删除标记
                with self._db_lock:
                    self._db.delete(marker_bytes)
                return

        # 普通值
        key_bytes = self._encode_key(key)

        with self._db_lock:
            # 检查键是否存在
            value_bytes = self._db.get(key_bytes)
            if value_bytes is None:
                raise KeyError(key)

            # 删除键
            self._db.delete(key_bytes)

        # 从TTL管理器中移除
        self._ttl_manager.remove(key)

    def __contains__(self, key):
        """检查键是否存在"""
        try:
            self[key]
            return True
        except KeyError:
            return False

    def get(self, key, default=None):
        """获取键值，不存在返回默认值"""
        try:
            return self[key]
        except KeyError:
            return default

    def update(self, d: Dict[Any, Any]):
        """批量更新多个键值对"""
        with self._db_lock:
            batch = self._db.write_batch()

            for key, value in d.items():
                key_bytes = self._encode_key(key)
                value_bytes = self._encode_value(value)
                batch.put(key_bytes, value_bytes)

            batch.write()

        # 如果设置了默认TTL，为所有键应用TTL
        if self._default_ttl is not None:
            for key in d.keys():
                self._ttl_manager.set(key, self._default_ttl)

    def keys(self) -> List:
        """获取所有键列表"""
        keys = []
        with self._db_lock:
            for key_bytes, _ in self._db:
                key = self._decode_key(key_bytes)
                keys.append(key)
        return keys

    def values(self) -> List:
        """获取所有值列表"""
        values = []
        with self._db_lock:
            for _, value_bytes in self._db:
                value = self._decode_value(value_bytes)
                values.append(value)
        return values

    def items(self) -> List[Tuple]:
        """获取所有键值对列表"""
        items = []
        with self._db_lock:
            for key_bytes, value_bytes in self._db:
                key = self._decode_key(key_bytes)
                value = self._decode_value(value_bytes)
                items.append((key, value))
        return items

    def __len__(self):
        """返回数据库大小"""
        return len(self.keys())

    def close(self):
        """关闭数据库"""
        if self._closed:
            return

        if self._db is not None:
            try:
                with self._db_lock:
                    self._db.close()
                    self._db = None
                    self._closed = True
                logger.debug(f"Raw LevelDB connection closed: {self.name}")
            except Exception as e:
                logger.error(f"Error closing raw LevelDB: {self.name}, error: {e}")

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.close()
        return False

    def stat(self) -> Dict:
        """返回数据库统计信息"""
        count = 0
        total_key_size = 0
        total_value_size = 0

        with self._db_lock:
            for key_bytes, value_bytes in self._db:
                count += 1
                total_key_size += len(key_bytes)
                total_value_size += len(value_bytes)

        return {
            'count': count,
            'avg_key_size': total_key_size / count if count > 0 else 0,
            'avg_value_size': total_value_size / count if count > 0 else 0,
            'total_size': total_key_size + total_value_size,
            'path': self.db_path,
            'backend': 'raw_leveldb'
        }

    def set_ttl(self, key: Any, ttl_seconds: int) -> None:
        """
        为指定键设置TTL

        Args:
            key: 键
            ttl_seconds: TTL时间（秒）
        """
        self._ttl_manager.set(key, ttl_seconds)

    def get_ttl(self, key: Any) -> Optional[int]:
        """
        获取指定键的剩余TTL

        Args:
            key: 键

        Returns:
            剩余TTL秒数，如果没有设置TTL返回None
        """
        return self._ttl_manager.get_remaining_ttl(key)

    def remove_ttl(self, key: Any) -> None:
        """
        移除指定键的TTL

        Args:
            key: 键
        """
        self._ttl_manager.remove(key)

    def cleanup_expired(self) -> int:
        """
        清理所有过期的键

        Returns:
            清理的键数量
        """
        return self._ttl_manager.cleanup_expired()

    def nested(self, prefix: str) -> NestedDBDict:
        """
        创建一个基于前缀的嵌套字典视图

        这是解决嵌套数据频繁序列化问题的推荐方案。
        使用 NestedDBDict 可以让每个字段独立存储和访问，避免整个字典的序列化/反序列化。

        性能优势：
        - 修改单个字段只需序列化该字段的值
        - 读取单个字段只需反序列化该字段的值
        - 利用 LevelDB 的前缀查询能力高效迭代

        使用示例：
            # 创建嵌套字典
            user = db.nested('user:1')

            # 设置字段（每个字段独立存储）
            user['name'] = 'Alice'
            user['age'] = 30
            user['city'] = 'NYC'

            # 高效修改（只序列化 age 的值）
            user['age'] = 31

            # 高效读取（只反序列化 name 的值）
            print(user['name'])

            # 迭代所有字段
            for key, value in user.items():
                print(key, value)

        Args:
            prefix: 前缀字符串，建议使用冒号分隔（如 'user:1'）

        Returns:
            NestedDBDict: 嵌套字典对象

        注意：
            - 前缀会自动添加冒号分隔符，实际存储的键格式为 'prefix:field'
            - NestedDBDict 支持完整的字典接口（get, set, del, keys, values, items 等）
            - 数据直接写入 LevelDB，不使用缓冲机制
        """
        # 确保数据库已打开
        if self._db is None:
            raise RuntimeError("Database is not open")

        # 创建带前缀的数据库视图
        prefix_with_colon = f"{prefix}:"
        prefix_bytes = prefix_with_colon.encode('utf-8')
        prefixed_db = self._db.prefixed_db(prefix_bytes)

        # 返回 NestedDBDict 对象，传入 root_db 以支持递归嵌套
        return NestedDBDict(prefixed_db, prefix_with_colon, parent_db=None, root_db=self)
