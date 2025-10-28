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
from flaxkv2.instance_manager import db_instance_manager

logger = get_logger(__name__)


class RawLevelDBDict:
    """
    原始LevelDB字典实现（无缓冲机制）

    直接写入LevelDB，不使用任何缓冲区、缓存、索引等高级功能。

    支持实例复用：同一个数据库路径多次实例化时，会返回已存在的实例（除非指定rebuild=True）。
    """

    def __new__(
        cls,
        name: str,
        path: str = ".",
        rebuild: bool = False,
        **kwargs
    ):
        """
        创建或返回数据库实例

        如果数据库已经打开，返回已有实例；否则创建新实例。
        如果rebuild=True，则关闭旧实例并创建新实例。
        """
        # 计算数据库路径
        abs_path = os.path.abspath(path)
        db_path = os.path.join(abs_path, name)

        # 如果需要重建，先关闭并删除旧实例
        if rebuild:
            existing = db_instance_manager.get_instance(db_path)
            if existing is not None:
                logger.debug(f"rebuild=True，关闭并删除旧实例: {db_path}")
                try:
                    # 先从缓存移除（但不调用close，因为我们需要手动删除文件）
                    db_instance_manager.unregister_instance(db_path)
                    if hasattr(existing, '_db') and existing._db is not None:
                        existing._db.close()
                    existing._closed = True
                except Exception as e:
                    logger.warning(f"关闭旧实例时出错: {e}")

            # 删除数据库文件（无论是否有缓存实例）
            if os.path.exists(db_path):
                import shutil
                try:
                    shutil.rmtree(db_path)
                    logger.debug(f"已删除旧数据库文件: {db_path}")
                except Exception as e:
                    logger.error(f"删除数据库文件失败: {e}")

            # 创建新实例
            instance = super().__new__(cls)
            instance._is_new_instance = True
            return instance

        # 检查是否已有实例
        existing = db_instance_manager.get_instance(db_path)
        if existing is not None:
            logger.debug(f"返回已存在的数据库实例: {db_path}")
            # 返回已有实例，不需要再次初始化
            return existing

        # 创建新实例
        instance = super().__new__(cls)
        instance._is_new_instance = True
        return instance

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
        # 如果不是新实例（即从缓存返回的实例），跳过初始化
        if not getattr(self, '_is_new_instance', False):
            logger.debug(f"跳过重复初始化，使用已有实例: {name}")
            return

        # 初始化实例属性
        self.name = name
        self.path = os.path.abspath(path)
        self.db_path = os.path.join(self.path, self.name)
        self._raw = raw
        self._db = None
        self._closed = False
        self._db_lock = threading.RLock()
        self._default_ttl = default_ttl
        self._auto_nested = auto_nested

        # 准备数据库目录
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

        # 注册到实例管理器
        db_instance_manager.register_instance(self.db_path, self)
        logger.debug(f"数据库实例已注册: {self.db_path}")

        # 标记初始化完成，删除临时标志
        del self._is_new_instance

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
        from flaxkv2.core.nested_dict import NestedDBDict

        # 特殊键：直接处理
        if isinstance(key, str) and key.startswith('__nested__:'):
            key_bytes = self._encode_key(key)
            value_bytes = self._encode_value(value)
            with self._db_lock:
                self._db.put(key_bytes, value_bytes)
            return

        # 如果值是 NestedDBDict，转换为普通字典
        if isinstance(value, NestedDBDict):
            value = value.to_dict()

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
        # 特殊键：直接处理（不触发TTL逻辑）
        if isinstance(key, str) and (key.startswith('__nested__:') or key.startswith('__ttl_info__:')):
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
                # 也要移除TTL
                self._ttl_manager.remove(key)
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

        # 从TTL管理器中移除（会删除对应的 __ttl_info__ 键）
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

    def _should_skip_internal_key(self, key_bytes: bytes) -> bool:
        """
        检查键是否是内部键（应该在 keys()/values()/items() 中跳过）

        内部键包括：
        1. __nested__: 前缀的标记键（已编码，如 b's__nested__:config'）
        2. __ttl_info__: 前缀的 TTL 信息键（已编码，如 b's__ttl_info__:key'）
        3. 嵌套存储的子键（通过 prefixed_db 创建，没有类型标识前缀，如 b'config:database:host'）

        Returns:
            True: 应该跳过（内部键或嵌套存储的子键）
            False: 不应该跳过（用户键）
        """
        # 方法1：先尝试用 _decode_key 解码（适用于带类型前缀的键）
        try:
            decoded_key = self._decode_key(key_bytes)
            # 检查是否是内部标记键
            if isinstance(decoded_key, str) and (
                decoded_key.startswith('__nested__:') or
                decoded_key.startswith('__ttl_info__:')
            ):
                return True
            # 正常的用户键
            return False
        except (ValueError, UnicodeDecodeError):
            # 解码失败，可能是嵌套存储的子键（没有类型前缀）
            pass

        # 方法2：尝试直接 UTF-8 解码（适用于嵌套子键）
        try:
            key_str = key_bytes.decode('utf-8')
            # 嵌套存储的子键通常包含 ':'（如 'config:database:host'）
            # 且不以 '__' 开头（内部标记键已经在上面处理了）
            if ':' in key_str and not key_str.startswith('__'):
                return True
        except UnicodeDecodeError:
            # UTF-8 解码也失败，可能是二进制数据，保守处理：不跳过
            pass

        return False

    def keys(self) -> List:
        """获取所有键列表"""
        keys = []
        with self._db_lock:
            for key_bytes, _ in self._db:
                # 先尝试解码
                try:
                    key = self._decode_key(key_bytes)

                    # 检查是否是嵌套标记键
                    if isinstance(key, str) and key.startswith('__nested__:'):
                        # 提取实际的键名（去掉 '__nested__:' 前缀）
                        actual_key = key[len('__nested__:'):]
                        # 只添加顶层嵌套键（不包含进一步的 ':'）
                        if ':' not in actual_key:
                            keys.append(actual_key)
                        continue

                    # 跳过 TTL 信息键
                    if isinstance(key, str) and key.startswith('__ttl_info__:'):
                        continue

                    # 正常的用户键
                    keys.append(key)

                except (ValueError, UnicodeDecodeError):
                    # 解码失败，可能是嵌套子键（没有类型前缀）
                    try:
                        key_str = key_bytes.decode('utf-8')
                        # 嵌套子键（如 'config:database:host'），跳过
                        if ':' in key_str:
                            continue
                    except UnicodeDecodeError as e:
                        # 真正的解码错误，记录日志
                        import logging
                        logging.warning(f"无法解码键 {key_bytes[:20]}...: {e}")

        return keys

    def values(self) -> List:
        """获取所有值列表"""
        # 使用 keys() 和 __getitem__ 来获取值
        # 这样可以正确处理嵌套字典（返回 NestedDBDict）
        return [self[key] for key in self.keys()]

    def items(self) -> List[Tuple]:
        """获取所有键值对列表"""
        # 使用 keys() 和 __getitem__ 来获取键值对
        # 这样可以正确处理嵌套字典（返回 NestedDBDict）
        return [(key, self[key]) for key in self.keys()]

    def __len__(self):
        """返回数据库大小"""
        return len(self.keys())

    def __repr__(self):
        """返回对象的字符串表示形式"""
        if self._closed:
            return f"RawLevelDBDict(name={self.name!r}, closed=True)"

        # 获取前几个键值对作为预览
        try:
            items = list(self.items())
            num_items = len(items)

            if num_items == 0:
                items_str = "{}"
            elif num_items <= 20:
                # 少于等于20个，显示所有
                items_repr = ", ".join(f"{k!r}: {v!r}" for k, v in items)
                items_str = f"{{{items_repr}}}"
            else:
                # 超过20个，显示前20个 + ...
                preview_items = items[:20]
                items_repr = ", ".join(f"{k!r}: {v!r}" for k, v in preview_items)
                items_str = f"{{{items_repr}, ...}} ({num_items} items)"

            return f"RawLevelDBDict(name={self.name!r}, path={self.db_path!r}, items={items_str})"
        except Exception as e:
            return f"RawLevelDBDict(name={self.name!r}, path={self.db_path!r}, error={e!r})"

    def __str__(self):
        """返回用户友好的字符串表示形式，类似dict"""
        if self._closed:
            return f"<RawLevelDBDict '{self.name}' (closed)>"

        try:
            items = list(self.items())
            num_items = len(items)

            if num_items == 0:
                return "{}"
            elif num_items <= 20:
                # 少于等于20个，显示所有（类似普通dict）
                items_repr = ", ".join(f"{k!r}: {v!r}" for k, v in items)
                return f"{{{items_repr}}}"
            else:
                # 超过20个，显示前20个 + 提示信息
                preview_items = items[:20]
                items_repr = ", ".join(f"{k!r}: {v!r}" for k, v in preview_items)
                return f"{{{items_repr}, ... and {num_items - 20} more items}}"
        except Exception as e:
            return f"<RawLevelDBDict '{self.name}' (error: {e})>"

    def to_dict(self) -> Dict:
        """转换为普通字典"""
        result = {}
        for k, v in self.items():
            result[k] = v
        return result
    
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

                # 从实例管理器中移除
                db_instance_manager.unregister_instance(self.db_path)
                logger.debug(f"数据库实例已从管理器移除: {self.db_path}")
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
        """
        返回数据库统计信息

        使用LevelDB内置的统计信息，加上必要的手动计算。

        Returns:
            包含以下信息的字典:
            - leveldb_stats: LevelDB原生统计信息（字符串）
            - count: 键值对数量
            - approximate_size: 数据库文件大小估算（字节）
            - path: 数据库路径
            - backend: 后端类型
        """
        stats = {
            'path': self.db_path,
            'backend': 'raw_leveldb'
        }

        # 获取LevelDB内置统计信息
        try:
            with self._db_lock:
                leveldb_stats_bytes = self._db.get_property(b'leveldb.stats')
                if leveldb_stats_bytes:
                    stats['leveldb_stats'] = leveldb_stats_bytes.decode('utf-8')

                # 获取大致的磁盘使用大小
                # 使用空字符串作为起始和结束，覆盖整个数据库
                try:
                    approximate_size = self._db.approximate_size(b'', b'\xff' * 100)
                    stats['approximate_size_bytes'] = approximate_size
                    # 转换为人类可读格式
                    if approximate_size < 1024:
                        stats['approximate_size'] = f"{approximate_size} B"
                    elif approximate_size < 1024 * 1024:
                        stats['approximate_size'] = f"{approximate_size / 1024:.2f} KB"
                    elif approximate_size < 1024 * 1024 * 1024:
                        stats['approximate_size'] = f"{approximate_size / (1024 * 1024):.2f} MB"
                    else:
                        stats['approximate_size'] = f"{approximate_size / (1024 * 1024 * 1024):.2f} GB"
                except Exception as e:
                    logger.debug(f"Failed to get approximate size: {e}")
                    stats['approximate_size'] = 'unknown'

        except Exception as e:
            logger.warning(f"Failed to get LevelDB stats: {e}")
            stats['leveldb_stats'] = 'unavailable'

        # 手动计算键值对数量（这个需要遍历）
        try:
            count = len(self.keys())
            stats['count'] = count
        except Exception as e:
            logger.warning(f"Failed to count keys: {e}")
            stats['count'] = 'unknown'

        return stats

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

    def get_default_ttl(self) -> Optional[int]:
        """
        获取默认TTL设置

        Returns:
            默认TTL秒数，如果没有设置返回None
        """
        return self._default_ttl

    def set_default_ttl(self, ttl_seconds: Optional[int]) -> None:
        """
        设置默认TTL

        Args:
            ttl_seconds: 默认TTL秒数，None表示取消默认TTL
        """
        self._default_ttl = ttl_seconds

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
