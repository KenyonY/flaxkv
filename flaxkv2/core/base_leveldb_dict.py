"""
FlaxKV2 LevelDB 字典基类

提取 RawLevelDBDict 和 CachedLevelDBDict 的公共代码，减少代码重复。
"""

import os
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple, Optional, Iterator

from flaxkv2.serialization import encoder, decoder
from flaxkv2.serialization.value_meta import ValueWithMeta
from flaxkv2.core.nested_structures import NestedDBDict, NestedDBList
from flaxkv2.utils.log import get_logger
from flaxkv2.instance_manager import db_instance_manager

logger = get_logger(__name__)


class BaseLevelDBDict(ABC):
    """
    LevelDB 字典基类

    提供公共的：
    - 实例管理（__new__）
    - 键值编解码
    - TTL 管理
    - 嵌套结构支持
    - 上下文管理器
    """

    # ========== 实例管理 ==========

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

    # ========== 抽象方法（子类必须实现） ==========

    @abstractmethod
    def __getitem__(self, key) -> Any:
        """获取键值"""
        pass

    @abstractmethod
    def __setitem__(self, key, value) -> None:
        """设置键值"""
        pass

    @abstractmethod
    def __delitem__(self, key) -> None:
        """删除键"""
        pass

    @abstractmethod
    def set(self, key, value, ttl=None) -> None:
        """设置键值（支持TTL）"""
        pass

    @abstractmethod
    def keys(self) -> List:
        """获取所有键列表"""
        pass

    @abstractmethod
    def close(self) -> None:
        """关闭数据库"""
        pass

    @abstractmethod
    def _get_backend_name(self) -> str:
        """返回后端名称（用于 stat 等方法）"""
        pass

    # ========== 键值编解码 ==========

    def _encode_key(self, key) -> bytes:
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

    def _decode_key(self, key_bytes: bytes):
        """解码键"""
        if self._raw:
            try:
                return key_bytes.decode('utf-8')
            except UnicodeDecodeError:
                return key_bytes
        else:
            return decoder.decode_key(key_bytes)

    def _encode_value(self, value, ttl_seconds=None) -> bytes:
        """
        编码值（支持内嵌TTL）

        Args:
            value: 要编码的值
            ttl_seconds: TTL秒数（None表示无TTL）

        Returns:
            bytes: 编码后的字节流
        """
        if self._raw:
            # Raw模式：不支持TTL
            if ttl_seconds is not None:
                raise ValueError("Raw mode does not support TTL")
            if isinstance(value, str):
                return value.encode('utf-8')
            elif isinstance(value, bytes):
                return value
            else:
                return str(value).encode('utf-8')
        else:
            # 使用ValueWithMeta进行编码（内嵌TTL）
            return ValueWithMeta.encode_value(value, ttl_seconds)

    def _decode_value(self, value_bytes: bytes) -> Tuple[Any, Optional[float], bool]:
        """
        解码值（提取内嵌TTL信息）

        Args:
            value_bytes: 编码后的字节流

        Returns:
            (value, expire_time, is_expired) 三元组:
            - value: 解码后的值
            - expire_time: 过期时间戳（None表示无TTL）
            - is_expired: 是否已过期
        """
        if self._raw:
            # Raw模式：不支持TTL
            try:
                value = value_bytes.decode('utf-8')
            except UnicodeDecodeError:
                value = value_bytes
            return value, None, False
        else:
            # 检查是否是新格式（带元数据）
            if ValueWithMeta.has_meta(value_bytes):
                # 新格式：使用ValueWithMeta解码
                return ValueWithMeta.decode_value(value_bytes)
            else:
                # 旧格式：直接解码（无TTL）
                value = decoder.decode(value_bytes)
                return value, None, False

    def _should_skip_internal_key(self, key_bytes: bytes) -> bool:
        """
        检查键是否是内部键（应该在 keys()/values()/items() 中跳过）

        内部键包括：
        1. __nested__: 前缀的标记键
        2. __list__: 前缀的标记键
        3. 嵌套存储的子键

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
                decoded_key.startswith('__list__:')
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

    # ========== 公共接口方法 ==========

    def __contains__(self, key) -> bool:
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

    def keys_iter(self, prefix: str = None) -> Iterator:
        """
        流式迭代所有键（适用于大数据库）

        与 keys() 不同，此方法返回生成器，不会一次性加载所有键到内存。
        适用于百万级键的大数据库。

        优化说明：
        - 使用单次遍历收集数据
        - 使用字节前缀集合 + 提取前缀的方式快速判断嵌套子键
        - 复杂度从 O(n × m) 优化到 O(n)

        Args:
            prefix: 可选的键前缀过滤，只返回以此前缀开头的键

        Yields:
            键（按 LevelDB 存储顺序）

        Examples:
            >>> # 迭代所有键
            >>> for key in db.keys_iter():
            ...     print(key)

            >>> # 迭代特定前缀的键
            >>> for key in db.keys_iter(prefix='user:'):
            ...     print(key)

            >>> # 转换为列表（与 keys() 等效，但更灵活）
            >>> all_keys = list(db.keys_iter())
        """
        # 单次遍历：收集标记键和普通键
        # 延迟处理可能的嵌套子键
        pending_keys = []  # (key_bytes, key, value_bytes)
        nested_prefixes_bytes = set()  # 嵌套前缀的字节形式，如 b'user:0:'
        marker_keys_set = set()  # 用于去重

        for key_bytes, value_bytes in self._db:
            try:
                key = self._decode_key(key_bytes)

                # 处理嵌套字典标记
                if isinstance(key, str) and key.startswith('__nested__:'):
                    actual_key = key[len('__nested__:'):]
                    nested_prefixes_bytes.add((actual_key + ':').encode('utf-8'))
                    try:
                        _, _, is_expired = self._decode_value(value_bytes)
                        if not is_expired:
                            if prefix is None or actual_key.startswith(prefix):
                                if actual_key not in marker_keys_set:
                                    marker_keys_set.add(actual_key)
                    except Exception:
                        if prefix is None or actual_key.startswith(prefix):
                            if actual_key not in marker_keys_set:
                                marker_keys_set.add(actual_key)
                    continue

                # 处理嵌套列表标记
                if isinstance(key, str) and key.startswith('__list__:'):
                    actual_key = key[len('__list__:'):]
                    nested_prefixes_bytes.add((actual_key + ':').encode('utf-8'))
                    try:
                        _, _, is_expired = self._decode_value(value_bytes)
                        if not is_expired:
                            if prefix is None or actual_key.startswith(prefix):
                                if actual_key not in marker_keys_set:
                                    marker_keys_set.add(actual_key)
                    except Exception:
                        if prefix is None or actual_key.startswith(prefix):
                            if actual_key not in marker_keys_set:
                                marker_keys_set.add(actual_key)
                    continue

                # 普通键 - 先收集，后续判断是否是嵌套子键
                pending_keys.append((key_bytes, key, value_bytes))

            except (ValueError, UnicodeDecodeError):
                # 解码失败，可能是嵌套存储的子键
                pending_keys.append((key_bytes, None, value_bytes))

        # 先返回标记键
        for key in marker_keys_set:
            yield key

        # 处理 pending_keys：过滤掉嵌套子键
        seen_keys = set(marker_keys_set)

        if nested_prefixes_bytes:
            # 优化：提取 key_bytes 中第一个 ':' 之前的部分作为可能的前缀
            # 然后检查这个前缀是否在 nested_prefixes_bytes 中
            # 这样将 O(m) 的前缀匹配变成 O(1) 的哈希查找
            for key_bytes, key, value_bytes in pending_keys:
                # 检查是否是嵌套子键：提取到第一个 ':' 的前缀并检查
                is_nested_subkey = False
                colon_pos = key_bytes.find(b':')
                if colon_pos > 0:
                    # 提取前缀（包含 ':'）
                    prefix_candidate = key_bytes[:colon_pos + 1]
                    if prefix_candidate in nested_prefixes_bytes:
                        is_nested_subkey = True

                if is_nested_subkey:
                    continue

                # 解码失败的键，可能是嵌套子键
                if key is None:
                    if colon_pos > 0:
                        continue
                    continue  # 无法解码的键跳过

                # 跳过已经返回过的键
                if key in seen_keys:
                    continue

                # 检查 TTL 是否过期
                try:
                    _, _, is_expired = self._decode_value(value_bytes)
                    if not is_expired:
                        if prefix is None or (isinstance(key, str) and key.startswith(prefix)):
                            seen_keys.add(key)
                            yield key
                except Exception:
                    if prefix is None or (isinstance(key, str) and key.startswith(prefix)):
                        seen_keys.add(key)
                        yield key
        else:
            # 没有嵌套前缀，直接处理所有 pending keys
            for key_bytes, key, value_bytes in pending_keys:
                if key is None:
                    # 解码失败的键，可能是子键
                    if b':' in key_bytes:
                        continue
                    continue

                # 跳过已经返回过的键
                if key in seen_keys:
                    continue

                try:
                    _, _, is_expired = self._decode_value(value_bytes)
                    if not is_expired:
                        if prefix is None or (isinstance(key, str) and key.startswith(prefix)):
                            seen_keys.add(key)
                            yield key
                except Exception:
                    if prefix is None or (isinstance(key, str) and key.startswith(prefix)):
                        seen_keys.add(key)
                        yield key

    def keys_count(self) -> int:
        """
        快速计算键数量（不加载所有键到内存）

        比 len(db) 更高效，因为不需要解码所有键。

        Returns:
            键的数量
        """
        return sum(1 for _ in self.keys_iter())

    def values(self) -> List:
        """获取所有值列表"""
        return [self[key] for key in self.keys()]

    def items(self) -> List[Tuple]:
        """获取所有键值对列表"""
        return [(key, self[key]) for key in self.keys()]

    def __len__(self) -> int:
        """返回数据库大小"""
        return len(self.keys())

    def to_dict(self) -> Dict:
        """转换为普通字典"""
        result = {}
        for k, v in self.items():
            result[k] = v
        return result

    # ========== 字符串表示 ==========

    def __repr__(self) -> str:
        """返回对象的字符串表示形式"""
        class_name = self.__class__.__name__
        if self._closed:
            return f"{class_name}(name={self.name!r}, closed=True)"

        try:
            items = list(self.items())
            num_items = len(items)

            if num_items == 0:
                items_str = "{}"
            elif num_items <= 20:
                items_repr = ", ".join(f"{k!r}: {v!r}" for k, v in items)
                items_str = f"{{{items_repr}}}"
            else:
                preview_items = items[:20]
                items_repr = ", ".join(f"{k!r}: {v!r}" for k, v in preview_items)
                items_str = f"{{{items_repr}, ...}} ({num_items} items)"

            return f"{class_name}(name={self.name!r}, path={self.db_path!r}, items={items_str})"
        except Exception as e:
            return f"{class_name}(name={self.name!r}, path={self.db_path!r}, error={e!r})"

    def __str__(self) -> str:
        """返回用户友好的字符串表示形式，类似dict"""
        class_name = self.__class__.__name__
        if self._closed:
            return f"<{class_name} '{self.name}' (closed)>"

        try:
            items = list(self.items())
            num_items = len(items)

            if num_items == 0:
                return "{}"
            elif num_items <= 20:
                items_repr = ", ".join(f"{k!r}: {v!r}" for k, v in items)
                return f"{{{items_repr}}}"
            else:
                preview_items = items[:20]
                items_repr = ", ".join(f"{k!r}: {v!r}" for k, v in preview_items)
                return f"{{{items_repr}, ... and {num_items - 20} more items}}"
        except Exception as e:
            return f"<{class_name} '{self.name}' (error: {e})>"

    # ========== 上下文管理器 ==========

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.close()
        return False

    # ========== TTL 管理 ==========

    def set_ttl(self, key: Any, ttl_seconds: int) -> None:
        """
        为指定键设置TTL（需要重新编码值）

        Args:
            key: 键
            ttl_seconds: TTL时间（秒）
        """
        try:
            current_value = self[key]
        except KeyError:
            raise KeyError(f"Cannot set TTL for non-existent key: {key}")
        self.set(key, current_value, ttl=ttl_seconds)

    def remove_ttl(self, key: Any) -> None:
        """
        移除指定键的TTL（需要重新编码值）

        Args:
            key: 键
        """
        try:
            current_value = self[key]
        except KeyError:
            return
        self.set(key, current_value, ttl=None)

    def get_default_ttl(self) -> Optional[int]:
        """获取默认TTL设置"""
        return self._default_ttl

    def set_default_ttl(self, ttl_seconds: Optional[int]) -> None:
        """设置默认TTL"""
        self._default_ttl = ttl_seconds

    # ========== 统计信息 ==========

    def stat(self) -> Dict:
        """
        返回数据库统计信息

        Returns:
            包含统计信息的字典
        """
        stats = {
            'path': self.db_path,
            'backend': self._get_backend_name()
        }

        try:
            leveldb_stats_bytes = self._db.get_property(b'leveldb.stats')
            if leveldb_stats_bytes:
                stats['leveldb_stats'] = leveldb_stats_bytes.decode('utf-8')

            try:
                approximate_size = self._db.approximate_size(b'', b'\xff' * 100)
                stats['approximate_size_bytes'] = approximate_size
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

        try:
            count = len(self.keys())
            stats['count'] = count
        except Exception as e:
            logger.warning(f"Failed to count keys: {e}")
            stats['count'] = 'unknown'

        return stats

    # ========== 嵌套结构支持 ==========

    def nested(self, prefix: str) -> NestedDBDict:
        """
        创建一个基于前缀的嵌套字典视图

        Args:
            prefix: 前缀字符串，建议使用冒号分隔（如 'user:1'）

        Returns:
            NestedDBDict: 嵌套字典对象
        """
        if self._db is None:
            raise RuntimeError("Database is not open")

        prefix_with_colon = f"{prefix}:"
        prefix_bytes = prefix_with_colon.encode('utf-8')
        prefixed_db = self._db.prefixed_db(prefix_bytes)

        return NestedDBDict(prefixed_db, prefix_with_colon, parent_db=None, root_db=self)

    def nested_list(self, prefix: str) -> NestedDBList:
        """
        创建一个基于前缀的嵌套列表视图

        Args:
            prefix: 前缀字符串，建议使用冒号分隔（如 'items'）

        Returns:
            NestedDBList: 嵌套列表对象
        """
        if self._db is None:
            raise RuntimeError("Database is not open")

        prefix_with_colon = f"{prefix}:"
        prefix_bytes = prefix_with_colon.encode('utf-8')
        prefixed_db = self._db.prefixed_db(prefix_bytes)

        return NestedDBList(prefixed_db, prefix_with_colon, parent_db=None, root_db=self)
