"""
FlaxKV2 带缓存的LevelDB后端实现
在LevelDB基础上增加读缓存支持，减少网络请求和反序列化开销
"""

import os
import threading
from typing import Any, Dict, List, Tuple, Optional

import plyvel

from flaxkv2.serialization import encoder, decoder
from flaxkv2.serialization.value_meta import ValueWithMeta
from flaxkv2.core.nested_structures import NestedDBDict, NestedDBList
from flaxkv2.utils.log import get_logger
from flaxkv2.utils.ttl_cleanup import TTLCleanup
from flaxkv2.utils.rwlock import RWLock
from flaxkv2.utils.simple_lru_cache import SimpleLRUCache
from flaxkv2.instance_manager import db_instance_manager
from flaxkv2.config import create_leveldb_options

logger = get_logger(__name__)


class CachedLevelDBDict:
    """
    带缓存的LevelDB字典实现

    在LevelDB基础上增加了SimpleLRUCache读缓存，可以显著提升热数据读取性能。

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
        read_cache_size: int = 1000,  # 默认启用缓存
        # TTL自动清理参数
        enable_ttl_cleanup: bool = True,
        cleanup_interval: int = 60,
        cleanup_batch_size: int = 1000,
        # 性能配置参数
        performance_profile: str = 'balanced',
        lru_cache_size: Optional[int] = None,
        bloom_filter_bits: Optional[int] = None,
        block_size: Optional[int] = None,
        write_buffer_size: Optional[int] = None,
        max_open_files: Optional[int] = None,
        compression: str = 'snappy',
        **kwargs
    ):
        """
        初始化带缓存的LevelDB字典

        Args:
            name: 数据库名称
            path: 数据库路径
            rebuild: 是否重建数据库
            create_if_missing: 如果数据库不存在是否创建
            raw: 是否使用原始模式（不进行序列化）
            default_ttl: 默认TTL（秒），为None表示不使用TTL
            auto_nested: 是否自动将字典类型转换为嵌套存储（默认False，保持性能基准纯粹）
            read_cache_size: 读缓存大小（条目数量），默认1000，设为0禁用缓存
                推荐值：1000-10000，具体取决于工作负载
            enable_ttl_cleanup: 是否启用TTL自动清理（默认True）
            cleanup_interval: TTL清理间隔（秒），默认60秒
            cleanup_batch_size: 每次清理扫描的键数量，默认1000

            performance_profile: 性能配置文件名称，可选值：
                - 'balanced' (默认): 通用平衡配置
                - 'read_optimized': 读密集型优化
                - 'write_optimized': 写密集型优化
                - 'memory_constrained': 内存受限配置
                - 'large_database': 大数据库配置(>100GB)
                - 'ml_workload': 机器学习/科学计算配置
            lru_cache_size: LRU缓存大小（字节），覆盖profile中的值
            bloom_filter_bits: 布隆过滤器位数，覆盖profile中的值
            block_size: 数据块大小（字节），覆盖profile中的值
            write_buffer_size: 写缓冲大小（字节），覆盖profile中的值
            max_open_files: 最大打开文件数，覆盖profile中的值
            compression: 压缩算法 ('snappy', 'zlib', None)

        Examples:
            >>> # 使用默认配置
            >>> db = CachedLevelDBDict("mydb", "./data")

            >>> # 使用读优化配置
            >>> db = CachedLevelDBDict("mydb", "./data", performance_profile='read_optimized')

            >>> # 在默认配置基础上自定义缓存大小
            >>> db = CachedLevelDBDict("mydb", "./data", lru_cache_size=512*1024*1024)

            >>> # 完全自定义（基于balanced）
            >>> db = CachedLevelDBDict("mydb", "./data",
            ...                      lru_cache_size=300*1024*1024,
            ...                      bloom_filter_bits=12)
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
        self._db_lock = RWLock()  # 使用读写锁提升并发性能
        self._default_ttl = default_ttl
        self._auto_nested = auto_nested

        # 初始化读缓存
        self._cache_enabled = read_cache_size > 0
        if self._cache_enabled:
            self._cache = SimpleLRUCache(maxsize=read_cache_size)
            logger.debug(f"读缓存已启用: {read_cache_size} 条目")
        else:
            self._cache = None
            logger.debug("读缓存已禁用")

        # 准备数据库目录
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        # 使用配置模块创建LevelDB选项
        self._leveldb_options = create_leveldb_options(
            create_if_missing=create_if_missing,
            compression=compression,
            performance_profile=performance_profile,
            lru_cache_size=lru_cache_size,
            bloom_filter_bits=bloom_filter_bits,
            block_size=block_size,
            write_buffer_size=write_buffer_size,
            max_open_files=max_open_files,
        )

        # 记录使用的配置（便于调试）
        logger.debug(f"数据库 '{name}' 使用配置文件: {performance_profile}")
        logger.debug(f"  LRU缓存: {self._leveldb_options['lru_cache_size'] / (1024*1024):.0f} MB")
        logger.debug(f"  写缓冲: {self._leveldb_options['write_buffer_size'] / (1024*1024):.0f} MB")
        logger.debug(f"  布隆过滤器: {self._leveldb_options['bloom_filter_bits']} bits")

        # 初始化数据库
        self._init_db()

        # 启动TTL自动清理（如果启用）
        self._ttl_cleanup = None
        if enable_ttl_cleanup:
            self._ttl_cleanup = TTLCleanup(
                db=self,
                cleanup_interval=cleanup_interval,
                batch_size=cleanup_batch_size
            )
            self._ttl_cleanup.start()
            logger.debug(f"TTL auto-cleanup enabled: interval={cleanup_interval}s, batch_size={cleanup_batch_size}")

        # 注册到实例管理器
        db_instance_manager.register_instance(self.db_path, self)
        logger.debug(f"数据库实例已注册: {self.db_path}")

        # 标记初始化完成，删除临时标志
        del self._is_new_instance

    def _init_db(self):
        """初始化数据库连接"""
        try:
            self._db = plyvel.DB(self.db_path, **self._leveldb_options)
            logger.info(f"Opened cached LevelDB at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to open cached LevelDB at {self.db_path}: {e}")
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

    def _encode_value(self, value, ttl_seconds=None):
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

    def _decode_value(self, value_bytes):
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

    def __getitem__(self, key):
        """获取键值（支持缓存）"""
        # 特殊键：直接处理（无缓存、无TTL检查）
        if isinstance(key, str) and key.startswith('__nested__:'):
            key_bytes = self._encode_key(key)
            with self._db_lock.read_lock():
                value_bytes = self._db.get(key_bytes)
            if value_bytes is None:
                raise KeyError(key)
            value, _, _ = self._decode_value(value_bytes)
            return value

        with self._db_lock.read_lock():
            # 1. 检查缓存（自动检查TTL）
            if self._cache_enabled:
                cached = self._cache.get(key)
                if cached is not None:
                    return cached

            # 2. 如果启用了自动嵌套，检查是否是嵌套字典或嵌套列表
            if self._auto_nested:
                # 检查是否是嵌套字典
                marker_key = f'__nested__:{key}'
                marker_bytes = self._encode_key(marker_key)
                marker_value = self._db.get(marker_bytes)

                if marker_value is not None:
                    # 解码marker并检查TTL
                    _, expire_time, is_expired = self._decode_value(marker_value)
                    if is_expired:
                        # 嵌套字典已过期：删除所有相关键（需要write_lock）
                        pass  # 稍后在write_lock中处理
                    else:
                        # 是嵌套字典且未过期，创建NestedDBDict并缓存
                        nested = self.nested(key)
                        if self._cache_enabled:
                            self._cache.put(key, nested, expire_time)
                        return nested

                # 检查是否是嵌套列表
                list_marker_key = f'__list__:{key}'
                list_marker_bytes = self._encode_key(list_marker_key)
                list_marker_value = self._db.get(list_marker_bytes)

                if list_marker_value is not None:
                    # 解码marker并检查TTL
                    _, expire_time, is_expired = self._decode_value(list_marker_value)
                    if is_expired:
                        # 嵌套列表已过期：删除所有相关键（需要write_lock）
                        pass  # 稍后在write_lock中处理
                    else:
                        # 是嵌套列表且未过期，创建NestedDBList并缓存
                        nested_list = self.nested_list(key)
                        if self._cache_enabled:
                            self._cache.put(key, nested_list, expire_time)
                        return nested_list

            # 3. 普通值：读取并检查TTL
            key_bytes = self._encode_key(key)
            value_bytes = self._db.get(key_bytes)

            if value_bytes is None:
                raise KeyError(key)

            # 解码并检查TTL
            value, expire_time, is_expired = self._decode_value(value_bytes)

            if is_expired:
                # 已过期：删除键并抛出异常（需要write_lock）
                pass  # 稍后在write_lock中处理
            else:
                # 未过期：加入缓存并返回
                if self._cache_enabled:
                    self._cache.put(key, value, expire_time)
                return value

        # 处理过期情况（需要write_lock删除）
        try:
            del self[key]
        except:
            pass
        raise KeyError(key)

    def __setitem__(self, key, value):
        """设置键值 - 直接写入数据库（使用default_ttl）"""
        self.set(key, value, ttl=self._default_ttl)

    def set(self, key, value, ttl=None):
        """
        设置键值（支持指定TTL，写入时更新缓存）

        Args:
            key: 键
            value: 值
            ttl: TTL秒数（None表示无TTL）
        """
        from flaxkv2.core.nested_structures import NestedDBDict

        # 特殊键：直接处理（无缓存、无TTL）
        if isinstance(key, str) and key.startswith('__nested__:'):
            key_bytes = self._encode_key(key)
            value_bytes = self._encode_value(value)
            with self._db_lock.write_lock():
                self._db.put(key_bytes, value_bytes)
            return

        # 如果值是 NestedDBDict，转换为普通字典
        if isinstance(value, NestedDBDict):
            value = value.to_dict()

        # 如果值是 NestedDBList，转换为普通列表
        if isinstance(value, NestedDBList):
            value = value.to_list()

        # 计算过期时间
        expire_time = None
        if ttl is not None:
            import time
            expire_time = time.time() + ttl

        # 如果启用了自动嵌套且值是字典
        if self._auto_nested and isinstance(value, dict):
            # 1. 标记为嵌套字典（带TTL）
            marker_key = f'__nested__:{key}'
            marker_bytes = self._encode_key(marker_key)
            marker_value = self._encode_value(True, ttl_seconds=ttl)
            with self._db_lock.write_lock():
                self._db.put(marker_bytes, marker_value)

            # 清除可能存在的列表标记
            try:
                list_marker_key = f'__list__:{key}'
                list_marker_bytes = self._encode_key(list_marker_key)
                with self._db_lock.write_lock():
                    self._db.delete(list_marker_bytes)
            except:
                pass

            # 2. 创建 nested，递归写入
            nested = self.nested(key)
            nested.clear()
            for k, v in value.items():
                nested[k] = v  # 递归

            # 3. 更新缓存（缓存NestedDBDict实例）
            if self._cache_enabled:
                self._cache.put(key, nested, expire_time)
            return

        # 如果启用了自动嵌套且值是列表
        if self._auto_nested and isinstance(value, list):
            # 1. 标记为嵌套列表（带TTL）
            list_marker_key = f'__list__:{key}'
            list_marker_bytes = self._encode_key(list_marker_key)
            list_marker_value = self._encode_value(True, ttl_seconds=ttl)
            with self._db_lock.write_lock():
                self._db.put(list_marker_bytes, list_marker_value)

            # 清除可能存在的字典标记
            try:
                marker_key = f'__nested__:{key}'
                marker_bytes = self._encode_key(marker_key)
                with self._db_lock.write_lock():
                    self._db.delete(marker_bytes)
            except:
                pass

            # 2. 创建 nested_list，递归写入
            nested_list = self.nested_list(key)
            nested_list.clear()
            for item in value:
                nested_list.append(item)  # 递归

            # 3. 更新缓存（缓存NestedDBList实例）
            if self._cache_enabled:
                self._cache.put(key, nested_list, expire_time)
            return

        # 非字典非列表或未启用自动嵌套：取消标记（如果有）
        if self._auto_nested:
            try:
                marker_key = f'__nested__:{key}'
                marker_bytes = self._encode_key(marker_key)
                with self._db_lock.write_lock():
                    self._db.delete(marker_bytes)
            except:
                pass

            try:
                list_marker_key = f'__list__:{key}'
                list_marker_bytes = self._encode_key(list_marker_key)
                with self._db_lock.write_lock():
                    self._db.delete(list_marker_bytes)
            except:
                pass

            # 删除可能存在的嵌套对象缓存
            if self._cache_enabled:
                self._cache.delete(key)

        # 普通存储（带TTL）
        key_bytes = self._encode_key(key)
        value_bytes = self._encode_value(value, ttl_seconds=ttl)

        with self._db_lock.write_lock():
            self._db.put(key_bytes, value_bytes)

        # 更新缓存（Write-through）
        if self._cache_enabled:
            self._cache.put(key, value, expire_time)

    def __delitem__(self, key):
        """删除键（同时删除缓存）"""
        # 特殊键：直接处理（无缓存）
        if isinstance(key, str) and key.startswith('__nested__:'):
            key_bytes = self._encode_key(key)
            with self._db_lock.write_lock():
                self._db.delete(key_bytes)
            return

        # 如果启用了自动嵌套，检查是否是嵌套字典或嵌套列表
        if self._auto_nested:
            # 检查是否是嵌套字典
            marker_key = f'__nested__:{key}'
            marker_bytes = self._encode_key(marker_key)

            with self._db_lock.write_lock():
                marker_value = self._db.get(marker_bytes)

            if marker_value is not None:
                # 是嵌套字典，递归删除所有子键
                nested = self.nested(key)
                nested.clear()
                # 删除标记（TTL已内嵌在marker中，无需单独删除）
                with self._db_lock.write_lock():
                    self._db.delete(marker_bytes)

                # 删除缓存
                if self._cache_enabled:
                    self._cache.delete(key)
                return

            # 检查是否是嵌套列表
            list_marker_key = f'__list__:{key}'
            list_marker_bytes = self._encode_key(list_marker_key)

            with self._db_lock.write_lock():
                list_marker_value = self._db.get(list_marker_bytes)

            if list_marker_value is not None:
                # 是嵌套列表，递归删除所有元素
                nested_list = self.nested_list(key)
                nested_list.clear()
                # 删除标记（TTL已内嵌在marker中，无需单独删除）
                with self._db_lock.write_lock():
                    self._db.delete(list_marker_bytes)

                # 删除缓存
                if self._cache_enabled:
                    self._cache.delete(key)
                return

        # 普通值
        key_bytes = self._encode_key(key)

        with self._db_lock.write_lock():
            # 检查键是否存在
            value_bytes = self._db.get(key_bytes)
            if value_bytes is None:
                raise KeyError(key)

            # 删除键（TTL已内嵌在value中，无需单独删除）
            self._db.delete(key_bytes)

        # 删除缓存
        if self._cache_enabled:
            self._cache.delete(key)

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
        """批量更新多个键值对（使用default_ttl）"""
        with self._db_lock.write_lock():
            batch = self._db.write_batch()

            for key, value in d.items():
                key_bytes = self._encode_key(key)
                # 使用ValueWithMeta编码，直接嵌入default_ttl
                value_bytes = self._encode_value(value, ttl_seconds=self._default_ttl)
                batch.put(key_bytes, value_bytes)

            batch.write()

        # 清除缓存中的相关键
        if self._cache_enabled:
            for key in d.keys():
                self._cache.delete(key)

    def _should_skip_internal_key(self, key_bytes: bytes) -> bool:
        """
        检查键是否是内部键（应该在 keys()/values()/items() 中跳过）

        内部键包括：
        1. __nested__: 前缀的标记键（已编码，如 b's__nested__:config'）
        2. __list__: 前缀的标记键（已编码，如 b's__list__:items'）
        3. 嵌套存储的子键（通过 prefixed_db 创建，没有类型标识前缀，如 b'config:database:host'）

        注意：TTL 信息已经通过 ValueWithMeta 直接编码到 value 字节流中，不再使用单独的键存储

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

    def keys(self) -> List:
        """
        获取所有键列表

        注意：会自动过滤已过期的TTL键
        """
        keys = []
        with self._db_lock.read_lock():
            for key_bytes, value_bytes in self._db:
                # 先尝试解码
                try:
                    key = self._decode_key(key_bytes)

                    # 检查是否是嵌套字典标记键
                    if isinstance(key, str) and key.startswith('__nested__:'):
                        # 提取实际的键名（去掉 '__nested__:' 前缀）
                        actual_key = key[len('__nested__:'):]
                        # 只添加顶层嵌套键（不包含进一步的 ':'）
                        if ':' not in actual_key:
                            # 检查TTL是否过期（从marker的value中提取）
                            try:
                                _, _, is_expired = self._decode_value(value_bytes)
                                if not is_expired:
                                    keys.append(actual_key)
                            except:
                                # 解码失败，保守处理：添加键
                                keys.append(actual_key)
                        continue

                    # 检查是否是嵌套列表标记键
                    if isinstance(key, str) and key.startswith('__list__:'):
                        # 提取实际的键名（去掉 '__list__:' 前缀）
                        actual_key = key[len('__list__:'):]
                        # 只添加顶层嵌套键（不包含进一步的 ':'）
                        if ':' not in actual_key:
                            # 检查TTL是否过期（从marker的value中提取）
                            try:
                                _, _, is_expired = self._decode_value(value_bytes)
                                if not is_expired:
                                    keys.append(actual_key)
                            except:
                                # 解码失败，保守处理：添加键
                                keys.append(actual_key)
                        continue

                    # 正常的用户键 - 检查TTL是否过期
                    try:
                        _, _, is_expired = self._decode_value(value_bytes)
                        if not is_expired:
                            keys.append(key)
                    except:
                        # 解码失败，保守处理：添加键
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
            return f"CachedLevelDBDict(name={self.name!r}, closed=True)"

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

            return f"CachedLevelDBDict(name={self.name!r}, path={self.db_path!r}, items={items_str})"
        except Exception as e:
            return f"CachedLevelDBDict(name={self.name!r}, path={self.db_path!r}, error={e!r})"

    def __str__(self):
        """返回用户友好的字符串表示形式，类似dict"""
        if self._closed:
            return f"<CachedLevelDBDict '{self.name}' (closed)>"

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
            return f"<CachedLevelDBDict '{self.name}' (error: {e})>"

    def to_dict(self) -> Dict:
        """转换为普通字典"""
        result = {}
        for k, v in self.items():
            result[k] = v
        return result
    
    def close(self):
        """关闭数据库（清理缓存）"""
        if self._closed:
            return

        # 停止TTL自动清理线程
        if self._ttl_cleanup is not None:
            self._ttl_cleanup.stop()
            self._ttl_cleanup = None

        if self._db is not None:
            try:
                with self._db_lock.write_lock():
                    self._db.close()
                    self._db = None
                    self._closed = True

                    # 清理缓存
                    if self._cache_enabled:
                        self._cache.clear()
                        logger.debug(f"缓存已清理: {self.name}")

                logger.debug(f"Cached LevelDB connection closed: {self.name}")

                # 从实例管理器中移除
                db_instance_manager.unregister_instance(self.db_path)
                logger.debug(f"数据库实例已从管理器移除: {self.db_path}")
            except Exception as e:
                logger.error(f"Error closing cached LevelDB: {self.name}, error: {e}")

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
            'backend': 'cached_leveldb'
        }

        # 获取LevelDB内置统计信息
        try:
            with self._db_lock.read_lock():
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
        为指定键设置TTL（需要重新编码值）

        注意：这会重新编码值，相当于 db.set(key, db[key], ttl=ttl_seconds)

        Args:
            key: 键
            ttl_seconds: TTL时间（秒）
        """
        # 读取当前值
        try:
            current_value = self[key]
        except KeyError:
            raise KeyError(f"Cannot set TTL for non-existent key: {key}")

        # 重新写入（带TTL）
        self.set(key, current_value, ttl=ttl_seconds)

    def get_ttl(self, key: Any) -> Optional[int]:
        """
        获取指定键的剩余TTL

        Args:
            key: 键

        Returns:
            剩余TTL秒数，如果没有设置TTL返回None
        """
        # 对于auto_nested的键，TTL存储在marker中
        if self._auto_nested:
            try:
                marker_key = f'__nested__:{key}'
                marker_bytes = self._encode_key(marker_key)
                with self._db_lock.read_lock():
                    marker_value = self._db.get(marker_bytes)

                if marker_value is not None:
                    # 是嵌套字典，从marker提取TTL
                    _, expire_time, is_expired = self._decode_value(marker_value)

                    if expire_time is None:
                        return None

                    if is_expired:
                        return 0

                    import time
                    remaining = int(expire_time - time.time())
                    return max(0, remaining)
            except:
                pass

        # 普通键或非嵌套字典：从value提取TTL
        try:
            key_bytes = self._encode_key(key)
            with self._db_lock.read_lock():
                value_bytes = self._db.get(key_bytes)

            if value_bytes is None:
                return None  # 键不存在，返回None

            # 解码并提取TTL
            _, expire_time, is_expired = self._decode_value(value_bytes)

            if expire_time is None:
                return None

            if is_expired:
                return 0

            import time
            remaining = int(expire_time - time.time())
            return max(0, remaining)
        except Exception:
            return None

    def remove_ttl(self, key: Any) -> None:
        """
        移除指定键的TTL（需要重新编码值）

        注意：这会重新编码值，相当于 db.set(key, db[key], ttl=None)

        Args:
            key: 键
        """
        # 读取当前值
        try:
            current_value = self[key]
        except KeyError:
            # 键不存在，无需操作
            return

        # 重新写入（无TTL）
        self.set(key, current_value, ttl=None)

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

        扫描所有键，检查ValueWithMeta中的TTL，删除过期的键。

        Returns:
            清理的键数量
        """
        count = 0
        keys_to_delete = []

        with self._db_lock.write_lock():
            # 扫描所有键
            for key_bytes, value_bytes in self._db:
                # 检查是否包含TTL元数据并已过期
                if ValueWithMeta.has_meta(value_bytes):
                    if ValueWithMeta.is_expired_fast(value_bytes):
                        keys_to_delete.append(key_bytes)

            # 批量删除
            if keys_to_delete:
                batch = self._db.write_batch()
                for key_bytes in keys_to_delete:
                    batch.delete(key_bytes)
                    count += 1
                batch.write()

        # 清除缓存中的过期键
        if self._cache_enabled and keys_to_delete:
            for key_bytes in keys_to_delete:
                try:
                    key = self._decode_key(key_bytes)
                    self._cache.delete(key)
                except:
                    pass

        return count

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

    def nested_list(self, prefix: str) -> NestedDBList:
        """
        创建一个基于前缀的嵌套列表视图

        这是解决嵌套列表频繁序列化问题的推荐方案。
        使用 NestedDBList 可以让每个元素独立存储和访问，避免整个列表的序列化/反序列化。

        性能优势：
        - 修改单个元素只需序列化该元素的值
        - 读取单个元素只需反序列化该元素的值
        - 利用 LevelDB 的前缀查询能力高效迭代

        使用示例：
            # 创建嵌套列表
            items = db.nested_list('items')

            # 添加元素（每个元素独立存储）
            items.append('item1')
            items.append('item2')
            items.append('item3')

            # 高效修改（只序列化该元素的值）
            items[1] = 'modified_item2'

            # 高效读取（只反序列化该元素的值）
            print(items[1])

            # 迭代所有元素
            for item in items:
                print(item)

        Args:
            prefix: 前缀字符串，建议使用冒号分隔（如 'items'）

        Returns:
            NestedDBList: 嵌套列表对象

        注意：
            - 前缀会自动添加冒号分隔符，实际存储的键格式为 'prefix:index'
            - NestedDBList 支持常用的列表接口（append, extend, pop, insert 等）
            - 数据直接写入 LevelDB，不使用缓冲机制
        """
        # 确保数据库已打开
        if self._db is None:
            raise RuntimeError("Database is not open")

        # 创建带前缀的数据库视图
        prefix_with_colon = f"{prefix}:"
        prefix_bytes = prefix_with_colon.encode('utf-8')
        prefixed_db = self._db.prefixed_db(prefix_bytes)

        # 返回 NestedDBList 对象，传入 root_db 以支持递归嵌套
        return NestedDBList(prefixed_db, prefix_with_colon, parent_db=None, root_db=self)
