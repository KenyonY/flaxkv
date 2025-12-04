"""
FlaxKV2 带缓存的LevelDB后端实现
在LevelDB基础上增加读缓存支持，减少网络请求和反序列化开销
"""

import os
import time
from typing import Any, Dict, List, Optional, Set

import plyvel

from flaxkv2.core.base_leveldb_dict import BaseLevelDBDict
from flaxkv2.core.nested_structures import NestedDBDict, NestedDBList
from flaxkv2.serialization.value_meta import ValueWithMeta
from flaxkv2.utils.log import get_logger
from flaxkv2.utils.ttl_cleanup import TTLCleanup
from flaxkv2.utils.rwlock import RWLock
from flaxkv2.utils.unified_cache import UnifiedCache
from flaxkv2.instance_manager import db_instance_manager
from flaxkv2.auto_close import db_close_manager
from flaxkv2.config import create_leveldb_options

logger = get_logger(__name__)


class CachedLevelDBDict(BaseLevelDBDict):
    """
    带缓存的LevelDB字典实现

    在LevelDB基础上增加了SimpleLRUCache读缓存，可以显著提升热数据读取性能。

    支持实例复用：同一个数据库路径多次实例化时，会返回已存在的实例（除非指定rebuild=True）。
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
        read_cache_size: int = 1000,
        # TTL自动清理参数
        enable_ttl_cleanup: bool = True,
        cleanup_interval: int = 60,
        cleanup_batch_size: int = 1000,
        # 写缓冲参数
        enable_write_buffer: bool = True,
        write_buffer_size: int = 500,
        write_buffer_flush_interval: int = 60,
        async_flush: bool = False,
        # 性能配置参数
        performance_profile: str = 'balanced',
        lru_cache_size: Optional[int] = None,
        bloom_filter_bits: Optional[int] = None,
        block_size: Optional[int] = None,
        write_buffer_size_leveldb: Optional[int] = None,
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
            auto_nested: 是否自动将字典类型转换为嵌套存储
            read_cache_size: 读缓存大小（条目数量），默认1000
            enable_ttl_cleanup: 是否启用TTL自动清理（默认True）
            cleanup_interval: TTL清理间隔（秒），默认60秒
            cleanup_batch_size: 每次清理扫描的键数量，默认1000
            enable_write_buffer: 是否启用写缓冲（默认True）
            write_buffer_size: 写缓冲区大小（条目数），默认500
            write_buffer_flush_interval: 写缓冲刷新间隔（秒），默认60秒
            async_flush: 是否使用异步flush（默认False）
            performance_profile: 性能配置文件名称
            lru_cache_size: LRU缓存大小（字节）
            bloom_filter_bits: 布隆过滤器位数
            block_size: 数据块大小（字节）
            write_buffer_size_leveldb: LevelDB写缓冲大小（字节）
            max_open_files: 最大打开文件数
            compression: 压缩算法 ('snappy', 'zlib', None)
        """
        # 如果不是新实例，跳过初始化
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
        self._db_lock = RWLock()
        self._default_ttl = default_ttl
        self._auto_nested = auto_nested

        # 统一缓存配置
        self._cache_enabled = read_cache_size > 0 or enable_write_buffer
        self._write_buffer_mode = enable_write_buffer
        self._async_flush = async_flush

        if self._cache_enabled:
            cache_size = max(read_cache_size, write_buffer_size) if enable_write_buffer else read_cache_size
            flush_threshold = write_buffer_size if enable_write_buffer else max(10, read_cache_size // 10)
            flush_interval = write_buffer_flush_interval if enable_write_buffer else 60

            self._cache = UnifiedCache(
                maxsize=cache_size,
                flush_threshold=flush_threshold,
                flush_interval=flush_interval,
                flush_callback=self._flush_unified_cache_callback,
                auto_flush=True,
                async_flush=async_flush
            )

            mode = "异步" if async_flush else "同步"
            if enable_write_buffer:
                logger.debug(f"统一缓存已启用: size={cache_size}, flush_threshold={flush_threshold}, mode={mode}")
                if async_flush:
                    logger.warning("异步flush模式：性能极佳但数据安全风险更大！")
                else:
                    logger.warning("写缓冲模式：进程崩溃可能导致数据丢失！")
            else:
                logger.debug(f"统一缓存已启用（只读优化）: size={cache_size}")
        else:
            self._cache = None
            logger.debug("缓存已禁用（所有操作直接访问数据库）")

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
            write_buffer_size=write_buffer_size_leveldb,
            max_open_files=max_open_files,
        )

        # 记录使用的配置
        logger.debug(f"数据库 '{name}' 使用配置文件: {performance_profile}")
        logger.debug(f"  LRU缓存: {self._leveldb_options['lru_cache_size'] / (1024*1024):.0f} MB")
        logger.debug(f"  写缓冲: {self._leveldb_options['write_buffer_size'] / (1024*1024):.0f} MB")
        logger.debug(f"  布隆过滤器: {self._leveldb_options['bloom_filter_bits']} bits")

        # 初始化数据库
        self._init_db()

        # 启动TTL自动清理
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
        logger.debug(f"数据库实例已注册到实例管理器: {self.db_path}")

        # 注册到自动关闭管理器
        db_close_manager.register(self)
        logger.debug(f"数据库实例已注册到自动关闭管理器: {self.db_path}")

        # 标记初始化完成
        del self._is_new_instance

    def _init_db(self):
        """初始化数据库连接"""
        try:
            self._db = plyvel.DB(self.db_path, **self._leveldb_options)
            logger.info(f"Opened cached LevelDB at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to open cached LevelDB at {self.db_path}: {e}")
            raise

    def _get_backend_name(self) -> str:
        """返回后端名称"""
        return 'cached_leveldb'

    def _flush_unified_cache_callback(self, writes: Dict, deletes: Set):
        """统一缓存刷新回调函数"""
        if len(writes) == 0 and len(deletes) == 0:
            return

        with self._db_lock.write_lock():
            batch = self._db.write_batch()

            for key, (value, ttl) in writes.items():
                key_bytes = self._encode_key(key)
                value_bytes = self._encode_value(value, ttl_seconds=ttl)
                batch.put(key_bytes, value_bytes)

            for key in deletes:
                key_bytes = self._encode_key(key)
                batch.delete(key_bytes)

            batch.write()

        logger.debug(f"Write buffer flushed: {len(writes)} writes, {len(deletes)} deletes")

    def __getitem__(self, key):
        """获取键值（支持统一缓存）"""
        # 特殊键：直接处理
        if isinstance(key, str) and key.startswith('__nested__:'):
            key_bytes = self._encode_key(key)
            with self._db_lock.read_lock():
                value_bytes = self._db.get(key_bytes)
            if value_bytes is None:
                raise KeyError(key)
            value, _, _ = self._decode_value(value_bytes)
            return value

        # 检查统一缓存
        if self._cache_enabled:
            if key in self._cache._delete_keys:
                raise KeyError(key)

            cached = self._cache.get(key)
            if cached is not None:
                return cached

        with self._db_lock.read_lock():
            # 如果启用了自动嵌套，检查是否是嵌套字典或嵌套列表
            if self._auto_nested:
                # 检查是否是嵌套字典
                marker_key = f'__nested__:{key}'
                marker_bytes = self._encode_key(marker_key)
                marker_value = self._db.get(marker_bytes)

                if marker_value is not None:
                    _, expire_time, is_expired = self._decode_value(marker_value)
                    if not is_expired:
                        nested = self.nested(key)
                        if self._cache_enabled:
                            ttl = None
                            if expire_time is not None:
                                ttl = int(max(0, expire_time - time.time()))
                            self._cache.put(key, nested, ttl=ttl, dirty=False)
                        return nested

                # 检查是否是嵌套列表
                list_marker_key = f'__list__:{key}'
                list_marker_bytes = self._encode_key(list_marker_key)
                list_marker_value = self._db.get(list_marker_bytes)

                if list_marker_value is not None:
                    _, expire_time, is_expired = self._decode_value(list_marker_value)
                    if not is_expired:
                        nested_list = self.nested_list(key)
                        if self._cache_enabled:
                            ttl = None
                            if expire_time is not None:
                                ttl = int(max(0, expire_time - time.time()))
                            self._cache.put(key, nested_list, ttl=ttl, dirty=False)
                        return nested_list

            # 普通值
            key_bytes = self._encode_key(key)
            value_bytes = self._db.get(key_bytes)

            if value_bytes is None:
                raise KeyError(key)

            value, expire_time, is_expired = self._decode_value(value_bytes)

            if is_expired:
                pass  # 稍后处理
            else:
                if self._cache_enabled:
                    ttl = None
                    if expire_time is not None:
                        ttl = int(max(0, expire_time - time.time()))
                    self._cache.put(key, value, ttl=ttl, dirty=False)
                return value

        # 处理过期情况
        try:
            del self[key]
        except Exception as e:
            logger.debug(f"Failed to delete expired key {key}: {e}")
        raise KeyError(key)

    def __setitem__(self, key, value):
        """设置键值 - 使用default_ttl"""
        self.set(key, value, ttl=self._default_ttl)

    def set(self, key, value, ttl=None):
        """设置键值（支持指定TTL，支持写缓冲）"""
        # 特殊键：直接处理
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

        # 如果启用了自动嵌套且值是字典
        if self._auto_nested and isinstance(value, dict):
            marker_key = f'__nested__:{key}'
            marker_bytes = self._encode_key(marker_key)
            marker_value = self._encode_value(True, ttl_seconds=ttl)
            with self._db_lock.write_lock():
                self._db.put(marker_bytes, marker_value)

            try:
                list_marker_key = f'__list__:{key}'
                list_marker_bytes = self._encode_key(list_marker_key)
                with self._db_lock.write_lock():
                    self._db.delete(list_marker_bytes)
            except Exception as e:
                logger.debug(f"Failed to delete list marker for {key}: {e}")

            nested = self.nested(key)
            nested.clear()
            for k, v in value.items():
                nested[k] = v

            if self._cache_enabled:
                self._cache.put(key, nested, ttl=ttl, dirty=False)
            return

        # 如果启用了自动嵌套且值是列表
        if self._auto_nested and isinstance(value, list):
            list_marker_key = f'__list__:{key}'
            list_marker_bytes = self._encode_key(list_marker_key)
            list_marker_value = self._encode_value(True, ttl_seconds=ttl)
            with self._db_lock.write_lock():
                self._db.put(list_marker_bytes, list_marker_value)

            try:
                marker_key = f'__nested__:{key}'
                marker_bytes = self._encode_key(marker_key)
                with self._db_lock.write_lock():
                    self._db.delete(marker_bytes)
            except Exception as e:
                logger.debug(f"Failed to delete nested marker for {key}: {e}")

            nested_list = self.nested_list(key)
            nested_list.clear()
            for item in value:
                nested_list.append(item)

            if self._cache_enabled:
                self._cache.put(key, nested_list, ttl=ttl, dirty=False)
            return

        # 非字典非列表：取消标记（如果有）
        if self._auto_nested:
            try:
                marker_key = f'__nested__:{key}'
                marker_bytes = self._encode_key(marker_key)
                with self._db_lock.write_lock():
                    self._db.delete(marker_bytes)
            except Exception as e:
                logger.debug(f"Failed to clear nested marker for {key}: {e}")

            try:
                list_marker_key = f'__list__:{key}'
                list_marker_bytes = self._encode_key(list_marker_key)
                with self._db_lock.write_lock():
                    self._db.delete(list_marker_bytes)
            except Exception as e:
                logger.debug(f"Failed to clear list marker for {key}: {e}")

            if self._cache_enabled:
                self._cache.delete(key)

        # 普通存储
        if self._cache_enabled and self._write_buffer_mode:
            self._cache.put(key, value, ttl=ttl, dirty=True)
        else:
            key_bytes = self._encode_key(key)
            value_bytes = self._encode_value(value, ttl_seconds=ttl)

            with self._db_lock.write_lock():
                self._db.put(key_bytes, value_bytes)

            if self._cache_enabled:
                self._cache.put(key, value, ttl=ttl, dirty=False)

    def __delitem__(self, key):
        """删除键（支持统一缓存）"""
        # 特殊键：直接处理
        if isinstance(key, str) and key.startswith('__nested__:'):
            key_bytes = self._encode_key(key)
            with self._db_lock.write_lock():
                self._db.delete(key_bytes)
            return

        # 统一缓存模式：标记为删除
        if self._cache_enabled:
            self._cache.delete(key)
            return

        # 直接删除模式
        if self._auto_nested:
            marker_key = f'__nested__:{key}'
            marker_bytes = self._encode_key(marker_key)

            with self._db_lock.write_lock():
                marker_value = self._db.get(marker_bytes)

            if marker_value is not None:
                nested = self.nested(key)
                nested.clear()
                with self._db_lock.write_lock():
                    self._db.delete(marker_bytes)
                if self._cache_enabled:
                    self._cache.delete(key)
                return

            list_marker_key = f'__list__:{key}'
            list_marker_bytes = self._encode_key(list_marker_key)

            with self._db_lock.write_lock():
                list_marker_value = self._db.get(list_marker_bytes)

            if list_marker_value is not None:
                nested_list = self.nested_list(key)
                nested_list.clear()
                with self._db_lock.write_lock():
                    self._db.delete(list_marker_bytes)
                if self._cache_enabled:
                    self._cache.delete(key)
                return

        # 普通值
        key_bytes = self._encode_key(key)

        with self._db_lock.write_lock():
            value_bytes = self._db.get(key_bytes)
            if value_bytes is None:
                raise KeyError(key)
            self._db.delete(key_bytes)

        if self._cache_enabled:
            self._cache.delete(key)

    def update(self, d: Dict[Any, Any]):
        """批量更新多个键值对"""
        with self._db_lock.write_lock():
            batch = self._db.write_batch()

            for key, value in d.items():
                key_bytes = self._encode_key(key)
                value_bytes = self._encode_value(value, ttl_seconds=self._default_ttl)
                batch.put(key_bytes, value_bytes)

            batch.write()

        if self._cache_enabled:
            for key in d.keys():
                self._cache.delete(key)

    def keys(self) -> List:
        """获取所有键列表"""
        keys_set = set()

        with self._db_lock.read_lock():
            # 第一遍：收集所有嵌套前缀（以字节形式存储，用于后续过滤子键）
            nested_prefixes_bytes = set()

            for key_bytes, value_bytes in self._db:
                try:
                    key = self._decode_key(key_bytes)

                    if isinstance(key, str) and key.startswith('__nested__:'):
                        actual_key = key[len('__nested__:'):]
                        nested_prefixes_bytes.add((actual_key + ':').encode('utf-8'))
                        try:
                            _, _, is_expired = self._decode_value(value_bytes)
                            if not is_expired:
                                keys_set.add(actual_key)
                        except Exception:
                            keys_set.add(actual_key)

                    elif isinstance(key, str) and key.startswith('__list__:'):
                        actual_key = key[len('__list__:'):]
                        nested_prefixes_bytes.add((actual_key + ':').encode('utf-8'))
                        try:
                            _, _, is_expired = self._decode_value(value_bytes)
                            if not is_expired:
                                keys_set.add(actual_key)
                        except Exception:
                            keys_set.add(actual_key)

                except (ValueError, UnicodeDecodeError):
                    pass

            # 第二遍：收集普通用户键（过滤掉嵌套子键）
            for key_bytes, value_bytes in self._db:
                # 先检查原始字节是否是嵌套子键（以已知嵌套前缀开头）
                is_nested_subkey = any(key_bytes.startswith(prefix) for prefix in nested_prefixes_bytes)
                if is_nested_subkey:
                    continue

                try:
                    key = self._decode_key(key_bytes)

                    # 跳过标记键（已处理）
                    if isinstance(key, str) and (key.startswith('__nested__:') or key.startswith('__list__:')):
                        continue

                    # 正常的用户键 - 检查TTL是否过期
                    try:
                        _, _, is_expired = self._decode_value(value_bytes)
                        if not is_expired:
                            keys_set.add(key)
                    except Exception:
                        keys_set.add(key)

                except (ValueError, UnicodeDecodeError):
                    # 解码失败，可能是嵌套存储的子键（没有类型前缀）
                    try:
                        key_str = key_bytes.decode('utf-8')
                        # 跳过嵌套子键
                        if ':' in key_str:
                            continue
                    except UnicodeDecodeError as e:
                        logger.warning(f"无法解码键 {key_bytes[:20]}...: {e}")

        # 添加缓存中的 dirty keys
        if self._cache_enabled and self._cache is not None:
            with self._cache._lock:
                for key in self._cache._cache.keys():
                    if isinstance(key, str) and (key.startswith('__nested__:') or key.startswith('__list__:')):
                        continue
                    if key in self._cache._dirty_keys:
                        entry = self._cache._cache.get(key)
                        if entry and not entry.is_expired():
                            keys_set.add(key)

        # 排除已删除的键
        if self._cache_enabled and self._cache is not None:
            with self._cache._lock:
                keys_set -= self._cache._delete_keys

        return list(keys_set)

    def close(self):
        """关闭数据库"""
        if self._closed:
            return

        # 停止统一缓存
        if self._cache_enabled and self._cache is not None:
            logger.debug(f"Stopping unified cache for {self.name}...")
            self._cache.stop()

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

                logger.debug(f"Cached LevelDB connection closed: {self.name}")

                db_instance_manager.unregister_instance(self.db_path)
                logger.debug(f"数据库实例已从实例管理器移除: {self.db_path}")

                db_close_manager.unregister(self)
                logger.debug(f"数据库实例已从自动关闭管理器移除: {self.db_path}")
            except Exception as e:
                logger.error(f"Error closing cached LevelDB: {self.name}, error: {e}")

    def flush(self):
        """手动刷新统一缓存"""
        if self._cache_enabled and self._cache is not None:
            self._cache.flush()
            logger.debug(f"Manual flush triggered for {self.name}")

    def get_ttl(self, key: Any) -> Optional[int]:
        """获取指定键的剩余TTL"""
        if self._auto_nested:
            try:
                marker_key = f'__nested__:{key}'
                marker_bytes = self._encode_key(marker_key)
                with self._db_lock.read_lock():
                    marker_value = self._db.get(marker_bytes)

                if marker_value is not None:
                    _, expire_time, is_expired = self._decode_value(marker_value)
                    if expire_time is None:
                        return None
                    if is_expired:
                        return 0
                    remaining = int(expire_time - time.time())
                    return max(0, remaining)
            except Exception as e:
                logger.debug(f"Error checking nested TTL for {key}: {e}")

        try:
            key_bytes = self._encode_key(key)
            with self._db_lock.read_lock():
                value_bytes = self._db.get(key_bytes)

            if value_bytes is None:
                return None

            _, expire_time, is_expired = self._decode_value(value_bytes)

            if expire_time is None:
                return None
            if is_expired:
                return 0

            remaining = int(expire_time - time.time())
            return max(0, remaining)
        except Exception:
            return None

    def cleanup_expired(self) -> int:
        """清理所有过期的键"""
        count = 0
        keys_to_delete = []

        with self._db_lock.write_lock():
            for key_bytes, value_bytes in self._db:
                if ValueWithMeta.has_meta(value_bytes):
                    if ValueWithMeta.is_expired_fast(value_bytes):
                        keys_to_delete.append(key_bytes)

            if keys_to_delete:
                batch = self._db.write_batch()
                for key_bytes in keys_to_delete:
                    batch.delete(key_bytes)
                    count += 1
                batch.write()

        if self._cache_enabled and keys_to_delete:
            for key_bytes in keys_to_delete:
                try:
                    key = self._decode_key(key_bytes)
                    self._cache.delete(key)
                except Exception as e:
                    logger.debug(f"Error removing expired key from cache: {e}")

        return count
