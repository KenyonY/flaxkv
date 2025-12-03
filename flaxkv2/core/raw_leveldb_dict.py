"""
FlaxKV2 原始LevelDB后端实现（无缓冲）
用于性能对比测试
"""

import os
import time
from typing import Any, Dict, List, Optional

import plyvel

from flaxkv2.core.base_leveldb_dict import BaseLevelDBDict
from flaxkv2.core.nested_structures import NestedDBDict, NestedDBList
from flaxkv2.serialization.value_meta import ValueWithMeta
from flaxkv2.utils.log import get_logger
from flaxkv2.utils.ttl_cleanup import TTLCleanup
from flaxkv2.instance_manager import db_instance_manager
from flaxkv2.auto_close import db_close_manager
from flaxkv2.config import create_leveldb_options

logger = get_logger(__name__)


class RawLevelDBDict(BaseLevelDBDict):
    """
    原始LevelDB字典实现（无缓冲机制）

    直接写入LevelDB，不使用任何缓冲区、缓存、索引等高级功能。

    线程安全：
    - 此类是线程安全的，因为 LevelDB 本身已经内部处理了多线程并发访问的同步
    - 多个线程可以安全地同时读写同一个 RawLevelDBDict 实例
    - 无需外部锁保护

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
        初始化原始LevelDB字典

        Args:
            name: 数据库名称
            path: 数据库路径
            rebuild: 是否重建数据库
            create_if_missing: 如果数据库不存在是否创建
            raw: 是否使用原始模式（不进行序列化）
            default_ttl: 默认TTL（秒），为None表示不使用TTL
            auto_nested: 是否自动将字典类型转换为嵌套存储
            enable_ttl_cleanup: 是否启用TTL自动清理（默认True）
            cleanup_interval: TTL清理间隔（秒），默认60秒
            cleanup_batch_size: 每次清理扫描的键数量，默认1000
            performance_profile: 性能配置文件名称
            lru_cache_size: LRU缓存大小（字节）
            bloom_filter_bits: 布隆过滤器位数
            block_size: 数据块大小（字节）
            write_buffer_size: 写缓冲大小（字节）
            max_open_files: 最大打开文件数
            compression: 压缩算法 ('snappy', 'zlib', None)
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
        self._default_ttl = default_ttl
        self._auto_nested = auto_nested

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
            logger.info(f"Opened raw LevelDB at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to open raw LevelDB at {self.db_path}: {e}")
            raise

    def _get_backend_name(self) -> str:
        """返回后端名称"""
        return 'raw_leveldb'

    def __getitem__(self, key):
        """获取键值"""
        # 特殊键：直接处理（无TTL检查）
        if isinstance(key, str) and key.startswith('__nested__:'):
            key_bytes = self._encode_key(key)
            value_bytes = self._db.get(key_bytes)
            if value_bytes is None:
                raise KeyError(key)
            value, _, _ = self._decode_value(value_bytes)
            return value

        # 如果启用了自动嵌套，检查是否是嵌套字典或嵌套列表
        if self._auto_nested:
            # 检查是否是嵌套字典
            marker_key = f'__nested__:{key}'
            marker_bytes = self._encode_key(marker_key)
            marker_value = self._db.get(marker_bytes)

            if marker_value is not None:
                _, expire_time, is_expired = self._decode_value(marker_value)
                if is_expired:
                    try:
                        del self[key]
                    except Exception as e:
                        logger.debug(f"Failed to delete expired nested key {key}: {e}")
                    raise KeyError(key)
                else:
                    return self.nested(key)

            # 检查是否是嵌套列表
            list_marker_key = f'__list__:{key}'
            list_marker_bytes = self._encode_key(list_marker_key)
            list_marker_value = self._db.get(list_marker_bytes)

            if list_marker_value is not None:
                _, expire_time, is_expired = self._decode_value(list_marker_value)
                if is_expired:
                    try:
                        del self[key]
                    except Exception as e:
                        logger.debug(f"Failed to delete expired list key {key}: {e}")
                    raise KeyError(key)
                else:
                    return self.nested_list(key)

        # 普通值：读取并检查TTL
        key_bytes = self._encode_key(key)
        value_bytes = self._db.get(key_bytes)

        if value_bytes is None:
            raise KeyError(key)

        value, expire_time, is_expired = self._decode_value(value_bytes)

        if is_expired:
            try:
                del self[key]
            except Exception as e:
                logger.debug(f"Failed to delete expired key {key}: {e}")
            raise KeyError(key)
        else:
            return value

    def __setitem__(self, key, value):
        """设置键值 - 使用default_ttl"""
        self.set(key, value, ttl=self._default_ttl)

    def set(self, key, value, ttl=None):
        """
        设置键值（支持指定TTL）

        Args:
            key: 键
            value: 值
            ttl: TTL秒数（None表示无TTL）
        """
        # 特殊键：直接处理（无TTL）
        if isinstance(key, str) and key.startswith('__nested__:'):
            key_bytes = self._encode_key(key)
            value_bytes = self._encode_value(value)
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
            # 标记为嵌套字典（带TTL）
            marker_key = f'__nested__:{key}'
            marker_bytes = self._encode_key(marker_key)
            marker_value = self._encode_value(True, ttl_seconds=ttl)
            self._db.put(marker_bytes, marker_value)

            # 清除可能存在的列表标记
            try:
                list_marker_key = f'__list__:{key}'
                list_marker_bytes = self._encode_key(list_marker_key)
                self._db.delete(list_marker_bytes)
            except Exception as e:
                logger.debug(f"Failed to delete list marker for {key}: {e}")

            # 创建 nested，递归写入
            nested = self.nested(key)
            nested.clear()
            for k, v in value.items():
                nested[k] = v
            return

        # 如果启用了自动嵌套且值是列表
        if self._auto_nested and isinstance(value, list):
            # 标记为嵌套列表（带TTL）
            list_marker_key = f'__list__:{key}'
            list_marker_bytes = self._encode_key(list_marker_key)
            list_marker_value = self._encode_value(True, ttl_seconds=ttl)
            self._db.put(list_marker_bytes, list_marker_value)

            # 清除可能存在的字典标记
            try:
                marker_key = f'__nested__:{key}'
                marker_bytes = self._encode_key(marker_key)
                self._db.delete(marker_bytes)
            except Exception as e:
                logger.debug(f"Failed to delete nested marker for {key}: {e}")

            # 创建 nested_list，递归写入
            nested_list = self.nested_list(key)
            nested_list.clear()
            for item in value:
                nested_list.append(item)
            return

        # 非字典非列表或未启用自动嵌套：取消标记（如果有）
        if self._auto_nested:
            try:
                marker_key = f'__nested__:{key}'
                marker_bytes = self._encode_key(marker_key)
                self._db.delete(marker_bytes)
            except Exception as e:
                logger.debug(f"Failed to clear nested marker for {key}: {e}")

            try:
                list_marker_key = f'__list__:{key}'
                list_marker_bytes = self._encode_key(list_marker_key)
                self._db.delete(list_marker_bytes)
            except Exception as e:
                logger.debug(f"Failed to clear list marker for {key}: {e}")

        # 普通存储（带TTL）
        key_bytes = self._encode_key(key)
        value_bytes = self._encode_value(value, ttl_seconds=ttl)
        self._db.put(key_bytes, value_bytes)

    def __delitem__(self, key):
        """删除键"""
        # 特殊键：直接处理
        if isinstance(key, str) and key.startswith('__nested__:'):
            key_bytes = self._encode_key(key)
            self._db.delete(key_bytes)
            return

        # 如果启用了自动嵌套，检查是否是嵌套字典或嵌套列表
        if self._auto_nested:
            # 检查是否是嵌套字典
            marker_key = f'__nested__:{key}'
            marker_bytes = self._encode_key(marker_key)
            marker_value = self._db.get(marker_bytes)

            if marker_value is not None:
                nested = self.nested(key)
                nested.clear()
                self._db.delete(marker_bytes)
                return

            # 检查是否是嵌套列表
            list_marker_key = f'__list__:{key}'
            list_marker_bytes = self._encode_key(list_marker_key)
            list_marker_value = self._db.get(list_marker_bytes)

            if list_marker_value is not None:
                nested_list = self.nested_list(key)
                nested_list.clear()
                self._db.delete(list_marker_bytes)
                return

        # 普通值
        key_bytes = self._encode_key(key)
        value_bytes = self._db.get(key_bytes)
        if value_bytes is None:
            raise KeyError(key)
        self._db.delete(key_bytes)

    def update(self, d: Dict[Any, Any]):
        """批量更新多个键值对"""
        batch = self._db.write_batch()

        for key, value in d.items():
            key_bytes = self._encode_key(key)
            value_bytes = self._encode_value(value, ttl_seconds=self._default_ttl)
            batch.put(key_bytes, value_bytes)

        batch.write()

    def keys(self) -> List:
        """
        获取所有键列表（自动过滤已过期的TTL键）

        优化说明（v2）：
        - 字节级前缀匹配：直接用编码后的标记前缀匹配，避免解码开销
        - 延迟解码：只在必要时才解码键和值
        - 单次遍历 + O(1) 哈希查找判断嵌套子键
        - 使用 is_expired_fast 快速检查 TTL（不解码值）

        键的存储格式说明：
        - 标记键使用 encode_key 编码：b's' + '__nested__:' + key  (字符串类型前缀)
        - 嵌套子键使用 prefixed_db 编码：key + ':' + field  (纯 UTF-8，无类型前缀)
        """
        # 预定义字节常量（考虑键编码格式：字符串键以 b's' 开头）
        NESTED_PREFIX = b's__nested__:'
        LIST_PREFIX = b's__list__:'
        NESTED_PREFIX_LEN = len(NESTED_PREFIX)
        LIST_PREFIX_LEN = len(LIST_PREFIX)

        # 嵌套子键前缀使用纯 UTF-8 编码（无类型前缀），如 b'user:0:'
        nested_prefixes_bytes = set()
        # 待处理的标记键：(actual_key_bytes, value_bytes)
        pending_marker_keys = []

        # 待处理的普通键：(key_bytes, value_bytes)
        pending_key_bytes = []

        # 第一阶段：收集所有标记键和普通键
        for key_bytes, value_bytes in self._db:
            # 字节级前缀匹配：直接检查是否是标记键
            if key_bytes.startswith(NESTED_PREFIX):
                actual_key_bytes = key_bytes[NESTED_PREFIX_LEN:]
                nested_prefixes_bytes.add(actual_key_bytes + b':')
                pending_marker_keys.append((actual_key_bytes, value_bytes))
                continue

            if key_bytes.startswith(LIST_PREFIX):
                actual_key_bytes = key_bytes[LIST_PREFIX_LEN:]
                nested_prefixes_bytes.add(actual_key_bytes + b':')
                pending_marker_keys.append((actual_key_bytes, value_bytes))
                continue

            # 普通键：只保存字节，延迟处理
            pending_key_bytes.append((key_bytes, value_bytes))

        # 第二阶段：处理标记键，过滤掉子标记键（如 __nested__:key_0:data 是 key_0 的子结构）
        keys = []
        marker_keys_set = set()

        for actual_key_bytes, value_bytes in pending_marker_keys:
            # 检查这个标记键是否是另一个嵌套结构的子键
            colon_pos = actual_key_bytes.find(b':')
            if colon_pos > 0:
                prefix_candidate = actual_key_bytes[:colon_pos + 1]
                if prefix_candidate in nested_prefixes_bytes:
                    continue  # 是子结构的标记，跳过

            # 这是顶层键
            if actual_key_bytes not in marker_keys_set:
                # 使用快速过期检查（不解码值）
                if not ValueWithMeta.is_expired_fast(value_bytes):
                    marker_keys_set.add(actual_key_bytes)
                    try:
                        keys.append(actual_key_bytes.decode('utf-8'))
                    except UnicodeDecodeError:
                        keys.append(actual_key_bytes)

        # 第三阶段：处理普通键，过滤嵌套子键
        if nested_prefixes_bytes:
            for key_bytes, value_bytes in pending_key_bytes:
                # 嵌套子键没有类型前缀，直接是 key:field 格式
                colon_pos = key_bytes.find(b':')
                if colon_pos > 0:
                    prefix_candidate = key_bytes[:colon_pos + 1]
                    if prefix_candidate in nested_prefixes_bytes:
                        continue  # 是嵌套子键，跳过

                # 解码键
                try:
                    key = self._decode_key(key_bytes)
                except (ValueError, UnicodeDecodeError):
                    continue  # 解码失败，跳过

                # 使用快速过期检查
                if not ValueWithMeta.is_expired_fast(value_bytes):
                    keys.append(key)
        else:
            # 没有嵌套结构，直接处理所有键
            for key_bytes, value_bytes in pending_key_bytes:
                try:
                    key = self._decode_key(key_bytes)
                except (ValueError, UnicodeDecodeError):
                    continue

                # 使用快速过期检查
                if not ValueWithMeta.is_expired_fast(value_bytes):
                    keys.append(key)

        return keys

    def close(self):
        """关闭数据库"""
        if self._closed:
            return

        # 停止TTL自动清理线程
        if self._ttl_cleanup is not None:
            self._ttl_cleanup.stop()
            self._ttl_cleanup = None

        if self._db is not None:
            try:
                self._db.close()
                self._db = None
                self._closed = True

                logger.debug(f"Raw LevelDB connection closed: {self.name}")

                # 从实例管理器中移除
                db_instance_manager.unregister_instance(self.db_path)
                logger.debug(f"数据库实例已从实例管理器移除: {self.db_path}")

                # 从自动关闭管理器中移除
                db_close_manager.unregister(self)
                logger.debug(f"数据库实例已从自动关闭管理器移除: {self.db_path}")
            except Exception as e:
                logger.error(f"Error closing raw LevelDB: {self.name}, error: {e}")

    def get_ttl(self, key: Any) -> Optional[int]:
        """获取指定键的剩余TTL"""
        if self._auto_nested:
            # 检查是否是嵌套字典
            try:
                marker_key = f'__nested__:{key}'
                marker_bytes = self._encode_key(marker_key)
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

            # 检查是否是嵌套列表
            try:
                list_marker_key = f'__list__:{key}'
                list_marker_bytes = self._encode_key(list_marker_key)
                list_marker_value = self._db.get(list_marker_bytes)

                if list_marker_value is not None:
                    _, expire_time, is_expired = self._decode_value(list_marker_value)
                    if expire_time is None:
                        return None
                    if is_expired:
                        return 0
                    remaining = int(expire_time - time.time())
                    return max(0, remaining)
            except Exception as e:
                logger.debug(f"Error checking list TTL for {key}: {e}")

        # 普通键
        try:
            key_bytes = self._encode_key(key)
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

        return count
