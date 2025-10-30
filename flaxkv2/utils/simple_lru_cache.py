"""
简单的LRU缓存实现（支持TTL）

使用OrderedDict实现，遵循KISS原则：
- 只限制条目数量（不限制内存大小）
- 自动检查TTL过期
- 线程安全由外部RWLock保证
"""

import time
from collections import OrderedDict
from typing import Any, Optional, Tuple


class SimpleLRUCache:
    """
    简单的LRU缓存实现

    特性：
    - LRU淘汰策略（最近最少使用）
    - 支持TTL（Time To Live）
    - 基于条目数量限制
    - 零依赖（仅使用Python内置库）

    线程安全：
    - 此类本身不是线程安全的
    - 应由外部锁（如RWLock）保护

    使用示例：
        cache = SimpleLRUCache(maxsize=1000)

        # 添加无TTL的缓存
        cache.put('key1', 'value1')

        # 添加带TTL的缓存（过期时间戳）
        expire_time = time.time() + 60
        cache.put('key2', 'value2', expire_time)

        # 读取（自动检查TTL）
        value = cache.get('key1')  # 'value1'
        value = cache.get('key2')  # 'value2' or None (if expired)
    """

    def __init__(self, maxsize: int):
        """
        初始化LRU缓存

        Args:
            maxsize: 最大缓存条目数量（必须 > 0）
        """
        if maxsize <= 0:
            raise ValueError(f"maxsize must be > 0, got {maxsize}")

        self.maxsize = maxsize
        # key -> (value, expire_time)
        # expire_time为None表示无TTL
        self.cache = OrderedDict()

    def get(self, key: Any) -> Optional[Any]:
        """
        获取缓存值

        如果键不存在或已过期，返回None
        如果键存在且未过期，将其移到末尾（标记为最近使用）

        Args:
            key: 缓存键

        Returns:
            缓存值，如果不存在或已过期返回None
        """
        if key not in self.cache:
            return None

        value, expire_time = self.cache[key]

        # 检查TTL
        if expire_time is not None:
            if time.time() > expire_time:
                # 已过期，删除缓存
                del self.cache[key]
                return None

        # 未过期，移到末尾（标记为最近使用）
        self.cache.move_to_end(key)
        return value

    def put(self, key: Any, value: Any, expire_time: Optional[float] = None) -> None:
        """
        添加或更新缓存

        如果缓存已满，移除最旧的条目（LRU）

        Args:
            key: 缓存键
            value: 缓存值（可以是任何Python对象）
            expire_time: 过期时间戳（Unix timestamp），None表示无TTL
        """
        if key in self.cache:
            # 已存在，移到末尾
            self.cache.move_to_end(key)

        # 存储 (value, expire_time) 元组
        self.cache[key] = (value, expire_time)

        # LRU淘汰：移除最旧的条目
        if len(self.cache) > self.maxsize:
            self.cache.popitem(last=False)

    def delete(self, key: Any) -> None:
        """
        删除缓存项

        如果键不存在，不做任何操作

        Args:
            key: 缓存键
        """
        self.cache.pop(key, None)

    def clear(self) -> None:
        """清空所有缓存"""
        self.cache.clear()

    def __len__(self) -> int:
        """返回缓存中的条目数量"""
        return len(self.cache)

    def __contains__(self, key: Any) -> bool:
        """
        检查键是否在缓存中（不检查TTL）

        注意：这个方法不检查TTL，只检查键是否存在
        如果需要检查有效性，使用 get() 方法
        """
        return key in self.cache

    def cleanup_expired(self) -> int:
        """
        清理所有过期的缓存项

        Returns:
            清理的条目数量
        """
        current_time = time.time()
        expired_keys = []

        for key, (value, expire_time) in self.cache.items():
            if expire_time is not None and current_time > expire_time:
                expired_keys.append(key)

        for key in expired_keys:
            del self.cache[key]

        return len(expired_keys)

    def stats(self) -> dict:
        """
        返回缓存统计信息

        Returns:
            包含统计信息的字典
        """
        total = len(self.cache)
        with_ttl = sum(1 for _, (_, expire_time) in self.cache.items() if expire_time is not None)

        return {
            'total_items': total,
            'items_with_ttl': with_ttl,
            'items_without_ttl': total - with_ttl,
            'maxsize': self.maxsize,
            'usage_percent': (total / self.maxsize * 100) if self.maxsize > 0 else 0,
        }
