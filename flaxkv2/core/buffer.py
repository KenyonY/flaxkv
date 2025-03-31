"""
FlaxKV2 高级缓冲区实现
"""

import threading
import time
from collections import OrderedDict
from typing import Any, Dict, List, Tuple, Optional, Set


class LRUCache:
    """
    LRU缓存实现
    """
    
    def __init__(self, capacity: int = 1000):
        """
        初始化LRU缓存
        
        Args:
            capacity: 缓存容量
        """
        self._cache = OrderedDict()
        self._capacity = capacity
        self._lock = threading.RLock()
        
    def get(self, key: Any, default: Any = None) -> Any:
        """
        获取键对应的值，不存在则返回默认值
        
        Args:
            key: 键
            default: 默认值
            
        Returns:
            键对应的值或默认值
        """
        with self._lock:
            if key not in self._cache:
                return default
                
            # 访问后移到末尾（最近使用）
            value = self._cache.pop(key)
            self._cache[key] = value
            return value
            
    def put(self, key: Any, value: Any) -> None:
        """
        设置键值对
        
        Args:
            key: 键
            value: 值
        """
        with self._lock:
            if key in self._cache:
                # 已存在，移除旧值
                self._cache.pop(key)
            elif len(self._cache) >= self._capacity:
                # 容量已满，移除最久未使用的项
                self._cache.popitem(last=False)
                
            # 添加到末尾（最近使用）
            self._cache[key] = value
    
    def remove(self, key: Any) -> bool:
        """
        移除键
        
        Args:
            key: 键
            
        Returns:
            bool: 是否成功移除
        """
        with self._lock:
            if key in self._cache:
                self._cache.pop(key)
                return True
            return False
    
    def contains(self, key: Any) -> bool:
        """
        检查键是否存在
        
        Args:
            key: 键
            
        Returns:
            bool: 键是否存在
        """
        with self._lock:
            return key in self._cache
    
    def clear(self) -> None:
        """清空缓存"""
        with self._lock:
            self._cache.clear()
    
    def keys(self) -> List[Any]:
        """获取所有键"""
        with self._lock:
            return list(self._cache.keys())
    
    def values(self) -> List[Any]:
        """获取所有值"""
        with self._lock:
            return list(self._cache.values())
    
    def items(self) -> List[Tuple[Any, Any]]:
        """获取所有键值对"""
        with self._lock:
            return list(self._cache.items())
    
    def __len__(self) -> int:
        """获取缓存大小"""
        with self._lock:
            return len(self._cache)


class TieredBuffer:
    """
    分层缓冲区实现，支持热/冷数据分层
    """
    
    def __init__(self, hot_size: int = 100, cold_size: int = 1000):
        """
        初始化分层缓冲区
        
        Args:
            hot_size: 热缓存大小
            cold_size: 冷缓存大小
        """
        self._hot_cache = LRUCache(hot_size)
        self._cold_cache = LRUCache(cold_size)
        
    def get(self, key: Any, default: Any = None) -> Any:
        """
        获取键对应的值
        
        Args:
            key: 键
            default: 默认值
            
        Returns:
            键对应的值或默认值
        """
        # 先查热缓存
        value = self._hot_cache.get(key)
        if value is not None:
            return value
            
        # 再查冷缓存
        value = self._cold_cache.get(key)
        if value is not None:
            # 提升到热缓存
            self._hot_cache.put(key, value)
            return value
            
        return default
    
    def put(self, key: Any, value: Any) -> None:
        """
        设置键值对
        
        Args:
            key: 键
            value: 值
        """
        # 直接放入热缓存
        self._hot_cache.put(key, value)
        
        # 如果冷缓存中存在，则移除
        self._cold_cache.remove(key)
    
    def remove(self, key: Any) -> bool:
        """
        移除键
        
        Args:
            key: 键
            
        Returns:
            bool: 是否成功移除
        """
        hot_removed = self._hot_cache.remove(key)
        cold_removed = self._cold_cache.remove(key)
        return hot_removed or cold_removed
    
    def contains(self, key: Any) -> bool:
        """
        检查键是否存在
        
        Args:
            key: 键
            
        Returns:
            bool: 键是否存在
        """
        return self._hot_cache.contains(key) or self._cold_cache.contains(key)
    
    def downgrade(self) -> None:
        """
        降级热缓存中的数据到冷缓存
        当热缓存容量接近上限时调用
        """
        # 获取热缓存中的项
        hot_items = self._hot_cache.items()
        
        # 只保留一半热数据
        half = len(hot_items) // 2
        if half <= 0:
            return
            
        # 将较老的一半数据移到冷缓存
        for i, (key, value) in enumerate(hot_items):
            if i >= half:
                break
                
            self._cold_cache.put(key, value)
            self._hot_cache.remove(key)
    
    def clear(self) -> None:
        """清空缓存"""
        self._hot_cache.clear()
        self._cold_cache.clear()
    
    def items(self) -> List[Tuple[Any, Any]]:
        """获取所有键值对"""
        # 合并热缓存和冷缓存的键值对
        # 注意：热缓存优先
        result = {}
        
        for key, value in self._cold_cache.items():
            result[key] = value
            
        for key, value in self._hot_cache.items():
            result[key] = value
            
        return list(result.items())
    
    def __len__(self) -> int:
        """获取缓存总大小"""
        return len(self._hot_cache) + len(self._cold_cache) 