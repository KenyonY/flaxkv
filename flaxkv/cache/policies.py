"""
FlaxKV 2.0 缓存策略实现

提供LRU、LFU、ARC等多种高性能缓存策略。
"""

import time
import logging
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict
from collections import OrderedDict, defaultdict
from threading import Lock
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """缓存条目。"""
    value: Any
    access_time: float
    access_count: int
    ttl: Optional[float] = None  # 过期时间戳
    size: int = 1  # 条目大小（字节）
    
    def is_expired(self) -> bool:
        """检查是否过期。"""
        if self.ttl is None:
            return False
        return time.time() > self.ttl
    
    def update_access(self):
        """更新访问信息。"""
        self.access_time = time.time()
        self.access_count += 1


class CachePolicy(ABC):
    """缓存策略抽象基类。"""
    
    @abstractmethod
    def get(self, key: str) -> Optional[CacheEntry]:
        """获取缓存条目。"""
        pass
    
    @abstractmethod
    def put(self, key: str, entry: CacheEntry) -> None:
        """存储缓存条目。"""
        pass
    
    @abstractmethod
    def remove(self, key: str) -> bool:
        """移除缓存条目。"""
        pass
    
    @abstractmethod
    def select_victim(self, new_key: str) -> Optional[str]:
        """选择要驱逐的键。"""
        pass
    
    @abstractmethod
    def clear(self) -> None:
        """清空缓存。"""
        pass
    
    @abstractmethod
    def size(self) -> int:
        """获取缓存大小。"""
        pass


class LRUCache(CachePolicy):
    """LRU (Least Recently Used) 缓存策略。"""
    
    def __init__(self, max_size: int):
        self.max_size = max_size
        self.cache = OrderedDict()
        self._lock = Lock()
    
    def get(self, key: str) -> Optional[CacheEntry]:
        """获取缓存条目。"""
        with self._lock:
            if key in self.cache:
                entry = self.cache[key]
                if entry.is_expired():
                    del self.cache[key]
                    return None
                
                # 移到末尾（最近使用）
                self.cache.move_to_end(key)
                entry.update_access()
                return entry
            return None
    
    def put(self, key: str, entry: CacheEntry) -> None:
        """存储缓存条目。"""
        with self._lock:
            if key in self.cache:
                # 更新现有条目
                self.cache[key] = entry
                self.cache.move_to_end(key)
            else:
                # 新条目
                self.cache[key] = entry
                
                # 检查是否超出容量
                if len(self.cache) > self.max_size:
                    # 移除最老的条目（第一个）
                    oldest_key, _ = self.cache.popitem(last=False)
                    logger.debug(f"LRU驱逐键: {oldest_key}")
    
    def remove(self, key: str) -> bool:
        """移除缓存条目。"""
        with self._lock:
            if key in self.cache:
                del self.cache[key]
                return True
            return False
    
    def select_victim(self, new_key: str) -> Optional[str]:
        """选择要驱逐的键（最老的键）。"""
        with self._lock:
            if len(self.cache) >= self.max_size:
                return next(iter(self.cache))
            return None
    
    def clear(self) -> None:
        """清空缓存。"""
        with self._lock:
            self.cache.clear()
    
    def size(self) -> int:
        """获取缓存大小。"""
        with self._lock:
            return len(self.cache)


class LFUCache(CachePolicy):
    """LFU (Least Frequently Used) 缓存策略。"""
    
    def __init__(self, max_size: int):
        self.max_size = max_size
        self.cache = {}
        self.frequencies = defaultdict(int)
        self.freq_groups = defaultdict(set)  # 频率 -> 键集合
        self.min_freq = 0
        self._lock = Lock()
    
    def get(self, key: str) -> Optional[CacheEntry]:
        """获取缓存条目。"""
        with self._lock:
            if key not in self.cache:
                return None
            
            entry = self.cache[key]
            if entry.is_expired():
                self._remove_key(key)
                return None
            
            # 更新频率
            self._update_frequency(key)
            entry.update_access()
            return entry
    
    def put(self, key: str, entry: CacheEntry) -> None:
        """存储缓存条目。"""
        with self._lock:
            if key in self.cache:
                # 更新现有条目
                self.cache[key] = entry
                self._update_frequency(key)
            else:
                # 新条目
                if len(self.cache) >= self.max_size:
                    # 移除最低频率的键
                    victim_key = self._select_lfu_victim()
                    if victim_key:
                        self._remove_key(victim_key)
                
                self.cache[key] = entry
                self.frequencies[key] = 1
                self.freq_groups[1].add(key)
                self.min_freq = 1
    
    def remove(self, key: str) -> bool:
        """移除缓存条目。"""
        with self._lock:
            if key in self.cache:
                self._remove_key(key)
                return True
            return False
    
    def select_victim(self, new_key: str) -> Optional[str]:
        """选择要驱逐的键（最低频率）。"""
        with self._lock:
            if len(self.cache) >= self.max_size:
                return self._select_lfu_victim()
            return None
    
    def _update_frequency(self, key: str):
        """更新键的频率。"""
        freq = self.frequencies[key]
        self.freq_groups[freq].discard(key)
        
        # 如果这是最小频率且该频率组为空，增加最小频率
        if freq == self.min_freq and not self.freq_groups[freq]:
            self.min_freq += 1
        
        # 增加频率
        new_freq = freq + 1
        self.frequencies[key] = new_freq
        self.freq_groups[new_freq].add(key)
    
    def _select_lfu_victim(self) -> Optional[str]:
        """选择最低频率的受害者。"""
        if not self.freq_groups[self.min_freq]:
            return None
        return next(iter(self.freq_groups[self.min_freq]))
    
    def _remove_key(self, key: str):
        """移除键的所有记录。"""
        if key in self.cache:
            freq = self.frequencies[key]
            self.freq_groups[freq].discard(key)
            del self.cache[key]
            del self.frequencies[key]
    
    def clear(self) -> None:
        """清空缓存。"""
        with self._lock:
            self.cache.clear()
            self.frequencies.clear()
            self.freq_groups.clear()
            self.min_freq = 0
    
    def size(self) -> int:
        """获取缓存大小。"""
        with self._lock:
            return len(self.cache)


class ARCCache(CachePolicy):
    """ARC (Adaptive Replacement Cache) 缓存策略。
    
    结合LRU和LFU的优点，自适应调整策略。
    """
    
    def __init__(self, max_size: int):
        self.max_size = max_size
        self.cache = {}
        
        # T1: 最近访问过一次的页面
        self.t1 = OrderedDict()
        # T2: 最近访问过多次的页面  
        self.t2 = OrderedDict()
        # B1: T1的历史记录
        self.b1 = OrderedDict()
        # B2: T2的历史记录
        self.b2 = OrderedDict()
        
        self.p = 0  # T1的目标大小
        self._lock = Lock()
    
    def get(self, key: str) -> Optional[CacheEntry]:
        """获取缓存条目。"""
        with self._lock:
            if key in self.cache:
                entry = self.cache[key]
                if entry.is_expired():
                    self._remove_key(key)
                    return None
                
                # 移动到T2（频繁访问）
                if key in self.t1:
                    del self.t1[key]
                    self.t2[key] = entry
                elif key in self.t2:
                    self.t2.move_to_end(key)
                
                entry.update_access()
                return entry
            
            return None
    
    def put(self, key: str, entry: CacheEntry) -> None:
        """存储缓存条目。"""
        with self._lock:
            if key in self.cache:
                # 更新现有条目
                self.cache[key] = entry
                if key in self.t1:
                    del self.t1[key]
                    self.t2[key] = entry
                elif key in self.t2:
                    self.t2.move_to_end(key)
                return
            
            # 新条目
            self.cache[key] = entry
            
            # Case I: 在B1中（最近驱逐的单次访问）
            if key in self.b1:
                del self.b1[key]
                self.p = min(self.p + max(1, len(self.b2) // len(self.b1)), self.max_size)
                self._replace(key)
                self.t2[key] = entry
                return
            
            # Case II: 在B2中（最近驱逐的多次访问）
            if key in self.b2:
                del self.b2[key]
                self.p = max(self.p - max(1, len(self.b1) // len(self.b2)), 0)
                self._replace(key)
                self.t2[key] = entry
                return
            
            # Case III: 新键
            if len(self.t1) + len(self.t2) >= self.max_size:
                self._replace(key)
            
            self.t1[key] = entry
            
            # 维护B1和B2的大小
            if len(self.b1) > self.max_size:
                self.b1.popitem(last=False)
            if len(self.b2) > self.max_size:
                self.b2.popitem(last=False)
    
    def _replace(self, new_key: str):
        """ARC替换算法。"""
        if len(self.t1) > 0 and (len(self.t1) > self.p or (new_key in self.b2 and len(self.t1) == self.p)):
            # 从T1驱逐
            old_key, old_entry = self.t1.popitem(last=False)
            self.b1[old_key] = old_entry
            del self.cache[old_key]
        else:
            # 从T2驱逐
            if len(self.t2) > 0:
                old_key, old_entry = self.t2.popitem(last=False)
                self.b2[old_key] = old_entry
                del self.cache[old_key]
    
    def remove(self, key: str) -> bool:
        """移除缓存条目。"""
        with self._lock:
            if key in self.cache:
                self._remove_key(key)
                return True
            return False
    
    def _remove_key(self, key: str):
        """移除键的所有记录。"""
        if key in self.cache:
            del self.cache[key]
        if key in self.t1:
            del self.t1[key]
        if key in self.t2:
            del self.t2[key]
        if key in self.b1:
            del self.b1[key]
        if key in self.b2:
            del self.b2[key]
    
    def select_victim(self, new_key: str) -> Optional[str]:
        """选择要驱逐的键。"""
        with self._lock:
            if len(self.cache) >= self.max_size:
                if len(self.t1) > self.p:
                    return next(iter(self.t1)) if self.t1 else None
                else:
                    return next(iter(self.t2)) if self.t2 else None
            return None
    
    def clear(self) -> None:
        """清空缓存。"""
        with self._lock:
            self.cache.clear()
            self.t1.clear()
            self.t2.clear()
            self.b1.clear()
            self.b2.clear()
            self.p = 0
    
    def size(self) -> int:
        """获取缓存大小。"""
        with self._lock:
            return len(self.cache)