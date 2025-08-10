"""
FlaxKV 2.0 缓存指标收集和访问模式分析

提供缓存性能监控和访问模式分析功能。
"""

import time
from typing import List, Dict
from collections import defaultdict
from threading import Lock


class CacheMetrics:
    """缓存指标收集器。"""
    
    def __init__(self):
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.access_patterns = defaultdict(list)
        self._lock = Lock()
    
    def record_hit(self, key: str):
        """记录缓存命中。"""
        with self._lock:
            self.hits += 1
            self.access_patterns[key].append(time.time())
    
    def record_miss(self, key: str):
        """记录缓存未命中。"""
        with self._lock:
            self.misses += 1
            self.access_patterns[key].append(time.time())
    
    def record_eviction(self, key: str):
        """记录缓存驱逐。"""
        with self._lock:
            self.evictions += 1
    
    def get_hit_rate(self) -> float:
        """获取缓存命中率。"""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0
    
    def get_access_pattern(self, key: str) -> 'AccessPattern':
        """获取键的访问模式。"""
        with self._lock:
            access_times = self.access_patterns.get(key, [])
            return AccessPattern(key, access_times)
    
    def get_stats(self) -> Dict[str, any]:
        """获取统计信息。"""
        with self._lock:
            return {
                'hits': self.hits,
                'misses': self.misses,
                'evictions': self.evictions,
                'hit_rate': self.get_hit_rate(),
                'total_accesses': self.hits + self.misses,
                'tracked_keys': len(self.access_patterns)
            }
    
    def reset(self):
        """重置统计。"""
        with self._lock:
            self.hits = 0
            self.misses = 0
            self.evictions = 0
            self.access_patterns.clear()


class AccessPattern:
    """访问模式分析器。"""
    
    def __init__(self, key: str, access_times: List[float]):
        self.key = key
        self.access_times = sorted(access_times)
    
    def suggests_sequential_access(self) -> bool:
        """判断是否为顺序访问模式。"""
        if len(self.access_times) < 3:
            return False
        
        # 分析访问间隔的稳定性
        intervals = []
        for i in range(1, len(self.access_times)):
            interval = self.access_times[i] - self.access_times[i-1]
            intervals.append(interval)
        
        if not intervals:
            return False
        
        # 计算间隔的标准差
        mean_interval = sum(intervals) / len(intervals)
        variance = sum((x - mean_interval) ** 2 for x in intervals) / len(intervals)
        std_deviation = variance ** 0.5
        
        # 如果标准差相对较小，认为是规律访问
        return std_deviation < mean_interval * 0.5
    
    def get_frequency(self) -> float:
        """获取访问频率（次数/秒）。"""
        if len(self.access_times) < 2:
            return 0.0
        
        duration = self.access_times[-1] - self.access_times[0]
        return len(self.access_times) / max(duration, 1.0)
    
    def is_hot_key(self, threshold: float = 10.0) -> bool:
        """判断是否为热点键。"""
        return self.get_frequency() > threshold
    
    def get_recent_activity(self, window_seconds: float = 60.0) -> int:
        """获取最近时间窗口内的访问次数。"""
        current_time = time.time()
        cutoff_time = current_time - window_seconds
        
        return sum(1 for access_time in self.access_times 
                  if access_time >= cutoff_time)