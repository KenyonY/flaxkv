"""
FlaxKV 2.0 智能缓存层

提供多种缓存策略和智能预加载功能的高性能缓存层。
"""

import time
import asyncio
import logging
from typing import Any, Optional, Dict, NamedTuple

from ..core.config import FlaxKVConfig
from .policies import CachePolicy, LRUCache, LFUCache, ARCCache, CacheEntry
from .metrics import CacheMetrics, AccessPattern
from .prefetch import PrefetchManager

logger = logging.getLogger(__name__)


class CacheResult(NamedTuple):
    """缓存查询结果。"""
    hit: bool
    value: Any = None
    access_time: float = 0.0


class IntelligentCache:
    """智能缓存管理器。
    
    特性：
    - 多种缓存策略支持
    - 智能预加载
    - TTL支持
    - 详细的监控指标
    """
    
    def __init__(self, config: FlaxKVConfig):
        """初始化智能缓存。
        
        Args:
            config: FlaxKVConfig配置对象
        """
        self.config = config
        self.max_size = self._calculate_max_entries(config.cache_size_bytes)
        self.policy = self._create_policy(config.cache_policy)
        self.metrics = CacheMetrics()
        
        # 预加载管理器
        self._prefetch_manager: Optional[PrefetchManager] = None
        self._storage_engine = None
        
        # TTL清理
        self._cleanup_task = None
        self._running = False
    
    def _calculate_max_entries(self, cache_size_bytes: int) -> int:
        """计算最大缓存条目数。
        
        假设平均每个条目1KB。
        """
        avg_entry_size = 1024  # 1KB
        return max(100, cache_size_bytes // avg_entry_size)
    
    def _create_policy(self, policy_name: str) -> CachePolicy:
        """创建缓存策略。"""
        if policy_name == "lru":
            return LRUCache(self.max_size)
        elif policy_name == "lfu":
            return LFUCache(self.max_size)
        elif policy_name == "arc":
            return ARCCache(self.max_size)
        else:
            logger.warning(f"未知缓存策略: {policy_name}，使用LRU")
            return LRUCache(self.max_size)
    
    def set_storage_engine(self, storage_engine):
        """设置存储引擎，用于预加载。"""
        self._storage_engine = storage_engine
        if storage_engine:
            self._prefetch_manager = PrefetchManager(storage_engine)
    
    async def start(self):
        """启动缓存服务。"""
        if self._running:
            return
        
        self._running = True
        
        # 启动预加载管理器
        if self._prefetch_manager:
            await self._prefetch_manager.start()
        
        # 启动TTL清理任务
        self._cleanup_task = asyncio.create_task(self._cleanup_expired())
        
        logger.info("智能缓存服务已启动")
    
    async def stop(self):
        """停止缓存服务。"""
        self._running = False
        
        # 停止预加载管理器
        if self._prefetch_manager:
            await self._prefetch_manager.stop()
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        logger.info("智能缓存服务已停止")
    
    def get(self, key: str) -> CacheResult:
        """获取缓存条目。"""
        entry = self.policy.get(key)
        
        if entry:
            self.metrics.record_hit(key)
            
            # 智能预加载
            if self._should_prefetch(key):
                self._schedule_prefetch(key)
            
            return CacheResult(hit=True, value=entry.value, access_time=entry.access_time)
        else:
            self.metrics.record_miss(key)
            return CacheResult(hit=False)
    
    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """存储缓存条目。
        
        Args:
            key: 键
            value: 值
            ttl: 生存时间（秒），None表示不过期
        """
        current_time = time.time()
        expire_time = current_time + ttl if ttl else None
        
        entry = CacheEntry(
            value=value,
            access_time=current_time,
            access_count=1,
            ttl=expire_time
        )
        
        # 检查是否需要驱逐
        victim_key = self.policy.select_victim(key)
        if victim_key:
            self.metrics.record_eviction(victim_key)
        
        self.policy.put(key, entry)
    
    def remove(self, key: str) -> bool:
        """移除缓存条目。"""
        return self.policy.remove(key)
    
    def clear(self) -> None:
        """清空缓存。"""
        self.policy.clear()
        self.metrics.reset()
    
    def size(self) -> int:
        """获取缓存大小。"""
        return self.policy.size()
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取缓存指标。"""
        base_stats = self.metrics.get_stats()
        base_stats.update({
            'cache_size': self.size(),
            'max_size': self.max_size,
            'policy': self.config.cache_policy,
            'ttl_cleanup_enabled': self._running
        })
        return base_stats
    
    def _should_prefetch(self, key: str) -> bool:
        """判断是否应该预加载。"""
        if not self._prefetch_manager:
            return False
        
        access_pattern = self.metrics.get_access_pattern(key)
        return self._prefetch_manager.should_prefetch(key, access_pattern)
    
    def _schedule_prefetch(self, key: str):
        """调度预加载任务。"""
        if self._prefetch_manager:
            # 生成相邻键进行预加载
            related_keys = self._generate_related_keys(key)
            for related_key in related_keys:
                self._prefetch_manager.schedule_prefetch(related_key)
    
    def _generate_related_keys(self, key: str, max_keys: int = 3) -> list[str]:
        """生成相关键列表（简单的数字递增策略）。"""
        related_keys = []
        
        # 尝试解析键中的数字部分
        import re
        match = re.search(r'(\d+)$', key)
        if match:
            base = key[:match.start()]
            num = int(match.group(1))
            
            # 生成相邻的键
            for i in range(1, max_keys + 1):
                related_keys.append(f"{base}{num + i}")
        
        return related_keys
    
    async def _cleanup_expired(self):
        """清理过期条目的后台任务。"""
        while self._running:
            try:
                # 简化的TTL清理逻辑
                # 在实际实现中，需要维护过期键的索引
                await asyncio.sleep(self.config.cache_cleanup_interval)
                
                # 这里可以添加具体的过期键清理逻辑
                # 由于策略类已经在get时检查过期，这里主要做定期清理
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"TTL清理任务错误: {e}")
                await asyncio.sleep(60)  # 错误时等待1分钟再重试