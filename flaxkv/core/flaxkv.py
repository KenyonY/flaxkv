"""
FlaxKV 2.0 核心接口

提供原生字典接口的高性能持久化字典实现。
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Tuple, AsyncIterator, Union
from pathlib import Path

from .config import FlaxKVConfig
from .component_manager import ComponentManager
from .factory import TransactionalFlaxKV
from ..transaction.manager import IsolationLevel

logger = logging.getLogger(__name__)


def _run_async_safely(coro):
    """安全地运行异步协程，避免在已存在事件循环中使用 asyncio.run()。"""
    try:
        # 尝试获取当前事件循环
        loop = asyncio.get_running_loop()
        
        # 如果已经在事件循环中运行，抛出运行时错误
        raise RuntimeError(
            "不能在异步上下文中使用同步接口。"
            "请使用异步接口（如 await db.get() 而不是 db['key']）"
        )
    except RuntimeError as e:
        if "no running event loop" in str(e):
            # 没有运行中的事件循环，安全使用 asyncio.run
            return asyncio.run(coro)
        else:
            # 重新抛出其他 RuntimeError
            raise


class FlaxKV:
    """FlaxKV 2.0 高性能持久化字典。
    
    统一接口，支持原生字典操作语法，提供：
    - 高性能 LevelDB 存储
    - 智能缓存管理
    - 高性能缓冲写入
    - ACID 事务支持
    - 监控和统计
    """
    
    def __init__(self, name: str, 
                 backend: str = "local",
                 location: Union[str, Path] = ".",
                 config: Optional[FlaxKVConfig] = None):
        """初始化FlaxKV实例。
        
        Args:
            name: 数据库名称
            backend: 后端类型 ("local" 或 "remote")
            location: 本地路径或远程URL
            config: FlaxKVConfig配置对象
        """
        self.name = name
        self.backend = backend
        self.location = str(location)
        self.config = config or FlaxKVConfig()
        
        # 组件管理器
        self.component_manager = ComponentManager(
            name, backend, self.location, self.config
        )
        
        # 组件引用（由组件管理器管理）
        self.storage_engine = None
        self.cache = None
        self.buffer = None
        self.tx_manager = None
        
        # 状态管理
        self._initialized = False
        self._closed = False
        
        # TTL 过期时间管理
        self._expire_times = {}  # key -> expire_timestamp
        
        # 统计信息
        self._stats = {
            'total_operations': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'transactions_started': 0,
            'start_time': time.time(),
        }
    
    async def initialize(self) -> None:
        """初始化FlaxKV实例。"""
        if self._initialized:
            return
        
        # 通过组件管理器初始化所有组件
        components = await self.component_manager.initialize()
        
        # 设置组件引用
        self.storage_engine = components['storage_engine']
        self.cache = components['cache']
        self.buffer = components['buffer']
        self.tx_manager = components['tx_manager']
        
        self._initialized = True
        logger.info(f"FlaxKV初始化完成: {self.name}")
    
    # === 上下文管理器 ===
    
    async def __aenter__(self):
        """异步上下文管理器入口。"""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口。"""
        await self.close()
    
    # === 同步字典接口（不推荐） ===
    
    def __getitem__(self, key: str) -> Any:
        """同步获取键值（不推荐）。"""
        return _run_async_safely(self.get(key))
    
    def __setitem__(self, key: str, value: Any) -> None:
        """同步设置键值（不推荐）。"""
        _run_async_safely(self.set(key, value))
    
    def __delitem__(self, key: str) -> None:
        """同步删除键（不推荐）。"""
        _run_async_safely(self.delete(key))
    
    def __contains__(self, key: str) -> bool:
        """同步检查键是否存在（不推荐）。"""
        return _run_async_safely(self.contains(key))
    
    def __len__(self) -> int:
        """同步获取键数量（不推荐）。"""
        return _run_async_safely(self.size())
    
    async def __aiter__(self):
        """异步迭代器。"""
        await self.initialize()
        async for key, value in self.storage_engine.scan():
            yield key, value
    
    # === TTL 辅助方法 ===
    
    def _is_expired(self, key: str) -> bool:
        """检查键是否已过期。"""
        if key not in self._expire_times:
            return False
        
        import time
        return time.time() > self._expire_times[key]
    
    def _set_expire_time(self, key: str, seconds: float) -> None:
        """设置键的过期时间。"""
        import time
        self._expire_times[key] = time.time() + seconds
    
    def _remove_expire_time(self, key: str) -> None:
        """移除键的过期时间记录。"""
        self._expire_times.pop(key, None)
    
    # === 异步接口（推荐使用） ===
    
    async def get(self, key: str, default: Any = None) -> Any:
        """异步获取键值。"""
        await self.initialize()
        self._stats['total_operations'] += 1
        
        # 检查键是否已过期
        if self._is_expired(key):
            self._remove_expire_time(key)
            # 如果键已过期，需要从所有层删除
            try:
                await self.delete(key)
            except KeyError:
                pass
            if default is not None:
                return default
            raise KeyError(key)
        
        try:
            # 1. 尝试从缓存获取
            cache_result = self.cache.get(key)
            if cache_result.hit:
                self._stats['cache_hits'] += 1
                return cache_result.value
            
            self._stats['cache_misses'] += 1
            
            # 2. 从缓冲区获取
            try:
                value = await self.buffer.read(key)
                
                # 存入缓存
                self.cache.put(key, value)
                return value
                
            except KeyError:
                if default is not None:
                    return default
                raise
            
        except Exception as e:
            logger.error(f"获取键失败 {key}: {e}")
            raise
    
    async def set(self, key: str, value: Any) -> None:
        """异步设置键值。"""
        await self.initialize()
        self._stats['total_operations'] += 1
        
        try:
            # 写入缓冲区（非阻塞）
            await self.buffer.write(key, value)
            
            # 更新缓存
            self.cache.put(key, value)
            
            # 清理可能存在的过期时间记录（除非通过setex设置）
            self._remove_expire_time(key)
            
        except Exception as e:
            logger.error(f"设置键失败 {key}: {e}")
            raise
    
    async def delete(self, key: str) -> bool:
        """异步删除键。"""
        await self.initialize()
        self._stats['total_operations'] += 1
        
        try:
            # 检查键是否存在
            exists = await self.contains(key)
            if not exists:
                return False
            
            # 从缓冲区删除
            await self.buffer.delete(key)
            
            # 从缓存删除
            self.cache.remove(key)
            
            # 清理过期时间记录
            self._remove_expire_time(key)
            
            return True
            
        except Exception as e:
            logger.error(f"删除键失败 {key}: {e}")
            raise
    
    async def contains(self, key: str) -> bool:
        """异步检查键是否存在。"""
        await self.initialize()
        
        try:
            # 1. 检查缓存
            cache_result = self.cache.get(key)
            if cache_result.hit:
                return True
            
            # 2. 检查缓冲区
            return await self.buffer.contains(key)
            
        except Exception as e:
            logger.error(f"检查键存在失败 {key}: {e}")
            return False
    
    async def size(self) -> int:
        """异步获取键数量。"""
        await self.initialize()
        
        # 强制刷新缓冲区以获得准确计数
        await self.buffer.flush()
        
        return await self.storage_engine.size()
    
    # === 批量操作 ===
    
    async def mget(self, keys: List[str]) -> Dict[str, Any]:
        """批量获取键值对。"""
        await self.initialize()
        
        result = {}
        cache_misses = []
        
        # 先从缓存获取
        for key in keys:
            cache_result = self.cache.get(key)
            if cache_result.hit:
                result[key] = cache_result.value
                self._stats['cache_hits'] += 1
            else:
                cache_misses.append(key)
                self._stats['cache_misses'] += 1
        
        # 从存储引擎批量获取缓存未命中的键
        if cache_misses:
            storage_result = {}
            
            # 先从缓冲区尝试读取
            for key in cache_misses:
                try:
                    value = await self.buffer.read(key)
                    storage_result[key] = value
                except KeyError:
                    continue
            
            # 如果还有未找到的键，从存储引擎获取
            remaining_keys = [k for k in cache_misses if k not in storage_result]
            if remaining_keys:
                engine_result = await self.storage_engine.batch_get(remaining_keys)
                storage_result.update(engine_result)
            
            # 存入缓存
            for key, value in storage_result.items():
                self.cache.put(key, value)
            
            result.update(storage_result)
        
        return result
    
    async def mset(self, items: Dict[str, Any]) -> None:
        """批量设置键值对。"""
        await self.initialize()
        
        # 批量写入缓冲区
        for key, value in items.items():
            await self.buffer.write(key, value)
            
            # 更新缓存
            self.cache.put(key, value)
    
    async def mdelete(self, keys: List[str]) -> int:
        """批量删除键。"""
        await self.initialize()
        
        deleted_count = 0
        for key in keys:
            if await self.delete(key):
                deleted_count += 1
        
        return deleted_count
    
    # === 高级查询 ===
    
    async def scan(self, 
                   prefix: Optional[str] = None,
                   start: Optional[str] = None,
                   end: Optional[str] = None,
                   limit: Optional[int] = None) -> AsyncIterator[Tuple[str, Any]]:
        """扫描键值对。"""
        await self.initialize()
        
        # 先刷新缓冲区确保数据一致性
        await self.buffer.flush()
        
        async for key, value in self.storage_engine.scan(prefix, start, end, limit):
            yield key, value
    
    async def keys(self, pattern: Optional[str] = None) -> AsyncIterator[str]:
        """获取所有键。"""
        async for key, _ in self.scan(prefix=pattern):
            yield key
    
    async def values(self) -> AsyncIterator[Any]:
        """获取所有值。"""
        async for _, value in self.scan():
            yield value
    
    async def items(self) -> AsyncIterator[Tuple[str, Any]]:
        """获取所有键值对。"""
        async for key, value in self.scan():
            yield key, value
    
    # === TTL支持 ===
    
    async def setex(self, key: str, value: Any, expire: float) -> None:
        """设置带过期时间的键值对。"""
        await self.initialize()
        self._stats['total_operations'] += 1
        
        try:
            # 写入缓冲区（非阻塞）
            await self.buffer.write(key, value)
            
            # 设置全局过期时间
            self._set_expire_time(key, expire)
            
            # 在缓存中设置TTL
            self.cache.put(key, value, ttl=expire)
            
        except Exception as e:
            logger.error(f"设置TTL键失败 {key}: {e}")
            raise
    
    async def expire(self, key: str, seconds: float) -> bool:
        """设置键的过期时间。"""
        if await self.contains(key):
            # 设置全局过期时间
            self._set_expire_time(key, seconds)
            # 在缓存中设置TTL
            cache_result = self.cache.get(key)
            if cache_result.hit:
                self.cache.put(key, cache_result.value, ttl=seconds)
            return True
        return False
    
    # === 事务支持 ===
    
    def transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED):
        """创建事务上下文管理器。"""
        return TransactionalFlaxKV(self, isolation_level)
    
    async def begin_transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED):
        """开始事务。"""
        await self.initialize()
        self._stats['transactions_started'] += 1
        return self.tx_manager.begin_transaction(isolation_level)
    
    # === 数据管理 ===
    
    async def flush(self) -> Dict[str, Any]:
        """强制刷新缓冲区。"""
        await self.initialize()
        return await self.buffer.flush()
    
    async def clear(self) -> None:
        """清空所有数据。"""
        await self.initialize()
        
        # 清空缓存和缓冲区
        self.cache.clear()
        await self.buffer.flush()
        
        # 清空存储引擎（通过删除所有键）
        keys_to_delete = []
        async for key, _ in self.storage_engine.scan():
            keys_to_delete.append(key)
        
        if keys_to_delete:
            await self.storage_engine.batch_delete(keys_to_delete)
    
    async def backup(self, path: str) -> Dict[str, Any]:
        """备份数据。"""
        await self.initialize()
        
        # 先刷新缓冲区
        await self.buffer.flush()
        
        return await self.storage_engine.backup(path)
    
    async def restore(self, path: str) -> Dict[str, Any]:
        """恢复数据。"""
        await self.initialize()
        
        # 清空现有数据
        await self.clear()
        
        # 恢复数据
        result = await self.storage_engine.restore(path)
        
        # 清空缓存
        self.cache.clear()
        
        return result
    
    # === 监控和统计 ===
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息。"""
        stats = self._stats.copy()
        stats['uptime'] = time.time() - stats['start_time']
        
        # 获取组件统计信息
        component_stats = self.component_manager.get_stats()
        stats.update(component_stats)
        
        return stats
    
    def get_info(self) -> Dict[str, Any]:
        """获取详细信息。"""
        info = {
            'name': self.name,
            'backend': self.backend,
            'location': self.location,
            'initialized': self._initialized,
            'closed': self._closed,
            'config': self.config.to_dict(),
            'stats': self.get_stats(),
        }
        
        return info
    
    # === 资源管理 ===
    
    async def close(self) -> None:
        """关闭FlaxKV实例。"""
        if self._closed:
            return
        
        logger.info(f"关闭FlaxKV: {self.name}")
        
        # 通过组件管理器关闭所有组件
        await self.component_manager.shutdown()
        
        # 清空组件引用
        self.storage_engine = None
        self.cache = None
        self.buffer = None
        self.tx_manager = None
        
        self._closed = True
        self._initialized = False
        logger.info(f"FlaxKV已关闭: {self.name}")