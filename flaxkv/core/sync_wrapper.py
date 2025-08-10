"""
FlaxKV 2.0 高性能线程安全同步封装

提供基于后台事件循环的线程安全同步接口，适用于集成到同步应用中。
"""

import threading
import logging
from typing import Any, Dict, List, Optional, Iterator, Tuple
from contextlib import contextmanager
import weakref

from .flaxkv import FlaxKV
from .config import FlaxKVConfig
from .async_loop_manager import get_global_async_loop, SyncAsyncIteratorWrapper, AsyncEventLoopThread
from .sync_transaction import SyncTransactionWrapper
from ..transaction.manager import IsolationLevel

logger = logging.getLogger(__name__)


class ThreadSafeFlaxKV:
    """高性能线程安全的FlaxKV同步封装。
    
    特性：
    - 基于后台事件循环，避免重复创建销毁事件循环的开销
    - 线程安全，可在多线程环境中使用
    - 保持FlaxKV的所有功能特性
    - 适合集成到同步应用框架
    
    使用方法：
        >>> db = ThreadSafeFlaxKV("mydb")
        >>> db.set("key", "value")
        >>> value = db.get("key")
        >>> print(value)
        
        >>> # 在同步应用中使用
        >>> app_db = ThreadSafeFlaxKV("appdb")
    """
    
    def __init__(self, 
                 name: str, 
                 storage_type: str = "local", 
                 path: Optional[str] = None,
                 config: Optional[FlaxKVConfig] = None,
                 timeout: float = 30.0):
        """初始化ThreadSafeFlaxKV。
        
        Args:
            name: 数据库名称
            storage_type: 存储类型
            path: 存储路径
            config: FlaxKV配置
            timeout: 默认操作超时时间（秒）
        """
        self.name = name
        self.storage_type = storage_type
        self.path = path
        self.config = config or FlaxKVConfig.for_production()
        self.timeout = timeout
        
        # 获取全局事件循环线程
        self._loop_thread = get_global_async_loop()
        
        # 在后台线程中创建并初始化FlaxKV实例
        self._flaxkv: Optional[FlaxKV] = None
        self._initialized = False
        self._init_lock = threading.Lock()
        
        # 弱引用计数，用于自动清理
        self._instances = weakref.WeakSet()
        self._instances.add(self)
    
    def _ensure_initialized(self):
        """确保FlaxKV实例已初始化。"""
        if self._initialized:
            return
        
        with self._init_lock:
            if self._initialized:
                return
            
            # 在后台事件循环中创建和初始化FlaxKV
            coro = self._async_initialize()
            self._loop_thread.run_coroutine(coro, timeout=self.timeout)
            self._initialized = True
    
    async def _async_initialize(self):
        """异步初始化FlaxKV实例。"""
        self._flaxkv = FlaxKV(self.name, self.storage_type, self.path, self.config)
        await self._flaxkv.initialize()
    
    def close(self):
        """关闭数据库连接。"""
        if self._initialized and self._flaxkv:
            coro = self._flaxkv.close()
            try:
                self._loop_thread.run_coroutine(coro, timeout=5.0)
            except Exception as e:
                logger.error(f"关闭FlaxKV失败: {e}")
            finally:
                self._initialized = False
                self._flaxkv = None
    
    def __del__(self):
        """析构函数，自动清理资源。"""
        try:
            self.close()
        except:
            pass  # 忽略析构时的错误
    
    # ===== 上下文管理器支持 =====
    
    def __enter__(self):
        """进入上下文管理器。"""
        self._ensure_initialized()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文管理器。"""
        self.close()
    
    # ===== 字典接口 =====
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取键值。"""
        self._ensure_initialized()
        coro = self._flaxkv.get(key, default)
        return self._loop_thread.run_coroutine(coro, self.timeout)
    
    def set(self, key: str, value: Any) -> None:
        """设置键值。"""
        self._ensure_initialized()
        coro = self._flaxkv.set(key, value)
        self._loop_thread.run_coroutine(coro, self.timeout)
    
    def delete(self, key: str) -> bool:
        """删除键。"""
        self._ensure_initialized()
        coro = self._flaxkv.delete(key)
        return self._loop_thread.run_coroutine(coro, self.timeout)
    
    def contains(self, key: str) -> bool:
        """检查键是否存在。"""
        self._ensure_initialized()
        coro = self._flaxkv.contains(key)
        return self._loop_thread.run_coroutine(coro, self.timeout)
    
    def size(self) -> int:
        """获取键数量。"""
        self._ensure_initialized()
        coro = self._flaxkv.size()
        return self._loop_thread.run_coroutine(coro, self.timeout)
    
    # ===== Python字典风格接口 =====
    
    def __getitem__(self, key: str) -> Any:
        """字典风格获取。"""
        try:
            return self.get(key)
        except KeyError:
            raise KeyError(key)
    
    def __setitem__(self, key: str, value: Any) -> None:
        """字典风格设置。"""
        self.set(key, value)
    
    def __delitem__(self, key: str) -> None:
        """字典风格删除。"""
        if not self.delete(key):
            raise KeyError(key)
    
    def __contains__(self, key: str) -> bool:
        """字典风格包含检查。"""
        return self.contains(key)
    
    def __len__(self) -> int:
        """字典风格长度。"""
        return self.size()
    
    def __iter__(self) -> Iterator[str]:
        """字典风格迭代。"""
        self._ensure_initialized()
        async_iter = self._flaxkv.keys()
        return SyncAsyncIteratorWrapper(async_iter, self._loop_thread)
    
    # ===== 批量操作 =====
    
    def mget(self, keys: List[str]) -> Dict[str, Any]:
        """批量获取键值对。"""
        self._ensure_initialized()
        coro = self._flaxkv.mget(keys)
        return self._loop_thread.run_coroutine(coro, self.timeout)
    
    def mset(self, items: Dict[str, Any]) -> None:
        """批量设置键值对。"""
        self._ensure_initialized()
        coro = self._flaxkv.mset(items)
        self._loop_thread.run_coroutine(coro, self.timeout)
    
    def mdelete(self, keys: List[str]) -> int:
        """批量删除键。"""
        self._ensure_initialized()
        coro = self._flaxkv.mdelete(keys)
        return self._loop_thread.run_coroutine(coro, self.timeout)
    
    # ===== 高级查询 =====
    
    def scan(self, prefix: Optional[str] = None, limit: Optional[int] = None) -> Iterator[Tuple[str, Any]]:
        """扫描键值对。"""
        self._ensure_initialized()
        async_iter = self._flaxkv.scan(prefix=prefix, limit=limit)
        return SyncAsyncIteratorWrapper(async_iter, self._loop_thread)
    
    def keys(self, pattern: Optional[str] = None) -> Iterator[str]:
        """获取所有键。"""
        self._ensure_initialized()
        async_iter = self._flaxkv.keys(pattern)
        return SyncAsyncIteratorWrapper(async_iter, self._loop_thread)
    
    def values(self) -> Iterator[Any]:
        """获取所有值。"""
        self._ensure_initialized()
        async_iter = self._flaxkv.values()
        return SyncAsyncIteratorWrapper(async_iter, self._loop_thread)
    
    def items(self) -> Iterator[Tuple[str, Any]]:
        """获取所有键值对。"""
        self._ensure_initialized()
        async_iter = self._flaxkv.items()
        return SyncAsyncIteratorWrapper(async_iter, self._loop_thread)
    
    # ===== TTL支持 =====
    
    def setex(self, key: str, value: Any, expire: int) -> None:
        """设置带过期时间的键值对。"""
        self._ensure_initialized()
        coro = self._flaxkv.setex(key, value, expire)
        self._loop_thread.run_coroutine(coro, self.timeout)
    
    def expire(self, key: str, seconds: int) -> bool:
        """设置键的过期时间。"""
        self._ensure_initialized()
        coro = self._flaxkv.expire(key, seconds)
        return self._loop_thread.run_coroutine(coro, self.timeout)
    
    # ===== 事务支持 =====
    
    @contextmanager
    def transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED):
        """创建事务上下文管理器。"""
        self._ensure_initialized()
        tx = SyncTransactionWrapper(
            self._flaxkv, self._loop_thread, isolation_level, self.timeout
        )
        try:
            yield tx
            tx.commit()
        except Exception:
            tx.rollback()
            raise
    
    def begin_transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED) -> SyncTransactionWrapper:
        """开始事务。"""
        self._ensure_initialized()
        return SyncTransactionWrapper(
            self._flaxkv, self._loop_thread, isolation_level, self.timeout
        )
    
    # ===== 数据管理 =====
    
    def flush(self) -> Dict[str, Any]:
        """强制刷新缓冲区。"""
        self._ensure_initialized()
        coro = self._flaxkv.flush()
        return self._loop_thread.run_coroutine(coro, self.timeout)
    
    def clear(self) -> None:
        """清空所有数据。"""
        self._ensure_initialized()
        coro = self._flaxkv.clear()
        self._loop_thread.run_coroutine(coro, self.timeout)
    
    def backup(self, path: str) -> Dict[str, Any]:
        """备份数据。"""
        self._ensure_initialized()
        coro = self._flaxkv.backup(path)
        return self._loop_thread.run_coroutine(coro, self.timeout)
    
    def restore(self, path: str) -> Dict[str, Any]:
        """恢复数据。"""
        self._ensure_initialized()
        coro = self._flaxkv.restore(path)
        return self._loop_thread.run_coroutine(coro, self.timeout)
    
    # ===== 统计和监控 =====
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息。"""
        if not self._initialized or not self._flaxkv:
            return {}
        return self._flaxkv.get_stats()
    
    def get_info(self) -> Dict[str, Any]:
        """获取详细信息。"""
        if not self._initialized or not self._flaxkv:
            return {'name': self.name, 'initialized': False}
        return self._flaxkv.get_info()
    
    # ===== 便捷方法 =====
    
    def setdefault(self, key: str, default: Any) -> Any:
        """如果键不存在则设置默认值。"""
        if self.contains(key):
            return self.get(key)
        else:
            self.set(key, default)
            return default
    
    def pop(self, key: str, default: Any = None) -> Any:
        """弹出键值。"""
        try:
            value = self.get(key)
            self.delete(key)
            return value
        except KeyError:
            if default is not None:
                return default
            raise
    
    def update(self, items: Dict[str, Any]) -> None:
        """批量更新键值对。"""
        self.mset(items)


# 便捷创建函数
def create_sync_flaxkv(name: str, 
                      storage_type: str = "local", 
                      path: Optional[str] = None,
                      config: Optional[FlaxKVConfig] = None,
                      timeout: float = 30.0) -> ThreadSafeFlaxKV:
    """创建ThreadSafeFlaxKV实例的便捷函数。
    
    Args:
        name: 数据库名称
        storage_type: 存储类型
        path: 存储路径
        config: FlaxKV配置
        timeout: 默认操作超时时间
        
    Returns:
        ThreadSafeFlaxKV实例
    """
    return ThreadSafeFlaxKV(name, storage_type, path, config, timeout)