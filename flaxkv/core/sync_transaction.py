"""
FlaxKV 2.0 同步事务包装器

为事务操作提供同步接口封装。
"""

import logging
from typing import Any, Optional

from .async_loop_manager import AsyncEventLoopThread
from .config import FlaxKVConfig
from ..transaction.manager import IsolationLevel

logger = logging.getLogger(__name__)


class SyncTransactionWrapper:
    """同步事务包装器。"""
    
    def __init__(self, flaxkv, loop_thread: AsyncEventLoopThread, 
                 isolation_level: IsolationLevel, timeout: float):
        self.flaxkv = flaxkv
        self.loop_thread = loop_thread
        self.isolation_level = isolation_level
        self.timeout = timeout
        self._tx = None
        self._committed = False
        self._rolled_back = False
    
    def _ensure_tx(self):
        """确保事务已创建。"""
        if self._tx is None:
            coro = self.flaxkv.begin_transaction(self.isolation_level)
            self._tx = self.loop_thread.run_coroutine(coro, self.timeout)
    
    def get(self, key: str) -> Any:
        """事务内获取键值。"""
        self._ensure_tx()
        coro = self._tx.get(key)
        return self.loop_thread.run_coroutine(coro, self.timeout)
    
    def set(self, key: str, value: Any) -> None:
        """事务内设置键值。"""
        self._ensure_tx()
        coro = self._tx.set(key, value)
        self.loop_thread.run_coroutine(coro, self.timeout)
    
    def delete(self, key: str) -> None:
        """事务内删除键。"""
        self._ensure_tx()
        coro = self._tx.delete(key)
        self.loop_thread.run_coroutine(coro, self.timeout)
    
    def commit(self) -> None:
        """提交事务。"""
        if self._committed or self._rolled_back or self._tx is None:
            return
        
        try:
            # 调用TransactionManager的commit_transaction，而不是Transaction的commit
            coro = self.flaxkv.tx_manager.commit_transaction(self._tx)
            self.loop_thread.run_coroutine(coro, self.timeout)
            self._committed = True
            logger.debug("同步事务提交成功")
        except Exception as e:
            logger.error(f"同步事务提交失败: {e}")
            raise
    
    def rollback(self) -> None:
        """回滚事务。"""
        if self._committed or self._rolled_back or self._tx is None:
            return
        
        try:
            # 调用TransactionManager的abort_transaction，而不是Transaction的rollback
            coro = self.flaxkv.tx_manager.abort_transaction(self._tx)
            self.loop_thread.run_coroutine(coro, self.timeout)
            self._rolled_back = True
            logger.debug("同步事务回滚成功")
        except Exception as e:
            logger.error(f"同步事务回滚失败: {e}")
            raise
    
    def __enter__(self):
        """进入事务上下文。"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出事务上下文。"""
        if exc_type is None:
            self.commit()
        else:
            self.rollback()


def create_sync_flaxkv(name: str, 
                      storage_type: str = "local", 
                      path: Optional[str] = None,
                      config: Optional[FlaxKVConfig] = None,
                      timeout: float = 30.0):
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
    from .sync_wrapper import ThreadSafeFlaxKV
    return ThreadSafeFlaxKV(name, storage_type, path, config, timeout)