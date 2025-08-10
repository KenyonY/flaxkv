"""
FlaxKV 2.0 工厂函数和事务包装器

提供FlaxKV实例创建的便捷函数和事务性包装器。
"""

import asyncio
import logging
from typing import Any, Optional, Union
from pathlib import Path

from .config import FlaxKVConfig
from ..transaction.manager import IsolationLevel

logger = logging.getLogger(__name__)


class TransactionalFlaxKV:
    """事务性FlaxKV包装器。
    
    在事务上下文中提供FlaxKV操作。
    """
    
    def __init__(self, flaxkv, isolation_level: IsolationLevel):
        self.flaxkv = flaxkv
        self.isolation_level = isolation_level
        self.tx = None
    
    async def __aenter__(self):
        """事务开始。"""
        self.tx = await self.flaxkv.begin_transaction(self.isolation_level)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """事务结束。"""
        if exc_type is None:
            await self.flaxkv.tx_manager.commit_transaction(self.tx)
        else:
            await self.flaxkv.tx_manager.abort_transaction(self.tx)
    
    async def get(self, key: str) -> Any:
        """在事务中获取键值。"""
        return await self.tx.get(key)
    
    async def set(self, key: str, value: Any) -> None:
        """在事务中设置键值。"""
        await self.tx.set(key, value)
    
    async def delete(self, key: str) -> None:
        """在事务中删除键。"""
        await self.tx.delete(key)
    
    def __getitem__(self, key: str) -> Any:
        """同步获取键值（不推荐）。"""
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
        
        return _run_async_safely(self.get(key))
    
    def __setitem__(self, key: str, value: Any) -> None:
        """同步设置键值（不推荐）。"""
        def _run_async_safely(coro):
            try:
                loop = asyncio.get_running_loop()
                raise RuntimeError(
                    "不能在异步上下文中使用同步接口。"
                    "请使用异步接口（如 await db.set() 而不是 db['key'] = value）"
                )
            except RuntimeError as e:
                if "no running event loop" in str(e):
                    return asyncio.run(coro)
                else:
                    raise
        
        _run_async_safely(self.set(key, value))
    
    def __delitem__(self, key: str) -> None:
        """同步删除键（不推荐）。"""
        def _run_async_safely(coro):
            try:
                loop = asyncio.get_running_loop()
                raise RuntimeError(
                    "不能在异步上下文中使用同步接口。"
                    "请使用异步接口（如 await db.delete() 而不是 del db['key']）"
                )
            except RuntimeError as e:
                if "no running event loop" in str(e):
                    return asyncio.run(coro)
                else:
                    raise
        
        _run_async_safely(self.delete(key))


# === 工厂函数 ===

async def create_flaxkv(name: str, 
                       backend: str = "local",
                       location: Union[str, Path] = ".",
                       config: Optional[FlaxKVConfig] = None):
    """创建并初始化FlaxKV实例的工厂函数。
    
    Args:
        name: 数据库名称
        backend: 后端类型
        location: 本地路径或远程URL
        config: 配置对象
        
    Returns:
        初始化好的FlaxKV实例
    """
    # 延迟导入避免循环引用
    from .flaxkv import FlaxKV
    
    flaxkv = FlaxKV(name, backend, location, config)
    await flaxkv.initialize()
    return flaxkv


def create_flaxkv_sync(name: str, 
                      backend: str = "local",
                      location: Union[str, Path] = ".",
                      config: Optional[FlaxKVConfig] = None):
    """创建线程安全的同步FlaxKV实例。
    
    Args:
        name: 数据库名称
        backend: 后端类型  
        location: 本地路径或远程URL
        config: 配置对象
        
    Returns:
        ThreadSafeFlaxKV实例
    """
    # 延迟导入避免循环引用
    from .sync_wrapper import ThreadSafeFlaxKV
    
    return ThreadSafeFlaxKV(name, backend, str(location), config)


def create_flax_dict(name: str, 
                     location: Union[str, Path] = ".",
                     config: Optional[FlaxKVConfig] = None):
    """创建字典式接口的FlaxDict实例。
    
    Args:
        name: 数据库名称
        location: 本地路径
        config: 配置对象
        
    Returns:
        FlaxDict实例
    """
    # 延迟导入避免循环引用
    from .flax_dict import FlaxDict
    
    return FlaxDict(name, str(location), config)