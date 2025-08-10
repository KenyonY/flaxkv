"""
FlaxKV 2.0 组件管理器

负责FlaxKV各个组件的创建、初始化和生命周期管理。
"""

import asyncio
import logging
from pathlib import Path
from typing import Optional, Callable

from .config import FlaxKVConfig
from ..storage.leveldb_engine import LevelDBEngine
from ..cache.intelligent import IntelligentCache
from ..buffer.high_performance import HighPerformanceBuffer
from ..transaction.manager import TransactionManager

logger = logging.getLogger(__name__)


class ComponentManager:
    """FlaxKV组件管理器。
    
    负责统一管理存储引擎、缓存层、缓冲层和事务管理器的创建和生命周期。
    """
    
    def __init__(self, name: str, backend: str, location: str, config: FlaxKVConfig):
        self.name = name
        self.backend = backend
        self.location = location
        self.config = config
        
        # 组件实例
        self.storage_engine: Optional[any] = None
        self.cache: Optional[IntelligentCache] = None
        self.buffer: Optional[HighPerformanceBuffer] = None
        self.tx_manager: Optional[TransactionManager] = None
        
        self._initialized = False
        self._lock = asyncio.Lock()
    
    async def initialize(self) -> dict:
        """初始化所有组件。
        
        Returns:
            包含所有组件实例的字典
        """
        if self._initialized:
            return self._get_components_dict()
        
        async with self._lock:
            if self._initialized:
                return self._get_components_dict()
            
            try:
                logger.info(f"初始化FlaxKV组件: {self.name} ({self.backend})")
                
                # 1. 创建存储引擎
                await self._create_storage_engine()
                
                # 2. 创建缓存层
                await self._create_cache_layer()
                
                # 3. 创建缓冲层
                await self._create_buffer_layer()
                
                # 4. 创建事务管理器
                await self._create_transaction_manager()
                
                self._initialized = True
                logger.info(f"FlaxKV组件初始化完成: {self.name}")
                
                return self._get_components_dict()
                
            except Exception as e:
                logger.error(f"FlaxKV组件初始化失败: {e}")
                await self._cleanup_on_error()
                raise
    
    async def _create_storage_engine(self) -> None:
        """创建存储引擎。"""
        if self.backend == "local":
            # 本地 LevelDB 存储
            db_path = Path(self.location) / self.name
            self.storage_engine = LevelDBEngine(str(db_path), self.config)
            await self.storage_engine.initialize()
            
        elif self.backend == "remote":
            # 远程存储（暂时抛出未实现错误）
            raise NotImplementedError("远程存储引擎将在后续版本实现")
        
        else:
            raise ValueError(f"不支持的后端类型: {self.backend}")
        
        logger.debug("存储引擎创建完成")
    
    async def _create_cache_layer(self) -> None:
        """创建缓存层。"""
        self.cache = IntelligentCache(self.config)
        self.cache.set_storage_engine(self.storage_engine)
        await self.cache.start()
        logger.debug("缓存层创建完成")
    
    async def _create_buffer_layer(self) -> None:
        """创建缓冲层。"""
        self.buffer = HighPerformanceBuffer(
            self.storage_engine, self.config
        )
        await self.buffer.initialize()
        logger.debug("缓冲层创建完成")
    
    async def _create_transaction_manager(self) -> None:
        """创建事务管理器。"""
        # 定义缓存失效回调
        def invalidate_cache(key: str):
            """使缓存中的键无效。"""
            if self.cache:
                self.cache.remove(key)
        
        self.tx_manager = TransactionManager(
            self.storage_engine, self.config, invalidate_cache
        )
        await self.tx_manager.start()
        logger.debug("事务管理器创建完成")
    
    def _get_components_dict(self) -> dict:
        """获取所有组件的字典。"""
        return {
            'storage_engine': self.storage_engine,
            'cache': self.cache,
            'buffer': self.buffer,
            'tx_manager': self.tx_manager
        }
    
    async def _cleanup_on_error(self) -> None:
        """错误时清理资源。"""
        components = [
            ('事务管理器', self.tx_manager),
            ('缓存层', self.cache),
            ('缓冲层', self.buffer),
            ('存储引擎', self.storage_engine)
        ]
        
        for name, component in components:
            if component:
                try:
                    if hasattr(component, 'stop'):
                        await component.stop()
                    elif hasattr(component, 'close'):
                        await component.close()
                    logger.debug(f"{name}清理完成")
                except Exception as e:
                    logger.warning(f"{name}清理失败: {e}")
    
    async def shutdown(self) -> None:
        """关闭所有组件。"""
        if not self._initialized:
            return
        
        logger.info(f"关闭FlaxKV组件: {self.name}")
        await self._cleanup_on_error()
        self._initialized = False
        
        # 清空组件引用
        self.storage_engine = None
        self.cache = None
        self.buffer = None
        self.tx_manager = None
        
        logger.info(f"FlaxKV组件已关闭: {self.name}")
    
    @property
    def is_initialized(self) -> bool:
        """检查是否已初始化。"""
        return self._initialized
    
    def get_stats(self) -> dict:
        """获取所有组件的统计信息。"""
        stats = {}
        
        if self.cache:
            stats['cache'] = self.cache.get_metrics()
        
        if self.buffer:
            stats['buffer'] = self.buffer.get_stats()
        
        if self.tx_manager:
            stats['transactions'] = self.tx_manager.get_stats()
        
        if self.storage_engine and hasattr(self.storage_engine, 'get_stats'):
            stats['storage'] = self.storage_engine.get_stats()
        
        return stats