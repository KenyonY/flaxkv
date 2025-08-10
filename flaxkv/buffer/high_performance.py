"""
FlaxKV 2.0 高性能缓冲层

提供非阻塞写入、智能刷新策略的高性能缓冲管理。
"""

import time
import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor

from ..core.config import FlaxKVConfig
from .wal import WriteAheadLog, CompressedWriteAheadLog
from .compression import CompressionManager
from .scheduler import FlushScheduler

logger = logging.getLogger(__name__)


@dataclass
class BufferEntry:
    """缓冲区条目。"""
    key: str
    value: Any
    timestamp: float = field(default_factory=time.time)
    operation: str = "set"  # set, delete
    size: int = 0
    
    def __post_init__(self):
        if self.size == 0:
            self.size = self._estimate_size()
    
    def _estimate_size(self) -> int:
        """估算条目大小。"""
        try:
            import sys
            key_size = sys.getsizeof(self.key)
            value_size = sys.getsizeof(self.value) if self.value is not None else 0
            return key_size + value_size + 100  # 添加一些开销
        except:
            return 1024  # 默认1KB



class HighPerformanceBuffer:
    """高性能缓冲管理器。
    
    特性：
    - 非阻塞写入
    - WAL保证数据安全
    - 智能刷新策略
    - 批量写入优化
    """
    
    def __init__(self, storage_engine, config: FlaxKVConfig, wal_path: Optional[str] = None):
        """初始化高性能缓冲层。
        
        Args:
            storage_engine: 底层存储引擎
            config: FlaxKVConfig配置对象
            wal_path: WAL文件路径（可选）
        """
        self.storage_engine = storage_engine
        self.config = config
        
        # 缓冲区
        self._buffer: Dict[str, BufferEntry] = {}
        self._buffer_lock = asyncio.Lock()
        self._buffer_size = 0
        self._last_write_time = time.time()
        
        # WAL
        wal_path = wal_path or f"{storage_engine.path}.wal" if hasattr(storage_engine, 'path') else "flaxkv.wal"
        # 根据配置选择是否使用压缩WAL
        if getattr(config, 'enable_wal_compression', False):
            self._wal = CompressedWriteAheadLog(wal_path, config)
        else:
            self._wal = WriteAheadLog(wal_path, config)
        
        # 压缩管理器
        compression_algo = getattr(config, 'compression_algorithm', 'gzip')
        self._compression_manager = CompressionManager(compression_algo)
        
        # 刷新调度器
        self._flush_scheduler = FlushScheduler(config.buffer_timeout, config.buffer_size)
        
        # 后台任务
        self._flush_task = None
        self._running = False
        
        # 线程池执行器
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="FlaxKV-Buffer")
        
        # 统计信息
        self._stats = {
            'total_writes': 0,
            'total_reads': 0,
            'buffer_hits': 0,
            'flushes': 0,
            'wal_writes': 0,
        }
    
    async def initialize(self) -> None:
        """初始化缓冲层。"""
        # 初始化WAL
        await self._wal.initialize()
        
        # 启动刷新调度器
        await self._flush_scheduler.start()
        
        # 注册刷新回调
        self._flush_scheduler.add_flush_callback(self._check_flush_conditions)
        
        # 启动刷新任务
        self._running = True
        self._flush_task = asyncio.create_task(self._flush_worker())
        
        # WAL恢复（如果需要）
        await self._recover_from_wal()
        
        logger.info("高性能缓冲层初始化完成")
    
    async def write(self, key: str, value: Any) -> None:
        """非阻塞写入。
        
        Args:
            key: 键
            value: 值
        """
        start_time = time.perf_counter()
        
        try:
            # 立即写入WAL确保不丢数据
            await self._wal.append(key, value, "set")
            self._stats['wal_writes'] += 1
            
            # 写入内存缓冲区
            async with self._buffer_lock:
                entry = BufferEntry(key=key, value=value, operation="set")
                
                # 如果键已存在，先减去旧条目的大小
                if key in self._buffer:
                    self._buffer_size -= self._buffer[key].size
                
                self._buffer[key] = entry
                self._buffer_size += entry.size
                self._last_write_time = time.time()
                
                self._stats['total_writes'] += 1
            
            # 检查是否需要立即刷新
            if self._should_immediate_flush():
                asyncio.create_task(self._flush_to_storage())
            
        except Exception as e:
            logger.error(f"缓冲写入失败 {key}: {e}")
            raise
        
        logger.debug(f"缓冲写入完成 {key}: {(time.perf_counter() - start_time) * 1000:.2f}ms")
    
    async def delete(self, key: str) -> None:
        """非阻塞删除。
        
        Args:
            key: 键
        """
        try:
            # 立即写入WAL
            await self._wal.append(key, None, "delete")
            self._stats['wal_writes'] += 1
            
            # 写入内存缓冲区
            async with self._buffer_lock:
                entry = BufferEntry(key=key, value=None, operation="delete")
                
                # 如果键已存在，先减去旧条目的大小
                if key in self._buffer:
                    self._buffer_size -= self._buffer[key].size
                
                self._buffer[key] = entry
                self._buffer_size += entry.size
                self._last_write_time = time.time()
            
        except Exception as e:
            logger.error(f"缓冲删除失败 {key}: {e}")
            raise
    
    async def read(self, key: str) -> Any:
        """读取数据（优先从缓冲区）。
        
        Args:
            key: 键
            
        Returns:
            键对应的值
            
        Raises:
            KeyError: 键不存在时抛出
        """
        self._stats['total_reads'] += 1
        
        # 优先从缓冲区读取
        async with self._buffer_lock:
            if key in self._buffer:
                entry = self._buffer[key]
                if entry.operation == "delete":
                    raise KeyError(key)
                
                self._stats['buffer_hits'] += 1
                return entry.value
        
        # 从持久化存储读取
        return await self.storage_engine.get(key)
    
    async def contains(self, key: str) -> bool:
        """检查键是否存在。"""
        # 检查缓冲区
        async with self._buffer_lock:
            if key in self._buffer:
                entry = self._buffer[key]
                return entry.operation != "delete"
        
        # 检查持久化存储
        return await self.storage_engine.contains(key)
    
    async def flush(self) -> Dict[str, Any]:
        """强制刷新缓冲区到存储。
        
        Returns:
            刷新统计信息
        """
        return await self._flush_to_storage()
    
    def _should_immediate_flush(self) -> bool:
        """判断是否应该立即刷新。"""
        return (len(self._buffer) >= self.config.buffer_size or
                self._buffer_size > 50 * 1024 * 1024)  # 50MB
    
    async def _check_flush_conditions(self) -> Tuple[bool, str]:
        """检查刷新条件（给调度器调用）。"""
        async with self._buffer_lock:
            buffer_count = len(self._buffer)
            buffer_size = self._buffer_size
            last_write = self._last_write_time
        
        return self._flush_scheduler.should_flush(buffer_size, buffer_count, last_write)
    
    async def _flush_to_storage(self) -> Dict[str, Any]:
        """批量刷新到存储层。"""
        start_time = time.perf_counter()
        
        # 复制缓冲区并清空
        async with self._buffer_lock:
            if not self._buffer:
                return {'flushed_count': 0, 'duration_ms': 0}
            
            buffer_copy = dict(self._buffer)
            self._buffer.clear()
            self._buffer_size = 0
        
        try:
            # 分离set和delete操作
            set_operations = {}
            delete_operations = []
            
            for key, entry in buffer_copy.items():
                if entry.operation == "set":
                    set_operations[key] = entry.value
                elif entry.operation == "delete":
                    delete_operations.append(key)
            
            # 批量执行操作
            tasks = []
            if set_operations:
                tasks.append(self.storage_engine.batch_set(set_operations))
            if delete_operations:
                tasks.append(self.storage_engine.batch_delete(delete_operations))
            
            if tasks:
                await asyncio.gather(*tasks)
            
            # WAL检查点
            await self._wal.checkpoint()
            
            # 统计
            flushed_count = len(buffer_copy)
            duration_ms = (time.perf_counter() - start_time) * 1000
            
            self._stats['flushes'] += 1
            
            logger.debug(f"缓冲区刷新完成: {flushed_count}条记录, {duration_ms:.2f}ms")
            
            return {
                'flushed_count': flushed_count,
                'set_count': len(set_operations),
                'delete_count': len(delete_operations),
                'duration_ms': duration_ms
            }
            
        except Exception as e:
            # 刷新失败，恢复缓冲区
            async with self._buffer_lock:
                for key, entry in buffer_copy.items():
                    if key not in self._buffer:  # 避免覆盖新写入的数据
                        self._buffer[key] = entry
                        self._buffer_size += entry.size
            
            logger.error(f"缓冲区刷新失败: {e}")
            raise
    
    async def _flush_worker(self):
        """刷新工作协程。"""
        while self._running:
            try:
                await asyncio.sleep(self.config.buffer_timeout / 2)
                
                # 检查是否需要刷新
                should_flush, reason = await self._check_flush_conditions()
                if should_flush:
                    await self._flush_to_storage()
                
            except Exception as e:
                logger.error(f"刷新工作协程错误: {e}")
                await asyncio.sleep(1)
    
    async def _recover_from_wal(self) -> None:
        """从WAL恢复数据。"""
        try:
            records = await self._wal.recover()
            if not records:
                return
            
            logger.info(f"开始WAL恢复，共{len(records)}条记录")
            
            # 重放WAL记录
            recovered_count = 0
            for record in records:
                key = record['key']
                value = record['value']
                operation = record['operation']
                
                try:
                    if operation == "set":
                        await self.storage_engine.set(key, value)
                    elif operation == "delete":
                        await self.storage_engine.delete(key)
                    
                    recovered_count += 1
                    
                except Exception as e:
                    logger.error(f"WAL记录恢复失败 {key}: {e}")
            
            # 清空WAL
            await self._wal.checkpoint()
            
            logger.info(f"WAL恢复完成，成功恢复{recovered_count}条记录")
            
        except Exception as e:
            logger.error(f"WAL恢复失败: {e}")
            raise
    
    async def close(self) -> None:
        """关闭缓冲层。"""
        self._running = False
        
        # 最后刷新
        try:
            await self._flush_to_storage()
        except Exception as e:
            logger.error(f"关闭时刷新失败: {e}")
        
        # 停止后台任务
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        
        await self._flush_scheduler.stop()
        await self._wal.close()
        
        # 关闭线程池
        self._executor.shutdown(wait=True)
        
        logger.info("高性能缓冲层已关闭")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓冲层统计信息。"""
        scheduler_stats = self._flush_scheduler.get_stats()
        compression_stats = self._compression_manager.get_stats()
        
        return {
            **self._stats,
            'buffer_size': len(self._buffer),
            'buffer_size_bytes': self._buffer_size,
            'scheduler_stats': scheduler_stats,
            'compression_stats': compression_stats,
        }
    
    def get_info(self) -> Dict[str, Any]:
        """获取缓冲层详细信息。"""
        stats = self.get_stats()
        
        buffer_hit_rate = (stats['buffer_hits'] / 
                          max(stats['total_reads'], 1))
        
        stats.update({
            'config': {
                'buffer_size': self.config.buffer_size,
                'buffer_timeout': self.config.buffer_timeout,
                'sync_mode': self.config.sync_mode,
            },
            'buffer_hit_rate': buffer_hit_rate,
            'running': self._running,
        })
        
        return stats