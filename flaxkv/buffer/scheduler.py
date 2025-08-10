"""
FlaxKV 2.0 智能刷新调度器

基于时间、大小、访问模式等因素智能决定何时刷新缓冲区。
"""

import time
import asyncio
import logging
from typing import Any, Dict, Tuple

logger = logging.getLogger(__name__)


class FlushScheduler:
    """智能刷新调度器。
    
    基于时间、大小、访问模式等因素智能决定何时刷新缓冲区。
    """
    
    def __init__(self, buffer_timeout: float, buffer_size_limit: int):
        self.buffer_timeout = buffer_timeout
        self.buffer_size_limit = buffer_size_limit
        self._last_flush_time = time.time()
        self._flush_callbacks = []
        self._scheduler_task = None
        self._running = False
        
        # 统计信息
        self._stats = {
            'flushes_by_time': 0,
            'flushes_by_size': 0,
            'flushes_by_pressure': 0,
            'total_flushes': 0
        }
    
    async def start(self):
        """启动调度器。"""
        if self._running:
            return
        
        self._running = True
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info("刷新调度器已启动")
    
    async def stop(self):
        """停止调度器。"""
        self._running = False
        
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        
        logger.info("刷新调度器已停止")
    
    def add_flush_callback(self, callback):
        """添加刷新回调函数。"""
        self._flush_callbacks.append(callback)
    
    def should_flush(self, buffer_size: int, buffer_count: int, 
                    last_write_time: float) -> Tuple[bool, str]:
        """判断是否应该刷新。
        
        Args:
            buffer_size: 当前缓冲区大小（字节）
            buffer_count: 当前缓冲区条目数
            last_write_time: 最后写入时间
            
        Returns:
            (是否刷新, 刷新原因)
        """
        current_time = time.time()
        
        # 1. 时间触发
        if current_time - self._last_flush_time >= self.buffer_timeout:
            return True, "timeout"
        
        # 2. 大小触发
        if buffer_count >= self.buffer_size_limit:
            return True, "size"
        
        # 3. 内存压力触发（简化实现）
        estimated_memory = buffer_size
        if estimated_memory > 100 * 1024 * 1024:  # 100MB
            return True, "memory_pressure"
        
        # 4. 写入频率触发
        if current_time - last_write_time > self.buffer_timeout / 2:
            if buffer_count > 0:  # 有数据需要刷新
                return True, "idle"
        
        return False, ""
    
    async def _scheduler_loop(self):
        """调度器主循环。"""
        while self._running:
            try:
                # 每秒检查一次
                await asyncio.sleep(1.0)
                
                # 调用所有注册的刷新回调
                for callback in self._flush_callbacks:
                    try:
                        should_flush, reason = await callback()
                        if should_flush:
                            await self._trigger_flush(reason)
                    except Exception as e:
                        logger.error(f"刷新回调执行失败: {e}")
                
            except Exception as e:
                logger.error(f"调度器循环错误: {e}")
                await asyncio.sleep(1)
    
    async def _trigger_flush(self, reason: str):
        """触发刷新。"""
        self._last_flush_time = time.time()
        
        # 更新统计
        if reason == "timeout":
            self._stats['flushes_by_time'] += 1
        elif reason in ["size", "memory_pressure"]:
            self._stats['flushes_by_size'] += 1
        else:
            self._stats['flushes_by_pressure'] += 1
        
        self._stats['total_flushes'] += 1
        
        logger.debug(f"刷新触发: {reason}")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取调度器统计信息。"""
        return self._stats.copy()