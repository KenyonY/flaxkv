"""
FlaxKV 2.0 事务管理器

负责事务的创建、管理、协调和监控。
"""

import time
import asyncio
import logging
import threading
from typing import Any, Dict, List
from contextlib import asynccontextmanager

from .transaction import Transaction, TransactionState, IsolationLevel
from .lock_manager import LockManager

logger = logging.getLogger(__name__)


class TransactionManager:
    """事务管理器。
    
    负责事务的创建、管理、协调和监控。
    """
    
    def __init__(self, storage_engine, config=None, cache_invalidation_callback=None):
        """初始化事务管理器。
        
        Args:
            storage_engine: 存储引擎
            config: 配置对象
            cache_invalidation_callback: 缓存失效回调函数
        """
        self.storage_engine = storage_engine
        self.config = config
        self.lock_manager = LockManager()
        self.cache_invalidation_callback = cache_invalidation_callback
        
        # 活跃事务
        self.active_transactions: Dict[str, Transaction] = {}
        self._tx_lock = threading.RLock()
        
        # 统计信息
        self.stats = {
            'transactions_started': 0,
            'transactions_committed': 0,
            'transactions_aborted': 0,
            'deadlocks_detected': 0,
            'expired_transactions_cleaned': 0,
        }
        
        # 清理任务
        self._cleanup_task = None
        self._running = False
        
        # 配置参数
        self.transaction_timeout = getattr(config, 'transaction_timeout', 3600) if config else 3600  # 1小时
        self.cleanup_interval = getattr(config, 'cleanup_interval', 60) if config else 60  # 1分钟
    
    async def start(self) -> None:
        """启动事务管理器。"""
        if self._running:
            return
        
        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_expired_transactions())
        logger.info("事务管理器已启动")
    
    async def stop(self) -> None:
        """停止事务管理器。"""
        if not self._running:
            return
        
        self._running = False
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        # 回滚所有活跃事务
        with self._tx_lock:
            active_tx_list = list(self.active_transactions.values())
        
        for tx in active_tx_list:
            try:
                logger.warning(f"停止时回滚活跃事务: {tx.tx_id}")
                await self.abort_transaction(tx)
            except Exception as e:
                logger.error(f"停止时回滚事务失败: {tx.tx_id}, {e}")
        
        with self._tx_lock:
            self.active_transactions.clear()
        
        logger.info("事务管理器已停止")
    
    def begin_transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED) -> Transaction:
        """开始新事务。
        
        Args:
            isolation_level: 隔离级别
            
        Returns:
            事务对象
        """
        tx = Transaction(self, isolation_level)
        
        with self._tx_lock:
            self.active_transactions[tx.tx_id] = tx
            self.stats['transactions_started'] += 1
        
        logger.debug(f"事务开始: {tx.tx_id}, 隔离级别: {isolation_level.value}")
        return tx
    
    async def commit_transaction(self, tx: Transaction) -> None:
        """提交事务。"""
        if tx.tx_id not in self.active_transactions:
            raise RuntimeError(f"事务不存在或已结束: {tx.tx_id}")
        
        try:
            await tx.commit()
            
            # 缓存失效
            if self.cache_invalidation_callback and not tx.is_read_only():
                for key in tx.get_write_set():
                    try:
                        self.cache_invalidation_callback(key)
                    except Exception as e:
                        logger.error(f"缓存失效回调失败 {key}: {e}")
            
            with self._tx_lock:
                self.active_transactions.pop(tx.tx_id, None)
                self.stats['transactions_committed'] += 1
            
            logger.debug(f"事务提交完成: {tx.tx_id}")
                
        except Exception as e:
            logger.error(f"提交事务失败: {tx.tx_id}, {e}")
            await self.abort_transaction(tx)
            raise
    
    async def abort_transaction(self, tx: Transaction) -> None:
        """中止事务。"""
        try:
            await tx.rollback()
            
            with self._tx_lock:
                self.active_transactions.pop(tx.tx_id, None)
                self.stats['transactions_aborted'] += 1
            
            logger.debug(f"事务中止完成: {tx.tx_id}")
                
        except Exception as e:
            logger.error(f"中止事务失败: {tx.tx_id}, {e}")
            # 即使回滚失败，也要从活跃事务中移除
            with self._tx_lock:
                self.active_transactions.pop(tx.tx_id, None)
                self.stats['transactions_aborted'] += 1
            raise
    
    @asynccontextmanager
    async def transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED):
        """事务上下文管理器。
        
        Usage:
            async with tx_manager.transaction() as tx:
                await tx.set('key', 'value')
                # 自动提交或回滚
        """
        tx = self.begin_transaction(isolation_level)
        try:
            yield tx
            await self.commit_transaction(tx)
        except Exception:
            await self.abort_transaction(tx)
            raise
    
    async def _cleanup_expired_transactions(self):
        """清理过期事务。"""
        while self._running:
            try:
                await asyncio.sleep(self.cleanup_interval)
                
                current_time = time.time()
                expired_transactions = []
                
                with self._tx_lock:
                    for tx in self.active_transactions.values():
                        # 检查事务是否超时
                        if current_time - tx.start_time > self.transaction_timeout:
                            expired_transactions.append(tx)
                
                # 回滚过期事务
                for tx in expired_transactions:
                    try:
                        logger.warning(f"清理过期事务: {tx.tx_id}, "
                                     f"已运行{tx.get_duration():.1f}秒")
                        await self.abort_transaction(tx)
                        self.stats['expired_transactions_cleaned'] += 1
                    except Exception as e:
                        logger.error(f"清理过期事务失败: {tx.tx_id}, {e}")
                
                # 清理锁管理器的统计信息（可选）
                if len(expired_transactions) > 0:
                    logger.info(f"清理了 {len(expired_transactions)} 个过期事务")
                
            except Exception as e:
                logger.error(f"清理任务错误: {e}")
                await asyncio.sleep(10)
    
    def get_active_transactions(self) -> List[Dict[str, Any]]:
        """获取活跃事务列表。"""
        with self._tx_lock:
            return [tx.get_info() for tx in self.active_transactions.values()]
    
    def get_transaction(self, tx_id: str) -> Transaction:
        """根据ID获取事务。"""
        with self._tx_lock:
            tx = self.active_transactions.get(tx_id)
            if tx is None:
                raise KeyError(f"事务不存在: {tx_id}")
            return tx
    
    def get_stats(self) -> Dict[str, Any]:
        """获取事务统计信息。"""
        with self._tx_lock:
            active_count = len(self.active_transactions)
            
            # 按状态分组统计
            state_counts = {}
            read_only_count = 0
            long_running_count = 0
            current_time = time.time()
            
            for tx in self.active_transactions.values():
                state = tx.state.value
                state_counts[state] = state_counts.get(state, 0) + 1
                
                if tx.is_read_only():
                    read_only_count += 1
                    
                if current_time - tx.start_time > 300:  # 5分钟
                    long_running_count += 1
        
        # 获取锁管理器统计
        lock_stats = self.lock_manager.get_stats()
        
        return {
            **self.stats,
            'active_transactions': active_count,
            'state_distribution': state_counts,
            'read_only_transactions': read_only_count,
            'long_running_transactions': long_running_count,
            'lock_stats': lock_stats,
            'deadlock_info': self.lock_manager.get_deadlock_info(),
        }
    
    def get_lock_status(self) -> Dict[str, List[Dict[str, Any]]]:
        """获取锁状态。"""
        all_locks = self.lock_manager.get_all_locks()
        
        lock_status = {}
        for key, locks in all_locks.items():
            lock_status[key] = [
                {
                    'transaction_id': lock.transaction_id,
                    'lock_type': lock.lock_type,
                    'acquired_at': lock.acquired_at,
                    'duration': time.time() - lock.acquired_at,
                }
                for lock in locks
            ]
        
        return lock_status
    
    def get_detailed_info(self) -> Dict[str, Any]:
        """获取详细的管理器信息。"""
        with self._tx_lock:
            active_txs = list(self.active_transactions.values())
        
        # 计算各种指标
        current_time = time.time()
        durations = [current_time - tx.start_time for tx in active_txs]
        
        avg_duration = sum(durations) / len(durations) if durations else 0
        max_duration = max(durations) if durations else 0
        
        total_operations = sum(len(tx.get_operations()) for tx in active_txs)
        total_read_set_size = sum(len(tx.get_read_set()) for tx in active_txs)
        total_write_set_size = sum(len(tx.get_write_set()) for tx in active_txs)
        
        return {
            'manager_info': {
                'running': self._running,
                'transaction_timeout': self.transaction_timeout,
                'cleanup_interval': self.cleanup_interval,
            },
            'statistics': self.get_stats(),
            'performance_metrics': {
                'average_transaction_duration': avg_duration,
                'max_transaction_duration': max_duration,
                'total_operations': total_operations,
                'total_read_set_size': total_read_set_size,
                'total_write_set_size': total_write_set_size,
            },
            'lock_status_summary': {
                'total_locked_keys': len(self.lock_manager.get_all_locks()),
                'total_active_locks': sum(len(locks) for locks in self.lock_manager.get_all_locks().values()),
            }
        }
    
    def force_abort_transaction(self, tx_id: str, reason: str = "强制中止") -> bool:
        """强制中止指定事务。
        
        Args:
            tx_id: 事务ID
            reason: 中止原因
            
        Returns:
            是否成功中止
        """
        with self._tx_lock:
            tx = self.active_transactions.get(tx_id)
            if tx is None:
                return False
        
        try:
            logger.warning(f"强制中止事务 {tx_id}: {reason}")
            # 这里使用 asyncio.create_task 来异步执行，避免阻塞
            asyncio.create_task(self.abort_transaction(tx))
            return True
        except Exception as e:
            logger.error(f"强制中止事务失败 {tx_id}: {e}")
            return False