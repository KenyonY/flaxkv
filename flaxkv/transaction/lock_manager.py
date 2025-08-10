"""
FlaxKV 2.0 锁管理和死锁检测

实现两阶段锁定（2PL）协议，支持共享锁和排他锁，以及死锁检测。
"""

import time
import asyncio
import logging
import threading
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class LockInfo:
    """锁信息。"""
    transaction_id: str
    lock_type: str  # "shared", "exclusive"
    key: str
    acquired_at: float = field(default_factory=time.time)


class DeadlockDetector:
    """死锁检测器。"""
    
    def __init__(self):
        self._wait_graph = {}  # 等待图：tx_id -> set of tx_ids
        self._lock = threading.Lock()
    
    def add_wait_edge(self, waiting_tx: str, blocking_tx: str):
        """添加等待边。"""
        with self._lock:
            if waiting_tx not in self._wait_graph:
                self._wait_graph[waiting_tx] = set()
            self._wait_graph[waiting_tx].add(blocking_tx)
    
    def remove_wait_edges(self, tx_id: str):
        """移除事务的所有等待边。"""
        with self._lock:
            # 移除该事务作为等待者的边
            if tx_id in self._wait_graph:
                del self._wait_graph[tx_id]
            
            # 移除该事务作为被等待者的边
            for waiting_tx in self._wait_graph:
                self._wait_graph[waiting_tx].discard(tx_id)
    
    def detect_deadlock(self) -> Optional[List[str]]:
        """检测死锁环。
        
        Returns:
            如果存在死锁，返回死锁环中的事务ID列表；否则返回None
        """
        with self._lock:
            visited = set()
            path = []
            
            def dfs(node: str) -> Optional[List[str]]:
                if node in path:
                    # 找到环，返回环中的节点
                    cycle_start = path.index(node)
                    return path[cycle_start:]
                
                if node in visited:
                    return None
                
                visited.add(node)
                path.append(node)
                
                for neighbor in self._wait_graph.get(node, []):
                    cycle = dfs(neighbor)
                    if cycle:
                        return cycle
                
                path.pop()
                return None
            
            for tx_id in self._wait_graph:
                if tx_id not in visited:
                    cycle = dfs(tx_id)
                    if cycle:
                        return cycle
            
            return None
    
    def get_wait_graph(self) -> Dict[str, Set[str]]:
        """获取等待图的副本。"""
        with self._lock:
            return {tx: deps.copy() for tx, deps in self._wait_graph.items()}
    
    def clear(self):
        """清空等待图。"""
        with self._lock:
            self._wait_graph.clear()


class LockManager:
    """锁管理器。
    
    实现两阶段锁定（2PL）协议，支持共享锁和排他锁。
    """
    
    def __init__(self):
        self._locks = {}  # key -> list of LockInfo
        self._lock = threading.RLock()
        self._deadlock_detector = DeadlockDetector()
        
        # 等待锁的事务
        self._waiting_transactions = {}  # tx_id -> asyncio.Event
        
        # 统计信息
        self._stats = {
            'locks_acquired': 0,
            'locks_released': 0,
            'deadlocks_detected': 0,
            'lock_timeouts': 0,
        }
    
    async def acquire_lock(self, tx_id: str, key: str, 
                          lock_type: str, timeout: float = 30.0) -> bool:
        """获取锁。
        
        Args:
            tx_id: 事务ID
            key: 要锁定的键
            lock_type: 锁类型 ("shared" 或 "exclusive")
            timeout: 超时时间（秒）
            
        Returns:
            是否成功获取锁
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            with self._lock:
                if self._can_acquire_lock(tx_id, key, lock_type):
                    # 可以获取锁
                    if key not in self._locks:
                        self._locks[key] = []
                    
                    lock_info = LockInfo(tx_id, lock_type, key)
                    self._locks[key].append(lock_info)
                    
                    # 从等待图中移除相关边
                    self._deadlock_detector.remove_wait_edges(tx_id)
                    
                    # 更新统计
                    self._stats['locks_acquired'] += 1
                    
                    return True
                else:
                    # 不能获取锁，检查是否会导致死锁
                    blocking_txs = self._get_blocking_transactions(key, lock_type)
                    
                    for blocking_tx in blocking_txs:
                        self._deadlock_detector.add_wait_edge(tx_id, blocking_tx)
                    
                    # 检测死锁
                    deadlock_cycle = self._deadlock_detector.detect_deadlock()
                    if deadlock_cycle and tx_id in deadlock_cycle:
                        logger.warning(f"检测到死锁: {deadlock_cycle}")
                        self._stats['deadlocks_detected'] += 1
                        return False
            
            # 等待一小段时间后重试
            await asyncio.sleep(0.1)
        
        logger.warning(f"事务 {tx_id} 获取锁超时: {key}")
        self._stats['lock_timeouts'] += 1
        return False
    
    def _can_acquire_lock(self, tx_id: str, key: str, lock_type: str) -> bool:
        """检查是否可以获取锁。"""
        if key not in self._locks:
            return True
        
        existing_locks = self._locks[key]
        
        for lock in existing_locks:
            if lock.transaction_id == tx_id:
                # 同一事务已持有锁
                if lock_type == "shared" or lock.lock_type == lock_type:
                    return True
                # 锁升级：从共享锁升级到排他锁需要检查是否只有当前事务持有共享锁
                if lock.lock_type == "shared" and lock_type == "exclusive":
                    return len(existing_locks) == 1
                return False
            
            if lock_type == "exclusive" or lock.lock_type == "exclusive":
                # 排他锁与任何锁都不兼容
                return False
        
        # 所有现有锁都是共享锁，且请求的也是共享锁
        return lock_type == "shared"
    
    def _get_blocking_transactions(self, key: str, lock_type: str) -> List[str]:
        """获取阻塞当前请求的事务列表。"""
        if key not in self._locks:
            return []
        
        blocking_txs = []
        for lock in self._locks[key]:
            if lock_type == "exclusive" or lock.lock_type == "exclusive":
                blocking_txs.append(lock.transaction_id)
        
        return blocking_txs
    
    def release_locks(self, tx_id: str):
        """释放事务的所有锁。"""
        with self._lock:
            keys_to_remove = []
            released_count = 0
            
            for key, locks in self._locks.items():
                # 统计要释放的锁数量
                before_count = len(locks)
                
                # 移除该事务的锁
                self._locks[key] = [lock for lock in locks 
                                  if lock.transaction_id != tx_id]
                
                # 计算释放的锁数量
                after_count = len(self._locks[key])
                released_count += before_count - after_count
                
                # 如果该键没有锁了，标记为删除
                if not self._locks[key]:
                    keys_to_remove.append(key)
            
            # 删除空的键
            for key in keys_to_remove:
                del self._locks[key]
            
            # 从死锁检测器中移除
            self._deadlock_detector.remove_wait_edges(tx_id)
            
            # 更新统计
            self._stats['locks_released'] += released_count
    
    def get_lock_info(self, key: str) -> List[LockInfo]:
        """获取键的锁信息。"""
        with self._lock:
            return self._locks.get(key, []).copy()
    
    def get_all_locks(self) -> Dict[str, List[LockInfo]]:
        """获取所有锁信息。"""
        with self._lock:
            return {key: locks.copy() for key, locks in self._locks.items()}
    
    def get_transaction_locks(self, tx_id: str) -> Dict[str, List[LockInfo]]:
        """获取指定事务持有的所有锁。"""
        with self._lock:
            tx_locks = {}
            for key, locks in self._locks.items():
                tx_key_locks = [lock for lock in locks if lock.transaction_id == tx_id]
                if tx_key_locks:
                    tx_locks[key] = tx_key_locks
            return tx_locks
    
    def get_stats(self) -> Dict[str, int]:
        """获取锁管理器统计信息。"""
        with self._lock:
            stats = self._stats.copy()
            stats['active_locks'] = sum(len(locks) for locks in self._locks.values())
            stats['locked_keys'] = len(self._locks)
        return stats
    
    def get_deadlock_info(self) -> Dict[str, any]:
        """获取死锁检测相关信息。"""
        wait_graph = self._deadlock_detector.get_wait_graph()
        return {
            'wait_graph': {tx: list(deps) for tx, deps in wait_graph.items()},
            'waiting_transactions': len(wait_graph),
            'deadlocks_detected': self._stats['deadlocks_detected'],
        }
    
    def clear_all_locks(self):
        """清空所有锁（仅用于测试）。"""
        with self._lock:
            self._locks.clear()
            self._deadlock_detector.clear()
            logger.warning("已清空所有锁（仅用于测试）")
    
    def is_locked(self, key: str, lock_type: Optional[str] = None) -> bool:
        """检查键是否被锁定。
        
        Args:
            key: 键名
            lock_type: 锁类型过滤，None表示任何类型
            
        Returns:
            是否被指定类型的锁锁定
        """
        with self._lock:
            if key not in self._locks:
                return False
            
            if lock_type is None:
                return len(self._locks[key]) > 0
            
            return any(lock.lock_type == lock_type for lock in self._locks[key])
    
    def get_lock_holders(self, key: str) -> List[str]:
        """获取持有指定键锁的事务ID列表。"""
        with self._lock:
            if key not in self._locks:
                return []
            
            return [lock.transaction_id for lock in self._locks[key]]