"""
FlaxKV 2.0 事务实现

提供ACID事务特性：原子性、一致性、隔离性、持久性。
"""

import time
import logging
import uuid
from enum import Enum
from typing import Any, Dict, List, Set
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


class TransactionState(Enum):
    """事务状态枚举。"""
    ACTIVE = "active"
    PREPARING = "preparing"
    PREPARED = "prepared"
    COMMITTING = "committing"
    COMMITTED = "committed"
    ABORTING = "aborting"
    ABORTED = "aborted"


class IsolationLevel(Enum):
    """事务隔离级别。"""
    READ_UNCOMMITTED = "read_uncommitted"
    READ_COMMITTED = "read_committed"
    REPEATABLE_READ = "repeatable_read"
    SERIALIZABLE = "serializable"


@dataclass
class TransactionOperation:
    """事务操作记录。"""
    op_type: str  # "read", "write", "delete"
    key: str
    value: Any = None
    old_value: Any = None
    timestamp: float = field(default_factory=time.time)


class Transaction:
    """事务对象。
    
    提供ACID事务特性：
    - Atomicity: 原子性，通过操作日志和回滚实现
    - Consistency: 一致性，通过业务逻辑保证  
    - Isolation: 隔离性，通过锁机制实现
    - Durability: 持久性，通过WAL和存储引擎保证
    """
    
    def __init__(self, tx_manager, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED):
        """初始化事务。
        
        Args:
            tx_manager: 事务管理器
            isolation_level: 事务隔离级别
        """
        self.tx_id = str(uuid.uuid4())
        self.tx_manager = tx_manager
        self.isolation_level = isolation_level
        
        self.state = TransactionState.ACTIVE
        self.start_time = time.time()
        
        # 操作日志
        self.operations: List[TransactionOperation] = []
        self.read_set: Set[str] = set()  # 读取的键集合
        self.write_set: Set[str] = set()  # 写入的键集合
        
        # 快照（用于回滚）
        self.snapshots: Dict[str, Any] = {}
        
        # 统计信息
        self.stats = {
            'reads': 0,
            'writes': 0,
            'deletes': 0,
            'locks_acquired': 0,
            'rollback_operations': 0,
        }
    
    async def get(self, key: str) -> Any:
        """在事务中读取键。"""
        if self.state != TransactionState.ACTIVE:
            raise RuntimeError(f"事务状态不正确: {self.state}")
        
        # 1. 首先检查事务内的修改（事务的本地写集）
        for op in reversed(self.operations):  # 从最新操作开始检查
            if op.key == key:
                if op.op_type == "write":
                    # 事务内已修改，返回新值
                    operation = TransactionOperation("read", key, op.value)
                    self.operations.append(operation)
                    self.read_set.add(key)
                    self.stats['reads'] += 1
                    return op.value
                elif op.op_type == "delete":
                    # 事务内已删除，抛出 KeyError
                    operation = TransactionOperation("read", key, None)
                    self.operations.append(operation)
                    self.read_set.add(key)
                    self.stats['reads'] += 1
                    raise KeyError(key)
        
        # 2. 检查快照（用于 REPEATABLE_READ）
        if key in self.snapshots:
            # 快照中有数据，直接返回
            value = self.snapshots[key]
            if value is None:
                # 快照记录该键不存在
                operation = TransactionOperation("read", key, None)
                self.operations.append(operation)
                self.read_set.add(key)
                self.stats['reads'] += 1
                raise KeyError(key)
            else:
                operation = TransactionOperation("read", key, value)
                self.operations.append(operation)
                self.read_set.add(key)
                self.stats['reads'] += 1
                return value
        
        # 根据隔离级别决定锁策略
        lock_type = None
        if self.isolation_level in [IsolationLevel.REPEATABLE_READ, IsolationLevel.SERIALIZABLE]:
            lock_type = "shared"
        
        # 获取锁（如果需要）
        if lock_type:
            success = await self.tx_manager.lock_manager.acquire_lock(
                self.tx_id, key, lock_type
            )
            if not success:
                raise RuntimeError(f"无法获取锁: {key}")
            self.stats['locks_acquired'] += 1
        
        # 3. 从存储引擎读取值
        try:
            value = await self.tx_manager.storage_engine.get(key)
            
            # 将值保存到快照中（用于可重复读）
            if self.isolation_level in [IsolationLevel.REPEATABLE_READ, IsolationLevel.SERIALIZABLE]:
                self.snapshots[key] = value
            
            # 记录操作
            operation = TransactionOperation("read", key, value)
            self.operations.append(operation)
            self.read_set.add(key)
            self.stats['reads'] += 1
            
            return value
            
        except KeyError as e:
            # 键不存在，记录到快照中（避免幻读）
            if self.isolation_level in [IsolationLevel.REPEATABLE_READ, IsolationLevel.SERIALIZABLE]:
                self.snapshots[key] = None
            
            # 记录读取不存在的键
            operation = TransactionOperation("read", key, None)
            self.operations.append(operation)
            self.read_set.add(key)
            self.stats['reads'] += 1
            raise
    
    async def set(self, key: str, value: Any) -> None:
        """在事务中设置键。"""
        if self.state != TransactionState.ACTIVE:
            raise RuntimeError(f"事务状态不正确: {self.state}")
        
        # 获取排他锁
        success = await self.tx_manager.lock_manager.acquire_lock(
            self.tx_id, key, "exclusive"
        )
        if not success:
            raise RuntimeError(f"无法获取排他锁: {key}")
        self.stats['locks_acquired'] += 1
        
        # 保存原值快照（如果还没有）
        if key not in self.snapshots:
            try:
                old_value = await self.tx_manager.storage_engine.get(key)
                self.snapshots[key] = old_value
            except KeyError:
                self.snapshots[key] = None  # 键不存在
        
        # 记录操作
        old_value = self.snapshots[key]
        operation = TransactionOperation("write", key, value, old_value)
        self.operations.append(operation)
        self.write_set.add(key)
        self.stats['writes'] += 1
    
    async def delete(self, key: str) -> None:
        """在事务中删除键。"""
        if self.state != TransactionState.ACTIVE:
            raise RuntimeError(f"事务状态不正确: {self.state}")
        
        # 获取排他锁
        success = await self.tx_manager.lock_manager.acquire_lock(
            self.tx_id, key, "exclusive"
        )
        if not success:
            raise RuntimeError(f"无法获取排他锁: {key}")
        self.stats['locks_acquired'] += 1
        
        # 保存原值快照
        if key not in self.snapshots:
            try:
                old_value = await self.tx_manager.storage_engine.get(key)
                self.snapshots[key] = old_value
            except KeyError:
                self.snapshots[key] = None
        
        # 记录操作
        old_value = self.snapshots[key]
        operation = TransactionOperation("delete", key, None, old_value)
        self.operations.append(operation)
        self.write_set.add(key)
        self.stats['deletes'] += 1
    
    async def contains(self, key: str) -> bool:
        """检查键是否存在。"""
        try:
            await self.get(key)
            return True
        except KeyError:
            return False
    
    async def commit(self) -> None:
        """提交事务。"""
        if self.state != TransactionState.ACTIVE:
            raise RuntimeError(f"事务状态不正确: {self.state}")
        
        self.state = TransactionState.COMMITTING
        
        try:
            # 执行所有写操作
            write_operations = 0
            delete_operations = 0
            
            for operation in self.operations:
                if operation.op_type == "write":
                    await self.tx_manager.storage_engine.set(operation.key, operation.value)
                    write_operations += 1
                elif operation.op_type == "delete":
                    try:
                        await self.tx_manager.storage_engine.delete(operation.key)
                        delete_operations += 1
                    except KeyError:
                        pass  # 键可能已经被删除
            
            self.state = TransactionState.COMMITTED
            
            logger.debug(f"事务提交成功: {self.tx_id}, "
                       f"读取{self.stats['reads']}次, "
                       f"写入{write_operations}次, "
                       f"删除{delete_operations}次, "
                       f"耗时{time.time() - self.start_time:.3f}秒")
            
        except Exception as e:
            logger.error(f"事务提交失败: {self.tx_id}, {e}")
            await self.rollback()
            raise
        finally:
            # 释放锁
            self.tx_manager.lock_manager.release_locks(self.tx_id)
    
    async def rollback(self) -> None:
        """回滚事务。"""
        if self.state in [TransactionState.COMMITTED, TransactionState.ABORTED]:
            return  # 已经结束的事务不需要回滚
        
        self.state = TransactionState.ABORTING
        
        try:
            # 恢复所有修改的键的原值
            rollback_count = 0
            
            for key in self.write_set:
                if key in self.snapshots:
                    old_value = self.snapshots[key]
                    if old_value is None:
                        # 原本不存在，删除
                        try:
                            await self.tx_manager.storage_engine.delete(key)
                            rollback_count += 1
                        except KeyError:
                            pass
                    else:
                        # 恢复原值
                        await self.tx_manager.storage_engine.set(key, old_value)
                        rollback_count += 1
            
            self.stats['rollback_operations'] = rollback_count
            self.state = TransactionState.ABORTED
            
            logger.debug(f"事务回滚成功: {self.tx_id}, "
                       f"回滚{rollback_count}个操作, "
                       f"耗时{time.time() - self.start_time:.3f}秒")
            
        except Exception as e:
            logger.error(f"事务回滚失败: {self.tx_id}, {e}")
            raise
        finally:
            # 释放锁
            self.tx_manager.lock_manager.release_locks(self.tx_id)
    
    def get_read_set(self) -> Set[str]:
        """获取读集合的副本。"""
        return self.read_set.copy()
    
    def get_write_set(self) -> Set[str]:
        """获取写集合的副本。"""
        return self.write_set.copy()
    
    def get_operations(self) -> List[TransactionOperation]:
        """获取操作列表的副本。"""
        return self.operations.copy()
    
    def get_snapshots(self) -> Dict[str, Any]:
        """获取快照的副本。"""
        return self.snapshots.copy()
    
    def get_info(self) -> Dict[str, Any]:
        """获取事务信息。"""
        return {
            'tx_id': self.tx_id,
            'state': self.state.value,
            'isolation_level': self.isolation_level.value,
            'start_time': self.start_time,
            'duration': time.time() - self.start_time,
            'operations_count': len(self.operations),
            'read_set_size': len(self.read_set),
            'write_set_size': len(self.write_set),
            'snapshot_size': len(self.snapshots),
            'stats': self.stats.copy(),
        }
    
    def is_read_only(self) -> bool:
        """判断是否为只读事务。"""
        return len(self.write_set) == 0
    
    def get_duration(self) -> float:
        """获取事务持续时间（秒）。"""
        return time.time() - self.start_time
    
    def __str__(self) -> str:
        return f"Transaction({self.tx_id[:8]}, {self.state.value}, {self.isolation_level.value})"
    
    def __repr__(self) -> str:
        return (f"Transaction(tx_id='{self.tx_id}', state={self.state}, "
                f"isolation_level={self.isolation_level}, "
                f"operations={len(self.operations)})")


class TransactionConflictError(Exception):
    """事务冲突异常。"""
    
    def __init__(self, message: str, tx_id: str, conflicting_tx_id: str = None):
        super().__init__(message)
        self.tx_id = tx_id
        self.conflicting_tx_id = conflicting_tx_id


class TransactionDeadlockError(Exception):
    """事务死锁异常。"""
    
    def __init__(self, message: str, deadlock_cycle: List[str]):
        super().__init__(message)
        self.deadlock_cycle = deadlock_cycle


class TransactionTimeoutError(Exception):
    """事务超时异常。"""
    
    def __init__(self, message: str, tx_id: str, timeout: float):
        super().__init__(message)
        self.tx_id = tx_id
        self.timeout = timeout