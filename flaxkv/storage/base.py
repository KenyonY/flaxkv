"""
FlaxKV 2.0 存储引擎抽象基类

定义统一的存储引擎接口，支持本地和远程存储引擎的无缝切换。
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, AsyncIterator, Union
from contextlib import asynccontextmanager
import time


class StorageEngine(ABC):
    """存储引擎抽象基类。
    
    定义了所有存储引擎必须实现的统一接口，包括：
    - 基础CRUD操作
    - 批量操作
    - 范围查询  
    - 事务支持
    - 监控指标
    """
    
    def __init__(self, config):
        """初始化存储引擎。
        
        Args:
            config: FlaxKVConfig 配置对象
        """
        self.config = config
        self._is_closed = False
        self._stats = {
            'total_reads': 0,
            'total_writes': 0,
            'total_deletes': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'read_times': [],
            'write_times': [],
        }
    
    @abstractmethod
    async def initialize(self) -> None:
        """初始化存储引擎。
        
        执行必要的初始化操作，如创建连接、初始化索引等。
        """
        pass
    
    @abstractmethod
    async def get(self, key: str) -> Any:
        """获取单个键的值。
        
        Args:
            key: 键名
            
        Returns:
            键对应的值
            
        Raises:
            KeyError: 键不存在时抛出
        """
        pass
    
    @abstractmethod
    async def set(self, key: str, value: Any) -> None:
        """设置单个键值对。
        
        Args:
            key: 键名
            value: 值
        """
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> bool:
        """删除单个键。
        
        Args:
            key: 键名
            
        Returns:
            是否删除成功（键存在返回True，不存在返回False）
        """
        pass
    
    @abstractmethod
    async def contains(self, key: str) -> bool:
        """检查键是否存在。
        
        Args:
            key: 键名
            
        Returns:
            键是否存在
        """
        pass
    
    @abstractmethod
    async def size(self) -> int:
        """获取数据库中键的总数。
        
        Returns:
            键的总数
        """
        pass
    
    # === 批量操作 ===
    
    async def batch_get(self, keys: List[str]) -> Dict[str, Any]:
        """批量获取键值对。
        
        Args:
            keys: 键名列表
            
        Returns:
            键值对字典（不存在的键不包含在结果中）
        """
        result = {}
        for key in keys:
            try:
                result[key] = await self.get(key)
            except KeyError:
                continue
        return result
    
    async def batch_set(self, items: Dict[str, Any]) -> None:
        """批量设置键值对。
        
        Args:
            items: 键值对字典
        """
        for key, value in items.items():
            await self.set(key, value)
    
    async def batch_delete(self, keys: List[str]) -> int:
        """批量删除键。
        
        Args:
            keys: 键名列表
            
        Returns:
            成功删除的键数量
        """
        deleted_count = 0
        for key in keys:
            if await self.delete(key):
                deleted_count += 1
        return deleted_count
    
    # === 范围查询 ===
    
    async def scan(self, 
                   prefix: Optional[str] = None,
                   start: Optional[str] = None,
                   end: Optional[str] = None,
                   limit: Optional[int] = None) -> AsyncIterator[Tuple[str, Any]]:
        """扫描键值对。
        
        Args:
            prefix: 键前缀过滤
            start: 起始键（包含）
            end: 结束键（不包含）
            limit: 最大返回数量
            
        Yields:
            (键, 值) 元组
        """
        # 默认实现：获取所有键然后过滤
        # 子类应该重写此方法提供更高效的实现
        count = 0
        async for key, value in self._iterate_all():
            # 前缀过滤
            if prefix and not key.startswith(prefix):
                continue
            
            # 范围过滤
            if start and key < start:
                continue
            if end and key >= end:
                continue
            
            yield key, value
            
            count += 1
            if limit and count >= limit:
                break
    
    @abstractmethod
    async def _iterate_all(self) -> AsyncIterator[Tuple[str, Any]]:
        """迭代所有键值对。
        
        子类必须实现此方法。
        
        Yields:
            (键, 值) 元组
        """
        pass
    
    # === 事务支持 ===
    
    @asynccontextmanager
    async def transaction(self):
        """事务上下文管理器。
        
        Usage:
            async with engine.transaction() as tx:
                await tx.set('key1', 'value1')
                await tx.set('key2', 'value2')
                # 自动提交或回滚
        """
        tx = Transaction(self)
        await tx.begin()
        try:
            yield tx
            await tx.commit()
        except Exception:
            await tx.rollback()
            raise
    
    async def begin_transaction(self):
        """开始事务。
        
        Returns:
            Transaction对象
        """
        tx = Transaction(self)
        await tx.begin()
        return tx
    
    # === TTL支持 ===
    
    async def setex(self, key: str, value: Any, expire: int) -> None:
        """设置带过期时间的键值对。
        
        Args:
            key: 键名
            value: 值
            expire: 过期时间（秒）
        """
        await self.set(key, value)
        await self.expire(key, expire)
    
    async def expire(self, key: str, seconds: int) -> bool:
        """设置键的过期时间。
        
        Args:
            key: 键名
            seconds: 过期时间（秒）
            
        Returns:
            设置是否成功
        """
        # 默认实现：不支持TTL
        # 支持TTL的引擎需要重写此方法
        return False
    
    async def ttl(self, key: str) -> int:
        """获取键的剩余过期时间。
        
        Args:
            key: 键名
            
        Returns:
            剩余过期时间（秒），-1表示永不过期，-2表示键不存在
        """
        # 默认实现：永不过期
        if await self.contains(key):
            return -1
        else:
            return -2
    
    # === 数据备份和恢复 ===
    
    async def backup(self, path: str) -> Dict[str, Any]:
        """备份数据到文件。
        
        Args:
            path: 备份文件路径
            
        Returns:
            备份统计信息
        """
        import pickle
        import gzip
        from pathlib import Path
        
        backup_data = {}
        stats = {'total_keys': 0, 'total_size': 0, 'start_time': time.time()}
        
        # 收集所有数据
        async for key, value in self._iterate_all():
            backup_data[key] = value
            stats['total_keys'] += 1
        
        # 写入备份文件
        backup_path = Path(path)
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        
        with gzip.open(backup_path, 'wb') as f:
            pickle.dump(backup_data, f)
        
        stats['total_size'] = backup_path.stat().st_size
        stats['end_time'] = time.time()
        stats['duration'] = stats['end_time'] - stats['start_time']
        
        return stats
    
    async def restore(self, path: str) -> Dict[str, Any]:
        """从备份文件恢复数据。
        
        Args:
            path: 备份文件路径
            
        Returns:
            恢复统计信息
        """
        import pickle
        import gzip
        from pathlib import Path
        
        backup_path = Path(path)
        if not backup_path.exists():
            raise FileNotFoundError(f"备份文件不存在: {path}")
        
        stats = {'restored_keys': 0, 'start_time': time.time()}
        
        # 读取备份数据
        with gzip.open(backup_path, 'rb') as f:
            backup_data = pickle.load(f)
        
        # 恢复数据
        await self.batch_set(backup_data)
        stats['restored_keys'] = len(backup_data)
        
        stats['end_time'] = time.time()
        stats['duration'] = stats['end_time'] - stats['start_time']
        
        return stats
    
    # === 监控和统计 ===
    
    def get_stats(self) -> Dict[str, Any]:
        """获取存储引擎统计信息。
        
        Returns:
            统计信息字典
        """
        read_times = self._stats['read_times']
        write_times = self._stats['write_times']
        
        stats = {
            'total_reads': self._stats['total_reads'],
            'total_writes': self._stats['total_writes'],
            'total_deletes': self._stats['total_deletes'],
            'cache_hits': self._stats['cache_hits'],
            'cache_misses': self._stats['cache_misses'],
        }
        
        # 计算平均响应时间
        if read_times:
            stats['avg_read_time'] = sum(read_times) / len(read_times)
            stats['p99_read_time'] = sorted(read_times)[int(len(read_times) * 0.99)]
        else:
            stats['avg_read_time'] = 0
            stats['p99_read_time'] = 0
        
        if write_times:
            stats['avg_write_time'] = sum(write_times) / len(write_times)
            stats['p99_write_time'] = sorted(write_times)[int(len(write_times) * 0.99)]
        else:
            stats['avg_write_time'] = 0
            stats['p99_write_time'] = 0
        
        # 计算缓存命中率
        total_cache_ops = stats['cache_hits'] + stats['cache_misses']
        if total_cache_ops > 0:
            stats['cache_hit_rate'] = stats['cache_hits'] / total_cache_ops
        else:
            stats['cache_hit_rate'] = 0
        
        return stats
    
    def _record_operation(self, operation: str, duration: float, cache_hit: bool = False):
        """记录操作统计。
        
        Args:
            operation: 操作类型 (read/write/delete)
            duration: 操作耗时（毫秒）
            cache_hit: 是否为缓存命中
        """
        if operation == 'read':
            self._stats['total_reads'] += 1
            self._stats['read_times'].append(duration)
            if cache_hit:
                self._stats['cache_hits'] += 1
            else:
                self._stats['cache_misses'] += 1
        elif operation == 'write':
            self._stats['total_writes'] += 1
            self._stats['write_times'].append(duration)
        elif operation == 'delete':
            self._stats['total_deletes'] += 1
        
        # 保持统计数据大小
        max_samples = 1000
        if len(self._stats['read_times']) > max_samples:
            self._stats['read_times'] = self._stats['read_times'][-max_samples:]
        if len(self._stats['write_times']) > max_samples:
            self._stats['write_times'] = self._stats['write_times'][-max_samples:]
    
    # === 资源管理 ===
    
    async def close(self) -> None:
        """关闭存储引擎，释放资源。"""
        self._is_closed = True
    
    async def __aenter__(self):
        """异步上下文管理器入口。"""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口。"""
        await self.close()
    
    def is_closed(self) -> bool:
        """检查引擎是否已关闭。"""
        return self._is_closed


class Transaction:
    """事务对象。
    
    提供事务的开始、提交、回滚功能。
    """
    
    def __init__(self, engine: StorageEngine):
        """初始化事务。
        
        Args:
            engine: 存储引擎实例
        """
        self.engine = engine
        self._operations = []
        self._snapshots = {}
        self._is_active = False
    
    async def begin(self) -> None:
        """开始事务。"""
        if self._is_active:
            raise RuntimeError("事务已经开始")
        self._is_active = True
        self._operations = []
        self._snapshots = {}
    
    async def commit(self) -> None:
        """提交事务。"""
        if not self._is_active:
            raise RuntimeError("事务未开始或已结束")
        
        # 执行所有操作
        for operation in self._operations:
            await operation.execute()
        
        self._is_active = False
    
    async def rollback(self) -> None:
        """回滚事务。"""
        if not self._is_active:
            raise RuntimeError("事务未开始或已结束")
        
        # 恢复所有快照
        for key, value in self._snapshots.items():
            if value is None:
                # 原本不存在，删除
                await self.engine.delete(key)
            else:
                # 恢复原值
                await self.engine.set(key, value)
        
        self._is_active = False
    
    async def set(self, key: str, value: Any) -> None:
        """在事务中设置键值对。"""
        if not self._is_active:
            raise RuntimeError("事务未开始")
        
        # 保存原值快照
        if key not in self._snapshots:
            try:
                self._snapshots[key] = await self.engine.get(key)
            except KeyError:
                self._snapshots[key] = None
        
        # 添加设置操作
        self._operations.append(SetOperation(self.engine, key, value))
    
    async def delete(self, key: str) -> None:
        """在事务中删除键。"""
        if not self._is_active:
            raise RuntimeError("事务未开始")
        
        # 保存原值快照
        if key not in self._snapshots:
            try:
                self._snapshots[key] = await self.engine.get(key)
            except KeyError:
                self._snapshots[key] = None
        
        # 添加删除操作
        self._operations.append(DeleteOperation(self.engine, key))
    
    def is_active(self) -> bool:
        """检查事务是否活跃。"""
        return self._is_active


class Operation(ABC):
    """事务操作基类。"""
    
    def __init__(self, engine: StorageEngine, key: str):
        self.engine = engine
        self.key = key
    
    @abstractmethod
    async def execute(self) -> None:
        """执行操作。"""
        pass


class SetOperation(Operation):
    """设置操作。"""
    
    def __init__(self, engine: StorageEngine, key: str, value: Any):
        super().__init__(engine, key)
        self.value = value
    
    async def execute(self) -> None:
        """执行设置操作。"""
        await self.engine.set(self.key, self.value)


class DeleteOperation(Operation):
    """删除操作。"""
    
    async def execute(self) -> None:
        """执行删除操作。"""
        await self.engine.delete(self.key)