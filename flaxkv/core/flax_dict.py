"""
FlaxDict - 基于FlaxKV的字典式持久化存储

提供完全兼容Python dict接口的持久化字典实现。
"""

from typing import Any, Dict, Iterator, Optional, Union, Tuple
from collections.abc import MutableMapping

from .sync_wrapper import ThreadSafeFlaxKV
from .config import FlaxKVConfig


class FlaxDict(MutableMapping):
    """基于FlaxKV的持久化字典。
    
    完全兼容Python dict接口的持久化字典实现，数据自动保存到磁盘。
    
    特性：
    - 完全兼容Python dict接口
    - 数据持久化存储
    - 线程安全
    - 高性能
    - 支持复杂数据类型
    
    使用示例：
        >>> db = FlaxDict('mydb')
        >>> db['key'] = 'value'
        >>> db['data'] = {'nested': [1, 2, 3]}
        >>> print(db['key'])  # 'value'
        >>> print(len(db))   # 2
        >>> 
        >>> # 字典方法
        >>> db.update({'a': 1, 'b': 2})
        >>> value = db.pop('a')
        >>> default = db.setdefault('c', 'default')
        >>>
        >>> # 迭代
        >>> for key, value in db.items():
        ...     print(key, value)
    """
    
    def __init__(self, 
                 name: str, 
                 storage_type: str = "local", 
                 path: Optional[str] = None,
                 config: Optional[FlaxKVConfig] = None,
                 timeout: float = 30.0,
                 auto_initialize: bool = True):
        """初始化FlaxDict。
        
        Args:
            name: 数据库名称
            storage_type: 存储类型
            path: 存储路径
            config: FlaxKV配置，默认使用生产环境配置
            timeout: 操作超时时间
            auto_initialize: 是否自动初始化
        """
        self._name = name
        self._storage_type = storage_type
        self._path = path
        self._config = config or FlaxKVConfig.for_production()
        self._timeout = timeout
        
        # 内部ThreadSafeFlaxKV实例
        self._flaxkv = ThreadSafeFlaxKV(
            name=name,
            storage_type=storage_type, 
            path=path,
            config=self._config,
            timeout=timeout
        )
        
        # 控制是否自动初始化
        self._initialized = False
        if auto_initialize:
            self._ensure_initialized()
    
    def _ensure_initialized(self):
        """确保已初始化。"""
        if not self._initialized:
            self._flaxkv._ensure_initialized()
            self._initialized = True
    
    # ===== 核心字典接口 =====
    
    def __getitem__(self, key: str) -> Any:
        """获取键对应的值。
        
        Args:
            key: 键名
            
        Returns:
            键对应的值
            
        Raises:
            KeyError: 键不存在时抛出
        """
        self._ensure_initialized()
        
        # 使用哨兵对象避免None值歧义
        _MISSING = object()
        value = self._flaxkv.get(key, default=_MISSING)
        if value is _MISSING:
            raise KeyError(key)
        
        return value
    
    def __setitem__(self, key: str, value: Any) -> None:
        """设置键值对。
        
        Args:
            key: 键名
            value: 值
        """
        self._ensure_initialized()
        self._flaxkv.set(key, value)
    
    def __delitem__(self, key: str) -> None:
        """删除键。
        
        Args:
            key: 键名
            
        Raises:
            KeyError: 键不存在时抛出
        """
        self._ensure_initialized()
        
        # 尝试删除，如果不存在delete操作会返回False
        success = self._flaxkv.delete(key)
        if not success:
            raise KeyError(key)
    
    def __contains__(self, key: str) -> bool:
        """检查键是否存在。
        
        Args:
            key: 键名
            
        Returns:
            键是否存在
        """
        self._ensure_initialized()
        return self._flaxkv.contains(key)
    
    def __len__(self) -> int:
        """获取字典大小。
        
        Returns:
            键的总数
        """
        self._ensure_initialized()
        return self._flaxkv.size()
    
    def __iter__(self) -> Iterator[str]:
        """迭代所有键。
        
        Returns:
            键的迭代器
        """
        self._ensure_initialized()
        return iter(self._flaxkv.keys())
    
    def __repr__(self) -> str:
        """字符串表示。"""
        if not self._initialized:
            return f"FlaxDict('{self._name}', uninitialized)"
        
        try:
            # 为了避免大字典的性能问题，只显示前几个键值对
            items = list(self.items())
            if len(items) <= 6:
                items_str = ', '.join(f'{k!r}: {v!r}' for k, v in items)
                return f"FlaxDict({{{items_str}}})"
            else:
                first_items = items[:3]
                items_str = ', '.join(f'{k!r}: {v!r}' for k, v in first_items)
                return f"FlaxDict({{{items_str}, ... ({len(items) - 3} more items)}})"
        except Exception:
            return f"FlaxDict('{self._name}', {len(self)} items)"
    
    def __str__(self) -> str:
        """字符串表示。"""
        return self.__repr__()
    
    # ===== 字典方法 =====
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取键的值，如果不存在返回默认值。
        
        Args:
            key: 键名
            default: 默认值
            
        Returns:
            键对应的值，或默认值
        """
        self._ensure_initialized()
        return self._flaxkv.get(key, default)
    
    def pop(self, key: str, *args) -> Any:
        """删除并返回键对应的值。
        
        Args:
            key: 键名
            default: 默认值（可选）
            
        Returns:
            键对应的值
            
        Raises:
            KeyError: 键不存在且未提供默认值时抛出
        """
        self._ensure_initialized()
        
        if len(args) > 1:
            raise TypeError(f"pop() accepts at most 2 arguments ({len(args) + 1} given)")
        
        # 使用contains检查键是否存在
        if not self._flaxkv.contains(key):
            if args:
                return args[0]
            raise KeyError(key)
        
        # 键存在，获取值并删除
        value = self._flaxkv.get(key)
        self._flaxkv.delete(key)
        return value
    
    def popitem(self) -> Tuple[str, Any]:
        """删除并返回一个任意的键值对。
        
        Returns:
            (键, 值) 元组
            
        Raises:
            KeyError: 字典为空时抛出
        """
        self._ensure_initialized()
        
        if len(self) == 0:
            raise KeyError('popitem(): dictionary is empty')
        
        # 获取第一个键
        keys = self._flaxkv.keys()
        if not keys:
            raise KeyError('popitem(): dictionary is empty')
        
        key = keys[0]
        value = self._flaxkv.get(key)
        self._flaxkv.delete(key)
        return (key, value)
    
    def setdefault(self, key: str, default: Any = None) -> Any:
        """如果键不存在，设置并返回默认值；否则返回现有值。
        
        Args:
            key: 键名
            default: 默认值
            
        Returns:
            键对应的值（现有的或新设置的）
        """
        self._ensure_initialized()
        
        if self._flaxkv.contains(key):
            return self._flaxkv.get(key)
        else:
            self._flaxkv.set(key, default)
            return default
    
    def update(self, *args, **kwargs) -> None:
        """使用其他字典或键值对更新字典。
        
        支持以下调用方式：
        - update(other_dict)
        - update(iterable_of_pairs)
        - update(**kwargs)
        - update(other_dict, **kwargs)
        """
        self._ensure_initialized()
        
        # 处理位置参数
        if args:
            if len(args) > 1:
                raise TypeError(f"update() takes at most 1 positional argument ({len(args)} given)")
            
            other = args[0]
            if hasattr(other, 'keys'):
                # 字典式对象
                for key in other.keys():
                    self._flaxkv.set(key, other[key])
            else:
                # 可迭代的键值对
                for key, value in other:
                    self._flaxkv.set(key, value)
        
        # 处理关键字参数
        for key, value in kwargs.items():
            self._flaxkv.set(key, value)
    
    def clear(self) -> None:
        """清空字典。"""
        self._ensure_initialized()
        self._flaxkv.clear()
    
    def copy(self) -> Dict[str, Any]:
        """返回字典的浅拷贝。
        
        Returns:
            包含所有键值对的普通字典
        """
        self._ensure_initialized()
        return dict(self.items())
    
    # ===== 视图方法 =====
    
    def keys(self):
        """返回所有键的迭代器。
        
        Returns:
            键的迭代器
        """
        self._ensure_initialized()
        return iter(self._flaxkv.keys())
    
    def values(self):
        """返回所有值的迭代器。
        
        Returns:
            值的迭代器
        """
        self._ensure_initialized()
        keys = self._flaxkv.keys()
        if not keys:
            return iter([])
        
        # 批量获取所有值以提高性能
        values_dict = self._flaxkv.mget(keys)
        for key in keys:
            if key in values_dict:
                yield values_dict[key]
    
    def items(self):
        """返回所有键值对的迭代器。
        
        Returns:
            键值对的迭代器
        """
        self._ensure_initialized()
        keys = self._flaxkv.keys()
        if not keys:
            return iter([])
        
        # 批量获取所有值以提高性能
        values = self._flaxkv.mget(keys)
        
        # 返回键值对迭代器
        for key in keys:
            if key in values:
                yield (key, values[key])
    
    # ===== 批量操作 =====
    
    def mget(self, keys: list) -> Dict[str, Any]:
        """批量获取多个键的值。
        
        Args:
            keys: 键名列表
            
        Returns:
            键值对字典（只包含存在的键）
        """
        self._ensure_initialized()
        return self._flaxkv.mget(keys)
    
    def mset(self, mapping: Dict[str, Any]) -> None:
        """批量设置多个键值对。
        
        Args:
            mapping: 键值对字典
        """
        self._ensure_initialized()
        self._flaxkv.mset(mapping)
    
    def mdelete(self, keys: list) -> int:
        """批量删除多个键。
        
        Args:
            keys: 键名列表
            
        Returns:
            实际删除的键数量
        """
        self._ensure_initialized()
        return self._flaxkv.mdelete(keys)
    
    # ===== 上下文管理器 =====
    
    def __enter__(self):
        """进入上下文管理器。"""
        self._ensure_initialized()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文管理器。"""
        self.close()
    
    # ===== 管理方法 =====
    
    def close(self) -> None:
        """关闭数据库连接。"""
        if self._initialized:
            self._flaxkv.close()
            self._initialized = False
    
    def flush(self) -> Dict[str, Any]:
        """强制刷新缓冲区。
        
        Returns:
            刷新统计信息
        """
        self._ensure_initialized()
        return self._flaxkv.flush()
    
    def backup(self, backup_path: str) -> Dict[str, Any]:
        """备份数据库。
        
        Args:
            backup_path: 备份文件路径
            
        Returns:
            备份统计信息
        """
        self._ensure_initialized()
        return self._flaxkv.backup(backup_path)
    
    def restore(self, backup_path: str) -> Dict[str, Any]:
        """恢复数据库。
        
        Args:
            backup_path: 备份文件路径
            
        Returns:
            恢复统计信息
        """
        self._ensure_initialized()
        return self._flaxkv.restore(backup_path)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息。
        
        Returns:
            统计信息字典
        """
        self._ensure_initialized()
        return self._flaxkv.get_stats()
    
    # ===== TTL 支持 =====
    
    def setex(self, key: str, value: Any, ttl: int) -> None:
        """设置带TTL的键值对。
        
        Args:
            key: 键名
            value: 值
            ttl: 过期时间（秒）
        """
        self._ensure_initialized()
        self._flaxkv.setex(key, value, ttl)
    
    def expire(self, key: str, ttl: int) -> bool:
        """设置键的过期时间。
        
        Args:
            key: 键名
            ttl: 过期时间（秒）
            
        Returns:
            是否设置成功
        """
        self._ensure_initialized()
        return self._flaxkv.expire(key, ttl)
    
    # ===== 扫描操作 =====
    
    def scan(self, prefix: str = "", limit: Optional[int] = None):
        """扫描数据库中的键值对。
        
        Args:
            prefix: 键前缀过滤
            limit: 返回的最大数量
            
        Returns:
            键值对迭代器
        """
        self._ensure_initialized()
        return self._flaxkv.scan(prefix=prefix, limit=limit)
    
    # ===== 事务支持 =====
    
    def transaction(self, isolation_level: str = "READ_COMMITTED", timeout: Optional[float] = None):
        """创建事务上下文管理器。
        
        Args:
            isolation_level: 事务隔离级别
            timeout: 操作超时时间
            
        Returns:
            事务上下文管理器
        """
        self._ensure_initialized()
        return self._flaxkv.transaction(isolation_level, timeout)


# 便捷创建函数
def create_flax_dict(name: str, 
                    storage_type: str = "local", 
                    path: Optional[str] = None,
                    config: Optional[FlaxKVConfig] = None,
                    timeout: float = 30.0) -> FlaxDict:
    """创建FlaxDict实例的便捷函数。
    
    Args:
        name: 数据库名称
        storage_type: 存储类型
        path: 存储路径
        config: FlaxKV配置
        timeout: 默认操作超时时间
        
    Returns:
        FlaxDict实例
    """
    return FlaxDict(name, storage_type, path, config, timeout)