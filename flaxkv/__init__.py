"""
FlaxKV 2.0 - 高性能持久化字典

新一代企业级持久化字典，提供原生Python字典接口，
具备LevelDB存储、智能缓存、事务支持等高级特性。

基本用法：
    >>> import flaxkv
    >>> 
    >>> # 创建本地数据库
    >>> async with flaxkv.FlaxKV("mydb") as db:
    ...     await db.set("key", "value")
    ...     value = await db.get("key")
    ...     print(value)
    
    >>> # 使用预设配置
    >>> config = flaxkv.FlaxKVConfig.for_production()
    >>> async with flaxkv.FlaxKV("mydb", config=config) as db:
    ...     await db.set("user:123", {"name": "Alice", "age": 25})

事务用法：
    >>> async with db.transaction() as tx:
    ...     await tx.set("account:1", 100)
    ...     await tx.set("account:2", 200)
    ...     # 自动提交或回滚
"""

from .core.config import FlaxKVConfig, parse_size
from .core.flaxkv import FlaxKV
from .core.factory import TransactionalFlaxKV, create_flaxkv, create_flaxkv_sync
from .core.sync_wrapper import ThreadSafeFlaxKV, create_sync_flaxkv
from .core.flax_dict import FlaxDict, create_flax_dict
from .transaction.transaction import IsolationLevel, TransactionState

# 版本信息
__version__ = "2.0.0"
__author__ = "FlaxKV Team"
__email__ = "team@flaxkv.org"

# 导出的主要接口
__all__ = [
    # 核心类
    "FlaxKV",
    "FlaxKVConfig", 
    "TransactionalFlaxKV",
    "ThreadSafeFlaxKV",
    "FlaxDict",
    
    # 工厂函数
    "create_flaxkv",
    "create_sync_flaxkv",
    "create_flax_dict",
    
    # 枚举和常量
    "IsolationLevel",
    "TransactionState",
    
    # 工具函数
    "parse_size",
    
    # 版本信息
    "__version__",
    "__author__", 
    "__email__",
]

# 快捷创建函数
def open(name: str, **kwargs):
    """同步创建FlaxKV实例（不推荐）。
    
    注意：这是阻塞调用，建议使用异步的 create_flaxkv()
    
    Args:
        name: 数据库名称
        **kwargs: 其他参数传递给FlaxKV构造函数
        
    Returns:
        FlaxKV实例（未初始化）
        
    Example:
        >>> db = flaxkv.open("mydb")
        >>> # 需要手动初始化
        >>> await db.initialize()
    """
    import warnings
    warnings.warn(
        "flaxkv.open() 是同步函数，建议使用异步的 create_flaxkv()",
        FutureWarning,
        stacklevel=2
    )
    return FlaxKV(name, **kwargs)


# 预设配置快捷方式
def dev_config():
    """开发环境配置快捷方式。"""
    return FlaxKVConfig.for_development()


def prod_config():
    """生产环境配置快捷方式。"""
    return FlaxKVConfig.for_production()


def perf_config():
    """高性能配置快捷方式。"""
    return FlaxKVConfig.for_high_performance()


def test_config():
    """测试环境配置快捷方式。"""
    return FlaxKVConfig.for_testing()


# 添加便捷访问
__all__.extend([
    "open",
    "dev_config",
    "prod_config", 
    "perf_config",
    "test_config",
])


# 模块级文档
__doc__ += f"""

版本: {__version__}
作者: {__author__}

主要特性:
- 🚀 高性能: LevelDB存储 + 智能缓存 + 异步I/O
- 💾 持久化: 数据安全存储，支持备份恢复
- 🔒 事务支持: ACID事务，多种隔离级别
- 📊 监控完备: 详细的性能指标和统计信息
- 🛠 易于使用: 原生Python字典接口
- 🔧 高度可配置: 灵活的配置选项

配置预设:
- dev_config(): 开发环境 - 快速启动，详细日志
- prod_config(): 生产环境 - 高性能，完整监控
- perf_config(): 极致性能 - 最大吞吐量优化
- test_config(): 测试环境 - 快速，确定性

支持的操作:
- 基本操作: get, set, delete, contains, size
- 批量操作: mget, mset, mdelete
- 查询操作: scan, keys, values, items
- TTL操作: setex, expire
- 事务操作: transaction, begin_transaction
- 管理操作: flush, clear, backup, restore

同步封装接口:
- ThreadSafeFlaxKV: 高性能线程安全同步封装
- FlaxDict: 完全兼容Python dict接口的持久化字典
- create_sync_flaxkv(): 便捷创建函数
- create_flax_dict(): 便捷字典创建函数
- 适用于同步应用框架集成
"""