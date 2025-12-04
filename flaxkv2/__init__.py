"""
FlaxKV2 - 高性能、多功能键值存储库

FlaxKV2 是基于 LevelDB 的 Python 键值存储库，提供类字典接口。

核心特性：
- 🚀 智能缓存：自动检测参数，选择最优后端
- 🎯 简单易用：类字典接口，上手即用
- 🔒 线程安全：内置线程安全支持
- 📦 丰富类型：支持字符串、数字、列表、字典、NumPy 数组、Pandas DataFrame
- 🌐 远程访问：基于 ZeroMQ 的客户端/服务器架构
- ⏰ TTL 支持：键自动过期功能
- 🪆 嵌套存储：高效的嵌套字典/列表存储（默认启用）

快速开始：
    >>> from flaxkv2 import FlaxKV
    >>>
    >>> # 无缓存（默认，简单可靠）
    >>> db = FlaxKV("mydb", "./data")
    >>> db["key"] = "value"
    >>>
    >>> # 只读缓存（提升读性能 ~10x）
    >>> db = FlaxKV("mydb", "./data", read_cache_size=5000)
    >>>
    >>> # 极致性能（读缓存 + 写缓冲，自动启用异步flush）
    >>> db = FlaxKV("mydb", "./data",
    ...            read_cache_size=10000,
    ...            write_buffer_size=500)
"""

from typing import Optional
from flaxkv2.utils.log import get_logger

__version__ = '0.1.0'

logger = get_logger(__name__)

# 导入自动关闭模块，确保它被初始化
from flaxkv2 import auto_close

# 导入核心类
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict
from flaxkv2.core.cached_leveldb_dict import CachedLevelDBDict
from flaxkv2.core.nested_structures import NestedDBDict, NestedDBList


class BackendType:
    """
    后端类型枚举

    Attributes:
        REMOTE: 远程 ZeroMQ 后端
        LOCAL: 本地 LevelDB 后端
    """
    REMOTE = "remote"
    LOCAL = "local"


class FlaxKV:
    """
    FlaxKV 主接口 - 智能工厂类

    FlaxKV 使用工厂模式，根据参数智能选择最优的后端实现：

    后端选择逻辑：
    ┌─────────────────────────────────────────────────────┐
    │ 1. 检测 URL 类型                                     │
    │    • tcp://... → RemoteDBDict (远程后端)            │
    │    • 其他 → 本地后端                                 │
    │                                                      │
    │ 2. 检测缓存参数（本地后端）                         │
    │    • 无 read_cache_size/write_buffer_size           │
    │      → RawLevelDBDict (无缓存)                      │
    │    • 有缓存参数                                      │
    │      → CachedLevelDBDict (智能缓存)                 │
    └─────────────────────────────────────────────────────┘

    三种后端实现：

    1. RawLevelDBDict（无缓存，默认）
       • 简单可靠，数据立即持久化
       • 性能优秀，适合小数据量
       • 对数据一致性要求高的场景

    2. CachedLevelDBDict（智能缓存）
       • 读缓存：热数据读取 ~10-13x 性能提升
       • 写缓冲：批量写入优化
       • 异步/同步 flush 可选
       • 适合高性能场景

    3. RemoteDBDict（远程访问）
       • 基于 ZeroMQ 的客户端/服务器架构
       • 支持多进程/多机器共享数据库
       • 适合分布式场景

    使用示例：
        >>> from flaxkv2 import FlaxKV
        >>>
        >>> # 1. 无缓存（默认，简单可靠）
        >>> db = FlaxKV("mydb", "./data")
        >>> db["key"] = "value"
        >>> db.close()
        >>>
        >>> # 2. 只读缓存（提升读性能）
        >>> db = FlaxKV("mydb", "./data", read_cache_size=5000)
        >>> value = db["key"]  # 热数据读取更快
        >>> db.close()
        >>>
        >>> # 3. 读缓存 + 写缓冲（极致性能，默认异步）
        >>> db = FlaxKV("mydb", "./data",
        ...            read_cache_size=10000,
        ...            write_buffer_size=500)
        >>> for i in range(10000):
        ...     db[f"key_{i}"] = f"value_{i}"
        >>> db.close()  # 确保刷新数据
        >>>
        >>> # 4. 同步 flush（更安全）
        >>> db = FlaxKV("mydb", "./data",
        ...            read_cache_size=5000,
        ...            write_buffer_size=100,
        ...            async_flush=False)
        >>> db["key"] = "value"
        >>> db.close()
        >>>
        >>> # 5. 远程后端
        >>> db = FlaxKV("mydb", "tcp://127.0.0.1:5555")
        >>> db["key"] = "value"
        >>> db.close()

    性能特征：
        配置                    热数据读取      写入性能
        ──────────────────────────────────────────
        无缓存（默认）          107K ops/s      1649 ops/s
        只读缓存                1064K ops/s     1367 ops/s  (9.9x 读)
        同步写缓冲              926K ops/s      848 ops/s   (8.6x 读)
        异步写缓冲（默认）      1434K ops/s     846 ops/s   (13.4x 读)

    最佳实践：
        1. 始终使用上下文管理器或确保调用 close()
        2. 启用缓存后必须正常关闭以刷新数据
        3. 生产环境建议使用 async_flush=False（更安全）
        4. 开发环境可使用默认配置（极致性能）

    注意事项：
        • 异步 flush 性能最高，但进程崩溃可能丢失缓冲区数据
        • 正常关闭不会丢失数据
        • 使用 with 语句可自动确保正常关闭
    """

    @staticmethod
    def _detect_backend_type(root_path_or_url: str) -> str:
        """
        自动检测后端类型

        通过分析 root_path_or_url 参数，自动判断应该使用本地后端还是远程后端。

        Args:
            root_path_or_url: 数据库根路径或远程 URL

        Returns:
            str: BackendType.REMOTE 或 BackendType.LOCAL

        检测规则：
            • 以 "tcp://" 开头 → REMOTE
            • 其他 → LOCAL

        示例：
            >>> FlaxKV._detect_backend_type("./data")
            'local'
            >>> FlaxKV._detect_backend_type("tcp://127.0.0.1:5555")
            'remote'
        """
        if root_path_or_url.startswith("tcp://"):
            return BackendType.REMOTE
        return BackendType.LOCAL

    @staticmethod
    def _create_local_backend(
        db_name: str,
        path: str,
        rebuild: bool = False,
        raw: bool = False,
        default_ttl: Optional[int] = None,
        auto_nested: bool = True,
        **kwargs
    ):
        """
        创建本地 LevelDB 后端实例（智能自动检测）

        根据传入的缓存参数，智能选择使用 RawLevelDBDict 或 CachedLevelDBDict。

        智能检测逻辑：
            1. 检查 kwargs 中是否包含 read_cache_size 或 write_buffer_size
            2. 如果有任一缓存参数 → 使用 CachedLevelDBDict
            3. 如果无缓存参数 → 使用 RawLevelDBDict

        Args:
            db_name: 数据库名称
            path: 数据库存储路径
            rebuild: 是否重建数据库（删除已有数据）
            raw: 是否使用原始模式（不进行序列化，存储原始字节）
            default_ttl: 默认 TTL（秒），所有新增键都会应用此 TTL
            auto_nested: 是否自动转换字典/列表为嵌套存储（默认 True，推荐）

            **kwargs: 缓存和性能参数
                # 缓存参数（传递后自动启用 CachedLevelDBDict）
                read_cache_size: 读缓存大小（条目数）
                    - 设置后自动启用缓存
                    - 启用缓存后默认值：10000
                    - 提升热数据读取性能 ~10-13x

                write_buffer_size: 写缓冲大小（条目数）
                    - 设置后自动启用写缓冲
                    - 启用缓存后默认值：500
                    - 批量写入优化，减少 I/O

                write_buffer_flush_interval: 写缓冲刷新间隔（秒）
                    - 默认：30 秒
                    - 定时刷新未达到阈值的数据

                async_flush: 是否使用异步 flush
                    - 默认：True（极致性能）
                    - False：同步 flush（更安全）

                # TTL 清理参数
                enable_ttl_cleanup: 是否启用 TTL 自动清理（默认 True）
                cleanup_interval: TTL 清理间隔（秒，默认 60）
                cleanup_batch_size: 每次清理扫描的键数量（默认 1000）

                # LevelDB 性能参数
                performance_profile: 性能配置文件
                    - 'balanced' (默认): 通用平衡配置
                    - 'read_optimized': 读密集型优化
                    - 'write_optimized': 写密集型优化
                    - 'memory_constrained': 内存受限配置
                    - 'large_database': 大数据库配置 (>100GB)
                    - 'ml_workload': 机器学习/科学计算配置

                lru_cache_size: LevelDB LRU 缓存大小（字节）
                bloom_filter_bits: 布隆过滤器位数
                block_size: 数据块大小（字节）
                max_open_files: 最大打开文件数
                compression: 压缩算法 ('snappy', 'zlib', None)

        Returns:
            RawLevelDBDict 或 CachedLevelDBDict 实例

        示例：
            >>> # 无缓存
            >>> db = FlaxKV._create_local_backend("mydb", "./data")
            >>> type(db).__name__
            'RawLevelDBDict'
            >>>
            >>> # 启用读缓存
            >>> db = FlaxKV._create_local_backend("mydb", "./data",
            ...                                   read_cache_size=5000)
            >>> type(db).__name__
            'CachedLevelDBDict'
        """
        # 智能检测：是否需要使用缓存后端
        has_cache_params = (
            'read_cache_size' in kwargs or
            'write_buffer_size' in kwargs
        )

        if has_cache_params:
            # 使用缓存后端（CachedLevelDBDict）
            # 默认采用极致性能配置
            cache_params = {
                'read_cache_size': 10000,           # 大读缓存
                'write_buffer_size': 500,           # 大写缓冲
                'write_buffer_flush_interval': 30,  # 30秒刷新
                'async_flush': True,                # 异步flush（极致性能）
            }
            # 用户自定义参数覆盖默认值
            cache_params.update(kwargs)

            logger.debug(
                f"智能选择缓存后端: db_name={db_name}, path={path}, "
                f"read_cache={cache_params.get('read_cache_size')}, "
                f"write_buffer={cache_params.get('write_buffer_size')}, "
                f"async_flush={cache_params.get('async_flush')}"
            )

            return CachedLevelDBDict(
                name=db_name,
                path=path,
                rebuild=rebuild,
                raw=raw,
                default_ttl=default_ttl,
                auto_nested=auto_nested,
                enable_write_buffer=True,
                **cache_params
            )
        else:
            # 使用无缓存后端（RawLevelDBDict）
            logger.debug(f"智能选择无缓存后端: db_name={db_name}, path={path}")
            return RawLevelDBDict(
                name=db_name,
                path=path,
                rebuild=rebuild,
                raw=raw,
                default_ttl=default_ttl,
                auto_nested=auto_nested,
                **kwargs
            )

    @staticmethod
    def _create_remote_backend(
        db_name: str,
        url: str,
        default_ttl: Optional[int] = None,
        root_path: Optional[str] = None,
        timeout: int = 5000,
        max_retries: int = 3,
        retry_delay: float = 0.1,
        enable_encryption: bool = False,
        password: Optional[str] = None,
        server_public_key: Optional[str] = None,
        derive_from_password: bool = True,
        enable_compression: bool = False,
        **kwargs
    ):
        """
        创建远程 ZeroMQ 后端实例

        Args:
            db_name: 数据库名称
            url: 远程服务器地址，支持多种格式：
                • "tcp://host:port" (推荐，例如: "tcp://127.0.0.1:5555")
                • "host:port" (例如: "127.0.0.1:5555")
                • "host" (使用默认端口 5555)

            default_ttl: 默认 TTL，单位为秒（远程后端暂不支持）
            root_path: 远程服务器上的数据库根路径（暂不支持）

            timeout: 请求超时时间（毫秒，默认 5000）
            max_retries: 最大重试次数（默认 3）
            retry_delay: 重试延迟（秒，默认 0.1）

            enable_encryption: 是否启用 CurveZMQ 加密（默认 False）
            password: 服务器密码（与 server_public_key 二选一）
                • 使用密码时会自动管理密钥对
                • 服务器端必须使用相同的密码
            server_public_key: 服务器公钥（Z85 编码，与 password 二选一）
                • 从服务器日志中获取
                • 适用于公钥分发场景
            derive_from_password: 是否从密码直接派生密钥（默认 True，推荐）
                • True: 确定性派生，相同密码总是生成相同密钥
                • False: 使用文件存储密钥
            enable_compression: 是否启用 LZ4 压缩（默认 False）

            **kwargs: 其他参数传递给底层实现
                • port: 端口号（会覆盖 URL 中的端口）
                • read_cache_size: 客户端读缓存大小
                • enable_write_buffer: 是否启用写缓冲

        Returns:
            RemoteDBDict: 远程数据库客户端实例

        示例：
            >>> # 不加密连接
            >>> db = FlaxKV._create_remote_backend("mydb", "tcp://127.0.0.1:5555")

            >>> # 使用密码加密
            >>> db = FlaxKV._create_remote_backend(
            ...     "mydb", "tcp://127.0.0.1:5555",
            ...     enable_encryption=True,
            ...     password="mypassword"
            ... )

            >>> # 使用公钥加密
            >>> db = FlaxKV._create_remote_backend(
            ...     "mydb", "tcp://127.0.0.1:5555",
            ...     enable_encryption=True,
            ...     server_public_key="<从服务器日志复制的公钥>"
            ... )

        注意：
            • 如果 kwargs 中传递了 port 参数，会覆盖 URL 中解析的端口
            • 启用加密时，必须提供 password 或 server_public_key
            • password 和 server_public_key 不能同时指定
        """
        from flaxkv2.client.zmq_client import RemoteDBDict

        # 如果 kwargs 中有 port，优先使用它
        port_from_kwargs = kwargs.pop('port', None)

        # 移除 tcp:// 前缀（如果有）
        clean_url = url
        if clean_url.startswith("tcp://"):
            clean_url = clean_url[6:]  # len("tcp://") = 6

        # 解析 host:port
        if ':' in clean_url:
            host, port_str = clean_url.rsplit(':', 1)
            try:
                port = int(port_str)
            except ValueError:
                # 如果端口不是数字，使用默认端口
                host = clean_url
                port = 5555
        else:
            host = clean_url
            port = 5555  # 默认端口

        # 如果 kwargs 中指定了 port，覆盖从 URL 解析的 port
        if port_from_kwargs is not None:
            port = port_from_kwargs

        logger.debug(f"创建远程 ZeroMQ 后端: db_name={db_name}, host={host}, port={port}")
        return RemoteDBDict(
            db_name=db_name,
            host=host,
            port=port,
            timeout=timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
            enable_encryption=enable_encryption,
            password=password,
            server_public_key=server_public_key,
            derive_from_password=derive_from_password,
            enable_compression=enable_compression,
            **kwargs
        )

    def __new__(
        cls,
        db_name: str,
        root_path_or_url: str = ".",
        backend: Optional[str] = None,
        auto_nested: bool = True,
        rebuild: bool = False,
        raw: bool = False,
        default_ttl: Optional[int] = None,
        root_path: Optional[str] = None,
        **kwargs
    ):
        """
        创建 FlaxKV 实例（工厂方法）

        FlaxKV 使用智能工厂模式，根据参数自动选择最优的后端实现：

        自动选择规则：
        ┌─────────────────────────────────────────────────────┐
        │ 1. 检测 backend 类型                                 │
        │    • root_path_or_url 以 "tcp://" 开头 → REMOTE   │
        │    • 其他 → LOCAL                                   │
        │                                                      │
        │ 2. 本地后端缓存检测                                  │
        │    • 无缓存参数 → RawLevelDBDict                    │
        │    • 有 read_cache_size/write_buffer_size           │
        │      → CachedLevelDBDict                            │
        └─────────────────────────────────────────────────────┘

        Args:
            db_name: 数据库名称
                • 用于标识数据库
                • 本地后端会在 root_path_or_url 下创建此名称的目录

            root_path_or_url: 数据库根路径或远程 URL（默认 "."）
                • 本地路径: ".", "./data", "/var/lib/flaxkv" 等
                • 远程 URL: "tcp://host:port" (推荐使用 tcp:// 前缀)

            backend: 显式指定后端类型（可选）
                • 'local': 强制使用本地后端
                • 'remote': 强制使用远程后端
                • None: 自动检测（推荐）

            auto_nested: 自动嵌套存储（默认 True，推荐）
                • True: 字典/列表自动转换为 NestedDBDict/NestedDBList
                • False: 使用普通序列化（完整序列化整个对象）
                • 嵌套存储可显著提升嵌套数据的修改性能

            rebuild: 重建数据库（默认 False，仅本地后端）
                • True: 删除已有数据，创建新数据库
                • False: 使用已有数据库或创建新数据库

            raw: 原始模式（默认 False，仅本地后端）
                • True: 不进行序列化，直接存储字节
                • False: 自动序列化 Python 对象

            default_ttl: 默认 TTL（秒，可选）
                • 设置后，所有新增的键都会自动应用此 TTL
                • None: 不使用 TTL

            root_path: 显式传递的根路径（暂不支持）

            **kwargs: 缓存和性能参数
                ═══════════════════════════════════════════════
                缓存参数（传递后自动启用 CachedLevelDBDict）
                ═══════════════════════════════════════════════

                read_cache_size: int, optional
                    读缓存大小（条目数）
                    • 启用缓存后默认：10000
                    • 提升热数据读取性能 ~10-13x
                    • 设置后自动使用 CachedLevelDBDict

                write_buffer_size: int, optional
                    写缓冲大小（条目数）
                    • 启用缓存后默认：500
                    • 达到此值时触发 flush
                    • 批量写入优化，减少 I/O

                write_buffer_flush_interval: int, optional
                    写缓冲刷新间隔（秒）
                    • 默认：30
                    • 定时刷新未达到阈值的数据

                async_flush: bool, optional
                    是否使用异步 flush
                    • 默认：True（极致性能）
                    • True: 异步 flush，不阻塞操作（更快但有风险）
                    • False: 同步 flush（更安全）
                    • ⚠️ 异步模式下进程崩溃可能丢失缓冲区数据

                ═══════════════════════════════════════════════
                TTL 清理参数
                ═══════════════════════════════════════════════

                enable_ttl_cleanup: bool, optional
                    是否启用 TTL 自动清理（默认 True）

                cleanup_interval: int, optional
                    TTL 清理间隔（秒，默认 60）

                cleanup_batch_size: int, optional
                    每次清理扫描的键数量（默认 1000）

                ═══════════════════════════════════════════════
                LevelDB 性能参数
                ═══════════════════════════════════════════════

                performance_profile: str, optional
                    性能配置文件（默认 'balanced'）
                    • 'balanced': 通用平衡配置
                    • 'read_optimized': 读密集型优化（512MB 缓存）
                    • 'write_optimized': 写密集型优化（256MB 写缓冲）
                    • 'memory_constrained': 内存受限（64MB 缓存）
                    • 'large_database': 大数据库 (>100GB，1GB 缓存)
                    • 'ml_workload': ML 工作负载（512MB 缓存+写缓冲）

                lru_cache_size: int, optional
                    LevelDB LRU 缓存大小（字节）

                bloom_filter_bits: int, optional
                    布隆过滤器位数

                block_size: int, optional
                    数据块大小（字节）

                max_open_files: int, optional
                    最大打开文件数

                compression: str, optional
                    压缩算法 ('snappy', 'zlib', None)

                ═══════════════════════════════════════════════
                远程连接参数（仅远程后端）
                ═══════════════════════════════════════════════

                timeout: int, optional
                    远程连接超时时间（毫秒，默认 5000）

                max_retries: int, optional
                    最大重试次数（默认 3）

                retry_delay: float, optional
                    重试延迟（秒，默认 0.1）

                enable_encryption: bool, optional
                    启用 CurveZMQ 加密（默认 False）
                    • 必须与服务器配置一致

                password: str, optional
                    服务器密码（与 server_public_key 二选一）
                    • 服务器端必须使用相同密码
                    • 自动管理密钥对

                server_public_key: str, optional
                    服务器公钥（Z85 编码，与 password 二选一）
                    • 从服务器日志中获取
                    • 适用于公钥分发场景

                derive_from_password: bool, optional
                    从密码派生密钥（默认 True，推荐）
                    • True: 确定性派生
                    • False: 文件存储

                enable_compression: bool, optional
                    启用 LZ4 压缩（默认 False）

        Returns:
            根据参数智能选择后端类型：
            • 无缓存参数 → RawLevelDBDict（简单、可靠）
            • 有缓存参数 → CachedLevelDBDict（极致性能）
            • 远程 URL → RemoteDBDict（分布式访问）

        Examples:
            基本用法：
            --------
            >>> from flaxkv2 import FlaxKV
            >>>
            >>> # 1. 无缓存（默认，简单可靠）
            >>> db = FlaxKV("mydb", "./data")
            >>> db["key"] = "value"
            >>> print(db["key"])
            'value'
            >>> db.close()

            只读缓存：
            --------
            >>> # 2. 只读缓存（提升读性能）
            >>> db = FlaxKV("mydb", "./data", read_cache_size=5000)
            >>> # 热数据读取性能提升 ~10x
            >>> value = db["key"]
            >>> db.close()

            极致性能：
            --------
            >>> # 3. 读缓存 + 异步写缓冲（极致性能）
            >>> db = FlaxKV("mydb", "./data",
            ...            read_cache_size=10000,
            ...            write_buffer_size=500)
            >>> # 默认 async_flush=True
            >>> for i in range(10000):
            ...     db[f"key_{i}"] = f"value_{i}"
            >>> db.close()  # ⚠️ 必须关闭以刷新缓冲区

            同步模式：
            --------
            >>> # 4. 同步 flush（更安全）
            >>> db = FlaxKV("mydb", "./data",
            ...            read_cache_size=5000,
            ...            write_buffer_size=100,
            ...            async_flush=False)
            >>> db["key"] = "value"
            >>> db.close()

            使用上下文管理器：
            ----------------
            >>> # 5. 推荐：使用 with 语句（自动关闭）
            >>> with FlaxKV("mydb", "./data", read_cache_size=5000) as db:
            ...     db["key"] = "value"
            ...     print(db["key"])
            'value'
            # 自动调用 close()

            远程访问：
            --------
            >>> # 6. 远程后端
            >>> db = FlaxKV("mydb", "tcp://127.0.0.1:5555")
            >>> db["key"] = "value"
            >>> db.close()
            >>>
            >>> # 或显式指定 backend
            >>> db = FlaxKV("mydb", "127.0.0.1:5555", backend='remote')
            >>> db.close()

            自定义配置：
            ----------
            >>> # 7. 完全自定义
            >>> db = FlaxKV("mydb", "./data",
            ...            read_cache_size=5000,
            ...            write_buffer_size=200,
            ...            write_buffer_flush_interval=60,
            ...            async_flush=False,
            ...            performance_profile='read_optimized',
            ...            default_ttl=3600,
            ...            auto_nested=True)
            >>> db.close()

        性能参考：
            配置                    热数据读取      写入性能
            ──────────────────────────────────────────
            无缓存（默认）          107K ops/s      1649 ops/s
            只读缓存                1064K ops/s     1367 ops/s  (9.9x)
            同步写缓冲              926K ops/s      848 ops/s   (8.6x)
            异步写缓冲（默认）      1434K ops/s     846 ops/s   (13.4x)

        注意事项：
            ⚠️ 启用缓存后必须确保正常关闭：
                • 使用 with 语句（推荐）
                • 或手动调用 close()
                • 否则缓冲区数据可能丢失

            ⚠️ async_flush=True 的风险：
                • 性能最高，但进程异常崩溃可能丢失缓冲区数据
                • 正常关闭不会丢失数据
                • 生产环境建议使用 async_flush=False

            ✅ 最佳实践：
                • 始终使用 with 语句或确保调用 close()
                • 生产环境使用 async_flush=False
                • 开发环境可使用默认配置（极致性能）

        Raises:
            ValueError: 如果 backend 参数不是 'local' 或 'remote'

        See Also:
            RawLevelDBDict: 无缓存后端实现
            CachedLevelDBDict: 缓存后端实现
            RemoteDBDict: 远程后端实现
        """
        # 自动检测或验证后端类型
        detected_backend = cls._detect_backend_type(root_path_or_url)

        if backend is not None:
            # 如果显式指定了 backend，验证是否与检测结果一致
            backend = backend.lower()
            if backend not in (BackendType.LOCAL, BackendType.REMOTE):
                raise ValueError(
                    f"不支持的后端类型: {backend}. "
                    f"支持的类型: {BackendType.LOCAL}, {BackendType.REMOTE}"
                )

            # 验证一致性
            if backend != detected_backend:
                logger.warning(
                    f"指定的后端类型 '{backend}' 与检测到的类型 '{detected_backend}' 不一致。"
                    f"将使用指定的类型 '{backend}'"
                )
                detected_backend = backend

        # 根据后端类型创建相应的实例
        if detected_backend == BackendType.REMOTE:
            return cls._create_remote_backend(
                db_name=db_name,
                url=root_path_or_url,
                default_ttl=default_ttl,
                root_path=root_path,
                **kwargs
            )
        else:  # BackendType.LOCAL
            return cls._create_local_backend(
                db_name=db_name,
                path=root_path_or_url,
                rebuild=rebuild,
                raw=raw,
                default_ttl=default_ttl,
                auto_nested=auto_nested,
                **kwargs
            )


__all__ = [
    "FlaxKV",
    "BackendType",
    "RawLevelDBDict",
    "CachedLevelDBDict",
    "NestedDBDict",
    "NestedDBList"
]
