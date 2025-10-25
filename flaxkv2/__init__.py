"""
FlaxKV2 - 高性能、多功能键值存储
"""

from typing import Optional
from flaxkv2.utils.log import get_logger

__version__ = '0.1.0'

logger = get_logger(__name__)

# 导入自动关闭模块，确保它被初始化
from flaxkv2 import auto_close

# 导入核心类
from flaxkv2.core.leveldb_dict import LevelDBDict
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict
from flaxkv2.core.nested_dict import NestedDBDict


class BackendType:
    """后端类型枚举"""
    REMOTE = "remote"
    LOCAL = "local"


class FlaxKV:
    """
    FlaxKV主接口，提供工厂方法创建合适的DB实现
    
    支持两种后端类型：
    1. LOCAL: 本地LevelDB后端，直接访问本地文件系统
    2. REMOTE: 远程HTTP后端，通过网络访问远程FlaxKV服务器
    """
    
    @staticmethod
    def _detect_backend_type(root_path_or_url: str) -> str:
        """
        检测后端类型
        
        Args:
            root_path_or_url: 数据库根路径或远程URL
            
        Returns:
            BackendType.REMOTE 或 BackendType.LOCAL
        """
        if root_path_or_url.startswith(("http://", "https://")):
            return BackendType.REMOTE
        return BackendType.LOCAL
    
    @staticmethod
    def _create_local_backend(
        db_name: str,
        path: str,
        rebuild: bool = False,
        raw: bool = False,
        default_ttl: Optional[int] = None,
        auto_nested: bool = False,
        **kwargs
    ):
        """
        创建本地LevelDB后端实例
        
        Args:
            db_name: 数据库名称
            path: 数据库存储路径
            rebuild: 是否重建数据库
            raw: 是否使用原始模式（不进行序列化）
            default_ttl: 默认TTL，单位为秒
            auto_nested: 是否自动将字典类型转换为嵌套存储
            **kwargs: 其他参数传递给底层实现
            
        Returns:
            RawLevelDBDict实例
        """
        logger.debug(f"创建本地后端: db_name={db_name}, path={path}")
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
        **kwargs
    ):
        """
        创建远程 ZeroMQ 后端实例
        
        Args:
            db_name: 数据库名称
            url: 远程服务器地址，格式: "host:port" 或 "host" (默认端口5555)
            default_ttl: 默认TTL，单位为秒（暂不支持）
            root_path: 远程服务器上的数据库根路径（暂不支持）
            timeout: 请求超时时间（毫秒）
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
            **kwargs: 其他参数传递给底层实现
            
        Returns:
            RemoteDBDict实例
        """
        from flaxkv2.client.zmq_client import RemoteDBDict
        
        # 解析 host:port
        if ':' in url:
            host, port_str = url.rsplit(':', 1)
            port = int(port_str)
        else:
            host = url
            port = 5555  # 默认端口
        
        logger.debug(f"创建远程 ZeroMQ 后端: db_name={db_name}, host={host}, port={port}")
        return RemoteDBDict(
            db_name=db_name,
            host=host,
            port=port,
            timeout=timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
            **kwargs
        )
    
    def __new__(
        cls,
        db_name: str,
        root_path_or_url: str = ".",
        backend: Optional[str] = None,
        auto_nested: bool = False,
        rebuild: bool = False,
        raw: bool = False,
        default_ttl: Optional[int] = None,
        root_path: Optional[str] = None,
        **kwargs
    ):
        """
        创建FlaxKV实例（工厂方法）

        Args:
            db_name: 数据库名称
            root_path_or_url: 数据库根路径或远程URL
                - 本地路径: ".", "./data", "/var/lib/flaxkv" 等
                - 远程URL: "http://localhost:8000", "https://api.example.com" 等
            backend: 后端类型，可选值：'local', 'remote'
                - 如果不指定，将根据 root_path_or_url 自动检测
                - 'local': 使用本地LevelDB后端
                - 'remote': 使用远程HTTP后端
            auto_nested: 是否自动将字典类型转换为嵌套存储（仅本地后端）
            rebuild: 是否重建数据库（仅本地后端）
            raw: 是否使用原始模式，不进行序列化（仅本地后端）
            default_ttl: 默认TTL，单位为秒。设置后，所有新增的键都会自动应用此TTL
            root_path: 显式传递的根路径，主要用于远程连接时指定服务器端的存储路径
            **kwargs: 其他参数传递给底层实现
            
        Returns:
            根据后端类型返回相应的数据库实例：
            - 本地后端: RawLevelDBDict
            - 远程后端: RemoteDBDict
            
        Examples:
            # 本地后端
            db = FlaxKV("mydb", "./data")
            db = FlaxKV("mydb", "/var/lib/flaxkv", backend='local')
            
            # 远程后端
            db = FlaxKV("mydb", "http://localhost:8000")
            db = FlaxKV("mydb", "https://api.example.com", backend='remote', root_path="/data")
        """
        # 自动检测或验证后端类型
        detected_backend = cls._detect_backend_type(root_path_or_url)
        
        if backend is not None:
            # 如果显式指定了backend，验证是否与检测结果一致
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
    "LevelDBDict",
    "RawLevelDBDict",
    "NestedDBDict"
] 