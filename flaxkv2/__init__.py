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
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict
from flaxkv2.core.cached_leveldb_dict import CachedLevelDBDict
from flaxkv2.core.nested_dict import NestedDBDict

# 注意: LevelDBDict 已弃用，不再从主模块导出
# 如需使用，请直接导入: from flaxkv2.core.leveldb_dict import LevelDBDict
# 但强烈建议迁移到 RawLevelDBDict，性能提升13-25%


class BackendType:
    """后端类型枚举"""
    REMOTE = "remote"
    LOCAL = "local"


class FlaxKV:
    """
    FlaxKV主接口，提供工厂方法创建合适的DB实现

    支持两种后端类型：
    1. LOCAL: 本地LevelDB后端，直接访问本地文件系统（返回 RawLevelDBDict）
    2. REMOTE: 远程ZeroMQ后端，通过网络访问远程FlaxKV服务器（返回 RemoteDBDict）

    推荐用法：
        db = FlaxKV("mydb", "./data")  # 自动创建 RawLevelDBDict
        db = FlaxKV("mydb", "tcp://host:5555")  # 自动创建 RemoteDBDict

    关于后端选择：
    - ✅ RawLevelDBDict: 推荐使用，高性能、低内存占用、代码简洁
    - ⚠️ LevelDBDict: 已弃用，将在未来版本移除
      原因：性能测试显示缓冲和索引机制反而降低了性能
      如果仍需使用，请直接导入：from flaxkv2 import LevelDBDict (会触发警告)
    """
    
    @staticmethod
    def _detect_backend_type(root_path_or_url: str) -> str:
        """
        检测后端类型
        
        Args:
            root_path_or_url: 数据库根路径或远程URL
            
        Returns:
            BackendType.REMOTE 或 BackendType.LOCAL
            
        规则：
            - 如果以 tcp:// 开头，视为远程后端
            - 其他情况视为本地后端
            - 如果需要远程后端，建议显式指定 backend='remote'
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
        auto_nested: bool = False,
        **kwargs
    ):
        """
        创建本地LevelDB后端实例
        
        默认返回 RawLevelDBDict，这是经过性能优化的版本。
        
        Args:
            db_name: 数据库名称
            path: 数据库存储路径
            rebuild: 是否重建数据库
            raw: 是否使用原始模式（不进行序列化）
            default_ttl: 默认TTL，单位为秒
            auto_nested: 是否自动将字典类型转换为嵌套存储
            **kwargs: 其他参数传递给底层实现
            
        Returns:
            RawLevelDBDict实例（高性能简化版本）
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
            url: 远程服务器地址，支持以下格式：
                - "tcp://host:port" (推荐，例如: "tcp://127.0.0.1:5555")
                - "host:port" (例如: "127.0.0.1:5555")
                - "host" (使用默认端口5555)
            default_ttl: 默认TTL，单位为秒（暂不支持）
            root_path: 远程服务器上的数据库根路径（暂不支持）
            timeout: 请求超时时间（毫秒）
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
            **kwargs: 其他参数传递给底层实现
                     注意：如果传递 port 参数，会覆盖 URL 中的端口
            
        Returns:
            RemoteDBDict实例
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
                - 远程URL: "tcp://host:port" (推荐使用 tcp:// 前缀)
            backend: 后端类型，可选值：'local', 'remote'
                - 如果不指定，只有 "tcp://" 前缀会自动识别为远程后端
                - 建议显式指定 backend='remote' 以避免歧义
                - 'local': 使用本地LevelDB后端
                - 'remote': 使用远程ZeroMQ后端
            auto_nested: 是否自动将字典类型转换为嵌套存储（仅本地后端）
            rebuild: 是否重建数据库（仅本地后端）
            raw: 是否使用原始模式，不进行序列化（仅本地后端）
            default_ttl: 默认TTL，单位为秒。设置后，所有新增的键都会自动应用此TTL
            root_path: 显式传递的根路径，主要用于远程连接时指定服务器端的存储路径（暂不支持）
            timeout: 远程连接超时时间（毫秒），默认5000（仅远程后端）
            max_retries: 最大重试次数，默认3（仅远程后端）
            retry_delay: 重试延迟（秒），默认0.1（仅远程后端）
            **kwargs: 其他参数传递给底层实现
            
        Returns:
            根据后端类型返回相应的数据库实例：
            - 本地后端: RawLevelDBDict
            - 远程后端: RemoteDBDict
            
        Examples:
            # 本地后端
            db = FlaxKV("mydb", "./data")
            db = FlaxKV("mydb", "/var/lib/flaxkv", backend='local')
            
            # 远程后端（推荐方式：显式指定 backend='remote'）
            db = FlaxKV("mydb", "127.0.0.1:5555", backend='remote')
            db = FlaxKV("mydb", "localhost", backend='remote', port=5555)
            
            # 远程后端（使用 tcp:// 前缀自动识别）
            db = FlaxKV("mydb", "tcp://127.0.0.1:5555")
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
    "RawLevelDBDict",
    "CachedLevelDBDict",
    "NestedDBDict"
]
# 注意: LevelDBDict 已从导出列表中移除（已弃用）
# 如仍需使用，请直接导入: from flaxkv2.core.leveldb_dict import LevelDBDict 