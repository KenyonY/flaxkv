"""
FlaxKV 2.0 核心配置管理系统

统一管理所有配置项，支持开发和生产环境预设配置。
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Union
from pathlib import Path


def parse_size(size_str: str) -> int:
    """解析大小字符串为字节数。
    
    Args:
        size_str: 大小字符串，如 '100MB', '1GB', '512KB'
        
    Returns:
        字节数
        
    Examples:
        >>> parse_size('100MB')
        104857600
        >>> parse_size('1GB') 
        1073741824
    """
    size_str = size_str.upper().strip()
    
    if size_str.endswith('B'):
        size_str = size_str[:-1]
    
    if size_str.endswith('K'):
        return int(size_str[:-1]) * 1024
    elif size_str.endswith('M'):
        return int(size_str[:-1]) * 1024 * 1024
    elif size_str.endswith('G'):
        return int(size_str[:-1]) * 1024 * 1024 * 1024
    else:
        return int(size_str)


@dataclass
class FlaxKVConfig:
    """FlaxKV 统一配置管理类。
    
    涵盖性能、持久化、可靠性、网络、监控等各个方面的配置选项。
    提供开发和生产环境的预设配置。
    """
    
    # === 性能配置 ===
    buffer_size: int = 1000
    """缓冲区大小，控制写入缓冲的键值对数量"""
    
    buffer_timeout: float = 5.0
    """缓冲超时时间(秒)，超时后强制写入存储"""
    
    cache_size: str = "100MB"
    """缓存大小，支持KB/MB/GB单位"""
    
    cache_policy: str = "lru"
    """缓存策略: lru/lfu/arc"""
    
    cache_cleanup_interval: float = 300.0
    """缓存清理间隔(秒)，用于清理过期的TTL条目"""
    
    # === 持久化配置 ===
    sync_mode: str = "async"
    """同步模式: async/sync/manual"""
    
    compression: bool = True
    """是否启用数据压缩"""
    
    encryption_key: Optional[str] = None
    """数据加密密钥，为None时不加密"""
    
    encryption_algorithm: str = "AES-256-GCM"
    """加密算法"""
    
    # === 可靠性配置 ===
    backup_interval: int = 3600
    """自动备份间隔(秒)"""
    
    integrity_check: bool = True
    """是否启用数据完整性检查"""
    
    auto_repair: bool = True
    """是否启用自动修复"""
    
    # === 网络配置 ===
    network_protocol: str = "zmq"
    """网络协议: zmq/http/auto"""
    
    connection_pool_size: int = 10
    """连接池大小"""
    
    request_timeout: float = 30.0
    """请求超时时间(秒)"""
    
    retry_attempts: int = 3
    """重试次数"""
    
    # === ZeroMQ 专用配置 ===
    zmq_socket_type: str = "REQ"
    """ZMQ套接字类型: REQ/DEALER/PUSH"""
    
    zmq_high_water_mark: int = 10000
    """ZMQ队列高水位标记"""
    
    zmq_linger: int = 0
    """ZMQ关闭延迟时间"""
    
    # === HTTP 专用配置 ===
    http_version: str = "2"
    """HTTP版本: 1.1/2/3"""
    
    http_keepalive: bool = True
    """是否启用HTTP连接保持"""
    
    http_compression: bool = True
    """是否启用HTTP压缩"""
    
    verify_ssl: bool = True
    """是否验证SSL证书"""
    
    auth_token: Optional[str] = None
    """认证令牌"""
    
    # === LevelDB 专用配置 ===
    leveldb_options: Dict[str, Any] = field(default_factory=dict)
    """LevelDB特定配置选项"""
    
    # === 监控配置 ===
    metrics_enabled: bool = True
    """是否启用指标收集"""
    
    slow_query_threshold: float = 1.0
    """慢查询阈值(秒)"""
    
    prometheus_endpoint: str = "/metrics"
    """Prometheus指标端点"""
    
    def __post_init__(self):
        """配置后处理，计算派生值和验证配置"""
        # 计算缓存大小（字节）
        self.cache_size_bytes = parse_size(self.cache_size)
        
        # 设置默认的LevelDB配置（简化版，只使用plyvel支持的选项）
        if not self.leveldb_options:
            self.leveldb_options = {
                # 'write_buffer_size': self.buffer_size * 1024 * 1024,
                # 'max_open_files': 1000,
                # 注意：许多LevelDB选项在plyvel中不直接支持
            }
        
        # 验证配置
        self._validate_config()
    
    def _validate_config(self):
        """验证配置参数有效性"""
        if self.buffer_size <= 0:
            raise ValueError("buffer_size必须大于0")
        
        if self.buffer_timeout <= 0:
            raise ValueError("buffer_timeout必须大于0")
        
        if self.cache_policy not in ["lru", "lfu", "arc"]:
            raise ValueError("cache_policy必须是lru/lfu/arc之一")
        
        if self.sync_mode not in ["async", "sync", "manual"]:
            raise ValueError("sync_mode必须是async/sync/manual之一")
        
        if self.network_protocol not in ["zmq", "http", "auto"]:
            raise ValueError("network_protocol必须是zmq/http/auto之一")
        
        if self.http_version not in ["1.1", "2", "3"]:
            raise ValueError("http_version必须是1.1/2/3之一")
    
    @property
    def is_encrypted(self) -> bool:
        """是否启用了加密"""
        return self.encryption_key is not None
    
    @property
    def cache_size_mb(self) -> float:
        """缓存大小（MB）"""
        return self.cache_size_bytes / (1024 * 1024)
    
    @classmethod
    def for_development(cls) -> "FlaxKVConfig":
        """开发环境配置预设。
        
        特点：快速启动，同步写入，小缓存，详细监控
        """
        return cls(
            buffer_size=100,
            buffer_timeout=1.0,
            cache_size="64MB",
            sync_mode="sync",
            backup_interval=0,  # 关闭自动备份
            metrics_enabled=True,
            slow_query_threshold=0.1,  # 更敏感的慢查询检测
            integrity_check=True,
            auto_repair=True,
            network_protocol="http",  # HTTP更易调试
            request_timeout=5.0
        )
    
    @classmethod
    def for_production(cls) -> "FlaxKVConfig":
        """生产环境配置预设。
        
        特点：高性能，大缓存，安全加密，完整监控
        """
        return cls(
            buffer_size=5000,
            buffer_timeout=10.0,
            cache_size="1GB",
            cache_policy="arc",  # 更智能的缓存策略
            sync_mode="async",
            compression=True,
            encryption_key=os.getenv("FLAXKV_ENCRYPTION_KEY"),
            backup_interval=1800,  # 30分钟备份一次
            integrity_check=True,
            auto_repair=True,
            network_protocol="zmq",  # ZMQ高性能
            connection_pool_size=20,
            request_timeout=30.0,
            retry_attempts=3,
            metrics_enabled=True,
            slow_query_threshold=1.0
        )
    
    @classmethod
    def for_high_performance(cls) -> "FlaxKVConfig":
        """高性能配置预设。
        
        特点：最大缓存，异步操作，最少检查，ZMQ协议
        """
        return cls(
            buffer_size=10000,
            buffer_timeout=60.0,
            cache_size="2GB", 
            cache_policy="lru",
            sync_mode="manual",  # 手动控制同步
            compression=False,  # 关闭压缩节省CPU
            backup_interval=0,  # 关闭自动备份
            integrity_check=False,  # 关闭完整性检查
            auto_repair=False,
            network_protocol="zmq",
            connection_pool_size=50,
            zmq_high_water_mark=50000,
            metrics_enabled=False,  # 关闭监控减少开销
            leveldb_options={
                'block_cache_size': parse_size("2GB"),
                'write_buffer_size': 128 * 1024 * 1024,  # 128MB
                'max_open_files': 5000,
                'block_size': 64 * 1024,
                'compression': None,
                'bloom_filter_bits': 12,
            }
        )
    
    @classmethod
    def for_testing(cls) -> "FlaxKVConfig":
        """测试环境配置预设。
        
        特点：快速，确定性，易清理
        """
        return cls(
            buffer_size=10,
            buffer_timeout=0.1,
            cache_size="16MB",
            sync_mode="sync",  # 同步确保测试确定性
            compression=False,
            backup_interval=0,
            integrity_check=True,
            auto_repair=True,
            network_protocol="http",
            request_timeout=1.0,
            retry_attempts=1,
            metrics_enabled=False,  # 测试时不需要监控
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            field.name: getattr(self, field.name)
            for field in self.__dataclass_fields__.values()
        }
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "FlaxKVConfig":
        """从字典创建配置对象"""
        # 只保留有效的字段
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_dict = {
            k: v for k, v in config_dict.items() 
            if k in valid_fields
        }
        return cls(**filtered_dict)
    
    @classmethod
    def from_file(cls, config_path: Union[str, Path]) -> "FlaxKVConfig":
        """从配置文件加载配置"""
        import yaml
        
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config_dict = yaml.safe_load(f)
        
        return cls.from_dict(config_dict)
    
    def save_to_file(self, config_path: Union[str, Path]):
        """保存配置到文件"""
        import yaml
        
        config_path = Path(config_path)
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False, 
                     allow_unicode=True, sort_keys=True)
    
    def __repr__(self) -> str:
        """简洁的配置表示"""
        return (
            f"FlaxKVConfig(buffer={self.buffer_size}, "
            f"cache={self.cache_size}, protocol={self.network_protocol}, "
            f"sync={self.sync_mode})"
        )