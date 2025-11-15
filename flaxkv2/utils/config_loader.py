"""
FlaxKV2 配置文件加载器

支持从 TOML 配置文件加载服务器端和客户端配置
配置文件查找顺序：
1. 当前目录下的 .flaxkv.toml
2. 用户主目录下的 ~/.flaxkv.toml
3. 使用默认值
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional
try:
    import tomllib  # Python 3.11+
except ImportError:
    try:
        import tomli as tomllib  # Python 3.6-3.10
    except ImportError:
        tomllib = None

from flaxkv2.utils.log import get_logger

logger = get_logger(__name__)


class ConfigLoader:
    """配置文件加载器"""

    DEFAULT_CONFIG_NAMES = ['flaxkv.toml', '.flaxkv.toml']

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化配置加载器

        Args:
            config_path: 配置文件路径，None 则自动查找
        """
        self.config_path = config_path
        self.config = {}
        self._load_config()

    def _find_config_file(self) -> Optional[Path]:
        """
        查找配置文件

        查找顺序：
        1. 当前目录
        2. 用户主目录

        Returns:
            配置文件路径，找不到返回 None
        """
        # 检查当前目录
        for config_name in self.DEFAULT_CONFIG_NAMES:
            current_dir_config = Path.cwd() / config_name
            if current_dir_config.exists():
                logger.debug(f"Found config in current directory: {current_dir_config}")
                return current_dir_config

        # 检查用户主目录
        for config_name in self.DEFAULT_CONFIG_NAMES:
            home_dir_config = Path.home() / config_name
            if home_dir_config.exists():
                logger.debug(f"Found config in home directory: {home_dir_config}")
                return home_dir_config

        logger.debug("No config file found")
        return None

    def _load_config(self):
        """加载配置文件"""
        if tomllib is None:
            logger.warning(
                "TOML support not available. Install 'tomli' package for Python < 3.11: "
                "pip install tomli"
            )
            return

        # 确定配置文件路径
        if self.config_path:
            config_file = Path(self.config_path)
            if not config_file.exists():
                raise FileNotFoundError(f"Config file not found: {config_file}")
        else:
            config_file = self._find_config_file()
            if config_file is None:
                logger.debug("No config file found, using defaults")
                return

        # 读取配置文件
        try:
            with open(config_file, 'rb') as f:
                self.config = tomllib.load(f)
            logger.info(f"Loaded config from: {config_file}")
        except Exception as e:
            logger.error(f"Failed to load config file {config_file}: {e}")
            raise

    def get_server_config(self, profile: str = 'default') -> Dict[str, Any]:
        """
        获取服务器配置

        Args:
            profile: 配置文件中的 server profile 名称

        Returns:
            服务器配置字典
        """
        if 'server' not in self.config:
            return {}

        server_config = self.config['server']

        # 如果指定了 profile，使用 profile 配置覆盖默认配置
        if profile != 'default' and 'profiles' in server_config:
            if profile in server_config['profiles']:
                # 合并默认配置和 profile 配置
                result = {}
                # 先复制所有非 profiles 的键
                for key, value in server_config.items():
                    if key != 'profiles':
                        result[key] = value
                # 然后用 profile 的值覆盖
                result.update(server_config['profiles'][profile])
                return result
            else:
                logger.warning(f"Server profile '{profile}' not found, using default")

        # 返回默认配置（排除 profiles）
        return {k: v for k, v in server_config.items() if k != 'profiles'}

    def get_client_config(self, profile: str = 'default') -> Dict[str, Any]:
        """
        获取客户端配置

        Args:
            profile: 配置文件中的 client profile 名称

        Returns:
            客户端配置字典
        """
        if 'client' not in self.config:
            return {}

        client_config = self.config['client']

        # 如果指定了 profile，使用 profile 配置覆盖默认配置
        if profile != 'default' and 'profiles' in client_config:
            if profile in client_config['profiles']:
                # 合并默认配置和 profile 配置
                result = {}
                # 先复制所有非 profiles 的键
                for key, value in client_config.items():
                    if key != 'profiles':
                        result[key] = value
                # 然后用 profile 的值覆盖
                result.update(client_config['profiles'][profile])
                return result
            else:
                logger.warning(f"Client profile '{profile}' not found, using default")

        # 返回默认配置（排除 profiles）
        return {k: v for k, v in client_config.items() if k != 'profiles'}

    def get_servers(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有定义的远程服务器配置

        Returns:
            服务器名称到配置的映射
        """
        if 'servers' not in self.config:
            return {}
        return self.config['servers']

    def get_server_address(self, server_name: str) -> Optional[str]:
        """
        获取指定服务器的地址

        Args:
            server_name: 服务器名称

        Returns:
            服务器地址 (host:port 格式)，找不到返回 None
        """
        servers = self.get_servers()
        if server_name in servers:
            server_config = servers[server_name]
            host = server_config.get('host', '127.0.0.1')
            port = server_config.get('port', 5555)
            return f"{host}:{port}"
        return None

    def get_defaults(self) -> Dict[str, Any]:
        """
        获取默认配置

        Returns:
            默认配置字典
        """
        return self.config.get('defaults', {})

    def list_profiles(self, section: str = 'server') -> list:
        """
        列出可用的配置 profiles

        Args:
            section: 'server' 或 'client'

        Returns:
            profile 名称列表
        """
        if section not in self.config:
            return []

        section_config = self.config[section]
        if 'profiles' not in section_config:
            return []

        return list(section_config['profiles'].keys())

    def list_servers(self) -> list:
        """
        列出所有定义的服务器名称

        Returns:
            服务器名称列表
        """
        return list(self.get_servers().keys())


def create_sample_config() -> str:
    """
    创建示例配置文件内容

    Returns:
        TOML 格式的示例配置
    """
    return '''# FlaxKV2 配置文件
# 支持服务器端和客户端配置，以及多服务器管理

# ============================================================================
# 默认设置（全局）
# ============================================================================
[defaults]
# 默认数据目录
data_dir = "./data"

# 默认日志级别 (DEBUG, INFO, WARNING, ERROR)
log_level = "INFO"

# 默认数据库名称
db_name = "default_db"


# ============================================================================
# 服务器端配置 (用于 'flaxkv2 run' 命令)
# ============================================================================
[server]
# 监听地址
host = "127.0.0.1"

# 监听端口
port = 5555

# 数据目录
data_dir = "./data"

# 工作线程数
workers = 4

# 日志级别
log_level = "INFO"

# 启用加密 (CurveZMQ)
enable_encryption = false

# 服务器密码（启用加密时使用）
# password = "your-secure-password"

# 从密码派生密钥（推荐）
derive_from_password = true

# 启用压缩 (LZ4)
enable_compression = false

# 性能配置文件
# 可选值: balanced, read_optimized, write_optimized, memory_constrained, large_database, ml_workload
performance_profile = "balanced"

# 服务器配置 profiles（用于不同的部署场景）
[server.profiles.production]
host = "0.0.0.0"
port = 5555
workers = 8
log_level = "WARNING"
enable_encryption = true
enable_compression = true
performance_profile = "balanced"

[server.profiles.development]
host = "127.0.0.1"
port = 5555
workers = 2
log_level = "DEBUG"
enable_encryption = false
enable_compression = false
performance_profile = "memory_constrained"

[server.profiles.high_performance]
host = "0.0.0.0"
port = 5555
workers = 16
log_level = "WARNING"
enable_compression = true
performance_profile = "read_optimized"


# ============================================================================
# 客户端配置（用于各种客户端命令）
# ============================================================================
[client]
# 默认服务器地址
server = "127.0.0.1:5555"

# 默认数据库名称
db_name = "default_db"

# 后端类型 ('local', 'remote', 'auto')
backend = "auto"

# 本地数据库路径
path = "./data"

# 连接超时（秒）
timeout = 30

# 客户端加密配置（连接远程服务器时使用）
enable_encryption = false
# password = ""
# derive_from_password = true

# 客户端配置 profiles
[client.profiles.local]
backend = "local"
path = "./data"

[client.profiles.remote]
backend = "remote"
server = "127.0.0.1:5555"

[client.profiles.production]
backend = "remote"
server = "prod-server:5555"
timeout = 60


# ============================================================================
# 远程服务器定义（可以定义多个服务器，通过名称引用）
# ============================================================================
[servers.local]
host = "127.0.0.1"
port = 5555
# 加密配置
enable_encryption = false
# password = "local-password"
# derive_from_password = true

[servers.production]
host = "192.168.1.100"
port = 5555
# 加密配置（生产环境建议启用）
enable_encryption = true
# password = "prod-password"
# derive_from_password = true

[servers.staging]
host = "192.168.1.200"
port = 5555
# 加密配置
enable_encryption = false
# password = "staging-password"
# derive_from_password = true

[servers.ml_cluster]
host = "ml.example.com"
port = 5555
# 加密配置
enable_encryption = false
# password = "ml-password"
# derive_from_password = true


# ============================================================================
# Inspector Web UI 配置
# ============================================================================
[inspector]
# 默认监听地址
host = "127.0.0.1"

# 默认监听端口
port = 8080

# 调试模式
debug = false


# ============================================================================
# 使用示例：
# ============================================================================
# 1. 启动服务器（使用默认配置）：
#    flaxkv2 run
#
# 2. 启动服务器（使用 production profile）：
#    flaxkv2 run --profile production
#
# 3. 连接到定义的服务器：
#    flaxkv2 list --server @production
#    flaxkv2 set myfile.txt --server @staging
#
# 4. 使用客户端 profile：
#    flaxkv2 inspect keys mydb --profile remote
#
# 5. 命令行参数会覆盖配置文件中的值：
#    flaxkv2 run --port 6666 --workers 8
'''


def save_sample_config(path: Optional[str] = None) -> Path:
    """
    保存示例配置文件

    Args:
        path: 保存路径，None 则保存到当前目录的 flaxkv.toml

    Returns:
        保存的文件路径
    """
    if path is None:
        path = Path.cwd() / 'flaxkv.toml'
    else:
        path = Path(path)

    # 检查文件是否已存在
    if path.exists():
        raise FileExistsError(f"Config file already exists: {path}")

    # 写入示例配置
    content = create_sample_config()
    path.write_text(content, encoding='utf-8')

    logger.info(f"Sample config saved to: {path}")
    return path


# 便捷函数：加载配置
def load_config(config_path: Optional[str] = None) -> ConfigLoader:
    """
    加载配置文件

    Args:
        config_path: 配置文件路径，None 则自动查找

    Returns:
        ConfigLoader 实例
    """
    return ConfigLoader(config_path)
