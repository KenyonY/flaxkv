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

# ============================================================================
# 服务器配置 (flaxkv2 run)
# ============================================================================
[server]
host = "127.0.0.1"           # 监听地址 (0.0.0.0 表示所有网络接口)
port = 5555                   # 监听端口
data_dir = "./data"           # 数据存储目录
workers = 4                   # 工作线程数
log_level = "INFO"            # 日志级别: DEBUG, INFO, WARNING, ERROR

# 加密配置 (推荐启用)
enable_encryption = false     # 是否启用 CurveZMQ 加密
# password = "your-password"  # 服务器密码 (启用加密时必需)
derive_from_password = true   # 从密码派生密钥 (推荐)

# 可选配置
enable_compression = false    # 启用 LZ4 压缩
# 性能配置: balanced, read_optimized, write_optimized, memory_constrained, large_database, ml_workload
performance_profile = "balanced"


# ============================================================================
# 客户端配置 (flaxkv2 set/get/list)
# ============================================================================
[client]
server = "127.0.0.1:5555"     # 默认服务器地址
db_name = "default_db"        # 默认数据库名称
timeout = 30                  # 连接超时（秒）

# 加密配置 (需与服务器匹配)
enable_encryption = false     # 是否启用加密
# password = "your-password"  # 客户端密码 (需与服务器一致)
derive_from_password = true   # 从密码派生密钥


# ============================================================================
# 多服务器管理 (可选)
# ============================================================================
# 定义多个服务器，通过 --server @name 引用
# 示例：flaxkv2 list --server @production

[servers.production]
host = "192.168.1.100"
port = 5555
enable_encryption = true
# password = "prod-password"


# ============================================================================
# 使用示例
# ============================================================================
# 启动服务器：
#   flaxkv2 run
#   flaxkv2 run --enable-encryption --password mypass
#
# 客户端操作：
#   flaxkv2 set myfile.txt                    # 使用默认配置
#   flaxkv2 list --server @production         # 使用定义的服务器
#   flaxkv2 get myfile --output ./downloads/  # 下载文件
#
# 命令行参数会覆盖配置文件
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
