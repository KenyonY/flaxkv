"""
FlaxFile配置管理 - 支持 TOML 格式
"""

import sys
from pathlib import Path
from typing import Dict, Any, Optional

# Python 3.11+ 有内置的 tomllib，否则使用 tomli
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None


# 默认配置
DEFAULT_CONFIG = {
    'client': {
        'default_server': 'local',
    },
    'server': {
        'host': '0.0.0.0',
        'port': 25555,
        'storage_dir': './zmq_streaming_storage',
    },
    'servers': {
        'local': {
            'host': '127.0.0.1',
            'port': 25555,
        }
    }
}

# 配置文件搜索路径（优先级从高到低）
CONFIG_SEARCH_PATHS = [
    Path.cwd() / 'flaxfile.toml',      # 当前目录
    Path.home() / 'flaxfile.toml',     # 家目录
]


def generate_default_config_toml() -> str:
    """生成默认配置文件内容（TOML格式）"""
    return """# FlaxFile 配置文件
# 支持从以下路径读取（优先级从高到低）：
#   1. ./flaxfile.toml  (当前目录)
#   2. ~/flaxfile.toml  (家目录)

# ============================================================
# 客户端配置
# ============================================================
[client]
# 默认使用的服务器（对应 [servers] 中的键名）
default_server = "local"

# ============================================================
# 服务端配置（运行 flaxfile serve 时使用）
# ============================================================
[server]
# 监听地址
#   - "0.0.0.0" = 监听所有网卡（允许远程连接）
#   - "127.0.0.1" = 仅本地访问
host = "0.0.0.0"

# 端口配置 (异步单端口设计)
port = 25555

# 存储目录
storage_dir = "./zmq_streaming_storage"

# ============================================================
# 远程服务器配置
# ============================================================

# 本地服务器
[servers.local]
host = "127.0.0.1"
port = 25555

# 生产环境服务器示例（取消注释以启用）
# [servers.prod]
# host = "192.168.1.100"
# port = 25555

# 开发环境服务器示例
# [servers.dev]
# host = "10.0.0.5"
# port = 26555
# download_port = 26556
# control_port = 26557
"""


class Config:
    """配置管理器 - 支持 TOML 格式和多路径读取"""

    def __init__(self):
        self.config_file: Optional[Path] = None
        self.config = self.load()
        self._data = None  # 用于交互式配置时存储修改后的配置

    def _load_config(self) -> Dict[str, Any]:
        """加载配置（用于交互式配置）"""
        if self._data is None:
            self._data = self.load()
        return self._data

    def load(self) -> Dict[str, Any]:
        """
        从配置文件加载配置

        优先级（从高到低）：
        1. ./flaxfile.toml  (当前目录)
        2. ~/flaxfile.toml  (家目录)
        3. 默认配置
        """
        # 检查 tomllib 是否可用
        if tomllib is None:
            print("警告: 未安装 tomli 库，无法读取 TOML 配置文件")
            print("请运行: pip install tomli")
            return DEFAULT_CONFIG.copy()

        # 按优先级搜索配置文件
        for config_path in CONFIG_SEARCH_PATHS:
            if config_path.exists():
                try:
                    with open(config_path, 'rb') as f:
                        config = tomllib.load(f)
                    self.config_file = config_path
                    # print(f"✓ 已加载配置: {config_path}")
                    return config
                except Exception as e:
                    print(f"警告: 加载配置文件失败 ({config_path}): {e}")

        # 未找到配置文件，使用默认配置
        return DEFAULT_CONFIG.copy()

    def get_server_config(self) -> Dict[str, Any]:
        """获取服务器配置（用于 flaxfile serve）"""
        return self.config.get('server', DEFAULT_CONFIG['server'])

    def get_server(self, name: Optional[str] = None) -> Dict[str, Any]:
        """
        获取远程服务器配置（用于客户端连接）

        Args:
            name: 服务器名称，如果为None则使用默认服务器
        """
        servers = self.config.get('servers', {})

        if name is None:
            # 使用默认服务器
            client_config = self.config.get('client', {})
            default_name = client_config.get('default_server', 'local')

            if default_name in servers:
                return servers[default_name]
            else:
                # 回退到 local
                return servers.get('local', DEFAULT_CONFIG['servers']['local'])

        # 使用指定的服务器
        if name in servers:
            return servers[name]

        # 服务器不存在
        raise ValueError(f"服务器 '{name}' 不存在")

    def list_servers(self) -> Dict[str, Dict[str, Any]]:
        """列出所有配置的服务器"""
        return self.config.get('servers', {})

    def get_default_server_name(self) -> str:
        """获取默认服务器名称"""
        client_config = self.config.get('client', {})
        return client_config.get('default_server', 'local')

    @staticmethod
    def init_config_file(path: Optional[Path] = None) -> Path:
        """
        初始化配置文件

        Args:
            path: 配置文件路径，默认为当前目录的 flaxfile.toml

        Returns:
            创建的配置文件路径
        """
        if path is None:
            path = Path.cwd() / 'flaxfile.toml'

        if path.exists():
            raise FileExistsError(f"配置文件已存在: {path}")

        # 生成默认配置
        content = generate_default_config_toml()

        # 写入文件
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)

        return path


# 配置文件搜索路径（用于显示）
CONFIG_FILE_PATHS = [str(p) for p in CONFIG_SEARCH_PATHS]
