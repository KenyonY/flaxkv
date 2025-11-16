#!/usr/bin/env python3
"""
FlaxFile CLI - 高性能文件传输工具

使用方法:
    flaxfile serve                         # 启动服务器
    flaxfile set myfile /path/to/file.bin  # 上传文件
    flaxfile get myfile output.bin         # 下载文件
    flaxfile list                          # 列出所有文件
    flaxfile delete myfile                 # 删除文件
    flaxfile config add-server prod 192.168.1.100  # 添加服务器
"""

import sys
from pathlib import Path
from typing import Optional
import fire

from .client import FlaxFileClient
from .server import FlaxFileServer
from .config import Config, CONFIG_FILE_PATHS


class ConfigCommands:
    """配置管理命令"""

    def __init__(self, config_obj):
        self._config = config_obj

    def init(self, path: Optional[str] = None):
        """
        初始化配置文件

        Args:
            path: 配置文件路径（可选，默认为当前目录的 flaxfile.toml）

        示例:
            flaxfile config init                    # 在当前目录创建
            flaxfile config init ~/flaxfile.toml    # 在家目录创建
        """
        from pathlib import Path

        try:
            config_path = Path(path) if path else None
            created_path = Config.init_config_file(config_path)
            print(f"✓ 已创建配置文件: {created_path}")
            print(f"\n请编辑配置文件以自定义设置")
        except FileExistsError as e:
            print(f"✗ {e}")
            sys.exit(1)
        except Exception as e:
            print(f"✗ 创建配置文件失败: {e}")
            sys.exit(1)

    def show(self):
        """显示当前配置"""
        print("="*60)
        print("FlaxFile 配置")
        print("="*60)

        # 显示配置文件来源
        if self._config.config_file:
            print(f"\n配置文件: {self._config.config_file}")
        else:
            print(f"\n配置文件: 未找到（使用默认配置）")

        # 显示搜索路径
        print(f"\n配置文件搜索路径（优先级从高到低）:")
        for i, path in enumerate(CONFIG_FILE_PATHS, 1):
            exists = "✓" if Path(path).exists() else "✗"
            print(f"  {i}. {exists} {path}")

        # 显示服务器配置
        server_config = self._config.get_server_config()
        print(f"\n服务器配置（flaxfile serve）:")
        print(f"  监听地址: {server_config['host']}")
        print(f"  上传端口: {server_config['upload_port']}")
        print(f"  下载端口: {server_config['download_port']}")
        print(f"  控制端口: {server_config['control_port']}")
        print(f"  存储目录: {server_config['storage_dir']}")

        # 显示客户端配置
        default_name = self._config.get_default_server_name()
        print(f"\n客户端配置:")
        print(f"  默认服务器: {default_name}")

        # 显示远程服务器列表
        servers = self._config.list_servers()
        if servers:
            print(f"\n远程服务器列表:")
            for name, config in servers.items():
                is_default = (name == default_name)
                marker = " [默认]" if is_default else ""
                print(f"\n  {name}{marker}:")
                print(f"    地址: {config['host']}")
                print(f"    上传端口: {config['upload_port']}")
                print(f"    下载端口: {config['download_port']}")
                print(f"    控制端口: {config['control_port']}")
        else:
            print("\n未配置任何远程服务器")

    def path(self):
        """
        显示配置文件路径

        示例:
            flaxfile config path
        """
        print("配置文件搜索路径（优先级从高到低）:")
        for i, path in enumerate(CONFIG_FILE_PATHS, 1):
            from pathlib import Path
            exists = "✓" if Path(path).exists() else "✗"
            current = "← 当前使用" if (self._config.config_file and str(self._config.config_file) == path) else ""
            print(f"  {i}. {exists} {path} {current}")

        if not self._config.config_file:
            print(f"\n未找到配置文件，使用默认配置")
            print(f"运行 'flaxfile config init' 创建配置文件")


class FlaxFileCLI:
    """FlaxFile CLI主类"""

    def __init__(self):
        self._config_obj = Config()
        self.config = ConfigCommands(self._config_obj)

    def serve(
        self,
        host: Optional[str] = None,
        upload_port: Optional[int] = None,
        download_port: Optional[int] = None,
        control_port: Optional[int] = None,
    ):
        """
        启动FlaxFile服务器

        Args:
            host: 监听地址 (可选，默认从配置文件读取)
            upload_port: 上传端口 (可选，默认从配置文件读取)
            download_port: 下载端口 (可选，默认从配置文件读取)
            control_port: 控制端口 (可选，默认从配置文件读取)

        示例:
            flaxfile serve                          # 使用配置文件
            flaxfile serve --host 127.0.0.1         # 覆盖配置
            flaxfile serve --upload-port 26555      # 覆盖端口
        """
        # 从配置文件读取服务器配置
        server_config = self._config_obj.get_server_config()

        # 命令行参数优先级更高
        final_host = host if host is not None else server_config['host']
        final_upload_port = upload_port if upload_port is not None else server_config['upload_port']
        final_download_port = download_port if download_port is not None else server_config['download_port']
        final_control_port = control_port if control_port is not None else server_config['control_port']

        server = FlaxFileServer(
            host=final_host,
            upload_port=final_upload_port,
            download_port=final_download_port,
            control_port=final_control_port
        )
        server.start()

    def set(
        self,
        file_path: str,
        key: Optional[str] = None,
        server: Optional[str] = None,
    ):
        """
        上传文件到服务器

        Args:
            file_path: 本地文件路径
            key: 文件键名（可选，默认使用文件名）
            server: 服务器名称（可选，默认使用配置中的默认服务器）

        示例:
            flaxfile set /path/to/file.bin              # key = file.bin
            flaxfile set /path/to/file.bin myfile       # key = myfile
            flaxfile set /path/to/video.mp4 --server prod
        """
        # 如果未指定 key，使用文件名作为 key
        if key is None:
            from pathlib import Path
            key = Path(file_path).name

        # 获取服务器配置
        server_config = self._config_obj.get_server(server)

        # 创建客户端
        client = FlaxFileClient(
            server_host=server_config['host'],
            upload_port=server_config['upload_port'],
            download_port=server_config['download_port'],
            control_port=server_config['control_port'],
        )

        try:
            result = client.upload_file(file_path, key, show_progress=True)
            print(f"\n✓ 上传成功")
            print(f"  键名: {key}")
            print(f"  大小: {result['size'] / (1024*1024):.2f} MB")
            print(f"  吞吐量: {result['throughput']:.2f} MB/s")
        finally:
            client.close()

    def get(
        self,
        key: str,
        output_path: Optional[str] = None,
        server: Optional[str] = None,
    ):
        """
        从服务器下载文件

        Args:
            key: 文件键名
            output_path: 输出路径（可选，默认使用 key 作为文件名）
            server: 服务器名称（可选）

        示例:
            flaxfile get myfile                     # 保存为 ./myfile
            flaxfile get myfile output.bin          # 保存为 ./output.bin
            flaxfile get video --server prod        # 从 prod 服务器下载
        """
        # 默认输出路径使用 key 作为文件名
        if output_path is None:
            output_path = key

        # 获取服务器配置
        server_config = self._config_obj.get_server(server)

        # 创建客户端
        client = FlaxFileClient(
            server_host=server_config['host'],
            upload_port=server_config['upload_port'],
            download_port=server_config['download_port'],
            control_port=server_config['control_port'],
        )

        try:
            result = client.download_file(key, output_path, show_progress=True)
            print(f"\n✓ 下载成功")
            print(f"  保存到: {output_path}")
            print(f"  大小: {result['size'] / (1024*1024):.2f} MB")
            print(f"  吞吐量: {result['throughput']:.2f} MB/s")
        finally:
            client.close()

    def delete(
        self,
        key: str,
        server: Optional[str] = None,
    ):
        """
        删除服务器上的文件

        Args:
            key: 文件键名
            server: 服务器名称（可选）

        示例:
            flaxfile delete myfile
            flaxfile delete video --server prod
        """
        # 获取服务器配置
        server_config = self._config_obj.get_server(server)

        # 创建客户端
        client = FlaxFileClient(
            server_host=server_config['host'],
            upload_port=server_config['upload_port'],
            download_port=server_config['download_port'],
            control_port=server_config['control_port'],
        )

        try:
            success = client.delete_file(key)
            if success:
                print(f"✓ 删除成功: {key}")
            else:
                print(f"✗ 删除失败: {key}")
                sys.exit(1)
        finally:
            client.close()

    def list(
        self,
        server: Optional[str] = None,
    ):
        """
        列出服务器上的所有文件

        Args:
            server: 服务器名称（可选）

        示例:
            flaxfile list
            flaxfile list --server prod

        注意: 需要服务器支持LIST命令（当前版本暂不支持，待实现）
        """
        print("注意: list功能待实现")
        print("当前服务器端暂不支持列出文件功能")
        print("可以通过服务器端存储目录查看：zmq_streaming_storage/")

    def version(self):
        """显示版本信息"""
        from . import __version__
        print(f"FlaxFile v{__version__}")
        print("高性能文件传输工具")
        print("基于ZMQ优化的跨网络文件传输系统")


def main():
    """CLI入口"""
    try:
        fire.Fire(FlaxFileCLI)
    except KeyboardInterrupt:
        print("\n\n中断")
        sys.exit(0)
    except Exception as e:
        print(f"错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
