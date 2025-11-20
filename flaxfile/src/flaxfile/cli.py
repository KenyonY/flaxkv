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

from .multi_socket_client import MultiSocketFlaxFileClientSync as FlaxFileClient
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
        print(f"  端口: {server_config['port']}")
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
                print(f"    端口: {config['port']}")
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

    def interactive(self):
        """
        交互式配置模式

        示例:
            flaxfile config interactive
            flaxfile config  # 默认进入交互模式
        """
        import questionary
        from rich.console import Console
        from rich.panel import Panel

        console = Console()
        console.print(Panel.fit(
            "[bold cyan]FlaxFile 交互式配置[/bold cyan]\n使用方向键选择，Enter确认",
            border_style="cyan"
        ))

        # 确保有配置文件
        if not self._config.config_file:
            if questionary.confirm("未找到配置文件，是否创建?").ask():
                config_path = Path.cwd() / 'flaxfile.toml'
                created_path = Config.init_config_file(config_path)
                console.print(f"[green]✓ 已创建配置文件: {created_path}")
                # 重新加载配置
                self._config = Config()
            else:
                console.print("[yellow]取消配置")
                return

        while True:
            action = questionary.select(
                "请选择操作:",
                choices=[
                    "📡 配置服务器设置 (本地服务器)",
                    "🌐 管理远程服务器",
                    "⚙️  设置默认服务器",
                    "📄 查看当前配置",
                    "💾 保存并退出",
                    "❌ 退出不保存",
                ]
            ).ask()

            if action is None or action.startswith("❌"):
                console.print("[yellow]已取消，配置未保存")
                break

            elif action.startswith("📡"):
                self._config_server_settings()

            elif action.startswith("🌐"):
                self._manage_remote_servers()

            elif action.startswith("⚙️"):
                self._set_default_server()

            elif action.startswith("📄"):
                self.show()

            elif action.startswith("💾"):
                self._save_config()
                console.print("[bold green]✓ 配置已保存")
                break

    def _config_server_settings(self):
        """配置本地服务器设置"""
        import questionary
        from rich.console import Console

        console = Console()
        console.print("\n[bold cyan]配置本地服务器设置[/bold cyan]")

        server_config = self._config.get_server_config()

        host = questionary.text(
            "监听地址 (0.0.0.0=允许远程, 127.0.0.1=仅本地):",
            default=server_config.get('host', '0.0.0.0')
        ).ask()

        port = questionary.text(
            "端口:",
            default=str(server_config.get('port', 25555))
        ).ask()

        storage_dir = questionary.text(
            "存储目录:",
            default=server_config.get('storage_dir', './zmq_streaming_storage')
        ).ask()

        # 更新配置
        if not hasattr(self._config, '_data'):
            self._config._data = self._config._load_config()

        if 'server' not in self._config._data:
            self._config._data['server'] = {}

        self._config._data['server']['host'] = host
        self._config._data['server']['port'] = int(port)
        self._config._data['server']['storage_dir'] = storage_dir

        console.print("[green]✓ 服务器设置已更新")

    def _manage_remote_servers(self):
        """管理远程服务器"""
        import questionary
        from rich.console import Console

        console = Console()

        while True:
            servers = self._config.list_servers()

            choices = [f"➕ 添加新服务器"]
            for name in servers.keys():
                choices.append(f"✏️  编辑: {name}")
                choices.append(f"🗑️  删除: {name}")
            choices.append("⬅️  返回")

            action = questionary.select(
                "远程服务器管理:",
                choices=choices
            ).ask()

            if action is None or action.startswith("⬅️"):
                break

            elif action.startswith("➕"):
                self._add_remote_server()

            elif action.startswith("✏️"):
                server_name = action.split(": ")[1]
                self._edit_remote_server(server_name)

            elif action.startswith("🗑️"):
                server_name = action.split(": ")[1]
                self._delete_remote_server(server_name)

    def _add_remote_server(self):
        """添加远程服务器"""
        import questionary
        from rich.console import Console

        console = Console()
        console.print("\n[bold cyan]添加远程服务器[/bold cyan]")

        name = questionary.text("服务器名称 (如: prod, dev):").ask()
        if not name:
            return

        host = questionary.text("服务器地址:").ask()
        if not host:
            return

        port = questionary.text("端口:", default="25555").ask()

        # 更新配置
        if not hasattr(self._config, '_data'):
            self._config._data = self._config._load_config()

        if 'servers' not in self._config._data:
            self._config._data['servers'] = {}

        self._config._data['servers'][name] = {
            'host': host,
            'port': int(port)
        }

        console.print(f"[green]✓ 已添加服务器: {name}")

    def _edit_remote_server(self, server_name: str):
        """编辑远程服务器"""
        import questionary
        from rich.console import Console

        console = Console()
        console.print(f"\n[bold cyan]编辑服务器: {server_name}[/bold cyan]")

        server_config = self._config.get_server(server_name)

        host = questionary.text("服务器地址:", default=server_config['host']).ask()
        port = questionary.text("端口:", default=str(server_config['port'])).ask()

        # 更新配置
        if not hasattr(self._config, '_data'):
            self._config._data = self._config._load_config()

        self._config._data['servers'][server_name] = {
            'host': host,
            'port': int(port)
        }

        console.print(f"[green]✓ 已更新服务器: {server_name}")

    def _delete_remote_server(self, server_name: str):
        """删除远程服务器"""
        import questionary
        from rich.console import Console

        console = Console()

        if questionary.confirm(f"确认删除服务器 '{server_name}'?").ask():
            if not hasattr(self._config, '_data'):
                self._config._data = self._config._load_config()

            if 'servers' in self._config._data and server_name in self._config._data['servers']:
                del self._config._data['servers'][server_name]
                console.print(f"[green]✓ 已删除服务器: {server_name}")
            else:
                console.print(f"[red]✗ 服务器不存在: {server_name}")

    def _set_default_server(self):
        """设置默认服务器"""
        import questionary
        from rich.console import Console

        console = Console()

        servers = self._config.list_servers()
        if not servers:
            console.print("[yellow]⚠ 没有可用的远程服务器")
            return

        server_names = list(servers.keys())
        current_default = self._config.get_default_server_name()

        selected = questionary.select(
            f"选择默认服务器 (当前: {current_default}):",
            choices=server_names
        ).ask()

        if selected:
            if not hasattr(self._config, '_data'):
                self._config._data = self._config._load_config()

            if 'client' not in self._config._data:
                self._config._data['client'] = {}

            self._config._data['client']['default_server'] = selected
            console.print(f"[green]✓ 已设置默认服务器: {selected}")

    def _save_config(self):
        """保存配置到文件"""
        import toml
        from pathlib import Path

        if not hasattr(self._config, '_data'):
            return

        config_file = self._config.config_file or (Path.cwd() / 'flaxfile.toml')

        with open(config_file, 'w', encoding='utf-8') as f:
            toml.dump(self._config._data, f)

    def __call__(self):
        """默认调用 interactive"""
        self.interactive()


class FlaxFileCLI:
    """FlaxFile CLI主类"""

    def __init__(self):
        self._config_obj = Config()
        self.config = ConfigCommands(self._config_obj)
        self.sync = SyncCommands(self._config_obj)

    def serve(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        password: Optional[str] = None,
    ):
        """
        启动FlaxFile服务器 (异步单端口)

        Args:
            host: 监听地址 (可选，默认从配置文件读取)
            port: 端口 (可选，默认从配置文件读取)
            password: 密码（用于加密传输，可选，优先使用环境变量 FLAXFILE_PASSWORD）

        示例:
            flaxfile serve                          # 使用配置文件
            flaxfile serve --host 127.0.0.1         # 覆盖配置
            flaxfile serve --port 26555             # 覆盖端口
            flaxfile serve --password mysecret      # 启用加密
            export FLAXFILE_PASSWORD=mysecret && flaxfile serve  # 推荐方式
        """
        import asyncio

        # 从配置文件读取服务器配置
        server_config = self._config_obj.get_server_config()

        # 命令行参数优先级更高
        final_host = host if host is not None else server_config['host']
        final_port = port if port is not None else server_config['port']

        server = FlaxFileServer(host=final_host, port=final_port, password=password)

        # 检查是否有运行中的事件循环
        try:
            loop = asyncio.get_running_loop()
            # 如果有运行中的事件循环，创建任务
            import sys
            sys.exit("错误: 请直接运行，不要在已有事件循环中调用")
        except RuntimeError:
            # 没有运行中的事件循环，正常启动
            asyncio.run(server.start())

    def set(
        self,
        file_path: str,
        key: Optional[str] = None,
        server: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """
        上传文件到服务器

        Args:
            file_path: 本地文件路径
            key: 文件键名（可选，默认使用文件名）
            server: 服务器名称（可选，默认使用配置中的默认服务器）
            password: 密码（用于加密传输，可选，优先使用环境变量 FLAXFILE_PASSWORD）

        示例:
            flaxfile set /path/to/file.bin              # key = file.bin
            flaxfile set /path/to/file.bin myfile       # key = myfile
            flaxfile set /path/to/video.mp4 --server prod
            export FLAXFILE_PASSWORD=mysecret && flaxfile set file.bin
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
            port=server_config['port'],
            password=password,
        )

        try:
            result = client.upload_file(file_path, key, show_progress=True)
            # Rich 已经显示了漂亮的结果，这里不需要再打印
        finally:
            client.close()

    def get(
        self,
        key: str,
        output_path: Optional[str] = None,
        server: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """
        从服务器下载文件

        Args:
            key: 文件键名
            output_path: 输出路径（可选，默认使用 key 作为文件名）
            server: 服务器名称（可选）
            password: 密码（用于加密传输，可选，优先使用环境变量 FLAXFILE_PASSWORD）

        示例:
            flaxfile get myfile                     # 保存为 ./myfile
            flaxfile get myfile output.bin          # 保存为 ./output.bin
            flaxfile get video --server prod        # 从 prod 服务器下载
            export FLAXFILE_PASSWORD=mysecret && flaxfile get myfile
        """
        # 默认输出路径使用 key 作为文件名
        if output_path is None:
            output_path = key

        # 获取服务器配置
        server_config = self._config_obj.get_server(server)

        # 创建客户端
        client = FlaxFileClient(
            server_host=server_config['host'],
            port=server_config['port'],
            password=password,
        )

        try:
            result = client.download_file(key, output_path, show_progress=True)
            # Rich 已经显示了漂亮的结果，这里不需要再打印
        finally:
            client.close()

    def delete(
        self,
        key: str,
        server: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """
        删除服务器上的文件

        Args:
            key: 文件键名
            server: 服务器名称（可选）
            password: 密码（用于加密传输，可选，优先使用环境变量 FLAXFILE_PASSWORD）

        示例:
            flaxfile delete myfile
            flaxfile delete video --server prod
            export FLAXFILE_PASSWORD=mysecret && flaxfile delete myfile
        """
        # 获取服务器配置
        server_config = self._config_obj.get_server(server)

        # 创建客户端
        client = FlaxFileClient(
            server_host=server_config['host'],
            port=server_config['port'],
            password=password,
        )

        try:
            success = client.delete_file(key)
            if success:
                from rich.console import Console
                Console().print(f"[bold green]✓ 删除成功:[/bold green] [cyan]{key}")
            else:
                from rich.console import Console
                Console().print(f"[bold red]✗ 删除失败:[/bold red] [yellow]{key}")
                sys.exit(1)
        finally:
            client.close()

    def list(
        self,
        prefix: str = "",
        server: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """
        列出服务器上的所有文件

        Args:
            prefix: 文件前缀过滤（可选）
            server: 服务器名称（可选）
            password: 密码（用于加密传输，可选，优先使用环境变量 FLAXFILE_PASSWORD）

        示例:
            flaxfile list                       # 列出所有文件
            flaxfile list myproject             # 列出 myproject 开头的文件
            flaxfile list --server prod         # 列出 prod 服务器的文件
            export FLAXFILE_PASSWORD=mysecret && flaxfile list
        """
        from rich.console import Console
        from rich.table import Table

        # 获取服务器配置
        server_config = self._config_obj.get_server(server)

        # 创建客户端
        client = FlaxFileClient(
            server_host=server_config['host'],
            port=server_config['port'],
            password=password,
        )

        console = Console()

        try:
            files = client.list_files(prefix=prefix)

            if not files:
                if prefix:
                    console.print(f"[yellow]⚠️  未找到匹配 '{prefix}' 的文件")
                else:
                    console.print("[yellow]⚠️  服务器上没有文件")
                return

            # 创建表格
            table = Table(title=f"[bold cyan]服务器文件列表 ({len(files)} 个文件)", show_header=True, header_style="bold magenta")
            table.add_column("文件键", style="cyan", no_wrap=False)
            table.add_column("大小", justify="right", style="yellow")
            table.add_column("修改时间", style="green")

            # 添加数据
            from datetime import datetime
            total_size = 0
            for file_info in files:
                key = file_info['key']
                size = file_info['size']
                mtime = file_info.get('mtime', 0)

                # 格式化大小
                if size < 1024:
                    size_str = f"{size} B"
                elif size < 1024 * 1024:
                    size_str = f"{size / 1024:.2f} KB"
                elif size < 1024 * 1024 * 1024:
                    size_str = f"{size / (1024 * 1024):.2f} MB"
                else:
                    size_str = f"{size / (1024 * 1024 * 1024):.2f} GB"

                # 格式化时间
                if mtime > 0:
                    mtime_str = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    mtime_str = "N/A"

                table.add_row(key, size_str, mtime_str)
                total_size += size

            console.print(table)

            # 显示总计
            if total_size < 1024 * 1024 * 1024:
                total_str = f"{total_size / (1024 * 1024):.2f} MB"
            else:
                total_str = f"{total_size / (1024 * 1024 * 1024):.2f} GB"

            console.print(f"\n[bold green]总计: {len(files)} 个文件，{total_str}")

        except Exception as e:
            console.print(f"[red]✗ 列出文件失败: {e}")
            sys.exit(1)
        finally:
            client.close()

    def version(self):
        """显示版本信息"""
        from . import __version__
        from rich.console import Console
        from rich.panel import Panel

        console = Console()
        version_text = f"""[bold cyan]FlaxFile v{__version__}[/bold cyan]

[yellow]高性能文件传输工具[/yellow]
基于 ZMQ + asyncio 的异步单端口文件传输系统

特性:
  • [green]异步单端口架构[/green] - 简化配置，提升并发性能
  • [green]DEALER/ROUTER[/green] - 可靠传输，每个chunk都有ACK确认
  • [green]Rich 进度条[/green] - 美观的终端显示
  • [green]高性能[/green] - 上传/下载速度可达 1+ GB/s"""

        console.print(Panel(version_text, border_style="cyan", title="[bold]FlaxFile"))


class SyncCommands:
    """目录同步命令"""

    def __init__(self, config_obj):
        self._config = config_obj

    def push(
        self,
        local_dir: str,
        remote_dir: Optional[str] = None,
        server: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """
        上传本地目录到服务器

        Args:
            local_dir: 本地目录路径
            remote_dir: 远程目录名称（可选，默认使用本地目录名）
            server: 服务器名称（可选）
            password: 密码（可选）

        示例:
            flaxfile sync push /path/to/myproject
            flaxfile sync push /path/to/myproject backup_v1
            flaxfile sync push /path/to/myproject backup_v1 --server prod
        """
        from pathlib import Path
        from .sync import push_directory

        # 如果未指定 remote_dir，使用本地目录名
        if remote_dir is None:
            remote_dir = Path(local_dir).name

        # 获取服务器配置
        server_config = self._config.get_server(server)

        # 创建客户端
        client = FlaxFileClient(
            server_host=server_config['host'],
            port=server_config['port'],
            password=password,
        )

        try:
            result = push_directory(
                client=client,
                local_dir=local_dir,
                remote_dir=remote_dir,
                show_progress=True,
                password=password
            )
        finally:
            client.close()

    def pull(
        self,
        remote_dir: str,
        local_dir: Optional[str] = None,
        server: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """
        从服务器下载目录到本地

        Args:
            remote_dir: 远程目录名称
            local_dir: 本地目录路径（可选，默认使用远程目录名）
            server: 服务器名称（可选）
            password: 密码（可选）

        示例:
            flaxfile sync pull my_project
            flaxfile sync pull my_project /path/to/download
            flaxfile sync pull my_project /path/to/download --server prod
        """
        from .sync import pull_directory

        # 如果未指定 local_dir，使用远程目录名
        if local_dir is None:
            local_dir = f"./{remote_dir}"

        # 获取服务器配置
        server_config = self._config.get_server(server)

        # 创建客户端
        client = FlaxFileClient(
            server_host=server_config['host'],
            port=server_config['port'],
            password=password,
        )

        try:
            result = pull_directory(
                client=client,
                remote_dir=remote_dir,
                local_dir=local_dir,
                show_progress=True,
                password=password
            )
        finally:
            client.close()


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
