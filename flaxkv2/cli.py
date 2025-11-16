"""
FlaxKV2 命令行接口 (使用Fire实现)
"""

import os
import json
import fire
import psutil
import asyncio
from pathlib import Path
from typing import Optional, List
from rich import print
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from flaxkv2 import __version__, FlaxKV
from flaxkv2.utils.log import set_log_level
from flaxkv2.utils.file_transfer import FileTransferUtil
from flaxkv2.utils.async_file_transfer import upload_large_file_async, download_large_file_async, format_size
from flaxkv2.inspector.cli import InspectCommands
from flaxkv2.utils.config_loader import ConfigLoader, save_sample_config

console = Console()


class FlaxKV2CLI:
    """FlaxKV2命令行工具"""

    def __init__(self):
        """初始化CLI"""
        self.inspect = InspectCommands()
        self.config_loader = None
        self._load_config()

    def _load_config(self):
        """加载配置文件（如果存在）"""
        try:
            self.config_loader = ConfigLoader()
        except Exception as e:
            # 配置文件加载失败时继续运行，但记录警告
            console.print(f"[yellow]Warning:[/yellow] Failed to load config: {e}")
            self.config_loader = None

    def _resolve_server_address(self, server: str) -> str:
        """
        解析服务器地址

        支持格式：
        - @server_name: 从配置文件中查找名为 server_name 的服务器
        - host:port: 直接使用地址

        Args:
            server: 服务器地址或名称

        Returns:
            解析后的服务器地址 (host:port)
        """
        if server.startswith('@'):
            # 从配置文件查找服务器
            server_name = server[1:]
            if self.config_loader:
                address = self.config_loader.get_server_address(server_name)
                if address:
                    return address
                else:
                    console.print(f"[yellow]Warning:[/yellow] Server '{server_name}' not found in config, using as-is")
                    return server
            else:
                console.print(f"[yellow]Warning:[/yellow] No config file loaded, cannot resolve @{server_name}")
                return server
        return server

    def _merge_config(self, section: str, profile: str, **kwargs) -> dict:
        """
        合并配置文件和命令行参数

        优先级：命令行参数 > 配置文件

        Args:
            section: 配置节名称 ('server' 或 'client')
            profile: profile 名称
            **kwargs: 命令行参数

        Returns:
            合并后的配置字典
        """
        # 从配置文件读取
        config = {}
        if self.config_loader:
            if section == 'server':
                config = self.config_loader.get_server_config(profile)
            elif section == 'client':
                config = self.config_loader.get_client_config(profile)

            # 合并默认配置
            defaults = self.config_loader.get_defaults()
            for key, value in defaults.items():
                if key not in config:
                    config[key] = value

        # 命令行参数覆盖配置文件
        for key, value in kwargs.items():
            if value is not None:
                config[key] = value

        return config

    def version(self):
        """显示版本信息"""
        return f"FlaxKV2 {__version__}"

    def run(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        data_dir: Optional[str] = None,
        workers: Optional[int] = None,
        log_level: Optional[str] = None,
        profile: str = 'default',
        enable_encryption: Optional[bool] = None,
        password: Optional[str] = None,
        derive_from_password: Optional[bool] = None,
        enable_compression: Optional[bool] = None,
        performance_profile: Optional[str] = None,
    ):
        """
        运行 FlaxKV2 ZeroMQ 服务器

        Args:
            host: 监听主机名
            port: 监听端口
            data_dir: 数据目录
            workers: 工作线程数
            log_level: 日志级别 (DEBUG, INFO, WARNING, ERROR)
            profile: 配置 profile 名称 (默认: default)
            enable_encryption: 启用 CurveZMQ 加密
            password: 服务器密码（用于加密）
            derive_from_password: 从密码派生密钥（推荐 True，默认 True）
            enable_compression: 启用 LZ4 压缩
            performance_profile: 性能配置文件名称

        示例:
            # 使用默认配置或配置文件中的默认值
            flaxkv2 run

            # 使用配置文件中的 production profile
            flaxkv2 run --profile production

            # 命令行参数覆盖配置文件
            flaxkv2 run --profile production --port 6666

            # 启用加密
            flaxkv2 run --enable-encryption --password mypassword
        """
        from flaxkv2.server.zmq_server import FlaxKVServer

        # 合并配置
        config = self._merge_config(
            'server',
            profile,
            host=host,
            port=port,
            data_dir=data_dir,
            workers=workers,
            log_level=log_level,
            enable_encryption=enable_encryption,
            password=password,
            derive_from_password=derive_from_password,
            enable_compression=enable_compression,
            performance_profile=performance_profile,
        )

        # 提取配置值（使用默认值作为后备）
        final_host = config.get('host', '127.0.0.1')
        final_port = config.get('port', 5555)
        final_data_dir = config.get('data_dir', '.')
        final_workers = config.get('workers', 4)
        final_log_level = config.get('log_level', 'INFO')
        final_enable_encryption = config.get('enable_encryption', False)
        final_password = config.get('password')
        final_derive_from_password = config.get('derive_from_password', True)
        final_enable_compression = config.get('enable_compression', False)
        final_performance_profile = config.get('performance_profile')

        # 设置日志级别
        set_log_level(final_log_level.upper())

        # 显示服务器信息
        print(f"\n[bold green]FlaxKV2 ZeroMQ 服务器启动中...[/bold green]\n")
        if profile != 'default':
            print(f"配置 Profile: [bold magenta]{profile}[/bold magenta]")
        print(f"服务器地址:   [bold blue]{final_host}:{final_port}[/bold blue]")
        print(f"数据目录:     [bold blue]{final_data_dir}[/bold blue]")
        print(f"工作线程:     [bold blue]{final_workers}[/bold blue]")
        print(f"日志级别:     [bold blue]{final_log_level}[/bold blue]")
        if final_enable_encryption:
            print(f"加密:         [bold green]启用[/bold green]")
        if final_enable_compression:
            print(f"压缩:         [bold green]启用[/bold green]")
        if final_performance_profile:
            print(f"性能配置:     [bold blue]{final_performance_profile}[/bold blue]")
        print(f"\n[dim]使用 Ctrl+C 停止服务器[/dim]\n")

        # 创建服务器参数
        server_kwargs = {
            'host': final_host,
            'port': final_port,
            'data_dir': final_data_dir,
            'max_workers': final_workers,
        }

        # 添加可选参数
        if final_enable_encryption:
            server_kwargs['enable_encryption'] = True
            if final_password:
                server_kwargs['password'] = final_password
                server_kwargs['derive_from_password'] = final_derive_from_password

        if final_enable_compression:
            server_kwargs['enable_compression'] = True

        # 创建并运行服务器
        server = FlaxKVServer(**server_kwargs)
        server.run()

    def set(
        self,
        path,
        key: Optional[str] = None,
        server: Optional[str] = None,
        db_name: Optional[str] = None,
        profile: str = 'default',
        chunked: Optional[bool] = None,
        chunk_size: int = 10 * 1024 * 1024,
        serial: bool = False,
        max_workers: int = 1,
    ):
        """
        上传文件或文件夹到远程 FlaxKV 服务器

        Args:
            path: 要上传的文件或文件夹路径
            key: 存储的键名（默认使用路径的 basename）
            server: 远程服务器地址 (host:port) 或服务器名称 (@name)
            db_name: 数据库名称
            profile: 配置 profile 名称 (默认: default)
            chunked: 是否使用分块上传（None=自动检测，大于100MB自动分块）
            chunk_size: 分块大小（字节），默认 10MB
            serial: (已废弃，保留以兼容旧版本) 现在总是使用异步传输
            max_workers: 异步并发数（默认 16）

        示例:
            # 使用配置文件中的默认服务器（大文件自动异步传输）
            flaxkv2 set /path/to/file.txt

            # 使用配置文件中定义的服务器
            flaxkv2 set /path/to/file.txt --server @production

            # 直接指定服务器地址
            flaxkv2 set /path/to/file.txt --server 192.168.1.100:5555

            # 指定并发数
            flaxkv2 set /path/to/large_file.mp4 --max-workers 8

            # 指定分块大小（20MB）
            flaxkv2 set /path/to/large_file.mp4 --chunk-size 20971520
        """
        # 合并配置
        config = self._merge_config(
            'client',
            profile,
            server=server,
            db_name=db_name,
        )

        # 提取配置值
        final_server = config.get('server', '127.0.0.1:5555')
        final_db_name = config.get('db_name', 'file_storage')

        # 解析服务器地址
        final_server = self._resolve_server_address(final_server)
        path = Path(path)

        # 检查路径是否存在
        if not path.exists():
            console.print(f"[bold red]错误:[/bold red] 路径不存在: {path}")
            return

        # 确定键名
        if key is None:
            key = path.name

        # 自动检测是否使用分块上传（文件 > 100MB）
        use_chunked = chunked
        if use_chunked is None and path.is_file():
            file_size = path.stat().st_size
            use_chunked = file_size > 100 * 1024 * 1024  # 100MB

        try:
            # 连接到远程服务器
            console.print(f"正在连接到 {final_server}...")

            # 从配置中获取连接参数
            timeout = config.get('timeout', 5) * 1000  # 转换为毫秒
            connect_timeout = config.get('connect_timeout', 5) * 1000  # 转换为毫秒
            enable_encryption = config.get('enable_encryption', False)
            password = config.get('password')
            derive_from_password = config.get('derive_from_password', True)

            # 创建客户端连接，传递配置参数
            db = FlaxKV(
                final_db_name,
                final_server,
                backend='remote',
                timeout=timeout,
                enable_encryption=enable_encryption,
                password=password,
                derive_from_password=derive_from_password
            )

            # 处理目录：先打包，然后根据大小决定是否分块
            if path.is_dir():
                import tempfile
                import tarfile

                console.print(f"[yellow]检测到目录，正在打包...[/yellow]")

                # 创建临时 tar.gz 文件
                temp_file = tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False)
                temp_path = temp_file.name
                temp_file.close()

                try:
                    # 打包目录
                    with tarfile.open(temp_path, 'w:gz') as tar:
                        tar.add(path, arcname=path.name)

                    # 检查打包后的文件大小
                    packed_size = Path(temp_path).stat().st_size
                    console.print(f"   打包完成: {format_size(packed_size)}")

                    # 根据大小决定是否使用分块上传
                    if packed_size > 100 * 1024 * 1024:  # > 100MB 使用分块
                        console.print(f"[yellow]打包文件较大，使用异步分块上传（{max_workers} 并发）[/yellow]\n")

                        # 使用异步上传
                        server_url = f"tcp://{final_server}" if not final_server.startswith('tcp://') else final_server
                        metadata = asyncio.run(upload_large_file_async(
                            final_db_name, server_url, key, temp_path,
                            chunk_size=chunk_size,
                            max_concurrency=max_workers,
                            show_progress=True,
                            verify=True,
                            password=password,
                            enable_encryption=enable_encryption
                        ))

                        # 更新元数据，标记为 chunked_folder
                        metadata['type'] = 'chunked_folder'
                        metadata['original_name'] = path.name
                        db[f"{key}:meta"] = metadata

                        console.print(f"[bold green]✓[/bold green] 目录上传成功!")
                        console.print(f"  类型: chunked_folder")
                        console.print(f"  目录名: [bold blue]{path.name}[/bold blue]")
                        console.print(f"  大小: {format_size(metadata['size'])}")
                        console.print(f"  分块: {metadata['chunks']} 个")
                        console.print(f"  并发数: {max_workers=}")
                        console.print(f"  服务器: {final_server}")

                    else:
                        # 小于 100MB，传统方式上传
                        console.print(f"[yellow]使用传统方式上传[/yellow]")

                        with open(temp_path, 'rb') as f:
                            content = f.read()

                        metadata = {
                            'type': 'folder',
                            'name': path.name,
                            'size': packed_size,
                        }

                        db[key] = content
                        db[f"{key}:meta"] = json.dumps(metadata)

                        console.print(f"[bold green]✓[/bold green] 目录上传成功!")
                        console.print(f"  类型: folder")
                        console.print(f"  目录名: [bold blue]{path.name}[/bold blue]")
                        console.print(f"  大小: {format_size(packed_size)}")
                        console.print(f"  服务器: {final_server}")

                finally:
                    # 删除临时文件
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)

            elif use_chunked and path.is_file():
                # 使用异步分块上传
                console.print(f"[yellow]使用异步分块上传（{max_workers=} 并发）[/yellow]\n")

                server_url = f"tcp://{final_server}" if not final_server.startswith('tcp://') else final_server
                metadata = asyncio.run(upload_large_file_async(
                    final_db_name, server_url, key, str(path),
                    chunk_size=chunk_size,
                    max_concurrency=max_workers,
                    show_progress=True,
                    verify=True,
                    password=password,
                    enable_encryption=enable_encryption
                ))

                console.print(f"[bold green]✓[/bold green] 上传成功!")
                console.print(f"  类型: chunked_file")
                console.print(f"  键名: [bold blue]{key}[/bold blue]")
                console.print(f"  大小: {format_size(metadata['size'])}")
                console.print(f"  分块: {metadata['chunks']} 个")
                console.print(f"  并发数: {max_workers}")
                console.print(f"  服务器: {final_server}")

            else:
                # 使用传统方式上传（适合小文件和文件夹）
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=console
                ) as progress:
                    # 打包文件/文件夹
                    task = progress.add_task(f"正在打包 {path.name}...", total=None)
                    content, metadata = FileTransferUtil.pack(str(path))
                    progress.update(task, completed=True)

                    # 上传数据
                    task = progress.add_task(f"正在上传 ({format_size(len(content))})...", total=None)
                    db[key] = content
                    db[f"{key}:meta"] = json.dumps(metadata)
                    progress.update(task, completed=True)

                console.print(f"\n[bold green]✓[/bold green] 上传成功!")
                console.print(f"  类型: {metadata['type']}")
                console.print(f"  键名: [bold blue]{key}[/bold blue]")
                console.print(f"  大小: {format_size(metadata['size'])}")
                console.print(f"  服务器: {final_server}")

            # 关闭连接
            db.close()

        except Exception as e:
            console.print(f"\n[bold red]✗ 上传失败:[/bold red] {str(e)}")

    def get(
        self,
        key,
        output: Optional[str] = None,
        server: Optional[str] = None,
        db_name: Optional[str] = None,
        profile: str = 'default',
        serial: bool = False,
        max_workers: int = 5,
    ):
        """
        从远程 FlaxKV 服务器下载文件或文件夹

        Args:
            key: 要下载的键名
            output: 保存路径（默认使用当前目录）
            server: 远程服务器地址 (host:port) 或服务器名称 (@name)
            db_name: 数据库名称
            profile: 配置 profile 名称 (默认: default)
            serial: (已废弃，保留以兼容旧版本) 现在总是使用异步传输
            max_workers: 异步并发数（默认 5）

        示例:
            # 使用配置文件中的默认服务器（大文件自动异步传输）
            flaxkv2 get my_file

            # 使用配置文件中定义的服务器
            flaxkv2 get my_file --server @production

            # 直接指定服务器地址
            flaxkv2 get my_file --server 192.168.1.100:5555

            # 指定并发数
            flaxkv2 get my_file --max-workers 8
        """
        # 合并配置
        config = self._merge_config(
            'client',
            profile,
            server=server,
            db_name=db_name,
        )

        # 提取配置值
        final_server = config.get('server', '127.0.0.1:5555')
        final_db_name = config.get('db_name', 'file_storage')

        # 解析服务器地址
        final_server = self._resolve_server_address(final_server)
        # 默认输出路径为当前目录
        if output is None:
            output = '.'

        output_path = Path(output)

        try:
            # 连接到远程服务器
            console.print(f"正在连接到 {final_server}...")

            # 从配置中获取连接参数
            timeout = config.get('timeout', 5) * 1000  # 转换为毫秒
            connect_timeout = config.get('connect_timeout', 5) * 1000  # 转换为毫秒
            enable_encryption = config.get('enable_encryption', False)
            password = config.get('password')
            derive_from_password = config.get('derive_from_password', True)

            # 创建客户端连接，传递配置参数
            db = FlaxKV(
                final_db_name,
                final_server,
                backend='remote',
                timeout=timeout,
                enable_encryption=enable_encryption,
                password=password,
                derive_from_password=derive_from_password
            )

            # 检查元数据，判断文件类型
            metadata = db.get(f"{key}:meta")

            if metadata is None:
                # 尝试检查键是否存在（非通过 flaxkv set 上传的文件）
                if key not in db:
                    console.print(f"[bold red]错误:[/bold red] 键不存在: {key}")
                    db.close()
                    return

                console.print(f"[bold red]错误:[/bold red] 元数据不存在，该键可能不是通过 'flaxkv set' 上传的")
                db.close()
                return

            # 处理元数据格式（可能是 JSON 字符串或字典）
            if isinstance(metadata, str):
                metadata = json.loads(metadata)

            # 判断文件类型
            file_type = metadata.get('type')
            is_chunked_file = file_type == 'chunked_file'
            is_chunked_folder = file_type == 'chunked_folder'

            if is_chunked_folder:
                # 下载分块打包的目录
                import tempfile
                import tarfile

                console.print(f"[yellow]检测到分块目录，正在下载...[/yellow]\n")

                # 创建临时文件用于存储下载的 tar.gz
                temp_file = tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False)
                temp_path = temp_file.name
                temp_file.close()

                try:
                    # 使用异步分块下载到临时文件
                    server_url = f"tcp://{final_server}" if not final_server.startswith('tcp://') else final_server
                    asyncio.run(download_large_file_async(
                        final_db_name, server_url, key, temp_path,
                        max_concurrency=max_workers,
                        show_progress=True,
                        verify=True,
                        password=password,
                        enable_encryption=enable_encryption,
                        connect_timeout=connect_timeout
                    ))

                    # 解包到目标目录
                    console.print(f"\n[yellow]正在解包...[/yellow]")
                    output_path.mkdir(parents=True, exist_ok=True)

                    with tarfile.open(temp_path, 'r:gz') as tar:
                        tar.extractall(output_path)

                    console.print(f"[bold green]✓[/bold green] 目录下载成功!")
                    console.print(f"  目录名: {metadata.get('original_name', metadata.get('filename', 'unknown'))}")
                    console.print(f"  大小: {format_size(metadata['size'])}")
                    console.print(f"  分块: {metadata['chunks']} 个")
                    console.print(f"  并发数: {max_workers}")
                    console.print(f"  保存路径: {output_path}")

                finally:
                    # 删除临时文件
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)

            elif is_chunked_file:
                # 使用异步分块下载
                console.print(f"[yellow]检测到分块文件，使用异步下载（{max_workers} 并发）[/yellow]\n")

                server_url = f"tcp://{final_server}" if not final_server.startswith('tcp://') else final_server
                metadata = asyncio.run(download_large_file_async(
                    final_db_name, server_url, key, str(output_path),
                    max_concurrency=max_workers,
                    show_progress=True,
                    verify=True,
                    password=password,
                    enable_encryption=enable_encryption,
                    connect_timeout=connect_timeout
                ))

                console.print(f"[bold green]✓[/bold green] 下载成功!")
                console.print(f"  文件名: {metadata['filename']}")
                console.print(f"  大小: {format_size(metadata['size'])}")
                console.print(f"  分块: {metadata['chunks']} 个")
                console.print(f"  并发数: {max_workers}")

            else:
                # 使用传统方式下载
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=console
                ) as progress:
                    # 下载数据
                    task = progress.add_task(f"正在下载...", total=None)

                    # metadata 已在前面统一处理为字典格式
                    content = db[key]
                    progress.update(task, completed=True)

                    # 解包文件/文件夹
                    task = progress.add_task(f"正在解包...", total=None)
                    FileTransferUtil.unpack(content, metadata, str(output_path))
                    progress.update(task, completed=True)

                # 确定实际保存路径
                if output_path.is_dir():
                    actual_path = output_path / metadata['name']
                else:
                    actual_path = output_path

                console.print(f"\n[bold green]✓[/bold green] 下载成功!")
                console.print(f"  类型: {metadata['type']}")
                console.print(f"  键名: [bold blue]{key}[/bold blue]")
                console.print(f"  保存路径: {actual_path}")
                console.print(f"  大小: {format_size(metadata['size'])}")

            # 关闭连接
            db.close()

        except Exception as e:
            console.print(f"\n[bold red]✗ 下载失败:[/bold red] {str(e)}")

    def list(
        self,
        server: Optional[str] = None,
        db_name: Optional[str] = None,
        profile: str = 'default',
    ):
        """
        列出远程 FlaxKV 服务器上所有可用的文件和文件夹

        Args:
            server: 远程服务器地址 (host:port) 或服务器名称 (@name)
            db_name: 数据库名称
            profile: 配置 profile 名称 (默认: default)

        示例:
            # 使用配置文件中的默认服务器
            flaxkv2 list

            # 使用配置文件中定义的服务器
            flaxkv2 list --server @production

            # 直接指定服务器地址
            flaxkv2 list --server 192.168.1.100:5555
        """
        # 合并配置
        config = self._merge_config(
            'client',
            profile,
            server=server,
            db_name=db_name,
        )

        # 提取配置值
        final_server = config.get('server', '127.0.0.1:5555')
        final_db_name = config.get('db_name', 'file_storage')

        # 解析服务器地址
        final_server = self._resolve_server_address(final_server)

        # 从配置中获取连接参数
        timeout = config.get('timeout', 5) * 1000  # 转换为毫秒
        connect_timeout = config.get('connect_timeout', 5) * 1000  # 转换为毫秒
        enable_encryption = config.get('enable_encryption', False)
        password = config.get('password')
        derive_from_password = config.get('derive_from_password', True)

        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                # 连接到远程服务器
                task = progress.add_task(f"正在连接到 {final_server}...", total=None)
                db = FlaxKV(
                    final_db_name,
                    final_server,
                    backend='remote',
                    timeout=timeout,
                    enable_encryption=enable_encryption,
                    password=password,
                    derive_from_password=derive_from_password
                )
                progress.update(task, completed=True)

                # 获取所有键
                task = progress.add_task("正在获取文件列表...", total=None)
                all_keys = list(db.keys())
                progress.update(task, completed=True)

                # 关闭连接
                db.close()

            # 过滤出文件键（排除元数据键）
            file_keys = [k for k in all_keys if not k.endswith(':meta')]

            if not file_keys:
                console.print(f"\n[yellow]服务器上没有文件[/yellow]")
                console.print(f"服务器: {final_server}")
                console.print(f"数据库: {final_db_name}")
                return

            # 重新连接获取详细信息
            db = FlaxKV(
                final_db_name,
                final_server,
                backend='remote',
                timeout=timeout,
                enable_encryption=enable_encryption,
                password=password,
                derive_from_password=derive_from_password
            )

            console.print(f"\n[bold]服务器上的文件列表[/bold]")
            console.print(f"服务器: [blue]{final_server}[/blue]")
            console.print(f"数据库: [blue]{final_db_name}[/blue]")
            console.print(f"\n共 {len(file_keys)} 个文件/文件夹:\n")

            # 显示每个文件的详细信息
            from rich.table import Table
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("键名", style="cyan", no_wrap=True)
            table.add_column("类型", style="green", no_wrap=True)
            table.add_column("大小", style="yellow", no_wrap=True)
            table.add_column("原始名称", style="white", no_wrap=True)

            for key in sorted(file_keys):
                try:
                    meta_key = f"{key}:meta"
                    if meta_key in db:
                        metadata = db[meta_key]
                        # 处理元数据格式（可能是 JSON 字符串或字典）
                        if isinstance(metadata, str):
                            metadata = json.loads(metadata)
                        table.add_row(
                            key,
                            metadata.get('type', 'unknown'),
                            format_size(metadata.get('size', 0)),
                            metadata.get('name', '-')
                        )
                    else:
                        # 没有元数据，可能不是通过 flaxkv set 上传的
                        table.add_row(
                            key,
                            "[dim]未知[/dim]",
                            "[dim]-[/dim]",
                            "[dim]非 flaxkv 文件[/dim]"
                        )
                except Exception:
                    table.add_row(
                        key,
                        "[red]错误[/red]",
                        "[red]-[/red]",
                        "[red]无法读取元数据[/red]"
                    )

            console.print(table)
            db.close()

        except Exception as e:
            console.print(f"\n[bold red]✗ 获取文件列表失败:[/bold red] {str(e)}")

    def web(
        self,
        db_name: str,
        path: str = '.',
        backend: str = 'auto',
        host: str = '127.0.0.1',
        port: int = 8080,
        debug: bool = False
    ):
        """
        启动 FlaxKV2 Inspector Web UI

        Args:
            db_name: 数据库名称
            path: 数据库路径（本地路径或远程地址）
            backend: 后端类型 ('local', 'remote', 'auto')，默认 'auto'
            host: 监听主机名（默认: 127.0.0.1）
            port: 监听端口（默认: 8080）
            debug: 调试模式（默认: False）

        示例:
            flaxkv2 web mydb
            flaxkv2 web mydb --path /data/db --port 8080
            flaxkv2 web mydb --path 127.0.0.1:5555 --backend remote
        """
        try:
            from flaxkv2.inspector.web import start_web_server
            start_web_server(
                db_name=db_name,
                path=path,
                backend=backend,
                host=host,
                port=port,
                debug=debug
            )
        except ImportError as e:
            # 检查是否真的是Flask相关的导入错误
            error_msg = str(e).lower()
            if 'flask' in error_msg or 'flask_cors' in error_msg or 'cors' in error_msg:
                console.print(f"[bold red]错误:[/bold red] Flask 未安装")
                console.print("请运行: [bold blue]pip install flask flask-cors[/bold blue]")
            else:
                console.print(f"[bold red]导入错误:[/bold red] {str(e)}")
                if debug:
                    import traceback
                    traceback.print_exc()
        except Exception as e:
            console.print(f"[bold red]错误:[/bold red] {str(e)}")
            if debug:
                import traceback
                traceback.print_exc()

    def config(self, action: str = 'show', path: Optional[str] = None):
        """
        管理 FlaxKV2 配置文件

        Args:
            action: 操作类型
                - 'show': 显示当前配置
                - 'init': 生成示例配置文件
                - 'path': 显示配置文件路径
                - 'servers': 列出所有定义的服务器
                - 'profiles': 列出所有 profiles
            path: 配置文件路径（用于 init 操作）

        示例:
            # 显示当前配置
            flaxkv2 config show

            # 生成示例配置文件到当前目录
            flaxkv2 config init

            # 生成示例配置文件到指定路径
            flaxkv2 config init --path ~/.flaxkv.toml

            # 显示配置文件路径
            flaxkv2 config path

            # 列出所有定义的服务器
            flaxkv2 config servers

            # 列出所有 profiles
            flaxkv2 config profiles
        """
        if action == 'init':
            # 生成示例配置文件
            try:
                config_path = save_sample_config(path)
                console.print(f"[bold green]✓[/bold green] 示例配置文件已生成:")
                console.print(f"  路径: [bold blue]{config_path}[/bold blue]")
                console.print(f"\n请编辑配置文件以适应您的环境")
            except FileExistsError:
                console.print(f"[bold red]错误:[/bold red] 配置文件已存在")
                console.print(f"如需重新生成，请先删除现有配置文件")
            except Exception as e:
                console.print(f"[bold red]错误:[/bold red] {str(e)}")

        elif action == 'show':
            # 显示当前配置
            if self.config_loader is None or not self.config_loader.config:
                console.print("[yellow]未找到配置文件[/yellow]")
                console.print("使用 [bold blue]flaxkv2 config init[/bold blue] 生成示例配置")
                return

            console.print("[bold]当前配置:[/bold]\n")

            # 显示服务器配置
            server_config = self.config_loader.get_server_config()
            if server_config:
                console.print("[bold cyan]服务器配置:[/bold cyan]")
                for key, value in server_config.items():
                    if key != 'profiles':
                        console.print(f"  {key}: {value}")

            # 显示客户端配置
            client_config = self.config_loader.get_client_config()
            if client_config:
                console.print("\n[bold cyan]客户端配置:[/bold cyan]")
                for key, value in client_config.items():
                    if key != 'profiles':
                        console.print(f"  {key}: {value}")

            # 显示默认配置
            defaults = self.config_loader.get_defaults()
            if defaults:
                console.print("\n[bold cyan]默认配置:[/bold cyan]")
                for key, value in defaults.items():
                    console.print(f"  {key}: {value}")

        elif action == 'path':
            # 显示配置文件路径
            if self.config_loader is None:
                console.print("[yellow]未找到配置文件[/yellow]")
            else:
                config_file = self.config_loader._find_config_file()
                if config_file:
                    console.print(f"配置文件路径: [bold blue]{config_file}[/bold blue]")
                else:
                    console.print("[yellow]未找到配置文件[/yellow]")

        elif action == 'servers':
            # 列出所有定义的服务器
            if self.config_loader is None or not self.config_loader.config:
                console.print("[yellow]未找到配置文件[/yellow]")
                return

            servers = self.config_loader.get_servers()
            if not servers:
                console.print("[yellow]未定义任何服务器[/yellow]")
                return

            console.print("[bold]定义的服务器:[/bold]\n")
            from rich.table import Table
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("名称", style="cyan", no_wrap=True)
            table.add_column("地址", style="green")
            table.add_column("密码", style="yellow")

            for name, config in servers.items():
                host = config.get('host', 'N/A')
                port = config.get('port', 'N/A')
                password = '***' if config.get('password') else '-'
                table.add_row(name, f"{host}:{port}", password)

            console.print(table)
            console.print(f"\n使用 [bold blue]--server @name[/bold blue] 引用服务器")

        elif action == 'profiles':
            # 列出所有 profiles
            if self.config_loader is None or not self.config_loader.config:
                console.print("[yellow]未找到配置文件[/yellow]")
                return

            server_profiles = self.config_loader.list_profiles('server')
            client_profiles = self.config_loader.list_profiles('client')

            if not server_profiles and not client_profiles:
                console.print("[yellow]未定义任何 profile[/yellow]")
                return

            if server_profiles:
                console.print("[bold cyan]服务器 Profiles:[/bold cyan]")
                for profile in server_profiles:
                    console.print(f"  • {profile}")

            if client_profiles:
                console.print("\n[bold cyan]客户端 Profiles:[/bold cyan]")
                for profile in client_profiles:
                    console.print(f"  • {profile}")

            console.print(f"\n使用 [bold blue]--profile name[/bold blue] 选择 profile")

        else:
            console.print(f"[bold red]错误:[/bold red] 未知的操作: {action}")
            console.print("可用操作: show, init, path, servers, profiles")

    def _find_pids_by_port(self, port: int) -> List[dict]:
        """
        根据端口查找进程 PID（跨平台）

        Returns:
            [{'pid': int, 'name': str, 'cmdline': str}, ...]
        """
        import platform
        import subprocess

        pids_info = []
        system = platform.system()

        try:
            if system == 'Darwin' or system == 'Linux':
                # macOS 和 Linux: 使用 lsof
                result = subprocess.run(
                    ['lsof', '-ti', f':{port}'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0:
                    pids = [int(pid.strip()) for pid in result.stdout.strip().split('\n') if pid.strip()]
                    for pid in pids:
                        try:
                            proc = psutil.Process(pid)
                            pids_info.append({
                                'pid': pid,
                                'name': proc.name(),
                                'cmdline': ' '.join(proc.cmdline()[:3]) if proc.cmdline() else proc.name(),
                            })
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass

            elif system == 'Windows':
                # Windows: 使用 netstat
                result = subprocess.run(
                    ['netstat', '-ano'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0:
                    for line in result.stdout.split('\n'):
                        if f':{port}' in line and 'LISTENING' in line:
                            parts = line.split()
                            if parts:
                                try:
                                    pid = int(parts[-1])
                                    proc = psutil.Process(pid)
                                    pids_info.append({
                                        'pid': pid,
                                        'name': proc.name(),
                                        'cmdline': ' '.join(proc.cmdline()[:3]) if proc.cmdline() else proc.name(),
                                    })
                                except (ValueError, psutil.NoSuchProcess, psutil.AccessDenied):
                                    pass

        except subprocess.TimeoutExpired:
            pass
        except FileNotFoundError:
            # lsof 或 netstat 命令不存在，使用 psutil 后备方案
            try:
                for conn in psutil.net_connections(kind='inet'):
                    if conn.status == 'LISTEN' and conn.laddr.port == port:
                        try:
                            proc = psutil.Process(conn.pid)
                            pids_info.append({
                                'pid': conn.pid,
                                'name': proc.name(),
                                'cmdline': ' '.join(proc.cmdline()[:3]) if proc.cmdline() else proc.name(),
                            })
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
            except (PermissionError, psutil.AccessDenied):
                pass

        return pids_info

    def kill(self, *ports):
        """
        根据端口号 kill 进程（跨平台支持）

        Args:
            *ports: 一个或多个端口号

        示例:
            # kill 单个端口
            flaxkv2 kill 5555

            # kill 多个端口
            flaxkv2 kill 5555 8080 3000

            # kill FlaxKV 服务器（默认端口 5555）
            flaxkv2 kill 5555
        """
        if not ports:
            console.print("[bold red]错误:[/bold red] 请指定至少一个端口号")
            console.print("用法: flaxkv2 kill <port1> [port2] ...]")
            return

        # 转换端口为整数
        port_list = []
        for port in ports:
            try:
                port_num = int(port)
                if not (1 <= port_num <= 65535):
                    console.print(f"[bold red]错误:[/bold red] 无效的端口号: {port} (必须在 1-65535 之间)")
                    return
                port_list.append(port_num)
            except ValueError:
                console.print(f"[bold red]错误:[/bold red] 无效的端口号: {port}")
                return

        console.print(f"\n[bold]正在查找监听端口的进程...[/bold]")

        # 记录每个端口的处理结果
        results = []

        for port in port_list:
            try:
                # 查找监听该端口的进程
                listening_processes = self._find_pids_by_port(port)

                if not listening_processes:
                    results.append({
                        'port': port,
                        'status': 'not_found',
                        'message': '未找到监听该端口的进程'
                    })
                    continue

                # kill 所有监听该端口的进程
                killed_processes = []
                failed_processes = []

                for proc_info in listening_processes:
                    try:
                        proc = psutil.Process(proc_info['pid'])
                        proc.kill()  # 强制 kill
                        try:
                            proc.wait(timeout=3)  # 等待进程退出
                        except psutil.TimeoutExpired:
                            # 进程未在超时时间内退出，但 kill 信号已发送
                            pass
                        killed_processes.append(proc_info)
                    except psutil.NoSuchProcess:
                        # 进程已经不存在了
                        killed_processes.append(proc_info)
                    except psutil.AccessDenied:
                        failed_processes.append({
                            **proc_info,
                            'reason': '权限不足'
                        })
                    except Exception as e:
                        failed_processes.append({
                            **proc_info,
                            'reason': f'{type(e).__name__}: {str(e)}'
                        })

                if killed_processes:
                    results.append({
                        'port': port,
                        'status': 'success',
                        'processes': killed_processes,
                        'failed': failed_processes
                    })
                else:
                    results.append({
                        'port': port,
                        'status': 'failed',
                        'failed': failed_processes
                    })

            except Exception as e:
                results.append({
                    'port': port,
                    'status': 'error',
                    'message': f'发生错误: {type(e).__name__}: {str(e)}'
                })

        # 显示结果
        console.print()
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("端口", style="cyan", no_wrap=True)
        table.add_column("状态", style="yellow")
        table.add_column("详情", style="white")

        for result in results:
            port = result['port']
            status = result['status']

            if status == 'success':
                killed = result['processes']
                failed = result.get('failed', [])

                if killed:
                    killed_info = '\n'.join([
                        f"✓ PID {p['pid']}: {p['name']}"
                        for p in killed
                    ])
                    if failed:
                        failed_info = '\n'.join([
                            f"✗ PID {p['pid']}: {p['name']} ({p['reason']})"
                            for p in failed
                        ])
                        info = killed_info + '\n' + failed_info
                        table.add_row(str(port), "[green]部分成功[/green]", info)
                    else:
                        table.add_row(str(port), "[green]成功[/green]", killed_info)
                else:
                    table.add_row(str(port), "[red]失败[/red]", "所有进程都无法 kill")

            elif status == 'failed':
                failed_info = '\n'.join([
                    f"✗ PID {p['pid']}: {p['name']} ({p['reason']})"
                    for p in result['failed']
                ])
                table.add_row(str(port), "[red]失败[/red]", failed_info)

            elif status == 'not_found':
                table.add_row(str(port), "[yellow]未找到[/yellow]", result['message'])

            elif status == 'permission_denied':
                table.add_row(str(port), "[red]权限不足[/red]", result['message'])

            else:  # error
                table.add_row(str(port), "[red]错误[/red]", result['message'])

        console.print(table)
        console.print()


def main():
    """主函数"""
    fire.Fire(FlaxKV2CLI)


if __name__ == '__main__':
    main() 