"""
FlaxKV2 命令行接口 (使用Fire实现)
"""

import os
import json
import fire
from pathlib import Path
from rich import print
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from flaxkv2 import __version__, FlaxKV
from flaxkv2.utils.log import set_log_level
from flaxkv2.utils.file_transfer import FileTransferUtil, format_size

console = Console()


class FlaxKV2CLI:
    """FlaxKV2命令行工具"""

    def __init__(self):
        """初始化CLI"""
        pass

    def version(self):
        """显示版本信息"""
        return f"FlaxKV2 {__version__}"

    def run(self, host='127.0.0.1', port=5555, data_dir='.', workers=4, log_level='INFO'):
        """
        运行 FlaxKV2 ZeroMQ 服务器
        
        Args:
            host: 监听主机名 (默认: 127.0.0.1)
            port: 监听端口 (默认: 5555)
            data_dir: 数据目录 (默认: 当前目录)
            workers: 工作线程数 (默认: 4)
            log_level: 日志级别 (DEBUG, INFO, WARNING, ERROR)
        """
        from flaxkv2.server.zmq_server import FlaxKVServer
        
        # 设置日志级别
        set_log_level(log_level.upper())
        
        # 显示服务器信息
        print(f"\n[bold green]FlaxKV2 ZeroMQ 服务器启动中...[/bold green]\n")
        print(f"服务器地址: [bold blue]{host}:{port}[/bold blue]")
        print(f"数据目录:   [bold blue]{data_dir}[/bold blue]")
        print(f"工作线程:   [bold blue]{workers}[/bold blue]")
        print(f"日志级别:   [bold blue]{log_level}[/bold blue]\n")
        print(f"[dim]使用 Ctrl+C 停止服务器[/dim]\n")
        
        # 创建并运行服务器
        server = FlaxKVServer(
            host=host,
            port=port,
            data_dir=data_dir,
            max_workers=workers
        )
        
        server.run()

    def set(self, path, key=None, server='127.0.0.1:5555', db_name='file_storage'):
        """
        上传文件或文件夹到远程 FlaxKV 服务器

        Args:
            path: 要上传的文件或文件夹路径
            key: 存储的键名（默认使用路径的 basename）
            server: 远程服务器地址，格式为 host:port (默认: 127.0.0.1:5555)
            db_name: 数据库名称 (默认: file_storage)

        示例:
            flaxkv set /path/to/file.txt --server 192.168.1.100:5555
            flaxkv set /path/to/folder --key my_folder --server 192.168.1.100:5555
        """
        path = Path(path)

        # 检查路径是否存在
        if not path.exists():
            console.print(f"[bold red]错误:[/bold red] 路径不存在: {path}")
            return

        # 确定键名
        if key is None:
            key = path.name

        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                # 打包文件/文件夹
                task = progress.add_task(f"正在打包 {path.name}...", total=None)
                content, metadata = FileTransferUtil.pack(str(path))
                progress.update(task, completed=True)

                # 连接到远程服务器
                task = progress.add_task(f"正在连接到 {server}...", total=None)
                db = FlaxKV(db_name, server, backend='remote')
                progress.update(task, completed=True)

                # 上传数据
                task = progress.add_task(f"正在上传 ({format_size(len(content))})...", total=None)
                db[key] = content
                db[f"{key}:meta"] = json.dumps(metadata)
                progress.update(task, completed=True)

                # 关闭连接
                db.close()

            console.print(f"\n[bold green]✓[/bold green] 上传成功!")
            console.print(f"  类型: {metadata['type']}")
            console.print(f"  键名: [bold blue]{key}[/bold blue]")
            console.print(f"  大小: {format_size(metadata['size'])}")
            console.print(f"  服务器: {server}")

        except Exception as e:
            console.print(f"\n[bold red]✗ 上传失败:[/bold red] {str(e)}")

    def get(self, key, output=None, server='127.0.0.1:5555', db_name='file_storage'):
        """
        从远程 FlaxKV 服务器下载文件或文件夹

        Args:
            key: 要下载的键名
            output: 保存路径（默认使用当前目录）
            server: 远程服务器地址，格式为 host:port (默认: 127.0.0.1:5555)
            db_name: 数据库名称 (默认: file_storage)

        示例:
            flaxkv get my_file --server 192.168.1.100:5555
            flaxkv get my_folder --output /path/to/save --server 192.168.1.100:5555
        """
        # 默认输出路径为当前目录
        if output is None:
            output = '.'

        output_path = Path(output)

        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                # 连接到远程服务器
                task = progress.add_task(f"正在连接到 {server}...", total=None)
                db = FlaxKV(db_name, server, backend='remote')
                progress.update(task, completed=True)

                # 下载数据
                task = progress.add_task(f"正在下载...", total=None)

                # 检查键是否存在
                if key not in db:
                    progress.stop()
                    console.print(f"[bold red]错误:[/bold red] 键不存在: {key}")
                    db.close()
                    return

                content = db[key]
                metadata_str = db.get(f"{key}:meta")

                if metadata_str is None:
                    progress.stop()
                    console.print(f"[bold red]错误:[/bold red] 元数据不存在，该键可能不是通过 'flaxkv set' 上传的")
                    db.close()
                    return

                metadata = json.loads(metadata_str)
                progress.update(task, completed=True)

                # 解包文件/文件夹
                task = progress.add_task(f"正在解包...", total=None)
                FileTransferUtil.unpack(content, metadata, str(output_path))
                progress.update(task, completed=True)

                # 关闭连接
                db.close()

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

        except Exception as e:
            console.print(f"\n[bold red]✗ 下载失败:[/bold red] {str(e)}")

    def list(self, server='127.0.0.1:5555', db_name='file_storage'):
        """
        列出远程 FlaxKV 服务器上所有可用的文件和文件夹

        Args:
            server: 远程服务器地址，格式为 host:port (默认: 127.0.0.1:5555)
            db_name: 数据库名称 (默认: file_storage)

        示例:
            flaxkv list --server 192.168.1.100:5555
        """
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                # 连接到远程服务器
                task = progress.add_task(f"正在连接到 {server}...", total=None)
                db = FlaxKV(db_name, server, backend='remote')
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
                console.print(f"服务器: {server}")
                console.print(f"数据库: {db_name}")
                return

            # 重新连接获取详细信息
            db = FlaxKV(db_name, server, backend='remote')

            console.print(f"\n[bold]服务器上的文件列表[/bold]")
            console.print(f"服务器: [blue]{server}[/blue]")
            console.print(f"数据库: [blue]{db_name}[/blue]")
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
                        metadata = json.loads(db[meta_key])
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


def main():
    """主函数"""
    fire.Fire(FlaxKV2CLI)


if __name__ == '__main__':
    main() 