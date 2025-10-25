"""
FlaxKV2 命令行接口 (使用Fire实现)
"""

import os
import fire
from rich import print

from flaxkv2 import __version__
from flaxkv2.utils.log import set_log_level


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


def main():
    """主函数"""
    fire.Fire(FlaxKV2CLI)


if __name__ == '__main__':
    main() 