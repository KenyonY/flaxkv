#!/usr/bin/env python3
"""
FlaxFile 异步单端口客户端 - 使用 DEALER/ROUTER 模式
"""

import sys
import zmq
import zmq.asyncio
import time
import json
import hashlib
import logging
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn, TransferSpeedColumn, DownloadColumn
from rich.panel import Panel
from rich.table import Table
from rich import print as rprint

from .crypto import get_password, configure_client_encryption

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

# 全局 Console
console = Console()


class AsyncFlaxFileClient:
    """FlaxFile 异步单端口客户端 - DEALER/ROUTER 可靠传输"""

    def __init__(
        self,
        server_host: str = "127.0.0.1",
        port: int = 25555,
        password: Optional[str] = None,
    ):
        self.server_host = server_host
        self.port = port
        self.password = password

        self.context = zmq.asyncio.Context()
        self.socket = None
        self.connected = False

    async def connect(self):
        """连接到服务器"""
        if self.connected:
            return

        # 获取密码（如果未提供）
        if self.password is None:
            self.password = get_password(
                prompt="服务器密码: ",
                allow_empty=True,
                env_var="FLAXFILE_PASSWORD",
                is_server=False
            )

        # 创建 DEALER socket
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.SNDBUF, 128 * 1024 * 1024)
        self.socket.setsockopt(zmq.RCVBUF, 128 * 1024 * 1024)
        self.socket.setsockopt(zmq.LINGER, 0)

        # 配置加密
        encryption_enabled = configure_client_encryption(self.socket, self.password)

        self.socket.connect(f"tcp://{self.server_host}:{self.port}")

        # 测试连接
        try:
            await self.socket.send_multipart([b'', b'PING'])
            frames = await self.socket.recv_multipart()

            if len(frames) < 2 or frames[1] != b'PONG':
                raise ConnectionError("服务器连接失败")

            if encryption_enabled:
                console.print(f"[green]🔒 已建立加密连接: {self.server_host}:{self.port}[/green]")
            else:
                console.print(f"[yellow]⚠️  连接到 {self.server_host}:{self.port} (未加密)[/yellow]")

            self.connected = True

        except zmq.error.ZMQError as e:
            self.socket.close()
            if encryption_enabled and "Connection refused" not in str(e):
                raise ConnectionError(
                    f"加密连接失败，可能原因：\n"
                    f"  1. 服务器未启用加密\n"
                    f"  2. 密码不匹配\n"
                    f"  原始错误: {e}"
                )
            raise ConnectionError(f"服务器连接失败: {e}")

    async def upload_file(
        self,
        file_path: str,
        file_key: str,
        chunk_size: int = 4 * 1024 * 1024,  # 4MB
        show_progress: bool = False
    ) -> Dict[str, Any]:
        """
        上传文件 (异步DEALER/ROUTER 可靠传输)

        每个chunk都会等待ACK确认，确保数据可靠传输
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")

        file_size = file_path.stat().st_size

        await self.connect()

        start_time = time.time()

        if show_progress:
            # 使用 Rich Progress
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                upload_task = progress.add_task(
                    f"[cyan]上传 {file_path.name}",
                    total=file_size
                )

                # 1. 发送上传开始请求
                await self.socket.send_multipart([
                    b'',
                    b'UPLOAD_START',
                    file_key.encode('utf-8'),
                    str(file_size).encode('utf-8')
                ])

                frames = await self.socket.recv_multipart()
                if len(frames) < 2 or frames[1] != b'OK':
                    raise Exception(f"服务器未就绪: {frames}")

                # 2. 流式发送文件数据
                bytes_sent = 0
                chunks_sent = 0

                with open(file_path, 'rb') as f:
                    while True:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break

                        # 发送数据块
                        await self.socket.send_multipart([b'', b'UPLOAD_CHUNK', chunk])

                        # 等待ACK确认
                        frames = await self.socket.recv_multipart()
                        if len(frames) < 2 or frames[1] != b'ACK':
                            raise Exception(f"服务器响应异常: {frames}")

                        bytes_sent += len(chunk)
                        chunks_sent += 1

                        # 更新进度条
                        progress.update(upload_task, completed=bytes_sent)

                # 3. 发送上传结束请求
                await self.socket.send_multipart([b'', b'UPLOAD_END'])
                frames = await self.socket.recv_multipart()

                if len(frames) < 3 or frames[1] != b'OK':
                    raise Exception(f"上传结束失败: {frames}")

                result = json.loads(frames[2].decode('utf-8'))

        else:
            # 无进度条模式 (保持原有逻辑)
            await self.socket.send_multipart([
                b'',
                b'UPLOAD_START',
                file_key.encode('utf-8'),
                str(file_size).encode('utf-8')
            ])

            frames = await self.socket.recv_multipart()
            if len(frames) < 2 or frames[1] != b'OK':
                raise Exception(f"服务器未就绪: {frames}")

            bytes_sent = 0
            chunks_sent = 0

            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break

                    await self.socket.send_multipart([b'', b'UPLOAD_CHUNK', chunk])
                    frames = await self.socket.recv_multipart()
                    if len(frames) < 2 or frames[1] != b'ACK':
                        raise Exception(f"服务器响应异常: {frames}")

                    bytes_sent += len(chunk)
                    chunks_sent += 1

            await self.socket.send_multipart([b'', b'UPLOAD_END'])
            frames = await self.socket.recv_multipart()

            if len(frames) < 3 or frames[1] != b'OK':
                raise Exception(f"上传结束失败: {frames}")

            result = json.loads(frames[2].decode('utf-8'))

        upload_time = time.time() - start_time
        throughput = (file_size / (1024 * 1024)) / upload_time if upload_time > 0 else 0

        if show_progress:
            # 使用 Rich Table 显示结果
            table = Table(title="[bold green]✓ 上传完成", show_header=False, border_style="green")
            table.add_row("文件名", f"[cyan]{file_key}")
            table.add_row("大小", f"[yellow]{file_size / (1024*1024):.2f} MB")
            table.add_row("耗时", f"[magenta]{upload_time:.2f}秒")
            table.add_row("吞吐量", f"[green]{throughput:.2f} MB/s")
            table.add_row("Chunks", f"{chunks_sent}")
            table.add_row("SHA256", f"[dim]{result.get('sha256', 'N/A')[:32]}...")
            console.print(table)

        return {
            'file_key': file_key,
            'size': file_size,
            'upload_time': upload_time,
            'throughput': throughput,
            'chunks': chunks_sent,
            'sha256': result.get('sha256')
        }

    async def download_file(
        self,
        file_key: str,
        output_path: str,
        show_progress: bool = False
    ) -> Dict[str, Any]:
        """下载文件"""
        await self.connect()

        start_time = time.time()

        # 1. 发送下载请求
        await self.socket.send_multipart([b'', b'DOWNLOAD', file_key.encode('utf-8')])
        frames = await self.socket.recv_multipart()

        if len(frames) < 2:
            raise Exception("服务器响应无效")

        if frames[1] == b'ERROR':
            error_msg = frames[2].decode('utf-8') if len(frames) > 2 else "Unknown error"
            raise FileNotFoundError(f"文件不存在: {error_msg}")

        if frames[1] != b'OK':
            raise Exception(f"下载请求失败: {frames[1]}")

        file_size = int(frames[2].decode('utf-8'))

        if show_progress:
            # 使用 Rich Progress
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                download_task = progress.add_task(
                    f"[cyan]下载 {file_key}",
                    total=file_size
                )

                # 2. 流式接收数据
                bytes_received = 0
                hash_obj = hashlib.sha256()

                with open(output_path, 'wb') as f:
                    while True:
                        frames = await self.socket.recv_multipart()

                        if len(frames) < 2:
                            break

                        if frames[1] == b'EOF':
                            break

                        if frames[1] == b'CHUNK':
                            if len(frames) < 3:
                                break

                            data = frames[2]
                            f.write(data)
                            hash_obj.update(data)
                            bytes_received += len(data)

                            # 更新进度条
                            progress.update(download_task, completed=bytes_received)

        else:
            # 无进度条模式
            bytes_received = 0
            hash_obj = hashlib.sha256()

            with open(output_path, 'wb') as f:
                while True:
                    frames = await self.socket.recv_multipart()

                    if len(frames) < 2:
                        break

                    if frames[1] == b'EOF':
                        break

                    if frames[1] == b'CHUNK':
                        if len(frames) < 3:
                            break

                        data = frames[2]
                        f.write(data)
                        hash_obj.update(data)
                        bytes_received += len(data)

        download_time = time.time() - start_time
        throughput = (bytes_received / (1024 * 1024)) / download_time if download_time > 0 else 0

        if show_progress:
            # 使用 Rich Table 显示结果
            table = Table(title="[bold green]✓ 下载完成", show_header=False, border_style="green")
            table.add_row("文件名", f"[cyan]{file_key}")
            table.add_row("保存到", f"[yellow]{output_path}")
            table.add_row("大小", f"[yellow]{bytes_received / (1024*1024):.2f} MB")
            table.add_row("耗时", f"[magenta]{download_time:.2f}秒")
            table.add_row("吞吐量", f"[green]{throughput:.2f} MB/s")
            table.add_row("SHA256", f"[dim]{hash_obj.hexdigest()[:32]}...")
            console.print(table)

        return {
            'file_key': file_key,
            'size': bytes_received,
            'download_time': download_time,
            'throughput': throughput,
            'sha256': hash_obj.hexdigest()
        }

    async def delete_file(self, file_key: str) -> bool:
        """删除文件"""
        await self.connect()

        await self.socket.send_multipart([b'', b'DELETE', file_key.encode('utf-8')])
        frames = await self.socket.recv_multipart()

        if len(frames) < 2:
            return False

        return frames[1] == b'OK'

    async def close(self):
        """关闭连接"""
        if self.socket:
            self.socket.close()
        self.context.term()
        self.connected = False

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class FlaxFileClient:
    """FlaxFile 同步包装器 - 兼容现有CLI"""

    def __init__(
        self,
        server_host: str = "127.0.0.1",
        port: int = 25555,
        password: Optional[str] = None,
        **kwargs  # 兼容旧参数
    ):
        # 忽略旧的 upload_port, download_port, control_port
        self.async_client = AsyncFlaxFileClient(server_host, port, password)

    def connect(self):
        """连接到服务器"""
        asyncio.run(self.async_client.connect())

    def upload_file(
        self,
        file_path: str,
        file_key: str,
        chunk_size: int = 4 * 1024 * 1024,
        show_progress: bool = False
    ) -> Dict[str, Any]:
        """上传文件 (同步)"""
        return asyncio.run(
            self.async_client.upload_file(file_path, file_key, chunk_size, show_progress)
        )

    def download_file(
        self,
        file_key: str,
        output_path: str,
        show_progress: bool = False
    ) -> Dict[str, Any]:
        """下载文件 (同步)"""
        return asyncio.run(
            self.async_client.download_file(file_key, output_path, show_progress)
        )

    def delete_file(self, file_key: str) -> bool:
        """删除文件 (同步)"""
        return asyncio.run(self.async_client.delete_file(file_key))

    def close(self):
        """关闭连接 (同步)"""
        asyncio.run(self.async_client.close())

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
