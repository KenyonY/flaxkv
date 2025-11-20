#!/usr/bin/env python3
"""
FlaxFile 异步单端口客户端 - 使用 DEALER/ROUTER 模式
支持多连接 + aiofiles异步文件I/O
"""

import sys
import os
import zmq
import zmq.asyncio
import time
import json
import hashlib
import logging
import asyncio
import aiofiles
from pathlib import Path
from typing import Dict, Any, Optional, List

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

        # 注意：密码应该在创建客户端时就已经获取
        # 如果到这里 password 仍然是 None，说明用户明确选择不加密
        # 不应该在异步上下文中再次尝试获取密码（会阻塞）

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

    async def upload_file_concurrent(
        self,
        file_path: str,
        file_key: str,
        chunk_size: int = 4 * 1024 * 1024,  # 4MB
        max_concurrency: int = 8,  # 最大并发chunk数（滑动窗口大小）
        show_progress: bool = False
    ) -> Dict[str, Any]:
        """
        并发上传文件 (Pipeline模式 + 滑动窗口，修复socket竞争问题)

        Args:
            file_path: 文件路径
            file_key: 文件键名
            chunk_size: chunk大小（默认4MB）
            max_concurrency: 滑动窗口大小（默认8，同时最多8个chunk在传输中）
            show_progress: 是否显示进度

        Returns:
            上传结果字典
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")

        file_size = file_path.stat().st_size

        await self.connect()

        start_time = time.time()

        # 1. 发送上传开始请求
        await self.socket.send_multipart([
            b'',
            b'UPLOAD_START_CONCURRENT',
            file_key.encode('utf-8'),
            str(file_size).encode('utf-8'),
            str(max_concurrency).encode('utf-8')
        ])

        frames = await self.socket.recv_multipart()
        if len(frames) < 2 or frames[1] != b'OK':
            raise Exception(f"服务器未就绪: {frames}")

        # 2. 使用滑动窗口并发上传 (单发送者+单接收者，避免socket竞争)
        total_chunks = (file_size + chunk_size - 1) // chunk_size

        # 滑动窗口状态
        pending_acks = {}  # {chunk_id: (send_time, chunk_len)}
        next_send_id = 0
        next_ack_id = 0
        bytes_sent = 0

        # 单一发送者协程
        async def sender():
            nonlocal next_send_id, bytes_sent

            # 使用aiofiles异步读取文件
            async with aiofiles.open(file_path, 'rb') as f:
                while next_send_id < total_chunks:
                    # 等待窗口有空位
                    while len(pending_acks) >= max_concurrency:
                        await asyncio.sleep(0.0001)  # 短暂等待

                    # 异步读取chunk
                    chunk_data = await f.read(chunk_size)
                    if not chunk_data:
                        break

                    # 发送chunk (只有这一个协程发送，无竞争)
                    await self.socket.send_multipart([
                        b'',
                        b'UPLOAD_CHUNK_CONCURRENT',
                        str(next_send_id).encode('utf-8'),
                        chunk_data
                    ])

                    # 记录待确认的chunk
                    pending_acks[next_send_id] = (time.time(), len(chunk_data))
                    next_send_id += 1

        # 单一接收者协程
        async def receiver():
            nonlocal next_ack_id, bytes_sent

            while next_ack_id < total_chunks:
                # 接收ACK (只有这一个协程接收，无竞争)
                frames = await self.socket.recv_multipart()

                if len(frames) < 3 or frames[1] != b'ACK':
                    raise Exception(f"收到非ACK响应: {frames}")

                ack_chunk_id = int(frames[2].decode('utf-8'))

                # 移除已确认的chunk
                if ack_chunk_id in pending_acks:
                    _, chunk_len = pending_acks.pop(ack_chunk_id)
                    bytes_sent += chunk_len

                    # 更新已确认的ID
                    if ack_chunk_id == next_ack_id:
                        next_ack_id += 1
                else:
                    logger.warning(f"收到重复ACK: {ack_chunk_id}")

        # 进度跟踪协程
        async def progress_tracker(progress_task=None, progress_obj=None):
            nonlocal bytes_sent
            last_bytes = 0

            while next_ack_id < total_chunks:
                await asyncio.sleep(0.1)  # 每100ms更新一次

                if show_progress and progress_obj and progress_task is not None:
                    if bytes_sent > last_bytes:
                        progress_obj.update(progress_task, completed=bytes_sent)
                        last_bytes = bytes_sent

        # 并发运行发送者和接收者
        if show_progress:
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
                    f"[cyan]滑动窗口上传 {file_path.name} (窗口{max_concurrency})",
                    total=file_size
                )

                # 同时运行发送、接收和进度跟踪
                await asyncio.gather(
                    sender(),
                    receiver(),
                    progress_tracker(upload_task, progress)
                )
        else:
            # 无进度条模式
            await asyncio.gather(sender(), receiver())

        # 3. 发送上传结束请求
        await self.socket.send_multipart([b'', b'UPLOAD_END'])
        frames = await self.socket.recv_multipart()

        if len(frames) < 3 or frames[1] != b'OK':
            raise Exception(f"上传结束失败: {frames}")

        result = json.loads(frames[2].decode('utf-8'))

        upload_time = time.time() - start_time
        throughput = (file_size / (1024 * 1024)) / upload_time if upload_time > 0 else 0

        if show_progress:
            table = Table(title="[bold green]✓ 并发上传完成", show_header=False, border_style="green")
            table.add_row("文件名", f"[cyan]{file_key}")
            table.add_row("大小", f"[yellow]{file_size / (1024*1024):.2f} MB")
            table.add_row("耗时", f"[magenta]{upload_time:.2f}秒")
            table.add_row("吞吐量", f"[green]{throughput:.2f} MB/s")
            table.add_row("并发度", f"[blue]{max_concurrency}")
            table.add_row("Chunks", f"{total_chunks}")
            table.add_row("SHA256", f"[dim]{result.get('sha256', 'N/A')[:32]}...")
            console.print(table)

        return {
            'file_key': file_key,
            'size': file_size,
            'upload_time': upload_time,
            'throughput': throughput,
            'chunks': total_chunks,
            'concurrency': max_concurrency,
            'sha256': result.get('sha256')
        }

    async def download_file_concurrent(
        self,
        file_key: str,
        output_path: str,
        max_concurrency: int = 8,  # 最大并发chunk数
        show_progress: bool = False
    ) -> Dict[str, Any]:
        """
        并发下载文件 (Pipeline模式，高性能)

        Args:
            file_key: 文件键名
            output_path: 输出路径
            max_concurrency: 最大并发chunk数（默认8）
            show_progress: 是否显示进度

        Returns:
            下载结果字典
        """
        await self.connect()

        start_time = time.time()

        # 1. 发送下载开始请求
        await self.socket.send_multipart([
            b'',
            b'DOWNLOAD_START_CONCURRENT',
            file_key.encode('utf-8')
        ])

        frames = await self.socket.recv_multipart()
        if len(frames) < 5 or frames[1] != b'OK':
            if frames[1] == b'ERROR':
                error_msg = frames[2].decode('utf-8') if len(frames) > 2 else "Unknown error"
                raise FileNotFoundError(f"文件不存在: {error_msg}")
            raise Exception(f"下载请求失败: {frames}")

        file_size = int(frames[2].decode('utf-8'))
        total_chunks = int(frames[3].decode('utf-8'))
        chunk_size = int(frames[4].decode('utf-8'))

        # 2. 使用滑动窗口并发下载 (单发送者+单接收者，避免socket竞争)
        pending_requests = {}  # {chunk_id: request_time}
        chunks_buffer = {}  # {chunk_id: data}
        next_request_id = 0
        next_write_id = 0
        bytes_received = 0
        hash_obj = hashlib.sha256()

        # 单一请求发送者
        async def requester():
            nonlocal next_request_id

            while next_request_id < total_chunks:
                # 等待窗口有空位
                while len(pending_requests) >= max_concurrency:
                    await asyncio.sleep(0.0001)

                # 请求chunk (只有这一个协程发送，无竞争)
                await self.socket.send_multipart([
                    b'',
                    b'DOWNLOAD_CHUNK_CONCURRENT',
                    file_key.encode('utf-8'),
                    str(next_request_id).encode('utf-8')
                ])

                pending_requests[next_request_id] = time.time()
                next_request_id += 1

        # 单一响应接收者
        async def receiver():
            nonlocal bytes_received

            received_count = 0
            while received_count < total_chunks:
                # 接收chunk数据 (只有这一个协程接收，无竞争)
                frames = await self.socket.recv_multipart()

                if len(frames) < 4 or frames[1] != b'CHUNK':
                    raise Exception(f"收到非CHUNK响应: {frames}")

                chunk_id = int(frames[2].decode('utf-8'))
                chunk_data = frames[3]

                # 移除待确认
                pending_requests.pop(chunk_id, None)

                # 缓存chunk数据
                chunks_buffer[chunk_id] = chunk_data
                bytes_received += len(chunk_data)
                received_count += 1

        # 文件写入器 (按序写入，使用aiofiles)
        async def writer(f, progress_task=None, progress_obj=None):
            nonlocal next_write_id

            while next_write_id < total_chunks:
                # 等待下一个chunk到达
                while next_write_id not in chunks_buffer:
                    await asyncio.sleep(0.0001)

                # 按序写入 (aiofiles异步写入)
                data = chunks_buffer.pop(next_write_id)
                await f.write(data)
                hash_obj.update(data)
                next_write_id += 1

                # 更新进度
                if show_progress and progress_obj and progress_task is not None:
                    progress_obj.update(progress_task, completed=bytes_received)

        # 并发运行请求、接收和写入
        if show_progress:
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
                    f"[cyan]滑动窗口下载 {file_key} (窗口{max_concurrency})",
                    total=file_size
                )

                # 使用aiofiles异步打开文件
                async with aiofiles.open(output_path, 'wb') as f:
                    await asyncio.gather(
                        requester(),
                        receiver(),
                        writer(f, download_task, progress)
                    )
        else:
            # 无进度条模式
            async with aiofiles.open(output_path, 'wb') as f:
                await asyncio.gather(
                    requester(),
                    receiver(),
                    writer(f)
                )

        download_time = time.time() - start_time
        throughput = (bytes_received / (1024 * 1024)) / download_time if download_time > 0 else 0

        if show_progress:
            table = Table(title="[bold green]✓ 并发下载完成", show_header=False, border_style="green")
            table.add_row("文件名", f"[cyan]{file_key}")
            table.add_row("保存到", f"[yellow]{output_path}")
            table.add_row("大小", f"[yellow]{bytes_received / (1024*1024):.2f} MB")
            table.add_row("耗时", f"[magenta]{download_time:.2f}秒")
            table.add_row("吞吐量", f"[green]{throughput:.2f} MB/s")
            table.add_row("并发度", f"[blue]{max_concurrency}")
            table.add_row("Chunks", f"{total_chunks}")
            table.add_row("SHA256", f"[dim]{hash_obj.hexdigest()[:32]}...")
            console.print(table)

        return {
            'file_key': file_key,
            'size': bytes_received,
            'download_time': download_time,
            'throughput': throughput,
            'chunks': total_chunks,
            'concurrency': max_concurrency,
            'sha256': hash_obj.hexdigest()
        }

    async def upload_file(
        self,
        file_path: str,
        file_key: str,
        chunk_size: int = 4 * 1024 * 1024,  # 4MB
        show_progress: bool = False,
        max_concurrency: int = 16  # 默认并发度16
    ) -> Dict[str, Any]:
        """
        上传文件 (默认使用并发模式，并发度16)

        Args:
            file_path: 文件路径
            file_key: 文件键名
            chunk_size: chunk大小（默认4MB）
            show_progress: 是否显示进度
            max_concurrency: 最大并发chunk数（默认16）

        Returns:
            上传结果字典
        """
        # 直接调用并发上传
        return await self.upload_file_concurrent(
            file_path, file_key, chunk_size, max_concurrency, show_progress
        )

    async def download_file(
        self,
        file_key: str,
        output_path: str,
        show_progress: bool = False,
        max_concurrency: int = 16  # 默认并发度16
    ) -> Dict[str, Any]:
        """
        下载文件 (默认使用并发模式，并发度16)

        Args:
            file_key: 文件键名
            output_path: 输出路径
            show_progress: 是否显示进度
            max_concurrency: 最大并发chunk数（默认16）

        Returns:
            下载结果字典
        """
        # 直接调用并发下载
        return await self.download_file_concurrent(
            file_key, output_path, max_concurrency, show_progress
        )

    async def delete_file(self, file_key: str) -> bool:
        """删除文件"""
        await self.connect()

        await self.socket.send_multipart([b'', b'DELETE', file_key.encode('utf-8')])
        frames = await self.socket.recv_multipart()

        if len(frames) < 2:
            return False

        return frames[1] == b'OK'

    async def list_files(self, prefix: str = "") -> list:
        """
        列出服务器上的文件

        Args:
            prefix: 文件前缀（可选，用于过滤）

        Returns:
            文件列表，每个文件包含 key, size, mtime
        """
        await self.connect()

        await self.socket.send_multipart([b'', b'LIST', prefix.encode('utf-8')])
        frames = await self.socket.recv_multipart()

        if len(frames) < 2:
            raise Exception("服务器响应无效")

        if frames[1] == b'ERROR':
            error_msg = frames[2].decode('utf-8') if len(frames) > 2 else "Unknown error"
            raise Exception(f"列出文件失败: {error_msg}")

        if frames[1] != b'OK':
            raise Exception(f"列出文件失败: {frames[1]}")

        # 解析文件列表
        files_json = frames[2].decode('utf-8')
        files = json.loads(files_json)

        return files

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

        # 在同步上下文中获取密码（避免在异步 connect() 中阻塞）
        # 密码获取优先级：1. 命令行参数 -> 2. 环境变量 -> 3. 交互式输入
        if password is None:
            password = get_password(
                prompt="服务器密码: ",
                allow_empty=True,
                env_var="FLAXFILE_PASSWORD",
                is_server=False
            )

        self.async_client = AsyncFlaxFileClient(server_host, port, password)

        # Windows 平台：设置事件循环策略为 Selector（ZMQ 需要）
        if sys.platform == 'win32':
            try:
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            except AttributeError:
                # Python < 3.8 不支持 WindowsSelectorEventLoopPolicy
                pass

    def connect(self):
        """连接到服务器"""
        asyncio.run(self.async_client.connect())

    def upload_file(
        self,
        file_path: str,
        file_key: str,
        chunk_size: int = 4 * 1024 * 1024,
        show_progress: bool = False,
        max_concurrency: int = 16  # 默认并发度16
    ) -> Dict[str, Any]:
        """上传文件 (同步，默认使用并发模式)"""
        return asyncio.run(
            self.async_client.upload_file(file_path, file_key, chunk_size, show_progress, max_concurrency)
        )

    def upload_file_concurrent(
        self,
        file_path: str,
        file_key: str,
        chunk_size: int = 4 * 1024 * 1024,
        max_concurrency: int = 16,
        show_progress: bool = False
    ) -> Dict[str, Any]:
        """并发上传文件 (同步包装器，等同于 upload_file)"""
        return self.upload_file(file_path, file_key, chunk_size, show_progress, max_concurrency)

    def download_file(
        self,
        file_key: str,
        output_path: str,
        show_progress: bool = False,
        max_concurrency: int = 16  # 默认并发度16
    ) -> Dict[str, Any]:
        """下载文件 (同步，默认使用并发模式)"""
        return asyncio.run(
            self.async_client.download_file(file_key, output_path, show_progress, max_concurrency)
        )

    def delete_file(self, file_key: str) -> bool:
        """删除文件 (同步)"""
        return asyncio.run(self.async_client.delete_file(file_key))

    def list_files(self, prefix: str = "") -> list:
        """列出文件 (同步)"""
        return asyncio.run(self.async_client.list_files(prefix))

    def close(self):
        """关闭连接 (同步)"""
        asyncio.run(self.async_client.close())

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
