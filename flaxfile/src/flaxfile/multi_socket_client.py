#!/usr/bin/env python3
"""
FlaxFile 真正的多Socket客户端

架构：N个独立socket，每个socket使用滑动窗口
总并发 = N × window_size

服务器端支持：
- 多个identity可以协同上传同一个file_key
- 按file_key而不是identity管理上传会话
"""

import asyncio
import time
import hashlib
import json
import aiofiles
from pathlib import Path
from typing import Dict, Any, List, Optional

import zmq
import zmq.asyncio
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn, TransferSpeedColumn, DownloadColumn

from .crypto import get_password, configure_client_encryption

console = Console()
logger = __import__('logging').getLogger(__name__)


class MultiSocketFlaxFileClient:
    """真正的多Socket + 滑动窗口客户端"""

    def __init__(
        self,
        server_host: str = "127.0.0.1",
        port: int = 25555,
        password: Optional[str] = None,
        num_connections: Optional[int] = None,
        window_size: int = 8
    ):
        self.server_host = server_host
        self.port = port
        self.password = password
        self.num_connections = num_connections
        self.window_size = window_size

        self.context = zmq.asyncio.Context()
        self.sockets = []
        self.connected = False

    def _auto_determine_config(self, file_size: int) -> tuple:
        """自动确定最优配置"""
        if self.num_connections:
            return self.num_connections, self.window_size

        # 自适应策略
        if file_size < 50 * 1024 * 1024:  # <50MB
            return 1, 16
        elif file_size < 200 * 1024 * 1024:  # <200MB
            return 2, 8
        elif file_size < 1024 * 1024 * 1024:  # <1GB
            return 4, 8
        else:  # >=1GB
            return 8, 8

    async def connect(self, num_connections: int):
        """创建多个socket连接"""
        if self.connected:
            return

        self.sockets = []

        for i in range(num_connections):
            sock = self.context.socket(zmq.DEALER)
            sock.setsockopt(zmq.SNDBUF, 128 * 1024 * 1024)
            sock.setsockopt(zmq.RCVBUF, 128 * 1024 * 1024)
            sock.setsockopt(zmq.LINGER, 0)

            # 配置加密
            encryption_enabled = configure_client_encryption(sock, self.password)
            sock.connect(f"tcp://{self.server_host}:{self.port}")

            # 测试连接
            await sock.send_multipart([b'', b'PING'])
            frames = await sock.recv_multipart()
            if len(frames) < 2 or frames[1] != b'PONG':
                raise ConnectionError(f"Socket {i} 连接失败")

            self.sockets.append(sock)

        if encryption_enabled:
            console.print(f"[green]🔒 已建立 {num_connections} 个加密连接[/green]")
        else:
            console.print(f"[yellow]⚠️  已建立 {num_connections} 个连接（未加密）[/yellow]")

        self.connected = True

    async def upload_file(
        self,
        file_path: str,
        file_key: str,
        chunk_size: int = 4 * 1024 * 1024,
        show_progress: bool = False
    ) -> Dict[str, Any]:
        """
        真正的多Socket并发上传

        架构：
        - Socket 0: chunks [0, N, 2N, 3N, ...]
        - Socket 1: chunks [1, N+1, 2N+1, ...]
        - ...
        每个socket内部使用滑动窗口
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")

        file_size = file_path.stat().st_size
        total_chunks = (file_size + chunk_size - 1) // chunk_size

        # 自动确定配置
        num_sockets, window_size = self._auto_determine_config(file_size)

        # 创建连接
        await self.connect(num_sockets)

        start_time = time.time()

        # 全局进度跟踪
        total_bytes_uploaded = 0
        upload_lock = asyncio.Lock()

        # 单个socket的工作线程
        async def socket_worker(socket_idx: int):
            """每个socket负责: socket_idx, socket_idx+N, socket_idx+2N, ..."""
            nonlocal total_bytes_uploaded

            socket = self.sockets[socket_idx]
            my_chunks = list(range(socket_idx, total_chunks, num_sockets))

            if not my_chunks:
                return 0

            # 每个socket独立注册上传会话
            await socket.send_multipart([
                b'', b'UPLOAD_START_CONCURRENT',
                file_key.encode('utf-8'),
                str(file_size).encode('utf-8'),
                str(window_size).encode('utf-8')
            ])
            frames = await socket.recv_multipart()
            if len(frames) < 2 or frames[1] != b'OK':
                raise Exception(f"Socket {socket_idx} 上传准备失败")

            # 滑动窗口状态
            pending_acks = {}
            bytes_sent = 0

            # 发送者
            async def sender():
                nonlocal bytes_sent

                async with aiofiles.open(file_path, 'rb') as f:
                    for chunk_id in my_chunks:
                        # 等待窗口
                        while len(pending_acks) >= window_size:
                            await asyncio.sleep(0.0001)

                        # 读取chunk
                        offset = chunk_id * chunk_size
                        await f.seek(offset)
                        chunk_data = await f.read(chunk_size)

                        if not chunk_data:
                            break

                        # 发送
                        await socket.send_multipart([
                            b'', b'UPLOAD_CHUNK_CONCURRENT',
                            str(chunk_id).encode('utf-8'),
                            chunk_data
                        ])

                        pending_acks[chunk_id] = (time.time(), len(chunk_data))

            # 接收者
            async def receiver():
                nonlocal bytes_sent, total_bytes_uploaded

                for _ in my_chunks:
                    frames = await socket.recv_multipart()
                    if len(frames) < 3 or frames[1] != b'ACK':
                        logger.warning(f"Socket {socket_idx} 收到非ACK: {frames}")
                        continue

                    ack_chunk_id = int(frames[2].decode('utf-8'))

                    if ack_chunk_id in pending_acks:
                        _, chunk_len = pending_acks.pop(ack_chunk_id)
                        bytes_sent += chunk_len

                        # 更新全局进度
                        async with upload_lock:
                            total_bytes_uploaded += chunk_len

            # 并发运行
            await asyncio.gather(sender(), receiver())

            # 结束上传
            await socket.send_multipart([b'', b'UPLOAD_END'])
            frames = await socket.recv_multipart()

            return bytes_sent

        # 并发运行所有socket
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
                    f"[cyan]多Socket上传 ({num_sockets}×窗口{window_size})",
                    total=file_size
                )

                # 进度更新协程
                async def progress_updater():
                    last_bytes = 0
                    while total_bytes_uploaded < file_size:
                        await asyncio.sleep(0.1)
                        if total_bytes_uploaded > last_bytes:
                            progress.update(upload_task, completed=total_bytes_uploaded)
                            last_bytes = total_bytes_uploaded

                # 运行worker和进度更新
                tasks = [socket_worker(i) for i in range(num_sockets)]
                tasks.append(progress_updater())

                results = await asyncio.gather(*tasks)
                bytes_total = sum(results[:-1])  # 排除progress_updater
        else:
            tasks = [socket_worker(i) for i in range(num_sockets)]
            results = await asyncio.gather(*tasks)
            bytes_total = sum(results)

        upload_time = time.time() - start_time
        throughput = (file_size / (1024 * 1024)) / upload_time if upload_time > 0 else 0

        if show_progress:
            from rich.table import Table
            table = Table(title="[bold green]✓ 多Socket上传完成", show_header=False, border_style="green")
            table.add_row("文件名", f"[cyan]{file_key}[/cyan]")
            table.add_row("大小", f"[yellow]{file_size / (1024*1024):.2f} MB[/yellow]")
            table.add_row("耗时", f"[magenta]{upload_time:.2f}秒[/magenta]")
            table.add_row("吞吐量", f"[green]{throughput:.2f} MB/s[/green]")
            table.add_row("连接数", f"[blue]{num_sockets}[/blue]")
            table.add_row("窗口大小", f"{window_size}")
            table.add_row("总并发", f"{num_sockets * window_size} chunks")
            console.print(table)

        return {
            'file_key': file_key,
            'size': file_size,
            'upload_time': upload_time,
            'throughput': throughput,
            'num_sockets': num_sockets,
            'window_size': window_size,
            'total_concurrency': num_sockets * window_size
        }

    async def download_file(
        self,
        file_key: str,
        output_path: str,
        chunk_size: int = 4 * 1024 * 1024,
        show_progress: bool = False
    ) -> Dict[str, Any]:
        """多Socket并发下载（与现有实现类似）"""
        # 先获取文件信息
        if not self.connected or not self.sockets:
            await self.connect(1)

        await self.sockets[0].send_multipart([
            b'', b'DOWNLOAD_START_CONCURRENT',
            file_key.encode('utf-8')
        ])

        frames = await self.sockets[0].recv_multipart()
        if len(frames) < 5 or frames[1] != b'OK':
            raise FileNotFoundError(f"文件不存在: {file_key}")

        file_size = int(frames[2].decode('utf-8'))
        total_chunks = int(frames[3].decode('utf-8'))
        chunk_size = int(frames[4].decode('utf-8'))

        # 自动确定配置
        num_sockets, window_size = self._auto_determine_config(file_size)

        # 创建更多连接
        if len(self.sockets) < num_sockets:
            await self.connect(num_sockets)

        start_time = time.time()

        # 用于按序写入的buffer
        chunks_buffer = {}
        next_write_id = 0
        bytes_received = 0
        buffer_lock = asyncio.Lock()
        hash_obj = hashlib.sha256()

        # 单个socket的下载线程
        async def socket_worker(socket_idx: int):
            socket = self.sockets[socket_idx]
            my_chunks = list(range(socket_idx, total_chunks, num_sockets))

            if not my_chunks:
                return 0

            # 滑动窗口状态
            pending_requests = {}

            # 请求者
            async def requester():
                for chunk_id in my_chunks:
                    # 等待窗口
                    while len(pending_requests) >= window_size:
                        await asyncio.sleep(0.0001)

                    # 请求chunk
                    await socket.send_multipart([
                        b'', b'DOWNLOAD_CHUNK_CONCURRENT',
                        file_key.encode('utf-8'),
                        str(chunk_id).encode('utf-8')
                    ])

                    pending_requests[chunk_id] = time.time()

            # 接收者
            async def receiver():
                for _ in my_chunks:
                    frames = await socket.recv_multipart()
                    if len(frames) < 4 or frames[1] != b'CHUNK':
                        continue

                    chunk_id = int(frames[2].decode('utf-8'))
                    chunk_data = frames[3]

                    pending_requests.pop(chunk_id, None)

                    # 缓存chunk
                    async with buffer_lock:
                        chunks_buffer[chunk_id] = chunk_data

            await asyncio.gather(requester(), receiver())
            return len(my_chunks)

        # 文件写入器
        async def writer():
            nonlocal next_write_id, bytes_received

            async with aiofiles.open(output_path, 'wb') as f:
                while next_write_id < total_chunks:
                    # 等待chunk
                    while next_write_id not in chunks_buffer:
                        await asyncio.sleep(0.0001)

                    # 按序写入
                    async with buffer_lock:
                        data = chunks_buffer.pop(next_write_id)

                    await f.write(data)
                    hash_obj.update(data)
                    bytes_received += len(data)
                    next_write_id += 1

        # 并发运行
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
                    f"[cyan]多Socket下载 ({num_sockets}×窗口{window_size})",
                    total=file_size
                )

                async def progress_updater():
                    last_bytes = 0
                    while bytes_received < file_size:
                        await asyncio.sleep(0.1)
                        if bytes_received > last_bytes:
                            progress.update(download_task, completed=bytes_received)
                            last_bytes = bytes_received

                tasks = [socket_worker(i) for i in range(num_sockets)]
                tasks.append(writer())
                tasks.append(progress_updater())

                await asyncio.gather(*tasks)
        else:
            tasks = [socket_worker(i) for i in range(num_sockets)]
            tasks.append(writer())
            await asyncio.gather(*tasks)

        download_time = time.time() - start_time
        throughput = (bytes_received / (1024 * 1024)) / download_time if download_time > 0 else 0

        if show_progress:
            from rich.table import Table
            table = Table(title="[bold green]✓ 多Socket下载完成", show_header=False, border_style="green")
            table.add_row("文件名", f"[cyan]{file_key}[/cyan]")
            table.add_row("保存到", f"[yellow]{output_path}[/yellow]")
            table.add_row("大小", f"[yellow]{bytes_received / (1024*1024):.2f} MB[/yellow]")
            table.add_row("耗时", f"[magenta]{download_time:.2f}秒[/magenta]")
            table.add_row("吞吐量", f"[green]{throughput:.2f} MB/s[/green]")
            table.add_row("连接数", f"[blue]{num_sockets}[/blue]")
            table.add_row("窗口大小", f"{window_size}")
            table.add_row("SHA256", f"[dim]{hash_obj.hexdigest()[:32]}...[/dim]")
            console.print(table)

        return {
            'file_key': file_key,
            'size': bytes_received,
            'download_time': download_time,
            'throughput': throughput,
            'num_sockets': num_sockets,
            'window_size': window_size,
            'sha256': hash_obj.hexdigest()
        }

    async def delete_file(self, file_key: str) -> bool:
        """删除文件"""
        if not self.connected or not self.sockets:
            await self.connect(1)

        await self.sockets[0].send_multipart([b'', b'DELETE', file_key.encode('utf-8')])
        frames = await self.sockets[0].recv_multipart()

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
        if not self.connected or not self.sockets:
            await self.connect(1)

        await self.sockets[0].send_multipart([b'', b'LIST', prefix.encode('utf-8')])
        frames = await self.sockets[0].recv_multipart()

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
        """关闭所有连接"""
        for sock in self.sockets:
            sock.close()
        self.context.term()
        self.sockets = []
        self.connected = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class MultiSocketFlaxFileClientSync:
    """MultiSocketFlaxFileClient的同步包装器 - 用于CLI"""

    def __init__(
        self,
        server_host: str = "127.0.0.1",
        port: int = 25555,
        password: Optional[str] = None,
        num_connections: Optional[int] = None,
        window_size: int = 8
    ):
        from .crypto import get_password
        import sys

        # 在同步上下文中获取密码
        if password is None:
            password = get_password(
                prompt="服务器密码: ",
                allow_empty=True,
                env_var="FLAXFILE_PASSWORD",
                is_server=False
            )

        self.async_client = MultiSocketFlaxFileClient(
            server_host=server_host,
            port=port,
            password=password,
            num_connections=num_connections,
            window_size=window_size
        )

        # Windows 平台：设置事件循环策略
        if sys.platform == 'win32':
            try:
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            except AttributeError:
                pass

    def upload_file(
        self,
        file_path: str,
        file_key: str,
        chunk_size: int = 4 * 1024 * 1024,
        show_progress: bool = False
    ) -> Dict[str, Any]:
        """上传文件（同步接口）"""
        return asyncio.run(
            self.async_client.upload_file(file_path, file_key, chunk_size, show_progress)
        )

    def download_file(
        self,
        file_key: str,
        output_path: str,
        chunk_size: int = 4 * 1024 * 1024,
        show_progress: bool = False
    ) -> Dict[str, Any]:
        """下载文件（同步接口）"""
        return asyncio.run(
            self.async_client.download_file(file_key, output_path, chunk_size, show_progress)
        )

    def delete_file(self, file_key: str) -> bool:
        """删除文件（同步接口）"""
        return asyncio.run(self.async_client.delete_file(file_key))

    def list_files(self, prefix: str = "") -> list:
        """列出文件（同步接口）"""
        return asyncio.run(self.async_client.list_files(prefix))

    def close(self):
        """关闭连接"""
        asyncio.run(self.async_client.close())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
