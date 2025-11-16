"""
异步连接池

提供连接复用和并发支持，显著提升大文件传输和批量操作性能
"""

import asyncio
from typing import Optional, List
from contextlib import asynccontextmanager

from flaxkv2.client.async_zmq_client import AsyncRemoteDBDict


class AsyncConnectionPool:
    """异步数据库连接池"""

    def __init__(
        self,
        db_name: str,
        server_url: str,
        pool_size: int = 16,
        timeout: int = 0,
        connect_timeout: int = 5000,
        enable_encryption: bool = False,
        password: Optional[str] = None,
    ):
        """
        初始化连接池

        Args:
            db_name: 数据库名称
            server_url: 服务器地址
            pool_size: 连接池大小（默认16）
            timeout: 数据请求超时时间（毫秒，0表示无限制）
            connect_timeout: 连接超时时间（毫秒）
            enable_encryption: 是否启用加密
            password: 加密密码
        """
        self.db_name = db_name
        self.server_url = server_url
        self.pool_size = pool_size
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.enable_encryption = enable_encryption
        self.password = password

        # 连接池
        self._pool: List[AsyncRemoteDBDict] = []
        self._available: asyncio.Queue = asyncio.Queue(maxsize=pool_size)
        self._initialized = False
        self._closed = False

    async def initialize(self):
        """初始化连接池"""
        if self._initialized:
            return

        # 创建所有连接
        for _ in range(self.pool_size):
            conn = AsyncRemoteDBDict(
                self.db_name,
                self.server_url,
                timeout=self.timeout,
                connect_timeout=self.connect_timeout,
                enable_encryption=self.enable_encryption,
                password=self.password,
                derive_from_password=True
            )
            await conn.connect()
            self._pool.append(conn)
            await self._available.put(conn)

        self._initialized = True

    @asynccontextmanager
    async def acquire(self):
        """
        获取一个连接（上下文管理器）

        使用示例:
            async with pool.acquire() as conn:
                await conn.set('key', 'value')
        """
        if not self._initialized:
            await self.initialize()

        if self._closed:
            raise RuntimeError("Connection pool is closed")

        # 从池中获取连接
        conn = await self._available.get()

        try:
            yield conn
        finally:
            # 归还连接到池
            await self._available.put(conn)

    async def close(self):
        """关闭连接池"""
        if self._closed:
            return

        self._closed = True

        # 关闭所有连接
        for conn in self._pool:
            await conn.close()

        self._pool.clear()

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        await self.close()


async def upload_large_file_with_pool(
    db_name: str,
    server_url: str,
    key: str,
    file_path: str,
    chunk_size: int = 10 * 1024 * 1024,
    pool_size: int = 16,
    show_progress: bool = True,
    verify: bool = True,
    password: Optional[str] = None,
    enable_encryption: bool = False,
    connect_timeout: int = 5000
):
    """
    使用连接池上传大文件（流水线并发）

    相比普通异步上传，使用多个连接实现真正的并发，
    显著提升大文件传输性能（预期+100-200%）

    Args:
        db_name: 数据库名称
        server_url: 服务器地址
        key: 存储键名
        file_path: 本地文件路径
        chunk_size: 分块大小
        pool_size: 连接池大小（并发数，默认16）
        show_progress: 是否显示进度
        verify: 是否验证哈希
        password: 加密密码
        enable_encryption: 是否启用加密
        connect_timeout: 连接超时时间（毫秒）

    Returns:
        上传信息字典
    """
    import time
    from pathlib import Path
    from flaxkv2.utils.async_file_transfer import calculate_file_hash, format_size

    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")

    # 文件信息
    file_size = file_path.stat().st_size
    total_chunks = (file_size + chunk_size - 1) // chunk_size

    # 计算哈希
    file_hash = None
    if verify:
        if show_progress:
            print(f"🔍 计算文件哈希值...")
        file_hash = calculate_file_hash(str(file_path))
        if show_progress:
            print(f"   SHA256: {file_hash}")

    # 元数据
    metadata = {
        'type': 'chunked_file',
        'filename': file_path.name,
        'size': file_size,
        'chunks': total_chunks,
        'chunk_size': chunk_size,
        'upload_time': None,
    }

    if file_hash:
        metadata['hash'] = file_hash
        metadata['hash_algorithm'] = 'sha256'

    # 创建连接池
    async with AsyncConnectionPool(
        db_name,
        server_url,
        pool_size=pool_size,
        timeout=0,  # 数据请求无超时限制
        connect_timeout=connect_timeout,
        enable_encryption=enable_encryption,
        password=password
    ) as pool:

        # 保存元数据（使用第一个连接）
        async with pool.acquire() as conn:
            metadata['status'] = 'uploading'
            await conn.set(f"{key}:meta", metadata)

        # 开始上传
        if show_progress:
            print(f"\n📤 流水线并发上传文件: {file_path.name}")
            print(f"   大小: {format_size(file_size)}")
            print(f"   分块: {total_chunks} 个 (每块 {format_size(chunk_size)})")
            print(f"   并发连接: {pool_size}\n")

        start_time = time.time()

        # 读取所有chunks
        chunks_data = []
        with open(file_path, 'rb') as f:
            for i in range(total_chunks):
                chunk = f.read(chunk_size)
                chunks_data.append((i, chunk))

        # 上传进度
        uploaded_count = 0
        progress_lock = asyncio.Lock()

        async def upload_chunk(chunk_index: int, chunk_data: bytes):
            """上传单个chunk（使用连接池）"""
            nonlocal uploaded_count

            # 从池中获取连接并上传
            async with pool.acquire() as conn:
                await conn.set(f"{key}:chunk:{chunk_index}", chunk_data)

            # 更新进度
            if show_progress:
                async with progress_lock:
                    uploaded_count += 1
                    progress = uploaded_count * 100 // total_chunks
                    size_uploaded = min(uploaded_count * chunk_size, file_size)
                    print(f"   进度: [{progress:3d}%] {format_size(size_uploaded)}/{format_size(file_size)}", end='\r')

        # 并发上传所有chunks
        tasks = [upload_chunk(idx, data) for idx, data in chunks_data]
        await asyncio.gather(*tasks)

        if show_progress:
            print()

        elapsed = time.time() - start_time
        throughput = file_size / elapsed / (1024 * 1024)

        # 更新元数据
        async with pool.acquire() as conn:
            metadata['status'] = 'completed'
            metadata['upload_time'] = elapsed
            metadata['throughput_mbps'] = throughput
            await conn.set(f"{key}:meta", metadata)

        if show_progress:
            print(f"\n✅ 上传完成: {file_path.name}")
            print(f"   耗时: {elapsed:.2f}秒")
            print(f"   吞吐量: {throughput:.1f} MB/s")

        return metadata


async def download_large_file_with_pool(
    db_name: str,
    server_url: str,
    key: str,
    output_path: str,
    pool_size: int = 16,
    show_progress: bool = True,
    verify: bool = True,
    password: Optional[str] = None,
    enable_encryption: bool = False,
    connect_timeout: int = 5000
):
    """
    使用连接池下载大文件（流水线并发）

    相比普通异步下载，使用多个连接实现真正的并发，
    显著提升大文件传输性能

    Args:
        db_name: 数据库名称
        server_url: 服务器地址
        key: 存储键名
        output_path: 输出文件路径
        pool_size: 连接池大小（并发数，默认16）
        show_progress: 是否显示进度
        verify: 是否验证哈希
        password: 加密密码
        enable_encryption: 是否启用加密
        connect_timeout: 连接超时时间（毫秒）

    Returns:
        文件信息字典
    """
    import time
    from pathlib import Path
    from flaxkv2.utils.async_file_transfer import calculate_file_hash, format_size

    output_path = Path(output_path)

    # 创建连接池
    async with AsyncConnectionPool(
        db_name,
        server_url,
        pool_size=pool_size,
        timeout=0,  # 数据请求无超时限制
        connect_timeout=connect_timeout,
        enable_encryption=enable_encryption,
        password=password
    ) as pool:

        # 获取元数据
        async with pool.acquire() as conn:
            metadata = await conn.get(f"{key}:meta")
            if metadata is None:
                raise FileNotFoundError(f"文件不存在: {key}")

        total_chunks = metadata['chunks']
        file_size = metadata['size']

        if show_progress:
            print(f"\n📥 流水线并发下载文件: {metadata['filename']}")
            print(f"   大小: {format_size(file_size)}")
            print(f"   分块: {total_chunks} 个")
            print(f"   并发连接: {pool_size}\n")

        start_time = time.time()

        # 下载进度
        downloaded_count = 0
        download_lock = asyncio.Lock()
        chunks_dict = {}

        async def download_chunk(chunk_index: int):
            """下载单个chunk（使用连接池）"""
            nonlocal downloaded_count

            # 从池中获取连接并下载
            async with pool.acquire() as conn:
                chunk_data = await conn.get(f"{key}:chunk:{chunk_index}")

            # 保存chunk并更新进度
            async with download_lock:
                chunks_dict[chunk_index] = chunk_data
                downloaded_count += 1

                if show_progress:
                    progress = downloaded_count * 100 // total_chunks
                    size_downloaded = min(downloaded_count * metadata['chunk_size'], file_size)
                    print(f"   进度: [{progress:3d}%] {format_size(size_downloaded)}/{format_size(file_size)}", end='\r')

        # 并发下载所有chunks
        tasks = [download_chunk(i) for i in range(total_chunks)]
        await asyncio.gather(*tasks)

        if show_progress:
            print()

        # 写入文件
        with open(output_path, 'wb') as f:
            for i in range(total_chunks):
                f.write(chunks_dict[i])

        download_time = time.time() - start_time

        # 验证哈希
        if verify and 'hash' in metadata:
            if show_progress:
                print(f"   验证哈希值...")

            actual_hash = calculate_file_hash(str(output_path))

            if actual_hash != metadata['hash']:
                output_path.unlink()
                raise ValueError(f"文件哈希不匹配！期望: {metadata['hash']}, 实际: {actual_hash}")

            if show_progress:
                print(f"   ✓ 哈希验证通过")

        if show_progress:
            throughput = file_size / download_time / (1024 * 1024)
            print(f"\n✅ 下载完成!")
            print(f"   总耗时: {download_time:.2f}秒")
            print(f"   吞吐量: {throughput:.1f} MB/s\n")

        return metadata
