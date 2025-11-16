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
        pool_size: int = 8,
        timeout: int = 60000,
        enable_encryption: bool = False,
        password: Optional[str] = None,
    ):
        """
        初始化连接池

        Args:
            db_name: 数据库名称
            server_url: 服务器地址
            pool_size: 连接池大小（默认8）
            timeout: 请求超时时间
            enable_encryption: 是否启用加密
            password: 加密密码
        """
        self.db_name = db_name
        self.server_url = server_url
        self.pool_size = pool_size
        self.timeout = timeout
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
    pool_size: int = 8,
    show_progress: bool = True,
    verify: bool = True,
    password: Optional[str] = None,
    enable_encryption: bool = False
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
        pool_size: 连接池大小（并发数）
        show_progress: 是否显示进度
        verify: 是否验证哈希
        password: 加密密码
        enable_encryption: 是否启用加密

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
