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
