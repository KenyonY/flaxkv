"""
异步 ZeroMQ 客户端（使用 asyncio，无需线程锁）

相比同步客户端的优势：
1. 无需 socket_lock - asyncio 天然单线程
2. 更清晰的并发模型 - async/await
3. 更好的性能 - 减少线程上下文切换
"""

import asyncio
import zmq
import zmq.asyncio
import msgpack
from typing import Dict, Any, Optional
from loguru import logger

from flaxkv2.serialization import encoder, decoder
from flaxkv2.serialization.value_meta import ValueWithMeta


class AsyncRemoteDBDict:
    """
    异步远程数据库客户端（基于 ZeroMQ + asyncio）

    用法:
        async with AsyncRemoteDBDict('default_db', 'tcp://127.0.0.1:25555',
                                      password='yao') as db:
            await db.set('key', 'value')
            value = await db.get('key')

            # 批量写入
            await db.batch_set({'key1': 'val1', 'key2': 'val2'})
    """

    # 命令常量
    CMD_CONNECT = b'CONNECT'
    CMD_DISCONNECT = b'DISCONNECT'
    CMD_GET = b'GET'
    CMD_SET = b'SET'
    CMD_DELETE = b'DELETE'
    CMD_BATCH_WRITE = b'BATCH_WRITE'
    CMD_KEYS = b'KEYS'
    CMD_PING = b'PING'

    # 响应状态
    STATUS_OK = b'OK'
    STATUS_ERROR = b'ERROR'
    STATUS_NOT_FOUND = b'NOT_FOUND'

    def __init__(
        self,
        db_name: str,
        url: str,
        timeout: int = 30000,
        connect_timeout: int = 5000,
        enable_encryption: bool = False,
        password: Optional[str] = None,
        server_public_key: Optional[str] = None,
        derive_from_password: bool = True
    ):
        """
        初始化异步客户端

        Args:
            db_name: 数据库名称
            url: 服务器地址 (tcp://host:port)
            timeout: 数据请求超时时间（毫秒，0表示无限制，默认30秒）
            connect_timeout: 连接超时时间（毫秒，默认5秒）
            enable_encryption: 是否启用加密
            password: 加密密码
            server_public_key: 服务器公钥（可选，从密码派生）
            derive_from_password: 是否从密码派生密钥
        """
        self.db_name = db_name
        self.url = url
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.enable_encryption = enable_encryption
        self.password = password
        self.server_public_key = server_public_key
        self.derive_from_password = derive_from_password

        # ZMQ 异步上下文
        self.context = zmq.asyncio.Context()
        self.socket: Optional[zmq.asyncio.Socket] = None

        # 客户端密钥（如果启用加密）
        self.client_public_key = None
        self.client_secret_key = None

        self._closed = False

        # 请求ID机制（替代锁，实现真正的并发）
        self._request_id = 0
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._receive_task: Optional[asyncio.Task] = None

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        await self.close()

    async def connect(self):
        """连接到服务器"""
        if self.socket is not None:
            return

        # 创建异步 DEALER socket
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.RCVTIMEO, self.timeout)
        self.socket.setsockopt(zmq.SNDTIMEO, self.timeout)

        # 性能优化：增大发送/接收缓冲区，提升大文件传输吞吐量
        self.socket.setsockopt(zmq.SNDBUF, 10 * 1024 * 1024)  # 10MB发送缓冲区
        self.socket.setsockopt(zmq.RCVBUF, 10 * 1024 * 1024)  # 10MB接收缓冲区

        # 注意：TCP_NODELAY在ZMQ 4.2+中默认启用，无需手动设置

        # 配置加密
        if self.enable_encryption:
            await self._setup_encryption()

        # 连接
        self.socket.connect(self.url)
        logger.debug(f"Connected to {self.url}")

        # 启动后台接收循环（实现真正的并发）
        self._receive_task = asyncio.create_task(self._receive_loop())

        # 发送 CONNECT 命令（使用连接超时）
        request = [self.CMD_CONNECT, self.db_name.encode('utf-8')]
        status, result = await self._send_request(request, timeout=self.connect_timeout)

        if status != self.STATUS_OK:
            error_msg = result.decode('utf-8') if isinstance(result, bytes) else str(result)
            raise ConnectionError(f"Failed to connect to database: {error_msg}")

    async def _setup_encryption(self):
        """设置 CurveZMQ 加密"""
        # 生成客户端密钥对
        self.client_public_key, self.client_secret_key = zmq.curve_keypair()

        # 派生服务器公钥（如果需要）
        if self.server_public_key is None and self.derive_from_password:
            from flaxkv2.utils.key_manager import get_keypair_from_password
            keypair = get_keypair_from_password(self.password, derive_from_password=True)
            self.server_public_key = keypair['public_key']

        if self.server_public_key is None:
            raise ValueError("Server public key is required for encryption")

        # 配置 socket
        self.socket.curve_secretkey = self.client_secret_key
        self.socket.curve_publickey = self.client_public_key
        self.socket.curve_serverkey = self.server_public_key.encode('utf-8')

        logger.debug("CurveZMQ encryption configured")

    def _compress_data(self, data: bytes) -> bytes:
        """添加压缩标志（暂不支持压缩）"""
        return b'\x00' + data  # 0x00 = 未压缩

    def _decompress_data(self, data: bytes) -> bytes:
        """移除压缩标志"""
        if len(data) == 0:
            return data
        # 跳过第一个字节（压缩标志）
        return data[1:]

    async def _receive_loop(self):
        """
        后台接收循环 - 持续接收响应并分发到对应的Future

        这是实现真正并发的关键：
        - 不再需要锁
        - 多个请求可以同时发送
        - 响应根据request_id自动路由到正确的等待者
        """
        try:
            while not self._closed:
                try:
                    # 接收响应
                    response_data_compressed = await self.socket.recv()

                    # 移除压缩标志
                    response_data = self._decompress_data(response_data_compressed)

                    # 解包响应 [request_id, status, result]
                    response = msgpack.unpackb(response_data, raw=True)

                    request_id = response[0]
                    status = response[1]
                    result = response[2] if len(response) > 2 else None

                    # 根据request_id找到对应的Future并设置结果
                    if request_id in self._pending_requests:
                        future = self._pending_requests.pop(request_id)
                        if not future.done():
                            future.set_result((status, result))
                    else:
                        logger.warning(f"Received response for unknown request_id: {request_id}")

                except zmq.Again:
                    # 超时，继续循环
                    continue
                except asyncio.CancelledError:
                    # 正常关闭
                    break
                except Exception as e:
                    if not self._closed:
                        logger.error(f"Error in receive loop: {e}")
                    break
        finally:
            logger.debug("Receive loop stopped")

    async def _send_request(self, request, timeout=None):
        """
        发送请求并等待响应（使用请求ID，无锁并发）

        Args:
            request: 请求列表 [command, ...]
            timeout: 请求超时时间（毫秒），None表示使用默认timeout

        Returns:
            (status, result) 元组
        """
        if self._closed:
            raise ConnectionError("Connection is closed")

        # 分配请求ID
        request_id = self._request_id
        self._request_id += 1

        # 创建Future等待响应
        future = asyncio.Future()
        self._pending_requests[request_id] = future

        try:
            # 序列化请求（在请求前加上request_id）
            request_with_id = [request_id] + request
            request_data = msgpack.packb(request_with_id, use_bin_type=True)

            # 添加压缩标志
            request_data_compressed = self._compress_data(request_data)

            # 发送请求（异步，无需等待响应）
            await self.socket.send(request_data_compressed)

            # 确定超时时间
            if timeout is None:
                timeout = self.timeout

            # 等待后台接收循环设置结果
            if timeout > 0:
                # 有超时限制
                status, result = await asyncio.wait_for(future, timeout=timeout / 1000)
            else:
                # 无超时限制
                status, result = await future

            return status, result

        except asyncio.TimeoutError:
            # 超时，清理Future
            self._pending_requests.pop(request_id, None)
            raise TimeoutError(f"Request {request_id} timed out")
        except Exception as e:
            # 其他错误，清理Future
            self._pending_requests.pop(request_id, None)
            raise

    async def get(self, key: Any) -> Any:
        """
        异步获取值

        Args:
            key: 键

        Returns:
            值

        Raises:
            KeyError: 如果键不存在
        """
        key_bytes = encoder.encode_key(key)
        request = [self.CMD_GET, self.db_name.encode('utf-8'), key_bytes]

        status, result = await self._send_request(request)

        if status == self.STATUS_NOT_FOUND:
            raise KeyError(key)
        elif status == self.STATUS_ERROR:
            error_msg = result.decode('utf-8') if isinstance(result, bytes) else str(result)
            raise RuntimeError(f"Get failed: {error_msg}")

        # 解码值
        value, expire_time, is_expired = ValueWithMeta.decode_value(result)

        if is_expired:
            # 已过期，抛出KeyError
            raise KeyError(key)

        return value

    async def set(self, key: Any, value: Any, ttl: Optional[int] = None):
        """
        异步设置值

        Args:
            key: 键
            value: 值
            ttl: 过期时间（秒）
        """
        key_bytes = encoder.encode_key(key)
        value_bytes = ValueWithMeta.encode_value(value, ttl_seconds=ttl)

        request = [self.CMD_SET, self.db_name.encode('utf-8'), key_bytes, value_bytes]

        status, result = await self._send_request(request)

        if status == self.STATUS_ERROR:
            error_msg = result.decode('utf-8') if isinstance(result, bytes) else str(result)
            raise RuntimeError(f"Set failed: {error_msg}")

    async def delete(self, key: Any):
        """
        异步删除键

        Args:
            key: 键
        """
        key_bytes = encoder.encode_key(key)
        request = [self.CMD_DELETE, self.db_name.encode('utf-8'), key_bytes]

        status, result = await self._send_request(request)

        if status == self.STATUS_ERROR:
            error_msg = result.decode('utf-8') if isinstance(result, bytes) else str(result)
            raise RuntimeError(f"Delete failed: {error_msg}")

    async def batch_set(self, items: Dict[Any, Any], ttl: Optional[int] = None):
        """
        异步批量设置（使用 WriteBatch）

        Args:
            items: 字典 {key: value, ...}
            ttl: 统一的过期时间（秒）
        """
        if not items:
            return

        # 序列化所有数据
        serialized_writes = {}
        for key, value in items.items():
            key_bytes = encoder.encode_key(key)
            value_bytes = ValueWithMeta.encode_value(value, ttl_seconds=ttl)
            serialized_writes[key_bytes] = value_bytes

        # 发送批量写入请求
        request = [
            self.CMD_BATCH_WRITE,
            self.db_name.encode('utf-8'),
            serialized_writes,
            []  # 空的删除列表
        ]

        status, result = await self._send_request(request)

        if status == self.STATUS_ERROR:
            error_msg = result.decode('utf-8') if isinstance(result, bytes) else str(result)
            raise RuntimeError(f"Batch set failed: {error_msg}")

        logger.debug(f"Batch set completed: {len(items)} items")

    async def keys(self):
        """异步获取所有键"""
        request = [self.CMD_KEYS, self.db_name.encode('utf-8')]
        status, result = await self._send_request(request)

        if status == self.STATUS_ERROR:
            error_msg = result.decode('utf-8') if isinstance(result, bytes) else str(result)
            raise RuntimeError(f"Keys failed: {error_msg}")

        # 解码键，并过滤内部键
        keys = []
        for k in result:
            key = decoder.decode_key(k)
            # 过滤内部键（TTL信息键、嵌套标记键等）
            if isinstance(key, str) and (key.startswith('__ttl_info__:') or key.startswith('__nested__:')):
                continue
            keys.append(key)
        return keys

    async def ping(self) -> bool:
        """异步 ping 服务器"""
        request = [self.CMD_PING, b'']  # 添加空的 db_name（服务器兼容性）
        try:
            status, result = await self._send_request(request)
            return status == self.STATUS_OK and result == b'PONG'
        except:
            return False

    async def close(self):
        """关闭连接"""
        if self._closed:
            return

        self._closed = True

        try:
            # 发送 DISCONNECT 命令
            request = [self.CMD_DISCONNECT, self.db_name.encode('utf-8')]
            await self._send_request(request)
        except Exception as e:
            logger.warning(f"Error disconnecting: {e}")

        # 停止接收循环
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass

        # 取消所有待处理的请求
        for future in self._pending_requests.values():
            if not future.done():
                future.set_exception(ConnectionError("Connection closed"))
        self._pending_requests.clear()

        # 关闭 socket
        if self.socket:
            self.socket.close()
            self.socket = None

        logger.debug(f"Async client closed: {self.db_name}")

    # 便捷方法（字典风格）
    async def __getitem__(self, key):
        """db[key] 语法"""
        value = await self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    async def __setitem__(self, key, value):
        """db[key] = value 语法"""
        await self.set(key, value)

    async def __delitem__(self, key):
        """del db[key] 语法"""
        await self.delete(key)
