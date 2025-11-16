"""
FlaxKV2 ZeroMQ 服务器
高性能的远程数据库服务器实现

设计理念：
- 服务器端直接操作二进制数据，不进行序列化/反序列化
- 客户端负责数据的序列化，服务器只负责存储和传输
- 服务器端负责TTL验证，确保过期数据不被读取
- 最小化服务器端 CPU 开销，最大化吞吐量
"""

import os
import signal
import threading
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Optional
import msgpack
import zmq
import zmq.asyncio
import zmq.auth
from zmq.auth.thread import ThreadAuthenticator

from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict
from flaxkv2.serialization import encoder, decoder
from flaxkv2.serialization.value_meta import ValueWithMeta
from flaxkv2.utils.log import get_logger
from flaxkv2.utils.key_manager import get_keypair_from_password

# 尝试导入LZ4压缩
try:
    import lz4.frame
    LZ4_AVAILABLE = True
except ImportError:
    LZ4_AVAILABLE = False

logger = get_logger(__name__)


class FlaxKVServer:
    """
    FlaxKV ZeroMQ 服务器
    
    使用 ROUTER socket 支持多客户端并发访问
    服务器端只处理二进制数据，不进行序列化/反序列化
    """
    
    # 命令常量
    CMD_CONNECT = b'CONNECT'
    CMD_DISCONNECT = b'DISCONNECT'
    CMD_GET = b'GET'
    CMD_SET = b'SET'
    CMD_DELETE = b'DELETE'
    CMD_CONTAINS = b'CONTAINS'
    CMD_KEYS = b'KEYS'
    CMD_VALUES = b'VALUES'
    CMD_ITEMS = b'ITEMS'
    CMD_UPDATE = b'UPDATE'
    CMD_STAT = b'STAT'
    CMD_CLEANUP_EXPIRED = b'CLEANUP_EXPIRED'
    CMD_PING = b'PING'
    CMD_LEN = b'LEN'
    CMD_BATCH_WRITE = b'BATCH_WRITE'  # 新增：批量写入命令（支持写缓冲）
    # 注意：SET_TTL 和 GET_TTL 已移除，客户端直接使用 SET/GET 操作 TTL 信息键
    
    # 响应状态
    STATUS_OK = b'OK'
    STATUS_ERROR = b'ERROR'
    STATUS_NOT_FOUND = b'NOT_FOUND'
    
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 5555,
        data_dir: str = ".",
        max_workers: int = 4,
        enable_encryption: bool = False,
        server_secret_key: Optional[str] = None,
        password: Optional[str] = None,
        derive_from_password: bool = True,
        enable_compression: bool = False,
    ):
        """
        初始化服务器

        Args:
            host: 绑定地址
            port: 绑定端口
            data_dir: 数据库存储目录
            max_workers: 工作线程数
            enable_encryption: 启用CurveZMQ加密（默认False）
            server_secret_key: 服务器密钥（Z85编码），None则自动生成
            password: 密码（用于密钥管理），与server_secret_key二选一
            derive_from_password: True=从密码直接派生密钥(推荐)，False=使用文件存储
            enable_compression: 启用LZ4压缩（默认False）
        """
        self.host = host
        self.port = port
        self.data_dir = os.path.abspath(data_dir)
        self.max_workers = max_workers

        # 加密和压缩配置
        self.enable_encryption = enable_encryption
        self.enable_compression = enable_compression

        # 处理密钥：password 和 server_secret_key 二选一
        if enable_encryption:
            if password and server_secret_key:
                raise ValueError("Cannot specify both 'password' and 'server_secret_key'. Please use one.")

            if password:
                # 使用密码管理密钥
                if derive_from_password:
                    logger.info("Deriving keys from password (deterministic)")
                else:
                    logger.info("Loading keys from password-based file storage")

                keypair = get_keypair_from_password(password, derive_from_password=derive_from_password)
                self.server_secret_key = keypair['secret_key']
                self.server_public_key = keypair['public_key']
                logger.info(f"  Server Public Key: {self.server_public_key}")
            else:
                # 使用直接指定的密钥或自动生成
                self.server_secret_key = server_secret_key
                self.server_public_key = None

                # ⚠️ 重要警告：如果启用加密但没有提供密码或密钥，密钥将随机生成
                if not server_secret_key:
                    logger.warning("=" * 80)
                    logger.warning("⚠️  警告：启用了加密但未提供密码或密钥！")
                    logger.warning("⚠️  服务器将生成随机密钥对，客户端需要使用服务器公钥连接。")
                    logger.warning("⚠️  建议：")
                    logger.warning("    1. 使用密码：在配置文件或命令行中提供 --password 参数")
                    logger.warning("    2. 或在客户端使用下方的公钥进行连接")
                    logger.warning("=" * 80)
        else:
            self.server_secret_key = None
            self.server_public_key = None

        # 检查压缩支持
        if self.enable_compression and not LZ4_AVAILABLE:
            raise RuntimeError("LZ4 compression enabled but lz4 package not installed. Run: pip install lz4")

        # 数据库管理
        self.databases: Dict[str, RawLevelDBDict] = {}
        self.db_lock = threading.RLock()

        # ZeroMQ 上下文（使用asyncio）
        self.context = zmq.asyncio.Context()
        self.socket = None
        self.auth = None  # CurveZMQ认证器

        # 运行状态
        self.running = False
        self.executor = None  # 线程池执行器（用于LevelDB同步操作）
        self.pending_tasks = set()  # 跟踪未完成的异步任务

        # 统计信息
        self.stats = {
            'requests': 0,
            'errors': 0,
            'connections': 0,
            'bytes_sent': 0,
            'bytes_received': 0,
            'bytes_compressed': 0,  # 压缩后的字节数
        }
        self.stats_lock = threading.Lock()

        logger.info(f"FlaxKV Server initialized at {host}:{port}, data_dir={data_dir}")
        logger.info(f"  Encryption: {'Enabled' if enable_encryption else 'Disabled'}")
        logger.info(f"  Compression: {'Enabled' if enable_compression else 'Disabled'}")
    
    def _get_or_create_db(self, db_name: str) -> RawLevelDBDict:
        """获取或创建数据库实例"""
        with self.db_lock:
            if db_name not in self.databases:
                logger.info(f"Creating database: {db_name}")
                db = RawLevelDBDict(
                    name=db_name,
                    path=self.data_dir,
                    create_if_missing=True,
                    raw=True  # 使用原始模式，直接存储二进制数据
                )
                self.databases[db_name] = db
                
                with self.stats_lock:
                    self.stats['connections'] += 1
            
            return self.databases[db_name]
    
    def _close_db(self, db_name: str) -> bool:
        """关闭数据库连接"""
        with self.db_lock:
            if db_name in self.databases:
                logger.info(f"Closing database: {db_name}")
                self.databases[db_name].close()
                del self.databases[db_name]
                return True
            return False

    def _compress_data(self, data: bytes) -> bytes:
        """
        压缩数据（如果启用压缩）

        格式：[压缩标志(1byte)][数据]
        - 0x00: 未压缩
        - 0x01: LZ4压缩
        """
        if self.enable_compression:
            compressed = lz4.frame.compress(data)
            with self.stats_lock:
                self.stats['bytes_compressed'] += len(compressed)
            return b'\x01' + compressed
        else:
            return b'\x00' + data

    def _decompress_data(self, data: bytes) -> bytes:
        """
        解压缩数据

        根据首字节判断是否压缩
        """
        if len(data) == 0:
            return data

        compression_flag = data[0]
        payload = data[1:]

        if compression_flag == 0x01:  # LZ4压缩
            return lz4.frame.decompress(payload)
        else:  # 未压缩
            return payload
    
    def _handle_request(self, identity: bytes, request: list) -> list:
        """
        处理客户端请求（支持请求ID）

        Args:
            identity: 客户端标识
            request: 请求数据
                    新格式: [request_id, command, db_name, *args]
                    旧格式: [command, db_name, *args]
                    所有的 key 和 value 都是已序列化的 bytes

        Returns:
            响应数据 [request_id, status, result]
        """
        try:
            with self.stats_lock:
                self.stats['requests'] += 1

            if not request or len(request) < 2:
                return [0, self.STATUS_ERROR, b"Invalid request format"]

            # 检测格式：request[0]是整数→新格式，字节→旧格式
            if isinstance(request[0], int):
                # 新格式：[request_id, command, db_name, *args]
                request_id = request[0]
                command = request[1]
                db_name_bytes = request[2] if len(request) > 2 else b''
                args_offset = 3  # 参数从索引3开始
            else:
                # 旧格式：[command, db_name, *args]（向后兼容）
                request_id = 0
                command = request[0]
                db_name_bytes = request[1] if len(request) > 1 else b''
                args_offset = 2  # 参数从索引2开始
            
            # 将 db_name 从 bytes 转换为 string
            if isinstance(db_name_bytes, bytes):
                db_name = db_name_bytes.decode('utf-8')
            else:
                db_name = db_name_bytes

            # PING 命令不需要数据库
            if command == self.CMD_PING:
                return [request_id, self.STATUS_OK, b'PONG']

            # CONNECT 命令
            if command == self.CMD_CONNECT:
                try:
                    self._get_or_create_db(db_name)
                    return [request_id, self.STATUS_OK, None]
                except Exception as e:
                    logger.error(f"Error connecting to database {db_name}: {e}")
                    return [request_id, self.STATUS_ERROR, str(e).encode('utf-8')]

            # DISCONNECT 命令
            if command == self.CMD_DISCONNECT:
                # 不关闭数据库，保持打开以支持多个并发连接
                # 数据库由服务器统一管理，在服务器停止时关闭
                logger.debug(f"Client disconnected from database: {db_name}")
                return [request_id, self.STATUS_OK, True]

            # 其他命令需要数据库实例
            try:
                db = self._get_or_create_db(db_name)
            except Exception as e:
                logger.error(f"Error getting database {db_name}: {e}")
                return [request_id, self.STATUS_ERROR, f"Database error: {str(e)}".encode('utf-8')]
            
            # 处理各种命令（所有 key/value 都是 bytes，直接操作）
            if command == self.CMD_GET:
                key_bytes = request[args_offset]  # 已序列化的 key
                try:
                    # 读取数据
                    value_bytes = db._db.get(key_bytes)
                    if value_bytes is None:
                        return [request_id, self.STATUS_NOT_FOUND, None]

                    # 检查是否包含TTL元数据并已过期
                    if ValueWithMeta.has_meta(value_bytes):
                        if ValueWithMeta.is_expired_fast(value_bytes):
                            # 已过期，删除键
                            db._db.delete(key_bytes)
                            try:
                                key = decoder.decode_key(key_bytes)
                                logger.debug(f"Deleted expired key: {key}")
                            except:
                                pass
                            return [request_id, self.STATUS_NOT_FOUND, None]

                    return [request_id, self.STATUS_OK, value_bytes]
                except Exception as e:
                    return [request_id, self.STATUS_ERROR, str(e).encode('utf-8')]
            
            elif command == self.CMD_SET:
                key_bytes = request[args_offset]  # 已序列化的 key
                value_bytes = request[args_offset + 1]  # 已序列化的 value
                try:
                    db._db.put(key_bytes, value_bytes)  # 直接写入 LevelDB
                    return [request_id, self.STATUS_OK, None]
                except Exception as e:
                    return [request_id, self.STATUS_ERROR, str(e).encode('utf-8')]

            elif command == self.CMD_DELETE:
                key_bytes = request[args_offset]
                try:
                    # 先检查是否存在
                    if db._db.get(key_bytes) is None:
                        return [request_id, self.STATUS_NOT_FOUND, None]
                    db._db.delete(key_bytes)
                    return [request_id, self.STATUS_OK, None]
                except Exception as e:
                    return [request_id, self.STATUS_ERROR, str(e).encode('utf-8')]

            elif command == self.CMD_CONTAINS:
                key_bytes = request[args_offset]
                try:
                    exists = db._db.get(key_bytes) is not None
                    return [request_id, self.STATUS_OK, exists]
                except Exception as e:
                    return [request_id, self.STATUS_ERROR, str(e).encode('utf-8')]

            elif command == self.CMD_KEYS:
                try:
                    keys = []
                    for key_bytes, _ in db._db:
                        keys.append(key_bytes)
                    return [request_id, self.STATUS_OK, keys]
                except Exception as e:
                    return [request_id, self.STATUS_ERROR, str(e).encode('utf-8')]

            elif command == self.CMD_VALUES:
                try:
                    values = []
                    for _, value_bytes in db._db:
                        values.append(value_bytes)
                    return [request_id, self.STATUS_OK, values]
                except Exception as e:
                    return [request_id, self.STATUS_ERROR, str(e).encode('utf-8')]

            elif command == self.CMD_ITEMS:
                try:
                    items = []
                    for key_bytes, value_bytes in db._db:
                        items.append([key_bytes, value_bytes])
                    return [request_id, self.STATUS_OK, items]
                except Exception as e:
                    return [request_id, self.STATUS_ERROR, str(e).encode('utf-8')]

            elif command == self.CMD_UPDATE:
                items_list = request[args_offset]  # [[key_bytes, value_bytes], ...]
                try:
                    batch = db._db.write_batch()
                    for key_bytes, value_bytes in items_list:
                        batch.put(key_bytes, value_bytes)
                    batch.write()
                    return [request_id, self.STATUS_OK, None]
                except Exception as e:
                    return [request_id, self.STATUS_ERROR, str(e).encode('utf-8')]

            elif command == self.CMD_LEN:
                try:
                    count = sum(1 for _ in db._db)
                    return [request_id, self.STATUS_OK, count]
                except Exception as e:
                    return [request_id, self.STATUS_ERROR, str(e).encode('utf-8')]

            elif command == self.CMD_STAT:
                try:
                    stat = db.stat()
                    return [request_id, self.STATUS_OK, stat]
                except Exception as e:
                    return [request_id, self.STATUS_ERROR, str(e).encode('utf-8')]

            elif command == self.CMD_BATCH_WRITE:
                # 批量写入命令（支持写缓冲）
                # 新格式: [request_id, CMD_BATCH_WRITE, db_name, writes_dict, deletes_list]
                # 旧格式: [CMD_BATCH_WRITE, db_name, writes_dict, deletes_list]
                # writes_dict: {key_bytes: value_bytes}
                # deletes_list: [key_bytes1, key_bytes2, ...]
                try:
                    writes_dict = request[args_offset]  # {key_bytes: value_bytes}
                    deletes_list = request[args_offset + 1]  # [key_bytes1, key_bytes2, ...]

                    # 使用 write_batch 批量操作
                    batch = db._db.write_batch()

                    # 批量写入
                    for key_bytes, value_bytes in writes_dict.items():
                        batch.put(key_bytes, value_bytes)

                    # 批量删除
                    for key_bytes in deletes_list:
                        batch.delete(key_bytes)

                    # 提交批量操作
                    batch.write()

                    logger.debug(f"Batch write completed: {len(writes_dict)} writes, {len(deletes_list)} deletes")
                    return [request_id, self.STATUS_OK, None]
                except Exception as e:
                    logger.error(f"Batch write error: {e}", exc_info=True)
                    return [request_id, self.STATUS_ERROR, str(e).encode('utf-8')]

            elif command == self.CMD_CLEANUP_EXPIRED:
                try:
                    # 遍历数据库，删除所有过期的键
                    count = 0
                    expired_keys = []

                    # 遍历所有键
                    for key_bytes, value_bytes in db._db:
                        # 检查是否包含TTL元数据并已过期
                        if ValueWithMeta.has_meta(value_bytes):
                            if ValueWithMeta.is_expired_fast(value_bytes):
                                expired_keys.append(key_bytes)

                    # 删除过期的键
                    for key_bytes in expired_keys:
                        try:
                            db._db.delete(key_bytes)
                            count += 1
                        except:
                            pass

                    logger.info(f"Cleaned up {count} expired keys")

                    # 返回编码后的计数
                    return [request_id, self.STATUS_OK, encoder.encode(count)]
                except Exception as e:
                    return [request_id, self.STATUS_ERROR, str(e).encode('utf-8')]

            else:
                return [request_id, self.STATUS_ERROR, f"Unknown command: {command}".encode('utf-8')]

        except Exception as e:
            logger.error(f"Error handling request: {e}", exc_info=True)
            with self.stats_lock:
                self.stats['errors'] += 1
            # 异常时使用request_id=0（因为我们可能无法解析request_id）
            return [0, self.STATUS_ERROR, str(e).encode('utf-8')]
    
    async def _server_loop(self):
        """异步服务器主循环（并发处理所有请求）"""
        logger.info(f"Async server loop started with {self.max_workers} workers")

        async def process_request(identity, request_data_compressed):
            """处理单个请求的异步协程"""
            try:
                # 统计接收字节数
                with self.stats_lock:
                    self.stats['bytes_received'] += len(request_data_compressed)

                # 解压缩请求数据
                try:
                    request_data = self._decompress_data(request_data_compressed)
                except Exception as e:
                    logger.error(f"Error decompressing request: {e}")
                    response = [self.STATUS_ERROR, b"Decompression failed"]
                    response_data = msgpack.packb(response, use_bin_type=True)
                    response_data_compressed = self._compress_data(response_data)
                    await self.socket.send_multipart([identity, response_data_compressed])
                    return

                # 解包请求
                try:
                    request = msgpack.unpackb(request_data, raw=True)
                except Exception as e:
                    logger.error(f"Error unpacking request: {e}")
                    response = [self.STATUS_ERROR, b"Invalid request format"]
                    response_data = msgpack.packb(response, use_bin_type=True)
                    response_data_compressed = self._compress_data(response_data)
                    await self.socket.send_multipart([identity, response_data_compressed])
                    return

                # 在线程池中执行同步的LevelDB操作
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    self.executor,
                    self._handle_request,
                    identity,
                    request
                )

                # 打包响应
                response_data = msgpack.packb(response, use_bin_type=True)

                # 压缩响应数据
                response_data_compressed = self._compress_data(response_data)

                # 统计发送字节数
                with self.stats_lock:
                    self.stats['bytes_sent'] += len(response_data_compressed)

                # 异步发送响应
                await self.socket.send_multipart([identity, response_data_compressed])

            except Exception as e:
                logger.error(f"Error processing request: {e}", exc_info=True)

        # 主接收循环
        while self.running:
            try:
                # 异步接收消息（带超时，以便定期检查running标志）
                try:
                    frames = await asyncio.wait_for(
                        self.socket.recv_multipart(),
                        timeout=1.0  # 1秒超时
                    )
                except asyncio.TimeoutError:
                    # 超时后继续循环，检查running标志
                    continue

                if len(frames) < 2:
                    logger.warning(f"Invalid message format: {len(frames)} frames")
                    continue

                identity = frames[0]
                request_data_compressed = frames[1]

                # 创建异步任务处理请求（不等待，立即接收下一个请求）
                task = asyncio.create_task(process_request(identity, request_data_compressed))

                # 跟踪任务（用于优雅关闭）
                self.pending_tasks.add(task)
                task.add_done_callback(self.pending_tasks.discard)

            except asyncio.CancelledError:
                logger.info("Server loop cancelled")
                break
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    logger.info("Context terminated")
                    break
                # 如果服务器正在关闭，任何ZMQ错误都应该退出循环
                if not self.running:
                    logger.info(f"ZMQ error during shutdown: {e}, exiting loop")
                    break
                logger.error(f"ZMQ error in server loop: {e}")
            except Exception as e:
                # 如果服务器正在关闭，任何异常都应该退出循环
                if not self.running:
                    logger.info(f"Exception during shutdown: {e}, exiting loop")
                    break
                logger.error(f"Error in server loop: {e}", exc_info=True)

        logger.info("Server loop stopped")
    
    def start(self, register_signals: bool = True):
        """
        启动服务器

        Args:
            register_signals: 是否注册信号处理器（仅在主线程中有效）
        """
        if self.running:
            logger.warning("Server is already running")
            return

        logger.info(f"Starting FlaxKV Server on {self.host}:{self.port}")

        # 设置 CurveZMQ 加密
        if self.enable_encryption:
            logger.info("Setting up CurveZMQ encryption")

            # 启动认证线程
            self.auth = ThreadAuthenticator(self.context)
            self.auth.start()
            self.auth.configure_curve(domain='*', location=zmq.auth.CURVE_ALLOW_ANY)

            # 生成或使用服务器密钥对
            if self.server_secret_key:
                # 使用提供的密钥，从私钥派生公钥
                self.server_public_key = zmq.curve_public(self.server_secret_key.encode('utf-8')).decode('utf-8')
                logger.info(f"Using provided server keys")
                logger.info(f"  Server Public Key: {self.server_public_key}")
            else:
                # 自动生成密钥对
                public_key, secret_key = zmq.curve_keypair()
                self.server_public_key = public_key.decode('utf-8')
                self.server_secret_key = secret_key.decode('utf-8')
                logger.info(f"Generated new server keys")
                logger.info(f"  Server Public Key: {self.server_public_key}")
                logger.info(f"  Server Secret Key: {self.server_secret_key}")
                logger.warning("⚠️  Please save the server keys for client connections!")

        # 创建 socket
        self.socket = self.context.socket(zmq.ROUTER)

        # 配置 CurveZMQ（必须在 bind 之前）
        if self.enable_encryption:
            self.socket.curve_secretkey = self.server_secret_key.encode('utf-8')
            self.socket.curve_publickey = self.server_public_key.encode('utf-8')
            self.socket.curve_server = True  # 启用服务器模式

        self.socket.bind(f"tcp://{self.host}:{self.port}")

        # 设置 socket 选项
        self.socket.setsockopt(zmq.LINGER, 0)

        # 性能优化：增大发送/接收缓冲区，提升大文件传输吞吐量
        self.socket.setsockopt(zmq.SNDBUF, 10 * 1024 * 1024)  # 10MB发送缓冲区
        self.socket.setsockopt(zmq.RCVBUF, 10 * 1024 * 1024)  # 10MB接收缓冲区

        # 注意：TCP_NODELAY在ZMQ 4.2+中默认启用，无需手动设置

        # 创建线程池用于LevelDB同步操作
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)

        self.running = True

        logger.info(f"Server started (async mode with {self.max_workers} workers)")

        # 只在主线程中注册信号处理
        if register_signals:
            try:
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
            except ValueError:
                # 不在主线程中，跳过信号注册
                logger.debug("Skipping signal registration (not in main thread)")
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
    
    def stop(self):
        """停止服务器"""
        if not self.running:
            return

        logger.info("Stopping FlaxKV Server...")
        self.running = False

        # 先关闭socket，中断阻塞的recv
        if self.socket:
            try:
                self.socket.close()
                self.socket = None
                logger.info("Socket closed")
            except Exception as e:
                logger.error(f"Error closing socket: {e}")

        # 等待所有pending异步任务完成（最多5秒）
        if self.pending_tasks:
            logger.info(f"Waiting for {len(self.pending_tasks)} pending tasks to complete...")
            # 注意：在asyncio.run()退出时，所有任务会自动取消

        # 关闭线程池executor
        if self.executor:
            logger.info("Shutting down executor...")
            # 不等待任务完成，立即关闭（避免hang住）
            # 注：使用 wait=False 可能导致正在执行的任务被中断，但这在关闭时是可接受的
            self.executor.shutdown(wait=False)
            self.executor = None

        # 关闭所有数据库
        with self.db_lock:
            for db_name, db in list(self.databases.items()):
                try:
                    logger.info(f"Closing database: {db_name}")
                    db.close()
                except Exception as e:
                    logger.error(f"Error closing database {db_name}: {e}")
            self.databases.clear()

        # 停止认证器
        if self.auth:
            try:
                self.auth.stop()
            except Exception as e:
                logger.error(f"Error stopping authenticator: {e}")
            self.auth = None

        # 终止 context
        try:
            self.context.term()
        except Exception as e:
            logger.error(f"Error terminating context: {e}")

        # 打印统计信息
        logger.info(f"Server stopped. Stats: {self.stats}")
    
    def run(self):
        """运行服务器（阻塞）"""
        self.start()

        try:
            # 运行异步服务器循环
            asyncio.run(self._server_loop())
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        finally:
            self.stop()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="FlaxKV ZeroMQ Server")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address")
    parser.add_argument("--port", type=int, default=5555, help="Bind port")
    parser.add_argument("--data-dir", default=".", help="Data directory")
    parser.add_argument("--workers", type=int, default=4, help="Number of worker threads")
    
    args = parser.parse_args()
    
    server = FlaxKVServer(
        host=args.host,
        port=args.port,
        data_dir=args.data_dir,
        max_workers=args.workers
    )
    
    server.run()


if __name__ == "__main__":
    main()
