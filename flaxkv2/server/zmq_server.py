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
from typing import Dict, Any, Optional
import msgpack
import zmq
import zmq.auth
from zmq.auth.thread import ThreadAuthenticator

from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict
from flaxkv2.serialization import encoder, decoder
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
        else:
            self.server_secret_key = None
            self.server_public_key = None

        # 检查压缩支持
        if self.enable_compression and not LZ4_AVAILABLE:
            raise RuntimeError("LZ4 compression enabled but lz4 package not installed. Run: pip install lz4")

        # 数据库管理
        self.databases: Dict[str, RawLevelDBDict] = {}
        self.db_lock = threading.RLock()

        # ZeroMQ 上下文
        self.context = zmq.Context()
        self.socket = None
        self.auth = None  # CurveZMQ认证器

        # 运行状态
        self.running = False
        self.worker_threads = []

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
        处理客户端请求
        
        Args:
            identity: 客户端标识
            request: 请求数据 [command, db_name, *args]
                    所有的 key 和 value 都是已序列化的 bytes
            
        Returns:
            响应数据 [status, result]
        """
        try:
            with self.stats_lock:
                self.stats['requests'] += 1
            
            if not request or len(request) < 2:
                return [self.STATUS_ERROR, b"Invalid request format"]
            
            command = request[0]
            db_name_bytes = request[1]
            
            # 将 db_name 从 bytes 转换为 string
            if isinstance(db_name_bytes, bytes):
                db_name = db_name_bytes.decode('utf-8')
            else:
                db_name = db_name_bytes
            
            # PING 命令不需要数据库
            if command == self.CMD_PING:
                return [self.STATUS_OK, b'PONG']
            
            # CONNECT 命令
            if command == self.CMD_CONNECT:
                try:
                    self._get_or_create_db(db_name)
                    return [self.STATUS_OK, None]
                except Exception as e:
                    logger.error(f"Error connecting to database {db_name}: {e}")
                    return [self.STATUS_ERROR, str(e).encode('utf-8')]
            
            # DISCONNECT 命令
            if command == self.CMD_DISCONNECT:
                success = self._close_db(db_name)
                return [self.STATUS_OK, success]
            
            # 其他命令需要数据库实例
            try:
                db = self._get_or_create_db(db_name)
            except Exception as e:
                logger.error(f"Error getting database {db_name}: {e}")
                return [self.STATUS_ERROR, f"Database error: {str(e)}".encode('utf-8')]
            
            # 处理各种命令（所有 key/value 都是 bytes，直接操作）
            if command == self.CMD_GET:
                key_bytes = request[2]  # 已序列化的 key
                try:
                    # 检查是否是内部键（TTL信息键、嵌套标记键等）
                    # 内部键不需要TTL检查
                    is_internal_key = False
                    try:
                        decoded_key = decoder.decode_key(key_bytes)
                        if isinstance(decoded_key, str) and (
                            decoded_key.startswith('__ttl_info__:') or
                            decoded_key.startswith('__nested__:')
                        ):
                            is_internal_key = True
                    except:
                        pass

                    # 对于普通键，检查TTL
                    if not is_internal_key:
                        try:
                            # 解码键名以构造TTL键
                            key = decoder.decode_key(key_bytes)
                            ttl_key = f"__ttl_info__:{key}"
                            ttl_key_bytes = encoder.encode_key(ttl_key)

                            # 获取TTL信息
                            ttl_value_bytes = db._db.get(ttl_key_bytes)

                            if ttl_value_bytes is not None:
                                # 解析过期时间
                                expiry_time = float(decoder.decode(ttl_value_bytes))

                                # 检查是否过期
                                if time.time() > expiry_time:
                                    # 已过期，删除键和TTL信息
                                    db._db.delete(key_bytes)
                                    db._db.delete(ttl_key_bytes)
                                    logger.debug(f"Deleted expired key: {key}")
                                    return [self.STATUS_NOT_FOUND, None]
                        except Exception as e:
                            # TTL检查失败不影响正常读取
                            logger.debug(f"TTL check failed for key: {e}")

                    # 正常读取数据
                    value_bytes = db._db.get(key_bytes)
                    if value_bytes is None:
                        return [self.STATUS_NOT_FOUND, None]
                    return [self.STATUS_OK, value_bytes]
                except Exception as e:
                    return [self.STATUS_ERROR, str(e).encode('utf-8')]
            
            elif command == self.CMD_SET:
                key_bytes = request[2]  # 已序列化的 key
                value_bytes = request[3]  # 已序列化的 value
                try:
                    db._db.put(key_bytes, value_bytes)  # 直接写入 LevelDB
                    return [self.STATUS_OK, None]
                except Exception as e:
                    return [self.STATUS_ERROR, str(e).encode('utf-8')]
            
            elif command == self.CMD_DELETE:
                key_bytes = request[2]
                try:
                    # 先检查是否存在
                    if db._db.get(key_bytes) is None:
                        return [self.STATUS_NOT_FOUND, None]
                    db._db.delete(key_bytes)
                    return [self.STATUS_OK, None]
                except Exception as e:
                    return [self.STATUS_ERROR, str(e).encode('utf-8')]
            
            elif command == self.CMD_CONTAINS:
                key_bytes = request[2]
                try:
                    exists = db._db.get(key_bytes) is not None
                    return [self.STATUS_OK, exists]
                except Exception as e:
                    return [self.STATUS_ERROR, str(e).encode('utf-8')]
            
            elif command == self.CMD_KEYS:
                try:
                    keys = []
                    for key_bytes, _ in db._db:
                        keys.append(key_bytes)
                    return [self.STATUS_OK, keys]
                except Exception as e:
                    return [self.STATUS_ERROR, str(e).encode('utf-8')]
            
            elif command == self.CMD_VALUES:
                try:
                    values = []
                    for _, value_bytes in db._db:
                        values.append(value_bytes)
                    return [self.STATUS_OK, values]
                except Exception as e:
                    return [self.STATUS_ERROR, str(e).encode('utf-8')]
            
            elif command == self.CMD_ITEMS:
                try:
                    items = []
                    for key_bytes, value_bytes in db._db:
                        items.append([key_bytes, value_bytes])
                    return [self.STATUS_OK, items]
                except Exception as e:
                    return [self.STATUS_ERROR, str(e).encode('utf-8')]
            
            elif command == self.CMD_UPDATE:
                items_list = request[2]  # [[key_bytes, value_bytes], ...]
                try:
                    batch = db._db.write_batch()
                    for key_bytes, value_bytes in items_list:
                        batch.put(key_bytes, value_bytes)
                    batch.write()
                    return [self.STATUS_OK, None]
                except Exception as e:
                    return [self.STATUS_ERROR, str(e).encode('utf-8')]
            
            elif command == self.CMD_LEN:
                try:
                    count = sum(1 for _ in db._db)
                    return [self.STATUS_OK, count]
                except Exception as e:
                    return [self.STATUS_ERROR, str(e).encode('utf-8')]
            
            elif command == self.CMD_STAT:
                try:
                    stat = db.stat()
                    return [self.STATUS_OK, stat]
                except Exception as e:
                    return [self.STATUS_ERROR, str(e).encode('utf-8')]
            
            elif command == self.CMD_CLEANUP_EXPIRED:
                try:
                    count = db.cleanup_expired()
                    return [self.STATUS_OK, count]
                except Exception as e:
                    return [self.STATUS_ERROR, str(e).encode('utf-8')]
            
            else:
                return [self.STATUS_ERROR, f"Unknown command: {command}".encode('utf-8')]
        
        except Exception as e:
            logger.error(f"Error handling request: {e}", exc_info=True)
            with self.stats_lock:
                self.stats['errors'] += 1
            return [self.STATUS_ERROR, str(e).encode('utf-8')]
    
    def _server_loop(self):
        """服务器主循环（单线程处理所有请求）"""
        logger.info("Server loop started")

        while self.running:
            try:
                # 接收消息（非阻塞）
                if self.socket.poll(timeout=100):  # 100ms 超时
                    # 接收多帧消息
                    # DEALER 发送 [request_data]
                    # ROUTER 接收 [identity, request_data]
                    frames = self.socket.recv_multipart(zmq.NOBLOCK)

                    if len(frames) < 2:
                        logger.warning(f"Invalid message format: {len(frames)} frames: {frames}")
                        continue

                    identity = frames[0]
                    request_data_compressed = frames[1]

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
                        self.socket.send_multipart([identity, response_data_compressed], zmq.NOBLOCK)
                        continue

                    # 解包请求
                    try:
                        request = msgpack.unpackb(request_data, raw=True)  # raw=True 保持 bytes
                    except Exception as e:
                        logger.error(f"Error unpacking request: {e}")
                        response = [self.STATUS_ERROR, b"Invalid request format"]
                        response_data = msgpack.packb(response, use_bin_type=True)
                        response_data_compressed = self._compress_data(response_data)
                        self.socket.send_multipart([identity, response_data_compressed], zmq.NOBLOCK)
                        continue

                    # 处理请求
                    response = self._handle_request(identity, request)

                    # 打包响应
                    response_data = msgpack.packb(response, use_bin_type=True)

                    # 压缩响应数据
                    response_data_compressed = self._compress_data(response_data)

                    # 统计发送字节数
                    with self.stats_lock:
                        self.stats['bytes_sent'] += len(response_data_compressed)

                    # 发送响应
                    # ROUTER 发送 [identity, response_data]
                    # DEALER 接收 [response_data]
                    self.socket.send_multipart([identity, response_data_compressed], zmq.NOBLOCK)

            except zmq.Again:
                # 非阻塞操作，没有消息
                continue
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    # Context 被终止
                    break
                logger.error(f"ZMQ error in server loop: {e}")
            except Exception as e:
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

        self.running = True

        logger.info(f"Server started (single-threaded mode)")

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

        # 等待工作线程结束
        for thread in self.worker_threads:
            thread.join(timeout=5.0)

        # 关闭所有数据库
        with self.db_lock:
            for db_name, db in list(self.databases.items()):
                try:
                    logger.info(f"Closing database: {db_name}")
                    db.close()
                except Exception as e:
                    logger.error(f"Error closing database {db_name}: {e}")
            self.databases.clear()

        # 关闭 socket
        if self.socket:
            self.socket.close()
            self.socket = None

        # 停止认证器
        if self.auth:
            try:
                self.auth.stop()
            except Exception as e:
                logger.error(f"Error stopping authenticator: {e}")
            self.auth = None

        # 终止 context
        self.context.term()

        # 打印统计信息
        logger.info(f"Server stopped. Stats: {self.stats}")
    
    def run(self):
        """运行服务器（阻塞）"""
        self.start()
        
        try:
            # 运行服务器循环
            self._server_loop()
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
