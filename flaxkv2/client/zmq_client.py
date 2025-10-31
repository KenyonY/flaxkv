"""
FlaxKV2 ZeroMQ 客户端
高性能的远程数据库客户端实现

设计理念：
- 客户端负责所有的序列化/反序列化
- 服务器端只处理二进制数据
- 使用 FlaxKV2 统一的序列化系统
"""

import time
import threading
from typing import Any, Dict, List, Tuple, Optional
import msgpack
import zmq

from flaxkv2.serialization import encoder, decoder
from flaxkv2.utils.log import get_logger
from flaxkv2.utils.simple_lru_cache import SimpleLRUCache
from flaxkv2.utils.key_manager import get_keypair_from_password

# 尝试导入LZ4压缩
try:
    import lz4.frame
    LZ4_AVAILABLE = True
except ImportError:
    LZ4_AVAILABLE = False

logger = get_logger(__name__)


class RemoteDBDict:
    """
    基于 ZeroMQ 的远程数据库字典
    
    提供与本地数据库相同的字典接口，但数据存储在远程服务器
    客户端负责序列化，服务器只负责存储和传输二进制数据
    """
    
    # 命令常量（与服务器保持一致）
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
    # 注意：SET_TTL 和 GET_TTL 不再作为独立命令，客户端直接使用 SET/GET 操作 TTL 信息键
    
    # 响应状态
    STATUS_OK = b'OK'
    STATUS_ERROR = b'ERROR'
    STATUS_NOT_FOUND = b'NOT_FOUND'
    
    def __init__(
        self,
        db_name: str,
        host: str = "127.0.0.1",
        port: int = 5555,
        timeout: int = 5000,  # 毫秒
        max_retries: int = 3,
        retry_delay: float = 0.1,  # 秒
        read_cache_size: int = 0,  # 读缓存大小（条目数量）
        enable_encryption: bool = False,  # 启用CurveZMQ加密
        server_public_key: Optional[str] = None,  # 服务器公钥（Z85编码）
        password: Optional[str] = None,  # 密码（自动管理密钥）
        derive_from_password: bool = True,  # 从密码派生密钥
        enable_compression: bool = False,  # 启用LZ4压缩
    ):
        """
        初始化远程数据库客户端

        Args:
            db_name: 数据库名称
            host: 服务器地址
            port: 服务器端口
            timeout: 请求超时时间（毫秒）
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
            read_cache_size: 读缓存大小（条目数量），0表示禁用缓存（默认）
                推荐值：1000-10000，可显著减少网络请求
            enable_encryption: 启用CurveZMQ加密（默认False）
            server_public_key: 服务器公钥（Z85编码），与password二选一
            password: 密码（用于密钥管理），与server_public_key二选一
            derive_from_password: True=从密码直接派生密钥(推荐)，False=使用文件存储
            enable_compression: 启用LZ4压缩（默认False）
        """
        self.name = db_name
        self.db_name = db_name
        self.host = host
        self.port = port
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # 加密和压缩配置
        self.enable_encryption = enable_encryption
        self.enable_compression = enable_compression
        self.client_public_key = None
        self.client_secret_key = None

        # 处理密钥：password 和 server_public_key 二选一
        if enable_encryption:
            if password and server_public_key:
                raise ValueError("Cannot specify both 'password' and 'server_public_key'. Please use one.")

            if password:
                # 使用密码管理密钥
                if derive_from_password:
                    logger.info("Deriving keys from password (deterministic)")
                else:
                    logger.info("Loading keys from password-based file storage")

                keypair = get_keypair_from_password(password, derive_from_password=derive_from_password)
                self.server_public_key = keypair['public_key']
                logger.debug(f"  Server Public Key: {self.server_public_key}")
            elif server_public_key:
                # 使用直接指定的公钥
                self.server_public_key = server_public_key
            else:
                raise ValueError("enable_encryption=True requires either 'password' or 'server_public_key'")
        else:
            self.server_public_key = None

        # 检查压缩支持
        if self.enable_compression and not LZ4_AVAILABLE:
            raise RuntimeError("LZ4 compression enabled but lz4 package not installed. Run: pip install lz4")

        # ZeroMQ 上下文和 socket
        self.context = zmq.Context()
        self.socket = None
        self.socket_lock = threading.RLock()

        # 连接状态
        self._connected = False
        self._closed = False

        # 初始化读缓存
        self._cache_enabled = read_cache_size > 0
        if self._cache_enabled:
            self._cache = SimpleLRUCache(maxsize=read_cache_size)
            logger.debug(f"读缓存已启用: {read_cache_size} 条目")
        else:
            self._cache = None
            logger.debug("读缓存已禁用")

        # 连接到服务器
        self._connect_socket()
        self._connect_db()

        logger.info(f"RemoteDBDict connected to {host}:{port}, db={db_name}")
        logger.info(f"  Encryption: {'Enabled' if enable_encryption else 'Disabled'}")
        logger.info(f"  Compression: {'Enabled' if enable_compression else 'Disabled'}")
    
    def _connect_socket(self):
        """创建并连接 socket"""
        with self.socket_lock:
            if self.socket:
                self.socket.close()

            self.socket = self.context.socket(zmq.DEALER)
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.setsockopt(zmq.RCVTIMEO, self.timeout)
            self.socket.setsockopt(zmq.SNDTIMEO, self.timeout)

            # 配置 CurveZMQ 加密（必须在 connect 之前）
            if self.enable_encryption:
                # 生成客户端密钥对
                client_public, client_secret = zmq.curve_keypair()
                self.client_public_key = client_public.decode('utf-8')
                self.client_secret_key = client_secret.decode('utf-8')

                # 设置客户端密钥
                self.socket.curve_secretkey = client_secret
                self.socket.curve_publickey = client_public

                # 设置服务器公钥
                self.socket.curve_serverkey = self.server_public_key.encode('utf-8')

                logger.debug(f"CurveZMQ encryption enabled")
                logger.debug(f"  Client Public Key: {self.client_public_key}")
                logger.debug(f"  Server Public Key: {self.server_public_key}")

            self.socket.connect(f"tcp://{self.host}:{self.port}")

    def _compress_data(self, data: bytes) -> bytes:
        """
        压缩数据（如果启用压缩）

        格式：[压缩标志(1byte)][数据]
        - 0x00: 未压缩
        - 0x01: LZ4压缩
        """
        if self.enable_compression:
            compressed = lz4.frame.compress(data)
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

    def _send_request(self, request: list, retry: bool = True) -> list:
        """
        发送请求并接收响应

        Args:
            request: 请求数据
            retry: 是否在失败时重试

        Returns:
            响应数据 [status, result]
        """
        retries = self.max_retries if retry else 1

        for attempt in range(retries):
            try:
                with self.socket_lock:
                    # 打包请求（raw=True 保持 bytes）
                    request_data = msgpack.packb(request, use_bin_type=True)

                    # 压缩请求数据
                    request_data_compressed = self._compress_data(request_data)

                    # 发送请求（DEALER socket 自动添加空帧）
                    self.socket.send(request_data_compressed)

                    # 接收响应
                    response_data_compressed = self.socket.recv()

                    # 解压缩响应数据
                    response_data = self._decompress_data(response_data_compressed)

                    # 解包响应（raw=True 保持 bytes）
                    response = msgpack.unpackb(response_data, raw=True)

                    return response

            except zmq.Again:
                # 超时
                logger.warning(f"Request timeout (attempt {attempt + 1}/{retries})")
                if attempt < retries - 1:
                    time.sleep(self.retry_delay)
                    # 重新连接 socket
                    self._connect_socket()
                else:
                    raise TimeoutError(f"Request timeout after {retries} attempts")

            except zmq.ZMQError as e:
                logger.error(f"ZMQ error: {e}")
                if attempt < retries - 1:
                    time.sleep(self.retry_delay)
                    self._connect_socket()
                else:
                    raise

            except Exception as e:
                logger.error(f"Error sending request: {e}")
                if attempt < retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise

        raise RuntimeError("Failed to send request")
    
    def _connect_db(self):
        """连接到远程数据库"""
        request = [self.CMD_CONNECT, self.db_name.encode('utf-8')]
        status, result = self._send_request(request)
        
        if status != self.STATUS_OK:
            raise RuntimeError(f"Failed to connect to database: {result}")
        
        self._connected = True
    
    def ping(self) -> bool:
        """测试连接"""
        try:
            request = [self.CMD_PING, b'']  # 添加空的 db_name
            status, result = self._send_request(request, retry=False)
            return status == self.STATUS_OK and result == b'PONG'
        except Exception:
            return False
    
    def _get_ttl_key(self, key: Any) -> str:
        """获取TTL信息键"""
        if isinstance(key, str):
            key_str = key
        elif isinstance(key, bytes):
            try:
                key_str = key.decode('utf-8')
            except:
                key_str = str(key)
        else:
            key_str = str(key)

        return "__ttl_info__:" + key_str

    def __getitem__(self, key: Any) -> Any:
        """获取键值（支持缓存）"""
        # 检查缓存（自动检查TTL）
        if self._cache_enabled:
            cached = self._cache.get(key)
            if cached is not None:
                return cached

        # 从服务器读取
        key_bytes = encoder.encode_key(key)
        request = [self.CMD_GET, self.db_name.encode('utf-8'), key_bytes]
        status, result = self._send_request(request)

        if status == self.STATUS_NOT_FOUND:
            raise KeyError(key)
        elif status == self.STATUS_ERROR:
            raise RuntimeError(f"Server error: {result.decode('utf-8')}")

        # 反序列化 value
        value = decoder.decode(result)

        # 尝试获取 TTL 信息并缓存
        if self._cache_enabled:
            expire_time = None
            # 尝试读取 TTL（如果不是内部键）
            if not (isinstance(key, str) and (key.startswith('__ttl_info__:') or key.startswith('__nested__:'))):
                try:
                    ttl_remaining = self.get_ttl(key)
                    if ttl_remaining is not None:
                        expire_time = time.time() + ttl_remaining
                except:
                    pass

            self._cache.put(key, value, expire_time)

        return value
    
    def __setitem__(self, key: Any, value: Any):
        """设置键值（更新缓存）"""
        # 序列化 key 和 value
        key_bytes = encoder.encode_key(key)
        value_bytes = encoder.encode(value)

        request = [self.CMD_SET, self.db_name.encode('utf-8'), key_bytes, value_bytes]
        status, result = self._send_request(request)

        if status == self.STATUS_ERROR:
            raise RuntimeError(f"Server error: {result.decode('utf-8')}")

        # 更新缓存（write-through）
        if self._cache_enabled:
            # 不是内部键才缓存
            if not (isinstance(key, str) and (key.startswith('__ttl_info__:') or key.startswith('__nested__:'))):
                # 尝试获取TTL
                expire_time = None
                try:
                    ttl_remaining = self.get_ttl(key)
                    if ttl_remaining is not None:
                        expire_time = time.time() + ttl_remaining
                except:
                    pass

                self._cache.put(key, value, expire_time)
    
    def __delitem__(self, key: Any):
        """删除键（同时删除缓存）"""
        # 序列化 key
        key_bytes = encoder.encode_key(key)

        request = [self.CMD_DELETE, self.db_name.encode('utf-8'), key_bytes]
        status, result = self._send_request(request)

        if status == self.STATUS_NOT_FOUND:
            raise KeyError(key)
        elif status == self.STATUS_ERROR:
            raise RuntimeError(f"Server error: {result.decode('utf-8')}")

        # 删除TTL信息（如果不是内部键）
        if not (isinstance(key, str) and (key.startswith('__ttl_info__:') or key.startswith('__nested__:'))):
            ttl_key = self._get_ttl_key(key)
            try:
                ttl_key_bytes = encoder.encode_key(ttl_key)
                request = [self.CMD_DELETE, self.db_name.encode('utf-8'), ttl_key_bytes]
                self._send_request(request)
            except:
                pass  # TTL信息不存在也没关系

        # 删除缓存
        if self._cache_enabled:
            self._cache.delete(key)
    
    def __contains__(self, key: Any) -> bool:
        """检查键是否存在"""
        # 序列化 key
        key_bytes = encoder.encode_key(key)
        
        request = [self.CMD_CONTAINS, self.db_name.encode('utf-8'), key_bytes]
        status, result = self._send_request(request)
        
        if status == self.STATUS_ERROR:
            raise RuntimeError(f"Server error: {result.decode('utf-8')}")
        
        return result
    
    def get(self, key: Any, default: Any = None) -> Any:
        """获取键值，不存在返回默认值"""
        try:
            return self[key]
        except KeyError:
            return default
    
    def keys(self) -> List[Any]:
        """获取所有键"""
        request = [self.CMD_KEYS, self.db_name.encode('utf-8')]
        status, result = self._send_request(request)

        if status == self.STATUS_ERROR:
            raise RuntimeError(f"Server error: {result.decode('utf-8')}")

        # 反序列化所有 key，并过滤内部键
        keys = []
        for key_bytes in result:
            key = decoder.decode_key(key_bytes)
            # 过滤内部键（TTL信息键、嵌套标记键等）
            if isinstance(key, str) and (key.startswith('__ttl_info__:') or key.startswith('__nested__:')):
                continue
            keys.append(key)

        return keys

    def values(self) -> List[Any]:
        """获取所有值"""
        # 使用 keys() 和 __getitem__ 来获取值
        # 这样可以确保过滤了内部键，并且应用了TTL检查
        return [self[key] for key in self.keys()]

    def items(self) -> List[Tuple[Any, Any]]:
        """获取所有键值对"""
        # 使用 keys() 和 __getitem__ 来获取键值对
        # 这样可以确保过滤了内部键，并且应用了TTL检查
        return [(key, self[key]) for key in self.keys()]
    
    def update(self, d: Dict[Any, Any]):
        """批量更新"""
        # 序列化所有 key 和 value
        items_list = [
            [encoder.encode_key(k), encoder.encode(v)]
            for k, v in d.items()
        ]
        
        request = [self.CMD_UPDATE, self.db_name.encode('utf-8'), items_list]
        status, result = self._send_request(request)
        
        if status == self.STATUS_ERROR:
            raise RuntimeError(f"Server error: {result.decode('utf-8')}")
    
    def pop(self, key: Any, default: Any = None) -> Any:
        """弹出键值对"""
        try:
            value = self[key]
            del self[key]
            return value
        except KeyError:
            return default
    
    def __len__(self) -> int:
        """返回数据库大小"""
        request = [self.CMD_LEN, self.db_name.encode('utf-8')]
        status, result = self._send_request(request)
        
        if status == self.STATUS_ERROR:
            raise RuntimeError(f"Server error: {result.decode('utf-8')}")
        
        return result
    
    def stat(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        request = [self.CMD_STAT, self.db_name.encode('utf-8')]
        status, result = self._send_request(request)
        
        if status == self.STATUS_ERROR:
            raise RuntimeError(f"Server error: {result.decode('utf-8')}")
        
        return result
    
    def set_ttl(self, key: Any, ttl_seconds: int):
        """设置键的过期时间"""
        # 检查键是否存在
        if key not in self:
            raise KeyError(key)

        # 计算过期时间戳
        expiry_time = time.time() + ttl_seconds

        # 使用普通的 __setitem__ 存储 TTL 信息
        ttl_key = self._get_ttl_key(key)
        self[ttl_key] = expiry_time

    def get_ttl(self, key: Any) -> Optional[int]:
        """获取键的剩余过期时间"""
        ttl_key = self._get_ttl_key(key)

        try:
            expiry_time = self[ttl_key]
            remaining = int(expiry_time - time.time())
            return max(0, remaining)
        except KeyError:
            # 没有设置 TTL
            return None

    def remove_ttl(self, key: Any):
        """移除键的TTL设置"""
        ttl_key = self._get_ttl_key(key)
        try:
            del self[ttl_key]
        except KeyError:
            pass
    
    def cleanup_expired(self) -> int:
        """清理过期键"""
        current_time = time.time()
        count = 0
        expired_keys = []

        # 遍历所有键，找到过期的TTL信息键
        try:
            all_keys = self.keys()
            for key in all_keys:
                if isinstance(key, str) and key.startswith('__ttl_info__:'):
                    # 提取原始键名
                    original_key = key[len('__ttl_info__:'):]

                    try:
                        expiry_time = float(self[key])
                        if current_time > expiry_time:
                            expired_keys.append(original_key)
                    except (ValueError, KeyError):
                        continue

            # 删除过期的键和TTL信息
            for original_key in expired_keys:
                try:
                    if original_key in self:
                        del self[original_key]  # 会自动删除TTL信息
                        count += 1
                except KeyError:
                    pass
        except Exception as e:
            import logging
            logger.error(f"清理过期键失败: {e}")

        return count
    
    def to_dict(self) -> Dict[Any, Any]:
        """转换为普通字典"""
        result = {}
        for k, v in self.items():
            result[k] = v
        return result
    
    def close(self):
        """关闭连接（清理缓存）"""
        if self._closed:
            return

        try:
            # 发送断开连接命令
            request = [self.CMD_DISCONNECT, self.db_name.encode('utf-8')]
            self._send_request(request, retry=False)
        except Exception as e:
            logger.warning(f"Error disconnecting: {e}")

        # 关闭 socket
        with self.socket_lock:
            if self.socket:
                self.socket.close()
                self.socket = None

        # 清理缓存
        if self._cache_enabled:
            self._cache.clear()
            logger.debug(f"缓存已清理: {self.db_name}")

        self._closed = True
        self._connected = False

        logger.info(f"RemoteDBDict closed: {self.db_name}")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.close()
        return False
    
    def __repr__(self):
        """返回对象的字符串表示形式"""
        if self._closed:
            return f"RemoteDBDict(name={self.name!r}, closed=True)"
        
        status = "connected" if self._connected else "disconnected"
        return f"RemoteDBDict(name={self.name!r}, host={self.host!r}, port={self.port}, status={status})"
    
    def __str__(self):
        """返回用户友好的字符串表示形式"""
        if self._closed:
            return f"<RemoteDBDict '{self.name}' (closed)>"
        
        try:
            items = list(self.items())
            num_items = len(items)
            
            if num_items == 0:
                return "{}"
            elif num_items <= 20:
                items_repr = ", ".join(f"{k!r}: {v!r}" for k, v in items)
                return f"{{{items_repr}}}"
            else:
                preview_items = items[:20]
                items_repr = ", ".join(f"{k!r}: {v!r}" for k, v in preview_items)
                return f"{{{items_repr}, ... and {num_items - 20} more items}}"
        except Exception as e:
            return f"<RemoteDBDict '{self.name}' (error: {e})>"
