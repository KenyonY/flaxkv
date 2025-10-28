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
        """
        self.name = db_name
        self.db_name = db_name
        self.host = host
        self.port = port
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # ZeroMQ 上下文和 socket
        self.context = zmq.Context()
        self.socket = None
        self.socket_lock = threading.RLock()
        
        # 连接状态
        self._connected = False
        self._closed = False
        
        # 连接到服务器
        self._connect_socket()
        self._connect_db()
        
        logger.info(f"RemoteDBDict connected to {host}:{port}, db={db_name}")
    
    def _connect_socket(self):
        """创建并连接 socket"""
        with self.socket_lock:
            if self.socket:
                self.socket.close()
            
            self.socket = self.context.socket(zmq.DEALER)
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.setsockopt(zmq.RCVTIMEO, self.timeout)
            self.socket.setsockopt(zmq.SNDTIMEO, self.timeout)
            self.socket.connect(f"tcp://{self.host}:{self.port}")
    
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
                    
                    # 发送请求（DEALER socket 自动添加空帧）
                    self.socket.send(request_data)
                    
                    # 接收响应
                    response_data = self.socket.recv()
                    
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
        """获取键值"""
        # 服务器端已经处理TTL检查，客户端不需要额外检查
        # 序列化 key
        key_bytes = encoder.encode_key(key)

        request = [self.CMD_GET, self.db_name.encode('utf-8'), key_bytes]
        status, result = self._send_request(request)

        if status == self.STATUS_NOT_FOUND:
            raise KeyError(key)
        elif status == self.STATUS_ERROR:
            raise RuntimeError(f"Server error: {result.decode('utf-8')}")

        # 反序列化 value
        return decoder.decode(result)
    
    def __setitem__(self, key: Any, value: Any):
        """设置键值"""
        # 序列化 key 和 value
        key_bytes = encoder.encode_key(key)
        value_bytes = encoder.encode(value)
        
        request = [self.CMD_SET, self.db_name.encode('utf-8'), key_bytes, value_bytes]
        status, result = self._send_request(request)
        
        if status == self.STATUS_ERROR:
            raise RuntimeError(f"Server error: {result.decode('utf-8')}")
    
    def __delitem__(self, key: Any):
        """删除键"""
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
        """关闭连接"""
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
