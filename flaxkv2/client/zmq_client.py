"""
FlaxKV2 ZeroMQ 客户端
高性能的远程数据库客户端实现

设计理念：
- 同步包装器 (RemoteDBDict) 内部使用异步客户端 (AsyncRemoteDBDict)
- 只维护一套核心代码（异步实现），同步接口通过包装器提供
- 保持向后兼容性
"""

import asyncio
import time
import threading
from typing import Any, Dict, List, Tuple, Optional

from flaxkv2.client.async_zmq_client import AsyncRemoteDBDict
from flaxkv2.utils.log import get_logger
from flaxkv2.auto_close import db_close_manager

logger = get_logger(__name__)


class RemoteDBDict:
    """
    同步远程数据库客户端（AsyncRemoteDBDict 的包装器）

    设计：
    - 内部使用 AsyncRemoteDBDict（异步实现）
    - 通过事件循环提供同步接口
    - 保持向后兼容性

    提供与本地数据库相同的字典接口，但数据存储在远程服务器
    """

    def __init__(
        self,
        db_name: str,
        host: str = "127.0.0.1",
        port: int = 5555,
        timeout: int = 5000,  # 毫秒
        connect_timeout: int = 5000,  # 毫秒
        max_retries: int = 3,
        retry_delay: float = 0.1,  # 秒
        read_cache_size: int = 0,  # 读缓存大小（已废弃，保留用于兼容性）
        enable_encryption: bool = False,  # 启用CurveZMQ加密
        server_public_key: Optional[str] = None,  # 服务器公钥（Z85编码）
        password: Optional[str] = None,  # 密码（自动管理密钥）
        derive_from_password: bool = True,  # 从密码派生密钥
        enable_compression: bool = False,  # 启用LZ4压缩（已废弃，保留用于兼容性）
        # 写缓冲参数（已废弃，保留用于兼容性）
        enable_write_buffer: bool = False,
        write_buffer_size: int = 100,
        write_buffer_flush_interval: int = 60,
    ):
        """
        初始化远程数据库客户端（同步包装器）

        Args:
            db_name: 数据库名称
            host: 服务器地址
            port: 服务器端口
            timeout: 数据请求超时时间（毫秒，0表示无限制）
            connect_timeout: 连接超时时间（毫秒）
            max_retries: 最大重试次数（已废弃，保留用于兼容性）
            retry_delay: 重试延迟（已废弃，保留用于兼容性）
            read_cache_size: 读缓存大小（已废弃，保留用于兼容性）
            enable_encryption: 启用CurveZMQ加密（默认False）
            server_public_key: 服务器公钥（Z85编码），与password二选一
            password: 密码（用于密钥管理），与server_public_key二选一
            derive_from_password: True=从密码直接派生密钥(推荐)，False=使用文件存储
            enable_compression: 启用LZ4压缩（已废弃，保留用于兼容性）
            enable_write_buffer: 是否启用写缓冲（已废弃，保留用于兼容性）
            write_buffer_size: 写缓冲区大小（已废弃，保留用于兼容性）
            write_buffer_flush_interval: 写缓冲刷新间隔（已废弃，保留用于兼容性）

        注意：
            - 缓存和写缓冲功能已移除（简化设计）
            - 若需要缓存，请直接使用 AsyncRemoteDBDict
        """
        self.name = db_name
        self.db_name = db_name
        self.db_path = f"tcp://{host}:{port}/{db_name}"  # 虚拟路径，用于日志显示
        self.host = host
        self.port = port
        self._closed = False

        # 创建后台事件循环线程
        self._loop = None
        self._loop_thread = None
        self._async_client = None

        # 启动后台事件循环
        self._start_event_loop(
            db_name=db_name,
            host=host,
            port=port,
            timeout=timeout,
            connect_timeout=connect_timeout,
            enable_encryption=enable_encryption,
            password=password,
            server_public_key=server_public_key,
            derive_from_password=derive_from_password
        )

        logger.info(f"RemoteDBDict (sync wrapper) connected to {host}:{port}, db={db_name}")
        logger.info(f"  Encryption: {'Enabled' if enable_encryption else 'Disabled'}")

        # 注册到自动关闭管理器（程序退出时自动清理）
        db_close_manager.register(self)
        logger.debug(f"远程数据库实例已注册到自动关闭管理器: {self.db_path}")

    def _start_event_loop(self, **kwargs):
        """启动后台事件循环线程"""
        def run_loop():
            # 创建新的事件循环
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

            # 在事件循环中创建并连接异步客户端
            url = f"tcp://{kwargs['host']}:{kwargs['port']}"
            self._async_client = AsyncRemoteDBDict(
                db_name=kwargs['db_name'],
                url=url,
                timeout=kwargs['timeout'],
                connect_timeout=kwargs['connect_timeout'],
                enable_encryption=kwargs['enable_encryption'],
                password=kwargs['password'],
                server_public_key=kwargs['server_public_key'],
                derive_from_password=kwargs['derive_from_password']
            )

            # 连接到服务器
            loop.run_until_complete(self._async_client.connect())

            # 运行事件循环直到被停止
            loop.run_forever()

            # 清理
            loop.run_until_complete(self._async_client.close())
            loop.close()

        # 启动后台线程
        self._loop_thread = threading.Thread(target=run_loop, daemon=True)
        self._loop_thread.start()

        # 等待事件循环启动
        for _ in range(100):  # 最多等待1秒
            if self._loop is not None and self._async_client is not None:
                break
            time.sleep(0.01)
        else:
            raise RuntimeError("Failed to start event loop")

    def _run_async(self, coro):
        """在后台事件循环中运行异步协程（线程安全）"""
        if self._closed:
            raise RuntimeError("Client is closed")

        # 使用 asyncio.run_coroutine_threadsafe 在后台循环中运行协程
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    def __getitem__(self, key: Any) -> Any:
        """获取键值"""
        return self._run_async(self._async_client.get(key))

    def __setitem__(self, key: Any, value: Any):
        """设置键值（无TTL）"""
        self._run_async(self._async_client.set(key, value))

    def __delitem__(self, key: Any):
        """删除键"""
        self._run_async(self._async_client.delete(key))

    def __contains__(self, key: Any) -> bool:
        """检查键是否存在"""
        try:
            self._run_async(self._async_client.get(key))
            return True
        except KeyError:
            return False

    def __len__(self) -> int:
        """返回数据库大小"""
        keys = self._run_async(self._async_client.keys())
        return len(keys)

    def get(self, key: Any, default: Any = None) -> Any:
        """获取键值，不存在返回默认值"""
        try:
            return self[key]
        except KeyError:
            return default

    def set(self, key: Any, value: Any, ttl: Optional[int] = None):
        """设置键值（支持指定TTL）"""
        self._run_async(self._async_client.set(key, value, ttl))

    def keys(self) -> List[Any]:
        """获取所有键"""
        return self._run_async(self._async_client.keys())

    def values(self) -> List[Any]:
        """获取所有值"""
        return [self[key] for key in self.keys()]

    def items(self) -> List[Tuple[Any, Any]]:
        """获取所有键值对"""
        return [(key, self[key]) for key in self.keys()]

    def update(self, d: Dict[Any, Any]):
        """批量更新"""
        for k, v in d.items():
            self[k] = v

    def pop(self, key: Any, default: Any = None) -> Any:
        """弹出键值对"""
        try:
            value = self[key]
            del self[key]
            return value
        except KeyError:
            return default

    def batch_set(self, items: Dict[Any, Any], ttl: Optional[int] = None):
        """批量设置多个键值对（使用 WriteBatch）"""
        self._run_async(self._async_client.batch_set(items, ttl))

    def ping(self) -> bool:
        """测试连接"""
        return self._run_async(self._async_client.ping())

    def to_dict(self) -> Dict[Any, Any]:
        """转换为普通字典"""
        result = {}
        for k, v in self.items():
            result[k] = v
        return result

    def flush(self):
        """手动刷新缓存（兼容性方法，无实际作用）"""
        pass  # 异步客户端无缓存，保留此方法仅用于兼容性

    def close(self):
        """关闭连接"""
        if self._closed:
            return

        self._closed = True

        # 停止事件循环
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)

        # 等待线程结束（最多1秒）
        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=1.0)

        # 从自动关闭管理器中移除
        db_close_manager.unregister(self)
        logger.debug(f"远程数据库实例已从自动关闭管理器移除: {self.db_path}")

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

        return f"RemoteDBDict(name={self.name!r}, host={self.host!r}, port={self.port})"

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


# ==================== 兼容性别名 ====================
# 保留旧的类名以便向后兼容
ZMQRemoteDict = RemoteDBDict
