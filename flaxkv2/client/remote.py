"""
FlaxKV2 远程客户端实现
"""

import json
import time
import urllib.parse
import os
import asyncio
from typing import Any, Dict, List, Tuple, Optional, Iterator, Union
import threading

import aiohttp
from aiohttp.client_exceptions import ClientResponseError

from flaxkv2.core.base import BaseDBDict
from flaxkv2.utils.log import get_logger

logger = get_logger(__name__)


class RemoteDBDict(BaseDBDict):
    """
    远程数据库字典实现，通过HTTP与远程FlaxKV服务器通信
    """
    
    def __init__(
        self,
        db_name: str,
        url: str,
        timeout: float = 10.0,
        max_retries: int = 3,
        retry_delay: float = 0.5,
        default_ttl: int = None,
        root_path: str = None,
        **kwargs
    ):
        """
        初始化远程数据库字典
        
        Args:
            db_name: 数据库名称
            url: 服务器URL
            timeout: 请求超时时间（秒）
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
            default_ttl: 默认TTL，单位为秒。设置后，所有新增的键都会自动应用此TTL
            root_path: 数据库根路径，用于远程连接
        """
        self.url = url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._default_ttl = default_ttl  # 默认TTL
        self.root_path = root_path
        
        # 创建HTTP会话（延迟初始化）
        self._session = None
        self._loop_lock = threading.RLock()  # 用于保护事件循环访问
        
        # 创建一个合理的虚拟路径，用于BaseDBDict的内部表示
        # 即使设置了create_dirs=False，这也可以防止在日志和错误消息中出现奇怪的URL路径
        # 同时，如果将来BaseDBDict的实现变化，这可以作为额外的防御措施
        virtual_path = os.path.join(os.environ.get("TEMP", "/tmp"), "flaxkv_remote", 
                                  urllib.parse.quote_plus(self.url))
        
        # 初始化基类，使用虚拟路径，并指定不创建目录
        super().__init__(name=db_name, path=virtual_path, create_dirs=False, **kwargs)
        
        # 连接远程数据库
        self._connect()
    
    def _init_db(self):
        """初始化数据库，远程模式不需要创建本地文件目录"""
        # 在远程模式下，我们不需要初始化本地数据库，所以这个方法是空的
        # 基类会创建必要的缓冲区和线程
        pass
    
    def _get_session(self):
        """获取aiohttp会话，如果不存在则创建一个"""
        if self._session is None or self._session.closed:
            # 不在初始化时设置超时，而是在每个请求中设置
            self._session = aiohttp.ClientSession()
        return self._session
    
    def _run_async(self, coroutine):
        """运行异步协程并返回结果，确保线程安全"""
        with self._loop_lock:
            # 根据当前线程创建或获取事件循环
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # 如果当前循环正在运行（可能在其他线程中），创建一个新的
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
            except RuntimeError:
                # 如果当前线程没有事件循环
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            try:
                # 运行协程
                return loop.run_until_complete(coroutine)
            except Exception as e:
                # 重新抛出异常，保持与同步API相同的异常类型
                raise type(e)(str(e)) from e
    
    async def _async_request(self, method, endpoint, **kwargs):
        """执行异步HTTP请求，统一处理超时和重试逻辑"""
        session = self._get_session()
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        
        for attempt in range(self.max_retries):
            try:
                async with getattr(session, method)(endpoint, timeout=timeout, **kwargs) as response:
                    return response, await response.text()
            except Exception as e:
                logger.error(f"Error in HTTP {method} request to {endpoint}: {e}")
                if attempt < self.max_retries - 1:
                    logger.debug(f"Retrying request after {self.retry_delay}s...")
                    await asyncio.sleep(self.retry_delay)
                else:
                    raise
                    
    async def _async_connect(self):
        """异步连接远程数据库"""
        endpoint = f"{self.url}/connect"
        
        try:
            # 使用self.root_path或环境变量
            root_path = self.root_path or os.environ.get("FLAXKV_DATA_DIR", ".")
            
            logger.debug(f"Connecting to database {self.name} at {endpoint}")
            response, text = await self._async_request('post', endpoint, json={
                'db_name': self.name,
                'create_if_missing': True,
                'root_path': root_path
            })
            
            if response.status in (200, 201):
                logger.info(f"Connected to remote database {self.name} at {self.url}")
                return
            
            logger.warning(f"Failed to connect to remote database: {text}, status: {response.status}")
                
        except Exception as e:
            logger.error(f"Error connecting to remote database: {e}")
                
        raise ConnectionError(f"Failed to connect to remote database after {self.max_retries} attempts")
    
    def _connect(self):
        """连接远程数据库"""
        return self._run_async(self._async_connect())
    
    async def _async_get_from_db(self, key):
        """异步从远程数据库获取值"""
        endpoint = f"{self.url}/get"
        
        try:
            response, text = await self._async_request('post', endpoint, json={
                'db_name': self.name,
                'key': key
            })
            
            if response.status in (200, 201):
                result = await response.json()
                return result['value']
            elif response.status == 404:
                raise KeyError(key)
            else:
                raise RuntimeError(f"Error getting value: {text}")
                
        except ClientResponseError as e:
            if e.status == 404:
                raise KeyError(key)
            raise RuntimeError(f"HTTP error getting value: {e}")
            
        except KeyError:
            raise
            
        except Exception as e:
            raise RuntimeError(f"Error getting value: {e}")
    
    def _get_from_db(self, key):
        """从远程数据库获取值"""
        return self._run_async(self._async_get_from_db(key))
    
    async def _async_set_batch(self, items: Dict[Any, Any]):
        """异步批量设置键值对"""
        endpoint = f"{self.url}/set_batch"
        
        try:
            response, text = await self._async_request('post', endpoint, json={
                'db_name': self.name,
                'items': items
            })
            
            if response.status in (200, 201):
                return
            
            logger.warning(f"Failed to set batch: {text}")
            raise RuntimeError(f"Failed to set batch: {text}")
                
        except Exception as e:
            logger.error(f"Error setting batch: {e}")
            raise RuntimeError(f"Failed to set batch: {e}")
    
    def _set_batch(self, items: Dict[Any, Any]):
        """批量设置键值对"""
        return self._run_async(self._async_set_batch(items))
    
    async def _async_delete_batch(self, keys: List[Any]):
        """异步批量删除键"""
        endpoint = f"{self.url}/delete_batch"
        
        try:
            response, text = await self._async_request('post', endpoint, json={
                'db_name': self.name,
                'keys': keys
            })
            
            if response.status in (200, 201):
                return
            
            logger.warning(f"Failed to delete batch: {text}")
            raise RuntimeError(f"Failed to delete batch: {text}")
                
        except Exception as e:
            logger.error(f"Error deleting batch: {e}")
            raise RuntimeError(f"Failed to delete batch: {e}")
            
    def _delete_batch(self, keys: List[Any]):
        """批量删除键"""
        return self._run_async(self._async_delete_batch(keys))
    
    def _write_buffer_to_db(self):
        """将缓冲区写入远程数据库"""
        with self._buffer_lock:
            # 创建缓冲区快照
            buffer_dict_snapshot = self._buffer_dict.copy()
            # 清空缓冲区
            self._buffer_dict = {}
            self._buffered_count = 0
        
        if not buffer_dict_snapshot:
            return
        
        # 构建批处理
        to_set = {}
        to_delete = []
        
        for key, value in buffer_dict_snapshot.items():
            if value is None:
                to_delete.append(key)
            else:
                to_set[key] = value
        
        # 批量设置
        if to_set:
            self._set_batch(to_set)
        
        # 批量删除
        if to_delete:
            self._delete_batch(to_delete)
    
    async def _async_delete_from_db(self, key):
        """异步从远程数据库删除键"""
        endpoint = f"{self.url}/delete"
        
        try:
            response, text = await self._async_request('post', endpoint, json={
                'db_name': self.name,
                'key': key
            })
            
            if response.status in (200, 201):
                return
            elif response.status == 404:
                raise KeyError(key)
            else:
                raise RuntimeError(f"Error deleting key: {text}")
                
        except ClientResponseError as e:
            if e.status == 404:
                raise KeyError(key)
            raise RuntimeError(f"HTTP error deleting key: {e}")
            
        except KeyError:
            raise
            
        except Exception as e:
            raise RuntimeError(f"Error deleting key: {e}")
    
    def _delete_from_db(self, key):
        """从远程数据库删除键"""
        return self._run_async(self._async_delete_from_db(key))
    
    async def _async_close_db(self):
        """异步关闭远程数据库连接"""
        try:
            logger.debug(f"发送远程数据库断开请求: {self.name}, URL: {self.url}")
            endpoint = f"{self.url}/disconnect"
            
            # 用try块包装请求部分，以便在请求失败时也能关闭会话
            try:
                response, _ = await self._async_request('post', endpoint, json={
                    'db_name': self.name
                })
                logger.debug(f"远程数据库断开请求已发送: {self.name}")
            except Exception as e:
                logger.error(f"断开远程数据库连接时发生错误: {self.name}, 错误: {e}")
        finally:
            # 确保无论如何都能关闭会话
            if self._session is not None and not self._session.closed:
                logger.debug(f"关闭远程数据库客户端连接: {self.name}")
                await self._session.close()
                logger.debug(f"远程数据库客户端已关闭: {self.name}")
    
    def _close_db(self):
        """关闭远程数据库连接"""
        return self._run_async(self._async_close_db())
    
    async def _async_keys(self) -> List:
        """异步获取所有键列表"""
        endpoint = f"{self.url}/keys"
        
        try:
            response, text = await self._async_request('post', endpoint, json={
                'db_name': self.name
            })
            
            if response.status in (200, 201):
                result = await response.json()
                return result['keys']
            else:
                raise RuntimeError(f"Error getting keys: {text}")
                
        except Exception as e:
            raise RuntimeError(f"Error getting keys: {e}")
    
    def keys(self) -> List:
        """获取所有键列表"""
        # 先刷新缓冲区
        self._write_buffer_to_db()
        return self._run_async(self._async_keys())
    
    def values(self) -> List:
        """获取所有值列表"""
        # 获取所有项，返回值部分
        return [value for _, value in self.items()]
    
    async def _async_items(self) -> List[Tuple]:
        """异步获取所有键值对列表"""
        endpoint = f"{self.url}/dict"
        
        try:
            response, text = await self._async_request('post', endpoint, json={
                'db_name': self.name
            })
            
            if response.status in (200, 201):
                result = await response.json()
                # 将字典转换为键值对列表
                dict_data = result['dict']
                return list(dict_data.items())
            else:
                raise RuntimeError(f"Error getting items: {text}")
                
        except Exception as e:
            raise RuntimeError(f"Error getting items: {e}")
    
    def items(self) -> List[Tuple]:
        """获取所有键值对列表"""
        # 先刷新缓冲区
        self._write_buffer_to_db()
        return self._run_async(self._async_items())
    
    async def _async_stat(self) -> Dict:
        """异步返回数据库统计信息"""
        endpoint = f"{self.url}/stat"
        
        try:
            response, text = await self._async_request('post', endpoint, json={
                'db_name': self.name
            })
            
            if response.status == 200:
                return await response.json()
            else:
                raise RuntimeError(f"Error getting stats: {text}")
                
        except Exception as e:
            raise RuntimeError(f"Error getting stats: {e}")
    
    def stat(self) -> Dict:
        """返回数据库统计信息"""
        # 先刷新缓冲区
        self._write_buffer_to_db()
        return self._run_async(self._async_stat())
    
    async def _async_ping(self):
        """异步检查服务器连接状态"""
        endpoint = f"{self.url}/healthz"
        
        try:
            response, _ = await self._async_request('get', endpoint)
            return response.status == 200
                
        except Exception:
            return False
    
    def ping(self):
        """检查服务器连接状态"""
        return self._run_async(self._async_ping())
    
    def set_default_ttl(self, ttl_seconds: Optional[int]) -> None:
        """
        设置默认TTL值
        
        Args:
            ttl_seconds: 默认TTL秒数，None表示不设置默认TTL
        """
        self._default_ttl = ttl_seconds
    
    def get_default_ttl(self) -> Optional[int]:
        """
        获取当前默认TTL值
        
        Returns:
            int: 默认TTL秒数，None表示未设置默认TTL
        """
        return self._default_ttl
        
    def __setitem__(self, key, value):
        """
        设置键值对，如果设置了默认TTL，则自动应用
        """
        # 调用父类的__setitem__方法
        super().__setitem__(key, value)
        
        # 如果设置了默认TTL，则自动应用
        if self._default_ttl is not None:
            self.set_ttl(key, self._default_ttl)
            
    def set_with_ttl(self, key: Any, value: Any, ttl: int) -> None:
        """
        设置带有TTL的键值对
        
        Args:
            key: 键
            value: 值
            ttl: 过期时间（秒）
        """
        # 先设置值
        self[key] = value
        
        # 再设置TTL
        self.set_ttl(key, ttl)
    
    async def _async_set_ttl(self, key: Any, ttl_seconds: int) -> None:
        """异步设置键的过期时间"""
        endpoint = f"{self.url}/set_ttl"
        
        try:
            response, text = await self._async_request('post', endpoint, json={
                'db_name': self.name,
                'key': key,
                'ttl_seconds': ttl_seconds
            })
            
            if response.status in (200, 201):
                return
            elif response.status == 404:
                raise KeyError(key)
            else:
                logger.warning(f"Failed to set TTL: {text}")
                raise RuntimeError(f"Failed to set TTL: {text}")
                
        except KeyError:
            raise
                
        except Exception as e:
            logger.error(f"Error setting TTL: {key!r}")
            raise RuntimeError(f"Failed to set TTL: {e}")
            
    def set_ttl(self, key: Any, ttl_seconds: int) -> None:
        """
        设置键的过期时间
        
        Args:
            key: 键
            ttl_seconds: 过期时间（秒）
        """
        return self._run_async(self._async_set_ttl(key, ttl_seconds))
    
    async def _async_get_ttl(self, key: Any) -> Optional[float]:
        """异步获取键的剩余过期时间"""
        endpoint = f"{self.url}/get_ttl"
        
        try:
            response, text = await self._async_request('post', endpoint, json={
                'db_name': self.name,
                'key': key
            })
            
            if response.status in (200, 201):
                result = await response.json()
                return result.get('ttl')
            elif response.status == 404:
                raise KeyError(key)
            else:
                logger.warning(f"Failed to get TTL: {text}")
                raise RuntimeError(f"Failed to get TTL: {text}")
                
        except KeyError:
            raise
                
        except Exception as e:
            logger.error(f"Error getting TTL: {e}")
            raise RuntimeError(f"Failed to get TTL: {e}")
    
    def get_ttl(self, key: Any) -> Optional[float]:
        """
        获取键的剩余过期时间
        
        Args:
            key: 键
            
        Returns:
            float: 剩余过期时间（秒），如果没有设置则返回None
        """
        return self._run_async(self._async_get_ttl(key))
    
    def update(self, d: Dict[Any, Any]):
        """
        批量更新多个键值对，如果设置了默认TTL，则对新添加的键应用默认TTL
        """
        # 先获取要添加的新键
        new_keys = []
        for key in d.keys():
            if key not in self:
                new_keys.append(key)
        
        # 调用父类的update方法进行批量更新
        super().update(d)
        
        # 如果设置了默认TTL，则为新键应用默认TTL
        if self._default_ttl is not None and new_keys:
            for key in new_keys:
                try:
                    self.set_ttl(key, self._default_ttl)
                except Exception as e:
                    logger.warning(f"为键 {key} 设置TTL失败: {e}")
                    # 继续处理其他键
                    continue 