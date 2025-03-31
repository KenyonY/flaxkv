"""
FlaxKV2 远程客户端实现
"""

import json
import time
import urllib.parse
import os
from typing import Any, Dict, List, Tuple, Optional, Iterator, Union

import httpx

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
        
        # HTTP客户端
        self._client = httpx.Client(timeout=timeout)
        
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
    
    def _connect(self):
        """连接远程数据库"""
        endpoint = f"{self.url}/connect"
        
        for attempt in range(self.max_retries):
            try:
                # 使用self.root_path或环境变量
                root_path = self.root_path or os.environ.get("FLAXKV_DATA_DIR", ".")
                
                response = self._client.post(
                    endpoint,
                    json={
                        'db_name': self.name,
                        'create_if_missing': True,
                        'root_path': root_path
                    }
                )
                
                if response.status_code in (200, 201):
                    logger.info(f"Connected to remote database {self.name} at {self.url}")
                    return
                
                logger.warning(f"Failed to connect to remote database: {response.text}")
                
            except Exception as e:
                logger.error(f"Error connecting to remote database: {e}")
                
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay)
                
        raise ConnectionError(f"Failed to connect to remote database after {self.max_retries} attempts")
    
    def _get_from_db(self, key):
        """从远程数据库获取值"""
        # 构建URL
        endpoint = f"{self.url}/get"
        
        try:
            # 发送请求
            response = self._client.post(
                endpoint,
                json={
                    'db_name': self.name,
                    'key': key
                }
            )
            
            # 检查响应
            if response.status_code in (200, 201):
                result = response.json()
                return result['value']
            elif response.status_code == 404:
                raise KeyError(key)
            else:
                raise RuntimeError(f"Error getting value: {response.text}")
                
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise KeyError(key)
            raise RuntimeError(f"HTTP error getting value: {e}")
            
        except KeyError:
            raise
            
        except Exception as e:
            raise RuntimeError(f"Error getting value: {e}")
    
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
    
    def _set_batch(self, items: Dict[Any, Any]):
        """批量设置键值对"""
        endpoint = f"{self.url}/set_batch"
        
        for attempt in range(self.max_retries):
            try:
                response = self._client.post(
                    endpoint,
                    json={
                        'db_name': self.name,
                        'items': items
                    }
                )
                
                if response.status_code in (200, 201):
                    return
                
                logger.warning(f"Failed to set batch: {response.text}")
                
            except Exception as e:
                logger.error(f"Error setting batch: {e}")
                
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay)
        
        raise RuntimeError(f"Failed to set batch after {self.max_retries} attempts")
    
    def _delete_batch(self, keys: List[Any]):
        """批量删除键"""
        endpoint = f"{self.url}/delete_batch"
        
        for attempt in range(self.max_retries):
            try:
                response = self._client.post(
                    endpoint,
                    json={
                        'db_name': self.name,
                        'keys': keys
                    }
                )
                
                if response.status_code in (200, 201):
                    return
                
                logger.warning(f"Failed to delete batch: {response.text}")
                
            except Exception as e:
                logger.error(f"Error deleting batch: {e}")
                
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay)
        
        raise RuntimeError(f"Failed to delete batch after {self.max_retries} attempts")
    
    def _delete_from_db(self, key):
        """从远程数据库删除键"""
        endpoint = f"{self.url}/delete"
        
        try:
            response = self._client.post(
                endpoint,
                json={
                    'db_name': self.name,
                    'key': key
                }
            )
            
            if response.status_code in (200, 201):
                return
            elif response.status_code == 404:
                raise KeyError(key)
            else:
                raise RuntimeError(f"Error deleting key: {response.text}")
                
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise KeyError(key)
            raise RuntimeError(f"HTTP error deleting key: {e}")
            
        except KeyError:
            raise
            
        except Exception as e:
            raise RuntimeError(f"Error deleting key: {e}")
    
    def _close_db(self):
        """关闭远程数据库连接"""
        try:
            logger.debug(f"发送远程数据库断开请求: {self.name}, URL: {self.url}")
            endpoint = f"{self.url}/disconnect"
            
            # 用try块包装请求部分，以便在请求失败时也能关闭客户端
            try:
                self._client.post(
                    endpoint,
                    json={
                        'db_name': self.name
                    },
                    timeout=3.0  # 设置较短的超时时间，避免长时间等待
                )
                logger.debug(f"远程数据库断开请求已发送: {self.name}")
            except Exception as e:
                logger.error(f"断开远程数据库连接时发生错误: {self.name}, 错误: {e}")
        finally:
            # 确保无论如何都能关闭客户端
            logger.debug(f"关闭远程数据库客户端连接: {self.name}")
            self._client.close()
            logger.debug(f"远程数据库客户端已关闭: {self.name}")
    
    def keys(self) -> List:
        """获取所有键列表"""
        # 先刷新缓冲区
        self._write_buffer_to_db()
        
        endpoint = f"{self.url}/keys"
        
        try:
            response = self._client.post(
                endpoint,
                json={
                    'db_name': self.name
                }
            )
            
            if response.status_code in (200, 201):
                result = response.json()
                return result['keys']
            else:
                raise RuntimeError(f"Error getting keys: {response.text}")
                
        except Exception as e:
            raise RuntimeError(f"Error getting keys: {e}")
    
    def values(self) -> List:
        """获取所有值列表"""
        # 获取所有项，返回值部分
        return [value for _, value in self.items()]
    
    def items(self) -> List[Tuple]:
        """获取所有键值对列表"""
        # 先刷新缓冲区
        self._write_buffer_to_db()
        
        endpoint = f"{self.url}/dict"
        
        try:
            response = self._client.post(
                endpoint,
                json={
                    'db_name': self.name
                }
            )
            
            if response.status_code in (200, 201):
                result = response.json()
                # 将字典转换为键值对列表
                dict_data = result['dict']
                return list(dict_data.items())
            else:
                raise RuntimeError(f"Error getting items: {response.text}")
                
        except Exception as e:
            raise RuntimeError(f"Error getting items: {e}")
    
    def stat(self) -> Dict:
        """返回数据库统计信息"""
        # 先刷新缓冲区
        self._write_buffer_to_db()
        
        endpoint = f"{self.url}/stat"
        
        try:
            response = self._client.post(
                endpoint,
                json={
                    'db_name': self.name
                }
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                raise RuntimeError(f"Error getting stats: {response.text}")
                
        except Exception as e:
            raise RuntimeError(f"Error getting stats: {e}")
    
    def ping(self):
        """检查服务器连接状态"""
        endpoint = f"{self.url}/healthz"
        
        try:
            response = self._client.get(endpoint)
            
            if response.status_code == 200:
                return True
            else:
                return False
                
        except Exception:
            return False
    
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
            
    def set_ttl(self, key: Any, ttl_seconds: int) -> None:
        """
        设置键的过期时间
        
        Args:
            key: 键
            ttl_seconds: 过期时间（秒）
        """
        endpoint = f"{self.url}/set_ttl"
        
        for attempt in range(self.max_retries):
            try:
                response = self._client.post(
                    endpoint,
                    json={
                        'db_name': self.name,
                        'key': key,
                        'ttl_seconds': ttl_seconds
                    }
                )
                
                if response.status_code in (200, 201):
                    return
                elif response.status_code == 404:
                    raise KeyError(key)
                else:
                    logger.warning(f"Failed to set TTL: {response.text}")
                    
            except Exception as e:
                logger.error(f"Error setting TTL: {e}")
                
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay)
        
        raise RuntimeError(f"Failed to set TTL after {self.max_retries} attempts")
    
    def get_ttl(self, key: Any) -> Optional[float]:
        """
        获取键的剩余过期时间
        
        Args:
            key: 键
            
        Returns:
            float: 剩余过期时间（秒），如果没有设置则返回None
        """
        endpoint = f"{self.url}/get_ttl"
        
        for attempt in range(self.max_retries):
            try:
                response = self._client.post(
                    endpoint,
                    json={
                        'db_name': self.name,
                        'key': key
                    }
                )
                
                if response.status_code in (200, 201):
                    result = response.json()
                    return result.get('ttl')
                elif response.status_code == 404:
                    raise KeyError(key)
                else:
                    logger.warning(f"Failed to get TTL: {response.text}")
                    
            except Exception as e:
                logger.error(f"Error getting TTL: {e}")
                
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay)
        
        raise RuntimeError(f"Failed to get TTL after {self.max_retries} attempts")
    
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