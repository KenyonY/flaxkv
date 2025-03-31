"""
FlaxKV2 基础类定义
"""

import atexit
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union, Set
import os
import shutil

from flaxkv2.serialization import encoder, decoder
from flaxkv2.utils.log import get_logger
from flaxkv2.auto_close import db_close_manager

logger = get_logger(__name__)


class BaseDBDict(ABC):
    """
    数据库字典基类，定义统一接口
    """
    # 默认配置
    DEFAULT_MAX_BUFFER_SIZE = 100
    DEFAULT_COMMIT_INTERVAL = 600  # 10分钟，单位秒
    MIN_BUFFER_SIZE = 10
    ABSOLUTE_MAX_BUFFER_SIZE = 10000
    
    def __init__(
        self,
        name: str,
        path: str = ".",
        max_buffer_size: int = None,
        commit_interval: int = None,
        rebuild: bool = False,
        **kwargs
    ):
        """
        初始化基础数据库字典
        
        Args:
            name: 数据库名称
            path: 数据库存储路径
            max_buffer_size: 最大缓冲区大小
            commit_interval: 自动提交间隔（秒）
            rebuild: 是否重建数据库
        """
        self.name = name
        self.path = os.path.abspath(path)
        self.db_path = os.path.join(self.path, self.name)
        
        # 配置
        self.MAX_BUFFER_SIZE = max_buffer_size or self.DEFAULT_MAX_BUFFER_SIZE
        self.COMMIT_TIME_INTERVAL = commit_interval or self.DEFAULT_COMMIT_INTERVAL
        
        # 内部状态
        self._buffer_dict = {}  # 写缓冲区
        self._buffered_count = 0  # 当前缓冲记录数
        self._buffer_lock = threading.RLock()  # 缓冲区锁
        self._closed = False
        self._commit_thread = None
        
        # 准备数据库
        if rebuild and os.path.exists(self.db_path):
            shutil.rmtree(self.db_path)
            
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()
        
        # 启动后台提交线程
        self._start_commit_thread()
        
        # 向关闭管理器注册实例，用于程序退出时自动关闭
        db_close_manager.register(self)
    
    # 添加上下文管理器支持
    def __enter__(self):
        """上下文管理器入口"""
        logger.debug(f"进入数据库上下文: {self.name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        if exc_type:
            logger.debug(f"因异常退出数据库上下文: {self.name}, 异常类型: {exc_type.__name__}")
        else:
            logger.debug(f"正常退出数据库上下文: {self.name}")
        
        self.close(write=True, wait=True)
        logger.debug(f"数据库上下文已关闭: {self.name}")
        # 不处理异常
        return False
    
    def _init_db(self):
        """初始化数据库连接，子类实现"""
        pass
        
    def _start_commit_thread(self):
        """启动后台提交线程"""
        if self._commit_thread is not None:
            return
            
        self._commit_thread_stop = threading.Event()
        self._commit_thread = threading.Thread(
            target=self._commit_thread_func,
            daemon=True,
            name=f"FlaxKV-Commit-{self.name}"
        )
        self._commit_thread.start()
    
    def _commit_thread_func(self):
        """后台提交线程函数"""
        while not self._commit_thread_stop.is_set():
            try:
                time.sleep(1.0)  # 检查间隔
                
                # 检查是否需要提交
                current_time = time.time()
                time_to_commit = (not hasattr(self, '_last_commit_time') or 
                                  current_time - self._last_commit_time > self.COMMIT_TIME_INTERVAL)
                                  
                buffer_to_commit = False
                with self._buffer_lock:
                    buffer_to_commit = self._buffered_count >= self.MAX_BUFFER_SIZE
                
                if time_to_commit or buffer_to_commit:
                    self._write_buffer_to_db()
                    self._last_commit_time = current_time
                    
            except Exception as e:
                logger.error(f"Error in commit thread: {e}")
    
    @abstractmethod
    def _write_buffer_to_db(self):
        """将缓冲区内容写入数据库，子类实现"""
        pass
        
    @abstractmethod
    def _get_from_db(self, key):
        """从数据库获取键值，子类实现"""
        pass
    
    def __getitem__(self, key):
        """实现字典的获取方法"""
        # 先查缓冲区
        with self._buffer_lock:
            if key in self._buffer_dict:
                return self._buffer_dict[key]
        
        # 再查数据库
        try:
            value = self._get_from_db(key)
            return value
        except KeyError:
            raise KeyError(key)
    
    def __setitem__(self, key, value):
        """实现字典的设置方法"""
        with self._buffer_lock:
            # 写入缓冲区
            self._buffer_dict[key] = value
            self._buffered_count = len(self._buffer_dict)
            
            # 如果缓冲区满，触发写入
            if self._buffered_count >= self.MAX_BUFFER_SIZE:
                self._write_buffer_to_db()
    
    def __delitem__(self, key):
        """实现字典的删除方法"""
        # 标记删除（使用None作为特殊值）
        with self._buffer_lock:
            self._buffer_dict[key] = None
            self._buffered_count = len(self._buffer_dict)
        
        # 尝试从数据库删除，但忽略不存在的键
        try:
            self._delete_from_db(key)
        except KeyError:
            # 如果键不在缓冲区也不在数据库，抛出KeyError
            if key not in self._buffer_dict:
                raise KeyError(key)
    
    @abstractmethod
    def _delete_from_db(self, key):
        """从数据库删除键，子类实现"""
        pass
    
    def __contains__(self, key):
        """实现in操作符"""
        # 先查缓冲区
        with self._buffer_lock:
            if key in self._buffer_dict:
                # 如果标记为删除，返回False
                return self._buffer_dict[key] is not None
        
        # 再查数据库
        try:
            self._get_from_db(key)
            return True
        except KeyError:
            return False
    
    def get(self, key, default=None):
        """获取键值，不存在返回默认值"""
        try:
            return self[key]
        except KeyError:
            return default
    
    def update(self, d: Dict[Any, Any]):
        """批量更新多个键值对"""
        with self._buffer_lock:
            self._buffer_dict.update(d)
            self._buffered_count = len(self._buffer_dict)
            
            if self._buffered_count >= self.MAX_BUFFER_SIZE:
                self._write_buffer_to_db()
    
    def pop(self, key, default=None):
        """弹出键值对"""
        try:
            value = self[key]
            del self[key]
            return value
        except KeyError:
            return default
    
    @abstractmethod
    def keys(self) -> List:
        """返回所有键列表"""
        pass
    
    @abstractmethod
    def values(self) -> List:
        """返回所有值列表"""
        pass
    
    @abstractmethod
    def items(self) -> List[Tuple]:
        """返回所有键值对列表"""
        pass
    
    def __len__(self):
        """返回数据库大小"""
        # 这是一个估计值，可能不精确
        key_count = len(self.keys())
        return key_count
    
    def write_immediately(self, write=True, block=False):
        """立即将缓冲区写入数据库"""
        if write:
            if block:
                self._write_buffer_to_db()
            else:
                # 创建一个临时线程执行写入
                t = threading.Thread(target=self._write_buffer_to_db)
                t.daemon = True
                t.start()
    
    def wait_until_write_complete(self, timeout=None):
        """等待所有写入完成"""
        # 创建一个临时写入，并等待它完成
        event = threading.Event()
        
        def _write_and_set():
            try:
                self._write_buffer_to_db()
            finally:
                event.set()
        
        thread = threading.Thread(target=_write_and_set)
        thread.daemon = True
        thread.start()
        
        return event.wait(timeout)
    
    def close(self, write=True, wait=False):
        """关闭数据库"""
        if self._closed:
            logger.debug(f"数据库 {self.name} 已经关闭，忽略重复关闭请求")
            return
        
        logger.debug(f"正在关闭数据库: {self.name} (路径: {self.db_path})")
        
        # 停止提交线程
        if self._commit_thread is not None:
            logger.debug(f"停止数据库 {self.name} 的提交线程")
            self._commit_thread_stop.set()
            if wait:
                self._commit_thread.join(timeout=5.0)
                logger.debug(f"已等待数据库 {self.name} 的提交线程结束")
            self._commit_thread = None
        
        # 最后写入
        if write:
            logger.debug(f"执行数据库 {self.name} 的最终数据写入")
            if wait:
                self._write_buffer_to_db()
                logger.debug(f"已完成数据库 {self.name} 的最终数据写入（同步模式）")
            else:
                try:
                    self._write_buffer_to_db()
                    logger.debug(f"已完成数据库 {self.name} 的最终数据写入（异步模式）")
                except Exception as e:
                    logger.error(f"数据库 {self.name} 最终写入时发生错误: {e}")
        
        # 关闭数据库连接
        logger.debug(f"关闭数据库 {self.name} 的底层存储连接")
        self._close_db()
        self._closed = True
        logger.debug(f"数据库 {self.name} 已成功关闭")
        
        # 从关闭管理器注销实例
        db_close_manager.unregister(self)
        logger.debug(f"数据库 {self.name} 已从自动关闭管理器中注销")
    
    @abstractmethod
    def _close_db(self):
        """关闭数据库连接，子类实现"""
        pass
    
    def destroy(self):
        """销毁数据库"""
        logger.debug(f"开始销毁数据库: {self.name} (路径: {self.db_path})")
        self.close(write=False)
        if os.path.exists(self.db_path):
            try:
                shutil.rmtree(self.db_path)
                logger.debug(f"数据库文件已删除: {self.db_path}")
            except Exception as e:
                logger.error(f"删除数据库文件时发生错误: {self.db_path}, 错误: {e}")
        logger.debug(f"数据库销毁完成: {self.name}")
    
    def to_dict(self) -> Dict:
        """转换为普通字典"""
        result = {}
        for k, v in self.items():
            result[k] = v
        return result
    
    @abstractmethod
    def stat(self) -> Dict:
        """返回数据库统计信息"""
        pass


class FlaxKV:
    """
    FlaxKV主接口，提供工厂方法创建合适的DB实现
    """
    def __new__(
        cls,
        db_name: str,
        root_path_or_url: str = ".",
        backend='leveldb',
        rebuild=False,
        raw=False,
        cache=False,
        default_ttl=None,
        **kwargs
    ):
        """
        创建FlaxKV实例
        
        Args:
            db_name: 数据库名称
            root_path_or_url: 数据库根路径或远程URL
            backend: 后端类型，支持'leveldb'
            rebuild: 是否重建数据库
            raw: 是否使用原始模式
            cache: 是否使用缓存模式
            default_ttl: 默认TTL，单位为秒。设置后，所有新增的键都会自动应用此TTL
        """
        # 检查是否为远程模式
        is_remote = root_path_or_url.startswith(("http://", "https://"))
        
        if is_remote:
            from flaxkv2.client.remote import RemoteDBDict
            return RemoteDBDict(
                db_name=db_name,
                url=root_path_or_url,
                default_ttl=default_ttl,
                **kwargs
            )
        
        # 本地模式
        if backend == 'leveldb':
            from flaxkv2.core.leveldb_dict import LevelDBDict
            return LevelDBDict(
                name=db_name,
                path=root_path_or_url,
                rebuild=rebuild,
                raw=raw,
                cache=cache,
                default_ttl=default_ttl,
                **kwargs
            )
        else:
            raise ValueError(f"不支持的后端类型: {backend}") 