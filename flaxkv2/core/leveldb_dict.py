"""
FlaxKV2 LevelDB后端实现

⚠️ DEPRECATED: LevelDBDict 已被弃用
========================================

此类已被标记为弃用，建议使用 RawLevelDBDict 替代。

原因：
1. 性能测试显示 RawLevelDBDict 在所有场景下都比 LevelDBDict 更快
2. LevelDBDict 的缓冲机制和索引功能在实际使用中没有带来性能提升
3. RawLevelDBDict 的代码更简单，更易于维护

迁移指南：
- 旧代码: from flaxkv2 import LevelDBDict
          db = LevelDBDict("mydb", "./data")

- 新代码: from flaxkv2 import RawLevelDBDict
          db = RawLevelDBDict("mydb", "./data")

或者直接使用 FlaxKV 工厂类（推荐）：
- 新代码: from flaxkv2 import FlaxKV
          db = FlaxKV("mydb", "./data")

功能对比：
- ✅ RawLevelDBDict 支持所有 LevelDBDict 的核心功能（TTL、嵌套存储等）
- ✅ RawLevelDBDict 性能更好（无缓冲开销）
- ✅ RawLevelDBDict 内存占用更少
- ❌ RawLevelDBDict 不支持写缓冲（实测发现缓冲反而降低性能）
- ❌ RawLevelDBDict 不支持索引（实测发现索引没有实际用途）

此类将在未来版本中移除。
"""

import os
import shutil
import threading
import time
import warnings
from typing import Any, Dict, List, Tuple, Optional, Iterator, Union

import plyvel

from flaxkv2.serialization import encoder, decoder
from flaxkv2.utils.ttl import TTLManager
# TieredBuffer 已移除 - 性能测试显示缓存在所有场景下都是负优化
# BloomFilter 已移除 - 在有内存缓冲区的设计中是多余的优化
from flaxkv2.core.index import IndexManager
from flaxkv2.core.nested_dict import NestedDBDict
from flaxkv2.utils.log import get_logger
from flaxkv2.instance_manager import db_instance_manager
from flaxkv2.auto_close import db_close_manager

logger = get_logger(__name__)


# 删除标记 - 使用特殊的 Sentinel 对象而不是 None
# 这样用户可以正常存储 None 值而不会与删除操作混淆
class _DeletedMarker:
    """删除标记类，用于在缓冲区中标记已删除的键"""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "<DELETED>"

    def __bool__(self):
        return False


# 全局删除标记实例
_DELETED = _DeletedMarker()


class LevelDBDict:
    """
    ⚠️ DEPRECATED: 此类已弃用，请使用 RawLevelDBDict 替代

    基于LevelDB的字典实现，支持缓冲机制、布隆过滤器、索引和TTL
    """
    
    # 默认配置
    DEFAULT_MAX_BUFFER_SIZE = 5000  # 增大默认缓冲区，提升批量写入性能
    DEFAULT_COMMIT_INTERVAL = 600  # 10分钟，单位秒
    MIN_BUFFER_SIZE = 10
    ABSOLUTE_MAX_BUFFER_SIZE = 100000  # 增大上限
    
    def __new__(
        cls,
        name: str,
        path: str = ".",
        rebuild: bool = False,
        **kwargs
    ):
        """
        创建或返回数据库实例
        
        如果数据库已经打开，返回已有实例；否则创建新实例。
        如果rebuild=True，则关闭旧实例并创建新实例。
        """
        # 计算数据库路径
        abs_path = os.path.abspath(path)
        db_path = os.path.join(abs_path, name)
        
        # 如果需要重建，先关闭并删除旧实例
        if rebuild:
            existing = db_instance_manager.get_instance(db_path)
            if existing is not None:
                logger.debug(f"rebuild=True，关闭并删除旧实例: {db_path}")
                try:
                    # 先从缓存移除（但不调用close，因为我们需要手动删除文件）
                    db_instance_manager.unregister_instance(db_path)
                    if hasattr(existing, '_db') and existing._db is not None:
                        existing._db.close()
                    existing._closed = True
                except Exception as e:
                    logger.warning(f"关闭旧实例时出错: {e}")
            
            # 删除数据库文件（无论是否有缓存实例）
            if os.path.exists(db_path):
                try:
                    shutil.rmtree(db_path)
                    logger.debug(f"已删除旧数据库文件: {db_path}")
                except Exception as e:
                    logger.error(f"删除数据库文件失败: {e}")
            
            # 创建新实例
            instance = super().__new__(cls)
            instance._is_new_instance = True
            return instance
        
        # 检查是否已有实例
        existing = db_instance_manager.get_instance(db_path)
        if existing is not None:
            logger.debug(f"返回已存在的数据库实例: {db_path}")
            # 返回已有实例，不需要再次初始化
            return existing
        
        # 创建新实例
        instance = super().__new__(cls)
        instance._is_new_instance = True
        return instance
    
    def __init__(
        self,
        name: str,
        path: str = ".",
        rebuild: bool = False,
        create_if_missing: bool = True,
        raw: bool = False,
        error_if_exists: bool = False,
        default_ttl: int = None,
        auto_nested: bool = True,
        max_buffer_size: int = None,
        commit_interval: int = None,
        **kwargs
    ):
        """
        ⚠️ DEPRECATED: LevelDBDict 已弃用，请使用 RawLevelDBDict 替代

        初始化LevelDB字典

        Args:
            name: 数据库名称
            path: 数据库路径
            rebuild: 是否重建数据库
            create_if_missing: 如果数据库不存在是否创建
            raw: 是否使用原始模式（不进行序列化）
            error_if_exists: 如果数据库已存在是否抛出错误
            default_ttl: 默认TTL，单位为秒。设置后，所有新增的键都会自动应用此TTL，None表示不设置默认TTL
            auto_nested: 是否自动将字典类型转换为嵌套存储（默认True，自动优化性能）
            max_buffer_size: 最大缓冲区大小
            commit_interval: 自动提交间隔（秒）
        """
        # 发出弃用警告
        warnings.warn(
            "LevelDBDict is deprecated and will be removed in a future version. "
            "Please use RawLevelDBDict instead, which has better performance. "
            "Migration: Replace 'LevelDBDict' with 'RawLevelDBDict' in your code, "
            "or use the FlaxKV factory class (recommended): FlaxKV('db_name', './data')",
            DeprecationWarning,
            stacklevel=2
        )

        # 如果不是新实例，跳过初始化
        if not getattr(self, '_is_new_instance', False):
            logger.debug(f"跳过重复初始化，使用已有实例: {name}")
            return
        
        # 基本属性
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
        
        # LevelDB特定属性
        self._db = None
        self._raw = raw
        self._default_ttl = default_ttl  # 默认TTL
        self._auto_nested = auto_nested

        # 高级功能
        self._ttl_manager = TTLManager()  # 初始时不传递数据库引用，等待数据库初始化完成
        self._index_manager = IndexManager()
            
        # 设置选项
        self._leveldb_options = {
            'create_if_missing': create_if_missing,
            'error_if_exists': error_if_exists and rebuild,
            'write_buffer_size': 64 * 1024 * 1024,  # 64MB
            'max_open_files': 100,
            'compression': 'snappy',
        }
        
        # 锁
        self._db_lock = threading.RLock()
        
        # 初始化数据库
        self._init_db()
        
        # 启动后台提交线程
        self._start_commit_thread()
        
        # 向实例管理器注册
        db_instance_manager.register_instance(self.db_path, self)
        logger.debug(f"数据库实例已注册到实例管理器: {self.db_path}")
        
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
    
    def _init_db(self):
        """初始化数据库连接"""
        try:
            self._db = plyvel.DB(self.db_path, **self._leveldb_options)
            logger.info(f"Opened LevelDB at {self.db_path}")
            
            # 在数据库初始化完成后，将数据库引用传递给TTL管理器
            self._ttl_manager.set_db(self)
            
        except Exception as e:
            logger.error(f"Failed to open LevelDB at {self.db_path}: {e}")
            raise
    
    def _encode_key(self, key):
        """编码键"""
        if self._raw:
            if isinstance(key, str):
                return key.encode('utf-8')
            elif isinstance(key, bytes):
                return key
            else:
                return str(key).encode('utf-8')
        else:
            return encoder.encode_key(key)
    
    def _decode_key(self, key_bytes):
        """解码键"""
        if self._raw:
            try:
                return key_bytes.decode('utf-8')
            except UnicodeDecodeError:
                return key_bytes
        else:
            return decoder.decode_key(key_bytes)
    
    def _encode_value(self, value):
        """编码值"""
        if self._raw:
            if isinstance(value, str):
                return value.encode('utf-8')
            elif isinstance(value, bytes):
                return value
            else:
                return str(value).encode('utf-8')
        else:
            return encoder.encode(value)
    
    def _decode_value(self, value_bytes):
        """解码值"""
        if self._raw:
            try:
                return value_bytes.decode('utf-8')
            except UnicodeDecodeError:
                return value_bytes
        else:
            return decoder.decode(value_bytes)
    
    def _get_from_db(self, key):
        """从数据库获取值"""
            
        # 缓存已移除 - 性能测试显示缓存在所有场景下都是负优化
        
        # 检查TTL
        if self._ttl_manager.is_expired(key):
            # 删除过期键
            try:
                self._delete_from_db(key)
            except:
                pass
            raise KeyError(key)
        
        # 从数据库获取
        key_bytes = self._encode_key(key)
        with self._db_lock:
            value_bytes = self._db.get(key_bytes)
            
        if value_bytes is None:
            raise KeyError(key)
            
        # 解码
        value = self._decode_value(value_bytes)
        
        # 缓存已移除
            
        return value
    
    def __contains__(self, key):
        """实现in操作符"""
        # 先查缓冲区
        with self._buffer_lock:
            if key in self._buffer_dict:
                # 如果标记为删除，返回False
                return self._buffer_dict[key] is not _DELETED
        
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
    
    def pop(self, key, default=None):
        """弹出键值对"""
        try:
            value = self[key]
            del self[key]
            return value
        except KeyError:
            return default
    
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
        
        # 从实例管理器注销
        db_instance_manager.unregister_instance(self.db_path)
        logger.debug(f"数据库 {self.name} 已从实例管理器中注销")
        
        # 从关闭管理器注销实例
        db_close_manager.unregister(self)
        logger.debug(f"数据库 {self.name} 已从自动关闭管理器中注销")
    
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

    def __repr__(self):
        """返回对象的字符串表示形式"""
        if self._closed:
            return f"{self.__class__.__name__}(name={self.name!r}, closed=True)"

        # 获取前几个键值对作为预览
        try:
            items = list(self.items())
            num_items = len(items)

            if num_items == 0:
                items_str = "{}"
            elif num_items <= 20:
                # 少于等于20个，显示所有
                items_repr = ", ".join(f"{k!r}: {v!r}" for k, v in items)
                items_str = f"{{{items_repr}}}"
            else:
                # 超过20个，显示前20个 + ...
                preview_items = items[:20]
                items_repr = ", ".join(f"{k!r}: {v!r}" for k, v in preview_items)
                items_str = f"{{{items_repr}, ...}} ({num_items} items)"

            return f"{self.__class__.__name__}(name={self.name!r}, path={self.db_path!r}, items={items_str})"
        except Exception as e:
            return f"{self.__class__.__name__}(name={self.name!r}, path={self.db_path!r}, error={e!r})"

    def __str__(self):
        """返回用户友好的字符串表示形式，类似dict"""
        if self._closed:
            return f"<{self.__class__.__name__} '{self.name}' (closed)>"

        try:
            items = list(self.items())
            num_items = len(items)

            if num_items == 0:
                return "{}"
            elif num_items <= 20:
                # 少于等于20个，显示所有（类似普通dict）
                items_repr = ", ".join(f"{k!r}: {v!r}" for k, v in items)
                return f"{{{items_repr}}}"
            else:
                # 超过20个，显示前20个 + 提示信息
                preview_items = items[:20]
                items_repr = ", ".join(f"{k!r}: {v!r}" for k, v in preview_items)
                return f"{{{items_repr}, ... and {num_items - 20} more items}}"
        except Exception as e:
            return f"<{self.__class__.__name__} '{self.name}' (error: {e})>"
    
    def _write_buffer_to_db(self):
        """将缓冲区写入数据库"""
        with self._buffer_lock:
            # 创建缓冲区快照
            buffer_dict_snapshot = self._buffer_dict.copy()
            # 清空缓冲区
            self._buffer_dict = {}
            self._buffered_count = 0
        
        if not buffer_dict_snapshot:
            return
            
        # 创建批量写入
        with self._db_lock:
            batch = self._db.write_batch()
            
            # 收集删除和设置操作
            for key, value in buffer_dict_snapshot.items():
                if value is _DELETED:
                    # 删除操作
                    key_bytes = self._encode_key(key)
                    batch.delete(key_bytes)
                        
                    # 从索引中移除
                    self._index_manager.remove_from_indexes(key)
                    
                    # 从TTL中移除
                    self._ttl_manager.remove(key)
                    
                    # 缓存已移除
                else:
                    # 设置操作
                    key_bytes = self._encode_key(key)
                    value_bytes = self._encode_value(value)
                    batch.put(key_bytes, value_bytes)
                        
                    # 更新索引
                    self._index_manager.update_indexes(key, value)

                    # 缓存已移除
            
            # 提交批量写入
            batch.write()
    
    def _delete_from_db(self, key):
        """从数据库删除键"""
        key_bytes = self._encode_key(key)
        
        with self._db_lock:
            # 检查键是否存在
            value_bytes = self._db.get(key_bytes)
            if value_bytes is None:
                raise KeyError(key)
                
            # 删除键
            self._db.delete(key_bytes)
        
        # 从TTL中移除
        self._ttl_manager.remove(key)
        
        # 从索引中移除
        self._index_manager.remove_from_indexes(key)

        # 缓存已移除
    
    def _close_db(self):
        """关闭数据库连接"""
        if self._db is not None:
            try:
                with self._db_lock:
                    logger.debug(f"关闭LevelDB连接: {self.name}")
                    # 确保所有TTL信息已持久化保存
                    self._write_buffer_to_db()  # 将缓冲区数据写入磁盘
                    self._db.close()
                    self._db = None
                    logger.debug(f"LevelDB连接已关闭: {self.name}")
            except Exception as e:
                logger.error(f"关闭LevelDB连接时发生错误: {self.name}, 错误: {e}")
    
    def keys(self) -> List:
        """获取所有键列表"""
        # 先刷新缓冲区
        self._write_buffer_to_db()
        
        # 获取所有键
        keys = []
        with self._db_lock:
            for key_bytes, _ in self._db:
                key = self._decode_key(key_bytes)
                # 检查TTL
                if not self._ttl_manager.is_expired(key):
                    keys.append(key)
        
        return keys
    
    def values(self) -> List:
        """获取所有值列表"""
        # 此操作较为昂贵，先刷新缓冲区
        self._write_buffer_to_db()
        
        values = []
        with self._db_lock:
            for key_bytes, value_bytes in self._db:
                key = self._decode_key(key_bytes)
                # 检查TTL
                if not self._ttl_manager.is_expired(key):
                    value = self._decode_value(value_bytes)
                    values.append(value)
        
        return values
    
    def items(self) -> List[Tuple]:
        """获取所有键值对列表"""
        # 此操作较为昂贵，先刷新缓冲区
        self._write_buffer_to_db()
        
        items = []
        with self._db_lock:
            for key_bytes, value_bytes in self._db:
                key = self._decode_key(key_bytes)
                # 检查TTL
                if not self._ttl_manager.is_expired(key):
                    value = self._decode_value(value_bytes)
                    items.append((key, value))
        
        return items
    
    def set_ttl(self, key: Any, ttl_seconds: int) -> None:
        """
        设置键的过期时间
        
        Args:
            key: 键
            ttl_seconds: 过期时间（秒）
        """
        # 检查键是否存在
        if key not in self:
            raise KeyError(key)
            
        self._ttl_manager.set(key, ttl_seconds)
    
    def get_ttl(self, key: Any) -> Optional[float]:
        """
        获取键的剩余过期时间
        
        Args:
            key: 键
            
        Returns:
            float: 剩余过期时间（秒），如果没有设置则返回None
        """
        expiry = self._ttl_manager.get_expiry(key)
        if expiry is None:
            return None
            
        # 计算剩余时间
        import time
        remaining = expiry - time.time()
        return max(0, remaining)
    
    def query_index(self, index_name: str, *args, **kwargs) -> List[Any]:
        """
        使用索引查询
        
        Args:
            index_name: 索引名称
            *args, **kwargs: 传递给索引的搜索参数
            
        Returns:
            List[Any]: 匹配的键列表
        """
        return self._index_manager.search(index_name, *args, **kwargs)
    
    def save_indexes(self, base_path: str = None) -> None:
        """
        保存所有索引
        
        Args:
            base_path: 索引保存路径，默认为数据库路径
        """
        if base_path is None:
            base_path = os.path.join(self.db_path, 'indexes')
            
        os.makedirs(base_path, exist_ok=True)
        self._index_manager.save_all(base_path)
    
    def load_indexes(self, base_path: str = None) -> None:
        """
        加载所有索引
        
        Args:
            base_path: 索引加载路径，默认为数据库路径
        """
        if base_path is None:
            base_path = os.path.join(self.db_path, 'indexes')
            
        if os.path.exists(base_path):
            self._index_manager.load_all(base_path)
    
    def stat(self) -> Dict:
        """返回数据库统计信息"""
        # 刷新缓冲区以获取准确统计
        self._write_buffer_to_db()
        
        # 计算键值对数量
        count = 0
        total_key_size = 0
        total_value_size = 0
        
        with self._db_lock:
            for key_bytes, value_bytes in self._db:
                count += 1
                total_key_size += len(key_bytes)
                total_value_size += len(value_bytes)
        
        return {
            'count': count,
            'avg_key_size': total_key_size / count if count > 0 else 0,
            'avg_value_size': total_value_size / count if count > 0 else 0,
            'total_size': total_key_size + total_value_size,
            'path': self.db_path,
            'backend': 'leveldb'
        }
    
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
        设置键值对，自动检测字典类型并使用嵌套存储

        Args:
            key: 键
            value: 值
        """
        from flaxkv2.core.nested_dict import NestedDBDict

        # 特殊键：元数据标记，直接写入缓冲区
        if isinstance(key, str) and key.startswith('__nested__:'):
            should_flush = False
            with self._buffer_lock:
                # 写入缓冲区
                self._buffer_dict[key] = value
                self._buffered_count = len(self._buffer_dict)
                # 如果缓冲区满，标记需要刷新（但不在锁内执行）
                if self._buffered_count >= self.MAX_BUFFER_SIZE:
                    should_flush = True
            # 在锁外执行刷新，避免阻塞其他操作
            if should_flush:
                self._write_buffer_to_db()
            return

        # 如果值是 NestedDBDict，转换为普通字典
        if isinstance(value, NestedDBDict):
            value = value.to_dict()

        # 如果启用了自动嵌套且值是字典类型
        if self._auto_nested and isinstance(value, dict):
            # 1. 标记为嵌套字典
            self.__setitem__(f'__nested__:{key}', True)

            # 2. 创建 nested，递归写入
            nested = self.nested(key)
            nested.clear()  # 清除旧数据
            for k, v in value.items():
                nested[k] = v  # 递归！NestedDBDict 会继续检测类型
        else:
            # 非字典或未启用自动嵌套：取消嵌套标记（如果有）
            if self._auto_nested:
                try:
                    self.__delitem__(f'__nested__:{key}')
                except KeyError:
                    pass

            # 使用缓冲机制存储
            should_flush = False
            with self._buffer_lock:
                # 写入缓冲区
                self._buffer_dict[key] = value
                self._buffered_count = len(self._buffer_dict)
                # 如果缓冲区满，标记需要刷新（但不在锁内执行）
                if self._buffered_count >= self.MAX_BUFFER_SIZE:
                    should_flush = True
            # 在锁外执行刷新，避免阻塞其他操作
            if should_flush:
                self._write_buffer_to_db()

            # 如果设置了默认TTL，则应用
            if self._default_ttl is not None:
                self._ttl_manager.set(key, self._default_ttl)

    def __getitem__(self, key):
        """
        获取键值，自动检测嵌套字典

        Args:
            key: 键

        Returns:
            如果是嵌套字典，返回 NestedDBDict；否则返回值
        """
        # 特殊键：先查缓冲区，再查数据库
        if isinstance(key, str) and key.startswith('__nested__:'):
            # 先查缓冲区
            with self._buffer_lock:
                if key in self._buffer_dict:
                    value = self._buffer_dict[key]
                    # 如果是删除标记，抛出 KeyError
                    if value is _DELETED:
                        raise KeyError(key)
                    return value
            # 再查数据库
            try:
                value = self._get_from_db(key)
                return value
            except KeyError:
                raise KeyError(key)

        # 如果启用了自动嵌套，检查是否是嵌套字典
        if self._auto_nested:
            try:
                is_nested = self.__getitem__(f'__nested__:{key}')
                if is_nested:
                    # 返回 NestedDBDict
                    return self.nested(key)
            except KeyError:
                pass

        # 普通值：先查缓冲区，再查数据库
        # 先查缓冲区
        with self._buffer_lock:
            if key in self._buffer_dict:
                value = self._buffer_dict[key]
                # 如果是删除标记，抛出 KeyError
                if value is _DELETED:
                    raise KeyError(key)
                return value
        
        # 再查数据库
        try:
            value = self._get_from_db(key)
            return value
        except KeyError:
            raise KeyError(key)

    def __delitem__(self, key):
        """
        删除键值，如果是嵌套字典则删除所有子键

        Args:
            key: 键
        """
        # 特殊键：标记删除（使用 _DELETED 作为删除标记）
        if isinstance(key, str) and key.startswith('__nested__:'):
            with self._buffer_lock:
                self._buffer_dict[key] = _DELETED
                self._buffered_count = len(self._buffer_dict)
            # 尝试从数据库删除，但忽略不存在的键
            try:
                self._delete_from_db(key)
            except KeyError:
                # 如果键不在缓冲区也不在数据库，抛出KeyError
                if key not in self._buffer_dict:
                    raise KeyError(key)
            return

        # 如果启用了自动嵌套，检查是否是嵌套字典
        if self._auto_nested:
            marker_key = f'__nested__:{key}'
            is_nested = False

            # 先检查缓冲区
            with self._buffer_lock:
                if marker_key in self._buffer_dict:
                    marker_value = self._buffer_dict[marker_key]
                    if marker_value is not _DELETED:  # 不是删除标记
                        is_nested = True

            # 如果缓冲区中没有，再查数据库
            if not is_nested:
                try:
                    marker_value = self._get_from_db(marker_key)
                    if marker_value:
                        is_nested = True
                except KeyError:
                    pass

            if is_nested:
                # 删除所有子键
                nested = self.nested(key)
                nested.clear()
                # 删除标记（立即从数据库删除，不经过缓冲）
                try:
                    self._delete_from_db(marker_key)
                except KeyError:
                    pass
                # 同时从缓冲区删除
                with self._buffer_lock:
                    self._buffer_dict.pop(marker_key, None)
                return

        # 删除普通值：标记删除（使用 _DELETED 作为删除标记）
        with self._buffer_lock:
            self._buffer_dict[key] = _DELETED
            self._buffered_count = len(self._buffer_dict)
        
        # 尝试从数据库删除，但忽略不存在的键
        try:
            self._delete_from_db(key)
        except KeyError:
            # 如果键不在缓冲区也不在数据库，抛出KeyError
            if key not in self._buffer_dict:
                raise KeyError(key)

    def update(self, d: Dict[Any, Any]):
        """
        批量更新多个键值对，如果设置了默认TTL，则对所有键应用默认TTL

        注意：为了性能，对所有键都应用TTL，而不是只对新键
        """
        # 批量更新缓冲区
        should_flush = False
        with self._buffer_lock:
            self._buffer_dict.update(d)
            self._buffered_count = len(self._buffer_dict)

            if self._buffered_count >= self.MAX_BUFFER_SIZE:
                should_flush = True

        # 在锁外执行刷新，避免阻塞其他操作
        if should_flush:
            self._write_buffer_to_db()

        # 如果设置了默认TTL，为所有键应用默认TTL
        # 不检查是否为新键，避免数据库查询开销
        if self._default_ttl is not None:
            for key in d.keys():
                self._ttl_manager.set(key, self._default_ttl)

    def nested(self, prefix: str) -> NestedDBDict:
        """
        创建一个基于前缀的嵌套字典视图

        这是解决嵌套数据频繁序列化问题的推荐方案。
        使用 NestedDBDict 可以让每个字段独立存储和访问，避免整个字典的序列化/反序列化。

        性能优势：
        - 修改单个字段只需序列化该字段的值
        - 读取单个字段只需反序列化该字段的值
        - 利用 LevelDB 的前缀查询能力高效迭代

        使用示例：
            # 创建嵌套字典
            user = db.nested('user:1')

            # 设置字段（每个字段独立存储）
            user['name'] = 'Alice'
            user['age'] = 30
            user['city'] = 'NYC'

            # 高效修改（只序列化 age 的值）
            user['age'] = 31

            # 高效读取（只反序列化 name 的值）
            print(user['name'])

            # 迭代所有字段
            for key, value in user.items():
                print(key, value)

        对比传统方式：
            # ❌ 传统方式（低效）
            db['user:1'] = {'name': 'Alice', 'age': 30, 'city': 'NYC'}
            data = db['user:1']  # 反序列化整个字典
            data['age'] = 31
            db['user:1'] = data  # 重新序列化整个字典

            # ✅ 嵌套字典方式（高效）
            user = db.nested('user:1')
            user['age'] = 31  # 只序列化 age 的值

        Args:
            prefix: 前缀字符串，建议使用冒号分隔（如 'user:1'）

        Returns:
            NestedDBDict: 嵌套字典对象

        注意：
            - 前缀会自动添加冒号分隔符，实际存储的键格式为 'prefix:field'
            - NestedDBDict 支持完整的字典接口（get, set, del, keys, values, items 等）
            - 使用父数据库的缓冲机制，写入性能优秀
        """
        # 确保数据库已打开
        if self._db is None:
            raise RuntimeError("Database is not open")

        # 创建带前缀的数据库视图
        # 注意：prefix 后面会自动添加 ':' 分隔符
        prefix_with_colon = f"{prefix}:"
        prefix_bytes = prefix_with_colon.encode('utf-8')
        prefixed_db = self._db.prefixed_db(prefix_bytes)

        # 返回 NestedDBDict 对象，传入 root_db 以支持递归
        return NestedDBDict(prefixed_db, prefix_with_colon, parent_db=self, root_db=self) 