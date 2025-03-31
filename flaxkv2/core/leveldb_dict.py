"""
FlaxKV2 LevelDB后端实现
"""

import os
import threading
from typing import Any, Dict, List, Tuple, Optional, Iterator, Union

import plyvel

from flaxkv2.core.base import BaseDBDict
from flaxkv2.serialization import encoder, decoder
from flaxkv2.utils.bloom import BloomFilter
from flaxkv2.utils.ttl import TTLManager
from flaxkv2.core.buffer import TieredBuffer
from flaxkv2.core.index import IndexManager
from flaxkv2.utils.log import get_logger

logger = get_logger(__name__)


class LevelDBDict(BaseDBDict):
    """
    基于LevelDB的字典实现
    """
    
    def __init__(
        self,
        name: str,
        path: str = ".",
        rebuild: bool = False,
        create_if_missing: bool = True,
        raw: bool = False,
        cache: bool = False,
        bloom_filter_capacity: int = 1000000,
        error_if_exists: bool = False,
        default_ttl: int = None,
        **kwargs
    ):
        """
        初始化LevelDB字典
        
        Args:
            name: 数据库名称
            path: 数据库路径
            rebuild: 是否重建数据库
            create_if_missing: 如果数据库不存在是否创建
            raw: 是否使用原始模式（不进行序列化）
            cache: 是否使用缓存
            bloom_filter_capacity: 布隆过滤器容量
            error_if_exists: 如果数据库已存在是否抛出错误
            default_ttl: 默认TTL，单位为秒。设置后，所有新增的键都会自动应用此TTL，None表示不设置默认TTL
        """
        self._db = None
        self._raw = raw
        self._use_cache = cache
        self._default_ttl = default_ttl  # 默认TTL
        
        # 高级功能
        self._bloom_filter = BloomFilter(capacity=bloom_filter_capacity)
        self._ttl_manager = TTLManager()  # 初始时不传递数据库引用，等待数据库初始化完成
        self._index_manager = IndexManager()
        
        # 缓存
        if self._use_cache:
            self._cache = TieredBuffer()
        else:
            self._cache = None
            
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
        
        # 初始化基类
        super().__init__(name, path, rebuild=rebuild, **kwargs)
    
    def _init_db(self):
        """初始化数据库连接"""
        try:
            self._db = plyvel.DB(self.db_path, **self._leveldb_options)
            logger.info(f"Opened LevelDB at {self.db_path}")
            
            # 初始化布隆过滤器
            self._init_bloom_filter()
            
            # 在数据库初始化完成后，将数据库引用传递给TTL管理器
            self._ttl_manager.set_db(self)
            
        except Exception as e:
            logger.error(f"Failed to open LevelDB at {self.db_path}: {e}")
            raise
    
    def _init_bloom_filter(self):
        """初始化布隆过滤器，加载现有键"""
        if self._bloom_filter is not None:
            # 重置过滤器
            self._bloom_filter.reset()
            
            # 加载所有键
            with self._db_lock:
                for key, _ in self._db:
                    decoded_key = self._decode_key(key)
                    self._bloom_filter.add(decoded_key)
    
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
        # 检查布隆过滤器
        if self._bloom_filter is not None and not self._bloom_filter.check(key):
            raise KeyError(key)
            
        # 检查缓存
        if self._cache is not None:
            cached_value = self._cache.get(key)
            if cached_value is not None:
                return cached_value
        
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
        
        # 更新缓存
        if self._cache is not None:
            self._cache.put(key, value)
            
        return value
    
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
                if value is None:
                    # 删除操作
                    key_bytes = self._encode_key(key)
                    batch.delete(key_bytes)
                    
                    # 从布隆过滤器中移除
                    if self._bloom_filter is not None:
                        # 由于布隆过滤器不支持删除，我们不做任何操作
                        # 这可能导致一些假阳性，但不会导致假阴性
                        pass
                        
                    # 从索引中移除
                    self._index_manager.remove_from_indexes(key)
                    
                    # 从TTL中移除
                    self._ttl_manager.remove(key)
                    
                    # 从缓存中移除
                    if self._cache is not None:
                        self._cache.remove(key)
                else:
                    # 设置操作
                    key_bytes = self._encode_key(key)
                    value_bytes = self._encode_value(value)
                    batch.put(key_bytes, value_bytes)
                    
                    # 添加到布隆过滤器
                    if self._bloom_filter is not None:
                        self._bloom_filter.add(key)
                        
                    # 更新索引
                    self._index_manager.update_indexes(key, value)
                    
                    # 更新缓存
                    if self._cache is not None:
                        self._cache.put(key, value)
            
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
        
        # 从缓存中移除
        if self._cache is not None:
            self._cache.remove(key)
    
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
    
    def create_hash_index(self, name: str, field_accessor: callable) -> None:
        """
        创建哈希索引
        
        Args:
            name: 索引名称
            field_accessor: 字段访问函数，从值中提取索引字段
        """
        from flaxkv2.core.index import HashIndex
        
        # 创建索引
        index = HashIndex(name, field_accessor)
        
        # 添加索引
        self._index_manager.add_index(index)
        
        # 构建索引（遍历所有数据）
        for key, value in self.items():
            index.add(key, value)
    
    def create_range_index(self, name: str, field_accessor: callable) -> None:
        """
        创建范围索引
        
        Args:
            name: 索引名称
            field_accessor: 字段访问函数，从值中提取索引字段
        """
        from flaxkv2.core.index import RangeIndex
        
        # 创建索引
        index = RangeIndex(name, field_accessor)
        
        # 添加索引
        self._index_manager.add_index(index)
        
        # 构建索引（遍历所有数据）
        for key, value in self.items():
            index.add(key, value)
    
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
        设置键值对
        
        Args:
            key: 键
            value: 值
        """
        with self._db_lock:
            # 如果使用缓存，先更新缓存
            if self._use_cache:
                self._cache[key] = value
                
            # 编码键值
            key_bytes = self._encode_key(key)
            value_bytes = self._encode_value(value)
            
            # 写入数据库
            self._db.put(key_bytes, value_bytes)
            
            # 更新布隆过滤器
            self._bloom_filter.add(key)
            
            # 更新索引
            self._index_manager.update_indexes(key, value)
            
            # 如果设置了默认TTL，则应用
            if self._default_ttl is not None:
                self._ttl_manager.set_ttl(key, self._default_ttl)
    
    def update(self, d: Dict[Any, Any]):
        """
        批量更新多个键值对，如果设置了默认TTL，则对新添加的键应用默认TTL
        """
        # 先获取要添加的新键
        new_keys = []
        with self._buffer_lock:
            # 收集不在数据库中的键
            for key in d.keys():
                if key not in self:
                    new_keys.append(key)
        
        # 调用父类的update方法进行批量更新
        super().update(d)
        
        # 如果设置了默认TTL，则为新键应用默认TTL
        if self._default_ttl is not None and new_keys:
            for key in new_keys:
                self._ttl_manager.set(key, self._default_ttl) 