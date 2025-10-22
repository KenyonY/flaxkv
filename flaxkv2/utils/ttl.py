"""
FlaxKV2 TTL(生存时间)支持
"""

import time
import threading
import pickle
from typing import Dict, Any, List, Tuple, Set, Optional


class TTLManager:
    """
    TTL管理器，处理键的过期时间
    """
    
    # 用于存储TTL信息的特殊键前缀
    TTL_KEY_PREFIX = b"__ttl_info__:"
    
    def __init__(self, cleanup_interval: int = 60, db=None):
        """
        初始化TTL管理器
        
        Args:
            cleanup_interval: 清理检查间隔（秒）
            db: 数据库引用，用于持久化TTL信息
        """
        self._expiry_dict = {}  # 键 -> 过期时间戳
        self._lock = threading.RLock()
        self._cleanup_interval = cleanup_interval
        self._last_cleanup_time = time.time()
        self._db = db  # 保存数据库引用
        
        # 如果提供了数据库引用，加载已保存的TTL数据
        if self._db is not None:
            self._load_ttl_data()
    
    def set_db(self, db):
        """
        设置数据库引用
        
        Args:
            db: 数据库引用
        """
        self._db = db
        # 加载已保存的TTL数据
        if self._db is not None:
            self._load_ttl_data()
    
    def set(self, key: Any, ttl_seconds: int):
        """
        设置键的TTL

        Args:
            key: 键
            ttl_seconds: 生存时间（秒）
        """
        if ttl_seconds <= 0:
            return

        with self._lock:
            expiry_time = time.time() + ttl_seconds
            self._expiry_dict[key] = expiry_time

            # 持久化TTL信息（如果数据库已初始化）
            if self._db is None:
                # 如果数据库未初始化，记录警告
                import logging
                logging.warning(
                    "TTL管理器的数据库引用未设置。TTL信息将只保存在内存中，"
                    "重启后会丢失。请在数据库初始化后调用 set_db() 方法。"
                )
            else:
                self._save_ttl_info(key, expiry_time)

    def set_ttl(self, key: Any, ttl_seconds: int):
        """
        设置键的TTL（set方法的别名）

        Args:
            key: 键
            ttl_seconds: 生存时间（秒）
        """
        self.set(key, ttl_seconds)
    
    def get_expiry(self, key: Any) -> Optional[float]:
        """
        获取键的过期时间

        Args:
            key: 键

        Returns:
            float: 过期时间戳，如果没有设置TTL则返回None
        """
        with self._lock:
            return self._expiry_dict.get(key)

    def get_remaining_ttl(self, key: Any) -> Optional[int]:
        """
        获取键的剩余TTL时间

        Args:
            key: 键

        Returns:
            int: 剩余TTL秒数，如果没有设置TTL则返回None
        """
        with self._lock:
            expiry_time = self._expiry_dict.get(key)
            if expiry_time is None:
                return None

            remaining = int(expiry_time - time.time())
            return max(0, remaining)  # 返回0而不是负数
    
    def is_expired(self, key: Any) -> bool:
        """
        检查键是否已过期
        
        Args:
            key: 键
            
        Returns:
            bool: 如果键已过期或不存在返回True，否则返回False
        """
        with self._lock:
            expiry_time = self._expiry_dict.get(key)
            if expiry_time is None:
                return False
            return time.time() > expiry_time
    
    def remove(self, key: Any):
        """
        移除键的TTL设置
        
        Args:
            key: 键
        """
        with self._lock:
            if key in self._expiry_dict:
                del self._expiry_dict[key]
                # 从持久化存储中删除TTL信息
                self._remove_ttl_info(key)
    
    def get_expired_keys(self) -> List[Any]:
        """
        获取所有已过期的键

        Returns:
            List[Any]: 过期键列表
        """
        current_time = time.time()
        expired_keys = []

        with self._lock:
            # 检查是否应该进行全量清理
            should_cleanup = (current_time - self._last_cleanup_time) > self._cleanup_interval

            if should_cleanup:
                # 保存需要删除的键，避免在迭代时修改字典
                to_remove = []

                for key, expiry_time in self._expiry_dict.items():
                    if current_time > expiry_time:
                        expired_keys.append(key)
                        to_remove.append(key)

                # 移除已过期的键
                for key in to_remove:
                    del self._expiry_dict[key]
                    # 从持久化存储中删除TTL信息
                    self._remove_ttl_info(key)

                self._last_cleanup_time = current_time
            else:
                # 简单检查
                for key, expiry_time in self._expiry_dict.items():
                    if current_time > expiry_time:
                        expired_keys.append(key)

        return expired_keys

    def cleanup_expired(self) -> int:
        """
        清理所有已过期的键（从数据库中删除）

        Returns:
            int: 清理的键数量
        """
        expired_keys = self.get_expired_keys()
        count = 0

        for key in expired_keys:
            try:
                # 从数据库中删除过期键
                if self._db is not None and key in self._db:
                    del self._db[key]
                    count += 1
            except Exception as e:
                import logging
                logging.error(f"清理过期键 {key} 失败: {e}")

        return count
        
    def _get_ttl_key_str(self, key):
        """
        获取用于存储TTL信息的特殊键（字符串格式）
        
        Args:
            key: 原始键
            
        Returns:
            str: 用于存储TTL信息的特殊键
        """
        # 将原始键序列化为字符串
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
    
    def _save_ttl_info(self, key, expiry_time):
        """
        将TTL信息保存到数据库
        
        Args:
            key: 键
            expiry_time: 过期时间戳
        """
        if self._db is None:
            return
            
        try:
            # 使用字符串格式的特殊键
            ttl_key = self._get_ttl_key_str(key)
            # 存储过期时间戳 - 直接使用底层_db对象避免调用__setitem__造成递归
            if hasattr(self._db, '_db'):
                key_bytes = self._db._encode_key(ttl_key)
                value_bytes = self._db._encode_value(str(expiry_time))
                with self._db._db_lock:
                    self._db._db.put(key_bytes, value_bytes)
            else:
                # 如果没有_db属性，则是RemoteDBDict，尝试直接操作
                self._db[ttl_key] = str(expiry_time)
        except Exception as e:
            import logging
            logging.error(f"保存TTL信息失败: {e}")
    
    def _remove_ttl_info(self, key):
        """
        从数据库中删除TTL信息
        
        Args:
            key: 键
        """
        if self._db is None:
            return
            
        try:
            ttl_key = self._get_ttl_key_str(key)
            # 直接使用底层_db对象避免调用__delitem__造成递归
            if hasattr(self._db, '_db'):
                key_bytes = self._db._encode_key(ttl_key)
                with self._db._db_lock:
                    self._db._db.delete(key_bytes)
            else:
                # 如果没有_db属性，则可能是RemoteDBDict，尝试直接操作
                if ttl_key in self._db:
                    del self._db[ttl_key]
        except Exception as e:
            import logging
            logging.error(f"删除TTL信息失败: {e}")
    
    def _load_ttl_data(self):
        """
        从数据库加载所有TTL信息
        """
        if self._db is None:
            return
            
        try:
            # 清空当前内存中的TTL信息
            self._expiry_dict = {}
            
            # 遍历数据库中所有的键
            prefix = "__ttl_info__:"
            for key in self._db.keys():
                # 检查是否为TTL信息键
                if isinstance(key, str) and key.startswith(prefix):
                    # 提取原始键
                    original_key = key[len(prefix):]
                    
                    # 获取过期时间
                    try:
                        expiry_time = float(self._db[key])
                        
                        # 检查是否已过期
                        current_time = time.time()
                        if expiry_time > current_time:
                            # 未过期，加载到内存
                            self._expiry_dict[original_key] = expiry_time
                        else:
                            # 已过期，从数据库中删除
                            del self._db[key]
                            if original_key in self._db:
                                del self._db[original_key]
                    except Exception as e:
                        import logging
                        logging.error(f"解析TTL数据失败 {key}: {e}")
                        
        except Exception as e:
            import logging
            logging.error(f"加载TTL数据失败: {e}") 