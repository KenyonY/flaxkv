"""
FlaxKV2 TTL(生存时间)支持

重构后的简化实现：
- 不使用内存存储 (_expiry_dict)
- TTL信息作为普通键存储在数据库中
- 所有操作直接通过数据库接口完成
"""

import time
import threading
from typing import Any, List, Optional


class TTLManager:
    """
    TTL管理器，处理键的过期时间

    简化设计：TTL信息作为普通键值对存储，格式为 '__ttl_info__:<key>' -> <expiry_timestamp>
    """

    # 用于存储TTL信息的特殊键前缀
    TTL_KEY_PREFIX = "__ttl_info__:"

    def __init__(self, cleanup_interval: int = 60, db=None):
        """
        初始化TTL管理器

        Args:
            cleanup_interval: 清理检查间隔（秒）
            db: 数据库引用
        """
        self._lock = threading.RLock()
        self._cleanup_interval = cleanup_interval
        self._last_cleanup_time = time.time()
        self._db = db

    def set_db(self, db):
        """
        设置数据库引用

        Args:
            db: 数据库引用
        """
        self._db = db
    
    def set(self, key: Any, ttl_seconds: int):
        """
        设置键的TTL

        Args:
            key: 键
            ttl_seconds: 生存时间（秒）
        """
        if ttl_seconds <= 0:
            return

        if self._db is None:
            import logging
            logging.warning("TTL管理器的数据库引用未设置，无法设置TTL")
            return

        with self._lock:
            expiry_time = time.time() + ttl_seconds
            ttl_key = self._get_ttl_key(key)

            # 直接操作底层数据库，绕过 __setitem__ 避免递归
            if hasattr(self._db, '_db') and hasattr(self._db, '_encode_key'):
                # 本地数据库：直接操作 LevelDB
                ttl_key_bytes = self._db._encode_key(ttl_key)
                ttl_value_bytes = self._db._encode_value(expiry_time)
                with self._db._db_lock:
                    self._db._db.put(ttl_key_bytes, ttl_value_bytes)
            else:
                # 远程数据库：无底层访问，使用普通 __setitem__
                # RemoteDBDict 不会有 default_ttl 递归问题
                self._db[ttl_key] = expiry_time

    def set_ttl(self, key: Any, ttl_seconds: int):
        """
        设置键的TTL（set方法的别名）

        Args:
            key: 键
            ttl_seconds: 生存时间（秒）
        """
        self.set(key, ttl_seconds)

    def _get_ttl_key(self, key: Any) -> str:
        """
        获取TTL信息键

        Args:
            key: 原始键

        Returns:
            str: TTL信息键
        """
        # 将键转换为字符串
        if isinstance(key, str):
            key_str = key
        elif isinstance(key, bytes):
            try:
                key_str = key.decode('utf-8')
            except:
                key_str = str(key)
        else:
            key_str = str(key)

        return self.TTL_KEY_PREFIX + key_str

    def get_expiry(self, key: Any) -> Optional[float]:
        """
        获取键的过期时间

        Args:
            key: 键

        Returns:
            float: 过期时间戳，如果没有设置TTL则返回None
        """
        if self._db is None:
            return None

        with self._lock:
            ttl_key = self._get_ttl_key(key)
            try:
                # 直接操作底层数据库，绕过 __getitem__ 避免递归
                if hasattr(self._db, '_db') and hasattr(self._db, '_encode_key'):
                    # 本地数据库：直接操作 LevelDB
                    ttl_key_bytes = self._db._encode_key(ttl_key)
                    with self._db._db_lock:
                        ttl_value_bytes = self._db._db.get(ttl_key_bytes)
                    if ttl_value_bytes is None:
                        return None
                    expiry_time = self._db._decode_value(ttl_value_bytes)
                    return float(expiry_time)
                else:
                    # 远程数据库：使用普通 __getitem__
                    expiry_time = self._db[ttl_key]
                    return float(expiry_time)
            except KeyError:
                return None

    def get_remaining_ttl(self, key: Any) -> Optional[int]:
        """
        获取键的剩余TTL时间

        Args:
            key: 键

        Returns:
            int: 剩余TTL秒数，如果没有设置TTL则返回None
        """
        expiry_time = self.get_expiry(key)
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
            bool: 如果键已过期返回True，否则返回False
        """
        expiry_time = self.get_expiry(key)
        if expiry_time is None:
            return False
        return time.time() > expiry_time

    def remove(self, key: Any):
        """
        移除键的TTL设置

        Args:
            key: 键
        """
        if self._db is None:
            return

        with self._lock:
            ttl_key = self._get_ttl_key(key)
            try:
                # 直接操作底层数据库，绕过 __delitem__ 避免递归
                if hasattr(self._db, '_db') and hasattr(self._db, '_encode_key'):
                    # 本地数据库：直接操作 LevelDB
                    ttl_key_bytes = self._db._encode_key(ttl_key)
                    with self._db._db_lock:
                        self._db._db.delete(ttl_key_bytes)
                else:
                    # 远程数据库：使用普通 __delitem__
                    del self._db[ttl_key]
            except KeyError:
                pass
    
    def get_expired_keys(self) -> List[Any]:
        """
        获取所有已过期的键

        Returns:
            List[Any]: 过期键列表
        """
        if self._db is None:
            return []

        current_time = time.time()
        expired_keys = []

        with self._lock:
            # 遍历数据库中的所有键，找到TTL信息键
            try:
                for key in self._db.keys():
                    # 检查是否是TTL信息键
                    if isinstance(key, str) and key.startswith(self.TTL_KEY_PREFIX):
                        # 提取原始键名
                        original_key = key[len(self.TTL_KEY_PREFIX):]

                        try:
                            expiry_time = float(self._db[key])
                            if current_time > expiry_time:
                                expired_keys.append(original_key)
                        except (ValueError, KeyError):
                            continue
            except Exception as e:
                import logging
                logging.error(f"获取过期键列表失败: {e}")

        return expired_keys

    def cleanup_expired(self) -> int:
        """
        清理所有已过期的键（从数据库中删除）

        Returns:
            int: 清理的键数量
        """
        if self._db is None:
            return 0

        expired_keys = self.get_expired_keys()
        count = 0

        for key in expired_keys:
            try:
                # 删除原始键
                if key in self._db:
                    del self._db[key]
                    count += 1

                # 删除TTL信息键
                ttl_key = self._get_ttl_key(key)
                if ttl_key in self._db:
                    del self._db[ttl_key]
            except Exception as e:
                import logging
                logging.error(f"清理过期键 {key} 失败: {e}")

        return count 