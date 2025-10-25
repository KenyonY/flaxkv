"""
数据库实例管理器

管理全局的数据库实例缓存，避免同一个数据库被多次打开导致的冲突。
"""

import os
import threading
from typing import Dict, Optional, Any
from flaxkv2.utils.log import get_logger

logger = get_logger(__name__)


class DBInstanceManager:
    """
    数据库实例管理器（单例模式）

    功能：
    - 缓存已打开的数据库实例
    - 避免同一数据库被多次打开
    - 在rebuild时自动关闭旧实例
    - 线程安全
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """初始化实例管理器"""
        if self._initialized:
            return

        self._instances: Dict[str, Any] = {}  # key: db_path, value: db_instance
        self._instance_lock = threading.RLock()
        self._initialized = True
        logger.debug("数据库实例管理器已初始化")

    def _normalize_path(self, path: str) -> str:
        """标准化路径（转为绝对路径）"""
        return os.path.abspath(path)

    def get_instance(self, db_path: str) -> Optional[Any]:
        """
        获取已缓存的数据库实例

        Args:
            db_path: 数据库路径

        Returns:
            数据库实例或None
        """
        normalized_path = self._normalize_path(db_path)
        with self._instance_lock:
            instance = self._instances.get(normalized_path)
            if instance is not None:
                logger.debug(f"返回已缓存的数据库实例: {normalized_path}")
            return instance

    def register_instance(self, db_path: str, instance: Any) -> None:
        """
        注册数据库实例到缓存

        Args:
            db_path: 数据库路径
            instance: 数据库实例
        """
        normalized_path = self._normalize_path(db_path)
        with self._instance_lock:
            # 如果已有实例，先关闭它
            if normalized_path in self._instances:
                old_instance = self._instances[normalized_path]
                logger.warning(f"数据库 {normalized_path} 已有实例，将关闭旧实例")
                try:
                    if hasattr(old_instance, 'close'):
                        old_instance.close()
                except Exception as e:
                    logger.error(f"关闭旧实例时出错: {e}")

            self._instances[normalized_path] = instance
            logger.debug(f"已注册数据库实例: {normalized_path}")

    def unregister_instance(self, db_path: str) -> None:
        """
        从缓存中移除数据库实例

        Args:
            db_path: 数据库路径
        """
        normalized_path = self._normalize_path(db_path)
        with self._instance_lock:
            if normalized_path in self._instances:
                del self._instances[normalized_path]
                logger.debug(f"已移除数据库实例: {normalized_path}")

    def close_instance(self, db_path: str) -> bool:
        """
        关闭并移除指定的数据库实例

        Args:
            db_path: 数据库路径

        Returns:
            是否成功关闭
        """
        normalized_path = self._normalize_path(db_path)
        with self._instance_lock:
            instance = self._instances.get(normalized_path)
            if instance is None:
                return False

            try:
                if hasattr(instance, 'close'):
                    instance.close()
                self.unregister_instance(db_path)
                logger.debug(f"已关闭数据库实例: {normalized_path}")
                return True
            except Exception as e:
                logger.error(f"关闭数据库实例时出错: {normalized_path}, 错误: {e}")
                return False

    def close_all(self) -> None:
        """关闭所有缓存的数据库实例"""
        with self._instance_lock:
            paths = list(self._instances.keys())
            for path in paths:
                try:
                    self.close_instance(path)
                except Exception as e:
                    logger.error(f"关闭数据库 {path} 时出错: {e}")
            logger.debug("已关闭所有数据库实例")

    def get_all_instances(self) -> Dict[str, Any]:
        """
        获取所有缓存的实例（用于调试）

        Returns:
            路径到实例的字典
        """
        with self._instance_lock:
            return dict(self._instances)


# 全局实例管理器
db_instance_manager = DBInstanceManager()
