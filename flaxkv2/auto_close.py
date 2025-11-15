"""
FlaxKV2 自动关闭管理模块

使用atexit确保所有打开的数据库实例在程序退出时自动关闭
"""

import atexit
import threading
from typing import Dict, Set, Any
import weakref
import logging

from flaxkv2.utils.log import get_logger

logger = get_logger(__name__)

class DBCloseManager:
    """
    数据库关闭管理器，负责追踪所有打开的数据库实例
    并确保在程序退出时关闭它们
    """
    
    def __init__(self):
        """初始化数据库关闭管理器"""
        self._instances = set()
        self._lock = threading.RLock()
        self._registered = False
        
    def register(self, db_instance: Any):
        """
        注册数据库实例到管理器
        
        Args:
            db_instance: 要注册的数据库实例
        """
        with self._lock:
            # 使用弱引用存储实例，避免内存泄漏
            self._instances.add(weakref.ref(db_instance, self._remove_instance))
            
            # 确保退出处理器已注册
            if not self._registered:
                atexit.register(self.close_all)
                self._registered = True
                
            logger.debug(f"Registered DB instance: {db_instance.name}")
    
    def _remove_instance(self, weak_ref):
        """
        在实例被垃圾回收时从集合中移除
        
        Args:
            weak_ref: 被回收的弱引用
        """
        with self._lock:
            try:
                self._instances.remove(weak_ref)
            except KeyError:
                pass
    
    def unregister(self, db_instance: Any):
        """
        从管理器中注销数据库实例
        
        Args:
            db_instance: 要注销的数据库实例
        """
        with self._lock:
            # 查找匹配的弱引用
            to_remove = None
            for weak_ref in self._instances:
                ref_instance = weak_ref()
                if ref_instance is not None and ref_instance is db_instance:
                    to_remove = weak_ref
                    break
                    
            if to_remove:
                self._instances.remove(to_remove)
                logger.debug(f"Unregistered DB instance: {db_instance.name}")
    
    def close_all(self):
        """
        关闭所有注册的数据库实例
        在程序退出时由atexit调用
        """
        logger.debug("程序退出，准备关闭所有数据库实例")
        
        with self._lock:
            instances_to_close = []
            
            # 收集有效的实例
            for weak_ref in list(self._instances):
                instance = weak_ref()
                if instance is not None and not instance._closed:
                    instances_to_close.append(instance)
            
            # 清空实例集合
            self._instances.clear()
            
            logger.debug(f"发现 {len(instances_to_close)} 个未关闭的数据库实例")
        
        # 关闭所有实例
        for instance in instances_to_close:
            try:
                logger.debug(f"自动关闭数据库实例: {instance.name} (路径: {instance.db_path})")
                instance.close()  # 修复：close() 方法不接受参数
                logger.debug(f"数据库实例 {instance.name} 已成功关闭")
            except Exception as e:
                logger.error(f"关闭数据库实例 {instance.name} 时发生错误: {e}")
        
        if instances_to_close:
            logger.debug(f"已成功关闭 {len(instances_to_close)} 个数据库实例")
        else:
            logger.debug("没有需要关闭的数据库实例")

# 创建全局单例实例
db_close_manager = DBCloseManager() 