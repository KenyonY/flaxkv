"""
FlaxKV2 布隆过滤器实现
"""

import math
import mmh3
import numpy as np
from typing import Any, Callable


class BloomFilter:
    """
    布隆过滤器实现，用于快速判断元素是否可能存在
    """
    
    def __init__(self, capacity: int = 1000000, error_rate: float = 0.001):
        """
        初始化布隆过滤器
        
        Args:
            capacity: 预期元素数量
            error_rate: 期望的错误率
        """
        self.capacity = capacity
        self.error_rate = error_rate
        
        # 计算最佳位数组大小和哈希函数数量
        self.size = self._calculate_size(capacity, error_rate)
        self.hash_count = self._calculate_hash_count(self.size, capacity)
        
        # 使用NumPy位数组存储
        self.bit_array = np.zeros(self.size // 32 + 1, dtype=np.uint32)
    
    def _calculate_size(self, capacity: int, error_rate: float) -> int:
        """计算最佳位数组大小"""
        size = -1 * capacity * math.log(error_rate) / (math.log(2) ** 2)
        return math.ceil(size)
    
    def _calculate_hash_count(self, size: int, capacity: int) -> int:
        """计算最佳哈希函数数量"""
        hash_count = size / capacity * math.log(2)
        return math.ceil(hash_count)
    
    def _get_hash_values(self, item: Any) -> list:
        """获取元素的多个哈希值"""
        # 使用MurmurHash3计算哈希值，修复seed值范围问题
        value1 = mmh3.hash(str(item), 0)
        value2 = mmh3.hash(str(item), value1 & 0x7FFFFFFF)  # 确保seed为正数且在有效范围内
        
        # 使用双重哈希法生成多个哈希值
        return [(value1 + i * value2) % self.size for i in range(self.hash_count)]
    
    def add(self, item: Any):
        """
        添加元素到布隆过滤器
        
        Args:
            item: 要添加的元素
        """
        for index in self._get_hash_values(item):
            # 设置位
            array_index = index // 32
            bit_index = index % 32
            self.bit_array[array_index] |= (1 << bit_index)
    
    def check(self, item: Any) -> bool:
        """
        检查元素是否可能存在
        
        Args:
            item: 要检查的元素
            
        Returns:
            bool: 如果元素可能存在返回True，如果元素肯定不存在返回False
        """
        for index in self._get_hash_values(item):
            array_index = index // 32
            bit_index = index % 32
            
            # 检查位是否被设置
            if not (self.bit_array[array_index] & (1 << bit_index)):
                return False
        
        # 所有位都被设置，元素可能存在
        return True
        
    def reset(self):
        """重置布隆过滤器"""
        self.bit_array = np.zeros(self.size // 32 + 1, dtype=np.uint32) 