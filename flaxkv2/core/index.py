"""
FlaxKV2 索引模块实现
"""

import threading
import bisect
from typing import Any, Dict, List, Tuple, Set, Callable, Optional, Union
import pickle
from collections import defaultdict


class BaseIndex:
    """索引基类"""
    
    def __init__(self, name: str):
        """
        初始化索引
        
        Args:
            name: 索引名称
        """
        self.name = name
        self._lock = threading.RLock()
    
    def add(self, key: Any, value: Any) -> None:
        """添加键值对到索引"""
        raise NotImplementedError()
    
    def remove(self, key: Any) -> None:
        """从索引中移除键"""
        raise NotImplementedError()
    
    def update(self, key: Any, value: Any) -> None:
        """更新索引中的键值对"""
        self.remove(key)
        self.add(key, value)
    
    def search(self, *args, **kwargs) -> List[Any]:
        """搜索索引"""
        raise NotImplementedError()
    
    def clear(self) -> None:
        """清空索引"""
        raise NotImplementedError()
    
    def save(self, path: str) -> None:
        """
        保存索引到文件
        
        Args:
            path: 文件路径
        """
        raise NotImplementedError()
    
    def load(self, path: str) -> None:
        """
        从文件加载索引
        
        Args:
            path: 文件路径
        """
        raise NotImplementedError()


class HashIndex(BaseIndex):
    """哈希索引实现"""
    
    def __init__(self, name: str, field_accessor: Callable[[Any], Any]):
        """
        初始化哈希索引
        
        Args:
            name: 索引名称
            field_accessor: 字段访问函数，从值中提取索引字段
        """
        super().__init__(name)
        self.field_accessor = field_accessor
        # 索引结构: {字段值: 键集合}
        self._index = defaultdict(set)
        # 反向映射: {键: 字段值}，用于快速更新
        self._reverse_map = {}
    
    def add(self, key: Any, value: Any) -> None:
        """
        添加键值对到索引
        
        Args:
            key: 键
            value: 值
        """
        try:
            field_value = self.field_accessor(value)
        except (KeyError, AttributeError, TypeError):
            # 如果无法获取字段值，则不添加到索引
            return
            
        with self._lock:
            # 添加到正向索引
            self._index[field_value].add(key)
            # 更新反向映射
            self._reverse_map[key] = field_value
    
    def remove(self, key: Any) -> None:
        """
        从索引中移除键
        
        Args:
            key: 键
        """
        with self._lock:
            if key in self._reverse_map:
                field_value = self._reverse_map[key]
                
                # 从正向索引中移除
                if field_value in self._index:
                    self._index[field_value].discard(key)
                    
                    # 如果集合为空，删除该条目
                    if not self._index[field_value]:
                        del self._index[field_value]
                
                # 从反向映射中移除
                del self._reverse_map[key]
    
    def search(self, field_value: Any) -> List[Any]:
        """
        根据字段值搜索键
        
        Args:
            field_value: 字段值
            
        Returns:
            List[Any]: 匹配的键列表
        """
        with self._lock:
            return list(self._index.get(field_value, set()))
    
    def clear(self) -> None:
        """清空索引"""
        with self._lock:
            self._index.clear()
            self._reverse_map.clear()
    
    def save(self, path: str) -> None:
        """
        保存索引到文件
        
        Args:
            path: 文件路径
        """
        with self._lock:
            data = {
                'index': dict(self._index),
                'reverse_map': self._reverse_map
            }
            with open(path, 'wb') as f:
                pickle.dump(data, f)
    
    def load(self, path: str) -> None:
        """
        从文件加载索引
        
        Args:
            path: 文件路径
        """
        with open(path, 'rb') as f:
            data = pickle.load(f)
            
        with self._lock:
            # 转换回defaultdict
            self._index = defaultdict(set)
            for key, value in data['index'].items():
                self._index[key] = set(value)
                
            self._reverse_map = data['reverse_map']


class RangeIndex(BaseIndex):
    """范围索引实现，支持范围查询"""
    
    def __init__(self, name: str, field_accessor: Callable[[Any], Any]):
        """
        初始化范围索引
        
        Args:
            name: 索引名称
            field_accessor: 字段访问函数，从值中提取索引字段
        """
        super().__init__(name)
        self.field_accessor = field_accessor
        # 索引结构为有序列表，每个元素是(字段值, 键集合)
        self._sorted_values = []
        self._keys_map = {}  # 字段值 -> 键集合
        # 反向映射: {键: 字段值}，用于快速更新
        self._reverse_map = {}
    
    def add(self, key: Any, value: Any) -> None:
        """
        添加键值对到索引
        
        Args:
            key: 键
            value: 值
        """
        try:
            field_value = self.field_accessor(value)
            # 确保字段值可比较
            if field_value is None:
                return
        except (KeyError, AttributeError, TypeError):
            # 如果无法获取字段值，则不添加到索引
            return
            
        with self._lock:
            # 检查是否已经存在该字段值的集合
            if field_value in self._keys_map:
                # 添加键到现有集合
                self._keys_map[field_value].add(key)
            else:
                # 创建新集合
                self._keys_map[field_value] = {key}
                # 二分插入以保持有序
                bisect.insort(self._sorted_values, field_value)
            
            # 更新反向映射
            self._reverse_map[key] = field_value
    
    def remove(self, key: Any) -> None:
        """
        从索引中移除键
        
        Args:
            key: 键
        """
        with self._lock:
            if key in self._reverse_map:
                field_value = self._reverse_map[key]
                
                # 从键集合中移除
                if field_value in self._keys_map:
                    self._keys_map[field_value].discard(key)
                    
                    # 如果集合为空，删除该字段值
                    if not self._keys_map[field_value]:
                        del self._keys_map[field_value]
                        # 从有序列表中移除
                        idx = bisect.bisect_left(self._sorted_values, field_value)
                        if idx < len(self._sorted_values) and self._sorted_values[idx] == field_value:
                            self._sorted_values.pop(idx)
                
                # 从反向映射中移除
                del self._reverse_map[key]
    
    def search(self, min_value: Any = None, max_value: Any = None, include_min: bool = True, include_max: bool = False) -> List[Any]:
        """
        范围搜索
        
        Args:
            min_value: 最小值，None表示无下限
            max_value: 最大值，None表示无上限
            include_min: 是否包含最小值
            include_max: 是否包含最大值
            
        Returns:
            List[Any]: 匹配的键列表
        """
        result = set()
        
        with self._lock:
            # 确定起始位置
            if min_value is None:
                start_idx = 0
            else:
                start_idx = bisect.bisect_left(self._sorted_values, min_value) if include_min else bisect.bisect_right(self._sorted_values, min_value)
            
            # 确定结束位置
            if max_value is None:
                end_idx = len(self._sorted_values)
            else:
                end_idx = bisect.bisect_right(self._sorted_values, max_value) if include_max else bisect.bisect_left(self._sorted_values, max_value)
            
            # 收集范围内的所有键
            for i in range(start_idx, end_idx):
                field_value = self._sorted_values[i]
                result.update(self._keys_map[field_value])
        
        return list(result)
    
    def clear(self) -> None:
        """清空索引"""
        with self._lock:
            self._sorted_values.clear()
            self._keys_map.clear()
            self._reverse_map.clear()
    
    def save(self, path: str) -> None:
        """
        保存索引到文件
        
        Args:
            path: 文件路径
        """
        with self._lock:
            data = {
                'sorted_values': self._sorted_values,
                'keys_map': self._keys_map,
                'reverse_map': self._reverse_map
            }
            with open(path, 'wb') as f:
                pickle.dump(data, f)
    
    def load(self, path: str) -> None:
        """
        从文件加载索引
        
        Args:
            path: 文件路径
        """
        with open(path, 'rb') as f:
            data = pickle.load(f)
            
        with self._lock:
            self._sorted_values = data['sorted_values']
            self._keys_map = data['keys_map']
            self._reverse_map = data['reverse_map']


class IndexManager:
    """
    索引管理器，负责管理多个索引
    """
    
    def __init__(self):
        """初始化索引管理器"""
        self._indexes = {}  # 索引名称 -> 索引对象
        self._lock = threading.RLock()
    
    def add_index(self, index: BaseIndex) -> None:
        """
        添加索引
        
        Args:
            index: 索引对象
        """
        with self._lock:
            self._indexes[index.name] = index
    
    def remove_index(self, name: str) -> None:
        """
        移除索引
        
        Args:
            name: 索引名称
        """
        with self._lock:
            if name in self._indexes:
                del self._indexes[name]
    
    def get_index(self, name: str) -> Optional[BaseIndex]:
        """
        获取指定名称的索引
        
        Args:
            name: 索引名称
            
        Returns:
            BaseIndex: 索引对象，不存在则返回None
        """
        with self._lock:
            return self._indexes.get(name)
    
    def update_indexes(self, key: Any, value: Any) -> None:
        """
        更新所有索引
        
        Args:
            key: 键
            value: 值
        """
        with self._lock:
            for index in self._indexes.values():
                index.update(key, value)
    
    def remove_from_indexes(self, key: Any) -> None:
        """
        从所有索引中移除键
        
        Args:
            key: 键
        """
        with self._lock:
            for index in self._indexes.values():
                index.remove(key)
                
    def search(self, index_name: str, *args, **kwargs) -> List[Any]:
        """
        使用指定索引搜索
        
        Args:
            index_name: 索引名称
            *args, **kwargs: 传递给索引的搜索参数
            
        Returns:
            List[Any]: 匹配的键列表
        """
        with self._lock:
            index = self._indexes.get(index_name)
            if index is None:
                return []
            return index.search(*args, **kwargs)
    
    def clear_all(self) -> None:
        """清空所有索引"""
        with self._lock:
            for index in self._indexes.values():
                index.clear()
    
    def save_all(self, base_path: str) -> None:
        """
        保存所有索引
        
        Args:
            base_path: 基础路径
        """
        import os
        with self._lock:
            for name, index in self._indexes.items():
                path = os.path.join(base_path, f"{name}.idx")
                index.save(path)
    
    def load_all(self, base_path: str) -> None:
        """
        加载所有索引
        
        Args:
            base_path: 基础路径
        """
        import os
        with self._lock:
            for name, index in self._indexes.items():
                path = os.path.join(base_path, f"{name}.idx")
                if os.path.exists(path):
                    index.load(path) 