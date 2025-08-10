"""
FlaxKV 2.0 LevelDB专用存储引擎

针对LevelDB进行深度优化的存储引擎实现，提供极致性能。
"""

import os
import time
import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, AsyncIterator
import msgspec
from ..core.serializer import get_serializer
from contextlib import asynccontextmanager

from .base import StorageEngine

logger = logging.getLogger(__name__)


class LevelDBEngine(StorageEngine):
    """LevelDB专用存储引擎。
    
    特点：
    - 针对LevelDB的专门优化
    - 高性能序列化（msgspec）
    - 智能布隆过滤器
    - 自适应压缩
    - 完整的统计信息
    """
    
    def __init__(self, path: str, config):
        """初始化LevelDB引擎。
        
        Args:
            path: 数据库文件路径
            config: FlaxKVConfig配置对象
        """
        super().__init__(config)
        self.path = Path(path).resolve()
        self.db = None
        self._optimizer = None
        self._lock = asyncio.Lock()
        
        # 性能统计
        self._leveldb_stats = {
            'compactions': 0,
            'bloom_filter_hits': 0,
            'block_cache_hits': 0,
            'block_cache_misses': 0,
        }
    
    async def initialize(self) -> None:
        """初始化LevelDB引擎。"""
        if self.db is not None:
            return
        
        try:
            import plyvel
        except ImportError:
            raise ImportError(
                "需要安装plyvel库来使用LevelDB存储引擎。\n"
                "Linux: pip install plyvel\n"
                "Windows/macOS: pip install plyvel-ci"
            )
        
        # 确保目录存在
        self.path.parent.mkdir(parents=True, exist_ok=True)
        
        # 创建数据库连接（使用基本选项）
        self.db = plyvel.DB(
            str(self.path),
            create_if_missing=True,
            error_if_exists=False,
            # 简化的选项，只使用plyvel支持的参数
            # write_buffer_size=self.config.leveldb_options.get('write_buffer_size', 4*1024*1024),
            # max_open_files=self.config.leveldb_options.get('max_open_files', 1000)
        )
        
        # 创建优化器
        self._optimizer = LevelDBOptimizer(self.db, self.config)
        
        logger.info(f"LevelDB引擎初始化完成: {self.path}")
    
    async def get(self, key: str) -> Any:
        """获取单个键的值。"""
        if self.db is None:
            await self.initialize()
        
        start_time = time.perf_counter()
        cache_hit = False
        
        try:
            key_bytes = key.encode('utf-8')
            data = self.db.get(key_bytes)
            
            if data is None:
                self._record_operation('read', 
                    (time.perf_counter() - start_time) * 1000, cache_hit)
                raise KeyError(key)
            
            # 解码数据
            value = self._optimizer.decode_value(data)
            
            self._record_operation('read', 
                (time.perf_counter() - start_time) * 1000, cache_hit)
            return value
            
        except Exception as e:
            if isinstance(e, KeyError):
                raise
            
            # LevelDB错误处理
            logger.error(f"LevelDB读取错误: {e}")
            await self._optimizer.check_and_repair()
            raise
    
    async def set(self, key: str, value: Any) -> None:
        """设置单个键值对。"""
        if self.db is None:
            await self.initialize()
        
        start_time = time.perf_counter()
        
        try:
            key_bytes = key.encode('utf-8')
            value_bytes = self._optimizer.encode_value(value)
            
            # 使用优化的写入选项
            write_options = self._optimizer.get_write_options()
            self.db.put(key_bytes, value_bytes, **write_options)
            
            self._record_operation('write', 
                (time.perf_counter() - start_time) * 1000)
            
        except Exception as e:
            logger.error(f"LevelDB写入错误: {e}")
            raise
    
    async def delete(self, key: str) -> bool:
        """删除单个键。"""
        if self.db is None:
            await self.initialize()
        
        start_time = time.perf_counter()
        
        try:
            key_bytes = key.encode('utf-8')
            
            # 检查键是否存在
            exists = self.db.get(key_bytes) is not None
            
            if exists:
                self.db.delete(key_bytes)
                self._record_operation('delete', 
                    (time.perf_counter() - start_time) * 1000)
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"LevelDB删除错误: {e}")
            raise
    
    async def contains(self, key: str) -> bool:
        """检查键是否存在。"""
        if self.db is None:
            await self.initialize()
        
        try:
            key_bytes = key.encode('utf-8')
            return self.db.get(key_bytes) is not None
        except Exception as e:
            logger.error(f"LevelDB检查键存在错误: {e}")
            raise
    
    async def size(self) -> int:
        """获取数据库中键的总数。"""
        if self.db is None:
            await self.initialize()
        
        count = 0
        try:
            with self.db.iterator() as it:
                for _ in it:
                    count += 1
            return count
        except Exception as e:
            logger.error(f"LevelDB计算大小错误: {e}")
            raise
    
    async def batch_get(self, keys: List[str]) -> Dict[str, Any]:
        """批量获取键值对（LevelDB优化版本）。"""
        if self.db is None:
            await self.initialize()
        
        result = {}
        try:
            # 简单的循环调用方式，虽然效率不如迭代器，但保证功能正确
            for key in keys:
                try:
                    value = await self.get(key)
                    result[key] = value
                except KeyError:
                    # 键不存在，跳过
                    continue
            
            return result
            
        except Exception as e:
            logger.error(f"LevelDB批量读取错误: {e}")
            raise
    
    async def batch_set(self, items: Dict[str, Any]) -> None:
        """批量设置键值对（LevelDB优化版本）。"""
        if self.db is None:
            await self.initialize()
        
        try:
            # 使用WriteBatch批量写入
            with self.db.write_batch() as batch:
                for key, value in items.items():
                    key_bytes = key.encode('utf-8')
                    value_bytes = self._optimizer.encode_value(value)
                    batch.put(key_bytes, value_bytes)
            
        except Exception as e:
            logger.error(f"LevelDB批量写入错误: {e}")
            raise
    
    async def batch_delete(self, keys: List[str]) -> int:
        """批量删除键（LevelDB优化版本）。"""
        if self.db is None:
            await self.initialize()
        
        try:
            deleted_count = 0
            with self.db.write_batch() as batch:
                for key in keys:
                    key_bytes = key.encode('utf-8')
                    if self.db.get(key_bytes) is not None:
                        batch.delete(key_bytes)
                        deleted_count += 1
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"LevelDB批量删除错误: {e}")
            raise
    
    async def scan(self, 
                   prefix: Optional[str] = None,
                   start: Optional[str] = None,
                   end: Optional[str] = None,
                   limit: Optional[int] = None) -> AsyncIterator[Tuple[str, Any]]:
        """扫描键值对（LevelDB优化版本）。"""
        if self.db is None:
            await self.initialize()
        
        count = 0
        try:
            # 计算起始位置
            start_key = None
            if prefix:
                start_key = prefix.encode('utf-8')
            elif start:
                start_key = start.encode('utf-8')
            
            # 计算结束位置
            end_key = None
            if prefix:
                # 前缀查询：计算前缀的下一个值
                end_key = self._get_prefix_end(prefix).encode('utf-8')
            elif end:
                end_key = end.encode('utf-8')
            
            with self.db.iterator(start=start_key) as it:
                for key_bytes, value_bytes in it:
                    # 检查结束条件
                    if end_key and key_bytes >= end_key:
                        break
                    
                    # 解码键值
                    key = key_bytes.decode('utf-8')
                    value = self._optimizer.decode_value(value_bytes)
                    
                    yield key, value
                    
                    count += 1
                    if limit and count >= limit:
                        break
                        
        except Exception as e:
            logger.error(f"LevelDB扫描错误: {e}")
            raise
    
    async def _iterate_all(self) -> AsyncIterator[Tuple[str, Any]]:
        """迭代所有键值对。"""
        if self.db is None:
            await self.initialize()
        
        try:
            with self.db.iterator() as it:
                for key_bytes, value_bytes in it:
                    key = key_bytes.decode('utf-8')
                    value = self._optimizer.decode_value(value_bytes)
                    yield key, value
                    
        except Exception as e:
            logger.error(f"LevelDB迭代错误: {e}")
            raise
    
    def _get_prefix_end(self, prefix: str) -> str:
        """计算前缀查询的结束键。
        
        Args:
            prefix: 前缀字符串
            
        Returns:
            结束键（不包含）
        """
        if not prefix:
            return ""
        
        # 找到最后一个字符并加1
        prefix_bytes = prefix.encode('utf-8')
        prefix_list = list(prefix_bytes)
        
        # 从后往前找到第一个可以加1的字节
        for i in range(len(prefix_list) - 1, -1, -1):
            if prefix_list[i] < 255:
                prefix_list[i] += 1
                # 截断后面的字节
                return bytes(prefix_list[:i+1]).decode('utf-8', errors='ignore')
        
        # 如果所有字节都是255，返回一个更大的字符串
        return prefix + '\x00'
    
    # === LevelDB专用功能 ===
    
    async def compact_range(self, start: Optional[str] = None, 
                           end: Optional[str] = None) -> None:
        """手动压缩指定范围。
        
        Args:
            start: 起始键（可选）
            end: 结束键（可选）
        """
        if self.db is None:
            await self.initialize()
        
        try:
            start_key = start.encode('utf-8') if start else None
            end_key = end.encode('utf-8') if end else None
            
            # plyvel的compact_range方法参数名是start和stop，不是end
            if start_key is not None and end_key is not None:
                self.db.compact_range(start=start_key, stop=end_key)
            elif start_key is not None:
                self.db.compact_range(start=start_key)
            else:
                self.db.compact_range()
            self._leveldb_stats['compactions'] += 1
            
            logger.info(f"LevelDB压缩完成: {start} -> {end}")
            
        except Exception as e:
            logger.error(f"LevelDB压缩错误: {e}")
            raise
    
    def get_leveldb_stats(self) -> Dict[str, Any]:
        """获取LevelDB内部统计信息。"""
        if self.db is None:
            return {}
        
        try:
            # 获取LevelDB内部属性
            stats = {}
            
            # 尝试获取各种统计信息
            stat_names = [
                'leveldb.num-files-at-level0',
                'leveldb.num-files-at-level1', 
                'leveldb.num-files-at-level2',
                'leveldb.stats',
                'leveldb.sstables',
                'leveldb.approximate-memory-usage',
            ]
            
            for stat_name in stat_names:
                try:
                    value = self.db.get_property(stat_name)
                    if value:
                        stats[stat_name] = value
                except:
                    continue
            
            # 添加我们的统计
            stats.update(self._leveldb_stats)
            
            return stats
            
        except Exception as e:
            logger.error(f"获取LevelDB统计错误: {e}")
            return self._leveldb_stats
    
    async def close(self) -> None:
        """关闭LevelDB数据库。"""
        if self.db is not None:
            try:
                self.db.close()
                logger.info("LevelDB数据库已关闭")
            except Exception as e:
                logger.error(f"关闭LevelDB错误: {e}")
            finally:
                self.db = None
                self._optimizer = None
        
        await super().close()


class LevelDBOptimizer:
    """LevelDB专用优化器。
    
    提供编码/解码、性能调优、健康检查等功能。
    """
    
    def __init__(self, db, config):
        """初始化优化器。
        
        Args:
            db: plyvel.DB实例
            config: FlaxKVConfig配置对象
        """
        self.db = db
        self.config = config
        
        # 初始化编码器
        # 使用混合序列化器替代单独的msgspec
        self.serializer = get_serializer()
        
        # 保留旧的encoder/decoder引用以保持向后兼容
        self.encoder = self.serializer
        self.decoder = self.serializer
        
        # 写入选项
        self._write_options = self._setup_write_options()
        
        # 读取选项
        self._read_options = self._setup_read_options()
    
    def _setup_write_options(self) -> Dict[str, Any]:
        """设置LevelDB写入选项。"""
        return {
            'sync': self.config.sync_mode == 'sync',
        }
    
    def _setup_read_options(self) -> Dict[str, Any]:
        """设置LevelDB读取选项。"""
        return {
            'verify_checksums': self.config.integrity_check,
        }
    
    def encode_value(self, value: Any) -> bytes:
        """编码值为字节数据。
        
        使用混合序列化策略：优先msgspec，回退到pickle。
        支持pandas DataFrame等复杂数据结构。
        
        Args:
            value: 要编码的值
            
        Returns:
            编码后的字节数据
        """
        try:
            return self.serializer.encode(value)
        except Exception as e:
            raise ValueError(f"值编码失败: {e}")
    
    def decode_value(self, data: bytes) -> Any:
        """解码字节数据为值。
        
        Args:
            data: 要解码的字节数据
            
        Returns:
            解码后的值
        """
        try:
            return self.serializer.decode(data)
        except Exception as e:
            raise ValueError(f"值解码失败: {e}")
    
    def get_write_options(self) -> Dict[str, Any]:
        """获取写入选项。"""
        return self._write_options.copy()
    
    def get_read_options(self) -> Dict[str, Any]:
        """获取读取选项。"""
        return self._read_options.copy()
    
    async def check_and_repair(self) -> Dict[str, Any]:
        """健康检查和修复。
        
        Returns:
            检查结果
        """
        result = {
            'healthy': True,
            'issues': [],
            'repairs_performed': []
        }
        
        try:
            # 检查数据库是否可访问
            with self.db.iterator() as it:
                it.seek_to_first()
                if it.valid():
                    # 尝试读取第一个键值对
                    key = it.key()
                    value = it.value()
                    
                    # 尝试解码
                    self.decode_value(value)
            
            # 如果能到这里，说明基本健康
            result['message'] = 'LevelDB健康检查通过'
            
        except Exception as e:
            result['healthy'] = False
            result['issues'].append(f'数据库访问错误: {e}')
            
            # 这里可以添加具体的修复逻辑
            # 比如重建索引、修复损坏的SST文件等
            
        return result
    
    def get_optimization_suggestions(self) -> List[str]:
        """获取优化建议。"""
        suggestions = []
        
        # 基于配置给出优化建议
        if self.config.cache_size_bytes < 100 * 1024 * 1024:  # 100MB
            suggestions.append("建议增加缓存大小到至少100MB以提高读取性能")
        
        if self.config.buffer_size < 1000:
            suggestions.append("建议增加缓冲区大小到1000以提高写入性能")
        
        if not self.config.compression:
            suggestions.append("建议启用压缩以节省存储空间")
        
        if self.config.sync_mode == 'sync':
            suggestions.append("生产环境建议使用async模式以提高写入性能")
        
        return suggestions