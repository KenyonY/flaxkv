"""
FlaxKV 2.0 压缩算法处理

提供多种压缩算法支持，优化存储空间和传输效率。
"""

import logging
from typing import Any, Optional, Tuple
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class CompressionAlgorithm(ABC):
    """压缩算法抽象基类。"""
    
    @abstractmethod
    def compress(self, data: bytes) -> bytes:
        """压缩数据。"""
        pass
    
    @abstractmethod
    def decompress(self, compressed_data: bytes) -> bytes:
        """解压数据。"""
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """算法名称。"""
        pass


class NoCompression(CompressionAlgorithm):
    """无压缩算法（直接传递）。"""
    
    def compress(self, data: bytes) -> bytes:
        return data
    
    def decompress(self, compressed_data: bytes) -> bytes:
        return compressed_data
    
    @property
    def name(self) -> str:
        return "none"


class GzipCompression(CompressionAlgorithm):
    """Gzip压缩算法。"""
    
    def __init__(self, level: int = 6):
        """
        Args:
            level: 压缩级别 (1-9)，数字越大压缩率越高但速度越慢
        """
        self.level = max(1, min(9, level))
        self._available = True
        
        try:
            import gzip
            self._gzip = gzip
        except ImportError:
            logger.warning("gzip模块不可用")
            self._available = False
    
    def compress(self, data: bytes) -> bytes:
        if not self._available:
            return data
        
        try:
            return self._gzip.compress(data, compresslevel=self.level)
        except Exception as e:
            logger.error(f"Gzip压缩失败: {e}")
            return data
    
    def decompress(self, compressed_data: bytes) -> bytes:
        if not self._available:
            return compressed_data
        
        try:
            return self._gzip.decompress(compressed_data)
        except Exception as e:
            logger.error(f"Gzip解压失败: {e}")
            return compressed_data
    
    @property
    def name(self) -> str:
        return f"gzip-{self.level}"


class LZ4Compression(CompressionAlgorithm):
    """LZ4压缩算法（高性能）。"""
    
    def __init__(self):
        self._available = True
        
        try:
            import lz4.frame
            self._lz4 = lz4.frame
        except ImportError:
            logger.warning("lz4模块不可用，请安装: pip install lz4")
            self._available = False
    
    def compress(self, data: bytes) -> bytes:
        if not self._available:
            return data
        
        try:
            return self._lz4.compress(data)
        except Exception as e:
            logger.error(f"LZ4压缩失败: {e}")
            return data
    
    def decompress(self, compressed_data: bytes) -> bytes:
        if not self._available:
            return compressed_data
        
        try:
            return self._lz4.decompress(compressed_data)
        except Exception as e:
            logger.error(f"LZ4解压失败: {e}")
            return compressed_data
    
    @property
    def name(self) -> str:
        return "lz4"


class ZstdCompression(CompressionAlgorithm):
    """Zstandard压缩算法（平衡压缩率和速度）。"""
    
    def __init__(self, level: int = 3):
        """
        Args:
            level: 压缩级别 (1-22)
        """
        self.level = max(1, min(22, level))
        self._available = True
        
        try:
            import zstandard as zstd
            self._zstd = zstd
            self._compressor = zstd.ZstdCompressor(level=self.level)
            self._decompressor = zstd.ZstdDecompressor()
        except ImportError:
            logger.warning("zstandard模块不可用，请安装: pip install zstandard")
            self._available = False
    
    def compress(self, data: bytes) -> bytes:
        if not self._available:
            return data
        
        try:
            return self._compressor.compress(data)
        except Exception as e:
            logger.error(f"Zstd压缩失败: {e}")
            return data
    
    def decompress(self, compressed_data: bytes) -> bytes:
        if not self._available:
            return compressed_data
        
        try:
            return self._decompressor.decompress(compressed_data)
        except Exception as e:
            logger.error(f"Zstd解压失败: {e}")
            return compressed_data
    
    @property
    def name(self) -> str:
        return f"zstd-{self.level}"


class CompressionManager:
    """压缩管理器，负责算法选择和性能监控。"""
    
    ALGORITHMS = {
        'none': NoCompression,
        'gzip': lambda: GzipCompression(),
        'gzip-1': lambda: GzipCompression(1),
        'gzip-6': lambda: GzipCompression(6),  
        'gzip-9': lambda: GzipCompression(9),
        'lz4': LZ4Compression,
        'zstd': lambda: ZstdCompression(),
        'zstd-1': lambda: ZstdCompression(1),
        'zstd-3': lambda: ZstdCompression(3),
        'zstd-10': lambda: ZstdCompression(10),
    }
    
    def __init__(self, algorithm_name: str = 'gzip'):
        self.algorithm_name = algorithm_name
        self.algorithm = self._create_algorithm(algorithm_name)
        
        # 统计信息
        self._stats = {
            'compressions': 0,
            'decompressions': 0,
            'total_input_bytes': 0,
            'total_compressed_bytes': 0,
            'total_errors': 0,
        }
    
    def _create_algorithm(self, name: str) -> CompressionAlgorithm:
        """创建压缩算法实例。"""
        if name not in self.ALGORITHMS:
            logger.warning(f"未知压缩算法: {name}，使用默认算法")
            name = 'gzip'
        
        try:
            algorithm_factory = self.ALGORITHMS[name]
            return algorithm_factory()
        except Exception as e:
            logger.error(f"创建压缩算法失败 {name}: {e}")
            return NoCompression()
    
    def compress(self, data: bytes) -> Tuple[bytes, bool]:
        """压缩数据。
        
        Args:
            data: 原始数据
            
        Returns:
            (压缩后的数据, 是否成功压缩)
        """
        if not isinstance(data, bytes):
            raise TypeError("数据必须是bytes类型")
        
        original_size = len(data)
        
        try:
            compressed_data = self.algorithm.compress(data)
            compressed_size = len(compressed_data)
            
            # 更新统计
            self._stats['compressions'] += 1
            self._stats['total_input_bytes'] += original_size
            self._stats['total_compressed_bytes'] += compressed_size
            
            # 判断压缩是否有效（压缩后大小更小）
            compression_effective = compressed_size < original_size
            
            logger.debug(f"压缩完成: {original_size} -> {compressed_size} bytes "
                        f"({compressed_size/original_size*100:.1f}%)")
            
            return compressed_data, compression_effective
            
        except Exception as e:
            self._stats['total_errors'] += 1
            logger.error(f"压缩失败: {e}")
            return data, False
    
    def decompress(self, compressed_data: bytes) -> bytes:
        """解压数据。"""
        if not isinstance(compressed_data, bytes):
            raise TypeError("压缩数据必须是bytes类型")
        
        try:
            decompressed_data = self.algorithm.decompress(compressed_data)
            self._stats['decompressions'] += 1
            
            logger.debug(f"解压完成: {len(compressed_data)} -> {len(decompressed_data)} bytes")
            
            return decompressed_data
            
        except Exception as e:
            self._stats['total_errors'] += 1
            logger.error(f"解压失败: {e}")
            return compressed_data
    
    def get_stats(self) -> dict:
        """获取压缩统计信息。"""
        stats = self._stats.copy()
        
        if stats['compressions'] > 0:
            stats['average_compression_ratio'] = (
                stats['total_compressed_bytes'] / stats['total_input_bytes']
            )
            stats['space_saved_bytes'] = (
                stats['total_input_bytes'] - stats['total_compressed_bytes']
            )
            stats['space_saved_percent'] = (
                (1 - stats['average_compression_ratio']) * 100
            )
        else:
            stats['average_compression_ratio'] = 1.0
            stats['space_saved_bytes'] = 0
            stats['space_saved_percent'] = 0.0
        
        stats['algorithm'] = self.algorithm.name
        
        return stats
    
    def reset_stats(self) -> None:
        """重置统计信息。"""
        self._stats = {
            'compressions': 0,
            'decompressions': 0, 
            'total_input_bytes': 0,
            'total_compressed_bytes': 0,
            'total_errors': 0,
        }
        logger.info("压缩统计信息已重置")
    
    def benchmark(self, test_data: bytes, algorithms: Optional[list] = None) -> dict:
        """基准测试不同压缩算法。
        
        Args:
            test_data: 测试数据
            algorithms: 要测试的算法列表，None表示测试所有可用算法
            
        Returns:
            基准测试结果
        """
        import time
        
        if algorithms is None:
            algorithms = ['none', 'gzip', 'lz4', 'zstd']
        
        results = {}
        
        for algo_name in algorithms:
            if algo_name not in self.ALGORITHMS:
                continue
            
            try:
                algo = self._create_algorithm(algo_name)
                
                # 压缩测试
                start_time = time.perf_counter()
                compressed_data = algo.compress(test_data)
                compress_time = time.perf_counter() - start_time
                
                # 解压测试
                start_time = time.perf_counter()
                decompressed_data = algo.decompress(compressed_data)
                decompress_time = time.perf_counter() - start_time
                
                # 验证数据完整性
                data_integrity = test_data == decompressed_data
                
                results[algo_name] = {
                    'original_size': len(test_data),
                    'compressed_size': len(compressed_data),
                    'compression_ratio': len(compressed_data) / len(test_data),
                    'compress_time': compress_time,
                    'decompress_time': decompress_time,
                    'total_time': compress_time + decompress_time,
                    'data_integrity': data_integrity,
                    'throughput_mb_s': (len(test_data) / 1024 / 1024) / (compress_time + decompress_time),
                }
                
            except Exception as e:
                logger.error(f"算法 {algo_name} 基准测试失败: {e}")
                results[algo_name] = {'error': str(e)}
        
        return results