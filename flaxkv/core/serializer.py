"""
FlaxKV 2.0 混合序列化器

提供高性能的混合序列化方案：
- 优先使用msgspec进行高性能序列化
- 当msgspec不支持时自动回退到pickle
- 支持pandas DataFrame等复杂数据结构
"""

import pickle
import logging
from typing import Any, Union
from enum import IntEnum

try:
    import msgspec
    MSGSPEC_AVAILABLE = True
except ImportError:
    MSGSPEC_AVAILABLE = False

logger = logging.getLogger(__name__)


class SerializationMethod(IntEnum):
    """序列化方法标识。"""
    MSGSPEC = 1  # msgspec.msgpack 高性能序列化
    PICKLE = 2   # pickle 通用序列化


class HybridSerializer:
    """混合序列化器。
    
    优先使用高性能的msgspec，当不支持时自动回退到pickle。
    这样既能保证基本数据类型的高性能，又能支持复杂数据结构。
    """
    
    def __init__(self):
        """初始化混合序列化器。"""
        self.msgspec_encoder = None
        self.msgspec_decoder = None
        
        if MSGSPEC_AVAILABLE:
            self.msgspec_encoder = msgspec.msgpack.Encoder()
            self.msgspec_decoder = msgspec.msgpack.Decoder()
            logger.debug("msgspec可用，将优先使用高性能序列化")
        else:
            logger.warning("msgspec不可用，将使用pickle序列化")
    
    def encode(self, value: Any) -> bytes:
        """编码值为字节数据。
        
        Args:
            value: 要编码的值
            
        Returns:
            编码后的字节数据（包含序列化方法标记）
            
        Raises:
            ValueError: 编码失败时抛出
        """
        # 优先尝试msgspec
        if self.msgspec_encoder is not None:
            try:
                data = self.msgspec_encoder.encode(value)
                # 添加方法标记：1字节标记 + 实际数据
                return bytes([SerializationMethod.MSGSPEC]) + data
            except Exception as e:
                logger.debug(f"msgspec编码失败，回退到pickle: {e}")
        
        # 回退到pickle
        try:
            data = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
            # 添加方法标记：1字节标记 + 实际数据
            return bytes([SerializationMethod.PICKLE]) + data
        except Exception as e:
            raise ValueError(f"序列化失败: {e}")
    
    def decode(self, data: bytes) -> Any:
        """解码字节数据为值。
        
        Args:
            data: 要解码的字节数据（包含序列化方法标记）
            
        Returns:
            解码后的值
            
        Raises:
            ValueError: 解码失败时抛出
        """
        if not data or len(data) < 2:
            raise ValueError("数据太短，无法包含序列化方法标记")
        
        # 读取序列化方法标记
        method = data[0]
        actual_data = data[1:]
        
        if method == SerializationMethod.MSGSPEC:
            if self.msgspec_decoder is None:
                raise ValueError("数据使用msgspec序列化，但msgspec不可用")
            
            try:
                return self.msgspec_decoder.decode(actual_data)
            except Exception as e:
                raise ValueError(f"msgspec反序列化失败: {e}")
                
        elif method == SerializationMethod.PICKLE:
            try:
                return pickle.loads(actual_data)
            except Exception as e:
                raise ValueError(f"pickle反序列化失败: {e}")
        
        else:
            raise ValueError(f"未知的序列化方法标记: {method}")
    
    def get_method_for_value(self, value: Any) -> SerializationMethod:
        """获取值应该使用的序列化方法。
        
        Args:
            value: 要检查的值
            
        Returns:
            推荐的序列化方法
        """
        if self.msgspec_encoder is None:
            return SerializationMethod.PICKLE
        
        try:
            # 尝试msgspec编码
            self.msgspec_encoder.encode(value)
            return SerializationMethod.MSGSPEC
        except Exception:
            return SerializationMethod.PICKLE
    
    def get_stats(self) -> dict:
        """获取序列化器统计信息。
        
        Returns:
            统计信息字典
        """
        return {
            'msgspec_available': MSGSPEC_AVAILABLE,
            'supports_pandas': True,  # 通过pickle支持
            'supports_numpy': True,   # 通过pickle支持
            'high_performance_types': [
                'dict', 'list', 'str', 'int', 'float', 'bool', 'NoneType'
            ] if MSGSPEC_AVAILABLE else [],
            'fallback_types': [
                'pandas.DataFrame', 'pandas.Series', 'numpy.ndarray', 
                'custom_classes', 'complex_objects'
            ]
        }


# 全局序列化器实例
_global_serializer = None


def get_serializer() -> HybridSerializer:
    """获取全局序列化器实例。
    
    Returns:
        HybridSerializer实例
    """
    global _global_serializer
    if _global_serializer is None:
        _global_serializer = HybridSerializer()
    return _global_serializer


def encode_value(value: Any) -> bytes:
    """编码值的便捷函数。
    
    Args:
        value: 要编码的值
        
    Returns:
        编码后的字节数据
    """
    return get_serializer().encode(value)


def decode_value(data: bytes) -> Any:
    """解码值的便捷函数。
    
    Args:
        data: 要解码的字节数据
        
    Returns:
        解码后的值
    """
    return get_serializer().decode(data)


def get_serialization_stats() -> dict:
    """获取序列化统计信息的便捷函数。
    
    Returns:
        统计信息字典
    """
    return get_serializer().get_stats()


# 检测pandas支持
def test_pandas_support() -> bool:
    """测试pandas支持。
    
    Returns:
        是否支持pandas DataFrame序列化
    """
    try:
        import pandas as pd
        
        # 创建测试DataFrame
        df = pd.DataFrame({
            'A': [1, 2, 3],
            'B': ['a', 'b', 'c'],
            'C': [1.1, 2.2, 3.3]
        })
        
        # 测试序列化和反序列化
        serializer = get_serializer()
        encoded = serializer.encode(df)
        decoded = serializer.decode(encoded)
        
        # 验证数据完整性
        return df.equals(decoded)
        
    except ImportError:
        logger.info("pandas未安装，跳过pandas支持测试")
        return False
    except Exception as e:
        logger.error(f"pandas支持测试失败: {e}")
        return False


def test_numpy_support() -> bool:
    """测试numpy支持。
    
    Returns:
        是否支持numpy数组序列化
    """
    try:
        import numpy as np
        
        # 创建测试数组
        arr = np.array([[1, 2, 3], [4, 5, 6]])
        
        # 测试序列化和反序列化
        serializer = get_serializer()
        encoded = serializer.encode(arr)
        decoded = serializer.decode(encoded)
        
        # 验证数据完整性
        return np.array_equal(arr, decoded)
        
    except ImportError:
        logger.info("numpy未安装，跳过numpy支持测试")
        return False
    except Exception as e:
        logger.error(f"numpy支持测试失败: {e}")
        return False