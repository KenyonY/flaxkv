"""
FlaxKV2 数据编码模块
"""

import pickle
import msgpack
import numpy as np
from typing import Any, Dict, List, Tuple, Union

# Pandas 是可选依赖
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    pd = None

# 类型标识
TYPE_MSGPACK = 0  # 普通类型使用msgpack
TYPE_NUMPY = 1    # Numpy数组
TYPE_PANDAS = 2   # Pandas DataFrame
TYPE_PICKLE = 3   # 其他复杂对象

def encode(value: Any) -> bytes:
    """
    将Python对象编码为二进制数据
    
    Args:
        value: 需要编码的Python对象
    
    Returns:
        bytes: 编码后的二进制数据
        
    Raises:
        ImportError: 如果尝试序列化 Pandas 对象但未安装 pandas
    """
    # NumPy数组特殊处理
    if isinstance(value, np.ndarray):
        array_data = {
            'dtype': str(value.dtype),
            'shape': value.shape,
            'data': value.tobytes()
        }
        packed = msgpack.packb(array_data, use_bin_type=True)
        return bytes([TYPE_NUMPY]) + packed
    
    # Pandas DataFrame特殊处理
    elif HAS_PANDAS and pd is not None and isinstance(value, pd.DataFrame):
        # 检查是否包含对象类型列
        has_object = any(dt == 'object' for dt in value.dtypes)
        
        if has_object:
            # 对于包含对象类型的DataFrame，使用pickle序列化
            df_dict = {
                'columns': value.columns.tolist(),
                'index': value.index.tolist(),
                'values': pickle.dumps(value.values),
                'dtypes': [str(dt) for dt in value.dtypes],
                'has_object': True
            }
        else:
            # 对于纯数值类型的DataFrame，使用二进制序列化
            df_dict = {
                'columns': value.columns.tolist(),
                'index': value.index.tolist(),
                'values': value.values.tobytes(),
                'dtypes': [str(dt) for dt in value.dtypes],
                'has_object': False
            }
            
        packed = msgpack.packb(df_dict, use_bin_type=True)
        return bytes([TYPE_PANDAS]) + packed
        
    # 尝试使用msgpack（高效）
    try:
        packed = msgpack.packb(value, use_bin_type=True)
        return bytes([TYPE_MSGPACK]) + packed
    except (TypeError, OverflowError):
        # 回退到pickle（兼容性好）
        pickled = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        return bytes([TYPE_PICKLE]) + pickled


def encode_key(key: Any) -> bytes:
    """
    将键编码为二进制数据
    
    键的编码需要保证顺序性质，特别是对于数值类型
    
    Args:
        key: 需要编码的键
    
    Returns:
        bytes: 编码后的二进制数据
    """
    # 支持字符串、整数和浮点数作为键
    if isinstance(key, str):
        return b's' + key.encode('utf-8')
    elif isinstance(key, int):
        # 确保整数编码保持排序（使用网络字节序）
        return b'i' + key.to_bytes(8, byteorder='big', signed=True)
    elif isinstance(key, float):
        # 浮点数转换为字节
        import struct
        return b'f' + struct.pack('>d', key)
    elif isinstance(key, bytes):
        return b'b' + key
    elif isinstance(key, tuple):
        # 元组编码为连续的各元素编码
        result = b't'
        for item in key:
            encoded = encode_key(item)
            # 存储每段的长度以便解码
            result += len(encoded).to_bytes(4, byteorder='big')
            result += encoded
        return result
    else:
        # 其他类型尝试转换为字符串
        return b'o' + str(key).encode('utf-8') 