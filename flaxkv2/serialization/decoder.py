"""
FlaxKV2 数据解码模块
"""

import pickle
import msgpack
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Tuple, Union

# 导入与编码器相同的类型标识
from flaxkv2.serialization.encoder import (
    TYPE_MSGPACK, TYPE_NUMPY, TYPE_PANDAS, TYPE_PICKLE
)

def decode(data: bytes) -> Any:
    """
    将二进制数据解码为Python对象
    
    Args:
        data: 二进制编码数据
    
    Returns:
        解码后的Python对象
    """
    if not data:
        return None
        
    # 获取类型标识
    type_id = data[0]
    payload = data[1:]
    
    # 根据类型解码
    if type_id == TYPE_MSGPACK:
        return msgpack.unpackb(payload, raw=False)
    
    elif type_id == TYPE_NUMPY:
        array_data = msgpack.unpackb(payload, raw=False)
        array_bytes = array_data['data']
        dtype = np.dtype(array_data['dtype'])
        shape = tuple(array_data['shape'])
        return np.frombuffer(array_bytes, dtype=dtype).reshape(shape)
    
    elif type_id == TYPE_PANDAS:
        df_dict = msgpack.unpackb(payload, raw=False)
        columns = df_dict['columns']
        index = df_dict['index']
        values_bytes = df_dict['values']
        dtypes = [np.dtype(dt) for dt in df_dict['dtypes']]
        has_object = df_dict.get('has_object', False)
        
        if has_object:
            # 对于包含对象类型的DataFrame，使用pickle反序列化
            values = pickle.loads(values_bytes)
            df = pd.DataFrame(values, index=index, columns=columns)
            # 确保列的数据类型正确
            for col, dt in zip(columns, dtypes):
                df[col] = df[col].astype(dt)
        else:
            # 对于纯数值类型的DataFrame，使用二进制反序列化
            row_size = sum(dt.itemsize for dt in dtypes)
            n_rows = len(index)
            values = np.frombuffer(values_bytes, dtype=np.uint8).reshape(n_rows, -1)
            df = pd.DataFrame(index=index)
            
            offset = 0
            for i, (col, dt) in enumerate(zip(columns, dtypes)):
                col_size = dt.itemsize
                col_data = values[:, offset:offset+col_size].view(dt)
                df[col] = col_data
                offset += col_size
                
        return df
    
    elif type_id == TYPE_PICKLE:
        return pickle.loads(payload)
    
    else:
        raise ValueError(f"未知的数据类型标识: {type_id}")


def decode_key(key_bytes: bytes) -> Any:
    """
    将二进制数据解码为键
    
    Args:
        key_bytes: 二进制编码的键
    
    Returns:
        解码后的键
    """
    if not key_bytes:
        raise ValueError("Cannot decode empty key bytes")
        
    key_type = key_bytes[0:1]
    key_data = key_bytes[1:]
    
    if key_type == b's':
        return key_data.decode('utf-8')
    
    elif key_type == b'i':
        return int.from_bytes(key_data, byteorder='big', signed=True)
    
    elif key_type == b'f':
        import struct
        return struct.unpack('>d', key_data)[0]
    
    elif key_type == b'b':
        return key_data
    
    elif key_type == b't':
        # 解码元组
        result = []
        pos = 0
        while pos < len(key_data):
            # 读取长度
            length = int.from_bytes(key_data[pos:pos+4], byteorder='big')
            pos += 4
            # 读取数据
            item_bytes = key_data[pos:pos+length]
            item = decode_key(item_bytes)
            result.append(item)
            pos += length
        return tuple(result)
    
    elif key_type == b'o':
        return key_data.decode('utf-8')
    
    else:
        raise ValueError(f"未知的键类型标识: {key_type}") 