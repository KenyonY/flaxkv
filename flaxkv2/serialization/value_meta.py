"""
FlaxKV2 带元数据的值编码/解码

实现TTL内嵌编码，将TTL信息直接嵌入到value的字节流中，
避免分离式存储带来的复杂性。

编码格式：
- 无TTL: [VERSION(1byte)][FLAGS(1byte)][VALUE_BYTES]
- 有TTL: [VERSION(1byte)][FLAGS(1byte)][EXPIRE_TIME(8bytes)][VALUE_BYTES]

VERSION: 0x01 (当前版本)
FLAGS: bit0=TTL标志, bit1-7保留用于未来扩展
"""

import struct
import time
from typing import Tuple, Optional, Any

from flaxkv2.serialization.encoder import encode
from flaxkv2.serialization.decoder import decode


# 版本号
VERSION = 0x01

# FLAGS位定义
FLAG_HAS_TTL = 0x01  # bit0: 是否有TTL


class ValueWithMeta:
    """带元数据的值编码/解码器"""

    @staticmethod
    def encode_value(value: Any, ttl_seconds: Optional[int] = None) -> bytes:
        """
        编码值（可选TTL）

        Args:
            value: 要编码的值
            ttl_seconds: TTL秒数（None表示无TTL）

        Returns:
            bytes: 编码后的字节流
        """
        # 序列化value
        value_bytes = encode(value)

        if ttl_seconds is None:
            # 格式：VERSION + FLAGS(无TTL) + VALUE
            return bytes([VERSION, 0x00]) + value_bytes
        else:
            # 格式：VERSION + FLAGS(有TTL) + EXPIRE_TIME + VALUE
            expire_time = time.time() + ttl_seconds
            return (
                bytes([VERSION, FLAG_HAS_TTL]) +
                struct.pack('d', expire_time) +  # 8字节double
                value_bytes
            )

    @staticmethod
    def decode_value(data: bytes) -> Tuple[Any, Optional[float], bool]:
        """
        解码值并提取TTL信息

        Args:
            data: 编码后的字节流

        Returns:
            (value, expire_time, is_expired):
            - value: 解码后的值
            - expire_time: 过期时间戳（None表示无TTL）
            - is_expired: 是否已过期

        Raises:
            ValueError: 数据格式无效
        """
        if len(data) < 2:
            raise ValueError(f"Invalid data format: too short (len={len(data)})")

        version = data[0]
        if version != VERSION:
            raise ValueError(f"Unsupported version: {version:#x}, expected {VERSION:#x}")

        flags = data[1]

        # 检查TTL标志
        if flags & FLAG_HAS_TTL:
            # 有TTL
            if len(data) < 10:  # VERSION(1) + FLAGS(1) + EXPIRE_TIME(8) = 10
                raise ValueError(f"Invalid TTL data: too short (len={len(data)})")

            expire_time = struct.unpack('d', data[2:10])[0]
            value_bytes = data[10:]
            value = decode(value_bytes)

            is_expired = time.time() > expire_time

            return value, expire_time, is_expired
        else:
            # 无TTL
            value_bytes = data[2:]
            value = decode(value_bytes)

            return value, None, False

    @staticmethod
    def has_meta(data: bytes) -> bool:
        """
        检查字节流是否包含元数据（是否是新格式）

        Args:
            data: 编码后的字节流

        Returns:
            bool: True表示新格式，False表示旧格式
        """
        if len(data) < 2:
            return False

        # 检查版本号
        return data[0] == VERSION

    @staticmethod
    def get_ttl_info(data: bytes) -> Tuple[bool, Optional[float]]:
        """
        快速提取TTL信息（不解码value）

        Args:
            data: 编码后的字节流

        Returns:
            (has_ttl, expire_time):
            - has_ttl: 是否有TTL
            - expire_time: 过期时间戳（如果没有TTL则为None）

        Raises:
            ValueError: 数据格式无效
        """
        if len(data) < 2:
            raise ValueError(f"Invalid data format: too short (len={len(data)})")

        version = data[0]
        if version != VERSION:
            raise ValueError(f"Unsupported version: {version:#x}")

        flags = data[1]

        if flags & FLAG_HAS_TTL:
            if len(data) < 10:
                raise ValueError(f"Invalid TTL data: too short (len={len(data)})")

            expire_time = struct.unpack('d', data[2:10])[0]
            return True, expire_time
        else:
            return False, None

    @staticmethod
    def is_expired_fast(data: bytes) -> bool:
        """
        快速检查是否过期（不解码value）

        Args:
            data: 编码后的字节流

        Returns:
            bool: 是否已过期（无TTL返回False）
        """
        try:
            has_ttl, expire_time = ValueWithMeta.get_ttl_info(data)
            if not has_ttl:
                return False
            return time.time() > expire_time
        except ValueError:
            # 格式错误：保守处理，认为未过期
            return False


# 便捷函数
def encode_with_ttl(value: Any, ttl_seconds: Optional[int] = None) -> bytes:
    """编码值（可选TTL）"""
    return ValueWithMeta.encode_value(value, ttl_seconds)


def decode_with_meta(data: bytes) -> Tuple[Any, Optional[float], bool]:
    """解码值并提取元数据"""
    return ValueWithMeta.decode_value(data)
