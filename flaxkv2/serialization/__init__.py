"""
FlaxKV2 序列化模块
"""

from flaxkv2.serialization.encoder import encode, encode_key
from flaxkv2.serialization.decoder import decode, decode_key

__all__ = ["encode", "encode_key", "decode", "decode_key"] 