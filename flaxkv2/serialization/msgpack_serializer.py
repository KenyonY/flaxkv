"""MsgPack serializer implementation."""
import pickle
from typing import Any

import msgpack
import msgspec
import numpy as np

from ..core.interfaces import Serializer

class NPArray(msgspec.Struct, gc=False, array_like=True):
    """Numpy array serialization structure."""
    dtype: str
    shape: tuple
    data: bytes

class MsgPackSerializer(Serializer):
    """MsgPack-based serializer with support for numpy arrays and custom objects."""
    
    def __init__(self):
        """Initialize the serializer with numpy array support."""
        self._np_encoder = msgspec.msgpack.Encoder()
        self._np_decoder = msgspec.msgpack.Decoder(type=NPArray)
        self._encoder = msgspec.msgpack.Encoder(enc_hook=self._encode_hook)
        self._decoder = msgspec.msgpack.Decoder(ext_hook=self._ext_hook)
    
    def _encode_hook(self, obj: Any) -> Any:
        """Hook for encoding special types."""
        if isinstance(obj, np.ndarray):
            return msgspec.msgpack.Ext(
                1,  # type code for numpy arrays
                self._np_encoder.encode(
                    NPArray(dtype=obj.dtype.str, shape=obj.shape, data=obj.data)
                ),
            )
        # For other non-serializable objects, use pickle as fallback
        return msgspec.msgpack.Ext(2, pickle.dumps(obj))
    
    def _ext_hook(self, type_code: int, data: memoryview) -> Any:
        """Hook for decoding special types."""
        if type_code == 1:  # numpy array
            array_data = self._np_decoder.decode(data)
            return np.frombuffer(
                array_data.data,
                dtype=array_data.dtype
            ).reshape(array_data.shape)
        elif type_code == 2:  # pickled object
            return pickle.loads(data.tobytes())
        return data
    
    def serialize(self, obj: Any) -> bytes:
        """Serialize an object to bytes using msgpack."""
        try:
            return self._encoder.encode(obj)
        except Exception as e:
            raise ValueError(f"Failed to serialize object: {e}") from e
    
    def deserialize(self, data: bytes) -> Any:
        """Deserialize bytes to an object using msgpack."""
        try:
            return self._decoder.decode(data)
        except Exception as e:
            raise ValueError(f"Failed to deserialize data: {e}") from e 