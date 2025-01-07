"""Key encoder implementation."""
from typing import Any

import msgpack

from ..core.interfaces import KeyEncoder

class MsgPackKeyEncoder(KeyEncoder):
    """MsgPack-based key encoder."""
    
    def encode(self, key: Any) -> bytes:
        """Encode a key to bytes using msgpack."""
        try:
            return msgpack.packb(key, use_bin_type=True)
        except Exception as e:
            raise ValueError(f"Failed to encode key: {e}") from e
    
    def decode(self, key_bytes: bytes) -> Any:
        """Decode bytes to a key using msgpack."""
        try:
            return msgpack.unpackb(key_bytes, use_list=False, raw=False)
        except Exception as e:
            raise ValueError(f"Failed to decode key: {e}") from e 