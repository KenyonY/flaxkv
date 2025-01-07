"""LevelDB storage backend implementation."""
import os
from typing import Iterator, Optional

import plyvel

from ..core.interfaces import StorageBackend

class LevelDBBackend(StorageBackend):
    """LevelDB storage backend implementation."""
    
    def __init__(self, path: str, create_if_missing: bool = True):
        """Initialize the LevelDB backend.
        
        Args:
            path: Path to the database directory
            create_if_missing: Create the database if it doesn't exist
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        try:
            self._db = plyvel.DB(
                path,
                create_if_missing=create_if_missing,
                write_buffer_size=4 * 1024 * 1024,  # 4MB write buffer
                block_size=4 * 1024,  # 4KB block size
                compression='snappy',  # Use Snappy compression
            )
        except Exception as e:
            raise RuntimeError(f"Failed to open LevelDB at {path}: {e}") from e
    
    def get(self, key: bytes) -> Optional[bytes]:
        """Retrieve a value by key."""
        try:
            return self._db.get(key)
        except Exception as e:
            raise RuntimeError(f"Failed to get value: {e}") from e
    
    def put(self, key: bytes, value: bytes) -> None:
        """Store a key-value pair."""
        try:
            self._db.put(key, value)
        except Exception as e:
            raise RuntimeError(f"Failed to put value: {e}") from e
    
    def delete(self, key: bytes) -> None:
        """Delete a key-value pair."""
        try:
            self._db.delete(key)
        except Exception as e:
            raise RuntimeError(f"Failed to delete key: {e}") from e
    
    def iterator(self) -> Iterator[tuple[bytes, bytes]]:
        """Return an iterator over all key-value pairs."""
        try:
            return self._db.iterator()
        except Exception as e:
            raise RuntimeError(f"Failed to create iterator: {e}") from e
    
    def close(self) -> None:
        """Close the database."""
        try:
            self._db.close()
        except Exception as e:
            raise RuntimeError(f"Failed to close database: {e}") from e 