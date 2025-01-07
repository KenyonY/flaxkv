"""Main database implementation."""
import atexit
import threading
import time
from collections import defaultdict
from queue import Queue
from threading import Event, Lock, Thread
from typing import Any, Dict, Iterator, Optional, TypeVar

from ..core.interfaces import Database, DatabaseConfig, KeyEncoder, Serializer, StorageBackend
from ..serialization.key_encoder import MsgPackKeyEncoder
from ..serialization.msgpack_serializer import MsgPackSerializer
from ..storage.factory import StorageBackendFactory

KT = TypeVar('KT')  # Key type
VT = TypeVar('VT')  # Value type

class FlaxDatabase(Database[KT, VT]):
    """Main database implementation with buffered writes and background flushing."""
    
    _instances = set()  # Keep track of all database instances
    _instances_lock = Lock()  # Lock for thread-safe instance tracking
    
    @classmethod
    def _cleanup_all(cls) -> None:
        """Close all database instances."""
        with cls._instances_lock:
            for db in cls._instances:
                try:
                    if not db._closed:  # Check if database is already closed
                        db.close()
                except:
                    pass  # Ignore errors during cleanup
            cls._instances.clear()
    
    def __init__(
        self,
        config: DatabaseConfig,
        storage_backend: Optional[StorageBackend] = None,
        serializer: Optional[Serializer] = None,
        key_encoder: Optional[KeyEncoder] = None,
    ):
        """Initialize the database.
        
        Args:
            config: Database configuration
            storage_backend: Optional storage backend instance
            serializer: Optional serializer instance
            key_encoder: Optional key encoder instance
        """
        self._config = config
        self._storage = storage_backend or StorageBackendFactory.create_backend(
            config.backend,
            path=config.path,
            create_if_missing=config.create_if_missing,
        )
        self._serializer = serializer or MsgPackSerializer()
        self._key_encoder = key_encoder or MsgPackKeyEncoder()
        
        # Write buffer
        self._buffer: Dict[bytes, Optional[bytes]] = {}
        self._buffer_lock = Lock()
        self._buffer_size = 0
        
        # Background flush thread
        self._flush_queue: Queue[Dict[bytes, Optional[bytes]]] = Queue()
        self._stop_event = Event()
        if config.auto_flush:
            self._flush_thread = Thread(target=self._background_flush, daemon=True)
            self._flush_thread.start()
        
        # Track instance and register cleanup
        self._closed = False
        with self._instances_lock:
            self._instances.add(self)
            if len(self._instances) == 1:  # Register cleanup only once
                atexit.register(self._cleanup_all)
    
    def _background_flush(self) -> None:
        """Background thread for flushing writes to storage."""
        while not self._stop_event.is_set():
            try:
                # Wait for the flush interval or until the buffer is full
                self._stop_event.wait(self._config.flush_interval)
                if self._buffer_size > 0:
                    self.flush()
            except Exception as e:
                # Log error but continue running
                print(f"Error in background flush: {e}")
    
    def _encode_key(self, key: KT) -> bytes:
        """Encode a key to bytes."""
        return self._key_encoder.encode(key)
    
    def _decode_key(self, key_bytes: bytes) -> KT:
        """Decode bytes to a key."""
        return self._key_encoder.decode(key_bytes)
    
    def _encode_value(self, value: VT) -> bytes:
        """Encode a value to bytes."""
        return self._serializer.serialize(value)
    
    def _decode_value(self, value_bytes: bytes) -> VT:
        """Decode bytes to a value."""
        return self._serializer.deserialize(value_bytes)
    
    def __getitem__(self, key: KT) -> VT:
        """Get a value by key."""
        encoded_key = self._encode_key(key)
        
        # Check write buffer first
        with self._buffer_lock:
            if encoded_key in self._buffer:
                value_bytes = self._buffer[encoded_key]
                if value_bytes is None:
                    raise KeyError(key)
                return self._decode_value(value_bytes)
        
        # Check storage
        value_bytes = self._storage.get(encoded_key)
        if value_bytes is None:
            raise KeyError(key)
        return self._decode_value(value_bytes)
    
    def __setitem__(self, key: KT, value: VT) -> None:
        """Set a value by key."""
        encoded_key = self._encode_key(key)
        encoded_value = self._encode_value(value)
        
        with self._buffer_lock:
            self._buffer[encoded_key] = encoded_value
            self._buffer_size += 1
            
            if self._buffer_size >= self._config.buffer_size:
                self.flush()
    
    def __delitem__(self, key: KT) -> None:
        """Delete a key-value pair."""
        encoded_key = self._encode_key(key)
        
        with self._buffer_lock:
            # Mark as deleted in buffer
            self._buffer[encoded_key] = None
            self._buffer_size += 1
            
            if self._buffer_size >= self._config.buffer_size:
                self.flush()
    
    def __iter__(self) -> Iterator[tuple[KT, VT]]:
        """Iterate over all key-value pairs."""
        # First, get all items from storage
        storage_items = {
            key: value for key, value in self._storage.iterator()
        }
        
        # Apply buffered changes
        with self._buffer_lock:
            for key, value in self._buffer.items():
                if value is None:
                    storage_items.pop(key, None)
                else:
                    storage_items[key] = value
        
        # Yield decoded items
        for key_bytes, value_bytes in storage_items.items():
            yield self._decode_key(key_bytes), self._decode_value(value_bytes)
    
    def __len__(self) -> int:
        """Return the number of key-value pairs."""
        count = 0
        for _ in self:
            count += 1
        return count
    
    def get(self, key: KT, default: Optional[VT] = None) -> Optional[VT]:
        """Get a value by key with a default value."""
        try:
            return self[key]
        except KeyError:
            return default
    
    def update(self, other: Dict[KT, VT]) -> None:
        """Update multiple key-value pairs."""
        for key, value in other.items():
            self[key] = value
    
    def clear(self) -> None:
        """Clear all key-value pairs."""
        # Clear buffer
        with self._buffer_lock:
            self._buffer.clear()
            self._buffer_size = 0
        
        # Clear storage by deleting all keys
        for key, _ in self._storage.iterator():
            self._storage.delete(key)
    
    def close(self) -> None:
        """Close the database."""
        if self._closed:
            return
        
        # Stop background flush thread
        if hasattr(self, '_flush_thread'):
            self._stop_event.set()
            self._flush_thread.join()
        
        # Final flush with buffer lock
        with self._buffer_lock:
            # Apply buffered changes to storage
            for key, value in self._buffer.items():
                if value is None:
                    self._storage.delete(key)
                else:
                    self._storage.put(key, value)
            
            # Clear buffer
            self._buffer.clear()
            self._buffer_size = 0
        
        # Close storage
        self._storage.close()
        self._closed = True
        
        # Remove from instances set
        with self._instances_lock:
            self._instances.discard(self)
    
    def flush(self) -> None:
        """Flush buffered writes to storage."""
        with self._buffer_lock:
            # Apply buffered changes to storage
            for key, value in self._buffer.items():
                if value is None:
                    self._storage.delete(key)
                else:
                    self._storage.put(key, value)
            
            # Clear buffer
            self._buffer.clear()
            self._buffer_size = 0 