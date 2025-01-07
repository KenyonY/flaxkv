"""Core interfaces for FlaxKV."""
from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, Iterator, Optional, TypeVar

KT = TypeVar('KT')  # Key type
VT = TypeVar('VT')  # Value type

class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    def get(self, key: bytes) -> Optional[bytes]:
        """Retrieve a value by key."""
        pass
    
    @abstractmethod
    def put(self, key: bytes, value: bytes) -> None:
        """Store a key-value pair."""
        pass
    
    @abstractmethod
    def delete(self, key: bytes) -> None:
        """Delete a key-value pair."""
        pass
    
    @abstractmethod
    def iterator(self) -> Iterator[tuple[bytes, bytes]]:
        """Return an iterator over all key-value pairs."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the storage backend."""
        pass

class Serializer(ABC):
    """Abstract base class for serializers."""
    
    @abstractmethod
    def serialize(self, obj: Any) -> bytes:
        """Serialize an object to bytes."""
        pass
    
    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """Deserialize bytes to an object."""
        pass

class KeyEncoder(ABC):
    """Abstract base class for key encoders."""
    
    @abstractmethod
    def encode(self, key: Any) -> bytes:
        """Encode a key to bytes."""
        pass
    
    @abstractmethod
    def decode(self, key_bytes: bytes) -> Any:
        """Decode bytes to a key."""
        pass

class DatabaseConfig:
    """Configuration for the database."""
    
    def __init__(
        self,
        path: str,
        backend: str = "leveldb",
        create_if_missing: bool = True,
        buffer_size: int = 1000,
        auto_flush: bool = True,
        flush_interval: float = 1.0,
        serializer: Optional[Serializer] = None,
        key_encoder: Optional[KeyEncoder] = None,
    ):
        self.path = path
        self.backend = backend
        self.create_if_missing = create_if_missing
        self.buffer_size = buffer_size
        self.auto_flush = auto_flush
        self.flush_interval = flush_interval
        self.serializer = serializer
        self.key_encoder = key_encoder

class DatabaseManager(ABC):
    """Abstract base class for database managers."""
    
    @abstractmethod
    def create_database(self, name: str, config: DatabaseConfig) -> 'Database[Any, Any]':
        """Create a new database instance."""
        pass
    
    @abstractmethod
    def get_database(self, name: str) -> Optional['Database[Any, Any]']:
        """Get an existing database instance."""
        pass
    
    @abstractmethod
    def list_databases(self) -> list[str]:
        """List all available databases."""
        pass
    
    @abstractmethod
    def delete_database(self, name: str) -> None:
        """Delete a database."""
        pass

class Database(Generic[KT, VT], ABC):
    """Abstract base class for the main database interface."""
    
    @abstractmethod
    def __getitem__(self, key: KT) -> VT:
        """Get a value by key."""
        pass
    
    @abstractmethod
    def __setitem__(self, key: KT, value: VT) -> None:
        """Set a value by key."""
        pass
    
    @abstractmethod
    def __delitem__(self, key: KT) -> None:
        """Delete a key-value pair."""
        pass
    
    @abstractmethod
    def __iter__(self) -> Iterator[tuple[KT, VT]]:
        """Iterate over all key-value pairs."""
        pass
    
    @abstractmethod
    def __len__(self) -> int:
        """Return the number of key-value pairs."""
        pass
    
    @abstractmethod
    def get(self, key: KT, default: Optional[VT] = None) -> Optional[VT]:
        """Get a value by key with a default value."""
        pass
    
    @abstractmethod
    def update(self, other: Dict[KT, VT]) -> None:
        """Update multiple key-value pairs."""
        pass
    
    @abstractmethod
    def clear(self) -> None:
        """Clear all key-value pairs."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the database."""
        pass
    
    @abstractmethod
    def flush(self) -> None:
        """Flush buffered writes to storage."""
        pass
    
    def __enter__(self) -> 'Database[KT, VT]':
        """Enter the context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context manager."""
        self.close()
    
    def __del__(self) -> None:
        """Destructor to ensure resources are released."""
        try:
            self.close()
        except:
            pass  # Ignore errors during garbage collection 