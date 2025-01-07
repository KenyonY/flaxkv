"""Storage backend factory."""
from typing import Dict, Type

from ..core.interfaces import StorageBackend
from .leveldb import LevelDBBackend

class StorageBackendFactory:
    """Factory for creating storage backends."""
    
    _backends: Dict[str, Type[StorageBackend]] = {
        'leveldb': LevelDBBackend,
    }
    
    @classmethod
    def register_backend(cls, name: str, backend_class: Type[StorageBackend]) -> None:
        """Register a new storage backend.
        
        Args:
            name: Name of the backend
            backend_class: Backend class to register
        """
        if not issubclass(backend_class, StorageBackend):
            raise TypeError(f"Backend class must implement StorageBackend interface")
        cls._backends[name] = backend_class
    
    @classmethod
    def create_backend(cls, backend_type: str, **kwargs) -> StorageBackend:
        """Create a storage backend instance.
        
        Args:
            backend_type: Type of backend to create
            **kwargs: Additional arguments to pass to the backend constructor
        
        Returns:
            StorageBackend instance
        
        Raises:
            ValueError: If the backend type is not registered
        """
        if backend_type not in cls._backends:
            raise ValueError(
                f"Unknown backend type: {backend_type}. "
                f"Available backends: {list(cls._backends.keys())}"
            )
        
        try:
            return cls._backends[backend_type](**kwargs)
        except Exception as e:
            raise RuntimeError(f"Failed to create {backend_type} backend: {e}") from e 