"""FlaxKV2 gRPC client implementation."""
import logging
from typing import Any, Dict, Iterator, Optional, Union

import grpc

from flaxkv2.core.interfaces import Database
from flaxkv2.serve.proto import database_pb2, database_pb2_grpc

logger = logging.getLogger(__name__)

class RemoteDatabase(Database):
    """A remote database client that implements the Database interface."""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 50051,
        serializer: Optional[Any] = None,
        **kwargs
    ):
        """Initialize the remote database client.
        
        Args:
            host: The host address of the server
            port: The port number of the server
            serializer: The serializer to use for encoding/decoding values
            **kwargs: Additional arguments for the gRPC channel
        """
        self.address = f"{host}:{port}"
        self.channel = grpc.insecure_channel(self.address)
        self.stub = database_pb2_grpc.DatabaseServiceStub(self.channel)
        self.serializer = serializer
        
        # Try to connect to the server
        try:
            self.info()
            logger.info(f"Connected to remote database at {self.address}")
        except grpc.RpcError as e:
            logger.error(f"Failed to connect to remote database: {e}")
            self.close()
            raise ConnectionError(f"Could not connect to remote database at {self.address}")
    
    def __getitem__(self, key: Union[str, bytes]) -> Any:
        """Get a value by key."""
        if isinstance(key, str):
            key = key.encode()
            
        response = self.stub.Get(database_pb2.GetRequest(key=key))
        if not response.found:
            raise KeyError(key)
            
        if self.serializer:
            return self.serializer.deserialize(response.value)
        return response.value
    
    def __setitem__(self, key: Union[str, bytes], value: Any) -> None:
        """Set a key-value pair."""
        if isinstance(key, str):
            key = key.encode()
            
        if self.serializer:
            value = self.serializer.serialize(value)
        elif not isinstance(value, bytes):
            value = str(value).encode()
            
        response = self.stub.Set(database_pb2.SetRequest(key=key, value=value))
        if not response.success:
            raise RuntimeError("Failed to set value")
    
    def __delitem__(self, key: Union[str, bytes]) -> None:
        """Delete a key-value pair."""
        if isinstance(key, str):
            key = key.encode()
            
        response = self.stub.Delete(database_pb2.DeleteRequest(key=key))
        if not response.success:
            raise KeyError(key)
    
    def __contains__(self, key: Union[str, bytes]) -> bool:
        """Check if a key exists."""
        if isinstance(key, str):
            key = key.encode()
            
        response = self.stub.Contains(database_pb2.ContainsRequest(key=key))
        return response.exists
    
    def __len__(self) -> int:
        """Get the number of key-value pairs."""
        return self.info().size
    
    def __iter__(self) -> Iterator[bytes]:
        """Iterate over all keys."""
        response = self.stub.Keys(database_pb2.KeysRequest())
        yield from response.keys
    
    def get(self, key: Union[str, bytes], default: Any = None) -> Any:
        """Get a value by key with a default value."""
        try:
            return self[key]
        except KeyError:
            return default
    
    def keys(self, prefix: str = "") -> Iterator[bytes]:
        """Get all keys with an optional prefix filter."""
        response = self.stub.Keys(database_pb2.KeysRequest(prefix=prefix))
        yield from response.keys
    
    def info(self) -> Dict[str, Any]:
        """Get information about the remote database."""
        response = self.stub.Info(database_pb2.InfoRequest())
        return {
            "size": response.size,
            "db_path": response.db_path,
            "storage_backend": response.storage_backend,
            "serializer": response.serializer
        }
    
    def close(self) -> None:
        """Close the connection to the remote database."""
        if hasattr(self, 'channel'):
            self.channel.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        
    def clear(self) -> None:
        """Clear all key-value pairs."""
        # Not implemented in remote database
        raise NotImplementedError("Clear operation is not supported in remote database")
    
    def flush(self) -> None:
        """Flush changes to disk."""
        # Not implemented in remote database
        # Changes are automatically flushed on the server side
        pass
    
    def update(self, other: Dict[Union[str, bytes], Any]) -> None:
        """Update multiple key-value pairs."""
        for key, value in other.items():
            self[key] = value 