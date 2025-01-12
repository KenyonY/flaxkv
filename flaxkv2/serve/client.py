"""FlaxKV2 gRPC client implementation."""
import logging
from typing import Any, Dict, List, Optional, Iterator, Tuple

import grpc
import ast

from flaxkv2.core.interfaces import Database
from flaxkv2.serialization.pandas_serializer import PandasSerializer
from flaxkv2.serve.proto import database_pb2
from flaxkv2.serve.proto import database_pb2_grpc

logger = logging.getLogger(__name__)

class RemoteDatabase(Database):
    """A remote database client that implements the Database interface."""
    
    def __init__(self, host: str = 'localhost', port: int = 50051, serializer: Optional[PandasSerializer] = None):
        """Initialize the remote database client.
        
        Args:
            host: The host address of the server
            port: The port number of the server
            serializer: The serializer to use for encoding/decoding values
        """
        self.channel = grpc.insecure_channel(f'{host}:{port}')
        self.stub = database_pb2_grpc.DatabaseServiceStub(self.channel)
        self.serializer = serializer or PandasSerializer()
        
        # Try to connect to the server
        try:
            self.info()
            logger.info(f"Connected to remote database at {host}:{port}")
        except grpc.RpcError as e:
            logger.error(f"Failed to connect to remote database: {e}")
            self.close()
            raise ConnectionError(f"Could not connect to remote database at {host}:{port}")
    
    def _encode_key(self, key: str) -> bytes:
        """Encode a key to bytes."""
        return key.encode('utf-8')
    
    def _decode_key(self, key: bytes) -> str:
        """Decode bytes to a key."""
        return key.decode('utf-8')
    
    def _encode_value(self, value: Any) -> bytes:
        """Encode a value to bytes."""
        return self.serializer.serialize(value)
    
    def _decode_value(self, value: bytes) -> Any:
        """Decode bytes to a value."""
        return self.serializer.deserialize(value)
    
    def __getitem__(self, key: str) -> Any:
        """Get a value by key."""
        try:
            response = self.stub.Get(database_pb2.GetRequest(key=self._encode_key(key)))
            if response.value:
                return self._decode_value(response.value)
            raise KeyError(key)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                raise KeyError(key)
            elif e.code() == grpc.StatusCode.UNAVAILABLE:
                raise ConnectionError("Failed to connect to the server")
            else:
                raise RuntimeError(f"gRPC error: {e.details()}")
    
    def __setitem__(self, key: str, value: Any) -> None:
        """Set a key-value pair."""
        try:
            encoded_value = self._encode_value(value)
            response = self.stub.Set(database_pb2.SetRequest(
                key=self._encode_key(key),
                value=encoded_value
            ))
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise ConnectionError("Failed to connect to the server")
            else:
                raise RuntimeError(f"gRPC error: {e.details()}")
    
    def __delitem__(self, key: str) -> None:
        """Delete a key-value pair."""
        try:
            response = self.stub.Delete(database_pb2.DeleteRequest(key=self._encode_key(key)))
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                raise KeyError(key)
            elif e.code() == grpc.StatusCode.UNAVAILABLE:
                raise ConnectionError("Failed to connect to the server")
            else:
                raise RuntimeError(f"gRPC error: {e.details()}")
    
    def __contains__(self, key: str) -> bool:
        """Check if a key exists."""
        try:
            response = self.stub.Contains(database_pb2.ContainsRequest(key=self._encode_key(key)))
            return response.exists
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise ConnectionError("Failed to connect to the server")
            else:
                raise RuntimeError(f"gRPC error: {e.details()}")
    
    def __len__(self) -> int:
        """Get the number of key-value pairs."""
        try:
            response = self.stub.Info(database_pb2.InfoRequest())
            info = ast.literal_eval(response.info)
            return info['size']
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise ConnectionError("Failed to connect to the server")
            else:
                raise RuntimeError(f"gRPC error: {e.details()}")
    
    def __iter__(self) -> Iterator[Tuple[str, Any]]:
        """Iterate over all key-value pairs."""
        try:
            keys = self.keys()
            for key in keys:
                try:
                    value = self[key]
                    yield key, value
                except KeyError:
                    continue  # Skip keys that were deleted
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise ConnectionError("Failed to connect to the server")
            else:
                raise RuntimeError(f"gRPC error: {e.details()}")
    
    def keys(self) -> List[str]:
        """Get all keys with an optional prefix filter."""
        try:
            response = self.stub.Keys(database_pb2.KeysRequest())
            return [self._decode_key(key) for key in response.keys]
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise ConnectionError("Failed to connect to the server")
            else:
                raise RuntimeError(f"gRPC error: {e.details()}")
    
    def info(self) -> Dict[str, Any]:
        """Get information about the remote database."""
        try:
            response = self.stub.Info(database_pb2.InfoRequest())
            return ast.literal_eval(response.info)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise ConnectionError("Failed to connect to the server")
            else:
                raise RuntimeError(f"gRPC error: {e.details()}")
    
    def close(self) -> None:
        """Close the connection to the remote database."""
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
    
    def update(self, other: Dict[str, Any]) -> None:
        """Update multiple key-value pairs."""
        for key, value in other.items():
            self[key] = value
            
    def get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        """Get a value by key with a default value."""
        try:
            return self[key]
        except KeyError:
            return default 