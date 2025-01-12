"""FlaxKV2 gRPC server implementation."""
import concurrent.futures
import logging
from typing import Any, Dict, List, Optional

import grpc
from grpc_reflection.v1alpha import reflection

from flaxkv2.core.database import FlaxDatabase
from flaxkv2.serialization.pandas_serializer import PandasSerializer
from flaxkv2.serve.proto import database_pb2
from flaxkv2.serve.proto import database_pb2_grpc

logger = logging.getLogger(__name__)

class DatabaseServicer(database_pb2_grpc.DatabaseServiceServicer):
    """Implementation of the DatabaseService."""
    
    def __init__(self, db: FlaxDatabase):
        self.db = db
        self.serializer = PandasSerializer()
    
    def _encode_key(self, key: str) -> bytes:
        """Encode a key to bytes."""
        return key.encode('utf-8')
    
    def _decode_key(self, key: bytes) -> str:
        """Decode a key from bytes."""
        return key.decode('utf-8')
    
    def _encode_value(self, value: Any) -> bytes:
        """Encode a value to bytes."""
        return self.serializer.serialize(value)
    
    def _decode_value(self, value: bytes) -> Any:
        """Decode a value from bytes."""
        return self.serializer.deserialize(value)
    
    def Get(self, request, context):
        """Get value by key."""
        try:
            key = self._decode_key(request.key)
            value = self.db[key]
            encoded_value = self._encode_value(value)
            return database_pb2.GetResponse(value=encoded_value)
        except KeyError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Key '{key}' not found")
            return database_pb2.GetResponse()
    
    def Set(self, request, context):
        """Set key-value pair."""
        try:
            key = self._decode_key(request.key)
            value = self._decode_value(request.value)
            self.db[key] = value
            return database_pb2.SetResponse()
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.SetResponse()
    
    def Delete(self, request, context):
        """Delete key."""
        try:
            key = self._decode_key(request.key)
            del self.db[key]
            return database_pb2.DeleteResponse()
        except KeyError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Key '{key}' not found")
            return database_pb2.DeleteResponse()
    
    def Contains(self, request, context):
        """Check if key exists."""
        try:
            key = self._decode_key(request.key)
            exists = key in self.db
            return database_pb2.ContainsResponse(exists=exists)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.ContainsResponse()
    
    def Keys(self, request, context):
        """Get all keys."""
        try:
            keys = [self._encode_key(key) for key in self.db.keys()]
            return database_pb2.KeysResponse(keys=keys)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.KeysResponse()
    
    def Info(self, request, context):
        """Get database info."""
        try:
            info = {
                'db_path': str(self.db._storage.path),
                'storage_backend': self.db._storage.__class__.__name__,
                'size': len(self.db)
            }
            return database_pb2.InfoResponse(info=str(info))
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.InfoResponse()

def serve(
    db: FlaxDatabase,
    host: str = "localhost",
    port: int = 50051,
    max_workers: Optional[int] = None
) -> None:
    """Start the gRPC server.
    
    Args:
        db: The database instance to serve
        host: Host address to bind to
        port: Port to listen on
        max_workers: Maximum number of workers for the server
    """
    server = grpc.server(
        concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    )
    
    # Add the database service to the server
    database_pb2_grpc.add_DatabaseServiceServicer_to_server(
        DatabaseServicer(db), server
    )
    
    # Enable reflection
    SERVICE_NAMES = (
        database_pb2.DESCRIPTOR.services_by_name['DatabaseService'].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)
    
    # Start the server
    address = f"{host}:{port}"
    server.add_insecure_port(address)
    server.start()
    
    logger.info(f"Server started on {address}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
        server.stop(0)
        db.close()
        logger.info("Server stopped") 