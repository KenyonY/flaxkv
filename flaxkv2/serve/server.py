"""FlaxKV2 gRPC server implementation."""
import concurrent.futures
import logging
import os
from typing import Optional

import grpc
from grpc_reflection.v1alpha import reflection

from flaxkv2 import Database, get_database
from flaxkv2.core.database import FlaxDatabase
from flaxkv2.serve.proto import database_pb2, database_pb2_grpc

logger = logging.getLogger(__name__)

class DatabaseServicer(database_pb2_grpc.DatabaseServiceServicer):
    """Implementation of the DatabaseService."""
    
    def __init__(self, db: Database, db_path: str):
        if not isinstance(db, FlaxDatabase):
            raise TypeError("Database must be an instance of FlaxDatabase")
        self.db = db
        self.db_path = db_path
    
    def Get(self, request, context):
        """Get value by key."""
        try:
            value = self.db.get(request.key)
            return database_pb2.GetResponse(
                value=value if value is not None else b"",
                found=value is not None
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.GetResponse()
    
    def Set(self, request, context):
        """Set key-value pair."""
        try:
            self.db[request.key] = request.value
            return database_pb2.SetResponse(success=True)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.SetResponse(success=False)
    
    def Delete(self, request, context):
        """Delete key."""
        try:
            del self.db[request.key]
            return database_pb2.DeleteResponse(success=True)
        except KeyError:
            return database_pb2.DeleteResponse(success=False)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DeleteResponse(success=False)
    
    def Contains(self, request, context):
        """Check if key exists."""
        try:
            exists = request.key in self.db
            return database_pb2.ContainsResponse(exists=exists)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.ContainsResponse(exists=False)
    
    def Keys(self, request, context):
        """Get all keys."""
        try:
            keys = [k for k in self.db.keys() if not request.prefix or k.startswith(request.prefix.encode())]
            return database_pb2.KeysResponse(keys=keys)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.KeysResponse()
    
    def Info(self, request, context):
        """Get database info."""
        try:
            return database_pb2.InfoResponse(
                size=len(self.db),
                db_path=str(self.db_path),
                storage_backend=self.db._storage_backend.__class__.__name__,
                serializer=self.db._serializer.__class__.__name__
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.InfoResponse()


def serve(
    db_name: str,
    host: str = "0.0.0.0",
    port: int = 50051,
    max_workers: Optional[int] = None,
    **db_kwargs
) -> None:
    """Start the gRPC server.
    
    Args:
        db_name: Name of the database to serve
        host: Host address to bind to
        port: Port to listen on
        max_workers: Maximum number of workers for the server
        **db_kwargs: Additional arguments passed to get_database
    """
    root_path = db_kwargs.get('root_path', os.path.expanduser('~/.flaxkv'))
    db_path = os.path.join(root_path, db_name)
    db = get_database(db_name, **db_kwargs)
    
    if not isinstance(db, FlaxDatabase):
        raise TypeError("Database must be an instance of FlaxDatabase")
    
    server = grpc.server(
        concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    )
    
    # Add the database service to the server
    database_pb2_grpc.add_DatabaseServiceServicer_to_server(
        DatabaseServicer(db, db_path), server
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