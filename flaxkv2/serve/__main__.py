"""Command line entry point for the FlaxKV2 server."""
import argparse
import logging
import os
from pathlib import Path

from flaxkv2.core.database import FlaxDatabase
from flaxkv2.serialization.pandas_serializer import PandasSerializer
from flaxkv2.serve.server import serve

def main():
    """Main entry point for the FlaxKV2 server."""
    parser = argparse.ArgumentParser(description='Start the FlaxKV2 server')
    parser.add_argument('db_path', type=str, help='Path to the database file')
    parser.add_argument('--host', type=str, default='localhost',
                      help='Host to bind to (default: localhost)')
    parser.add_argument('--port', type=int, default=50051,
                      help='Port to listen on (default: 50051)')
    parser.add_argument('--workers', type=int, default=None,
                      help='Number of worker threads (default: CPU count * 2)')
    parser.add_argument('--log-level', type=str, default='INFO',
                      choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                      help='Logging level (default: INFO)')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create database directory if it doesn't exist
    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Initialize database
    db = FlaxDatabase(str(db_path), serializer=PandasSerializer())
    
    # Start server
    serve(db, host=args.host, port=args.port, max_workers=args.workers)

if __name__ == '__main__':
    main() 