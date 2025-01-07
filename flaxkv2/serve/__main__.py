"""Command line entry point for the FlaxKV2 server."""
import argparse
import logging

from flaxkv2.serve.server import serve

def main():
    """Main entry point for the server."""
    parser = argparse.ArgumentParser(description="FlaxKV2 gRPC Server")
    parser.add_argument("db_name", help="Name of the database to serve")
    parser.add_argument("--host", default="0.0.0.0", help="Host address to bind to")
    parser.add_argument("--port", type=int, default=50051, help="Port to listen on")
    parser.add_argument("--workers", type=int, help="Number of worker threads")
    parser.add_argument("--root-path", help="Root path for database files")
    parser.add_argument("--log-level", default="INFO", 
                       choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                       help="Logging level")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Start server
    db_kwargs = {}
    if args.root_path:
        db_kwargs['root_path'] = args.root_path
        
    serve(
        db_name=args.db_name,
        host=args.host,
        port=args.port,
        max_workers=args.workers,
        **db_kwargs
    )

if __name__ == "__main__":
    main() 