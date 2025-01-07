"""FlaxKV - A high-performance dictionary database."""

from .core.database import FlaxDatabase
from .core.interfaces import Database, DatabaseConfig
from .core.manager import FlaxDatabaseManager

__version__ = "0.1.0"

def create_database(name: str, root_path: str = ".flaxkv", **kwargs) -> Database:
    """Create a new database instance.
    
    Args:
        name: Database name
        root_path: Root path for all databases
        **kwargs: Additional configuration options
    
    Returns:
        Database instance
    """
    manager = FlaxDatabaseManager(root_path)
    config = DatabaseConfig(
        path="",  # Will be set by manager
        **kwargs
    )
    return manager.create_database(name, config)

def get_database(name: str, root_path: str = ".flaxkv", **kwargs) -> Database:
    """Get an existing database instance or create a new one.
    
    Args:
        name: Database name
        root_path: Root path for all databases
        **kwargs: Additional configuration options
    
    Returns:
        Database instance
    """
    manager = FlaxDatabaseManager(root_path)
    db = manager.get_database(name)
    if db is None:
        config = DatabaseConfig(
            path="",  # Will be set by manager
            **kwargs
        )
        db = manager.create_database(name, config)
    return db

__all__ = [
    "Database",
    "DatabaseConfig",
    "FlaxDatabase",
    "FlaxDatabaseManager",
    "create_database",
    "get_database",
] 