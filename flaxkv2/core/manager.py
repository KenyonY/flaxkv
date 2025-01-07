"""Database manager implementation."""
import os
import shutil
from pathlib import Path
from typing import Dict, Optional

from .database import FlaxDatabase
from .interfaces import Database, DatabaseConfig, DatabaseManager

class FlaxDatabaseManager(DatabaseManager):
    """Database manager implementation."""
    
    def __init__(self, root_path: str):
        """Initialize the database manager.
        
        Args:
            root_path: Root path for all databases
        """
        self._root_path = Path(root_path)
        self._root_path.mkdir(parents=True, exist_ok=True)
        self._databases: Dict[str, FlaxDatabase] = {}
    
    def _get_db_path(self, name: str) -> Path:
        """Get the path for a database.
        
        Args:
            name: Database name
        
        Returns:
            Path to the database
        """
        return self._root_path / name
    
    def create_database(self, name: str, config: DatabaseConfig) -> Database:
        """Create a new database instance.
        
        Args:
            name: Database name
            config: Database configuration
        
        Returns:
            Database instance
        
        Raises:
            ValueError: If a database with the same name already exists
        """
        if name in self._databases:
            raise ValueError(f"Database {name} already exists")
        
        db_path = self._get_db_path(name)
        if db_path.exists():
            raise ValueError(f"Database directory {db_path} already exists")
        
        # Update config with database path
        config.path = str(db_path)
        
        # Create database instance
        db = FlaxDatabase(config)
        self._databases[name] = db
        return db
    
    def get_database(self, name: str) -> Optional[Database]:
        """Get an existing database instance.
        
        Args:
            name: Database name
        
        Returns:
            Database instance or None if not found
        """
        # Return cached instance if available
        if name in self._databases:
            return self._databases[name]
        
        # Check if database directory exists
        db_path = self._get_db_path(name)
        if not db_path.exists():
            return None
        
        # Create and cache database instance
        config = DatabaseConfig(path=str(db_path))
        db = FlaxDatabase(config)
        self._databases[name] = db
        return db
    
    def list_databases(self) -> list[str]:
        """List all available databases.
        
        Returns:
            List of database names
        """
        return [
            path.name for path in self._root_path.iterdir()
            if path.is_dir() and not path.name.startswith('.')
        ]
    
    def delete_database(self, name: str) -> None:
        """Delete a database.
        
        Args:
            name: Database name
        
        Raises:
            ValueError: If the database doesn't exist
        """
        db_path = self._get_db_path(name)
        if not db_path.exists():
            raise ValueError(f"Database {name} does not exist")
        
        # Close database if it's open
        if name in self._databases:
            self._databases[name].close()
            del self._databases[name]
        
        # Delete database directory
        try:
            shutil.rmtree(db_path)
        except Exception as e:
            raise RuntimeError(f"Failed to delete database {name}: {e}") from e 