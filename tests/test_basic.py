"""Basic tests for FlaxKV."""
import os
import shutil
import tempfile
from pathlib import Path

import numpy as np
import pytest

from flaxkv2 import create_database, get_database

@pytest.fixture
def db_root():
    """Temporary directory for database files."""
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path)

def test_basic_operations(db_root):
    """Test basic database operations."""
    # Using context manager
    with create_database("test", root_path=db_root) as db:
        # Test string values
        db["key1"] = "value1"
        assert db["key1"] == "value1"
        
        # Test numeric values
        db[1] = 42
        assert db[1] == 42
        db[3.14] = 2.718
        assert db[3.14] == 2.718
        
        # Test dictionary values
        db["dict"] = {"a": 1, "b": [1, 2, 3]}
        assert db["dict"] == {"a": 1, "b": [1, 2, 3]}
        
        # Test list values
        db["list"] = [1, 2, {"x": "y"}]
        assert db["list"] == [1, 2, {"x": "y"}]
        
        # Test numpy array values
        array = np.random.randn(10, 10)
        db["array"] = array
        np.testing.assert_array_equal(db["array"], array)
        
        # Test deletion
        del db["key1"]
        with pytest.raises(KeyError):
            _ = db["key1"]
        
        # Test update
        db.update({"key2": "value2", "key3": "value3"})
        assert db["key2"] == "value2"
        assert db["key3"] == "value3"
        
        # Test iteration
        items = dict(db)
        assert len(items) == 7  # 1, 3.14, dict, list, array, key2, key3
        
        # Test clear
        db.clear()
        assert len(db) == 0

def test_persistence(db_root):
    """Test database persistence."""
    # Create and populate database
    db1 = create_database("test", root_path=db_root)
    db1["key1"] = "value1"
    db1["key2"] = [1, 2, 3]
    array = np.random.randn(5, 5)
    db1["array"] = array
    db1.close()
    
    # Reopen and verify
    with get_database("test", root_path=db_root) as db2:
        assert db2["key1"] == "value1"
        assert db2["key2"] == [1, 2, 3]
        np.testing.assert_array_equal(db2["array"], array)

def test_multiple_databases(db_root):
    """Test multiple database instances."""
    # Using context managers
    with create_database("db1", root_path=db_root) as db1, \
         create_database("db2", root_path=db_root) as db2:
        
        db1["key"] = "value1"
        db2["key"] = "value2"
        
        assert db1["key"] == "value1"
        assert db2["key"] == "value2"
    
    # Verify databases are independent
    assert set(os.listdir(db_root)) == {"db1", "db2"}

def test_auto_resource_management(db_root):
    """Test automatic resource management."""
    # Test context manager
    with create_database("test1", root_path=db_root) as db:
        db["key"] = "value"
    # Database should be closed after the with block
    
    # Test garbage collection
    db = create_database("test2", root_path=db_root)
    db["key"] = "value"
    db = None  # Should trigger __del__
    
    # Both databases should be accessible
    with get_database("test1", root_path=db_root) as db1, \
         get_database("test2", root_path=db_root) as db2:
        assert db1["key"] == "value"
        assert db2["key"] == "value" 