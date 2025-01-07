"""Tests for remote database functionality."""
import multiprocessing
import os
import shutil
import tempfile
import time
from typing import Generator

import numpy as np
import pandas as pd
import pytest

from flaxkv2 import create_database, get_database
from flaxkv2.core.database import FlaxDatabase
from flaxkv2.serialization.pandas_serializer import PandasSerializer
from flaxkv2.serve.client import RemoteDatabase
from flaxkv2.serve.server import serve


@pytest.fixture
def db_root() -> Generator[str, None, None]:
    """Temporary directory for database files."""
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path)


@pytest.fixture
def server_process(db_root) -> Generator[multiprocessing.Process, None, None]:
    """Start a server process for testing."""
    # Create a test database
    db = create_database("test", root_path=db_root, serializer=PandasSerializer())
    assert isinstance(db, FlaxDatabase)  # Ensure we have the correct database type
    db.close()
    
    # Start server in a separate process
    process = multiprocessing.Process(
        target=serve,
        args=("test",),
        kwargs={
            "host": "localhost",
            "port": 50051,
            "root_path": db_root,
            "serializer": PandasSerializer()
        }
    )
    process.start()
    time.sleep(1)  # Give the server time to start
    
    yield process
    
    process.terminate()
    process.join()


def test_basic_operations(server_process):
    """Test basic remote database operations."""
    db = RemoteDatabase(serializer=PandasSerializer())
    
    # Test string values
    db["test_str"] = "hello"
    assert db["test_str"] == "hello"
    
    # Test numeric values
    db["test_int"] = 42
    assert db["test_int"] == 42
    
    # Test deletion
    del db["test_str"]
    assert "test_str" not in db
    
    # Test contains
    assert "test_int" in db
    
    # Test get with default
    assert db.get("nonexistent", "default") == "default"
    
    db.close()


def test_pandas_operations(server_process):
    """Test pandas DataFrame and Series operations."""
    db = RemoteDatabase(serializer=PandasSerializer())
    
    # Test DataFrame
    df = pd.DataFrame({
        'int_col': [1, 2, 3],
        'float_col': [1.1, 2.2, 3.3],
        'str_col': ['a', 'b', 'c']
    })
    db["test_df"] = df
    retrieved_df = db["test_df"]
    pd.testing.assert_frame_equal(df, retrieved_df)
    
    # Test Series
    series = pd.Series([1, 2, 3], name="test_series")
    db["test_series"] = series
    retrieved_series = db["test_series"]
    pd.testing.assert_series_equal(series, retrieved_series)
    
    db.close()


def test_numpy_operations(server_process):
    """Test numpy array operations."""
    db = RemoteDatabase(serializer=PandasSerializer())
    
    # Test numpy array
    arr = np.array([[1, 2, 3], [4, 5, 6]])
    db["test_array"] = arr
    retrieved_arr = db["test_array"]
    np.testing.assert_array_equal(arr, retrieved_arr)
    
    db.close()


def test_large_data(server_process):
    """Test operations with large data."""
    db = RemoteDatabase(serializer=PandasSerializer())
    
    # Create large DataFrame (100k rows)
    n_rows = 100_000
    df = pd.DataFrame({
        'int_col': range(n_rows),
        'float_col': np.random.randn(n_rows),
        'str_col': [f'str_{i}' for i in range(n_rows)]
    })
    
    # Test storage and retrieval
    db["large_df"] = df
    retrieved_df = db["large_df"]
    pd.testing.assert_frame_equal(df, retrieved_df)
    
    db.close()


def test_error_handling(server_process):
    """Test error handling in remote operations."""
    db = RemoteDatabase(serializer=PandasSerializer())
    
    # Test KeyError
    with pytest.raises(KeyError):
        _ = db["nonexistent"]
    
    # Test connection error
    server_process.terminate()
    time.sleep(1)
    
    with pytest.raises(ConnectionError):
        db["test"] = "value"
    
    db.close() 