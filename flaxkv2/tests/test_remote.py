"""Tests for remote database functionality."""
import multiprocessing
import os
import shutil
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from flaxkv2.core.database import FlaxDatabase
from flaxkv2.core.interfaces import DatabaseConfig
from flaxkv2.serialization.pandas_serializer import PandasSerializer
from flaxkv2.serve.client import RemoteDatabase
from flaxkv2.serve.server import serve


@pytest.fixture
def db_root():
    """Create a temporary directory for database files."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def server_process(db_root):
    """Start a server process for testing."""
    db_path = os.path.join(db_root, "test.db")
    config = DatabaseConfig(
        path=db_path,
        backend="leveldb",
        create_if_missing=True,
        auto_flush=True
    )
    db = FlaxDatabase(config, serializer=PandasSerializer())
    
    # Start server in a separate process
    process = multiprocessing.Process(
        target=serve,
        args=(db,),
        kwargs={'host': 'localhost', 'port': 50051}
    )
    process.start()
    time.sleep(1)  # Wait for server to start
    
    yield process
    
    # Cleanup
    process.terminate()
    process.join()


def test_basic_operations(server_process):
    """Test basic database operations."""
    db = RemoteDatabase(serializer=PandasSerializer())
    
    # Test setting and getting values
    db["test_int"] = 42
    assert "test_int" in db
    assert db["test_int"] == 42
    
    db["test_str"] = "hello"
    assert db["test_str"] == "hello"
    
    # Test deleting values
    del db["test_int"]
    assert "test_int" not in db
    
    # Test getting all keys
    assert set(db.keys()) == {"test_str"}


def test_pandas_operations(server_process):
    """Test pandas DataFrame and Series operations."""
    db = RemoteDatabase(serializer=PandasSerializer())
    
    # Test DataFrame
    df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': ['a', 'b', 'c']
    })
    db["test_df"] = df
    retrieved_df = db["test_df"]
    pd.testing.assert_frame_equal(df, retrieved_df)
    
    # Test Series
    series = pd.Series([1, 2, 3], name='test')
    db["test_series"] = series
    retrieved_series = db["test_series"]
    pd.testing.assert_series_equal(series, retrieved_series)


def test_numpy_operations(server_process):
    """Test numpy array operations."""
    db = RemoteDatabase(serializer=PandasSerializer())
    
    # Test numpy array
    arr = np.array([1, 2, 3])
    db["test_array"] = arr
    retrieved_arr = db["test_array"]
    np.testing.assert_array_equal(arr, retrieved_arr)
    
    # Test 2D array
    arr_2d = np.array([[1, 2], [3, 4]])
    db["test_array_2d"] = arr_2d
    retrieved_arr_2d = db["test_array_2d"]
    np.testing.assert_array_equal(arr_2d, retrieved_arr_2d)


def test_large_data(server_process):
    """Test operations with large DataFrames."""
    db = RemoteDatabase(serializer=PandasSerializer())
    
    # Create a large DataFrame (100k rows)
    n_rows = 100000
    df = pd.DataFrame({
        'int_col': range(n_rows),
        'float_col': np.random.randn(n_rows),
        'str_col': [f'str_{i}' for i in range(n_rows)]
    })
    
    # Test storage and retrieval
    db["large_df"] = df
    retrieved_df = db["large_df"]
    pd.testing.assert_frame_equal(df, retrieved_df)


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