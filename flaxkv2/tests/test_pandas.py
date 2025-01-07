"""Tests for pandas support."""
import os
import shutil
import tempfile

import numpy as np
import pandas as pd
import pytest

from flaxkv2 import create_database, get_database
from flaxkv2.serialization.pandas_serializer import PandasSerializer


@pytest.fixture
def db_root():
    """Temporary directory for database files."""
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path)


def test_dataframe_operations(db_root):
    """Test DataFrame storage and retrieval."""
    # Create database with pandas serializer
    db = create_database("test", root_path=db_root, serializer=PandasSerializer())
    
    # Create test DataFrame
    df = pd.DataFrame({
        'int_col': [1, 2, 3, 4, 5],
        'float_col': [1.1, 2.2, 3.3, 4.4, 5.5],
        'str_col': ['a', 'b', 'c', 'd', 'e'],
        'bool_col': [True, False, True, False, True],
        'datetime_col': pd.date_range('2023-01-01', periods=5)
    })
    df.index = ['row' + str(i) for i in range(5)]
    
    # Store DataFrame
    db['test_df'] = df
    
    # Retrieve DataFrame
    retrieved_df = db['test_df']
    
    # Check equality
    pd.testing.assert_frame_equal(df, retrieved_df)
    
    # Test with numpy array in DataFrame
    df['array_col'] = [np.array([1, 2, 3])] * 5
    db['test_df_with_array'] = df
    retrieved_df = db['test_df_with_array']
    
    # Check basic properties
    assert df.shape == retrieved_df.shape
    assert all(df.columns == retrieved_df.columns)
    assert all(df.index == retrieved_df.index)
    assert all(df.dtypes == retrieved_df.dtypes)
    
    db.close()


def test_series_operations(db_root):
    """Test Series storage and retrieval."""
    db = create_database("test", root_path=db_root, serializer=PandasSerializer())
    
    # Create test Series
    series = pd.Series([1, 2, 3, 4, 5], 
                      index=['a', 'b', 'c', 'd', 'e'],
                      name='test_series')
    
    # Store Series
    db['test_series'] = series
    
    # Retrieve Series
    retrieved_series = db['test_series']
    
    # Check equality
    pd.testing.assert_series_equal(series, retrieved_series)
    
    # Test with different dtypes
    series_types = {
        'int_series': pd.Series([1, 2, 3]),
        'float_series': pd.Series([1.1, 2.2, 3.3]),
        'str_series': pd.Series(['a', 'b', 'c']),
        'bool_series': pd.Series([True, False, True]),
        'datetime_series': pd.Series(pd.date_range('2023-01-01', periods=3))
    }
    
    for name, s in series_types.items():
        db[name] = s
        retrieved = db[name]
        pd.testing.assert_series_equal(s, retrieved)
    
    db.close()


def test_large_dataframe(db_root):
    """Test with larger DataFrame to check performance."""
    db = create_database("test", root_path=db_root, serializer=PandasSerializer())
    
    # Create large DataFrame (100k rows, 10 columns)
    n_rows = 100_000
    n_cols = 10
    data = {
        f'col_{i}': np.random.randn(n_rows) 
        for i in range(n_cols)
    }
    large_df = pd.DataFrame(data)
    
    # Store DataFrame
    db['large_df'] = large_df
    
    # Retrieve DataFrame
    retrieved_df = db['large_df']
    
    # Check equality
    pd.testing.assert_frame_equal(large_df, retrieved_df)
    
    db.close()


def test_mixed_operations(db_root):
    """Test mixed operations with different types."""
    db = create_database("test", root_path=db_root, serializer=PandasSerializer())
    
    # Store different types
    db['str'] = "string"
    db['int'] = 42
    db['list'] = [1, 2, 3]
    db['dict'] = {'a': 1, 'b': 2}
    db['array'] = np.array([1, 2, 3])
    db['df'] = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    db['series'] = pd.Series([1, 2, 3])
    
    # Verify all types
    assert db['str'] == "string"
    assert db['int'] == 42
    assert db['list'] == [1, 2, 3]
    assert db['dict'] == {'a': 1, 'b': 2}
    np.testing.assert_array_equal(db['array'], np.array([1, 2, 3]))
    pd.testing.assert_frame_equal(
        db['df'], 
        pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    )
    pd.testing.assert_series_equal(
        db['series'],
        pd.Series([1, 2, 3])
    )
    
    db.close() 