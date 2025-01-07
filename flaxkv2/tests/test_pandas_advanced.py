"""Advanced tests for pandas support in FlaxKV2."""
import os
import shutil
import tempfile
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from flaxkv2 import create_database
from flaxkv2.serialization.pandas_serializer import PandasSerializer


@pytest.fixture
def db_root():
    """Temporary directory for database files."""
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path)


def test_dataframe_dtypes(db_root):
    """Test DataFrame with various data types."""
    db = create_database("test", root_path=db_root, serializer=PandasSerializer())
    
    # Create test DataFrame with various dtypes
    df = pd.DataFrame({
        'int8': pd.Series([1, 2, 3], dtype='int8'),
        'int16': pd.Series([1, 2, 3], dtype='int16'),
        'int32': pd.Series([1, 2, 3], dtype='int32'),
        'int64': pd.Series([1, 2, 3], dtype='int64'),
        'uint8': pd.Series([1, 2, 3], dtype='uint8'),
        'uint16': pd.Series([1, 2, 3], dtype='uint16'),
        'uint32': pd.Series([1, 2, 3], dtype='uint32'),
        'uint64': pd.Series([1, 2, 3], dtype='uint64'),
        'float32': pd.Series([1.1, 2.2, 3.3], dtype='float32'),
        'float64': pd.Series([1.1, 2.2, 3.3], dtype='float64'),
        'bool': pd.Series([True, False, True], dtype='bool'),
        'string': pd.Series(['a', 'b', 'c'], dtype='string'),
        'object': pd.Series([{'a': 1}, {'b': 2}, {'c': 3}], dtype='object'),
        'datetime': pd.Series(pd.date_range('2023-01-01', periods=3)),
        'timedelta': pd.Series([timedelta(days=i) for i in range(3)]),
    })
    
    # Store DataFrame
    db['test_df'] = df
    
    # Retrieve and verify
    retrieved_df = db['test_df']
    assert_frame_equal(df, retrieved_df)
    
    # Verify dtypes are preserved
    for col in df.columns:
        assert df[col].dtype == retrieved_df[col].dtype
    
    db.close()


def test_dataframe_index_types(db_root):
    """Test DataFrame with various index types."""
    db = create_database("test", root_path=db_root, serializer=PandasSerializer())
    
    # Test different index types
    index_cases = {
        'range_index': pd.RangeIndex(start=0, stop=5),
        'int_index': pd.Index([1, 2, 3, 4, 5]),
        'string_index': pd.Index(['a', 'b', 'c', 'd', 'e']),
        'datetime_index': pd.date_range('2023-01-01', periods=5),
        'multi_index': pd.MultiIndex.from_tuples([
            ('A', 1), ('A', 2), ('B', 1), ('B', 2), ('C', 1)
        ]),
        'float_index': pd.Index([1.1, 2.2, 3.3, 4.4, 5.5]),
    }
    
    for name, index in index_cases.items():
        df = pd.DataFrame({
            'col1': range(5),
            'col2': ['a', 'b', 'c', 'd', 'e']
        }, index=index)
        
        db[name] = df
        retrieved = db[name]
        
        assert_frame_equal(df, retrieved)
        assert df.index.equals(retrieved.index)
    
    db.close()


def test_series_advanced(db_root):
    """Test Series with advanced features."""
    db = create_database("test", root_path=db_root, serializer=PandasSerializer())
    
    # Test different Series configurations
    series_cases = {
        'basic': pd.Series([1, 2, 3], name='basic'),
        'with_index': pd.Series([1, 2, 3], index=['a', 'b', 'c'], name='indexed'),
        'datetime': pd.Series(
            [1, 2, 3],
            index=pd.date_range('2023-01-01', periods=3),
            name='datetime_indexed'
        ),
        'multi_index': pd.Series(
            [1, 2, 3, 4],
            index=pd.MultiIndex.from_tuples([
                ('A', 1), ('A', 2), ('B', 1), ('B', 2)
            ]),
            name='multi_indexed'
        ),
        'with_nans': pd.Series([1, np.nan, 3], name='with_nans'),
        'boolean': pd.Series([True, False, True], name='boolean'),
        'string': pd.Series(['a', 'b', 'c'], name='string'),
        'categorical': pd.Series(['a', 'b', 'a'], dtype='category', name='categorical'),
    }
    
    for name, series in series_cases.items():
        db[name] = series
        retrieved = db[name]
        assert_series_equal(series, retrieved)
    
    db.close()


def test_large_dataframe_performance(db_root):
    """Test performance with large DataFrames."""
    db = create_database("test", root_path=db_root, serializer=PandasSerializer())
    
    # Create large DataFrame (1M rows, 10 columns)
    n_rows = 1_000_000
    n_cols = 10
    
    # Generate different types of columns
    data = {
        'int_col': np.random.randint(0, 1000, n_rows),
        'float_col': np.random.randn(n_rows),
        'bool_col': np.random.choice([True, False], n_rows),
        'str_col': np.random.choice(['A', 'B', 'C', 'D'], n_rows),
        'timestamp_col': np.random.randint(
            0, int(1e9), n_rows, dtype=np.int64
        ),  # Unix timestamps
    }
    
    # Add more numeric columns
    for i in range(5):
        data[f'num_col_{i}'] = np.random.randn(n_rows)
    
    large_df = pd.DataFrame(data)
    large_df['datetime_col'] = pd.to_datetime(large_df['timestamp_col'], unit='s')
    del large_df['timestamp_col']
    
    # Test write performance
    import time
    start_time = time.time()
    db['large_df'] = large_df
    write_time = time.time() - start_time
    
    # Test read performance
    start_time = time.time()
    retrieved_df = db['large_df']
    read_time = time.time() - start_time
    
    # Verify data integrity
    assert_frame_equal(large_df, retrieved_df)
    
    # Log performance metrics
    print(f"\nPerformance metrics for {n_rows:,} rows x {n_cols} columns:")
    print(f"Write time: {write_time:.2f} seconds")
    print(f"Read time: {read_time:.2f} seconds")
    print(f"Write throughput: {n_rows/write_time:,.0f} rows/second")
    print(f"Read throughput: {n_rows/read_time:,.0f} rows/second")
    
    db.close()


def test_dataframe_operations(db_root):
    """Test DataFrame operations and modifications."""
    db = create_database("test", root_path=db_root, serializer=PandasSerializer())
    
    # Create initial DataFrame
    df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': ['a', 'b', 'c'],
        'C': [True, False, True]
    })
    
    # Test column operations
    db['df'] = df
    df['D'] = [4, 5, 6]  # Add column
    db['df'] = df
    retrieved = db['df']
    assert_frame_equal(df, retrieved)
    
    # Test row operations
    df.loc[3] = [4, 'd', False, 7]  # Add row
    db['df'] = df
    retrieved = db['df']
    assert_frame_equal(df, retrieved)
    
    # Test deletion
    del df['D']  # Delete column
    db['df'] = df
    retrieved = db['df']
    assert_frame_equal(df, retrieved)
    
    # Test update
    df.iloc[0] = [10, 'x', False]  # Update row
    db['df'] = df
    retrieved = db['df']
    assert_frame_equal(df, retrieved)
    
    db.close()


def test_nested_structures(db_root):
    """Test nested structures with pandas objects."""
    db = create_database("test", root_path=db_root, serializer=PandasSerializer())
    
    # Create test data
    df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    df2 = pd.DataFrame({'C': [5, 6], 'D': [7, 8]})
    series1 = pd.Series([1, 2, 3], name='series1')
    series2 = pd.Series([4, 5, 6], name='series2')
    
    # Test dictionary of pandas objects
    data_dict = {
        'df1': df1,
        'df2': df2,
        'series1': series1,
        'series2': series2,
        'mixed': {
            'df': df1,
            'series': series1,
            'nested': {
                'df': df2,
                'series': series2
            }
        }
    }
    
    # Store and retrieve
    db['nested_data'] = data_dict
    retrieved = db['nested_data']
    
    # Verify structure and contents
    assert set(data_dict.keys()) == set(retrieved.keys())
    assert_frame_equal(data_dict['df1'], retrieved['df1'])
    assert_frame_equal(data_dict['df2'], retrieved['df2'])
    assert_series_equal(data_dict['series1'], retrieved['series1'])
    assert_series_equal(data_dict['series2'], retrieved['series2'])
    
    # Verify nested structure
    assert_frame_equal(data_dict['mixed']['df'], retrieved['mixed']['df'])
    assert_series_equal(data_dict['mixed']['series'], retrieved['mixed']['series'])
    assert_frame_equal(
        data_dict['mixed']['nested']['df'],
        retrieved['mixed']['nested']['df']
    )
    assert_series_equal(
        data_dict['mixed']['nested']['series'],
        retrieved['mixed']['nested']['series']
    )
    
    db.close()


def test_error_handling(db_root):
    """Test error handling for pandas objects."""
    db = create_database("test", root_path=db_root, serializer=PandasSerializer())
    
    # Test invalid DataFrame (trying to convert string to int)
    with pytest.raises((ValueError, TypeError)):
        df = pd.DataFrame({'A': ['1', 'a', '3']})
        df = df.astype('int32')
        db['invalid'] = df
    
    # Test invalid Series (trying to convert string to int)
    with pytest.raises((ValueError, TypeError)):
        series = pd.Series(['1', 'a', '3'])
        series = series.astype('int32')
        db['invalid'] = series
    
    # Test DataFrame with complex objects that can't be serialized
    class UnserializableObject:
        def __init__(self, x):
            self.x = x
    
    with pytest.raises((ValueError, TypeError)):
        df = pd.DataFrame({
            'A': [UnserializableObject(1), UnserializableObject(2)]
        })
        db['invalid'] = df
    
    # Test Series with complex objects that can't be serialized
    with pytest.raises((ValueError, TypeError)):
        series = pd.Series([
            UnserializableObject(1),
            UnserializableObject(2)
        ])
        db['invalid'] = series
    
    db.close() 