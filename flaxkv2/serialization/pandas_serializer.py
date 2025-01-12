"""Pandas serializer implementation."""
import json
from typing import Any, Dict, Optional, Union

import msgspec
import numpy as np
import pandas as pd


class PandasStruct(msgspec.Struct):
    """Structure for serializing pandas objects."""
    type: str  # 'dataframe', 'series', or 'ndarray'
    data: bytes  # Serialized data
    index: Optional[bytes] = None  # Serialized index
    columns: Optional[bytes] = None  # Serialized columns (for DataFrame)
    dtype: Optional[str] = None  # dtype string for numpy arrays


class PandasSerializer:
    """Serializer for pandas DataFrames and Series."""
    
    def __init__(self):
        """Initialize the serializer."""
        self._encoder = msgspec.msgpack.Encoder()
        self._decoder = msgspec.msgpack.Decoder()
    
    def _serialize_array(self, arr: np.ndarray) -> bytes:
        """Serialize a numpy array."""
        struct = PandasStruct(
            type='ndarray',
            data=arr.tobytes(),
            dtype=str(arr.dtype),
            index=None,
            columns=None
        )
        return self._encoder.encode(struct)
    
    def _deserialize_array(self, struct: PandasStruct) -> np.ndarray:
        """Deserialize a numpy array."""
        arr = np.frombuffer(struct.data, dtype=np.dtype(struct.dtype))
        if struct.columns is not None:
            # This is a 2D array
            cols = len(self._decoder.decode(struct.columns))
            arr = arr.reshape(-1, cols)
        return arr
    
    def _serialize_index(self, idx: Union[pd.Index, pd.MultiIndex]) -> bytes:
        """Serialize an index."""
        if isinstance(idx, pd.MultiIndex):
            return self._encoder.encode({
                'type': 'multi',
                'names': idx.names,
                'levels': [self._serialize_array(level.values) for level in idx.levels],
                'codes': [self._serialize_array(code) for code in idx.codes]
            })
        return self._encoder.encode({
            'type': 'single',
            'name': idx.name,
            'values': self._serialize_array(idx.values)
        })
    
    def _deserialize_index(self, data: bytes) -> pd.Index:
        """Deserialize an index."""
        idx_data = self._decoder.decode(data)
        if idx_data['type'] == 'multi':
            levels = [
                self._deserialize_array(self._decoder.decode(level))
                for level in idx_data['levels']
            ]
            codes = [
                self._deserialize_array(self._decoder.decode(code))
                for code in idx_data['codes']
            ]
            return pd.MultiIndex(levels=levels, codes=codes, names=idx_data['names'])
        values = self._deserialize_array(self._decoder.decode(idx_data['values']))
        return pd.Index(values, name=idx_data['name'])
    
    def _serialize_dataframe(self, df: pd.DataFrame) -> bytes:
        """Serialize a DataFrame."""
        data = {
            col: self._serialize_array(df[col].values)
            for col in df.columns
        }
        struct = PandasStruct(
            type='dataframe',
            data=self._encoder.encode(data),
            index=self._serialize_index(df.index),
            columns=self._encoder.encode(list(df.columns))
        )
        return self._encoder.encode(struct)
    
    def _deserialize_dataframe(self, struct: PandasStruct) -> pd.DataFrame:
        """Deserialize a DataFrame."""
        data = self._decoder.decode(struct.data)
        columns = self._decoder.decode(struct.columns)
        index = self._deserialize_index(struct.index)
        
        df_data = {}
        for col in columns:
            col_struct = self._decoder.decode(data[col], type=PandasStruct)
            df_data[col] = self._deserialize_array(col_struct)
        
        return pd.DataFrame(df_data, index=index, columns=columns)
    
    def _serialize_series(self, series: pd.Series) -> bytes:
        """Serialize a Series."""
        struct = PandasStruct(
            type='series',
            data=self._serialize_array(series.values),
            index=self._serialize_index(series.index),
            dtype=str(series.dtype)
        )
        return self._encoder.encode(struct)
    
    def _deserialize_series(self, struct: PandasStruct) -> pd.Series:
        """Deserialize a Series."""
        values = self._deserialize_array(struct)
        index = self._deserialize_index(struct.index)
        return pd.Series(values, index=index, dtype=struct.dtype)
    
    def serialize(self, obj: Any) -> bytes:
        """Serialize an object to bytes using msgpack."""
        try:
            if isinstance(obj, pd.DataFrame):
                return self._serialize_dataframe(obj)
            elif isinstance(obj, pd.Series):
                return self._serialize_series(obj)
            elif isinstance(obj, np.ndarray):
                return self._serialize_array(obj)
            elif isinstance(obj, (str, int, float, bool, type(None))):
                return self._encoder.encode(obj)
            return self._encoder.encode(obj)
        except Exception as e:
            raise ValueError(f"Failed to serialize object: {e}") from e
    
    def deserialize(self, data: bytes) -> Any:
        """Deserialize bytes to an object."""
        try:
            # Try to decode as a simple type first
            try:
                return self._decoder.decode(data)
            except msgspec.DecodeError:
                pass
            
            # Try to decode as a PandasStruct
            struct = self._decoder.decode(data, type=PandasStruct)
            
            if struct.type == 'dataframe':
                return self._deserialize_dataframe(struct)
            elif struct.type == 'series':
                return self._deserialize_series(struct)
            elif struct.type == 'ndarray':
                return self._deserialize_array(struct)
            else:
                raise ValueError(f"Unknown type: {struct.type}")
        except Exception as e:
            raise ValueError(f"Failed to deserialize data: {e}") from e 