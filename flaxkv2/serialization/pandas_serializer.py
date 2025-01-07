"""Pandas serializer implementation."""
import msgspec
import numpy as np
import pandas as pd

from ..core.interfaces import Serializer


class PandasFrame(msgspec.Struct, gc=False):
    """Pandas DataFrame serialization structure."""
    values: bytes  # numpy array data
    columns: bytes  # column names
    index: bytes   # index data
    dtypes: bytes  # column dtypes


class PandasSeries(msgspec.Struct, gc=False):
    """Pandas Series serialization structure."""
    values: bytes  # numpy array data
    index: bytes   # index data
    dtype: str     # data type
    name: str      # series name


class PandasSerializer(Serializer):
    """MsgPack-based serializer with optimized pandas support."""
    
    def __init__(self):
        """Initialize the serializer with pandas support."""
        self._encoder = msgspec.msgpack.Encoder()
        self._decoder = msgspec.msgpack.Decoder()
        self._frame_encoder = msgspec.msgpack.Encoder()
        self._frame_decoder = msgspec.msgpack.Decoder(type=PandasFrame)
        self._series_encoder = msgspec.msgpack.Encoder()
        self._series_decoder = msgspec.msgpack.Decoder(type=PandasSeries)

    def serialize(self, obj: any) -> bytes:
        """Serialize an object to bytes using msgpack."""
        try:
            if isinstance(obj, pd.DataFrame):
                return self._serialize_dataframe(obj)
            elif isinstance(obj, pd.Series):
                return self._serialize_series(obj)
            return self._encoder.encode(obj)
        except Exception as e:
            raise ValueError(f"Failed to serialize object: {e}") from e

    def deserialize(self, data: bytes) -> any:
        """Deserialize bytes to an object using msgpack."""
        try:
            # Try to decode as DataFrame first
            try:
                frame = self._frame_decoder.decode(data)
                return self._deserialize_dataframe(frame)
            except:
                pass

            # Try to decode as Series
            try:
                series = self._series_decoder.decode(data)
                return self._deserialize_series(series)
            except:
                pass

            # Fall back to regular decoding
            return self._decoder.decode(data)
        except Exception as e:
            raise ValueError(f"Failed to deserialize data: {e}") from e

    def _serialize_dataframe(self, df: pd.DataFrame) -> bytes:
        """Serialize a pandas DataFrame."""
        frame = PandasFrame(
            values=df.values.tobytes(),
            columns=self._encoder.encode(df.columns.tolist()),
            index=self._encoder.encode(df.index.tolist()),
            dtypes=self._encoder.encode([str(dt) for dt in df.dtypes])
        )
        return self._frame_encoder.encode(frame)

    def _deserialize_dataframe(self, frame: PandasFrame) -> pd.DataFrame:
        """Deserialize bytes to a pandas DataFrame."""
        columns = self._decoder.decode(frame.columns)
        index = self._decoder.decode(frame.index)
        dtypes = self._decoder.decode(frame.dtypes)
        
        # Reconstruct the numpy array with correct shape and dtypes
        values = np.frombuffer(frame.values)
        if len(dtypes) > 0:
            values = values.reshape(-1, len(dtypes))
            # Convert to appropriate dtypes
            df = pd.DataFrame(values, columns=columns, index=index)
            for col, dtype in zip(df.columns, dtypes):
                df[col] = df[col].astype(dtype)
            return df
        return pd.DataFrame(values.reshape(-1, 1), columns=columns, index=index)

    def _serialize_series(self, series: pd.Series) -> bytes:
        """Serialize a pandas Series."""
        series_struct = PandasSeries(
            values=series.values.tobytes(),
            index=self._encoder.encode(series.index.tolist()),
            dtype=str(series.dtype),
            name=str(series.name) if series.name is not None else ""
        )
        return self._series_encoder.encode(series_struct)

    def _deserialize_series(self, series: PandasSeries) -> pd.Series:
        """Deserialize bytes to a pandas Series."""
        values = np.frombuffer(series.values, dtype=series.dtype)
        index = self._decoder.decode(series.index)
        name = series.name if series.name != "" else None
        return pd.Series(values, index=index, name=name, dtype=series.dtype) 