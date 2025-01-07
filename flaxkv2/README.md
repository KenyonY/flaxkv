# FlaxKV2

A high-performance dictionary database with improved design, providing a Python dictionary-like interface for persistent storage.

## Features

- **High Performance**: Near-native dictionary performance with LevelDB backend
- **Dictionary-like Interface**: Familiar Python dictionary operations
- **Non-blocking Writes**: Write operations don't block the user process
- **Buffered Writes**: Efficient write batching with background flushing
- **Type Support**: Native support for Python types, NumPy arrays, Pandas DataFrames, and more
- **Thread Safety**: Safe concurrent access with minimal locking
- **Modular Design**: Easy to extend with new storage backends and serializers

## Installation

```bash
pip install flaxkv2
```

## Quick Start

```python
from flaxkv2 import get_database
from flaxkv2.serialization.pandas_serializer import PandasSerializer
import numpy as np
import pandas as pd

# Create or open a database with pandas support
db = get_database("mydb", serializer=PandasSerializer())

# Basic operations
db["key"] = "value"
db[1] = 42
db["list"] = [1, 2, 3]
db["dict"] = {"a": 1, "b": 2}

# NumPy array support
array = np.random.randn(100, 100)
db["array"] = array

# Pandas DataFrame support
df = pd.DataFrame({
    'int_col': [1, 2, 3],
    'float_col': [1.1, 2.2, 3.3],
    'str_col': ['a', 'b', 'c']
})
db["dataframe"] = df

# Pandas Series support
series = pd.Series([1, 2, 3], name='my_series')
db["series"] = series

# Dictionary-like operations
value = db.get("key", default="not found")
items = dict(db)
del db["key"]

# Batch updates
db.update({
    "key1": "value1",
    "key2": "value2"
})

# Clean up
db.close()
```

## Architecture

FlaxKV2 is built with a modular, extensible architecture:

- **Core Layer**: Defines the interfaces and base implementations
- **Storage Layer**: Pluggable storage backends (default: LevelDB)
- **Serialization Layer**: Flexible serialization strategies
  - MsgPack Serializer: For basic Python types
  - Pandas Serializer: Optimized for DataFrame and Series
- **Buffer Layer**: Efficient write buffering and background flushing

## Design Improvements

Compared to the original FlaxKV, FlaxKV2 offers several improvements:

1. **Clean Architecture**
   - Clear separation of concerns
   - Well-defined interfaces
   - Modular components

2. **Extensibility**
   - Pluggable storage backends
   - Customizable serialization
   - Easy to add new features

3. **Code Quality**
   - Improved type hints
   - Better error handling
   - Comprehensive documentation

4. **Testing**
   - Extensive test coverage
   - Property-based testing
   - Integration tests

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details. 