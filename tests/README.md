# FlaxKV2 Tests

## Test Structure

```
tests/
├── unit/              # Unit tests for individual components
│   ├── test_core.py
│   ├── test_special_values.py
│   ├── test_raw_leveldb_ttl.py
│   ├── test_default_ttl.py
│   └── test_ttl_persistence.py
│
├── integration/       # Integration tests for features
│   ├── test_auto_nested.py
│   ├── test_nested_dict.py
│   ├── test_nested_dict_improvements.py
│   ├── test_ttl_auto_nested_bug.py
│   ├── test_ttl_refactor.py
│   ├── test_zmq_remote.py
│   ├── test_cache_basic.py
│   ├── test_server_ttl.py
│   ├── test_encryption_compression.py
│   ├── test_password_auth.py
│   └── test_derive_password.py
│
└── benchmarks/        # Performance benchmarks
    ├── benchmark_cache.py
    ├── benchmark_nested.py
    └── benchmark_nested_real.py
```

## Running Tests

### Run all tests
```bash
pytest tests/
```

### Run unit tests only
```bash
pytest tests/unit/
```

### Run integration tests only
```bash
pytest tests/integration/
```

### Run specific test file
```bash
pytest tests/integration/test_encryption_compression.py
```

### Run benchmarks
```bash
python tests/benchmarks/benchmark_cache.py
python tests/benchmarks/benchmark_nested.py
```

## Test Categories

### Unit Tests
- **test_core.py**: Core RawLevelDBDict functionality
- **test_special_values.py**: Special value handling (None, empty, etc.)
- **test_raw_leveldb_ttl.py**: TTL functionality at the raw level
- **test_default_ttl.py**: Default TTL behavior
- **test_ttl_persistence.py**: TTL persistence across restarts

### Integration Tests
- **test_auto_nested.py**: Auto-nested dictionary functionality
- **test_nested_dict.py**: Nested dictionary operations
- **test_nested_dict_improvements.py**: NestedDBDict MutableMapping interface
- **test_ttl_auto_nested_bug.py**: TTL + auto_nested edge cases
- **test_ttl_refactor.py**: TTL refactor validation
- **test_zmq_remote.py**: ZeroMQ remote database operations
- **test_cache_basic.py**: SimpleLRUCache functionality
- **test_server_ttl.py**: Server-side TTL handling
- **test_encryption_compression.py**: CurveZMQ + LZ4 compression
- **test_password_auth.py**: Password-based authentication (file storage)
- **test_derive_password.py**: Password-based authentication (password derivation)

### Benchmarks
- **benchmark_cache.py**: Cache performance comparison
- **benchmark_nested.py**: Nested dictionary performance
- **benchmark_nested_real.py**: Real-world nested dictionary scenarios
