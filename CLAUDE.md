# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FlaxKV2 is a high-performance key-value database system with a Python dictionary-like interface, built on LevelDB. It supports both local and remote (ZeroMQ) backends, TTL expiration, nested dictionaries, and automatic serialization for complex data types.

**Important Architecture Note**: The project recently deprecated `LevelDBDict` in favor of `RawLevelDBDict`, which provides 13-25% better performance by eliminating buffering mechanisms. `RawLevelDBDict` is now the default backend.

## Development Commands

### Testing
```bash
# Run all tests
pytest -v -s

# Run specific test file
pytest tests/test_core.py -v

# Run tests with coverage (if pytest-cov installed)
pytest --cov=flaxkv2 --cov-report=html
```

### Code Formatting and Linting
```bash
# Format code with black
black .

# Sort imports with isort
isort . --profile black

# Run pre-commit hooks manually
pre-commit run --all-files

# Install pre-commit hooks (first time)
pre-commit install

# Lint with flake8 (basic syntax errors)
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics --exclude .git,__pycache__,docs,old,build,dist,tests/
```

### Running the Server
```bash
# Start ZeroMQ server locally
flaxkv2 run --host 127.0.0.1 --port 5555 --data-dir ./data

# Start server accessible from network (WARNING: no authentication/encryption)
flaxkv2 run --host 0.0.0.0 --port 5555 --data-dir ./data

# With custom settings
flaxkv2 run --host 127.0.0.1 --port 5555 --data-dir /var/lib/flaxkv --workers 8 --log-level DEBUG
```

### Docker
```bash
# Build Docker image
make build

# Start container
make start

# View logs
make log

# Execute shell in container
make exec

# Remove container
make rm
```

## Architecture Overview

### Core Components

**Backend Hierarchy**:
- `FlaxKV` (factory class in `flaxkv2/__init__.py`): Entry point that auto-detects backend type
  - Local Backend: `RawLevelDBDict` (recommended, in `flaxkv2/core/raw_leveldb_dict.py`)
  - Remote Backend: `RemoteDBDict` (ZeroMQ client in `flaxkv2/client/zmq_client.py`)

**Deprecated**:
- `LevelDBDict` (in `flaxkv2/core/leveldb_dict.py`): Feature-rich but slower version with buffering/bloom filters. Performance tests showed 30-87% slower than RawLevelDBDict on modern SSDs. Still importable but will be removed in future versions.

**Key Supporting Modules**:
- `flaxkv2/serialization/`: Encoder/Decoder for msgpack, pickle, NumPy arrays, Pandas DataFrames
- `flaxkv2/core/nested_dict.py`: NestedDBDict for hierarchical key-value storage
- `flaxkv2/utils/ttl.py`: TTLManager for key expiration (stored as `__ttl__:<key>` in LevelDB)
- `flaxkv2/instance_manager.py`: Instance caching to avoid reopening same database
- `flaxkv2/auto_close.py`: Automatic cleanup on program exit
- `flaxkv2/server/zmq_server.py`: FlaxKVServer for remote access

### Data Flow

**Local Write**: `db["key"] = value` → Encoder.encode() → TTLManager (if default_ttl) → plyvel.DB.put() → LevelDB

**Local Read**: `value = db["key"]` → TTLManager.is_expired() → plyvel.DB.get() → Decoder.decode() → Python object

**Remote Write**: `db["key"] = value` → Encoder.encode() → ZeroMQ [CMD_SET, db_name, key_bytes, value_bytes] → Server → RawLevelDBDict.put() → LevelDB

### Serialization Strategy
1. msgpack for basic types (fastest)
2. Special handling for NumPy arrays (binary with dtype/shape metadata)
3. Special handling for Pandas DataFrames (optional dependency)
4. pickle fallback for complex objects (⚠️ security risk - only use in trusted environments)

**Performance Optimization - Type Caching (P1)**:
- Encoder caches the best serialization method for each Python type
- First encode: try msgpack → cache result
- Subsequent encodes: use cached encoder directly (no try-except overhead)
- Achieves 99%+ cache hit rate, 15-30% performance improvement
- See `P1_SERIALIZATION_OPTIMIZATION_SUMMARY.md` for details

## Important Implementation Details

### TTL Implementation
- TTL metadata stored as regular keys with prefix `__ttl__:<original_key>`
- Values are expiration timestamps (float)
- Server-side TTL validation reduces network requests by ~50%
- No in-memory dictionary; all TTL data persisted in LevelDB
- Recent refactor (see `CHANGELOG.md`) simplified architecture significantly

### Instance Management
- `InstanceManager` caches DB instances by (name, path) tuple
- Prevents multiple opens of same database
- Thread-safe with locks
- Auto-close on program exit via `auto_close.py`

### Remote Protocol (ZeroMQ)
- Client: REQ socket, server: ROUTER socket
- Message format: `[command_type, db_name, ...args]`
- Response format: `[status_code, data]`
- Commands: CONNECT, GET, SET, DELETE, KEYS, VALUES, ITEMS, UPDATE, PING
- Server only handles binary data; client does all serialization

### Nested Dictionary Pattern
- Uses LevelDB's `prefixed_db()` feature
- Keys stored as `<prefix>:<field>`
- Each field serialized independently (avoids re-serializing entire dict on single field update)
- Recursive nesting supported
- Enable with `auto_nested=True` or use `db.nested(prefix)` explicitly

## Testing Considerations

### Test Structure
- `tests/conftest.py`: Shared fixtures
- `tests/test_core.py`: Core RawLevelDBDict functionality
- `tests/test_zmq_remote.py`: Remote ZeroMQ backend tests
- `tests/test_*_ttl.py`: Various TTL-related tests
- `tests/test_nested_dict.py`: Nested dictionary tests
- `tests/test_special_values.py`: Edge cases and special value handling

### Running Remote Tests
Remote tests in `test_zmq_remote.py` require starting a test server:
```python
# The test file should handle server startup/shutdown in fixtures
# If tests fail with connection errors, check if server process is running
```

## Code Style

- **Formatter**: black (version 24.3.0)
- **Import sorting**: isort with black profile
- **Line length**: 127 characters (flake8 config)
- **Pre-commit hooks**: Automatically format on commit
- Follow patterns in existing code for consistency

## Security Warnings

⚠️ **Pickle Serialization**: FlaxKV2 uses pickle for complex objects. This is unsafe with untrusted data as pickle can execute arbitrary code. Only use in trusted environments or store simple data types.

⚠️ **Remote Connections**: ZeroMQ server has no authentication or encryption by default. Only use in trusted networks, behind firewalls, through VPN/SSH tunnels, or bind to 127.0.0.1 only.

## Performance Notes

### Baseline Performance
- RawLevelDBDict achieves 500K+ ops/sec on SSDs
- For best performance: use RawLevelDBDict (default), deploy on SSD, avoid LevelDBDict buffering
- Nested dictionaries reduce serialization overhead for large objects with frequent partial updates

### Recent Performance Optimizations (2025-10)

**✅ P0: LevelDB Configuration Optimization** (Completed)
- Enabled 256MB LRU cache (from default 8MB)
- Added bloom filters (10 bits per key)
- Optimized block size (16KB from 4KB)
- **Result**: 4-25% performance improvement across scenarios
- **Usage**: Automatic with `FlaxKV()` (uses 'balanced' profile by default)
- See `P0_PERFORMANCE_OPTIMIZATION_SUMMARY.md` and `doc/PERFORMANCE_CONFIG_QUICKSTART.md`

**✅ P1: Intelligent Type Caching** (Completed)
- Caches best encoder for each Python type (avoids repeated try-except)
- 99%+ cache hit rate in typical workloads
- **Result**: 14-30% encoding performance improvement
- **Usage**: Automatic, no code changes needed
- Monitor with: `from flaxkv2.serialization import get_cache_stats, print_cache_stats`
- See `P1_SERIALIZATION_OPTIMIZATION_SUMMARY.md`

**Combined Impact**: ~20-50% overall performance improvement for typical workloads

### Performance Configuration Profiles

Available profiles: `balanced` (default), `read_optimized`, `write_optimized`, `memory_constrained`, `large_database`, `ml_workload`

```python
# Use default optimized config
db = FlaxKV("mydb", "./data")

# Choose specific profile
db = FlaxKV("cache", "./data", performance_profile='read_optimized')

# Custom tuning
db = FlaxKV("mydb", "./data", lru_cache_size=512*1024*1024)
```

See `doc/LEVELDB_CONFIGURATION_GUIDE.md` for detailed parameter explanations.

## Key Files to Understand

1. `flaxkv2/__init__.py` - Main factory class and backend routing
2. `flaxkv2/core/raw_leveldb_dict.py` - Primary local backend implementation
3. `flaxkv2/client/zmq_client.py` - Remote client (RemoteDBDict)
4. `flaxkv2/server/zmq_server.py` - Remote server (FlaxKVServer)
5. `flaxkv2/serialization/encoder.py` & `decoder.py` - Serialization logic
6. `ARCHITECTURE.md` - Comprehensive architecture documentation
7. `CHANGELOG.md` - Recent changes and deprecations
