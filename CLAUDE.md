# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FlaxKV2 is a high-performance persistent key-value storage library for Python built on LevelDB. It provides a dict-like interface with support for:
- Thread-safe local LevelDB backend with optional unified caching
- Remote access via ZeroMQ client/server architecture
- Rich data types (strings, numbers, lists, dicts, NumPy arrays, Pandas DataFrames)
- TTL (time-to-live) support with automatic expiration
- Nested dict/list storage for efficient partial updates
- 6 performance profiles for different workload scenarios

## Core Architecture

### Backend Types
- **RawLevelDBDict** (`flaxkv2/core/raw_leveldb_dict.py`): Default local backend, no caching, data safety prioritized
- **CachedLevelDBDict** (`flaxkv2/core/cached_leveldb_dict.py`): Local backend with unified cache (Write-back Cache design)
  - Read cache (LRU): ~20x performance boost for hot data
  - Write buffer: 1.6-10x performance boost with batch writes
  - Async flush option for extreme performance (with trade-offs)
- **RemoteDBDict** (`flaxkv2/client/zmq_client.py`): ZeroMQ client for remote server access
- **LevelDBDict** (deprecated): Old implementation, removed from main exports

### Factory Pattern
The `FlaxKV` class in `flaxkv2/__init__.py` is a factory that creates the appropriate backend:
- Local backend: `FlaxKV("mydb", "./data")` or `FlaxKV("mydb", "./data", use_cache=True)`
- Remote backend: `FlaxKV("mydb", "tcp://host:5555")` or with explicit `backend='remote'`

### Key Components
- **Serialization** (`flaxkv2/serialization/`):
  - `encoder.py`: Serializes Python objects to bytes (msgpack, pickle, numpy, pandas)
  - `decoder.py`: Deserializes bytes back to Python objects
  - `value_meta.py`: Metadata wrapper for values with TTL info
- **Server** (`flaxkv2/server/zmq_server.py`): ZeroMQ server for remote access
- **Nested Structures** (`flaxkv2/core/nested_structures.py`): NestedDBDict and NestedDBList for efficient nested data access
- **Unified Cache** (`flaxkv2/utils/unified_cache.py`): Write-back cache with LRU eviction, dirty tracking, and async flush
- **TTL Management** (`flaxkv2/utils/ttl_cleanup.py`): Background thread for automatic expiration
- **Config** (`flaxkv2/config.py`): 6 performance profiles (balanced, read_optimized, write_optimized, memory_constrained, large_database, ml_workload)
- **Inspector** (`flaxkv2/inspector/`): Visualization and management tools (CLI + Web UI)
  - `__init__.py`: Core Inspector class for data browsing, stats, and management
  - `cli.py`: CLI commands for terminal-based inspection
  - `web.py`: Flask-based Web UI server
  - `flaxkv2/static/`: Web UI static files (HTML/CSS/JS)

### Cache Design
The unified cache uses a Write-back Cache pattern:
- Single cache for both reads and writes (avoids dual-cache complexity)
- Dirty tracking with flush triggers (threshold, interval, manual)
- LRU eviction that flushes dirty entries before eviction
- Optional async flush with double-buffering for non-blocking writes
- Thread-safe with RLock (recursive lock)

See `CACHE_DESIGN_REVIEW.md` for detailed design analysis.

## Development Commands

### Installation
```bash
# Development install
pip install -e .

# With optional dependencies
pip install -e .[full]    # pandas support + web UI
pip install -e .[web]     # web UI support (Flask)
pip install -e .[test]    # test dependencies
```

### Testing
```bash
# Run all unit tests (excluding stress tests)
python -m pytest tests/unit/ -v -k "not stress"

# Run specific test file
python -m pytest tests/unit/test_unified_cache.py -v

# Run integration tests
python -m pytest tests/integration/ -v

# Run with coverage
pytest tests/unit/ --cov=flaxkv2 --cov-report=html

# Run stress/concurrency tests
python -m pytest tests/stress/ -v
```

### Running the Server
```bash
# Start ZeroMQ server (CLI)
flaxkv2 run --host 0.0.0.0 --port 5555 --data-dir ./data

# Or via Python module
python -m flaxkv2 run --host 0.0.0.0 --port 5555 --data-dir ./data
```

### Using Inspector (Visualization Tool)
```bash
# CLI: List all keys
flaxkv2 inspect keys mydb --path /data

# CLI: View key details
flaxkv2 inspect get mydb user123 --path /data

# CLI: Get statistics
flaxkv2 inspect stats mydb --path /data

# CLI: Search keys
flaxkv2 inspect search mydb "user_.*" --path /data

# CLI: Delete key
flaxkv2 inspect delete mydb temp_key --path /data

# CLI: Set key value
flaxkv2 inspect set mydb name "John" --path /data

# Web UI: Start visualization server
flaxkv2 web mydb --path /data --port 8080

# Remote database inspection
flaxkv2 inspect keys mydb --path 127.0.0.1:5555 --backend remote

# See docs/INSPECTOR.md for detailed documentation
```

### Benchmarks
```bash
# Located in benchmarks/ directory
python benchmarks/unified_cache_benchmark.py
python benchmarks/comprehensive_comparison.py
```

### Docker
```bash
# Build image
make build

# Start container
make start

# View logs
make log

# Remove container
make rm
```

## Code Structure Guidelines

### Key Files by Layer
1. **User Interface**: `flaxkv2/__init__.py` (FlaxKV factory)
2. **Core Implementations**: `flaxkv2/core/{raw_leveldb_dict.py, cached_leveldb_dict.py, nested_structures.py}`
3. **Network Layer**: `flaxkv2/server/zmq_server.py`, `flaxkv2/client/zmq_client.py`
4. **Data Layer**: `flaxkv2/serialization/{encoder.py, decoder.py, value_meta.py}`
5. **Utilities**: `flaxkv2/utils/{unified_cache.py, ttl_cleanup.py, key_manager.py, log.py}`

### When Modifying Cache Logic
- Cache tests: `tests/unit/test_unified_cache.py` (35 tests covering all cache functionality)
- Cache implementation: `flaxkv2/utils/unified_cache.py` (461 lines)
- Integration with LevelDB: `flaxkv2/core/cached_leveldb_dict.py`
- Always verify thread safety and dirty tracking
- Test both sync and async flush modes

### When Modifying Serialization
- Encoder tests: Check existing tests for type support
- Decoder tests: Verify round-trip serialization
- Add new type support in both `encoder.py` and `decoder.py`
- Update `value_meta.py` if TTL metadata format changes

### TTL Implementation
- Metadata stored in `ValueWithMeta` during serialization
- Cache-level expiration check on reads (in UnifiedCache)
- Persistent-level cleanup via background thread (`TTLCleanupThread`)
- Tests: `test_default_ttl.py`, `test_ttl_persistence.py`, `test_raw_leveldb_ttl.py`

## Testing Strategy

### Test Organization
- `tests/unit/`: Unit tests for individual components (147 tests)
- `tests/integration/`: Integration tests for multi-component interactions
- `tests/stress/`: Concurrency and stress tests
- `tests/benchmarks/`: Performance benchmarks

### Critical Test Files
- `test_unified_cache.py`: 35 tests for cache core (LRU, flush, TTL, thread safety)
- `test_cached_write_buffer.py`: Write buffer functionality
- `test_core.py`: Basic DB operations
- `test_nested_list.py`: Nested structure tests
- `test_zmq_remote.py`: Remote client/server tests

### Running Single Tests
```bash
# Run a single test function
pytest tests/unit/test_unified_cache.py::test_basic_put_get -v

# Run a test class
pytest tests/unit/test_core.py::TestFlaxKV -v
```

## Performance Profiles

Choose the right profile in `FlaxKV()` constructor:
- `balanced` (default): 256MB cache, 128MB write buffer - general purpose
- `read_optimized`: 512MB cache, 64MB write buffer - caching, API queries
- `write_optimized`: 128MB cache, 256MB write buffer - logging, batch imports
- `memory_constrained`: 64MB cache, 32MB write buffer - embedded devices
- `large_database`: 1GB cache, 256MB write buffer - databases >100GB
- `ml_workload`: 512MB cache, 512MB write buffer - ML model parameters, arrays

## Important Notes

### Data Safety
- **RawLevelDBDict** (default): Immediate writes, safest option
- **CachedLevelDBDict with sync flush**: Safe, writes are synchronous
- **CachedLevelDBDict with async flush**: Fast but data may be lost on process crash before flush completes
- Always call `db.close()` or use context manager to ensure data is flushed

### Security Considerations
- Uses pickle for complex objects (security risk from untrusted data)
- Remote server has no authentication/encryption (use VPN/SSH tunnel in production)
- Documented in README.md "安全注意事项" section

### Logging
- Library is silent by default (doesn't pollute application logs)
- Enable via `from flaxkv2.utils.log import enable_logging; enable_logging(level="INFO")`
- Or set environment variables: `FLAXKV_ENABLE_LOGGING=1` and `FLAXKV_LOG_LEVEL=DEBUG`

### Deprecated Code
- `LevelDBDict` is deprecated but still available for backwards compatibility
- New code should use `RawLevelDBDict` or `CachedLevelDBDict`
- 13-25% performance improvement over old implementation

## Common Tasks

### Adding a New Test
1. Choose appropriate directory: `tests/unit/`, `tests/integration/`, or `tests/stress/`
2. Follow naming convention: `test_*.py`
3. Use pytest fixtures for test data/cleanup
4. Run the test: `pytest tests/unit/test_newfile.py -v`

### Adding a New Performance Profile
1. Edit `flaxkv2/config.py` → `PerformanceProfiles` class
2. Add new dict with LevelDB parameters
3. Update `PerformanceProfiles.list_profiles()` method
4. Document in README.md

### Debugging Cache Issues
1. Enable logging: `enable_logging(level="DEBUG")`
2. Check cache stats: `db._cache.stats()` (if using CachedLevelDBDict)
3. Monitor flush behavior: Look for "Flushing" log messages
4. Run cache-specific tests: `pytest tests/unit/test_unified_cache.py -v -s`

### Testing Remote Server
1. Start server: `flaxkv2 run --host 127.0.0.1 --port 5555 --data-dir /tmp/test_data`
2. In another terminal: `pytest tests/integration/test_zmq_remote.py -v`
3. Or test manually:
```python
from flaxkv2 import FlaxKV
db = FlaxKV("testdb", "127.0.0.1:5555", backend='remote')
db["key"] = "value"
print(db["key"])
db.close()
```

## Architecture Decisions

### Why Unified Cache (Not Separate Read/Write Caches)?
- Simplicity: Single source of truth, no synchronization needed
- Proven pattern: Write-back cache is used in CPUs, databases (InnoDB buffer pool)
- Natural consistency: No read-write cache coherency issues
- See `CACHE_DESIGN_REVIEW.md` for detailed analysis

### Why Three Backend Implementations?
- **RawLevelDBDict**: For users who prioritize data safety over performance
- **CachedLevelDBDict**: For users who want performance but can accept write buffer risks
- **RemoteDBDict**: For distributed access, multi-process scenarios

### Why Nested Structures?
- Avoid frequent full serialization/deserialization of large dicts/lists
- Store nested keys separately in LevelDB (e.g., `parent_key/child_key`)
- Trade-off: More LevelDB keys but better performance for partial updates
