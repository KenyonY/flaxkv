from __future__ import annotations

import random
import shutil
import subprocess
import time
import os
import socket
from pathlib import Path
import tempfile
from contextlib import contextmanager

import numpy as np
import pandas as pd
import pytest
from rich import print
from sparrow import MeasureTime  # pip install sparrow-python
from flaxkv2.core.leveldb_dict import LevelDBDict

from dbclass import (
    RedisDict, 
    RocksDict, 
    ShelveDict, 
    SQLiteDict, 
    FlaxKV2LevelDB, 
    FlaxKV2Remote
)
from helpers import plot, wait_for_server_to_start

try:
    # Try to import FlaxKV if available (for backward compatibility)
    from flaxkv import FlaxKV
except ImportError:
    FlaxKV = None

benchmark_info = {}

# Configuration
N = 1500                # Number of items to write/read
VECTOR_DIM = 1000       # Dimension of random vectors
BENCHMARK_REPEAT = 3    # Number of times to repeat each benchmark for averaging
SERVER_PORT = 8000      # Port for FlaxKV2 server


def prepare_data(n, key_only=False):
    """Generate test data as key-value pairs or keys only."""
    for i in range(n):
        if key_only:
            yield f'vector-{i}'
        else:
            # Use a list instead of numpy array for JSON compatibility
            random_data = np.random.rand(VECTOR_DIM).tolist()
            yield (f'vector-{i}', random_data)


def find_free_port():
    """Find a free port to use for the server."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


@contextmanager
def start_flaxkv2_server(port=SERVER_PORT):
    """Start a FlaxKV2 server for remote testing."""
    # Create a temporary directory for the server database
    temp_dir = tempfile.mkdtemp()
    db_path = str(Path(temp_dir) / "server_db")
    
    # Set environment variables for the server
    env = os.environ.copy()
    env["FLAXKV_ROOT_PATH"] = db_path
    
    # Start the server process
    command = ["flaxkv2", "run", "--port", str(port)]
    process = subprocess.Popen(command, env=env)
    
    try:
        # Wait for the server to be ready
        server_url = f"http://localhost:{port}"
        wait_for_server_to_start(url=f"{server_url}/healthz")
        yield server_url
    finally:
        # Clean up
        process.kill()
        process.wait()
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="session", autouse=True)
def startup_and_shutdown(request):
    """Setup and teardown for the benchmark session."""
    # Clean up any previous benchmark data
    for path in ["benchmark_data", "sqlite_db.db", "test_rocksdict", "shelve_db"]:
        if os.path.exists(path):
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            else:
                os.remove(path)
    
    # For compatibility with old FlaxKV
    if FlaxKV is not None:
        process = subprocess.Popen(["flaxkv", "run", "--log", "warning", "--port", "8001"])
        try:
            wait_for_server_to_start(url="http://localhost:8001/healthz")
            print("FlaxKV server started")
            yield
        finally:
            process.kill()
    else:
        yield
    
    def process_result():
        # Clean up benchmark data
        for path in ["benchmark_data", "sqlite_db.db", "test_rocksdict", "shelve_db"]:
            if os.path.exists(path):
                if os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    os.remove(path)
        
        # Plot and display results
        if benchmark_info:
            df = pd.DataFrame(benchmark_info).T
            df = df.sort_values(by="write", ascending=True)
            print("\n", df)
            title = f"Read and Write Performance - {N} items with {VECTOR_DIM}-dim vectors"
            plot(df, title, log=True)
    
    request.addfinalizer(process_result)


@pytest.fixture(
    params=[
        "dict",
        # "SQLite",
        "RocksDict", 
        # "Shelve",
        # "FlaxKV2-Remote",
        "FlaxKV2-LevelDB",
    ]
)
def temp_db(request):
    """Create a temporary database for testing."""
    db_type = request.param
    
    if db_type == "FlaxKV2-LevelDB":
        db = FlaxKV2LevelDB()
    elif db_type == "FlaxKV2-Remote":
        port = find_free_port()
        with start_flaxkv2_server(port=port) as server_url:
            db = FlaxKV2Remote(server_url=server_url)
            yield db, db_type
            try:
                db.destroy()
            except:
                pass
            return
    elif db_type == "RocksDict":
        db = RocksDict()
    elif db_type == "Shelve":
        db = ShelveDict()
    elif db_type == "SQLite":
        db = SQLiteDict()
    elif db_type == "Redis":
        db = RedisDict()
    elif db_type == "dict":
        db = {}
    else:
        raise ValueError(f"Unknown database type: {db_type}")
    
    yield db, db_type
    
    try:
        if hasattr(db, 'destroy'):
            db.destroy()
    except:
        pass


def benchmark_operation(db, operation, data, description):
    """Benchmark a specific operation and return the execution time."""
    mt = MeasureTime().start()
    operation(db, data)
    return float(mt.show_interval(description))


def write_operation(db, data):
    """Write data to the database."""
    db: LevelDBDict
    for key, value in data.items():
        db[key] = value

    # Ensure data is persisted for database backends that need it
    if hasattr(db, 'flush') and callable(db.flush):
        db.flush()


def read_keys_operation(db, _):
    """Read all keys from the database."""
    keys = []
    for key in db.keys():
        keys.append(key)
    return keys


def read_values_operation(db, keys):
    """Read all values for the given keys from the database."""
    for key in keys:
        _ = db[key]


def benchmark(db, db_name, n=200):
    """Run the benchmark suite for a given database."""
    print(f"\n--- Benchmarking {db_name} ---")
    
    # Prepare test data
    data = dict(prepare_data(n))
    
    # Run write benchmark
    write_times = []
    for i in range(BENCHMARK_REPEAT):
        # Clear the database if possible before each write benchmark
        if hasattr(db, 'clear') and callable(db.clear):
            db.clear()
        
        write_time = benchmark_operation(
            db, write_operation, data, f"{db_name} write (run {i+1}/{BENCHMARK_REPEAT})"
        )
        write_times.append(write_time)
    
    avg_write_time = sum(write_times) / len(write_times)
    print(f"{db_name} average write time: {avg_write_time:.4f}s")
    
    # Run read keys benchmark
    keys_time = benchmark_operation(
        db, read_keys_operation, None, f"{db_name} read (keys only)"
    )
    
    # Get keys for reading values
    keys = list(db.keys())
    
    # Run read values benchmark
    read_times = []
    for i in range(BENCHMARK_REPEAT):
        read_time = benchmark_operation(
            db, read_values_operation, keys, f"{db_name} read values (run {i+1}/{BENCHMARK_REPEAT})"
        )
        read_times.append(read_time)
    
    avg_read_time = sum(read_times) / len(read_times)
    print(f"{db_name} average read time: {avg_read_time:.4f}s")
    
    return avg_write_time, avg_read_time


def test_benchmark(temp_db):
    """Test function that runs the benchmark for each database type."""
    db, db_name = temp_db
    write_cost, read_cost = benchmark(db, db_name=db_name, n=N)
    benchmark_info[db_name] = {"write": write_cost, "read": read_cost}


if __name__ == "__main__":
    """Run the benchmark directly without pytest if needed."""
    pytest.main(["-xvs", __file__])
