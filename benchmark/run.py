from __future__ import annotations

import random
import shutil
import subprocess

import numpy as np
import pandas as pd
import pytest
from dbclass import RedisDict, RocksDict, ShelveDict
from helpers import plot, wait_for_server_to_start
from rich import print
from sparrow import MeasureTime  # pip install sparrow-python

from flaxkv import FlaxKV
from flaxkv._sqlite import SQLiteDict
from flaxkv.core import BaseDBDict

benchmark_info = {}

N = 1000
dim = 1000


def prepare_data(n, key_only=False):
    global large_df

    for i in range(n):
        if key_only:
            yield f'vector-{i}'
        else:
            yield (f'vector-{i}', np.random.rand(dim))
            # yield (f'vector-{i}', f"{i}")
            # yield (f'vector-{i}', large_df)


def gen_large_df():
    global large_df
    num_rows = 100_000
    num_cols = 10
    data = {
        f'col{i}': random.sample(range(num_rows), num_rows) for i in range(num_cols)
    }
    large_df = pd.DataFrame(data)


@pytest.fixture(scope="session", autouse=True)
def startup_and_shutdown(request):
    # gen_large_df()

    process = subprocess.Popen(["flaxkv", "run", "--log", "warning", "--port", "8000"])
    try:
        wait_for_server_to_start(url="http://localhost:8000/healthz")
        yield

    finally:
        process.kill()

    def process_result():
        shutil.rmtree("FLAXKV_DB", ignore_errors=True)
        shutil.rmtree("SQLiteDB", ignore_errors=True)
        df = pd.DataFrame(benchmark_info).T
        df = df.sort_values(by="write", ascending=True)
        print("\n", df)
        title = f"Read and Write ({N=}) {dim}-dim vectors"
        plot(df, title, log=True)

    request.addfinalizer(process_result)


@pytest.fixture(
    params=[
        "dict",
        # "Redis",
        "RocksDict",
        "Shelve",
        # "Sqlite3",
        # "flaxkv-LMDB",
        "flaxkv-LevelDB",
        "flaxkv-REMOTE",
    ]
)
def temp_db(request):
    cache = False
    if request.param == "flaxkv-LMDB":
        db = FlaxKV('benchmark', backend='lmdb', cache=cache)
    elif request.param == "flaxkv-LevelDB":
        db = FlaxKV('benchmark', backend='leveldb', cache=cache)
    elif request.param == "flaxkv-REMOTE":
        db = FlaxKV('benchmark', "http://localhost:8000", cache=cache)
    elif request.param == "RocksDict":
        db = RocksDict()
    elif request.param == "Shelve":
        db = ShelveDict()
    elif request.param == "Redis":
        db = RedisDict()
    elif request.param == "Sqlite3":
        db = SQLiteDict('benchmark.db')
    elif request.param == "dict":
        db = {}
    else:
        raise
    yield db, request.param
    try:
        db.destroy()
    except:
        ...


def benchmark(db, db_name, n=200):
    print("\n--------------------------")
    idx = 0
    data = dict(prepare_data(n))
    mt = MeasureTime().start()
    for key, value in data.items():
        idx += 1
        db[key] = value

    if isinstance(db, BaseDBDict):
        db.write_immediately()
    write_cost = float(mt.show_interval(f"{db_name} write"))

    if isinstance(db, BaseDBDict):
        db.write_immediately(block=True)

    mt.start()
    keys = []
    for key in db.keys():
        keys.append(key)
    mt.show_interval(f"{db_name} read (keys only)")

    for key in keys:
        value = db[key]
    read_cost = float(mt.show_interval(f"{db_name} read (traverse elements) "))
    print("--------------------------")
    return write_cost, read_cost


def test_benchmark(temp_db):
    db, db_name = temp_db
    write_cost, read_cost = benchmark(db, db_name=db_name, n=N)
    benchmark_info[db_name] = {"write": write_cost, "read": read_cost}
