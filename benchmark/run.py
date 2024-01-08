from __future__ import annotations

import random
import shutil
import subprocess
import time

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


def prepare_data(n, key_only=False):
    import numpy as np

    for i in range(n):
        if key_only:
            yield f'vector-{i}'
        else:
            # yield (f'vector-{i}', f"{i}")
            yield (f'vector-{i}', np.random.rand(1000))


@pytest.fixture(scope="session", autouse=True)
def startup_and_shutdown(request):
    process = subprocess.Popen(["flaxkv", "run"])
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
        title = f"Read and Write ({N=}) 1000-dim vectors"
        plot(df, title, log=True)

    request.addfinalizer(process_result)


@pytest.fixture(
    params=[
        "dict",
        "Redis",
        "RocksDict",
        # "Shelve",
        # "Sqlite3",
        # "flaxkv-LMDB",
        "flaxkv-LevelDB",
        "flaxkv-REMOTE",
    ]
)
def temp_db(request):
    if request.param == "flaxkv-LMDB":
        db = FlaxKV('benchmark', backend='lmdb')
    elif request.param == "flaxkv-LevelDB":
        db = FlaxKV('benchmark', backend='leveldb')
    elif request.param == "flaxkv-REMOTE":
        db = FlaxKV('benchmark', "http://localhost:8000")
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
        db.write_immediately(block=True)
    write_cost = mt.show_interval(f"{db_name} write")

    mt.start()
    for key in db.keys():
        ...
    mt.show_interval(f"{db_name} read (keys only)")
    idx = 0
    for key, value in db.items():
        idx += 1
        # print(key, value)
        # assert value == db[key]
        # if idx >= 1000:
        #     break
    read_cost = mt.show_interval(f"{db_name} read (traverse elements) ")
    print("--------------------------")
    return write_cost, read_cost


def test_benchmark(temp_db):
    db, db_name = temp_db
    write_cost, read_cost = benchmark(db, db_name=db_name, n=N)
    benchmark_info[db_name] = {"write": write_cost, "read": read_cost}
