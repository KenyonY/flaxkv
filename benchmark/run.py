from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from db_class import RocksDict, ShelveDict
from rich import print
from sparrow import MeasureTime

from flaxkv import FlaxKV

benchmark_info = {}

N = 10_000


def prepare_data(n):
    d = {}
    for i in range(n):
        d[f'vector-{i}'] = np.random.rand(1000)
    return d


@pytest.fixture(scope="module", autouse=True)
def print_info(request):
    def plot(df: pd.DataFrame):
        import matplotlib.pyplot as plt

        # df.plot(kind="bar", figsize=(10, 7))
        df.reset_index(inplace=True)
        plt.figure(figsize=(10, 6))
        write_color = '#ADD8E6'
        read_color = '#3EB489'
        plt.bar(
            df["index"],
            df["write"],
            width=0.4,
            color=write_color,
            label='Write',
            align='center',
        )
        plt.bar(
            df["index"],
            df["read"],
            width=0.4,
            color=read_color,
            label='Read',
            align='edge',
        )

        plt.title(f"Read and Write (N={N}) 1000-dim vectors")
        plt.xlabel("DB Type")
        plt.ylabel("Time (seconds)")
        plt.yscale('log')
        plt.xticks(rotation=20)
        plt.legend(title="Operation")
        plt.show()

    def print_result():
        df = pd.DataFrame(benchmark_info).T
        df = df.sort_values(by="read", ascending=True)
        print()
        print(df)
        plot(df)

    request.addfinalizer(print_result)


@pytest.fixture(
    params=[
        "dict",
        "flaxkv-LMDB",
        "flaxkv-LevelDB",
        "RocksDict",
        "Shelve",
    ]
)
def temp_db(request):

    if request.param == "flaxkv-LMDB":
        db = FlaxKV('benchmark', backend='lmdb')
    elif request.param == "flaxkv-LevelDB":
        db = FlaxKV('benchmark', backend='leveldb')
    elif request.param == "RocksDict":
        db = RocksDict()
    elif request.param == "Shelve":
        db = ShelveDict()
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
    data = prepare_data(n)
    mt = MeasureTime().start()
    for i, (key, value) in enumerate(data.items()):
        db[key] = value

    write_cost = mt.show_interval(f"{db_name} write")

    keys = list(db.keys())
    # shuffle keys
    import random

    random.shuffle(keys)

    mt.start()
    for key in keys:
        a, b = key, db[key]
    read_cost = mt.show_interval(f"{db_name} read (traverse elements) ")
    print("--------------------------")
    return write_cost, read_cost


def test_benchmark(temp_db):
    db, db_name = temp_db
    write_cost, read_cost = benchmark(db, db_name=db_name, n=N)
    benchmark_info[db_name] = {"write": write_cost, "read": read_cost}
