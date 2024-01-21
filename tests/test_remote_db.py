from __future__ import annotations

import subprocess
import time

import pytest

from flaxkv import RemoteDBDict


def wait_for_server_to_start(url, timeout=10):
    import httpx

    start_time = time.time()
    while True:
        try:
            response = httpx.get(url)
            response.raise_for_status()
            break
        except Exception:
            time.sleep(0.5)
            if time.time() - start_time > timeout:
                raise RuntimeError("Server didn't start in time")


@pytest.fixture(scope="session", autouse=True)
def start_server():
    process = subprocess.Popen(["flaxkv", "run", "--log", "info"])
    try:
        wait_for_server_to_start(url="http://localhost:8000/healthz")
        yield
    finally:
        process.kill()


@pytest.fixture(
    scope="function",
    params=[
        # dict(db_name="test_server_db", backend="lmdb", rebuild=True, cache=False),
        dict(db_name="test_server_db", backend="leveldb", rebuild=True, cache=False),
        # dict(db_name="test_server_db", backend="lmdb", rebuild=True, cache=True),
        dict(db_name="test_server_db", backend="leveldb", rebuild=True, cache=True),
    ],
)
def temp_db(request):
    # from litestar.testing import TestClient
    # from flaxkv.serve.app import app

    time.sleep(2)
    db = RemoteDBDict(
        root_path_or_url="http://localhost:8000",
        **request.param,
        # client=TestClient(app=app),
    )
    assert len(db) == 0
    yield db
    db.close(wait=True)


from test_local_db import (  # test_large_value,; test_list_keys_values_items,; test_set_get_write,; test_setdefault,; test_update,
    test_buffered_writing,
    test_key_checks_and_deletion,
    test_numpy_array,
)
