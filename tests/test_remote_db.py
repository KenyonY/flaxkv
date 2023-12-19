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
    process = subprocess.Popen(["flaxkv", "run"])
    try:
        wait_for_server_to_start(url="http://localhost:8000/healthz")
        yield
    finally:
        process.kill()


@pytest.fixture(
    scope="function",
    params=[
        dict(db_name="test_server_db", backend="lmdb", rebuild=True),
        dict(db_name="test_server_db", backend="leveldb", rebuild=True),
    ],
)
def temp_db(request):
    # from litestar.testing import TestClient
    # from flaxkv.serve.app import app

    db = RemoteDBDict(
        root_path_or_url="http://localhost:8000",
        db_name=request.param["db_name"],
        backend=request.param["backend"],
        rebuild=request.param["rebuild"],
        # client=TestClient(app=app),
    )
    yield db


from test_local_db import (
    test_buffered_writing,
    test_key_checks_and_deletion,
    test_large_value,
    test_list_keys_values_items,
    test_numpy_array,
    test_set_get_write,
    test_setdefault,
    test_update,
)
