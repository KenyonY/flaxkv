# from litestar.testing import TestClient
import subprocess
import time

import pytest
import requests
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED

from flaxkv import RemoteDBDict
from flaxkv.serve.app import app


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


def test_api():
    import httpx

    db_name = "test_db"
    backend = "lmdb"
    rebuild = True
    db = RemoteDBDict(
        root_path_or_url="http://localhost:8000",
        db_name=db_name,
        backend=backend,
        rebuild=rebuild,
        # client=TestClient(app=app),
    )
    assert db.keys() == []
    target_dict = dict(
        [
            ("string", "string"),
            ("int", 123),
            (2, 2),
            (2.5, 1 / 3),
            (True, False),
            (b'string', b'string'),
            ((1, 2, 3), [1, 2, 3]),
            ((1, (2, 3)), [1, 2, 3]),
            ((1, (2, 3), (2, 3, (3, 4))), [1, 2, 3]),
            ("test_key", "test_value"),
            ('dict', {'a': 1, 'b': 2}),
            # ('set', {1, 2, 3, '1', '2', '3'}), # do not support currently
            ('list', [1, 2, 3, '1', '2', '3']),
            ('nest_dict', {'a': {'b': 1, 'c': 2, 'd': {'e': 1, 'f': '2', 'g': 3}}}),
        ]
    )
    for key, value in target_dict.items():
        db[key] = value
    for key, value in target_dict.items():
        assert db[key] == value
        print(key, db[key])

    db.write_immediately(wait=True)
    for key, value in target_dict.items():
        assert db[key] == value
        print(key, db[key])
