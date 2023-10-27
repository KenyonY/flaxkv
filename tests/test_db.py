import os
import tempfile
import time

import numpy as np
import pytest

try:
    import lmdb

    from flaxkv import LMDBDict

except ImportError:
    LMDBDict = None

try:
    import plyvel

    from flaxkv import LevelDBDict
except ImportError:
    LevelDBDict = None

DB_DICTS = [db for db in (LMDBDict, LevelDBDict) if db is not None]

# If DB_DICTS is empty，skip the test
if not DB_DICTS:
    pytestmark = pytest.mark.skip(reason="No database modules available")


@pytest.fixture(params=DB_DICTS)
def temp_db(request):
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_db")
    db: LMDBDict = request.param(db_path, recreate=True)

    yield db
    db.destroy()


def test_set_get_write(temp_db):
    if isinstance(temp_db, type(None)):
        pytest.skip("Skipping")

    items = [
        ("string", "string"),
        ("int", 123),
        (2, 2),
        (2.5, 1 / 3),
        (True, False),
        (b'string', b'string'),
        ((1, 2, 3), [1, 2, 3]),
        ("test_key", "test_value"),
        ('dict', {'a': 1, 'b': 2}),
        # ('set', {1, 2, 3, '1', '2', '3'}), # do not support currently
        ('list', [1, 2, 3, '1', '2', '3']),
        ('nest_dict', {'a': {'b': 1, 'c': 2, 'd': {'e': 1, 'f': '2', 'g': 3}}}),
    ]
    temp_db.MAX_BUFFER_SIZE = 1000
    for key, value in items:
        temp_db[key] = value
    for key, value in items:
        assert temp_db[key] == value

    assert temp_db.stat()['db'] == 0
    assert temp_db.stat()['buffer'] == len(items)
    assert temp_db.stat()['count'] == len(items)

    temp_db.write_immediately()
    time.sleep(0.1)
    for key, value in items:
        assert temp_db[key] == value

    assert temp_db.stat()['db'] == len(items)
    assert temp_db.stat()['count'] == len(items)
    assert temp_db.stat()['buffer'] == 0


def test_numpy_array(temp_db):
    if isinstance(temp_db, type(None)):
        pytest.skip("Skipping")

    target_dict = {
        'array1': np.array([1, 2, 3]),
        'array2': np.random.rand(100),
        'array3': np.random.rand(100, 100),
    }

    for key, value in target_dict.items():
        temp_db[key] = value

    for key, value in target_dict.items():
        assert np.array_equal(temp_db[key], value)
    temp_db.write_immediately()
    time.sleep(0.01)

    assert temp_db.stat()['db'] == len(target_dict)
    assert temp_db.stat()['buffer'] == 0

    for key, value in target_dict.items():
        assert np.array_equal(temp_db[key], value)


def test_setdefault(temp_db):
    if isinstance(temp_db, type(None)):
        pytest.skip("Skipping")
    temp_db.setdefault("test_key", "test_value")
    temp_db.setdefault("test_key", "another_value0")
    assert temp_db.get("test_key") == "test_value"
    time.sleep(0.01)
    temp_db.setdefault("test_key", "another_value1")

    temp_db['test_key'] = "another_value"
    assert temp_db.get("test_key") == "another_value"
    time.sleep(0.01)
    assert temp_db.get("test_key") == "another_value"


def test_update(temp_db):
    if isinstance(temp_db, type(None)):
        pytest.skip("Skipping")
    temp_db.update({"test_key": "test_value"})
    assert temp_db.get("test_key") == "test_value"

    temp_db.write_immediately()
    time.sleep(0.01)

    temp_db.update({"test_key": "another_value0"})
    temp_db.write_immediately()
    time.sleep(0.01)
    assert temp_db.get("test_key") == "another_value0"

    temp_db.update({"test_key": "another_value1"})
    temp_db.write_immediately()
    time.sleep(0.01)
    assert temp_db.get("test_key") == "another_value1"


def test_buffered_writing(temp_db):
    if isinstance(temp_db, type(None)):
        pytest.skip("Skipping")
    temp_db.MAX_BUFFER_SIZE = 10
    extra_len = 6
    data_len = temp_db.MAX_BUFFER_SIZE + extra_len
    for i in range(data_len):
        temp_db[f"key_{i}"] = f"value_{i}"
        time.sleep(0.01)

    assert len(temp_db) == data_len
    assert len(temp_db.buffer_dict) == extra_len
    assert temp_db.stat()['db'] == data_len - extra_len


def test_key_checks_and_deletion(temp_db):
    if isinstance(temp_db, type(None)):
        pytest.skip("Skipping")
    target_dict = {"key1": "value1", "key2": "value2", "key3": "value3"}
    temp_db.update(target_dict)
    temp_db.write_immediately()

    assert "key1" in temp_db
    time.sleep(0.01)
    assert "key1" in temp_db

    del temp_db["key1"]
    temp_db.write_immediately()
    assert "key1" not in temp_db
    time.sleep(0.01)
    assert "key1" not in temp_db

    value = temp_db.pop("key2")
    assert value == "value2"

    assert "key2" not in temp_db
    temp_db.write_immediately()
    time.sleep(0.01)
    assert "key2" not in temp_db

    assert len(temp_db) == 1


def test_list_keys_values_items(temp_db):
    if isinstance(temp_db, type(None)):
        pytest.skip("Skipping")
    data = {"key1": "value1", "key2": "value2", "key3": "value3"}

    for key, value in data.items():
        temp_db[key] = value
    assert set(temp_db.keys()) == set(data.keys())
    assert set(temp_db.values()) == set(data.values())
    assert set(temp_db.items()) == set(data.items())

    temp_db.write_immediately()
    time.sleep(0.01)

    assert set(temp_db.keys()) == set(data.keys())
    assert set(temp_db.values()) == set(data.values())
    assert set(temp_db.items()) == set(data.items())
