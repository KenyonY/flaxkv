from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.testing import TestClient

from flaxkv.serve.app import app
from flaxkv.serve.client import RemoteDictDB


def test_api():
    db_name = "test_db"
    backend = "lmdb"
    rebuild = True
    with TestClient(app=app) as client:
        db = RemoteDictDB(
            url="", db_name=db_name, backend=backend, rebuild=rebuild, client=client
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
