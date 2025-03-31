from __future__ import annotations

import shelve
import shutil
from pathlib import Path
import sqlite3
import tempfile
import os

from rocksdict import Options, Rdict

try:
    from flaxkv.pack import decode, encode
except ImportError:
    # Fallback encode/decode functions if flaxkv is not available
    import pickle
    def encode(obj):
        return pickle.dumps(obj)
    def decode(data):
        return pickle.loads(data) if data else None

# Import FlaxKV2 classes
try:
    from flaxkv2.core.leveldb_dict import LevelDBDict
    from flaxkv2.client.remote import RemoteDBDict
except ImportError:
    pass


class RocksDict:
    rdict_path = "./test_rocksdict"

    def __init__(self):
        self.db = Rdict(self.rdict_path)

    def __setitem__(self, key, value):
        self.db[key] = value

    def __getitem__(self, key):
        return self.db[key]

    def __delitem__(self, key):
        del self.db[key]

    def keys(self):
        return self.db.keys()

    def items(self):
        return self.db.items()

    def __contains__(self, key):
        return key in self.db

    def __iter__(self):
        return iter(self.db)

    def destroy(self):
        self.db.close()
        Rdict.destroy(self.rdict_path)


class ShelveDict:
    root_path = Path("./shelve_db")
    db_path = str(root_path / "test_shelve")

    def __init__(self):
        if not self.root_path.exists():
            self.root_path.mkdir(parents=True, exist_ok=True)
        self.sd = shelve.open(self.db_path)

    def __getitem__(self, key):
        return self.sd[key]

    def __setitem__(self, key, value):
        self.sd[key] = value

    def __delitem__(self, key):
        del self.sd[key]

    def __contains__(self, key):
        return key in self.sd

    def __iter__(self):
        return iter(self.sd)

    def __len__(self):
        return len(self.sd)

    def keys(self):
        return self.sd.keys()

    def items(self):
        return self.sd.items()

    def destroy(self):
        self.sd.close()
        shutil.rmtree(self.root_path)


class RedisDict:
    def __init__(self):
        import redis

        self.client = redis.Redis(host="localhost", port=6379, db=0)

    def __getitem__(self, key):
        value = self.client.get(key)
        return decode(value)

    def __setitem__(self, key, value):
        self.client.set(key, encode(value))

    def __contains__(self, item):
        return self.client.exists(item)

    def __len__(self):
        return self.client.dbsize()

    def keys(self):
        return self.client.keys('*')

    def items(self):
        keys = self.keys()
        # values = self.client.mget(keys)
        # for key, value in zip(keys, values):
        #     yield key, decode(value)
        for key in keys:
            yield key, self[key]

    def destroy(self):
        self.client.flushdb()
        self.client.close()


class SQLiteDict:
    def __init__(self):
        self.db_path = "./sqlite_db.db"
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value BLOB)")
        self.conn.commit()
    
    def __getitem__(self, key):
        self.cursor.execute("SELECT value FROM kv WHERE key = ?", (key,))
        result = self.cursor.fetchone()
        if result is None:
            raise KeyError(key)
        return decode(result[0])
    
    def __setitem__(self, key, value):
        encoded_value = encode(value)
        self.cursor.execute("INSERT OR REPLACE INTO kv VALUES (?, ?)", (key, encoded_value))
        self.conn.commit()
    
    def __delitem__(self, key):
        if key not in self:
            raise KeyError(key)
        self.cursor.execute("DELETE FROM kv WHERE key = ?", (key,))
        self.conn.commit()
    
    def __contains__(self, key):
        self.cursor.execute("SELECT 1 FROM kv WHERE key = ?", (key,))
        return self.cursor.fetchone() is not None
    
    def keys(self):
        self.cursor.execute("SELECT key FROM kv")
        return [row[0] for row in self.cursor.fetchall()]
    
    def items(self):
        self.cursor.execute("SELECT key, value FROM kv")
        for key, value in self.cursor.fetchall():
            yield key, decode(value)
    
    def destroy(self):
        self.conn.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)


class FlaxKV2LevelDB:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = str(Path(self.temp_dir) / "leveldb_test")
        self.db = LevelDBDict(self.db_path)
    
    def __getitem__(self, key):
        result = self.db[key]
        return result
    
    def __setitem__(self, key, value):
        self.db[key] = value
    
    def __delitem__(self, key):
        del self.db[key]
    
    def __contains__(self, key):
        return key in self.db
    
    def keys(self):
        return self.db.keys()
    
    def items(self):
        return self.db.items()
    
    def destroy(self):
        self.db.close()
        shutil.rmtree(self.temp_dir)


class FlaxKV2Remote:
    def __init__(self, server_url="http://localhost:8000"):
        self.server_url = server_url
        # Use a standard database name for benchmarks
        self.db_name = "benchmark_db"
        self.db = RemoteDBDict(
            db_name=self.db_name,
            url=self.server_url
        )
    
    def __getitem__(self, key):
        result = self.db[key]
        return result
    
    def __setitem__(self, key, value):
        self.db[key] = value
    
    def __delitem__(self, key):
        del self.db[key]
    
    def __contains__(self, key):
        return key in self.db
    
    def keys(self):
        return self.db.keys()
    
    def items(self):
        return self.db.items()
    
    def destroy(self):
        self.db.close()
