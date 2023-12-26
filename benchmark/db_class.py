from __future__ import annotations

import shelve
import shutil
import time
from pathlib import Path

from rocksdict import Options, Rdict


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
    if not root_path.exists():
        root_path.mkdir(parents=True, exist_ok=True)

    db_path = str(root_path / "test_shelve")

    def __init__(self):
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
