# Copyright (c) 2023 K.Y. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import shutil
import sqlite3

from .pack import decode, decode_key, encode


class SQLiteDict:
    from pathlib import Path

    root_path = Path("./SQLiteDB")
    if not root_path.exists():
        root_path.mkdir(parents=True, exist_ok=True)

    def __init__(self, filename=None):

        self._filename = str(self.root_path / filename)
        self._conn = self._connect(self._filename)
        self._cursor = self._conn.cursor()
        self._cursor.execute(
            "CREATE TABLE IF NOT EXISTS kv (key BLOB PRIMARY KEY, value BLOB)"
        )

    def _connect(self, filename):
        if filename:
            return sqlite3.connect(filename)
        else:
            return sqlite3.connect(':memory:')

    def _commit(self):
        self._conn.commit()

    def __setitem__(self, key, value):
        key = encode(key)
        value = encode(value)
        self._cursor.execute("REPLACE INTO kv (key, value) VALUES (?, ?)", (key, value))
        self._conn.commit()

    def __getitem__(self, key):
        key = encode(key)
        self._cursor.execute("SELECT value FROM kv WHERE key=?", (key,))
        result = self._cursor.fetchone()
        if result:
            return decode(result[0])
        raise KeyError(f"'{key}' not found in database")

    def __delitem__(self, key):
        key = encode(key)
        if key not in self:
            raise KeyError(f"'{key}' not found in database")
        self._cursor.execute("DELETE FROM kv WHERE key=?", (key,))

    def __contains__(self, key):
        key = encode(key)
        self._cursor.execute("SELECT 1 FROM kv WHERE key=?", (key,))
        return bool(self._cursor.fetchone())

    def __len__(self):
        self._cursor.execute("SELECT COUNT(*) FROM kv")
        return self._cursor.fetchone()[0]

    def keys(self):
        self._cursor.execute("SELECT key FROM kv")
        return [decode_key(row[0]) for row in self._cursor.fetchall()]

    def values(self):
        self._cursor.execute("SELECT value FROM kv")
        return [decode(row[0]) for row in self._cursor.fetchall()]

    def items(self):
        self._cursor.execute("SELECT key, value FROM kv")
        return [(decode_key(row[0]), decode(row[1])) for row in self._cursor.fetchall()]

    def __iter__(self):
        return iter(self.keys())

    def close_conn_and_cursor(self):
        try:
            self._conn.close()
        except:
            ...
        try:
            self._cursor.close()
        except:
            ...

    def close(self):
        self.close_conn_and_cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self._conn.rollback()
            return False
        else:
            self._commit()
            self.close()

    def __del__(self):
        self.close_conn_and_cursor()
        if self._filename:
            shutil.rmtree(self._filename, ignore_errors=True)

    def clear(self):
        """Remove all items from the database."""
        self._cursor.execute("DELETE FROM kv")
        self._conn.commit()

    def update(self, E):
        if not isinstance(E, dict):
            raise ValueError("Input must be a dictionary.")

        # Start a transaction
        self._cursor.execute("BEGIN TRANSACTION")
        try:
            # Convert the dictionary items to a list of serialized items
            serialized_items = [
                (encode(key), encode(value)) for key, value in E.items()
            ]
            # Use executemany to efficiently insert multiple rows
            self._cursor.executemany(
                "REPLACE INTO kv (key, value) VALUES (?, ?)", serialized_items
            )
            self._conn.commit()  # Commit the transaction
        except:
            self._conn.rollback()  # Rollback in case of any exception
            raise  # Re-raise the exception

    def __repr__(self):
        return str(dict(self.items()))

    def destroy(self):
        self.__del__()
