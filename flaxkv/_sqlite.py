import shutil
import sqlite3

from .pack import decode, decode_key, encode


class SQLiteDict:
    def __init__(self, filename=None):
        self._filename = filename
        self._conn = self._connect(filename)
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

    @staticmethod
    def close_conn_and_cursor(conn, cursor):
        if conn:
            conn.close()
        if cursor:
            cursor.close()

    def close(self):
        if self._conn:
            self._conn.commit()  # Commit any pending changes
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._conn:
            self._conn.close()
            self._conn = None

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
        self.clear()
        self.close()
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
