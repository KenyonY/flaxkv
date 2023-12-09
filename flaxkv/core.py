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

import atexit
import threading
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import numpy as np
from loguru import logger

from .decorators import class_measure_time
from .helper import SimpleQueue
from .log import setting_log
from .manager import DBManager
from .pack import decode, decode_key, encode


class BaseDBDict(ABC):
    MAX_BUFFER_SIZE = 100
    _COMMIT_TIME_INTERVAL = 60 * 60 * 12
    _logger = logger

    # Unused
    @dataclass
    class _enc_prefix:
        str = b's'
        int = b'i'
        float = b'f'
        bool = b'b'
        list = b'l'
        tuple = b't'
        dict = b'd'
        array = b'a'

    def __init__(self, db_type, path, rebuild=False, raw=False, db_name=None, **kwargs):
        """
        Initializes the BaseDBDict class which provides a dictionary-like interface to a database.

        Args:
            db_type (str): Type of the database ("lmdb" or "leveldb").
            path (str): Path to the database.
            rebuild (bool, optional): Whether to recreate the database. Defaults to False.
            raw (bool): Only used by the server.
        """
        log_level = kwargs.pop('log', None)
        if log_level:
            log_configs = setting_log(
                level="DEBUG" if log_level is True else log_level,
                stdout=kwargs.pop("stdout", False),
                save_file=kwargs.pop('save_log', False),
            )
            log_ids = [logger.add(**log_conf) for log_conf in log_configs]
            self._logger = logger.bind(flaxkv=True)

        else:
            logger.disable('flaxkv')

        self._db_name = db_name
        self._db_manager = DBManager(
            db_type=db_type, db_path=path, rebuild=rebuild, **kwargs
        )
        self.raw = raw
        self._static_view = self._db_manager.new_static_view()

        self._buffered_count = 0
        self.buffer_dict = {}
        self.delete_buffer_set = set()
        self._buffer_lock = threading.Lock()

        self._write_complete = SimpleQueue(maxsize=1)
        self._write_event = threading.Event()
        self._latest_write_num = 0
        self._write_queue = SimpleQueue(maxsize=1)
        self._thread_running = True

        self._thread = threading.Thread(target=self._background_worker)
        self._thread.daemon = True

        atexit.register(self.close)

        # Start the background worker
        self._start()

    def _start(self):
        """
        Starts the background worker thread.
        """
        self._thread_running = True
        self._thread.start()

    @staticmethod
    def _diff_buffer(a: dict, b: dict):
        """
        Computes the difference between two buffers.
        Find a dictionary containing key-value pairs that exist in `a` but not in `b`.
        Args:
            a (dict): The latest buffer.
            b (dict): The older buffer.

        Q: Why don't you need to worry about the key-value pair existing in 'b' but not in 'a'?
        A: Because the absence of the key in 'a' indicates that the value has been deleted by the user,
           and this information will be stored in the 'deleted_set'.


        Returns:
            dict: A dictionary containing key-value pairs that exist in `a` but not in `b`.
        """
        result = {}
        for key, value in a.items():
            if key not in b:
                result[key] = value
            else:
                if type(value) is not type(b[key]):
                    continue
                if isinstance(value, np.ndarray):
                    if not np.array_equal(value, b[key]):
                        result[key] = value
                else:
                    if value != b[key]:
                        result[key] = value
        return result

    def _background_worker(self):
        """
        Background worker function to periodically write buffer to the database.
        """
        while self._thread_running or not self._write_queue.empty():

            self._write_event.wait(timeout=self._COMMIT_TIME_INTERVAL)
            self._write_event.clear()

            if not self._write_queue.empty():
                if self._write_queue.get() is False:
                    break

            self._write_complete.clear()

            try:
                self._write_buffer_to_db(current_write_num=self._latest_write_num)

            except:
                # todo:
                self._logger.warning(f"Write buffer to db failed. error")

            self._write_complete.put(True)

    def write_immediately(self, write=True, wait=False):
        """
        Triggers an immediate write of the buffer to the database.
        """
        self._write_queue.put(write)
        self._write_event.set()
        if wait:
            self._write_complete.clear()
            self._write_complete.get(block=True)

    def _close_background_worker(self, write=True):
        """
        Stops the background worker thread.
        """
        self._latest_write_num += 1

        self._thread_running = False
        self.write_immediately(write=write)

        self._thread.join(timeout=60)
        if self._thread.is_alive():
            self._logger.warning(
                "Warning: Background thread did not finish in time. Some data might not be saved."
            )

    def _encode_key(self, key):
        if self.raw:
            return key
        else:
            return encode(key)

    def _encode_value(self, value):
        if self.raw:
            return value
        else:
            return encode(value)

    def get(self, key: Any, default=None):
        """
        Retrieves the value associated with the given key.

        Args:
            key (Any): The key to retrieve.
            default: The default value to set if the key does not exist.

        Returns:
            value: The value associated with the key, or None if the key is not found.
        """
        with self._buffer_lock:
            if key in self.delete_buffer_set:
                return default

            if key in self.buffer_dict:
                return self.buffer_dict[key]

            key = self._encode_key(key)
            value = self._static_view.get(key)

            if value is None:
                return default

            return value if self.raw else decode(value)

    def get_db_value(self, key: str):
        """
        Directly retrieves the encoded value associated with the given key from the database.

        Args:
            key (str): The key to retrieve.

        Returns:
            value: The encoded value associated with the key.
        """
        key = self._encode_key(key)
        return self._static_view.get(key)

    def get_batch(self, keys):
        """
        Retrieves values for a batch of keys.

        Args:
            keys (list): A list of keys to retrieve.

        Returns:
            list: A list of values corresponding to the given keys.
        """
        values = []
        for key in keys:
            if self.delete_buffer_set and key in self.delete_buffer_set:
                values.append(None)
                continue
            if key in self.buffer_dict:
                values.append(self.buffer_dict[key])
                continue
            key = self._encode_key(key)
            value = self._static_view.get(key)
            if value is not None:
                value = decode(value)
            values.append(value)
        return values

    def _set(self, key, value):
        """
        Sets the value for a given key in the buffer.

        Args:
            key: The key to set.
            value: The value to associate with the key.
        """
        with self._buffer_lock:
            self.buffer_dict[key] = value
            self.delete_buffer_set.discard(key)

            self._buffered_count += 1
            # Trigger immediate write if buffer size exceeds MAX_BUFFER_SIZE
            if self._buffered_count >= self.MAX_BUFFER_SIZE:
                self._logger.debug("Trigger immediate write")
                self._latest_write_num += 1
                self._buffered_count = 0
                self.write_immediately()

    def setdefault(self, key, default=None):
        """
        Retrieves the value for a given key. If the key does not exist, sets it to the default value.

        Args:
            key (Any): The key to retrieve.
            default: The default value to set if the key does not exist.

        Returns:
            value: The value associated with the key.
        """
        value = self.get(key)
        if value is None:
            self._set(key, default)
            return default

        return value

    def update(self, d: dict):
        """
        Updates the buffer with the given dictionary.

        Args:
            d (dict): A dictionary of key-value pairs to update.
        """
        if not isinstance(d, dict):
            raise ValueError("Input must be a dictionary.")
        with self._buffer_lock:
            for key, value in d.items():
                if self.raw:
                    key, value = encode(key), encode(value)
                self.buffer_dict[key] = value
                self.delete_buffer_set.discard(key)

            self._buffered_count += 1
            # Trigger immediate write if buffer size exceeds MAX_BUFFER_SIZE
            if self._buffered_count >= self.MAX_BUFFER_SIZE:
                print("Trigger immediate write")
                self._latest_write_num += 1
                self._buffered_count = 0
                self.write_immediately()

    # @class_measure_time()
    def _write_buffer_to_db(
        self,
        current_write_num: int,
    ):
        """
        Writes the current buffer to the database.

        Args:
            current_write_num (int): The current write operation number.
        """
        with self._buffer_lock:
            self._logger.debug(f"Trigger write")
            self._logger.debug(f"{current_write_num=}")
            if not (self.buffer_dict or self.delete_buffer_set):
                self._logger.debug(
                    f"buffer is empty and delete_buffer_set is empty: {self._latest_write_num=} {current_write_num=}"
                )
                return
            else:
                # ensure atomicity
                buffer_dict_snapshot = self.buffer_dict.copy()
                delete_buffer_set_snapshot = self.delete_buffer_set.copy()
        # ensure atomicity
        with self._db_manager.write() as wb:
            try:
                for key, value in buffer_dict_snapshot.items():
                    key, value = self._encode_key(key), self._encode_value(value)
                    wb.put(key, value)
                for key in delete_buffer_set_snapshot:
                    key = self._encode_key(key)
                    wb.delete(key)

            except Exception as e:
                traceback.print_exc()
                self._logger.error(
                    f"Error writing to {self._db_manager.db_type}: {e}\n"
                    f"data will rollback"
                )
                raise

        with self._buffer_lock:
            self.delete_buffer_set = self.delete_buffer_set - delete_buffer_set_snapshot
            self.buffer_dict = self._diff_buffer(self.buffer_dict, buffer_dict_snapshot)

            self._db_manager.close_static_view(self._static_view)
            self._static_view = self._db_manager.new_static_view()
            self._logger.info(
                f"write {self._db_manager.db_type.upper()} buffer to db successfully-{current_write_num=}-{self._latest_write_num=}"
            )

    def __iter__(self):
        """
        Returns an iterator over the keys.
        """
        return iter(self.keys())

    def __getitem__(self, key):
        """
        Retrieves the value for a given key using the dictionary access syntax.

        Args:
            key: The key to retrieve.

        Returns:
            value: The value associated with the key.
        """

        value = self.get(key, b'iamnone')
        if isinstance(value, bytes) and value == b'iamnone':
            raise KeyError(f"Key `{key}` not found in the database.")
        return value

    def __setitem__(self, key, value):
        """
        Sets the value for a given key using the dictionary access syntax.

        Args:
            key: The key to set.
            value: The value to associate with the key.
        """
        self._set(key, value)

    def __delitem__(self, key):
        """
        Deletes a key-value pair using the dictionary access syntax.

        Args:
            key: The key to delete.
        """
        if key in self:
            with self._buffer_lock:
                self.delete_buffer_set.add(key)
                self._buffered_count += 1
                if key in self.buffer_dict:
                    del self.buffer_dict[key]
                    return
        else:
            raise KeyError(f"Key `{key}` not found in the database.")

    def pop(self, key, default=None):
        """
        Removes the key-value pair and returns the value.

        Args:
            key: The key to pop.
            default: The default value to return if the key does not exist.

        Returns:
            value: The value associated with the key, or the default value.
        """
        if key in self:
            with self._buffer_lock:
                self.delete_buffer_set.add(key)
                self._buffered_count += 1
                if key in self.buffer_dict:
                    value = self.buffer_dict.pop(key)
                    if self.raw:
                        return decode(value)
                    else:
                        return value
                else:
                    key = self._encode_key(key)
                    value = self._static_view.get(key)
                    return decode(value)
        else:
            return default

    def __contains__(self, key):
        """
        Checks if a key exists in the buffer or database.

        Args:
            key: The key to check.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        with self._buffer_lock:
            if key in self.buffer_dict:
                return True
            if key in self.delete_buffer_set:
                return False
            key = self._encode_key(key)
            return self._static_view.get(key) is not None

    def clear(self):
        """
        Clears the database and resets the buffer.
        """
        with self._buffer_lock:
            self._db_manager.close_static_view(self._static_view)
            self._db_manager.close()
            self._close_background_worker(write=False)

        self._db_manager.clear()
        self._static_view = self._db_manager.new_static_view()

        self._buffered_count = 0
        self.buffer_dict = {}
        self.delete_buffer_set = set()
        self._buffer_lock = threading.Lock()

        self._write_event = threading.Event()
        self._latest_write_num = 0
        self._thread_running = True
        self._thread = threading.Thread(target=self._background_worker)
        self._thread.daemon = True

        self._start()

    def destroy(self):
        """
        Destroys the database by closing and deleting it.
        """
        self.close(write=False)
        self._db_manager.destroy()
        self._logger.info(f"Destroyed database successfully.")

    def __del__(self):
        """
        Destructor for the BaseDBDict class. Closes the database before object deletion.
        """
        self.close(write=True)

    def __repr__(self):
        return str(dict(self.items()))

    def __len__(self):
        return self.stat()['count']

    def close(self, write=True):
        """
        Closes the database and stops the background worker.

        Args:
            write (bool, optional): Whether to write the buffer to the database before closing. Defaults to True.
        """
        self._close_background_worker(write=write)

        self._db_manager.close_static_view(self._static_view)
        self._db_manager.close()
        self._logger.info(f"Closed ({self._db_manager.db_type.upper()}) successfully")

    def _get_state_buffer_info(
        self, return_key=False, return_value=False, return_dict=False, decode_raw=True
    ):
        with self._buffer_lock:
            static_view = self._db_manager.new_static_view()
            buffer_dict = self.buffer_dict.copy()
            delete_buffer_set = self.delete_buffer_set.copy()

        buffer_keys, buffer_values = None, None
        if return_key:
            if self.raw and decode_raw:
                buffer_keys = set([decode_key(i) for i in buffer_dict.keys()])
            else:
                buffer_keys = set(buffer_dict.keys())
        if return_value:
            if self.raw and decode_raw:
                buffer_values = list([decode(i) for i in buffer_dict.values()])
            else:
                buffer_values = list(self.buffer_dict.values())
        if not return_dict:
            buffer_dict = None
        else:
            if self.raw and decode_raw:
                buffer_dict = {decode_key(k): decode(v) for k, v in buffer_dict.items()}

        return buffer_dict, buffer_keys, buffer_values, delete_buffer_set, static_view

    def values(self, decode_raw=True):
        """
        Retrieves all the values in the database and buffer.

        Returns:
            list: A list of values
        """
        values_list = []
        for key, value in self.items(decode_raw):
            values_list.append(value)
        return values_list

    @abstractmethod
    def keys(self, *args, **kwargs):
        """
        Retrieves all the keys in the database and buffer.

        Returns:
            list: A list of keys.
        """

    @abstractmethod
    def items(self, *args, **kwargs):
        """
        Retrieves all the key-value pairs in the database and buffer.

        Returns:
            list: A list of key-value pairs
        """

    @abstractmethod
    def stat(self, *args, **kwargs):
        """
        Database statistics

        Returns:
            dict: A dictionary containing the number of 'db', 'buffer' and 'count' in the database.
        """


class LMDBDict(BaseDBDict):
    """
    A dictionary-like class that stores key-value pairs in an LMDB database.
    Type:
        key: int, float, bool, str, tuple
        value: int, float, bool, str, list, dict, and np.ndarray,
    """

    def __init__(self, path: str, map_size=1024**3, rebuild=False, **kwargs):
        super().__init__(
            "lmdb", path, max_dbs=1, map_size=map_size, rebuild=rebuild, **kwargs
        )

    def keys(self, decode_raw=True):
        (
            buffer_dict,
            buffer_keys,
            buffer_values,
            delete_buffer_set,
            session,
        ) = self._get_state_buffer_info(return_key=True, decode_raw=decode_raw)
        cursor = session.cursor()

        lmdb_keys = set(
            decode_key(key) for key in cursor.iternext(keys=True, values=False)
        )
        self._db_manager.close_static_view(session)

        return list(lmdb_keys.union(buffer_keys) - delete_buffer_set)

    def db_dict(self, decode_raw=True):
        (
            buffer_dict,
            buffer_keys,
            buffer_values,
            delete_buffer_set,
            session,
        ) = self._get_state_buffer_info(return_dict=True, decode_raw=decode_raw)
        cursor = session.cursor()
        _db_dict = {}

        for key, value in cursor.iternext(keys=True, values=True):
            dk = key if self.raw else decode_key(key)
            if dk not in delete_buffer_set:
                if self.raw:
                    dk = decode_key(dk)
                _db_dict[dk] = decode(value)

        _db_dict.update(buffer_dict)

        self._db_manager.close_static_view(session)

        return _db_dict

    def items(self, decode_raw=True):
        _db_dict = self.db_dict(decode_raw=decode_raw)
        return _db_dict.items()

    def set_mapsize(self, map_size):
        """Change the maximum size of the map file.
        This function will fail if any transactions are active in the current process.
        """
        try:
            self._db_manager.env.set_mapsize(map_size)
        except Exception as e:
            self._logger.error(f"Error setting map size: {e}")

    def stat(self):
        env = self._db_manager.get_env()
        stats = env.stat()
        db_count = stats['entries']
        buffer_count = len(self.buffer_dict.keys())
        count = db_count + buffer_count
        return {
            'count': count,
            'buffer': buffer_count,
            'db': db_count,
            'marked_delete': len(self.delete_buffer_set),
        }


class LevelDBDict(BaseDBDict):
    """
    A dictionary-like class that stores key-value pairs in a LevelDB database.
    Type:
        key: int, float, bool, str, tuple
        value: int, float, bool, str, list, dict and np.ndarray,
    """

    def __init__(self, path: str, rebuild=False, **kwargs):
        super().__init__("leveldb", path=path, rebuild=rebuild)

    def keys(self, decode_raw=True):
        (
            buffer_dict,
            buffer_keys,
            buffer_values,
            delete_buffer_set,
            snapshot,
        ) = self._get_state_buffer_info(return_key=True, decode_raw=decode_raw)

        db_keys = set(decode_key(key) for key, _ in snapshot.iterator())
        snapshot.close()

        return list(db_keys.union(buffer_keys) - delete_buffer_set)

    def db_dict(self, decode_raw=True):
        (
            buffer_dict,
            buffer_keys,
            buffer_values,
            delete_buffer_set,
            snapshot,
        ) = self._get_state_buffer_info(return_dict=True, decode_raw=decode_raw)

        _db_dict = {}
        for key, value in snapshot.iterator():
            dk = key if self.raw else decode_key(key)
            if dk not in delete_buffer_set:
                if self.raw:
                    dk = decode_key(dk)
                _db_dict[dk] = decode(value)

        _db_dict.update(buffer_dict)

        self._db_manager.close_static_view(snapshot)
        return _db_dict

    def items(self, decode_raw=True):
        _db_dict = self.db_dict(decode_raw=decode_raw)
        return _db_dict.items()

    def stat(self):
        with self._buffer_lock:
            snapshot = self._db_manager.new_static_view()

        db_keys = set([key for key, _ in snapshot.iterator()])
        snapshot.close()

        db_count = len(db_keys)

        buffer_keys = set(self.buffer_dict.keys())

        db_valid_keys = db_keys - self.delete_buffer_set
        intersection_count = len(buffer_keys.intersection(db_valid_keys))
        buffer_count = len(buffer_keys)
        count = len(db_valid_keys) + buffer_count - intersection_count

        return {'count': count, 'buffer': buffer_count, "db": db_count}
