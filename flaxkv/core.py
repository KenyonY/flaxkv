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


from __future__ import annotations

import atexit
import threading
import time
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Type, TypeVar

import numpy as np
from loguru import logger

from .decorators import class_measure_time
from .helper import SimpleQueue
from .log import setting_log
from .manager import DBManager, RemoteTransaction
from .pack import check_pandas_type, decode, decode_key, encode

if TYPE_CHECKING:
    from httpx import Response
    from litestar.exceptions import HTTPException


class BaseDBDict(ABC):
    MAX_BUFFER_SIZE = 100  # unit: number of keys
    COMMIT_TIME_INTERVAL = 10 * 60  # unit: second
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

    def __init__(
        self,
        db_type,
        root_path_or_url: str,
        db_name: str,
        rebuild=False,
        raw=False,
        cache=False,
        **kwargs,
    ):
        """
        Initializes the BaseDBDict class which provides a dictionary-like interface to a database.

        Args:
            db_type (str): Type of the database ("lmdb" or "leveldb" or "remote").
            root_path_or_url (str): Root path or URL of the database.
            rebuild (bool, optional): Whether to recreate the database. Defaults to False.
            raw (bool): Only used by the server.
        """
        log_level = kwargs.pop('log', None)
        stdout = kwargs.pop("stdout", True)
        if log_level and stdout:
            log_configs = setting_log(
                level="DEBUG" if log_level is True else log_level,
                stdout=stdout,
                save_file=kwargs.pop('save_log', False),
            )
            try:
                logger.remove(0)
            except Exception:
                pass
            log_ids = [logger.add(**log_conf) for log_conf in log_configs]
            self._logger = logger.bind(flaxkv=True)

        else:
            logger.disable('flaxkv')

        self._db_manager = DBManager(
            db_type=db_type,
            root_path_or_url=root_path_or_url,
            db_name=db_name,
            rebuild=rebuild,
            **kwargs,
        )
        self._db_name = self._db_manager.db_name
        self._raw = raw
        self._cache_all_db = cache
        self._register_auto_close()
        self._init()

    def _init(self):
        self._static_view = self._db_manager.new_static_view()

        self.buffer_dict = {}
        self._stat_buffer_num = 0
        self.delete_buffer_set = set()

        self._buffered_count = 0
        self._buffer_lock = threading.Lock()

        self._stop_event = threading.Event()

        self._last_set_time = None

        self._write_complete = SimpleQueue(maxsize=1)
        self._write_event = threading.Event()
        self._latest_write_num = 0
        self._write_queue = SimpleQueue(maxsize=1)
        self._thread_running = True

        self._thread = threading.Thread(target=self._background_worker)
        self._thread.daemon = True

        self._thread_write_monitor = threading.Thread(target=self._write_monitor)
        self._thread_write_monitor.daemon = True

        # Start the background worker
        self._start()

        self._cache_dict = {}  # DB data that marked_delete has been deleted
        if self._cache_all_db:
            # load from db
            self._pull_db_data_to_cache()

    def _register_auto_close(self, func=None):
        if func is None:
            atexit.register(self.close)
        else:
            atexit.register(func)

    def _unregister_auto_close(self, func=None):
        if func is None:
            atexit.unregister(self.close)
        else:
            atexit.unregister(func)

    def _start(self):
        """
        Starts the background worker thread.
        """
        self._thread_running = True
        self._thread.start()
        self._thread_write_monitor.start()

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
                elif check_pandas_type(value):
                    if not value.equals(b[key]):
                        result[key] = value
                else:
                    if value != b[key]:
                        result[key] = value
        return result

    def _write_monitor(self):
        self._logger.info("Write monitor started")
        while not self._stop_event.is_set():
            time.sleep(0.2)
            if self._last_set_time is not None:
                if (time.time() - self._last_set_time) >= 0.6:
                    self._logger.debug("Write monitor triggered")
                    self.write_immediately()

    def _background_worker(self):
        """
        Background worker function to periodically write buffer to the database.
        """
        while self._thread_running or not self._write_queue.empty():

            self._write_event.wait(timeout=self.COMMIT_TIME_INTERVAL)
            self._write_event.clear()

            if not self._write_queue.empty():
                is_write = self._write_queue.get()
                if is_write is False:
                    self._write_complete.put(True)
                    break

            self._write_complete.clear()

            try:
                self._write_buffer_to_db(current_write_num=self._latest_write_num)

            except:
                # todo:
                self._logger.warning(f"Write buffer to db failed. error")
                traceback.print_exc()

            self._write_complete.put(True)

    def write_immediately(self, write=True, block=False):
        """
        Triggers an immediate write of the buffer to the database.
        """
        self._last_set_time = None
        self._latest_write_num += 1
        self._write_queue.put(write)
        self._write_event.set()
        if block:
            self._write_complete.clear()
            self._write_complete.get(block=True)

    def wait_until_write_complete(self, timeout=None):
        """
        Waits until the background worker thread has finished writing the buffer to the database.
        """
        self._write_complete.get(block=True, timeout=timeout)

    def _close_background_worker(self, write=True, block=False):
        """
        Stops the background worker thread.
        """
        self._stop_event.set()
        self._latest_write_num += 1

        self._thread_running = False

        self.write_immediately(write=write, block=block)

        self._thread.join(timeout=15)
        self._thread_write_monitor.join(timeout=3)
        if self._thread.is_alive():
            self._logger.warning(
                "Warning: Background thread did not finish in time. Some data might not be saved."
            )

    def _encode_key(self, key):
        if self._raw:
            return key
        else:
            return encode(key)

    def _encode_value(self, value):
        if self._raw:
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
                self.delete_buffer_set.discard(key)
                self.buffer_dict[key] = default
                return default

            if key in self.buffer_dict:
                return self.buffer_dict[key]

            if self._cache_all_db:
                return self._cache_dict.get(key, default)

            _encode_key = self._encode_key(key)
            value = self._static_view.get(_encode_key)

            if value is None:
                self.buffer_dict[key] = default
                return default

            v = value if self._raw else decode(value)
            self.buffer_dict[key] = v
            return v

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

            if self._cache_all_db:
                value = self._cache_dict.get(key)
            else:
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
            self._stat_buffer_num = len(self.buffer_dict)

            self._buffered_count += 1
        self._last_set_time = time.time()
        # Trigger immediate write if buffer size exceeds MAX_BUFFER_SIZE
        if self._buffered_count >= self.MAX_BUFFER_SIZE:
            self._logger.debug("Trigger immediate write")
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
                if self._raw:
                    key, value = encode(key), encode(value)
                self.buffer_dict[key] = value
                self.delete_buffer_set.discard(key)

            self._stat_buffer_num = len(self.buffer_dict)
            self._buffered_count += len(d)

        self._last_set_time = time.time()
        # Trigger immediate write if buffer size exceeds MAX_BUFFER_SIZE
        if self._buffered_count >= self.MAX_BUFFER_SIZE:
            self._logger.debug("Trigger immediate write")
            self._buffered_count = 0
            self.write_immediately()

    def from_dict(self, d: dict, clear=False):
        """
        Updates the buffer with the given dictionary.

        Args:
            d (dict): A dictionary of key-value pairs to update.
        """
        if clear:
            self.clear(wait=True)
        self.update(d)

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
                # ensure atomicity (shallow copy)
                buffer_dict_snapshot = self.buffer_dict.copy()
                delete_buffer_set_snapshot = self.delete_buffer_set.copy()
                cache_dict = self._cache_dict.copy()
        # ensure atomicity
        with self._db_manager.write() as wb:
            try:
                for key in delete_buffer_set_snapshot:
                    # delete from db
                    key = self._encode_key(key)
                    wb.delete(key)
                for key, value in buffer_dict_snapshot.items():
                    # set key, value to cache
                    if self._cache_all_db:
                        cache_dict[key] = value

                    # set key, value to db
                    key, value = self._encode_key(key), self._encode_value(value)
                    wb.put(key, value)

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
            self._cache_dict = cache_dict

            self._db_manager.close_static_view(self._static_view)
            self._static_view = self._db_manager.new_static_view()
            self._logger.info(
                f"write {self._db_manager.db_type.upper()} buffer to db successfully! "
                f"current_num={current_write_num} latest_num={self._latest_write_num}"
            )
            self._stat_buffer_num = len(self.buffer_dict)

    def __iter__(self):
        """
        Returns an iterator over the keys.
        """
        return self.keys()

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
                self._last_set_time = time.time()
                if key in self.buffer_dict:
                    del self.buffer_dict[key]
                    # If it is in the buffer (possibly obtained through get), then _stat_buffer_num -= 1,
                    # and _stat_buffer_num can be negative
                    self._stat_buffer_num -= 1
                    return
                else:
                    if self._cache_all_db:
                        self._cache_dict.pop(key)
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
                self._last_set_time = time.time()
                if key in self.buffer_dict:
                    value = self.buffer_dict.pop(key)
                    self._stat_buffer_num -= 1
                    if self._raw:
                        return decode(value)
                    else:
                        return value
                else:
                    if self._cache_all_db:
                        value = self._cache_dict.pop(key)
                    else:
                        key = self._encode_key(key)
                        value = decode(self._static_view.get(key))
                    return value
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

            if self._cache_all_db:
                return key in self._cache_dict

            key = self._encode_key(key)
            return (
                self._static_view.get(key) is not None
            )  # self._static_view.get() return a binary value or None

    def clear(self, wait=True):
        """
        Clears the database and resets the buffer.
        """

        self.close(write=False, wait=wait)

        self._db_manager.rebuild_db()
        self._init()

    def destroy(self):
        """
        Destroys the database by closing and deleting it.
        """
        self.close(write=False)
        self._unregister_auto_close()
        self._db_manager.destroy()
        self._logger.info(f"Destroyed database successfully.")

    def __del__(self):
        """
        Destructor for the BaseDBDict class. Closes the database before object deletion.
        """
        self.close(write=True)

    def __repr__(self):
        return str(self.db_dict())

    def __len__(self):
        return self.stat()['count']

    def close(self, write=True, wait=False):
        """
        Closes the database and stops the background worker.

        Args:
            write (bool, optional): Whether to write the buffer to the database before closing. Defaults to True.
            wait (bool, optional): Whether to wait for the background worker to finish. Defaults to False.
        """
        self._close_background_worker(write=write, block=wait)

        self._db_manager.close_static_view(self._static_view)
        self._db_manager.close()
        self._logger.info(f"Closed ({self._db_manager.db_type.upper()}) successfully")

    def _get_status_info(
        self,
        return_key=False,
        return_value=False,
        return_buffer_dict=False,
        return_view=True,
        decode_raw=True,
    ):
        static_view = None
        buffer_keys_set, buffer_values_list = None, None

        # shallow copy buffer data
        with self._buffer_lock:
            if return_view:
                static_view = self._db_manager.new_static_view()
            buffer_dict = self.buffer_dict.copy()
            delete_buffer_set = self.delete_buffer_set.copy()

        if self._raw and decode_raw:
            delete_buffer_set = {decode_key(i) for i in delete_buffer_set}

        if return_key:
            if self._raw and decode_raw:
                buffer_keys_set = {decode_key(i) for i in buffer_dict.keys()}
            else:
                buffer_keys_set = set(buffer_dict.keys())
        if return_value:
            if self._raw and decode_raw:
                buffer_values_list = [decode(i) for i in buffer_dict.values()]
            else:
                buffer_values_list = list(self.buffer_dict.values())
        if not return_buffer_dict:
            buffer_dict = None
        else:
            if self._raw and decode_raw:
                buffer_dict = {decode_key(k): decode(v) for k, v in buffer_dict.items()}

        return (
            buffer_dict,
            buffer_keys_set,
            buffer_values_list,
            delete_buffer_set,
            static_view,
        )

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

    def keys(self, decode_raw=True):
        """
        Retrieves all the keys in the database and buffer.

        Returns:
            list: A list of keys.
        """
        (
            buffer_dict,
            buffer_keys,
            buffer_values,
            delete_buffer_set,
            view,
        ) = self._get_status_info(
            return_key=True,
            return_view=False if self._cache_all_db else True,
            decode_raw=decode_raw,
        )

        for key in buffer_keys:
            yield key

        if self._cache_all_db:
            # `view` is None
            for key in self._cache_dict.keys():
                if key not in delete_buffer_set and key not in buffer_keys:
                    yield key
        else:
            for key in self._iter_db_view(view, include_value=False):
                d_key = decode_key(key)
                if d_key not in delete_buffer_set and key not in buffer_keys:
                    yield d_key
            self._db_manager.close_static_view(view)

    def to_dict(self, decode_raw=True):
        """
        Retrieves all the key-value pairs in the database and buffer.
        Returns: dict
        """
        return self.db_dict(decode_raw=decode_raw)

    def db_dict(self, decode_raw=True):
        """
        Retrieves all the key-value pairs in the database and buffer.
        Returns: dict
        """
        (
            buffer_dict,
            buffer_keys,
            buffer_values,
            delete_buffer_set,
            view,
        ) = self._get_status_info(
            return_buffer_dict=True,
            return_view=False if self._cache_all_db else True,
            decode_raw=decode_raw,
        )

        if self._cache_all_db:
            _db_dict = self._cache_dict.copy()
        else:
            _db_dict = {}
            for key, value in self._iter_db_view(view):
                dk = decode_key(key)
                if dk not in delete_buffer_set:
                    _db_dict[dk] = decode(value)

        if _db_dict:
            _db_dict.update(buffer_dict)
        else:
            _db_dict = buffer_dict

        self._db_manager.close_static_view(view)
        return _db_dict

    def items(self, decode_raw=True):
        """
        Retrieves all the key-value pairs in the database and buffer.

        Returns:
            list: A list of key-value pairs
        """
        (
            buffer_dict,
            buffer_keys,
            buffer_values,
            delete_buffer_set,
            view,
        ) = self._get_status_info(
            return_key=True,
            return_buffer_dict=True,
            return_view=False if self._cache_all_db else True,
            decode_raw=decode_raw,
        )
        for key, value in buffer_dict.items():
            if key not in delete_buffer_set:
                yield key, value

        if self._cache_all_db:
            for (
                key,
                value,
            ) in self._cache_dict.items():  # Attention: dict.items() is a dynamic view
                if key not in delete_buffer_set and key not in buffer_keys:
                    yield key, value
        else:
            for key, value in self._iter_db_view(view):
                # for key, value in view.iterator():
                dk = decode_key(key)
                if dk not in delete_buffer_set and key not in buffer_keys:
                    yield dk, decode(value)
            self._db_manager.close_static_view(view)

    def _pull_db_data_to_cache(self, decode_raw=True):
        """
        Retrieves all the key-value pairs in the database.
        Load db data to self._cache_dict
        """
        (
            buffer_dict,
            buffer_keys,
            buffer_values,
            delete_buffer_set,
            view,
        ) = self._get_status_info(return_view=True, decode_raw=decode_raw)
        for key, value in self._iter_db_view(view):
            dk = decode_key(key)
            if dk not in delete_buffer_set:
                self._cache_dict[dk] = decode(value)

        self._db_manager.close_static_view(view)

    @abstractmethod
    def stat(self, *args, **kwargs):
        """
        Database statistics

        Returns:
            dict: A dictionary containing the number of 'db', 'buffer' and 'count' in the database.
        """

    @abstractmethod
    def _iter_db_view(self, view, include_key=True, include_value=True):
        """
        Iterates over the items in the database view.
        """


class LMDBDict(BaseDBDict):
    """
    A dictionary-like class that stores key-value pairs in an LMDB database.
    Type:
        key: int, float, bool, str, tuple
        value: int, float, bool, str, list, dict, and np.ndarray,
    """

    _instances = {}

    def __new__(cls, db_name: str, root_path: str, rebuild=False, **kwargs):
        name = db_name + str(root_path)
        if name not in cls._instances:
            cls._instances[name] = super().__new__(cls)
        return cls._instances[name]

    def __init__(
        self,
        db_name: str,
        root_path: str = './',
        map_size=1024**3,
        rebuild=False,
        **kwargs,
    ):
        if not hasattr(self, '_initialized'):
            super().__init__(
                "lmdb",
                root_path,
                db_name,
                max_dbs=1,
                map_size=map_size,
                rebuild=rebuild,
                **kwargs,
            )
            self._initialized = True

    def _iter_db_view(self, view, include_key=True, include_value=True):
        """
        Iterates over the items in the database view.

        Args:
            view: The database view to iterate over.
        """

        cursor = view.cursor()
        if include_key and include_value:
            for key, value in cursor.iternext(keys=include_key, values=include_value):
                yield key, value
        else:
            for key_or_value in cursor.iternext(keys=include_key, values=include_value):
                yield key_or_value

    def set_mapsize(self, map_size):
        """Change the maximum size of the map file.
        This function will fail if any transactions are active in the current process.
        """
        try:
            self._db_manager.env.set_mapsize(map_size)
        except Exception as e:
            self._logger.error(f"Error setting map size: {e}")

    def stat(self):
        if self._cache_all_db:
            db_count = len(self._cache_dict)
            count = db_count + self._stat_buffer_num
            return {
                'count': count,
                'buffer': self._stat_buffer_num,
                'db': db_count,
                'marked_delete': len(self.delete_buffer_set),
                "type": 'lmdb',
            }
        else:
            env = self._db_manager.get_env()
            stats = env.stat()
            db_count = stats['entries']
            count = db_count + self._stat_buffer_num - len(self.delete_buffer_set)
            return {
                'count': count,
                'buffer': self._stat_buffer_num,
                'db': db_count,
                'marked_delete': len(self.delete_buffer_set),
                "type": 'lmdb',
            }


class LevelDBDict(BaseDBDict):
    """
    A dictionary-like class that stores key-value pairs in a LevelDB database.
    Type:
        key: int, float, bool, str, tuple
        value: int, float, bool, str, list, dict and np.ndarray,
    """

    _instances = {}

    def __new__(cls, db_name: str, root_path: str, rebuild=False, **kwargs):
        name = db_name + str(root_path)
        if name not in cls._instances:
            cls._instances[name] = super().__new__(cls)
        return cls._instances[name]

    def __init__(self, db_name: str, root_path: str, rebuild=False, **kwargs):
        if not hasattr(self, '_initialized'):
            super().__init__(
                "leveldb",
                root_path_or_url=root_path,
                db_name=db_name,
                rebuild=rebuild,
                **kwargs,
            )

            self._initialized = True

    def _iter_db_view(self, view, include_key=True, include_value=True):
        """
        Iterates over the items in the database view.

        Args:
            view: The database view to iterate over.
        """
        if include_key and include_value:
            for key, value in view.iterator(
                include_key=include_key, include_value=include_value
            ):
                yield key, value
        else:
            for key_or_value in view.iterator(
                include_key=include_key, include_value=include_value
            ):
                yield key_or_value

    def stat(self):

        if self._cache_all_db:
            db_keys = set(self._cache_dict.keys())
            db_count = len(db_keys)
            count = db_count + self._stat_buffer_num
            return {
                'count': count,
                'buffer': self._stat_buffer_num,
                'db': db_count,
                'marked_delete': len(self.delete_buffer_set),
                "type": 'leveldb',
            }
        else:
            with self._buffer_lock:
                view = self._db_manager.new_static_view()

            db_keys = {key for key in view.iterator(include_value=False)}
            view.close()

        db_count = len(db_keys)
        # db_valid_keys = db_keys - self.delete_buffer_set
        # buffer_keys = set(self.buffer_dict.keys())
        # intersection_count = len(buffer_keys.intersection(db_valid_keys))
        # count = len(db_valid_keys) + self._stat_buffer_num - intersection_count
        count = db_count + self._stat_buffer_num - len(self.delete_buffer_set)

        # db_valid_keys = db_keys.union(buffer_keys) - self.delete_buffer_set
        # count = len(db_valid_keys)
        return {
            'count': count,
            'buffer': self._stat_buffer_num,
            "db": db_count,
            'marked_delete': len(self.delete_buffer_set),
            'type': 'leveldb',
        }


class RemoteDBDict(BaseDBDict):
    """
    A dictionary-like class that stores key-value pairs in a Remote database.
    Type:
        key: int, float, bool, str, tuple
        value: int, float, bool, str, list, dict and np.ndarray,
    """

    MAX_BUFFER_SIZE = 100
    COMMIT_TIME_INTERVAL = 60

    def __init__(
        self,
        root_path_or_url: str,
        db_name: str,
        rebuild=False,
        backend='leveldb',
        **kwargs,
    ):
        self._start_event = threading.Event()

        super().__init__(
            "remote",
            root_path_or_url=root_path_or_url,
            db_name=db_name,
            backend=backend,
            rebuild=rebuild,
            **kwargs,
        )
        self._start_event.wait()

    def _start(self):
        """
        Starts the background worker thread.
        """

        self._thread_sync_notify = threading.Thread(target=self._attach_db)
        self._thread_sync_notify.daemon = True
        self._thread_sync_notify.start()

        self._thread_running = True
        self._thread.start()
        self._thread_write_monitor.start()

    def _attach_db(self):
        def set_cache(data):
            if data.type == "buffer_dict":
                buffer_dict = data.data
                for raw_key, raw_value in buffer_dict.items():
                    self._cache_dict[decode_key(raw_key)] = decode(raw_value)
            elif data.type == "delete_keys":
                for raw_key in data.data.keys():
                    self._cache_dict.pop(decode_key(raw_key))
            else:
                raise ValueError(f"Unknown data type: {data['type']}")

        view: RemoteTransaction = self._db_manager.new_static_view()

        for data in view.attach_db(self._start_event):
            if data is None:
                break
            set_cache(data)

    def _pull_db_data_to_cache(self, decode_raw=True):

        self._start_event.wait()

        (
            buffer_dict,
            buffer_keys,
            buffer_values,
            delete_buffer_set,
            view,
        ) = self._get_status_info(return_view=True, decode_raw=decode_raw)

        view: RemoteTransaction
        view.check_db_exist()
        with view.client.stream("GET", f"/dict_stream?db_name={self._db_name}") as r:
            buffer = bytearray()
            for data in r.iter_bytes():
                buffer.extend(data)

        remote_db_dict = decode(bytes(buffer))
        for dk, dv in remote_db_dict.items():
            if dk not in delete_buffer_set:
                self._cache_dict[dk] = dv

    def _iter_db_view(self, view, include_key=True, include_value=True):
        """
        Just a placeholder, now we don't use it.
        """

    def keys(self, fetch_all=True, decode_raw=True):
        (
            buffer_dict,
            buffer_keys,
            buffer_values,
            delete_buffer_set,
            view,
        ) = self._get_status_info(
            return_key=True,
            return_view=False if self._cache_all_db else True,
            decode_raw=decode_raw,
        )

        for key in buffer_keys:
            if key not in delete_buffer_set:
                yield key

        if self._cache_all_db:
            for key in self._cache_dict.keys():
                if key not in delete_buffer_set and key not in buffer_keys:
                    yield key

        else:
            if fetch_all:
                with view.client.stream(
                    "GET", f"/keys_stream?db_name={self._db_name}"
                ) as r:
                    buffer = bytearray()
                    for data in r.iter_bytes():
                        buffer.extend(data)

                db_keys = set(decode_key(bytes(buffer)))
                for key in db_keys - delete_buffer_set - buffer_keys:
                    yield key

            else:
                raise NotImplementedError

    def items(self, fetch_all=True, decode_raw=True):
        if fetch_all:
            return self.db_dict(decode_raw=decode_raw).items()
        else:
            raise NotImplementedError

    def db_dict(self, decode_raw=True):
        (
            buffer_dict,
            buffer_keys,
            buffer_values,
            delete_buffer_set,
            view,
        ) = self._get_status_info(
            return_buffer_dict=True,
            return_view=True,
            decode_raw=decode_raw,
        )

        if self._cache_all_db:
            _db_dict = self._cache_dict.copy()
        else:
            _db_dict = {}
            with view.client.stream(
                "GET", f"/dict_stream?db_name={self._db_name}"
            ) as r:
                buffer = bytearray()
                for data in r.iter_bytes():
                    buffer.extend(data)

            remote_db_dict = decode(bytes(buffer))
            for dk, dv in remote_db_dict.items():
                if dk not in delete_buffer_set:
                    _db_dict[dk] = dv

        if _db_dict:
            _db_dict.update(buffer_dict)
        else:
            _db_dict = buffer_dict

        return _db_dict

    def stat(self):
        if self._cache_all_db:
            db_count = len(self._cache_dict)
            buffer_num = self._stat_buffer_num
            count = db_count + buffer_num
        else:
            # fixme:
            env = self._db_manager.get_env()
            stats = env.stat()
            db_count = stats['count']
            buffer_num = self._stat_buffer_num
            count = db_count + buffer_num - len(self.delete_buffer_set)

        return {
            'count': count,
            'buffer': buffer_num,
            'db': db_count,
            'marked_delete': len(self.delete_buffer_set),
            'type': 'remote',
        }

    def __repr__(self):
        if self._cache_all_db:
            return str(self._cache_dict)
        return str({"keys": self.stat()['count']})

    def clear(self, wait=True):
        raise NotImplementedError

    def destroy(self):
        raise NotImplementedError

    def close(self, write=True, wait=False):
        """
        Closes the database and stops the background worker.

        Args:
            write (bool, optional): Whether to write the buffer to the database before closing. Defaults to True.
            wait (bool, optional): Whether to wait for the background worker to finish. Defaults to False.
        """
        self._close_background_worker(write=write, block=wait)

        self._db_manager.close_static_view(self._static_view)
        self._db_manager.close()
        self._db_manager.env.close_connection()
        self._logger.info(f"Closed ({self._db_manager.db_type.upper()}) successfully")
