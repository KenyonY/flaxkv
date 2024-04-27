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

import io
import os
import re
import shutil
import threading
import time
import traceback
from pathlib import Path
from typing import Dict, Literal
from uuid import uuid4

import msgspec
from loguru import logger

from .decorators import retry
from .pack import decode, decode_key, encode


class StructUpdateData(msgspec.Struct):
    type: str
    data: Dict[bytes, bytes]
    time: float


class DBManager:
    def __init__(
        self, db_type: str, root_path_or_url: str, db_name: str, rebuild=False, **kwargs
    ):
        """
        Initializes the database manager.

        Args:
            db_type (str): Type of the database ("lmdb", "leveldb", "remote").
            root_path_or_url (str): Root path or URL of the database.
            db_name (str): Name of the database.
            rebuild (bool, optional): Whether to create a new database. Defaults to False.
        """
        self.db_type = db_type.lower()
        self.db_name = db_name
        self._rebuild = rebuild

        url_pattern = re.compile(r'^(http://|https://|ftp://)')
        if url_pattern.match(root_path_or_url):
            self.db_address = root_path_or_url
            self.db_name = f"{db_name}-{kwargs.get('backend', 'leveldb')}"
        else:
            self.db_address = os.path.join(
                root_path_or_url, f"{db_name}-{self.db_type}"
            )

            root_path = Path(root_path_or_url)
            if not root_path.exists():
                root_path.mkdir(parents=True, exist_ok=True)

            if self._rebuild:
                self.destroy()

        self.env = self.connect(**kwargs)

    def connect(self, **kwargs):
        """
        Connects to the specified database.

        Returns:
            env: A database environment object based on the specified DB type.
        """
        if self.db_type == "lmdb":
            import lmdb

            env = lmdb.open(
                self.db_address,
                max_dbs=kwargs.get('max_dbs', 1),
                map_size=kwargs.get('map_size', 2 * 1024**3),
            )

        elif self.db_type == "leveldb":
            import plyvel

            env = plyvel.DB(self.db_address, create_if_missing=True)

        elif self.db_type == "remote":
            env = RemoteTransaction(
                base_url=self.db_address,
                db_name=self.db_name,
                backend=kwargs.pop("backend", "leveldb"),
                rebuild=self._rebuild,
                timeout=kwargs.pop(
                    "timeout", 10
                ),  # `timeout` refers to connection timeout
                **kwargs,
            )
        else:
            raise ValueError(f"Unsupported DB type {self.db_type}.")
        return env

    def rmtree(self):
        """
        Deletes the database at the specified path.
        """
        shutil.rmtree(self.db_address, ignore_errors=True)

    def destroy(self):
        """
        Destroys the database by closing and deleting it.
        """
        try:
            self.close()
        except:
            pass
        self.rmtree()
        logger.info(f"Destroyed database at {self.db_address}.")

    def rebuild_db(self):
        """
        Clears the database by closing and recreating it.
        """
        try:
            self.close()
        except:
            pass
        self.rmtree()
        self.env = self.connect()

    def get_env(self):
        """
        Retrieves the database environment.

        Returns:
            env: The database environment object.
        """
        return self.env

    def new_static_view(self):
        """
        Creates a new static view of the database.

        Returns:
            static_view: A static view of the database based on the specified DB type.
        """
        if self.db_type == "lmdb":
            return self.env.begin()
        elif self.db_type == "leveldb":
            return self.env.snapshot()
        elif self.db_type == "remote":
            return self.env
        else:
            raise ValueError(f"Unsupported DB type {self.db_type}.")

    def close_static_view(self, static_view):
        """
        Closes the provided static view of the database.

        Args:
            static_view: The static view to be closed.
        """
        if self.db_type == "lmdb":
            return static_view.abort()
        elif self.db_type == "leveldb":
            return static_view.close()
        elif self.db_type == "remote":
            return static_view.close()
        else:
            raise ValueError(f"Unsupported DB type {self.db_type}.")

    def write(self):
        """
        Initiates a write transaction on the database.

        Returns:
            wb: A write transaction object based on the specified DB type.
        """
        if self.db_type == "lmdb":
            return self.env.begin(write=True)
        elif self.db_type == "leveldb":
            return self.env.write_batch()
        elif self.db_type == "remote":
            return self.env
        else:
            traceback.print_exc()
            raise ValueError(f"Unsupported DB type {self.db_type}.")

    def close(self):
        """
        Closes the database connection.
        """
        if self.db_type == "lmdb":
            return self.env.close()
        elif self.db_type == "leveldb":
            return self.env.close()
        elif self.db_type == "remote":
            return self.env.close()
        else:
            raise ValueError(f"Unsupported DB type to {self.db_type}.")


class RemoteTransaction:
    def __init__(
        self,
        base_url: str,
        db_name: str,
        backend="leveldb",
        rebuild=False,
        timeout=10,
        http2=True,
        **kwargs,
    ):
        import httpx

        self.client = kwargs.pop(
            "client",
            httpx.Client(
                base_url=base_url,
                timeout=httpx.Timeout(None, connect=timeout),
                http2=http2,
            ),
        )
        self.db_name = db_name
        self._backend = backend
        self._rebuild = rebuild
        self.client_id = str(uuid4())

        self.put_buffer_dict = {}
        self.delete_buffer_set = set()

    @retry(
        max_retries=3,
        delay=0.2,
        backoff=2,
    )
    def attach_db(self, event: threading.Event):

        with self.client.stream(
            "POST",
            f"/connect",
            json={
                "db_name": self.db_name,
                "backend": self._backend,
                "rebuild": self._rebuild,
                "client_id": self.client_id,
            },
        ) as r:
            from httpx import Response

            r: Response
            if r.is_success:
                event.set()

            buffer = bytearray()
            chunk_size = 1024 * 1024
            try:
                for chunk in r.iter_raw(chunk_size=chunk_size):
                    if chunk == b"data: end\n\n":
                        print(f"{buffer=}")
                        yield msgspec.msgpack.decode(
                            bytes(buffer), type=StructUpdateData
                        )
                        buffer = bytearray()
                    else:
                        buffer.extend(chunk)
            except Exception as e:
                # traceback.print_exc()
                yield None

    def close_connection(self):
        url = f"/disconnect?client_id={self.client_id}"
        try:
            response = self.client.get(url)
            if not response.is_success:
                raise ValueError(response.json())
        except Exception as e:
            ...

    @retry(max_retries=3, delay=0.2, backoff=2)
    def detach_db(self, db_name=None):
        if db_name is None:
            db_name = self.db_name

        url = f"/detach"
        response = self.client.post(url, json={"db_name": db_name})
        if not response.is_success:
            raise ValueError
        return response.json()

    @retry(max_retries=3, delay=0.2, backoff=2)
    def check_db_exist(self):
        response = self.client.get(f"/check_db?db_name={self.db_name}")
        content = response.json()
        assert response.is_success, f"error: {content}"
        assert content is True

    @retry(max_retries=3, delay=0.2, backoff=2)
    def put(self, key: bytes, value: bytes):
        self.put_buffer_dict[key] = value

    def delete(self, key: bytes):
        self.delete_buffer_set.add(key)

    @retry(max_retries=3, delay=0.5, backoff=2)
    def _put_batch(self):
        byte_data = encode(
            {
                "data": self.put_buffer_dict,
                "client_id": self.client_id,
                "time": time.time(),
            }
        )
        with io.BytesIO(byte_data) as f:
            url = f"/set_batch_stream"
            files = {'file': (self.db_name, f)}
            response = self.client.post(url, files=files)

        if not response.is_success:
            raise RuntimeError
        self.put_buffer_dict = {}

    @retry(max_retries=3, delay=0.5, backoff=2)
    def _delete_batch(self):
        url = f"/delete_batch?db_name={self.db_name}"
        # todo: use stream
        response = self.client.post(
            url,
            content=encode(
                {
                    "keys": list(self.delete_buffer_set),
                    "client_id": self.client_id,
                    "time": time.time(),
                }
            ),
        )

        if not response.is_success:
            raise RuntimeError
        self.delete_buffer_set = set()

    def get(self, key: bytes, default=None):
        url = f"/get?db_name={self.db_name}"
        response = self.client.post(url, content=key)
        if not response.is_success:
            raise RuntimeError
        raw_data = response.read()
        if raw_data == b"iamnull123":
            return default
        return raw_data

    # def get_batch(self, keys: list[bytes]):
    #     url = f"/get_batch?db_name={self.db_name}"
    #     response = self.client.post(url, content=encode({"keys": keys}))
    #     if not response.is_success:
    #         raise RuntimeError
    #     raw_data = response.read()
    #     return decode(raw_data)  # non bytes

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.put_buffer_dict:
            self._put_batch()
        if self.delete_buffer_set:
            self._delete_batch()
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_val, exc_tb)
            return False

    def close(self):
        """
        Do nothing.
        """

    def stat(self):
        url = f"/stat?db_name={self.db_name}"
        response = self.client.get(url)
        if not response.is_success:
            raise RuntimeError
        return decode(response.read())
