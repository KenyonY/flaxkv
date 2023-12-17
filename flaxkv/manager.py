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

import os
import shutil
import traceback

from loguru import logger

from .pack import decode, decode_key, encode


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

        self.db_root = root_path_or_url
        self.db_name = db_name
        self.db_path = os.path.join(root_path_or_url, db_name)
        self._rebuild = rebuild
        if rebuild:
            if db_type == "remote":
                ...
            else:
                self.delete_db()
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
                self.db_path,
                max_dbs=kwargs.get('max_dbs', 1),
                map_size=kwargs.get('map_size', 2 * 1024**3),
            )

        elif self.db_type == "leveldb":
            import plyvel

            env = plyvel.DB(self.db_path, create_if_missing=True)
        elif self.db_type == "remote":
            env = kwargs.pop(
                "env",
                RemoteTransaction(
                    base_url=self.db_root,
                    db_name=self.db_name,
                    backend=kwargs.pop("backend", "lmdb"),
                    rebuild=self._rebuild,
                    timeout=kwargs.pop("timeout", 15),
                    **kwargs,
                ),
            )

        else:
            raise ValueError(f"Unsupported DB type {self.db_type}.")
        return env

    def delete_db(self):
        """
        Deletes the database at the specified path.
        """
        shutil.rmtree(self.db_path, ignore_errors=True)

    def destroy(self):
        """
        Destroys the database by closing and deleting it.
        """
        self.close()
        self.delete_db()
        logger.info(f"Destroyed database at {self.db_path}.")

    def clear(self):
        """
        Clears the database by closing and recreating it.
        """
        try:
            self.close()
        except:
            pass
        self.delete_db()
        self.env = self.connect()

    def get_view(self):
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
        backend="lmdb",
        rebuild=False,
        timeout=15,
        **kwargs,
    ):
        import httpx

        self.client = kwargs.pop(
            "client", httpx.Client(base_url=base_url, timeout=timeout)
        )
        self.db_name = db_name

        self._attach_db(db_name=db_name, rebuild=rebuild, backend=backend)

    def _attach_db(self, db_name: str, rebuild: bool, backend: str):
        response = self.client.post(
            "/attach", json={"db_name": db_name, "backend": backend, "rebuild": rebuild}
        )
        if not response.is_success:
            raise ValueError
        return response.json()

    def detach_db(self, db_name=None):
        if db_name is None:
            db_name = self.db_name

        url = f"/detach"
        response = self.client.post(url, json={"db_name": db_name})
        if not response.is_success:
            raise ValueError
        return response.json()

    def put(self, key: bytes, value: bytes):
        url = f"/set_raw?db_name={self.db_name}"
        data = {"key": key, "value": value}
        response = self.client.post(url, content=encode(data))
        if not response.is_success:
            raise ValueError

    def put_batch(self):
        pass

    def delete(self, key: bytes):
        url = f"/delete?db_name={self.db_name}"
        response = self.client.post(url, content=key)
        if not response.is_success:
            raise ValueError
        return response.read()

    def delete_batch(self):
        pass

    def get(self, key: bytes, default=None):
        url = f"/get_raw?db_name={self.db_name}"
        response = self.client.post(url, content=key)
        if not response.is_success:
            raise ValueError
        raw_data = response.read()
        if raw_data == b"iamnull123":
            return default
        return raw_data

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # todo: put batch
        # self.close()
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_val, exc_tb)
            return False

    def close(self):
        # do not close
        ...
        # try:
        #     self.client.close()
        # except:
        #     ...


# class Transaction:
#     def __init__(self, env):
#         self.env = env
#
#     def abort(self):
#         ...
#     def commit(self):
#         ...
#     def rollback(self):
#         ...
#     def __enter__(self):
#         ...
#     def __exit__(self, exc_type, exc_value, traceback):
#         ...
