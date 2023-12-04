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
import traceback

from loguru import logger


class DBManager:
    def __init__(self, db_type: str, db_path: str, rebuild=False, **kwargs):
        """
        Initializes the database manager.

        Args:
            db_type (str): Type of the database ("lmdb" or "leveldb").
            db_path (str): Path to the database.
            rebuild (bool, optional): Whether to create a new database. Defaults to False.
        """
        self.db_type = db_type.lower()
        self.db_path = db_path
        if rebuild:
            self.delete_db()
        self.env = self.connect()

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
        else:
            raise ValueError(f"Unsupported DB type to {self.db_type}.")


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
