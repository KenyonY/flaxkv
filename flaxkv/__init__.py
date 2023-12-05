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

from .core import LevelDBDict, LMDBDict
from .serve.client import RemoteDictDB

__version__ = "0.1.6"

__all__ = [
    "dictdb",
    "dbdict",
    "LMDBDict",
    "LevelDBDict",
    "RemoteDictDB",
]


def dictdb(
    path_or_url: str,
    backend='lmdb',
    remote=False,
    db_name=None,
    rebuild=False,
    raw=False,
    **kwargs,
):
    if remote:
        return RemoteDictDB(
            path_or_url, db_name=db_name, rebuild=rebuild, backend=backend
        )
    if backend == 'lmdb':
        return LMDBDict(path_or_url, rebuild=rebuild, raw=raw, **kwargs)
    elif backend == 'leveldb':
        return LevelDBDict(path_or_url, rebuild=rebuild, raw=raw, **kwargs)
    else:
        raise ValueError(f"Unsupported DB type {backend}.")


dbdict = dictdb
