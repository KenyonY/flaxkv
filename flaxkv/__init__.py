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
import re

from .core import LevelDBDict, LMDBDict
from .serve.client import RemoteDictDB

__version__ = "0.1.8"

__all__ = [
    "dictdb",
    "dbdict",
    "LMDBDict",
    "LevelDBDict",
    "RemoteDictDB",
]


url_pattern = re.compile(r'^(http://|https://|ftp://)')


def dictdb(
    db_name: str,
    root_path_or_url: str = ".",
    backend='lmdb',
    rebuild=False,
    raw=False,
    **kwargs,
):
    if url_pattern.match(root_path_or_url):
        return RemoteDictDB(
            root_path_or_url, db_name=db_name, rebuild=rebuild, backend=backend
        )
    db_path = os.path.join(root_path_or_url, db_name)
    if backend == 'lmdb':
        return LMDBDict(db_path, rebuild=rebuild, raw=raw, **kwargs)
    elif backend == 'leveldb':
        return LevelDBDict(db_path, rebuild=rebuild, raw=raw, **kwargs)
    else:
        raise ValueError(f"Unsupported DB type {backend}.")


dbdict = dictdb
