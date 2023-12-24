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

from pathlib import Path

from .. import FlaxKV, LevelDBDict, LMDBDict


class DBManager:
    def __init__(self, root_path="./FLAXKV_DB", raw_mode=True):
        self._db_dict = {}
        self._raw_mode = raw_mode
        self._root_path = Path(root_path)
        if not self._root_path.exists():
            self._root_path.mkdir(parents=True, exist_ok=True)

    def detach(self, db_name: str):
        return self._db_dict.pop(db_name, None)

    def set_db(self, db_name: str, backend: str, rebuild: bool):
        self._db_dict[db_name] = FlaxKV(
            db_name=db_name,
            root_path_or_url=self._root_path.__str__(),
            backend=backend,
            rebuild=rebuild,
            raw=self._raw_mode,
            log=True,
        )

    def get(self, db_name: str, raise_key_error=False) -> LMDBDict | LevelDBDict | None:
        if raise_key_error:
            return self._db_dict[db_name]
        else:
            return self._db_dict.get(db_name, None)
