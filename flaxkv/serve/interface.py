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

from dataclasses import dataclass
from typing import Dict, List

import msgspec
from litestar.enums import RequestEncodingType
from litestar.params import Body


@dataclass
class AttachRequest:
    db_name: str
    backend: str
    rebuild: bool
    client_id: str


@dataclass
class DetachRequest:
    db_name: str


class StructSetData(msgspec.Struct):
    key: bytes
    value: bytes


class StructSetBatchData(msgspec.Struct):
    data: Dict[bytes, bytes]
    client_id: str
    time: float


class StructGetBatchData(msgspec.Struct):
    keys: List[bytes]


class StructDeleteBatchData(msgspec.Struct):
    keys: List[bytes]
    client_id: str
    time: float
