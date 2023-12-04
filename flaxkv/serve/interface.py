from dataclasses import dataclass
from typing import Annotated, Any

import msgspec
from litestar.enums import RequestEncodingType
from litestar.params import Body


@dataclass
class AttachRequest:
    db_name: str
    backend: str
    rebuild: bool


@dataclass
class DetachRequest:
    db_name: str


class StructSetData(msgspec.Struct):
    key: bytes
    value: bytes
