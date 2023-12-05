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


@dataclass
class SetRequest:
    db_name: str
    key: Any
    value: Any


@dataclass
class PopKeyRequest:
    db_name: str
    key: Any


class StructSetData(msgspec.Struct):
    key: bytes
    value: bytes


class StructUpdateData(msgspec.Struct):
    dict: bytes
