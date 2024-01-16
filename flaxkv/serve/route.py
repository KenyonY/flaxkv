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

import asyncio
import io
import traceback
from typing import AsyncGenerator

import msgspec
from litestar import MediaType, Request, get, post, status_codes
from litestar.datastructures import UploadFile
from litestar.enums import RequestEncodingType
from litestar.exceptions import HTTPException
from litestar.params import Body
from litestar.response import Stream
from typing_extensions import Annotated

from ..pack import encode
from .interface import (
    AttachRequest,
    DetachRequest,
    StructDeleteBatchData,
    StructGetBatchData,
    StructSetBatchData,
    StructSetData,
)
from .manager import DBManager

# from typing import Any, Dict
# from litestar.serialization import decode_json, decode_msgpack


_db_manager = DBManager(root_path="./FLAXKV_DB", raw_mode=True)


def _get_db(db_name: str):
    db = _db_manager.get(db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="db not found")
    return db


@get(
    "/healthz",
    summary="Perform a Health Check",
    response_description="Return HTTP Status Code 200 (OK)",
    status_code=status_codes.HTTP_200_OK,
    media_type=MediaType.TEXT,
)
async def healthz() -> str:
    return "OK"


@post(path="/attach")
async def attach(data: AttachRequest) -> dict:
    # todo switch `post` to `get`
    try:
        db = _db_manager.get(data.db_name)
        if db is None:
            _db_manager.set_db(
                db_name=data.db_name, backend=data.backend, rebuild=data.rebuild
            )
        elif data.rebuild:
            db.destroy()
            _db_manager.set_db(
                db_name=data.db_name, backend=data.backend, rebuild=False
            )
    except Exception as e:
        traceback.print_exc()
        return {"success": False, "info": str(e)}
    return {"success": True}


@post(path="/detach")
async def detach(data: DetachRequest) -> dict:
    # todo switch `post` to `get`
    db = _db_manager.detach(db_name=data.db_name)
    if db is None:
        return {"success": False, "info": "db not found"}
    return {"success": True}


@post(path="/set")
async def set_(db_name: str, request: Request) -> None:
    db = _get_db(db_name)
    data = await request.body()
    try:
        data = msgspec.msgpack.decode(data, type=StructSetData)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    db[data.key] = data.value


@post("/set_batch")
async def set_batch_(db_name: str, request: Request) -> None:
    db = _get_db(db_name)
    data = await request.body()
    try:
        data = msgspec.msgpack.decode(data, type=StructSetBatchData)
        for key, value in data.data.items():
            db[key] = value
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@post("/set_batch_stream", media_type=MediaType.TEXT)
async def set_batch_stream_(
    data: Annotated[UploadFile, Body(media_type=RequestEncodingType.MULTI_PART)],
) -> None:
    # litestar >= 2.5.0 fixed: https://github.com/litestar-org/litestar/issues/2939
    content = await data.read()
    db_name = data.filename
    db = _get_db(db_name)

    try:
        content = msgspec.msgpack.decode(content, type=StructSetBatchData)
        for key, value in content.data.items():
            db[key] = value
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@post("/get", media_type=MediaType.TEXT)
async def get_(db_name: str, request: Request) -> bytes:
    db = _get_db(db_name)
    key = await request.body()
    value = db.get(key, None)
    if value is None:
        return b'iamnull123'
    return value


@post("/get_batch", media_type=MediaType.TEXT)
async def get_batch_(db_name: str, request: Request) -> bytes:
    db = _get_db(db_name)
    data = await request.body()
    data = msgspec.msgpack.decode(data, type=StructGetBatchData)
    values = db.get_batch(data.keys)
    return encode(values)


@post("/delete")
async def delete_(db_name: str, request: Request) -> None:
    db = _get_db(db_name)
    key = await request.body()
    try:
        db.pop(key)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@post("/delete_batch")
async def delete_batch_(db_name: str, request: Request) -> None:
    db = _get_db(db_name)
    data = await request.body()
    try:
        data = msgspec.msgpack.decode(data, type=StructDeleteBatchData)
        for key in data.keys:
            db.pop(key)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@get("/keys", media_type=MediaType.TEXT)
async def keys_(db_name: str) -> bytes:
    db = _get_db(db_name)
    try:
        return encode(list(db.keys()))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@get("/dict", media_type=MediaType.TEXT)
async def dict_(db_name: str) -> bytes:
    db = _get_db(db_name)
    try:
        return encode(db.db_dict())
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


async def stream_generator(
    data: bytes, chunk_size=1024 * 1024
) -> AsyncGenerator[bytes, None]:
    with io.BytesIO(data) as data_io:
        while chunk := data_io.read(chunk_size):
            yield chunk


@get("/keys_stream", media_type=MediaType.TEXT)
async def keys_stream_(db_name: str) -> Stream:
    db = _get_db(db_name)
    try:
        result_bin = encode(list(db.keys()))
        return Stream(stream_generator(result_bin))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@post("/get_batch_stream", media_type=MediaType.TEXT)
async def get_batch_stream_(db_name: str, request: Request) -> Stream:
    db = _get_db(db_name)
    data = await request.body()
    try:
        data = msgspec.msgpack.decode(data, type=StructGetBatchData)
        values = db.get_batch(data.keys)
        result_bin = encode(values)
        return Stream(stream_generator(result_bin))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@get("/dict_stream", media_type=MediaType.TEXT)
async def dict_stream_(db_name: str) -> Stream:

    db = _get_db(db_name)
    try:
        result_bin = encode(db.db_dict())
        return Stream(stream_generator(result_bin))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@get("/stat", media_type=MediaType.TEXT)
async def stat_(db_name: str) -> bytes:
    db = _get_db(db_name)
    try:
        return encode(db.stat())
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
