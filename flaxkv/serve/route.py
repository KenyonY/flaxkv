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
import os
import traceback
from typing import AsyncGenerator

import msgspec
from litestar import MediaType, Request, get, post, status_codes
from litestar.datastructures import UploadFile
from litestar.enums import RequestEncodingType
from litestar.exceptions import HTTPException
from litestar.params import Body
from litestar.response import Stream
from rich import print
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

# from litestar.serialization import decode_json, decode_msgpack


_db_manager = DBManager(
    root_path="./FLAXKV_DB",
    raw_mode=True,
    log_level=os.environ.get("FLAXKV_LOG_LEVEL", "INFO"),
)


def _get_db(db_name: str):
    db = _db_manager.get(db_name)
    if db is None:
        raise HTTPException(status_code=500, detail="db not found")
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


@get("/check_db")
async def check_db(db_name: str) -> bool:
    return _get_db(db_name) is not None


@get("/disconnect")
async def disconnect(client_id: str) -> dict:
    try:
        _db_manager.subscribers[client_id]['disconnect_event'].set()
        return {"success": True}
    except KeyError:
        ...
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@post(path="/connect")
async def connect(data: AttachRequest) -> Stream:
    try:
        db = _db_manager.get(data.db_name)
        q = asyncio.Queue()
        client = {
            "db_name": data.db_name,
            "update_data": q,
            "disconnect_event": asyncio.Event(),
        }
        client_id = data.client_id
        _db_manager.subscribers[client_id] = client

        print(f"{' Client connected ':*^60}")
        print(f"{client_id=}")
        print(f"Current subscribers: {_db_manager.subscribers.keys()}")

        if db is None:
            _db_manager.set_db(
                db_name=data.db_name, backend=data.backend, rebuild=data.rebuild
            )
        elif data.rebuild:
            db.clear(wait=True)
            _db_manager.set_db(
                db_name=data.db_name, backend=data.backend, rebuild=data.rebuild
            )

        async def stream(client: dict) -> AsyncGenerator:
            try:
                chunk_size = 1024 * 1024
                while True:
                    done, pending = await asyncio.wait(
                        [
                            asyncio.create_task(q.get()),
                            asyncio.create_task(client['disconnect_event'].wait()),
                        ],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if client['disconnect_event'].is_set():
                        for task in pending:
                            task.cancel()
                        break

                    else:
                        data_dict = done.pop().result()
                        # print(f"{data_dict=}")
                        # data_dict = await q.get()
                        assert client_id != data_dict['client_id']
                        buffer_dict = data_dict.get("buffer_dict")
                        if buffer_dict:
                            bytes_data = msgspec.msgpack.encode(
                                {
                                    "type": 'buffer_dict',
                                    'data': buffer_dict,
                                    'time': data_dict['time'],
                                }
                            )
                        else:
                            delete_list = data_dict['delete_keys']
                            bytes_data = msgspec.msgpack.encode(
                                {
                                    "type": 'delete_keys',
                                    'data': {key: b"" for key in delete_list},
                                    'time': data_dict['time'],
                                }
                            )

                        for i in range(0, len(bytes_data), chunk_size):
                            yield bytes_data[i : i + chunk_size]
                        yield b"data: end\n\n"
            finally:
                if isinstance(pending, set):
                    for task in pending:
                        task.cancel()
                print(f"{' Client disconnected ':*^60}")
                _db_manager.subscribers.pop(data.client_id)
                print(f"Current subscribers: {_db_manager.subscribers.keys()}")

        return Stream(stream(client))

    except Exception as e:

        async def stream_error(e):

            yield bytes(f"error {data.client_id} {e}\n", "utf-8")

        traceback.print_exc()
        return Stream(stream_error(e))


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
    raw_data = await data.read()
    db_name = data.filename
    db = _get_db(db_name)

    try:
        content = msgspec.msgpack.decode(raw_data, type=StructSetBatchData)
        assert db_name == _db_manager.subscribers[content.client_id]['db_name']
        clients = [
            client
            for client_id, client in _db_manager.subscribers.items()
            if client_id != content.client_id
        ]
        # print(f"{clients=}")
        for key, value in content.data.items():
            db[key] = value
        for client in clients:
            await client['update_data'].put(
                {
                    "buffer_dict": content.data,
                    'time': content.time,
                    "client_id": content.client_id,
                }
            )

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
        assert db_name == _db_manager.subscribers[data.client_id]['db_name']

        for key in data.keys:
            db.pop(key)

        clients = [
            client
            for client_id, client in _db_manager.subscribers.items()
            if client_id != data.client_id
        ]
        for client in clients:
            await client['update_data'].put(
                {
                    "delete_keys": data.keys,
                    'time': data.time,
                    "client_id": data.client_id,
                }
            )

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
