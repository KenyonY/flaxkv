import traceback

import msgspec
from litestar import Litestar, MediaType, Request, Response, get, post, status_codes
from litestar.exceptions import HTTPException
from litestar.openapi import OpenAPIConfig
from litestar.response import ServerSentEvent, Stream

from .. import __version__
from ..decorators import msg_encoder
from ..pack import decode, decode_key, encode
from .interface import (
    AttachRequest,
    DetachRequest,
    PopKeyRequest,
    SetRequest,
    StructDeleteBatchData,
    StructSetBatchData,
    StructSetData,
    StructUpdateData,
)
from .manager import DBManager

db_manager = DBManager(root_path="./FLAXKV_DB", raw_mode=True)


def get_db(db_name: str):
    db = db_manager.get(db_name)
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
        db = db_manager.get(data.db_name)
        if db is None:
            db_manager.set_db(
                db_name=data.db_name, backend=data.backend, rebuild=data.rebuild
            )
        elif data.rebuild:
            db.destroy()
            db_manager.set_db(db_name=data.db_name, backend=data.backend, rebuild=False)
    except Exception as e:
        traceback.print_exc()
        return {"success": False, "info": str(e)}
    return {"success": True}


@post(path="/detach")
async def detach(data: DetachRequest) -> dict:
    # todo switch `post` to `get`
    db = db_manager.detach(db_name=data.db_name)
    if db is None:
        return {"success": False, "info": "db not found"}
    return {"success": True}


@post(path="/set")
async def _set(db_name: str, request: Request) -> None:
    db = get_db(db_name)
    data = await request.body()
    try:
        data = msgspec.msgpack.decode(data, type=StructSetData)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    db[data.key] = data.value


@post("/set_batch")
async def _set_batch(db_name: str, request: Request) -> None:
    db = get_db(db_name)
    data = await request.body()
    try:
        data = msgspec.msgpack.decode(data, type=StructSetBatchData)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    for key, value in data.data.items():
        db[key] = value


@post("/get", media_type=MediaType.TEXT)
async def _get(db_name: str, request: Request) -> bytes:
    db = get_db(db_name)
    key = await request.body()
    value = db.get(key, None)
    if value is None:
        return b'iamnull123'
    return value


@post("/delete")
async def _delete(db_name: str, request: Request) -> None:
    db = get_db(db_name)
    key = await request.body()
    try:
        db.pop(key)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@post("/delete_batch")
async def _delete_batch(db_name: str, request: Request) -> None:
    db = get_db(db_name)
    data = await request.body()
    try:
        data = msgspec.msgpack.decode(data, type=StructDeleteBatchData)
        for key in data.keys:
            db.pop(key)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@get("/keys", media_type=MediaType.TEXT)
@msg_encoder
async def _keys(db_name: str) -> bytes:
    db = get_db(db_name)
    try:
        return db.keys()
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@get("/dict", media_type=MediaType.TEXT)
@msg_encoder
async def _items(db_name: str) -> bytes:
    db = get_db(db_name)
    try:
        return db.db_dict()
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@get("/stat", media_type=MediaType.TEXT)
@msg_encoder
async def _stat(db_name: str) -> bytes:
    db = get_db(db_name)
    try:
        return db.stat()
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


def on_startup():
    ...


def on_shutdown():
    ...


app = Litestar(
    route_handlers=[
        healthz,
        attach,
        detach,
        _get,
        _set,
        _set_batch,
        _delete,
        _delete_batch,
        _keys,
        _items,
        _stat,
    ],
    on_startup=[on_startup],
    on_shutdown=[on_shutdown],
    openapi_config=OpenAPIConfig(title="FlaxKV", version=f"v{__version__}"),
)
