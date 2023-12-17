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
    StructSetData,
    StructUpdateData,
)
from .manager import DBManager

db_manager = DBManager(root_path="./FLAXKV_DB", raw_mode=True)


@get(
    "/healthz",
    summary="Perform a Health Check",
    response_description="Return HTTP Status Code 200 (OK)",
    status_code=status_codes.HTTP_200_OK,
    media_type=MediaType.TEXT,
)
async def healthz(request: Request) -> str:
    return "OK"


@post(path="/attach")
async def attach(data: AttachRequest) -> dict:
    # todo switch `post` to `get`
    try:
        db = db_manager.get(data.db_name)
        if db is None or data.rebuild:
            db_manager.set_db(
                db_name=data.db_name, backend=data.backend, rebuild=data.rebuild
            )
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


@post(path="/set_raw")
async def set_raw(db_name: str, request: Request) -> dict:
    db = db_manager.get(db_name)
    if db is None:
        return {"success": False, "info": "db not found"}
    data = await request.body()
    try:
        data = msgspec.msgpack.decode(data, type=StructSetData)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    db[data.key] = data.value
    return {"success": True}


@post("/get_raw", media_type=MediaType.TEXT)
async def _get(db_name: str, request: Request) -> bytes:
    db = db_manager.get(db_name)
    if db is None:
        raise ValueError("db not found")
    key = await request.body()
    value = db.get(key, None)
    if value is None:
        return b'iamnull123'
    return value


@post("/delete")
async def delete(db_name: str, request: Request) -> dict:
    try:
        db = db_manager.get(db_name)
        key = await request.body()
        return db.pop(key)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@get("/keys", media_type=MediaType.TEXT)
@msg_encoder
async def get_keys(db_name: str) -> bytes:
    db = db_manager.get(db_name)
    try:
        return db.keys()
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# @get("/values", media_type=MediaType.TEXT)
# @msg_encoder
# async def _values(db_name: str) -> bytes:
#     db = db_manager.get(db_name)
#     if db is None:
#         return {"success": False, "info": "db not found"}
#     try:
#         return {"success": True, "data": db.values()}
#     except Exception as e:
#         traceback.print_exc()
#         return {"success": False, "info": str(e)}


@get("/items", media_type=MediaType.TEXT)
@msg_encoder
async def get_items(db_name: str) -> bytes:
    db = db_manager.get(db_name)
    try:
        return db.db_dict()
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


def on_startup():
    ...
    # global db_manager
    # db_manager = DBManager(root_path="./FLAXKV_DB", raw_mode=True)


def on_shutdown():
    # print("on_shutdown")
    ...


app = Litestar(
    route_handlers=[
        healthz,
        attach,
        detach,
        set_raw,
        # update_raw,
        _get,
        get_items,
        # _values,
        # set_value,
        delete,
        # contains,
        # pop,
        get_keys,
    ],
    on_startup=[on_startup],
    on_shutdown=[on_shutdown],
    openapi_config=OpenAPIConfig(title="FlaxKV", version=f"v{__version__}"),
)
