import traceback

import msgspec
from litestar import Litestar, MediaType, Request, get, post
from litestar.openapi import OpenAPIConfig

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


@post(path="/set_value")
async def set_value(data: SetRequest) -> dict:
    db = db_manager.get(data.db_name)
    if db is None:
        return {"success": False, "info": "db not found"}
    db[encode(data.key)] = encode(data.value)
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
        return {"success": False, "info": str(e)}
    db[data.key] = data.value
    return {"success": True}


@post(path="/update_raw")
async def update_raw(db_name: str, request: Request) -> dict:
    db = db_manager.get(db_name)
    if db is None:
        return {"success": False, "info": "db not found"}
    data = await request.body()
    try:
        db.update(decode(data))
        return {"success": True}
    except Exception as e:
        traceback.print_exc()
        return {"success": False, "info": str(e)}


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


@post("/contains", media_type=MediaType.TEXT)
async def contains(db_name: str, request: Request) -> bytes:
    db = db_manager.get(db_name)
    if db is None:
        return encode({"success": False, "info": "db not found"})
    key = await request.body()
    is_contain = key in db
    return encode(is_contain)


@post("/pop")
@msg_encoder
async def pop(data: PopKeyRequest) -> dict:
    db = db_manager.get(data.db_name)
    if db is None:
        return {"success": False, "info": "db not found"}
    try:
        return {"success": True, "data": db.pop(encode(data.key), None)}

    except Exception as e:
        traceback.print_exc()
        return {"success": False, "info": str(e)}


@get("/keys", media_type=MediaType.TEXT)
@msg_encoder
async def get_keys(db_name: str) -> bytes:
    db = db_manager.get(db_name)
    if db is None:
        return {"success": False, "info": "db not found"}
    try:
        return {"success": True, "data": db.keys()}
    except Exception as e:
        traceback.print_exc()
        return {"success": False, "info": str(e)}


@get("/values", media_type=MediaType.TEXT)
@msg_encoder
async def _values(db_name: str) -> bytes:
    db = db_manager.get(db_name)
    if db is None:
        return {"success": False, "info": "db not found"}
    try:
        return {"success": True, "data": db.values()}
    except Exception as e:
        traceback.print_exc()
        return {"success": False, "info": str(e)}


@get("/items", media_type=MediaType.TEXT)
@msg_encoder
async def get_items(db_name: str) -> bytes:
    db = db_manager.get(db_name)
    if db is None:
        return {"success": False, "info": "db not found"}
    try:
        return {"success": True, "data": db.db_dict()}
    except Exception as e:
        traceback.print_exc()
        return {"success": False, "info": str(e)}


def on_startup():
    # print("on_startup")
    ...


def on_shutdown():
    # print("on_shutdown")
    ...


app = Litestar(
    route_handlers=[
        attach,
        detach,
        set_raw,
        update_raw,
        _get,
        get_items,
        _values,
        set_value,
        contains,
        pop,
        get_keys,
    ],
    on_startup=[on_startup],
    on_shutdown=[on_shutdown],
    openapi_config=OpenAPIConfig(title="FlaxKV", version=f"v{__version__}"),
)
