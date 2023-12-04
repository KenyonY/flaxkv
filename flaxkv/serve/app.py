import msgspec
from litestar import Litestar, MediaType, Request, get, post

from ..pack import encode
from .interface import AttachRequest, DetachRequest, StructSetData
from .manager import DBManager

db_manager = DBManager(root_path="./FLAXKV_DB")


@post(path="/attach")
async def attach(data: AttachRequest) -> dict:
    db = db_manager.get(data.db_name)
    if db is None or data.rebuild:
        db_manager.set_db(
            db_name=data.db_name, backend=data.backend, rebuild=data.rebuild
        )
    return {"success": True}


@post(path="/detach")
async def detach(data: DetachRequest) -> dict:
    db = db_manager.detach(db_name=data.db_name)
    if db is None:
        return {"success": False, "info": "db not found"}
    return {"success": True}


@post(path="/set_raw")
async def set_raw(db_name: str, request: Request) -> dict:
    print(f"{db_name=}")
    db = db_manager.get(db_name)
    if db is None:
        return {"success": False, "info": "db not found"}
    data = await request.body()
    data = msgspec.msgpack.decode(data, type=StructSetData)
    db[data.key] = data.value
    db.write_immediately(write=True, wait=True)
    return {"success": True}


@post("/get_raw", media_type=MediaType.TEXT)
async def get_raw(db_name: str, request: Request) -> bytes:
    db = db_manager.get(db_name)
    if db is None:
        return encode({"success": False, "info": "db not found"})
    key = await request.body()
    value = db.get(key)
    if value is None:
        return encode(None)
    return db.get(key)


@post("/contains", media_type=MediaType.TEXT)
async def contains(db_name: str, request: Request) -> bytes:
    db = db_manager.get(db_name)
    if db is None:
        return encode({"success": False, "info": "db not found"})
    key = await request.body()
    is_contain = key in db
    return encode(is_contain)


@get("/keys")
async def get_keys(db_name: str) -> dict:
    db = db_manager.get(db_name)
    if db is None:
        return {"success": False, "info": "db not found"}
    return {"keys": list(db.keys())}


def on_shutdown():
    print("on_shutdown")


app = Litestar(
    route_handlers=[
        attach,
        detach,
        set_raw,
        get_raw,
        contains,
        get_keys,
    ],
    on_startup=[lambda: print("on_startup")],
    on_shutdown=[on_shutdown],
)
