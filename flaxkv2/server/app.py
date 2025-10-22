"""
FlaxKV2 服务器应用
"""

from typing import Dict, Any, List, Optional
import os
from dataclasses import dataclass
from litestar import Litestar, get, post
from litestar.response import Response
from litestar.params import Body
from litestar.status_codes import HTTP_200_OK, HTTP_404_NOT_FOUND, HTTP_400_BAD_REQUEST
from litestar.openapi.config import OpenAPIConfig
from litestar.openapi.plugins import SwaggerRenderPlugin, RedocRenderPlugin, ScalarRenderPlugin, RapidocRenderPlugin, StoplightRenderPlugin, YamlRenderPlugin

from flaxkv2.core.leveldb_dict import LevelDBDict
from flaxkv2.utils.log import get_logger
from flaxkv2 import __version__

logger = get_logger(__name__)

# 数据库管理器
db_manager: Dict[str, LevelDBDict] = {}


@get("/healthz")
async def health_check() -> Dict[str, str]:
    """健康检查接口"""
    return {"status": "ok"}


@dataclass
class ConnectRequest:
    db_name: str
    create_if_missing: bool = True
    root_path: Optional[str] = None


@post("/connect")
async def connect(data: ConnectRequest = Body()) -> Dict[str, Any]:
    """
    连接数据库
    
    Args:
        data: 连接请求数据
            - db_name: 数据库名称
            - create_if_missing: 如果数据库不存在则创建
            - root_path: 数据库根路径
    """
    global db_manager
    
    db_name = data.db_name
    create_if_missing = data.create_if_missing
    root_path = data.root_path
    
    # 检查数据库是否已连接
    if db_name in db_manager:
        return {"status": "connected", "db_name": db_name}
    
    # 确定根路径
    if not root_path:
        root_path = os.environ.get("FLAXKV_DATA_DIR", ".")
    
    try:
        # 创建数据库
        db = LevelDBDict(
            name=db_name,
            path=root_path,
            create_if_missing=create_if_missing,
            raw=True  # 服务器模式下使用原始模式
        )
        db_manager[db_name] = db
        
        logger.info(f"Connected to database {db_name}")
        return {"status": "success", "db_name": db_name}
        
    except Exception as e:
        logger.error(f"Error connecting to database {db_name}: {e}")
        return Response(
            content={"status": "error", "message": str(e)},
            status_code=HTTP_400_BAD_REQUEST
        )


@dataclass
class DisconnectRequest:
    db_name: str


@post("/disconnect")
async def disconnect(data: DisconnectRequest = Body()) -> Dict[str, Any]:
    """
    断开数据库连接
    
    Args:
        data: 断开连接请求
            - db_name: 数据库名称
    """
    global db_manager
    
    db_name = data.db_name
    
    if db_name not in db_manager:
        return {"status": "not_connected", "db_name": db_name}
    
    try:
        # 关闭数据库
        db_manager[db_name].close()
        del db_manager[db_name]
        
        logger.info(f"Disconnected from database {db_name}")
        return {"status": "success", "db_name": db_name}
        
    except Exception as e:
        logger.error(f"Error disconnecting from database {db_name}: {e}")
        return Response(
            content={"status": "error", "message": str(e)},
            status_code=HTTP_400_BAD_REQUEST
        )


@dataclass
class GetValueRequest:
    db_name: str
    key: Any


@post("/get")
async def get_value(data: GetValueRequest = Body()) -> Dict[str, Any]:
    """
    获取键值
    
    Args:
        data: 获取值请求
            - db_name: 数据库名称
            - key: 键
    """
    global db_manager
    
    db_name = data.db_name
    key = data.key
    
    if db_name not in db_manager:
        return Response(
            content={"status": "error", "message": f"Database {db_name} not connected"},
            status_code=HTTP_400_BAD_REQUEST
        )
    
    try:
        db = db_manager[db_name]
        value = db[key]
        
        return {"status": "success", "value": value}
        
    except KeyError:
        return Response(
            content={"status": "error", "message": f"Key {key} not found"},
            status_code=HTTP_404_NOT_FOUND
        )
    except Exception as e:
        logger.error(f"Error getting value for key {key}: {e}")
        return Response(
            content={"status": "error", "message": str(e)},
            status_code=HTTP_400_BAD_REQUEST
        )


@dataclass
class SetValueRequest:
    db_name: str
    key: Any
    value: Any


@post("/set")
async def set_value(data: SetValueRequest = Body()) -> Dict[str, Any]:
    """
    设置键值

    Args:
        data: 设置值请求
            - db_name: 数据库名称
            - key: 键
            - value: 值
    """
    global db_manager

    db_name = data.db_name
    key = data.key
    value = data.value

    if db_name not in db_manager:
        return Response(
            content={"status": "error", "message": f"Database {db_name} not connected"},
            status_code=HTTP_400_BAD_REQUEST
        )

    try:
        db = db_manager[db_name]
        db[key] = value

        return {"status": "success"}

    except Exception as e:
        logger.error(f"Error setting value for key {key}: {e}")
        return Response(
            content={"status": "error", "message": str(e)},
            status_code=HTTP_400_BAD_REQUEST
        )


@dataclass
class DeleteValueRequest:
    db_name: str
    key: Any


@post("/delete")
async def delete_value(data: DeleteValueRequest = Body()) -> Dict[str, Any]:
    """
    删除键值
    
    Args:
        data: 删除值请求
            - db_name: 数据库名称
            - key: 键
    """
    global db_manager
    
    db_name = data.db_name
    key = data.key
    
    if db_name not in db_manager:
        return Response(
            content={"status": "error", "message": f"Database {db_name} not connected"},
            status_code=HTTP_400_BAD_REQUEST
        )
    
    try:
        db = db_manager[db_name]
        del db[key]
        
        return {"status": "success"}
        
    except KeyError:
        return Response(
            content={"status": "error", "message": f"Key {key} not found"},
            status_code=HTTP_404_NOT_FOUND
        )
    except Exception as e:
        logger.error(f"Error deleting value for key {key}: {e}")
        return Response(
            content={"status": "error", "message": str(e)},
            status_code=HTTP_400_BAD_REQUEST
        )


@dataclass
class SetBatchRequest:
    db_name: str
    items: Dict[str, Any]


@post("/set_batch")
async def set_batch(data: SetBatchRequest = Body()) -> Dict[str, Any]:
    """
    批量设置键值
    
    Args:
        data: 批量设置请求
            - db_name: 数据库名称
            - items: 键值对字典
    """
    global db_manager
    
    db_name = data.db_name
    items = data.items
    
    if db_name not in db_manager:
        return Response(
            content={"status": "error", "message": f"Database {db_name} not connected"},
            status_code=HTTP_400_BAD_REQUEST
        )
    
    try:
        db = db_manager[db_name]
        # 确保 items 是一个字典
        if not isinstance(items, dict):
            raise ValueError("Items must be a dictionary")
        
        db.update(items)
        
        return {"status": "success", "count": len(items)}
        
    except Exception as e:
        logger.error(f"Error setting batch values: {e}")
        return Response(
            content={"status": "error", "message": str(e)},
            status_code=HTTP_400_BAD_REQUEST
        )


@dataclass
class DeleteBatchRequest:
    db_name: str
    keys: List[Any]


@post("/delete_batch")
async def delete_batch(data: DeleteBatchRequest = Body()) -> Dict[str, Any]:
    """
    批量删除键值
    
    Args:
        data: 批量删除请求
            - db_name: 数据库名称
            - keys: 键列表
    """
    global db_manager
    
    db_name = data.db_name
    keys = data.keys
    
    if db_name not in db_manager:
        return Response(
            content={"status": "error", "message": f"Database {db_name} not connected"},
            status_code=HTTP_400_BAD_REQUEST
        )
    
    try:
        db = db_manager[db_name]
        # 确保 keys 是一个列表
        if not isinstance(keys, list):
            raise ValueError("Keys must be a list")
        
        # 逐个删除键
        deleted_count = 0
        for key in keys:
            try:
                del db[key]
                deleted_count += 1
            except KeyError:
                # 忽略不存在的键
                pass
        
        return {"status": "success", "count": deleted_count}
        
    except Exception as e:
        logger.error(f"Error deleting batch: {e}")
        return Response(
            content={"status": "error", "message": str(e)},
            status_code=HTTP_400_BAD_REQUEST
        )


@dataclass
class GetKeysRequest:
    db_name: str


@post("/keys")
async def get_keys(data: GetKeysRequest = Body()) -> Dict[str, Any]:
    """
    获取所有键
    
    Args:
        data: 获取键请求
            - db_name: 数据库名称
    """
    global db_manager
    
    db_name = data.db_name
    
    if db_name not in db_manager:
        return Response(
            content={"status": "error", "message": f"Database {db_name} not connected"},
            status_code=HTTP_400_BAD_REQUEST
        )
    
    try:
        db = db_manager[db_name]
        keys = db.keys()
        
        return {"status": "success", "keys": keys}
        
    except Exception as e:
        logger.error(f"Error getting keys: {e}")
        return Response(
            content={"status": "error", "message": str(e)},
            status_code=HTTP_400_BAD_REQUEST
        )


@dataclass
class GetDictRequest:
    db_name: str
    max_items: Optional[int] = None


@post("/dict")
async def get_dict(data: GetDictRequest = Body()) -> Dict[str, Any]:
    """
    获取所有键值对
    
    Args:
        data: 获取字典请求
            - db_name: 数据库名称
            - max_items: 最大返回条目数
    """
    global db_manager
    
    db_name = data.db_name
    max_items = data.max_items
    
    if db_name not in db_manager:
        return Response(
            content={"status": "error", "message": f"Database {db_name} not connected"},
            status_code=HTTP_400_BAD_REQUEST
        )
    
    try:
        db = db_manager[db_name]
        items = db.items()
        
        if max_items is not None and max_items > 0:
            items = items[:max_items]
        
        # 构建字典
        result_dict = {key: value for key, value in items}
        
        return {"status": "success", "dict": result_dict}
        
    except Exception as e:
        logger.error(f"Error getting dictionary: {e}")
        return Response(
            content={"status": "error", "message": str(e)},
            status_code=HTTP_400_BAD_REQUEST
        )


@dataclass
class GetStatRequest:
    db_name: str


@post("/stat")
async def get_stat(data: GetStatRequest = Body()) -> Dict[str, Any]:
    """
    获取数据库统计信息

    Args:
        data: 获取统计信息请求
            - db_name: 数据库名称
    """
    global db_manager

    db_name = data.db_name

    if db_name not in db_manager:
        return Response(
            content={"status": "error", "message": f"Database {db_name} not connected"},
            status_code=HTTP_400_BAD_REQUEST
        )

    try:
        db = db_manager[db_name]
        stat = db.stat()

        return {"status": "success", "stat": stat}

    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        return Response(
            content={"status": "error", "message": str(e)},
            status_code=HTTP_400_BAD_REQUEST
        )


@dataclass
class SetTtlRequest:
    """设置TTL的请求体"""
    db_name: str
    key: Any
    ttl_seconds: int


@post("/set_ttl")
async def set_ttl(data: SetTtlRequest = Body()) -> Dict[str, Any]:
    """
    设置键的过期时间
    
    Args:
        data: 请求体包含数据库名称、键和TTL秒数
    """
    global db_manager
    
    db_name = data.db_name
    key = data.key
    ttl_seconds = data.ttl_seconds
    
    if db_name not in db_manager:
        return Response(
            content={"status": "error", "message": f"Database {db_name} not connected"},
            status_code=HTTP_400_BAD_REQUEST
        )
    
    try:
        db = db_manager[db_name]
        db.set_ttl(key, ttl_seconds)
        
        return {"status": "success"}
        
    except KeyError:
        return Response(
            content={"status": "error", "message": f"Key {key} not found"},
            status_code=HTTP_404_NOT_FOUND
        )
    except Exception as e:
        logger.error(f"Error setting TTL for key {key}: {e}")
        return Response(
            content={"status": "error", "message": str(e)},
            status_code=HTTP_400_BAD_REQUEST
        )


@dataclass
class GetTtlRequest:
    """获取TTL的请求体"""
    db_name: str
    key: Any


@post("/get_ttl")
async def get_ttl(data: GetTtlRequest = Body()) -> Dict[str, Any]:
    """
    获取键的剩余过期时间
    
    Args:
        data: 请求体包含数据库名称和键
    """
    global db_manager
    
    db_name = data.db_name
    key = data.key
    
    if db_name not in db_manager:
        return Response(
            content={"status": "error", "message": f"Database {db_name} not connected"},
            status_code=HTTP_400_BAD_REQUEST
        )
    
    try:
        db = db_manager[db_name]
        # 检查键是否存在
        if key not in db:
            raise KeyError(key)
            
        ttl = db.get_ttl(key)
        
        return {"status": "success", "ttl": ttl}
        
    except KeyError:
        return Response(
            content={"status": "error", "message": f"Key {key} not found"},
            status_code=HTTP_404_NOT_FOUND
        )
    except Exception as e:
        logger.error(f"Error getting TTL for key {key}: {e}")
        return Response(
            content={"status": "error", "message": str(e)},
            status_code=HTTP_400_BAD_REQUEST
        )


def create_app() -> Litestar:
    """创建应用"""
    routes = [
        health_check,
        connect,
        disconnect,
        get_value,
        set_value,
        delete_value,
        set_batch,
        delete_batch,
        get_keys,
        get_dict,
        get_stat,
        set_ttl,
        get_ttl
    ]
    # 创建OpenAPI配置
    openapi_config = OpenAPIConfig(
        title="FlaxKV2 API",
        version=__version__,
        description="FlaxKV2 REST API接口文档",
        path="/schema",
        render_plugins=[
            SwaggerRenderPlugin(path="/swagger"),
            RedocRenderPlugin(path="/redoc"),
            ScalarRenderPlugin(path="/scalar"),
            RapidocRenderPlugin(path="/rapidoc"),
            StoplightRenderPlugin(path="/stoplight"),
            YamlRenderPlugin(path="/yaml"),
        ]
    )
    
    app = Litestar(
        route_handlers=routes,
        debug=os.environ.get("FLAXKV_DEBUG", "0") == "1",
        openapi_config=openapi_config
    )
    
    return app 