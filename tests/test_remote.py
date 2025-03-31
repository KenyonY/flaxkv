"""
FlaxKV2 远程数据库测试
"""

import os
import shutil
import tempfile
import threading
import time
import pytest

from flaxkv2 import FlaxKV
from flaxkv2.server.app import create_app
import uvicorn


class TestRemoteDB:
    """远程数据库测试类"""
    
    @pytest.fixture
    def server_and_db(self):
        """创建服务器和测试数据库"""
        # 创建临时目录
        temp_dir = tempfile.mkdtemp()
        
        # 设置环境变量
        os.environ["FLAXKV_DATA_DIR"] = temp_dir
        
        # 创建应用
        app = create_app()
        
        # 启动服务器线程
        server_thread = threading.Thread(
            target=uvicorn.run,
            kwargs={
                "app": app,
                "host": "127.0.0.1",
                "port": 8765,
                "log_level": "error"
            }
        )
        server_thread.daemon = True
        server_thread.start()
        
        # 等待服务器启动
        time.sleep(1)
        
        # 连接远程数据库
        db = FlaxKV("test_remote_db", "http://127.0.0.1:8765")
        
        yield db
        
        # 清理
        try:
            db.close()
        except:
            pass
            
        shutil.rmtree(temp_dir)
    
    def test_basic_operations(self, server_and_db):
        """测试基本操作"""
        db = server_and_db
        
        # 设置值
        db["key1"] = "value1"
        db["key2"] = 123
        
        # 检查键存在
        assert "key1" in db
        assert "nonexistent" not in db
        
        # 获取值
        assert db["key1"] == "value1"
        assert db["key2"] == 123
        
        # 获取默认值
        assert db.get("nonexistent") is None
        assert db.get("nonexistent", "default") == "default"
        
        # 删除键
        del db["key1"]
        assert "key1" not in db
    
    def test_bulk_operations(self, server_and_db):
        """测试批量操作"""
        db = server_and_db
        
        # 批量添加
        items = {f"key{i}": f"value{i}" for i in range(10)}
        db.update(items)
        
        # 检查所有键
        for i in range(10):
            assert db[f"key{i}"] == f"value{i}"
            
        # 检查键数量
        keys = db.keys()
        assert len(keys) == 10
        
        # 检查键值对
        items = db.items()
        assert len(items) == 10
        
        # 转换为普通字典
        dict_data = db.to_dict()
        assert len(dict_data) == 10
        assert dict_data["key0"] == "value0" 