"""
FlaxKV2 核心功能测试
"""

import os
import shutil
import tempfile
import time
from pathlib import Path

import pytest
import numpy as np
import pandas as pd

from flaxkv2 import FlaxKV


class TestFlaxKV:
    """FlaxKV测试类"""
    
    @pytest.fixture
    def db_path(self):
        """创建临时数据库路径"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        # 清理
        shutil.rmtree(temp_dir)
    
    def test_basic_operations(self, db_path):
        """测试基本操作"""
        db = FlaxKV("test_db", db_path)
        
        # 设置值
        db["string"] = "value"
        db["int"] = 123
        db["float"] = 3.14
        db["list"] = [1, 2, 3, 4]
        db["dict"] = {"name": "test", "value": 456}
        
        # 检查键存在
        assert "string" in db
        assert "nonexistent" not in db
        
        # 获取值
        assert db["string"] == "value"
        assert db["int"] == 123
        assert db["float"] == 3.14
        assert db["list"] == [1, 2, 3, 4]
        assert db["dict"] == {"name": "test", "value": 456}
        
        # 获取默认值
        assert db.get("nonexistent") is None
        assert db.get("nonexistent", "default") == "default"
        
        # 删除键
        del db["string"]
        assert "string" not in db
        
        # 关闭数据库
        db.close()
        
        # 重新打开
        db2 = FlaxKV("test_db", db_path)
        assert db2["int"] == 123
        assert db2["dict"] == {"name": "test", "value": 456}
        db2.close()
    
    def test_numpy_support(self, db_path):
        """测试NumPy支持"""
        db = FlaxKV("test_db", db_path)
        
        # 创建NumPy数组
        arr1 = np.array([1, 2, 3, 4, 5])
        arr2 = np.random.rand(3, 4)
        
        # 存储数组
        db["arr1"] = arr1
        db["arr2"] = arr2
        
        # 检查存储的数组
        np.testing.assert_array_equal(db["arr1"], arr1)
        np.testing.assert_array_equal(db["arr2"], arr2)
        
        db.close()
    
    def test_pandas_support(self, db_path):
        """测试Pandas支持"""
        db = FlaxKV("test_db", db_path)
        
        # 创建DataFrame
        df = pd.DataFrame({
            'A': [1, 2, 3],
            'B': ['a', 'b', 'c'],
            'C': [True, False, True]
        })
        
        # 存储DataFrame
        db["df"] = df
        
        # 检查存储的DataFrame
        pd.testing.assert_frame_equal(db["df"], df)
        
        db.close()
    
    def test_bulk_operations(self, db_path):
        """测试批量操作"""
        db = FlaxKV("test_db", db_path)
        
        # 批量添加
        items = {f"key{i}": f"value{i}" for i in range(100)}
        db.update(items)
        
        # 检查所有键
        for i in range(100):
            assert db[f"key{i}"] == f"value{i}"
            
        # 检查键数量
        assert len(db.keys()) == 100
        
        # 检查值列表
        values = db.values()
        assert len(values) == 100
        assert "value0" in values
        assert "value99" in values
        
        # 检查键值对
        items = db.items()
        assert len(items) == 100
        assert ("key0", "value0") in items
        
        # 转换为普通字典
        dict_data = db.to_dict()
        assert len(dict_data) == 100
        assert dict_data["key0"] == "value0"
        
        db.close()
    
    def test_ttl(self, db_path):
        """测试TTL功能"""
        db = FlaxKV("test_db", db_path)
        
        # 设置键值
        db["temp_key"] = "value"
        
        # 设置TTL（1秒）
        db.set_ttl("temp_key", 1)
        
        # 检查TTL
        ttl = db.get_ttl("temp_key")
        assert ttl is not None
        assert ttl <= 1.0
        
        # 等待过期
        time.sleep(1.1)
        
        # 检查键是否已过期
        assert "temp_key" not in db
        
        db.close()
    
    def test_rebuild(self, db_path):
        """测试重建数据库"""
        # 创建数据库并添加数据
        db = FlaxKV("test_db", db_path)
        db["key"] = "value"
        db.close()
        
        # 验证数据存在
        db = FlaxKV("test_db", db_path)
        assert "key" in db
        db.close()
        
        # 重建数据库
        db = FlaxKV("test_db", db_path, rebuild=True)
        assert "key" not in db
        db.close()


# 注意：索引功能已从默认的 RawLevelDBDict 中移除
# FlaxKV() 默认返回 RawLevelDBDict（高性能简化版本）
# 如需使用索引功能，请直接使用 LevelDBDict：
# from flaxkv2 import LevelDBDict
# db = LevelDBDict("test_db", "./data")
# db.create_hash_index("city_index", lambda x: x.get("city")) 