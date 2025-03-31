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


class TestIndexes:
    """索引功能测试类"""
    
    @pytest.fixture
    def db_with_data(self):
        """创建带有测试数据的数据库"""
        temp_dir = tempfile.mkdtemp()
        
        # 创建数据库
        db = FlaxKV("test_db", temp_dir)
        
        # 添加用户数据
        users = [
            {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
            {"id": 2, "name": "Bob", "age": 25, "city": "Boston"},
            {"id": 3, "name": "Charlie", "age": 35, "city": "New York"},
            {"id": 4, "name": "David", "age": 28, "city": "Boston"},
            {"id": 5, "name": "Eve", "age": 40, "city": "Chicago"},
        ]
        
        for user in users:
            db[f"user:{user['id']}"] = user
            
        yield db
        
        # 清理
        db.close()
        shutil.rmtree(temp_dir)
    
    def test_hash_index(self, db_with_data):
        """测试哈希索引"""
        db = db_with_data
        
        # 创建城市索引
        db.create_hash_index("city_index", lambda x: x.get("city"))
        
        # 查询纽约的用户
        ny_keys = db.query_index("city_index", "New York")
        assert len(ny_keys) == 2
        ny_users = [db[key] for key in ny_keys]
        assert all(user["city"] == "New York" for user in ny_users)
        
        # 查询波士顿的用户
        boston_keys = db.query_index("city_index", "Boston")
        assert len(boston_keys) == 2
        boston_users = [db[key] for key in boston_keys]
        assert all(user["city"] == "Boston" for user in boston_users)
        
        # 查询芝加哥的用户
        chicago_keys = db.query_index("city_index", "Chicago")
        assert len(chicago_keys) == 1
        chicago_users = [db[key] for key in chicago_keys]
        assert all(user["city"] == "Chicago" for user in chicago_users)
        
        # 查询不存在的城市
        empty_keys = db.query_index("city_index", "San Francisco")
        assert len(empty_keys) == 0
    
    def test_range_index(self, db_with_data):
        """测试范围索引"""
        db = db_with_data
        
        # 创建年龄索引
        db.create_range_index("age_index", lambda x: x.get("age"))
        
        # 查询25-30岁的用户
        young_keys = db.query_index("age_index", min_value=25, max_value=30, include_max=True)
        assert len(young_keys) == 3
        young_users = [db[key] for key in young_keys]
        assert all(25 <= user["age"] <= 30 for user in young_users)
        
        # 查询35岁以上的用户
        older_keys = db.query_index("age_index", min_value=35)
        assert len(older_keys) == 2
        older_users = [db[key] for key in older_keys]
        assert all(user["age"] >= 35 for user in older_users)
        
        # 查询不包含边界的范围
        mid_keys = db.query_index("age_index", min_value=25, max_value=35, include_min=True, include_max=False)
        assert len(mid_keys) == 3
        mid_users = [db[key] for key in mid_keys]
        assert all(25 <= user["age"] < 35 for user in mid_users)
    
    def test_index_updates(self, db_with_data):
        """测试索引更新"""
        db = db_with_data
        
        # 创建城市索引
        db.create_hash_index("city_index", lambda x: x.get("city"))
        
        # 初始状态
        ny_keys = db.query_index("city_index", "New York")
        assert len(ny_keys) == 2
        
        # 更新用户
        user = db["user:1"]
        user["city"] = "Chicago"
        db["user:1"] = user
        
        # 检查索引是否更新
        ny_keys = db.query_index("city_index", "New York")
        assert len(ny_keys) == 1
        assert "user:1" not in ny_keys
        
        chicago_keys = db.query_index("city_index", "Chicago")
        assert len(chicago_keys) == 2
        assert "user:1" in chicago_keys
        
        # 删除用户
        del db["user:1"]
        
        # 检查索引是否更新
        chicago_keys = db.query_index("city_index", "Chicago")
        assert len(chicago_keys) == 1
        assert "user:1" not in chicago_keys 