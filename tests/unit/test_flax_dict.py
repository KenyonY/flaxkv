"""
FlaxDict 字典接口单元测试
"""

import pytest
import tempfile
import shutil
from pathlib import Path

from flaxkv.core.flax_dict import FlaxDict, create_flax_dict
from flaxkv.core.config import FlaxKVConfig


@pytest.fixture
def temp_db_path():
    """临时数据库路径。"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def test_config():
    """测试配置。"""
    return FlaxKVConfig.for_testing()


@pytest.fixture
def flax_dict(temp_db_path, test_config):
    """FlaxDict实例。"""
    fdict = FlaxDict("test_dict", "local", temp_db_path, test_config)
    yield fdict
    fdict.close()


class TestFlaxDictBasic:
    """测试FlaxDict基本功能。"""
    
    def test_initialization(self, temp_db_path, test_config):
        """测试FlaxDict初始化。"""
        # 自动初始化
        fdict = FlaxDict("test_dict", "local", temp_db_path, test_config)
        assert fdict._initialized is True
        assert fdict._name == "test_dict"
        fdict.close()
        
        # 禁用自动初始化
        fdict = FlaxDict("test_dict2", "local", temp_db_path, test_config, auto_initialize=False)
        assert fdict._initialized is False
        fdict._ensure_initialized()
        assert fdict._initialized is True
        fdict.close()
    
    def test_create_flax_dict_factory(self, temp_db_path, test_config):
        """测试创建工厂函数。"""
        fdict = create_flax_dict("factory_dict", "local", temp_db_path, test_config)
        assert fdict._name == "factory_dict"
        assert fdict._initialized is True
        fdict.close()


class TestFlaxDictCoreInterface:
    """测试FlaxDict核心字典接口。"""
    
    def test_getitem_setitem(self, flax_dict):
        """测试获取和设置项目。"""
        # 设置值
        flax_dict["key1"] = "value1"
        flax_dict["key2"] = {"nested": "data"}
        flax_dict["key3"] = [1, 2, 3]
        
        # 获取值
        assert flax_dict["key1"] == "value1"
        assert flax_dict["key2"] == {"nested": "data"}
        assert flax_dict["key3"] == [1, 2, 3]
        
        # 获取不存在的键应该抛出KeyError
        with pytest.raises(KeyError):
            _ = flax_dict["nonexistent_key"]
    
    def test_delitem(self, flax_dict):
        """测试删除项目。"""
        flax_dict["delete_me"] = "will be deleted"
        assert "delete_me" in flax_dict
        
        # 删除键
        del flax_dict["delete_me"]
        assert "delete_me" not in flax_dict
        
        # 删除不存在的键应该抛出KeyError
        with pytest.raises(KeyError):
            del flax_dict["nonexistent_key"]
    
    def test_contains(self, flax_dict):
        """测试键存在检查。"""
        flax_dict["exists"] = "yes"
        
        assert "exists" in flax_dict
        assert "not_exists" not in flax_dict
    
    def test_len(self, flax_dict):
        """测试长度。"""
        assert len(flax_dict) == 0
        
        flax_dict["key1"] = "value1"
        flax_dict["key2"] = "value2"
        assert len(flax_dict) == 2
        
        del flax_dict["key1"]
        assert len(flax_dict) == 1
    
    def test_iter(self, flax_dict):
        """测试迭代。"""
        test_data = {"a": 1, "b": 2, "c": 3}
        flax_dict.mset(test_data)
        
        # 迭代键
        keys = list(flax_dict)
        assert set(keys) == set(test_data.keys())
        
        # 使用迭代器
        keys_iter = []
        for key in flax_dict:
            keys_iter.append(key)
        assert set(keys_iter) == set(test_data.keys())
    
    def test_repr_str(self, flax_dict):
        """测试字符串表示。"""
        # 空字典
        repr_str = repr(flax_dict)
        assert "FlaxDict" in repr_str
        
        # 少量项目
        flax_dict.mset({"a": 1, "b": 2})
        repr_str = repr(flax_dict)
        assert "'a': 1" in repr_str
        assert "'b': 2" in repr_str
        
        # 字符串表示与repr一致
        assert str(flax_dict) == repr(flax_dict)


class TestFlaxDictMethods:
    """测试FlaxDict字典方法。"""
    
    def test_get(self, flax_dict):
        """测试get方法。"""
        flax_dict["existing"] = "value"
        
        # 获取存在的键
        assert flax_dict.get("existing") == "value"
        
        # 获取不存在的键，返回None
        assert flax_dict.get("nonexistent") is None
        
        # 获取不存在的键，返回默认值
        assert flax_dict.get("nonexistent", "default") == "default"
    
    def test_pop(self, flax_dict):
        """测试pop方法。"""
        flax_dict["pop_me"] = "value"
        
        # 弹出存在的键
        value = flax_dict.pop("pop_me")
        assert value == "value"
        assert "pop_me" not in flax_dict
        
        # 弹出不存在的键，返回默认值
        value = flax_dict.pop("nonexistent", "default")
        assert value == "default"
        
        # 弹出不存在的键且无默认值，抛出KeyError
        with pytest.raises(KeyError):
            flax_dict.pop("nonexistent")
        
        # 测试参数错误
        flax_dict["test"] = "value"
        with pytest.raises(TypeError):
            flax_dict.pop("test", "default", "extra_arg")
    
    def test_popitem(self, flax_dict):
        """测试popitem方法。"""
        # 空字典应该抛出KeyError
        with pytest.raises(KeyError, match="dictionary is empty"):
            flax_dict.popitem()
        
        # 有数据时应该返回键值对
        flax_dict.mset({"a": 1, "b": 2})
        original_len = len(flax_dict)
        
        key, value = flax_dict.popitem()
        assert isinstance(key, str)
        assert key in {"a", "b"}
        assert value in {1, 2}
        assert len(flax_dict) == original_len - 1
        assert key not in flax_dict
    
    def test_setdefault(self, flax_dict):
        """测试setdefault方法。"""
        # 键不存在，设置默认值
        value = flax_dict.setdefault("new_key", "default_value")
        assert value == "default_value"
        assert flax_dict["new_key"] == "default_value"
        
        # 键存在，返回现有值
        flax_dict["existing"] = "existing_value"
        value = flax_dict.setdefault("existing", "default_value")
        assert value == "existing_value"
        assert flax_dict["existing"] == "existing_value"
    
    def test_update(self, flax_dict):
        """测试update方法。"""
        # 使用字典更新
        flax_dict.update({"a": 1, "b": 2})
        assert flax_dict["a"] == 1
        assert flax_dict["b"] == 2
        
        # 使用键值对列表更新
        flax_dict.update([("c", 3), ("d", 4)])
        assert flax_dict["c"] == 3
        assert flax_dict["d"] == 4
        
        # 使用关键字参数更新
        flax_dict.update(e=5, f=6)
        assert flax_dict["e"] == 5
        assert flax_dict["f"] == 6
        
        # 组合使用
        flax_dict.update({"g": 7}, h=8)
        assert flax_dict["g"] == 7
        assert flax_dict["h"] == 8
        
        # 测试参数错误
        with pytest.raises(TypeError):
            flax_dict.update({"a": 1}, {"b": 2})  # 太多位置参数
    
    def test_clear(self, flax_dict):
        """测试clear方法。"""
        flax_dict.mset({"a": 1, "b": 2, "c": 3})
        assert len(flax_dict) == 3
        
        flax_dict.clear()
        assert len(flax_dict) == 0
        assert "a" not in flax_dict
    
    def test_copy(self, flax_dict):
        """测试copy方法。"""
        test_data = {"a": 1, "b": [2, 3], "c": {"nested": "value"}}
        flax_dict.mset(test_data)
        
        copied = flax_dict.copy()
        
        # 应该是普通dict
        assert isinstance(copied, dict)
        assert copied == test_data
        
        # 修改原字典不影响拷贝
        flax_dict["d"] = 4
        assert "d" not in copied


class TestFlaxDictViews:
    """测试FlaxDict视图方法。"""
    
    def test_keys(self, flax_dict):
        """测试keys方法。"""
        test_data = {"key1": "value1", "key2": "value2", "key3": "value3"}
        flax_dict.mset(test_data)
        
        keys = list(flax_dict.keys())
        assert set(keys) == set(test_data.keys())
    
    def test_values(self, flax_dict):
        """测试values方法。"""
        test_data = {"key1": "value1", "key2": "value2", "key3": "value3"}
        flax_dict.mset(test_data)
        
        values = list(flax_dict.values())
        assert set(values) == set(test_data.values())
        
        # 空字典
        flax_dict.clear()
        values = list(flax_dict.values())
        assert values == []
    
    def test_items(self, flax_dict):
        """测试items方法。"""
        test_data = {"key1": "value1", "key2": "value2", "key3": "value3"}
        flax_dict.mset(test_data)
        
        items = list(flax_dict.items())
        assert dict(items) == test_data
        
        # 空字典
        flax_dict.clear()
        items = list(flax_dict.items())
        assert items == []


class TestFlaxDictBatchOperations:
    """测试FlaxDict批量操作。"""
    
    def test_mget(self, flax_dict):
        """测试批量获取。"""
        test_data = {"a": 1, "b": 2, "c": 3}
        flax_dict.mset(test_data)
        
        # 获取存在的键
        result = flax_dict.mget(["a", "c"])
        assert result == {"a": 1, "c": 3}
        
        # 获取部分存在的键
        result = flax_dict.mget(["a", "nonexistent", "c"])
        assert result == {"a": 1, "c": 3}
        assert "nonexistent" not in result
    
    def test_mset(self, flax_dict):
        """测试批量设置。"""
        test_data = {"batch1": "value1", "batch2": [1, 2, 3], "batch3": {"nested": "data"}}
        flax_dict.mset(test_data)
        
        for key, expected_value in test_data.items():
            assert flax_dict[key] == expected_value
    
    def test_mdelete(self, flax_dict):
        """测试批量删除。"""
        test_data = {"del1": "value1", "del2": "value2", "del3": "value3"}
        flax_dict.mset(test_data)
        
        # 删除存在的键
        deleted_count = flax_dict.mdelete(["del1", "del3"])
        assert deleted_count == 2
        assert "del1" not in flax_dict
        assert "del2" in flax_dict
        assert "del3" not in flax_dict
        
        # 删除不存在的键
        deleted_count = flax_dict.mdelete(["nonexistent1", "nonexistent2"])
        assert deleted_count == 0


class TestFlaxDictContextManager:
    """测试FlaxDict上下文管理器。"""
    
    def test_context_manager(self, temp_db_path, test_config):
        """测试上下文管理器。"""
        with FlaxDict("context_test", "local", temp_db_path, test_config) as fdict:
            fdict["test_key"] = "test_value"
            assert fdict["test_key"] == "test_value"
            assert fdict._initialized is True
        
        # 上下文退出后应该关闭
        assert fdict._initialized is False


class TestFlaxDictAdvanced:
    """测试FlaxDict高级功能。"""
    
    def test_ttl_operations(self, flax_dict):
        """测试TTL操作。"""
        # setex
        flax_dict.setex("ttl_key", "ttl_value", 3600)
        assert flax_dict["ttl_key"] == "ttl_value"
        
        # expire
        flax_dict["normal_key"] = "normal_value"
        success = flax_dict.expire("normal_key", 1800)
        assert success is True
        
        # expire 不存在的键
        success = flax_dict.expire("nonexistent", 1800)
        assert success is False
    
    def test_scan_operations(self, flax_dict):
        """测试扫描操作。"""
        test_data = {
            "user:001": {"name": "Alice"},
            "user:002": {"name": "Bob"},
            "product:001": {"name": "Book"},
            "product:002": {"name": "Pen"},
        }
        flax_dict.mset(test_data)
        
        # 前缀扫描
        user_items = list(flax_dict.scan(prefix="user:"))
        assert len(user_items) == 2
        assert all(key.startswith("user:") for key, value in user_items)
        
        # 限制数量扫描
        limited_items = list(flax_dict.scan(limit=2))
        assert len(limited_items) == 2
    
    def test_management_methods(self, flax_dict):
        """测试管理方法。"""
        # 添加一些数据
        flax_dict.mset({"mgmt1": "value1", "mgmt2": "value2"})
        
        # flush
        result = flax_dict.flush()
        assert isinstance(result, dict)
        
        # get_stats
        stats = flax_dict.get_stats()
        assert isinstance(stats, dict)
    
    def test_backup_restore(self, flax_dict, temp_db_path):
        """测试备份和恢复。"""
        # 添加测试数据
        test_data = {"backup_key1": "value1", "backup_key2": {"nested": "data"}}
        flax_dict.mset(test_data)
        
        backup_path = temp_db_path / "backup.flaxkv"
        
        # 备份
        backup_stats = flax_dict.backup(str(backup_path))
        assert isinstance(backup_stats, dict)
        assert backup_path.exists()
        
        # 清空数据
        flax_dict.clear()
        assert len(flax_dict) == 0
        
        # 恢复
        restore_stats = flax_dict.restore(str(backup_path))
        assert isinstance(restore_stats, dict)
        
        # 验证数据
        for key, expected_value in test_data.items():
            assert flax_dict[key] == expected_value


class TestFlaxDictErrorHandling:
    """测试FlaxDict错误处理。"""
    
    def test_key_errors(self, flax_dict):
        """测试KeyError处理。"""
        # __getitem__
        with pytest.raises(KeyError):
            _ = flax_dict["nonexistent"]
        
        # __delitem__
        with pytest.raises(KeyError):
            del flax_dict["nonexistent"]
        
        # pop
        with pytest.raises(KeyError):
            flax_dict.pop("nonexistent")
        
        # popitem on empty dict
        with pytest.raises(KeyError):
            flax_dict.popitem()
    
    def test_type_errors(self, flax_dict):
        """测试TypeError处理。"""
        # pop with too many args
        flax_dict["test"] = "value"
        with pytest.raises(TypeError):
            flax_dict.pop("test", "default", "extra")
        
        # update with too many positional args
        with pytest.raises(TypeError):
            flax_dict.update({"a": 1}, {"b": 2})


class TestFlaxDictDataTypes:
    """测试FlaxDict数据类型支持。"""
    
    def test_various_data_types(self, flax_dict):
        """测试各种数据类型。"""
        test_data = {
            "string": "hello world",
            "integer": 42,
            "float": 3.14159,
            "boolean_true": True,
            "boolean_false": False,
            "none_value": None,
            "list": [1, "two", 3.0, None],
            "dict": {
                "nested_str": "nested_value",
                "nested_num": 100,
                "nested_list": [1, 2, 3]
            },
            "tuple": (1, 2, "three"),
            "complex": complex(1, 2),
        }
        
        # 设置各种数据类型
        for key, value in test_data.items():
            flax_dict[key] = value
        
        # 验证数据类型正确性
        for key, expected_value in test_data.items():
            actual_value = flax_dict[key]
            assert actual_value == expected_value
            assert type(actual_value) == type(expected_value)


class TestFlaxDictCompatibility:
    """测试FlaxDict与标准dict的兼容性。"""
    
    def test_dict_like_behavior(self, flax_dict):
        """测试类dict行为。"""
        # 测试基本dict操作
        flax_dict["a"] = 1
        flax_dict["b"] = 2
        
        # len
        assert len(flax_dict) == 2
        
        # in operator
        assert "a" in flax_dict
        assert "c" not in flax_dict
        
        # iteration
        keys = [k for k in flax_dict]
        assert set(keys) == {"a", "b"}
        
        # get with default
        assert flax_dict.get("a") == 1
        assert flax_dict.get("c", "default") == "default"
    
    def test_mutable_mapping_interface(self, flax_dict):
        """测试MutableMapping接口。"""
        from collections.abc import MutableMapping
        
        # FlaxDict应该是MutableMapping的实例
        assert isinstance(flax_dict, MutableMapping)
        
        # 应该有所有必需的方法
        required_methods = [
            '__getitem__', '__setitem__', '__delitem__',
            '__iter__', '__len__', '__contains__',
            'keys', 'values', 'items', 'get', 'pop',
            'popitem', 'clear', 'update', 'setdefault'
        ]
        
        for method in required_methods:
            assert hasattr(flax_dict, method)