"""
测试 NestedDBDict 的改进功能

包含:
1. MutableMapping 接口
2. __eq__ / __ne__ 比较操作
3. copy() 方法
4. 改进的 __repr__()
"""

import pytest
import tempfile
import shutil
import json
from collections.abc import Mapping, MutableMapping
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict
from flaxkv2.core.nested_dict import NestedDBDict


class TestNestedDBDictImprovements:
    """测试 NestedDBDict 的改进功能"""

    @pytest.fixture
    def db(self):
        """创建临时数据库"""
        tmpdir = tempfile.mkdtemp()
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True, auto_nested=True)
        yield db
        db.close()
        shutil.rmtree(tmpdir)

    def test_mutable_mapping_interface(self, db):
        """测试 MutableMapping 接口"""
        db['config'] = {'host': 'localhost', 'port': 8080}
        config = db['config']

        # 应该是 Mapping 和 MutableMapping 的实例
        assert isinstance(config, Mapping)
        assert isinstance(config, MutableMapping)

        # 但不是 dict 的实例
        assert not isinstance(config, dict)

    def test_equality_with_dict(self, db):
        """测试与普通 dict 的相等比较"""
        original = {'host': 'localhost', 'port': 8080, 'debug': True}
        db['config'] = original.copy()
        config = db['config']

        # 应该相等
        assert config == original
        assert original == config  # 反向比较

        # 不等的情况
        assert config != {'host': 'localhost'}
        assert config != {'host': 'localhost', 'port': 9999, 'debug': True}

    def test_equality_with_nested_db_dict(self, db):
        """测试两个 NestedDBDict 的比较"""
        db['config1'] = {'host': 'localhost', 'port': 8080}
        db['config2'] = {'host': 'localhost', 'port': 8080}
        db['config3'] = {'host': 'localhost', 'port': 9999}

        config1 = db['config1']
        config2 = db['config2']
        config3 = db['config3']

        # 相同内容应该相等
        assert config1 == config2
        assert config2 == config1

        # 不同内容应该不等
        assert config1 != config3

    def test_equality_with_nested_dicts(self, db):
        """测试深层嵌套字典的比较"""
        original = {
            'database': {
                'host': 'localhost',
                'port': 5432,
                'credentials': {
                    'user': 'admin',
                    'password': 'secret'
                }
            }
        }
        db['app'] = original
        app = db['app']

        # 应该相等
        assert app == original

    def test_copy_method(self, db):
        """测试 copy() 方法"""
        db['config'] = {'host': 'localhost', 'port': 8080, 'debug': True}
        config = db['config']

        # copy() 应该返回普通 dict
        copied = config.copy()
        assert isinstance(copied, dict)
        assert not isinstance(copied, NestedDBDict)

        # 内容应该相同
        assert copied == {'host': 'localhost', 'port': 8080, 'debug': True}

    def test_copy_nested_dict(self, db):
        """测试嵌套字典的 copy()"""
        db['app'] = {
            'database': {
                'host': 'localhost',
                'port': 5432
            },
            'cache': {
                'redis': 'localhost:6379'
            }
        }
        app = db['app']

        # copy() 应该递归转换
        copied = app.copy()
        assert isinstance(copied, dict)
        assert isinstance(copied['database'], dict)
        assert isinstance(copied['cache'], dict)

        # 内容应该相同
        expected = {
            'database': {'host': 'localhost', 'port': 5432},
            'cache': {'redis': 'localhost:6379'}
        }
        assert copied == expected

    def test_improved_repr(self, db):
        """测试改进的 __repr__()"""
        # 小字典：显示全部
        db['small'] = {'a': 1, 'b': 2, 'c': 3}
        small = db['small']
        repr_str = repr(small)

        assert 'NestedDBDict' in repr_str
        assert "'a'" in repr_str or '"a"' in repr_str
        assert '...' not in repr_str  # 小字典不应该有省略

        # 大字典：显示前5个+省略
        db['large'] = {f'key{i}': i for i in range(10)}
        large = db['large']
        repr_str = repr(large)

        assert 'NestedDBDict' in repr_str
        assert '...' in repr_str  # 应该有省略
        assert '5 more' in repr_str  # 应该显示剩余数量

    def test_json_serialization_via_copy(self, db):
        """测试通过 copy() 进行 JSON 序列化"""
        db['config'] = {
            'host': 'localhost',
            'port': 8080,
            'debug': True,
            'settings': {
                'timeout': 30,
                'retry': 3
            }
        }
        config = db['config']

        # 直接序列化应该失败
        with pytest.raises(TypeError):
            json.dumps(config)

        # 通过 copy() 应该成功
        json_str = json.dumps(config.copy())
        parsed = json.loads(json_str)

        assert parsed['host'] == 'localhost'
        assert parsed['port'] == 8080
        assert parsed['debug'] is True
        assert parsed['settings']['timeout'] == 30

    def test_dict_conversion(self, db):
        """测试 dict() 转换"""
        db['config'] = {'host': 'localhost', 'port': 8080}
        config = db['config']

        # dict() 应该工作
        d = dict(config)
        assert isinstance(d, dict)
        assert d == {'host': 'localhost', 'port': 8080}

    def test_in_operator_still_works(self, db):
        """确保 in 操作符仍然正常工作"""
        db['config'] = {'host': 'localhost', 'port': 8080}
        config = db['config']

        assert 'host' in config
        assert 'port' in config
        assert 'nonexistent' not in config

    def test_len_still_works(self, db):
        """确保 len() 仍然正常工作"""
        db['config'] = {'host': 'localhost', 'port': 8080, 'debug': True}
        config = db['config']

        assert len(config) == 3

    def test_iteration_still_works(self, db):
        """确保迭代仍然正常工作"""
        db['config'] = {'host': 'localhost', 'port': 8080, 'debug': True}
        config = db['config']

        keys = list(config.keys())
        assert len(keys) == 3
        assert 'host' in keys
        assert 'port' in keys
        assert 'debug' in keys

    def test_backward_compatibility_with_to_dict(self, db):
        """确保 to_dict() 仍然正常工作"""
        db['config'] = {'host': 'localhost', 'port': 8080}
        config = db['config']

        # to_dict() 和 copy() 应该返回相同结果
        dict1 = config.to_dict()
        dict2 = config.copy()

        assert dict1 == dict2
        assert isinstance(dict1, dict)
        assert isinstance(dict2, dict)


class TestMappingProtocol:
    """测试 Mapping 协议的完整性"""

    @pytest.fixture
    def nested_dict(self):
        """创建临时 NestedDBDict"""
        tmpdir = tempfile.mkdtemp()
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True, auto_nested=True)
        db['test'] = {'a': 1, 'b': 2, 'c': 3}
        nested = db['test']
        yield nested
        db.close()
        shutil.rmtree(tmpdir)

    def test_get_method(self, nested_dict):
        """测试 get() 方法"""
        assert nested_dict.get('a') == 1
        assert nested_dict.get('nonexistent') is None
        assert nested_dict.get('nonexistent', 'default') == 'default'

    def test_keys_method(self, nested_dict):
        """测试 keys() 方法"""
        keys = list(nested_dict.keys())
        assert set(keys) == {'a', 'b', 'c'}

    def test_values_method(self, nested_dict):
        """测试 values() 方法"""
        values = list(nested_dict.values())
        assert set(values) == {1, 2, 3}

    def test_items_method(self, nested_dict):
        """测试 items() 方法"""
        items = dict(nested_dict.items())
        assert items == {'a': 1, 'b': 2, 'c': 3}

    def test_update_method(self, nested_dict):
        """测试 update() 方法"""
        nested_dict.update({'d': 4, 'e': 5})
        assert nested_dict['d'] == 4
        assert nested_dict['e'] == 5

    def test_pop_method(self, nested_dict):
        """测试 pop() 方法"""
        value = nested_dict.pop('a')
        assert value == 1
        assert 'a' not in nested_dict

    def test_setdefault_method(self, nested_dict):
        """测试 setdefault() 方法"""
        # 已存在的键
        value = nested_dict.setdefault('a', 999)
        assert value == 1

        # 不存在的键
        value = nested_dict.setdefault('new_key', 100)
        assert value == 100
        assert nested_dict['new_key'] == 100

    def test_clear_method(self, nested_dict):
        """测试 clear() 方法"""
        nested_dict.clear()
        assert len(nested_dict) == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
