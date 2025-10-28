"""
NestedDBDict 单元测试
"""
import unittest
import tempfile
import shutil
import numpy as np
from flaxkv2 import RawLevelDBDict


class TestNestedDBDict(unittest.TestCase):
    """NestedDBDict 功能测试"""

    def setUp(self):
        """每个测试前创建临时数据库"""
        self.tmpdir = tempfile.mkdtemp()
        self.db = RawLevelDBDict('test_nested', path=self.tmpdir, rebuild=True)

    def tearDown(self):
        """每个测试后清理"""
        self.db.close()
        shutil.rmtree(self.tmpdir)

    def test_basic_operations(self):
        """测试基础操作：get, set, del"""
        nested = self.db.nested('test')

        # 设置值
        nested['key1'] = 'value1'
        nested['key2'] = 123
        nested['key3'] = [1, 2, 3]

        # 读取值
        self.assertEqual(nested['key1'], 'value1')
        self.assertEqual(nested['key2'], 123)
        self.assertEqual(nested['key3'], [1, 2, 3])

        # 删除
        del nested['key1']
        with self.assertRaises(KeyError):
            _ = nested['key1']

    def test_contains(self):
        """测试 in 操作符"""
        nested = self.db.nested('test')

        nested['key1'] = 'value1'
        self.assertTrue('key1' in nested)
        self.assertFalse('key2' in nested)

    def test_iteration(self):
        """测试迭代操作"""
        nested = self.db.nested('test')

        # 设置多个键值对
        data = {'a': 1, 'b': 2, 'c': 3}
        for k, v in data.items():
            nested[k] = v

        # 测试 keys()
        keys = set(nested.keys())
        self.assertEqual(keys, {'a', 'b', 'c'})

        # 测试 values()
        values = list(nested.values())
        self.assertEqual(sorted(values), [1, 2, 3])

        # 测试 items()
        items = dict(nested.items())
        self.assertEqual(items, data)

    def test_len(self):
        """测试 len() 函数"""
        nested = self.db.nested('test')

        self.assertEqual(len(nested), 0)

        nested['k1'] = 1
        nested['k2'] = 2
        self.assertEqual(len(nested), 2)

        del nested['k1']
        self.assertEqual(len(nested), 1)

    def test_get_method(self):
        """测试 get() 方法"""
        nested = self.db.nested('test')

        nested['key1'] = 'value1'

        self.assertEqual(nested.get('key1'), 'value1')
        self.assertIsNone(nested.get('key2'))
        self.assertEqual(nested.get('key2', 'default'), 'default')

    def test_update(self):
        """测试 update() 方法"""
        nested = self.db.nested('test')

        # 使用字典更新
        nested.update({'a': 1, 'b': 2})
        self.assertEqual(nested['a'], 1)
        self.assertEqual(nested['b'], 2)

        # 使用关键字参数更新
        nested.update(c=3, d=4)
        self.assertEqual(nested['c'], 3)
        self.assertEqual(nested['d'], 4)

    def test_clear(self):
        """测试 clear() 方法"""
        nested = self.db.nested('test')

        nested.update({'a': 1, 'b': 2, 'c': 3})
        self.assertEqual(len(nested), 3)

        nested.clear()
        self.assertEqual(len(nested), 0)

    def test_setdefault(self):
        """测试 setdefault() 方法"""
        nested = self.db.nested('test')

        # 键不存在时设置默认值
        result = nested.setdefault('key1', 'default')
        self.assertEqual(result, 'default')
        self.assertEqual(nested['key1'], 'default')

        # 键存在时返回现有值
        result = nested.setdefault('key1', 'new_default')
        self.assertEqual(result, 'default')
        self.assertEqual(nested['key1'], 'default')

    def test_pop(self):
        """测试 pop() 方法"""
        nested = self.db.nested('test')

        nested['key1'] = 'value1'

        # pop 存在的键
        result = nested.pop('key1')
        self.assertEqual(result, 'value1')
        self.assertFalse('key1' in nested)

        # pop 不存在的键（带默认值）
        result = nested.pop('key2', 'default')
        self.assertEqual(result, 'default')

    def test_to_dict(self):
        """测试 to_dict() 方法"""
        nested = self.db.nested('test')

        data = {'a': 1, 'b': 2, 'c': 3}
        nested.update(data)

        result = nested.to_dict()
        self.assertEqual(result, data)

    def test_numpy_arrays(self):
        """测试存储 NumPy 数组"""
        nested = self.db.nested('arrays')

        arr1 = np.array([1, 2, 3, 4, 5])
        arr2 = np.random.randn(10, 10)

        nested['arr1'] = arr1
        nested['arr2'] = arr2

        # 读取并验证
        retrieved_arr1 = nested['arr1']
        retrieved_arr2 = nested['arr2']

        np.testing.assert_array_equal(retrieved_arr1, arr1)
        np.testing.assert_array_equal(retrieved_arr2, arr2)

    def test_complex_values(self):
        """测试复杂数据类型"""
        nested = self.db.nested('complex')

        # 嵌套字典
        nested['config'] = {
            'settings': {'theme': 'dark', 'lang': 'en'},
            'features': ['feature1', 'feature2']
        }

        # 列表
        nested['items'] = [1, 'two', 3.0, {'four': 4}]

        # 读取并验证
        config = nested['config']
        self.assertEqual(config['settings']['theme'], 'dark')
        self.assertEqual(config['features'], ['feature1', 'feature2'])

        items = nested['items']
        self.assertEqual(items, [1, 'two', 3.0, {'four': 4}])

    def test_multiple_nested_dicts(self):
        """测试多个独立的嵌套字典"""
        user1 = self.db.nested('user:1')
        user2 = self.db.nested('user:2')

        user1['name'] = 'Alice'
        user2['name'] = 'Bob'

        self.assertEqual(user1['name'], 'Alice')
        self.assertEqual(user2['name'], 'Bob')

        # 修改一个不影响另一个
        user1['name'] = 'Alice2'
        self.assertEqual(user1['name'], 'Alice2')
        self.assertEqual(user2['name'], 'Bob')

    def test_prefix_isolation(self):
        """测试不同前缀的隔离性"""
        nested1 = self.db.nested('prefix1')
        nested2 = self.db.nested('prefix2')

        nested1['key'] = 'value1'
        nested2['key'] = 'value2'

        # 相同的键名，不同的前缀，互不影响
        self.assertEqual(nested1['key'], 'value1')
        self.assertEqual(nested2['key'], 'value2')

    def test_type_constraints(self):
        """测试键类型约束"""
        nested = self.db.nested('test')

        # 只支持字符串键
        with self.assertRaises(TypeError):
            nested[123] = 'value'

        with self.assertRaises(TypeError):
            _ = nested[123]

    def test_persistence(self):
        """测试数据持久化"""
        nested = self.db.nested('persist')
        nested['key1'] = 'value1'
        nested['key2'] = 123

        # 关闭并重新打开数据库
        self.db.close()
        self.db = RawLevelDBDict('test_nested', path=self.tmpdir)

        # 数据应该仍然存在
        nested = self.db.nested('persist')
        self.assertEqual(nested['key1'], 'value1')
        self.assertEqual(nested['key2'], 123)

    def test_repr_and_str(self):
        """测试字符串表示"""
        nested = self.db.nested('test')
        nested.update({'a': 1, 'b': 2})

        repr_str = repr(nested)
        self.assertIn('NestedDBDict', repr_str)
        self.assertIn('test:', repr_str)

        str_str = str(nested)
        self.assertIn('NestedDBDict', str_str)


if __name__ == '__main__':
    unittest.main()
