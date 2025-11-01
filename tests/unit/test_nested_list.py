#!/usr/bin/env python3
"""
测试 NestedDBList 功能

包括：
1. 基本列表操作
2. 嵌套数据（列表中包含字典或列表）
3. 手动创建 nested_list
4. 数据持久化
5. 字符串表示
6. RawLevelDBDict 和 CachedLevelDBDict 的兼容性
"""

import pytest
import os
import shutil
from flaxkv2 import CachedLevelDBDict, RawLevelDBDict


class TestNestedDBListBasic:
    """测试基本列表操作"""

    @pytest.fixture
    def db(self):
        """创建测试数据库"""
        db_path = "./test_nested_list_basic_db"
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

        db = CachedLevelDBDict("test_nested_list_basic_db", path=".", auto_nested=True)
        yield db

        db.close()
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

    def test_create_and_access(self, db):
        """测试创建列表并访问元素"""
        db['items'] = [1, 2, 3, 4, 5]

        assert db['items'][0] == 1
        assert db['items'][2] == 3
        assert db['items'][-1] == 5

    def test_modify_element(self, db):
        """测试修改单个元素"""
        db['items'] = [1, 2, 3, 4, 5]
        db['items'][2] = 10

        assert db['items'][2] == 10
        # 验证其他元素未受影响
        assert db['items'][0] == 1
        assert db['items'][1] == 2

    def test_list_length(self, db):
        """测试列表长度"""
        db['items'] = [1, 2, 3, 4, 5]
        items = db['items']

        assert len(items) == 5

    def test_append(self, db):
        """测试添加元素"""
        db['items'] = [1, 2, 3]
        items = db['items']

        items.append(4)
        items.append(5)

        assert len(items) == 5
        assert items[-1] == 5

    def test_delete_element(self, db):
        """测试删除元素"""
        db['items'] = [1, 2, 3, 4, 5]
        items = db['items']

        del items[0]

        assert len(items) == 4
        assert items[0] == 2

    def test_iteration(self, db):
        """测试迭代"""
        db['items'] = [1, 2, 3, 4, 5]
        items = db['items']

        result = list(items)
        assert result == [1, 2, 3, 4, 5]

    def test_insert(self, db):
        """测试插入元素"""
        db['items'] = [1, 3, 4]
        items = db['items']

        items.insert(1, 2)

        assert list(items) == [1, 2, 3, 4]

    def test_pop(self, db):
        """测试弹出元素"""
        db['items'] = [1, 2, 3, 4, 5]
        items = db['items']

        popped = items.pop()

        assert popped == 5
        assert len(items) == 4


class TestNestedDBListNested:
    """测试嵌套数据"""

    @pytest.fixture
    def db(self):
        """创建测试数据库"""
        db_path = "./test_nested_list_nested_db"
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

        db = CachedLevelDBDict("test_nested_list_nested_db", path=".", auto_nested=True)
        yield db

        db.close()
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

    def test_list_with_dicts(self, db):
        """测试包含字典的列表"""
        db['users'] = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25},
            {'name': 'Charlie', 'age': 35}
        ]

        users = db['users']

        # 访问嵌套字典
        assert users[0]['name'] == 'Alice'
        assert users[1]['age'] == 25

        # 修改嵌套字典
        users[0]['age'] = 31
        assert users[0]['age'] == 31

        # 验证其他字段未受影响
        assert users[0]['name'] == 'Alice'

    def test_list_with_lists(self, db):
        """测试包含列表的列表"""
        db['matrix'] = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

        matrix = db['matrix']

        # 访问嵌套列表
        assert matrix[0][0] == 1
        assert matrix[1][1] == 5

        # 修改嵌套列表元素
        matrix[0][0] = 10
        assert matrix[0][0] == 10


class TestNestedDBListManual:
    """测试手动创建 nested_list"""

    @pytest.fixture
    def db(self):
        """创建测试数据库"""
        db_path = "./test_nested_list_manual_db"
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

        db = CachedLevelDBDict("test_nested_list_manual_db", path=".")
        yield db

        db.close()
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

    def test_manual_nested_list(self, db):
        """测试手动创建 nested_list"""
        items = db.nested_list('items')

        # 添加元素
        items.append('apple')
        items.append('banana')
        items.append('cherry')

        assert len(items) == 3
        assert items[0] == 'apple'

        # 修改元素
        items[1] = 'blueberry'
        assert items[1] == 'blueberry'


class TestNestedDBListPersistence:
    """测试数据持久化"""

    def test_persistence(self):
        """测试列表数据持久化"""
        db_path = "./test_nested_list_persistence_db"
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

        # 创建数据库并写入数据
        db = CachedLevelDBDict("test_nested_list_persistence_db", path=".", auto_nested=True)
        db['numbers'] = [10, 20, 30, 40, 50]
        db['numbers'][2] = 100  # 修改一个元素
        db.close()

        # 重新打开数据库
        db = CachedLevelDBDict("test_nested_list_persistence_db", path=".", auto_nested=True)
        numbers = db['numbers']

        # 验证数据
        assert len(numbers) == 5
        assert numbers[0] == 10
        assert numbers[2] == 100
        assert numbers[4] == 50

        db.close()
        shutil.rmtree(db_path)


class TestNestedDBListStringRepresentation:
    """测试字符串表示"""

    @pytest.fixture
    def db(self):
        """创建测试数据库"""
        db_path = "./test_nested_list_str_db"
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

        db = CachedLevelDBDict("test_nested_list_str_db", path=".", auto_nested=True)
        yield db

        db.close()
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

    def test_empty_list_str(self, db):
        """测试空列表的字符串表示"""
        db['empty'] = []
        empty_list = db['empty']

        assert repr(empty_list) == "NestedDBList([])"
        assert str(empty_list) == "[]"

    def test_simple_list_str(self, db):
        """测试简单列表的字符串表示"""
        db['simple'] = [1, 2, 3, 4, 5]
        simple_list = db['simple']

        assert "NestedDBList([1, 2, 3, 4, 5])" == repr(simple_list)
        assert "[1, 2, 3, 4, 5]" == str(simple_list)

    def test_long_list_str(self, db):
        """测试长列表的字符串表示"""
        db['long'] = list(range(15))
        long_list = db['long']

        # repr 应该只显示前5个元素
        repr_str = repr(long_list)
        assert "... (10 more)" in repr_str

        # str 应该显示前10个元素
        str_str = str(long_list)
        assert "... and 5 more items" in str_str

    def test_nested_dict_str(self, db):
        """测试包含嵌套字典的列表的字符串表示"""
        db['users'] = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25}
        ]
        users_list = db['users']

        repr_str = repr(users_list)
        assert "NestedDBDict(...2 keys)" in repr_str

        str_str = str(users_list)
        assert "{...2 keys}" in str_str


class TestRawLevelDBDictNestedList:
    """测试 RawLevelDBDict 的 NestedDBList 支持"""

    @pytest.fixture
    def db(self):
        """创建测试数据库"""
        db_path = "./test_raw_nested_list_db"
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

        db = RawLevelDBDict("test_raw_nested_list_db", path=".", auto_nested=True)
        yield db

        db.close()
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

    def test_raw_basic_operations(self, db):
        """测试 RawLevelDBDict 的基本列表操作"""
        db['items'] = [1, 2, 3, 4, 5]

        assert db['items'][0] == 1
        assert db['items'][2] == 3

        # 修改元素
        db['items'][2] = 10
        assert db['items'][2] == 10

        # 添加元素
        items = db['items']
        items.append(6)
        assert len(items) == 6

    def test_raw_manual_nested_list(self, db):
        """测试 RawLevelDBDict 手动创建 nested_list"""
        items = db.nested_list('items')

        items.append('apple')
        items.append('banana')
        items.append('cherry')

        assert len(items) == 3
        assert items[0] == 'apple'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
