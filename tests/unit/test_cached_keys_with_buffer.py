"""
测试 CachedLevelDBDict 在写缓冲模式下的 keys() 方法

确保 keys() 能够正确返回：
1. 数据库中的键
2. 缓存中的 dirty 键（还未刷新到数据库）
3. 排除已删除的键（还未刷新到数据库）
"""

import pytest
import shutil
import os
from flaxkv2 import CachedLevelDBDict


class TestCachedKeysWithBuffer:
    """测试写缓冲模式下的 keys() 方法"""

    def setup_method(self):
        """每个测试前的设置"""
        self.db_name = "test_cached_keys_buffer"
        self.db_path = f"./{self.db_name}"
        if os.path.exists(self.db_path):
            shutil.rmtree(self.db_path)

    def teardown_method(self):
        """每个测试后的清理"""
        if os.path.exists(self.db_path):
            shutil.rmtree(self.db_path)

    def test_keys_with_dirty_cache(self):
        """测试 keys() 包含缓存中的 dirty 数据"""
        db = CachedLevelDBDict(
            self.db_name, ".",
            enable_write_buffer=True,
            write_buffer_size=100,
            async_flush=False
        )

        # 写入数据到缓存（dirty）
        db['key1'] = 'value1'
        db['key2'] = 'value2'
        db['key3'] = 'value3'

        # keys() 应该包含缓存中的数据
        keys = db.keys()
        assert set(keys) == {'key1', 'key2', 'key3'}

        db.close()

    def test_keys_exclude_deleted(self):
        """测试 keys() 排除已删除的键"""
        db = CachedLevelDBDict(
            self.db_name, ".",
            enable_write_buffer=True,
            write_buffer_size=100
        )

        # 写入并刷新
        db['key1'] = 'value1'
        db['key2'] = 'value2'
        db['key3'] = 'value3'
        db.flush()

        # 删除一个键（标记删除，但未刷新）
        del db['key2']

        # keys() 应该排除已删除的键
        keys = db.keys()
        assert set(keys) == {'key1', 'key3'}
        assert 'key2' not in keys

        db.close()

    def test_keys_mixed_db_and_cache(self):
        """测试混合场景：数据库中有数据 + 缓存中有新数据"""
        db = CachedLevelDBDict(
            self.db_name, ".",
            enable_write_buffer=True,
            write_buffer_size=100
        )

        # 写入并刷新到数据库
        db['key1'] = 'value1'
        db['key2'] = 'value2'
        db.flush()

        # 新数据在缓存中（dirty）
        db['key3'] = 'value3'
        db['key4'] = 'value4'

        # keys() 应该包含数据库和缓存的数据
        keys = db.keys()
        assert set(keys) == {'key1', 'key2', 'key3', 'key4'}

        db.close()

    def test_keys_no_duplicates(self):
        """测试 keys() 去重（数据库和缓存中有相同的键）"""
        db = CachedLevelDBDict(
            self.db_name, ".",
            enable_write_buffer=True,
            write_buffer_size=100
        )

        # 写入并刷新
        db['key1'] = 'value1'
        db.flush()

        # 更新同一个键（在缓存中标记为 dirty）
        db['key1'] = 'new_value1'

        # keys() 中 key1 应该只出现一次
        keys = db.keys()
        assert keys.count('key1') == 1
        assert 'key1' in keys

        db.close()

    def test_print_with_buffer(self):
        """测试 print(db) 在写缓冲模式下显示正确"""
        db = CachedLevelDBDict(
            self.db_name, ".",
            enable_write_buffer=True,
            write_buffer_size=100
        )

        # 写入数据到缓存
        db['key1'] = 'value1'
        db['key2'] = 'value2'
        db['key3'] = 'value3'

        # str(db) 应该显示所有数据
        db_str = str(db)
        assert 'key1' in db_str
        assert 'key2' in db_str
        assert 'key3' in db_str
        assert 'value1' in db_str
        assert 'value2' in db_str
        assert 'value3' in db_str

        db.close()

    def test_items_with_buffer(self):
        """测试 items() 在写缓冲模式下正确工作"""
        db = CachedLevelDBDict(
            self.db_name, ".",
            enable_write_buffer=True,
            write_buffer_size=100
        )

        # 写入数据到缓存
        db['key1'] = 'value1'
        db['key2'] = 'value2'

        # items() 应该返回所有键值对
        items = dict(db.items())
        assert items == {'key1': 'value1', 'key2': 'value2'}

        db.close()

    def test_len_with_buffer(self):
        """测试 len(db) 在写缓冲模式下正确"""
        db = CachedLevelDBDict(
            self.db_name, ".",
            enable_write_buffer=True,
            write_buffer_size=100
        )

        # 写入数据到缓存
        db['key1'] = 'value1'
        db['key2'] = 'value2'
        db['key3'] = 'value3'

        # len(db) 应该正确
        assert len(db) == 3

        # 删除一个键
        del db['key2']
        assert len(db) == 2

        db.close()

    def test_keys_after_delete_and_add(self):
        """测试删除后再添加相同的键"""
        db = CachedLevelDBDict(
            self.db_name, ".",
            enable_write_buffer=True,
            write_buffer_size=100
        )

        # 写入并刷新
        db['key1'] = 'value1'
        db.flush()

        # 删除
        del db['key1']
        assert 'key1' not in db.keys()

        # 重新添加
        db['key1'] = 'new_value1'
        assert 'key1' in db.keys()
        assert db['key1'] == 'new_value1'

        db.close()

    def test_readonly_cache_mode(self):
        """测试只读缓存模式（写直达数据库）"""
        db = CachedLevelDBDict(
            self.db_name, ".",
            read_cache_size=1000,
            enable_write_buffer=False  # 不启用写缓冲
        )

        # 写入（直接到数据库）
        db['key1'] = 'value1'
        db['key2'] = 'value2'

        # keys() 应该正确
        keys = db.keys()
        assert set(keys) == {'key1', 'key2'}

        db.close()
