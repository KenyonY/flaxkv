"""
CachedLevelDBDict 写缓冲功能测试
"""

import pytest
import os
import shutil
import numpy as np
from flaxkv2 import CachedLevelDBDict


class TestCachedLevelDBDictWriteBuffer:
    """CachedLevelDBDict 写缓冲功能测试"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        """每个测试前后清理"""
        self.test_dir = './test_cached_write_buffer_data'
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir, exist_ok=True)

        yield

        # 清理
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_write_buffer_disabled_by_default(self):
        """测试写缓冲默认禁用"""
        db = CachedLevelDBDict(
            'test_db',
            path=self.test_dir,
            enable_ttl_cleanup=False
        )

        assert not db._write_buffer_enabled
        assert db._write_buffer is None

        db.close()

    def test_write_buffer_enabled(self):
        """测试写缓冲启用"""
        db = CachedLevelDBDict(
            'test_db',
            path=self.test_dir,
            enable_ttl_cleanup=False,
            enable_write_buffer=True,
            write_buffer_size=10
        )

        assert db._write_buffer_enabled
        assert db._write_buffer is not None
        assert db._write_buffer._max_size == 10

        db.close()

    def test_basic_write_read_with_buffer(self):
        """测试基本的写入和读取（带缓冲）"""
        db = CachedLevelDBDict(
            'test_db',
            path=self.test_dir,
            enable_ttl_cleanup=False,
            enable_write_buffer=True,
            write_buffer_size=10
        )

        # 写入数据
        db['key1'] = 'value1'
        db['key2'] = 'value2'
        db['key3'] = 'value3'

        # 立即读取（应该从缓冲区读取）
        assert db['key1'] == 'value1'
        assert db['key2'] == 'value2'
        assert db['key3'] == 'value3'

        db.close()

    def test_persistence_after_close(self):
        """测试关闭后数据持久化"""
        # 写入数据
        db = CachedLevelDBDict(
            'test_db',
            path=self.test_dir,
            enable_ttl_cleanup=False,
            enable_write_buffer=True,
            write_buffer_size=100
        )

        db['key1'] = 'value1'
        db['key2'] = 'value2'
        db.close()

        # 重新打开，检查数据
        db2 = CachedLevelDBDict(
            'test_db',
            path=self.test_dir,
            enable_ttl_cleanup=False,
            enable_write_buffer=False
        )

        assert db2['key1'] == 'value1'
        assert db2['key2'] == 'value2'

        db2.close()

    def test_auto_flush_on_size_threshold(self):
        """测试达到大小阈值时自动刷新"""
        db = CachedLevelDBDict(
            'test_db',
            path=self.test_dir,
            enable_ttl_cleanup=False,
            enable_write_buffer=True,
            write_buffer_size=5  # 小阈值
        )

        # 写入6条数据，应该触发至少一次刷新
        for i in range(6):
            db[f'key_{i}'] = f'value_{i}'

        # 验证数据可读
        for i in range(6):
            assert db[f'key_{i}'] == f'value_{i}'

        db.close()

    def test_manual_flush(self):
        """测试手动刷新"""
        db = CachedLevelDBDict(
            'test_db',
            path=self.test_dir,
            enable_ttl_cleanup=False,
            enable_write_buffer=True,
            write_buffer_size=100
        )

        # 写入数据
        db['key1'] = 'value1'
        db['key2'] = 'value2'

        # 手动刷新
        db.flush()

        # 关闭后重新打开，数据应该存在
        db.close()

        db2 = CachedLevelDBDict(
            'test_db',
            path=self.test_dir,
            enable_ttl_cleanup=False,
            enable_write_buffer=False
        )

        assert db2['key1'] == 'value1'
        assert db2['key2'] == 'value2'

        db2.close()

    def test_delete_with_buffer(self):
        """测试删除操作（带缓冲）"""
        db = CachedLevelDBDict(
            'test_db',
            path=self.test_dir,
            enable_ttl_cleanup=False,
            enable_write_buffer=True,
            write_buffer_size=10
        )

        # 写入和删除
        db['key1'] = 'value1'
        db['key2'] = 'value2'
        del db['key1']

        # 检查删除
        assert 'key1' not in db
        assert 'key2' in db

        db.close()

    def test_overwrite_with_buffer(self):
        """测试覆盖写入（带缓冲）"""
        db = CachedLevelDBDict(
            'test_db',
            path=self.test_dir,
            enable_ttl_cleanup=False,
            enable_write_buffer=True,
            write_buffer_size=10
        )

        # 写入后覆盖
        db['key1'] = 'value1'
        db['key1'] = 'value2'
        db['key1'] = 'value3'

        assert db['key1'] == 'value3'

        db.close()

    def test_numpy_array_with_buffer(self):
        """测试 numpy 数组（带缓冲）"""
        db = CachedLevelDBDict(
            'test_db',
            path=self.test_dir,
            enable_ttl_cleanup=False,
            enable_write_buffer=True,
            write_buffer_size=10
        )

        # 写入 numpy 数组
        arr1 = np.array([1, 2, 3, 4, 5])
        arr2 = np.random.rand(100, 100)

        db['arr1'] = arr1
        db['arr2'] = arr2

        # 读取并验证
        assert np.array_equal(db['arr1'], arr1)
        assert np.array_equal(db['arr2'], arr2)

        db.close()

    def test_ttl_with_buffer(self):
        """测试 TTL（带缓冲）"""
        import time

        db = CachedLevelDBDict(
            'test_db',
            path=self.test_dir,
            enable_ttl_cleanup=False,
            enable_write_buffer=True,
            write_buffer_size=10
        )

        # 写入带TTL的数据
        db.set('key1', 'value1', ttl=2)  # 2秒过期

        # 立即读取应该成功
        assert db['key1'] == 'value1'

        # 手动刷新到数据库
        db.flush()

        # 等待过期
        time.sleep(2.5)

        # 读取应该失败（因为数据已经在数据库中过期）
        with pytest.raises(KeyError):
            _ = db['key1']

        db.close()

    def test_buffer_with_read_cache(self):
        """测试写缓冲与读缓存的交互"""
        db = CachedLevelDBDict(
            'test_db',
            path=self.test_dir,
            enable_ttl_cleanup=False,
            enable_write_buffer=True,
            write_buffer_size=10,
            read_cache_size=100  # 同时启用读缓存
        )

        # 写入数据
        db['key1'] = 'value1'

        # 读取（应该从写缓冲区读取）
        assert db['key1'] == 'value1'

        # 刷新
        db.flush()

        # 再次读取（应该从数据库或读缓存读取）
        assert db['key1'] == 'value1'

        db.close()

    def test_large_batch_write(self):
        """测试大批量写入"""
        db = CachedLevelDBDict(
            'test_db',
            path=self.test_dir,
            enable_ttl_cleanup=False,
            enable_write_buffer=True,
            write_buffer_size=100
        )

        # 写入1000条数据
        n = 1000
        for i in range(n):
            db[f'key_{i}'] = f'value_{i}'

        # 验证所有数据
        for i in range(n):
            assert db[f'key_{i}'] == f'value_{i}'

        db.close()

        # 重新打开验证持久化
        db2 = CachedLevelDBDict(
            'test_db',
            path=self.test_dir,
            enable_ttl_cleanup=False
        )

        for i in range(n):
            assert db2[f'key_{i}'] == f'value_{i}'

        db2.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
