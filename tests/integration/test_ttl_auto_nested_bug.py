"""
测试 TTL + auto_nested 的交互bug

Bug描述：
当使用 auto_nested=True 且设置了TTL，在TTL过期后：
- 第一次打印db会显示 error=KeyError('key')
- 第二次打印才正常显示为空

根因：keys()方法没有检查TTL是否过期
"""

import pytest
import tempfile
import shutil
import time
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict


class TestTTLAutoNestedInteraction:
    """测试 TTL 和 auto_nested 的交互"""

    def test_ttl_expired_keys_not_returned(self):
        """
        测试TTL过期后，keys()不应该返回已过期的键

        这是核心bug：keys()应该自动过滤TTL过期的键
        """
        tmpdir = tempfile.mkdtemp()
        try:
            db = RawLevelDBDict('test', path=tmpdir, auto_nested=True, rebuild=True)

            # 设置一个嵌套字典并设置短TTL
            db['temp'] = {'data': 123}
            db.set_ttl('temp', 1)  # 1秒过期

            # 立即检查：应该存在
            assert 'temp' in db.keys()

            # 等待过期
            time.sleep(1.1)

            # 过期后：keys()应该不返回已过期的键
            keys = db.keys()
            assert 'temp' not in keys, "过期的键不应该出现在keys()中"

            db.close()
        finally:
            shutil.rmtree(tmpdir)

    def test_ttl_expired_repr_no_error(self):
        """
        测试TTL过期后，__repr__()不应该抛出错误

        这是用户遇到的bug：__repr__显示 error=KeyError
        """
        tmpdir = tempfile.mkdtemp()
        try:
            db = RawLevelDBDict('test', path=tmpdir, auto_nested=True, rebuild=True)

            # 设置一个嵌套字典并设置短TTL
            db['temp'] = {'key': 'value'}
            db.set_ttl('temp', 1)

            # 等待过期
            time.sleep(1.1)

            # 第一次repr应该正常显示为空，不应该有error
            repr_str = repr(db)
            assert 'error' not in repr_str.lower(), f"repr不应该包含error: {repr_str}"
            assert 'items={}' in repr_str or '0 items' in repr_str, f"应该显示为空: {repr_str}"

            db.close()
        finally:
            shutil.rmtree(tmpdir)

    def test_ttl_expired_items_no_error(self):
        """
        测试TTL过期后，items()不应该抛出错误
        """
        tmpdir = tempfile.mkdtemp()
        try:
            db = RawLevelDBDict('test', path=tmpdir, auto_nested=True, rebuild=True)

            # 设置多个键，其中一个有TTL
            db['permanent'] = {'data': 'stays'}
            db['temp'] = {'data': 'goes'}
            db.set_ttl('temp', 1)

            # 等待temp过期
            time.sleep(1.1)

            # items()应该只返回未过期的键
            items = db.items()
            keys = [k for k, v in items]

            assert 'permanent' in keys
            assert 'temp' not in keys, "过期的键不应该出现在items()中"

            db.close()
        finally:
            shutil.rmtree(tmpdir)

    def test_ttl_expired_values_no_error(self):
        """
        测试TTL过期后，values()不应该抛出错误
        """
        tmpdir = tempfile.mkdtemp()
        try:
            db = RawLevelDBDict('test', path=tmpdir, auto_nested=True, rebuild=True)

            db['temp'] = {'data': 123}
            db.set_ttl('temp', 1)

            # 等待过期
            time.sleep(1.1)

            # values()应该返回空列表，不应该抛出异常
            values = db.values()
            assert len(values) == 0

            db.close()
        finally:
            shutil.rmtree(tmpdir)

    def test_ttl_expired_len_correct(self):
        """
        测试TTL过期后，len()返回正确的计数
        """
        tmpdir = tempfile.mkdtemp()
        try:
            db = RawLevelDBDict('test', path=tmpdir, auto_nested=False, rebuild=True)

            db['a'] = 'value_a'
            db['b'] = 'value_b'
            db.set_ttl('a', 1)

            # 立即检查：应该有2个
            assert len(db) == 2

            # 等待a过期
            time.sleep(1.1)

            # 过期后：应该只有1个
            assert len(db) == 1

            db.close()
        finally:
            shutil.rmtree(tmpdir)

    def test_ttl_with_nested_marker_cleanup(self):
        """
        测试TTL过期时，嵌套标记键也应该被正确清理

        这是bug的根本原因：__nested__:key 标记键没有被及时清理
        """
        tmpdir = tempfile.mkdtemp()
        try:
            db = RawLevelDBDict('test', path=tmpdir, auto_nested=True, rebuild=True)

            # 设置嵌套字典
            db['nested'] = {'a': 1, 'b': 2}
            db.set_ttl('nested', 1)

            # 验证标记键存在（通过检查keys()）
            keys_before = db.keys()
            assert 'nested' in keys_before, "设置后应该能看到nested键"

            # 等待过期
            time.sleep(1.1)

            # keys()应该不再返回nested（修复后的行为）
            keys_after = db.keys()
            assert 'nested' not in keys_after, "过期后keys()不应该返回nested"

            # 访问一次（触发物理删除）
            try:
                _ = db['nested']
            except KeyError:
                pass  # 预期的异常

            # 再次检查keys()，仍然不应该返回nested
            keys_final = db.keys()
            assert 'nested' not in keys_final

            db.close()
        finally:
            shutil.rmtree(tmpdir)

    def test_mixed_ttl_auto_nested_normal_keys(self):
        """
        测试混合场景：有TTL的嵌套键 + 无TTL的嵌套键 + 普通键
        """
        tmpdir = tempfile.mkdtemp()
        try:
            db = RawLevelDBDict('test', path=tmpdir, auto_nested=True, rebuild=True)

            # 混合场景
            db['nested_ttl'] = {'data': 'expires'}
            db['nested_permanent'] = {'data': 'stays'}
            db['normal_ttl'] = 'expires'
            db['normal_permanent'] = 'stays'

            db.set_ttl('nested_ttl', 1)
            db.set_ttl('normal_ttl', 1)

            # 立即检查：全部存在
            assert len(db) == 4

            # 等待过期
            time.sleep(1.1)

            # 检查剩余的键
            remaining_keys = db.keys()
            assert 'nested_permanent' in remaining_keys
            assert 'normal_permanent' in remaining_keys
            assert 'nested_ttl' not in remaining_keys
            assert 'normal_ttl' not in remaining_keys

            # 确保没有错误
            repr_str = repr(db)
            assert 'error' not in repr_str.lower()

            db.close()
        finally:
            shutil.rmtree(tmpdir)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
