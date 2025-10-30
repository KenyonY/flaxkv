"""
测试TTL重构的基本功能
"""

import tempfile
import shutil
import time
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict


def test_basic_ttl():
    """测试基本TTL功能"""
    print("\n=== 测试基本TTL功能 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True)

        # 写入带TTL的值
        db.set('key1', 'value1', ttl=2)
        print(f"✓ 写入 key1='value1' (ttl=2秒)")

        # 立即读取
        assert db['key1'] == 'value1'
        print(f"✓ 立即读取: {db['key1']}")

        # 检查TTL
        ttl = db.get_ttl('key1')
        print(f"✓ 剩余TTL: {ttl}秒")
        assert ttl is not None and ttl > 0

        # 等待过期
        print("  等待3秒...")
        time.sleep(3)

        # 过期后读取应该抛出KeyError
        try:
            value = db['key1']
            print(f"✗ 过期后仍能读取: {value}")
            assert False, "Should raise KeyError"
        except KeyError:
            print("✓ 过期后正确抛出KeyError")

        db.close()
        print("✓ 基本TTL功能测试通过")
    finally:
        shutil.rmtree(tmpdir)


def test_no_ttl():
    """测试无TTL的普通写入"""
    print("\n=== 测试无TTL写入 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True)

        # 写入无TTL的值
        db['key1'] = 'value1'
        print(f"✓ 写入 key1='value1' (无TTL)")

        # 读取
        assert db['key1'] == 'value1'
        print(f"✓ 读取: {db['key1']}")

        # 检查TTL应该为None
        ttl = db.get_ttl('key1')
        assert ttl is None
        print(f"✓ TTL为None")

        db.close()
        print("✓ 无TTL写入测试通过")
    finally:
        shutil.rmtree(tmpdir)


def test_auto_nested_with_ttl():
    """测试auto_nested + TTL"""
    print("\n=== 测试auto_nested + TTL ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True, auto_nested=True)

        # 写入嵌套字典 + TTL
        db.set('user', {'name': 'Alice', 'age': 30}, ttl=2)
        print(f"✓ 写入嵌套字典 user={{'name': 'Alice', 'age': 30}} (ttl=2秒)")

        # 立即读取
        user = db['user']
        print(f"✓ 读取类型: {type(user).__name__}")
        assert user['name'] == 'Alice'
        assert user['age'] == 30
        print(f"✓ 立即读取: name={user['name']}, age={user['age']}")

        # 等待过期
        print("  等待3秒...")
        time.sleep(3)

        # 过期后读取应该抛出KeyError
        try:
            value = db['user']
            print(f"✗ 过期后仍能读取: {value}")
            assert False, "Should raise KeyError"
        except KeyError:
            print("✓ 过期后正确抛出KeyError")

        db.close()
        print("✓ auto_nested + TTL测试通过")
    finally:
        shutil.rmtree(tmpdir)


def test_keys_filter_expired():
    """测试keys()自动过滤过期键"""
    print("\n=== 测试keys()过滤过期键 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True)

        # 写入多个键
        db.set('key1', 'value1', ttl=1)
        db['key2'] = 'value2'  # 无TTL
        db.set('key3', 'value3', ttl=10)
        print("✓ 写入3个键: key1(ttl=1), key2(无ttl), key3(ttl=10)")

        # 立即检查keys
        keys = db.keys()
        assert set(keys) == {'key1', 'key2', 'key3'}
        print(f"✓ 立即keys(): {sorted(keys)}")

        # 等待key1过期
        print("  等待2秒...")
        time.sleep(2)

        # keys()应该只返回key2和key3
        keys = db.keys()
        assert set(keys) == {'key2', 'key3'}
        print(f"✓ 过期后keys(): {sorted(keys)}")

        db.close()
        print("✓ keys()过滤过期键测试通过")
    finally:
        shutil.rmtree(tmpdir)


def test_set_ttl_method():
    """测试set_ttl()方法"""
    print("\n=== 测试set_ttl()方法 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True)

        # 先写入无TTL的值
        db['key1'] = 'value1'
        assert db.get_ttl('key1') is None
        print("✓ 写入无TTL的key1")

        # 使用set_ttl()添加TTL
        db.set_ttl('key1', 2)
        ttl = db.get_ttl('key1')
        assert ttl is not None and ttl > 0
        print(f"✓ 设置TTL=2秒，剩余TTL={ttl}秒")

        # 等待过期
        print("  等待3秒...")
        time.sleep(3)

        # 应该已过期
        try:
            value = db['key1']
            print(f"✗ 过期后仍能读取: {value}")
            assert False
        except KeyError:
            print("✓ 过期后正确抛出KeyError")

        db.close()
        print("✓ set_ttl()方法测试通过")
    finally:
        shutil.rmtree(tmpdir)


def test_remove_ttl():
    """测试remove_ttl()方法"""
    print("\n=== 测试remove_ttl()方法 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True)

        # 写入带TTL的值
        db.set('key1', 'value1', ttl=2)
        assert db.get_ttl('key1') is not None
        print("✓ 写入带TTL的key1")

        # 移除TTL
        db.remove_ttl('key1')
        assert db.get_ttl('key1') is None
        print("✓ 移除TTL")

        # 等待原本应该过期的时间
        print("  等待3秒...")
        time.sleep(3)

        # 应该仍然能读取（因为TTL已移除）
        assert db['key1'] == 'value1'
        print("✓ 移除TTL后不会过期")

        db.close()
        print("✓ remove_ttl()方法测试通过")
    finally:
        shutil.rmtree(tmpdir)


if __name__ == '__main__':
    print("=" * 60)
    print("TTL重构基本功能测试")
    print("=" * 60)

    test_no_ttl()
    test_basic_ttl()
    test_auto_nested_with_ttl()
    test_keys_filter_expired()
    test_set_ttl_method()
    test_remove_ttl()

    print("\n" + "=" * 60)
    print("✓ 所有测试通过！")
    print("=" * 60)
