"""
测试SimpleLRUCache基本功能
"""

import tempfile
import shutil
import time
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict
from flaxkv2.core.cached_leveldb_dict import CachedLevelDBDict


def test_cache_disabled():
    """测试RawLevelDBDict不带缓存功能"""
    print("\n=== 测试1: RawLevelDBDict不带缓存功能 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True)

        # RawLevelDBDict不再有缓存属性
        assert not hasattr(db, '_cache_enabled') or not db._cache_enabled
        assert not hasattr(db, '_cache') or db._cache is None
        print("✓ RawLevelDBDict无缓存功能")

        # 正常读写操作
        db['key'] = 'value'
        assert db['key'] == 'value'
        print("✓ 无缓存情况下读写正常")

        db.close()
        print("✓ 测试1通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_cache_enabled():
    """测试CachedLevelDBDict启用缓存"""
    print("\n=== 测试2: CachedLevelDBDict启用缓存 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = CachedLevelDBDict('test', path=tmpdir, rebuild=True, read_cache_size=100)

        assert db._cache_enabled
        assert db._cache is not None
        assert db._cache.maxsize == 100
        print("✓ 缓存已启用，大小=100")

        # 写入数据
        db['key1'] = 'value1'
        print("✓ 写入key1='value1'")

        # 第一次读取（从DB）
        v1 = db['key1']
        assert v1 == 'value1'
        assert len(db._cache) == 1
        print(f"✓ 第一次读取，缓存条目数={len(db._cache)}")

        # 第二次读取（从缓存）
        v2 = db['key1']
        assert v2 == 'value1'
        assert len(db._cache) == 1
        print(f"✓ 第二次读取（从缓存），缓存条目数={len(db._cache)}")

        db.close()
        print("✓ 测试2通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_cache_with_ttl():
    """测试CachedLevelDBDict缓存+TTL"""
    print("\n=== 测试3: CachedLevelDBDict缓存+TTL ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = CachedLevelDBDict('test', path=tmpdir, rebuild=True, read_cache_size=100)

        # 写入带TTL的数据
        db.set('key1', 'value1', ttl=2)
        print("✓ 写入key1='value1' (ttl=2秒)")

        # 立即读取（缓存）
        v1 = db['key1']
        assert v1 == 'value1'
        assert len(db._cache) == 1
        print(f"✓ 立即读取成功，缓存条目数={len(db._cache)}")

        # 从缓存读取
        v2 = db['key1']
        assert v2 == 'value1'
        print("✓ 从缓存读取成功")

        # 等待过期
        print("  等待3秒...")
        time.sleep(3)

        # 过期后读取（缓存会自动检查TTL）
        try:
            v3 = db['key1']
            print(f"✗ 错误：过期后仍能读取: {v3}")
            assert False
        except KeyError:
            print("✓ 过期后正确抛出KeyError")

        db.close()
        print("✓ 测试3通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_cache_update():
    """测试CachedLevelDBDict缓存更新"""
    print("\n=== 测试4: CachedLevelDBDict缓存更新 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = CachedLevelDBDict('test', path=tmpdir, rebuild=True, read_cache_size=100)

        # 写入并读取
        db['key1'] = 'value1'
        assert db['key1'] == 'value1'
        print("✓ 写入key1='value1'并缓存")

        # 更新值
        db['key1'] = 'value2'
        print("✓ 更新key1='value2'")

        # 读取（应该从缓存获取新值）
        v = db['key1']
        assert v == 'value2'
        print("✓ 读取到更新后的值'value2'（缓存已更新）")

        db.close()
        print("✓ 测试4通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_cache_delete():
    """测试CachedLevelDBDict缓存删除"""
    print("\n=== 测试5: CachedLevelDBDict缓存删除 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = CachedLevelDBDict('test', path=tmpdir, rebuild=True, read_cache_size=100)

        # 写入并读取
        db['key1'] = 'value1'
        assert db['key1'] == 'value1'
        assert len(db._cache) == 1
        print(f"✓ 写入并缓存key1，缓存条目数={len(db._cache)}")

        # 删除
        del db['key1']
        assert len(db._cache) == 0
        print(f"✓ 删除key1后，缓存条目数={len(db._cache)}")

        # 验证已删除
        try:
            _ = db['key1']
            assert False
        except KeyError:
            print("✓ key1已删除")

        db.close()
        print("✓ 测试5通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_cache_with_auto_nested():
    """测试CachedLevelDBDict缓存+auto_nested"""
    print("\n=== 测试6: CachedLevelDBDict缓存+auto_nested ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = CachedLevelDBDict('test', path=tmpdir, rebuild=True,
                               read_cache_size=100, auto_nested=True)

        # 写入嵌套字典
        db['user'] = {'name': 'Alice', 'age': 30}
        print("✓ 写入嵌套字典user")

        # 第一次读取（创建NestedDBDict并缓存）
        user1 = db['user']
        assert user1['name'] == 'Alice'
        assert len(db._cache) == 1
        print(f"✓ 第一次读取，缓存条目数={len(db._cache)}")

        # 第二次读取（从缓存）
        user2 = db['user']
        assert user1 is user2  # 应该是同一个对象
        print("✓ 第二次读取返回缓存的NestedDBDict实例（同一对象）")

        # 修改字段
        user1['age'] = 31
        assert db['user']['age'] == 31
        print("✓ 修改字段后，两个引用都能看到修改")

        db.close()
        print("✓ 测试6通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_cache_lru_eviction():
    """测试CachedLevelDBDict LRU淘汰"""
    print("\n=== 测试7: CachedLevelDBDict LRU淘汰 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        # 缓存大小=3
        db = CachedLevelDBDict('test', path=tmpdir, rebuild=True, read_cache_size=3)

        # 写入3个键（写入时会自动缓存）
        db['key1'] = 'value1'
        assert len(db._cache) == 1
        print(f"✓ 写入key1后，缓存={len(db._cache)}")

        db['key2'] = 'value2'
        assert len(db._cache) == 2
        print(f"✓ 写入key2后，缓存={len(db._cache)}")

        db['key3'] = 'value3'
        assert len(db._cache) == 3
        print(f"✓ 写入key3后，缓存={len(db._cache)} (已满)")

        # 写入key4（触发LRU淘汰，淘汰key1）
        db['key4'] = 'value4'
        assert len(db._cache) == 3
        print(f"✓ 写入key4后，缓存={len(db._cache)} (淘汰最旧的key1)")

        # 验证缓存统计
        stats = db._cache.stats()
        print(f"✓ 缓存统计: {stats}")
        assert stats['total_items'] == 3
        assert stats['usage_percent'] == 100.0

        db.close()
        print("✓ 测试7通过\n")
    finally:
        shutil.rmtree(tmpdir)


if __name__ == '__main__':
    print("=" * 60)
    print("SimpleLRUCache基本功能测试")
    print("=" * 60)

    test_cache_disabled()
    test_cache_enabled()
    test_cache_with_ttl()
    test_cache_update()
    test_cache_delete()
    test_cache_with_auto_nested()
    test_cache_lru_eviction()

    print("=" * 60)
    print("✅ 所有测试通过！")
    print("=" * 60)
