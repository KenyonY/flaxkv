"""
简单测试脚本验证TTL重构
"""

import time
import tempfile
import sys
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict


def test_raw_leveldb_set_ttl():
    """测试手动设置TTL"""
    print("测试1: 手动设置TTL...")
    with tempfile.TemporaryDirectory() as temp_dir:
        db = RawLevelDBDict("test_db", temp_dir, rebuild=True, raw=False)

        # 写入数据，没有默认TTL
        db["key1"] = "value1"
        assert db.get_ttl("key1") is None, "应该没有TTL"

        # 手动设置TTL（2秒）
        db.set_ttl("key1", 2)
        ttl = db.get_ttl("key1")
        assert ttl is not None, "应该有TTL"
        assert ttl <= 2, f"TTL应该<=2，实际为{ttl}"

        # 立即读取应该成功
        assert db["key1"] == "value1"

        # 等待3秒，应该过期
        print("  等待3秒...")
        time.sleep(3)

        try:
            _ = db["key1"]
            print("  ❌ 错误: 键应该过期但还能读取")
            return False
        except KeyError:
            print("  ✓ 键正确过期")

        db.close()

    print("✓ 测试1通过\n")
    return True


def test_raw_leveldb_cleanup_expired():
    """测试清理过期键"""
    print("测试2: 清理过期键...")
    with tempfile.TemporaryDirectory() as temp_dir:
        db = RawLevelDBDict("test_db", temp_dir, rebuild=True, raw=False)

        # 写入一些键，设置不同的TTL
        db["key1"] = "value1"
        db.set_ttl("key1", 1)  # 1秒后过期

        db["key2"] = "value2"
        db.set_ttl("key2", 10)  # 10秒后过期

        db["key3"] = "value3"  # 没有TTL

        # 等待2秒
        print("  等待2秒...")
        time.sleep(2)

        # 清理过期键
        cleaned = db.cleanup_expired()
        print(f"  清理了 {cleaned} 个过期键")

        # key1应该不存在（已过期）
        try:
            _ = db["key1"]
            print("  ❌ 错误: key1应该过期")
            return False
        except KeyError:
            print("  ✓ key1正确过期")

        # key2和key3应该还在
        assert db["key2"] == "value2", "key2应该存在"
        assert db["key3"] == "value3", "key3应该存在"
        print("  ✓ key2和key3仍然存在")

        db.close()

    print("✓ 测试2通过\n")
    return True


def test_raw_leveldb_delete_removes_ttl():
    """测试删除键时同时移除TTL"""
    print("测试3: 删除键时移除TTL...")
    with tempfile.TemporaryDirectory() as temp_dir:
        db = RawLevelDBDict("test_db", temp_dir, rebuild=True, raw=False, default_ttl=10)

        # 写入数据
        db["key1"] = "value1"
        assert db.get_ttl("key1") is not None, "应该有默认TTL"
        print("  ✓ key1有TTL")

        # 删除键
        del db["key1"]
        print("  ✓ 删除key1")

        # TTL也应该被移除
        ttl = db.get_ttl("key1")
        if ttl is not None:
            print(f"  ❌ 错误: TTL应该被移除，但还是{ttl}")
            return False

        print("  ✓ TTL也被移除")

        db.close()

    print("✓ 测试3通过\n")
    return True


def test_keys_filtering():
    """测试keys()方法过滤内部键"""
    print("测试4: keys()过滤TTL信息键...")
    with tempfile.TemporaryDirectory() as temp_dir:
        db = RawLevelDBDict("test_db", temp_dir, rebuild=True, raw=False)

        # 写入数据和TTL
        db["key1"] = "value1"
        db.set_ttl("key1", 10)

        db["key2"] = "value2"
        db.set_ttl("key2", 10)

        # keys()应该只返回用户键
        keys = db.keys()
        print(f"  所有键: {keys}")

        assert "key1" in keys, "应该包含key1"
        assert "key2" in keys, "应该包含key2"

        # 不应该包含TTL信息键
        for key in keys:
            if isinstance(key, str) and key.startswith('__ttl_info__:'):
                print(f"  ❌ 错误: keys()返回了内部键 {key}")
                return False

        print("  ✓ keys()正确过滤了内部键")

        db.close()

    print("✓ 测试4通过\n")
    return True


def main():
    print("=" * 60)
    print("TTL重构测试")
    print("=" * 60 + "\n")

    tests = [
        test_raw_leveldb_set_ttl,
        test_raw_leveldb_cleanup_expired,
        test_raw_leveldb_delete_removes_ttl,
        test_keys_filtering,
    ]

    failed = []

    for test_func in tests:
        try:
            if not test_func():
                failed.append(test_func.__name__)
        except Exception as e:
            print(f"❌ {test_func.__name__} 失败: {e}")
            import traceback
            traceback.print_exc()
            failed.append(test_func.__name__)

    print("=" * 60)
    if failed:
        print(f"❌ {len(failed)} 个测试失败:")
        for name in failed:
            print(f"  - {name}")
        return 1
    else:
        print("✅ 所有测试通过!")
        return 0


if __name__ == "__main__":
    sys.exit(main())
