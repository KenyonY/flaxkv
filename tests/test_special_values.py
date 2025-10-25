"""
测试特殊值和特殊键的处理

验证：
1. None 值可以正常存储和读取
2. 以 __nested__: 开头的键可以正常使用
3. 其他边界情况
"""

from flaxkv2.core.leveldb_dict import LevelDBDict
import tempfile
import shutil

def test_none_value():
    """测试 None 值的存储和读取"""
    print("=" * 60)
    print("测试 1: None 值")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db = LevelDBDict("test_none", path=tmpdir, rebuild=True)
        
        # 存储 None 值
        db["key1"] = None
        db["key2"] = "not none"
        db["key3"] = None
        
        # 强制刷新缓冲区
        db.close(write=True, wait=True)
        
        # 重新打开数据库
        db = LevelDBDict("test_none", path=tmpdir)
        
        # 读取 None 值
        assert db["key1"] is None, f"Expected None, got {db['key1']}"
        assert db["key2"] == "not none"
        assert db["key3"] is None, f"Expected None, got {db['key3']}"
        
        # 检查 keys
        assert "key1" in db
        assert "key2" in db
        assert "key3" in db
        
        print("✓ None 值可以正常存储和读取")
        
        db.close()

def test_special_key_prefix():
    """测试以 __nested__: 开头的键"""
    print("\n" + "=" * 60)
    print("测试 2: 特殊键前缀")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db = LevelDBDict("test_special", path=tmpdir, rebuild=True)
        
        # 尝试使用特殊前缀的键
        try:
            db["__nested__:mykey"] = "my value"
            value = db["__nested__:mykey"]
            print(f"✓ 可以使用 __nested__: 前缀的键，值: {value}")
        except Exception as e:
            print(f"✗ 使用 __nested__: 前缀的键失败: {e}")
        
        # 尝试删除
        try:
            del db["__nested__:mykey"]
            print("✓ 可以删除 __nested__: 前缀的键")
        except Exception as e:
            print(f"✗ 删除 __nested__: 前缀的键失败: {e}")
        
        db.close()

def test_delete_then_set_none():
    """测试删除后设置 None"""
    print("\n" + "=" * 60)
    print("测试 3: 删除后设置 None")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db = LevelDBDict("test_delete", path=tmpdir, rebuild=True)
        
        # 设置一个值
        db["key"] = "value"
        assert db["key"] == "value"
        
        # 删除
        del db["key"]
        assert "key" not in db
        
        # 设置为 None
        db["key"] = None
        assert "key" in db
        assert db["key"] is None, f"Expected None, got {db['key']}"
        
        print("✓ 删除后可以设置 None 值")
        
        db.close()

def test_mixed_operations():
    """测试混合操作"""
    print("\n" + "=" * 60)
    print("测试 4: 混合操作")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db = LevelDBDict("test_mixed", path=tmpdir, rebuild=True)
        
        # 批量操作包含 None
        db.update({
            "a": 1,
            "b": None,
            "c": "string",
            "d": None,
            "e": [1, 2, 3]
        })
        
        # 验证
        assert db["a"] == 1
        assert db["b"] is None
        assert db["c"] == "string"
        assert db["d"] is None
        assert db["e"] == [1, 2, 3]
        
        # 删除一个
        del db["c"]
        assert "c" not in db
        
        # 再次设置为 None
        db["c"] = None
        assert db["c"] is None
        
        print("✓ 混合操作正常")
        
        db.close()

if __name__ == "__main__":
    try:
        test_none_value()
        test_special_key_prefix()
        test_delete_then_set_none()
        test_mixed_operations()
        
        print("\n" + "=" * 60)
        print("✅ 所有测试通过！")
        print("=" * 60)
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

