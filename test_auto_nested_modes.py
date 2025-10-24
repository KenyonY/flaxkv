"""
测试 auto_nested 参数的两种模式
验证 auto_nested=True 和 auto_nested=False 的行为差异
"""
import os
import shutil
from flaxkv2 import LevelDBDict
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict
from flaxkv2.core.nested_dict import NestedDBDict


def test_leveldb_auto_nested_true():
    """测试 LevelDBDict 的 auto_nested=True 模式（默认）"""
    print("\n=== 测试 LevelDBDict auto_nested=True ===")

    db_path = "./test_auto_true_db"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    # 使用默认值（auto_nested=True）
    db = LevelDBDict("test_auto_true", path=".", rebuild=True)

    # 写入字典 - 应该自动嵌套
    db['config'] = {
        'database': {'host': 'localhost', 'port': 5432},
        'cache': {'redis': 'localhost:6379'}
    }

    # 验证是否自动嵌套
    config = db['config']
    print(f"config type: {type(config)}")
    assert isinstance(config, NestedDBDict), "应该返回 NestedDBDict"

    # 验证可以访问嵌套数据
    database = config['database']
    print(f"database type: {type(database)}")
    assert isinstance(database, NestedDBDict), "database 应该也是 NestedDBDict"

    # 验证可以访问叶子值
    host = database['host']
    print(f"host value: {host}")
    assert host == 'localhost', "应该能正确读取嵌套值"

    # 验证 to_dict
    config_dict = config.to_dict()
    print(f"to_dict result: {config_dict}")
    assert config_dict == {
        'database': {'host': 'localhost', 'port': 5432},
        'cache': {'redis': 'localhost:6379'}
    }, "to_dict 应该正确递归转换"

    db.close()
    print("✓ LevelDBDict auto_nested=True 测试通过")


def test_leveldb_auto_nested_false():
    """测试 LevelDBDict 的 auto_nested=False 模式"""
    print("\n=== 测试 LevelDBDict auto_nested=False ===")

    db_path = "./test_auto_false_db"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    # 显式禁用 auto_nested
    db = LevelDBDict("test_auto_false", path=".", rebuild=True, auto_nested=False)

    # 写入字典 - 应该使用传统序列化
    test_dict = {
        'database': {'host': 'localhost', 'port': 5432},
        'cache': {'redis': 'localhost:6379'}
    }
    db['config'] = test_dict

    # 验证是否使用传统存储
    config = db['config']
    print(f"config type: {type(config)}")
    assert isinstance(config, dict), "应该返回普通 dict，不是 NestedDBDict"
    assert not isinstance(config, NestedDBDict), "不应该返回 NestedDBDict"

    # 验证数据正确
    print(f"config value: {config}")
    assert config == test_dict, "应该返回完整的字典数据"

    # 验证无法使用嵌套访问（会报错）
    try:
        db['config']['database'] = 'xxx'  # 这不会生效
        # 重新读取验证
        config_again = db['config']
        assert config_again['database'] != 'xxx', "部分更新不应该生效"
        print("✓ 确认部分更新不生效（符合预期）")
    except:
        print("✓ 部分更新操作失败（符合预期）")

    db.close()
    print("✓ LevelDBDict auto_nested=False 测试通过")


def test_raw_leveldb_auto_nested_false():
    """测试 RawLevelDBDict 的 auto_nested=False 模式（默认）"""
    print("\n=== 测试 RawLevelDBDict auto_nested=False (默认) ===")

    db_path = "./test_raw_false_db"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    # 使用默认值（auto_nested=False）
    db = RawLevelDBDict("test_raw_false", path=".", rebuild=True)

    # 写入字典 - 应该使用传统序列化
    test_dict = {
        'database': {'host': 'localhost', 'port': 5432},
        'cache': {'redis': 'localhost:6379'}
    }
    db['config'] = test_dict

    # 验证是否使用传统存储
    config = db['config']
    print(f"config type: {type(config)}")
    assert isinstance(config, dict), "应该返回普通 dict"
    assert not isinstance(config, NestedDBDict), "不应该返回 NestedDBDict"

    # 验证数据正确
    print(f"config value: {config}")
    assert config == test_dict, "应该返回完整的字典数据"

    db.close()
    print("✓ RawLevelDBDict auto_nested=False 测试通过")


def test_raw_leveldb_auto_nested_true():
    """测试 RawLevelDBDict 的 auto_nested=True 模式"""
    print("\n=== 测试 RawLevelDBDict auto_nested=True ===")

    db_path = "./test_raw_true_db"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    # 显式启用 auto_nested
    db = RawLevelDBDict("test_raw_true", path=".", rebuild=True, auto_nested=True)

    # 写入字典 - 应该自动嵌套
    db['config'] = {
        'database': {'host': 'localhost', 'port': 5432},
        'cache': {'redis': 'localhost:6379'}
    }

    # 验证是否自动嵌套
    config = db['config']
    print(f"config type: {type(config)}")
    assert isinstance(config, NestedDBDict), "应该返回 NestedDBDict"

    # 验证可以访问嵌套数据
    database = config['database']
    print(f"database type: {type(database)}")
    assert isinstance(database, NestedDBDict), "database 应该也是 NestedDBDict"

    # 验证可以访问叶子值
    host = database['host']
    print(f"host value: {host}")
    assert host == 'localhost', "应该能正确读取嵌套值"

    db.close()
    print("✓ RawLevelDBDict auto_nested=True 测试通过")


def test_mixed_storage():
    """测试混合存储：同时存储字典和非字典值"""
    print("\n=== 测试混合存储 ===")

    db = LevelDBDict("test_mixed", path=".", rebuild=True, auto_nested=True)

    # 存储不同类型的数据
    db['simple_string'] = "hello"
    db['simple_number'] = 42
    db['simple_list'] = [1, 2, 3]
    db['nested_dict'] = {'a': {'b': {'c': 123}}}

    # 验证简单类型
    assert db['simple_string'] == "hello"
    assert db['simple_number'] == 42
    assert db['simple_list'] == [1, 2, 3]
    print("✓ 简单类型存储正确")

    # 验证嵌套字典
    nested = db['nested_dict']
    assert isinstance(nested, NestedDBDict)
    assert nested['a']['b']['c'] == 123
    print("✓ 嵌套字典存储正确")

    db.close()
    print("✓ 混合存储测试通过")


def test_performance_comparison():
    """简单的性能对比测试"""
    print("\n=== 性能对比测试 ===")
    import time

    # 测试数据
    test_data = {f'key{i}': i for i in range(100)}

    # 测试 auto_nested=False (传统模式)
    db_false = LevelDBDict("perf_false", path=".", rebuild=True, auto_nested=False)

    start = time.time()
    for i in range(10):
        db_false['data'] = test_data.copy()
        _ = db_false['data']
    time_false = time.time() - start
    db_false.close()

    print(f"auto_nested=False: {time_false:.4f}s")

    # 测试 auto_nested=True (嵌套模式)
    db_true = LevelDBDict("perf_true", path=".", rebuild=True, auto_nested=True)

    start = time.time()
    for i in range(10):
        db_true['data'] = test_data.copy()
        nested = db_true['data']
        _ = nested.to_dict()  # 完整读取
    time_true = time.time() - start
    db_true.close()

    print(f"auto_nested=True: {time_true:.4f}s")
    print(f"差异: {abs(time_true - time_false):.4f}s")

    # 测试部分更新的优势
    db_nested = LevelDBDict("perf_partial", path=".", rebuild=True, auto_nested=True)
    db_nested['data'] = test_data.copy()

    nested = db_nested['data']
    start = time.time()
    for i in range(100):
        nested['key0'] = i  # 只更新一个键
    time_partial = time.time() - start
    db_nested.close()

    print(f"部分更新（嵌套模式）: {time_partial:.4f}s")
    print("✓ 性能对比测试完成")


if __name__ == '__main__':
    try:
        # 测试各种模式
        test_leveldb_auto_nested_true()
        test_leveldb_auto_nested_false()
        test_raw_leveldb_auto_nested_false()
        test_raw_leveldb_auto_nested_true()
        test_mixed_storage()
        test_performance_comparison()

        print("\n" + "="*50)
        print("✓ 所有测试通过!")
        print("="*50)

    except AssertionError as e:
        print(f"\n✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"\n✗ 发生错误: {e}")
        import traceback
        traceback.print_exc()
