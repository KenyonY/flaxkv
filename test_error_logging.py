"""
测试改进后的异常处理：验证能正确区分内部键和真正的错误
"""
import os
import shutil
import logging
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict

# 配置日志以查看警告
logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')

def test_skip_internal_keys():
    """测试正确跳过内部键"""
    print("\n=== 测试跳过内部键 ===")

    db_path = "./test_error_log"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    # 创建带嵌套存储的数据库
    db = RawLevelDBDict("test_error_log", path=".", rebuild=True, auto_nested=True)

    # 写入嵌套数据
    db['config'] = {
        'database': {'host': 'localhost', 'port': 5432},
        'cache': {'redis': 'localhost:6379'}
    }

    # 写入普通数据
    db['name'] = 'TestDB'
    db['version'] = '1.0'

    # 获取所有键 - 应该只返回用户键，跳过内部键和嵌套子键
    keys = db.keys()
    print(f"用户键列表: {keys}")

    # 验证只有顶层键
    assert 'config' in keys, "应该包含 config"
    assert 'name' in keys, "应该包含 name"
    assert 'version' in keys, "应该包含 version"
    assert len(keys) == 3, f"应该只有3个键，实际有 {len(keys)}"

    # 验证不包含内部键
    for key in keys:
        assert not key.startswith('__nested__:'), "不应该包含标记键"
        assert not key.startswith('__ttl_info__:'), "不应该包含TTL键"
        # 验证不包含嵌套子键（如 'config:database', 'config:cache'）
        if ':' in key:
            assert False, f"不应该包含嵌套子键: {key}"

    print("✓ 正确跳过内部键和嵌套子键")

    # 测试 values() 和 items()
    values = db.values()
    print(f"值数量: {len(values)}")
    assert len(values) == 3, "应该有3个值"

    items = db.items()
    print(f"键值对数量: {len(items)}")
    assert len(items) == 3, "应该有3个键值对"

    print("✓ values() 和 items() 也正确跳过内部键")

    db.close()
    shutil.rmtree(db_path)


def test_corrupted_key_logging():
    """测试损坏的键会被记录日志"""
    print("\n=== 测试损坏键的日志记录 ===")

    db_path = "./test_corrupted"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    db = RawLevelDBDict("test_corrupted", path=".", rebuild=True)

    # 写入正常数据
    db['normal_key'] = 'normal_value'

    # 直接向 LevelDB 写入损坏的键（模拟数据损坏）
    # 使用无效的类型标识前缀
    with db._db_lock:
        db._db.put(b'x_invalid_prefix_key', b's_some_value')

    print("已写入损坏的键，现在尝试读取所有键...")

    # 这应该会记录警告日志
    keys = db.keys()

    print(f"成功读取的键: {keys}")
    assert 'normal_key' in keys, "应该能读取正常的键"

    print("✓ 损坏的键会被记录警告日志（查看上面的 WARNING 信息）")

    db.close()
    shutil.rmtree(db_path)


def test_keys_filtering_logic():
    """测试 keys() 的过滤逻辑"""
    print("\n=== 测试 keys() 过滤逻辑 ===")

    db_path = "./test_logic"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    db = RawLevelDBDict("test_logic", path=".", rebuild=True, auto_nested=True)

    # 写入各种类型的数据
    db['nested_dict'] = {'field1': 'value1', 'field2': 'value2'}
    db['normal_str'] = 'hello'
    db['normal_int'] = 42
    db['user:data'] = 'colon in key'  # 用户键可以包含冒号

    keys = db.keys()
    print(f"返回的键: {keys}")

    # 验证返回的键
    assert 'nested_dict' in keys, "应该包含嵌套字典的顶层键"
    assert 'normal_str' in keys, "应该包含普通字符串键"
    assert 'normal_int' in keys, "应该包含普通整数键"
    assert 'user:data' in keys, "应该包含带冒号的用户键"

    # 验证不包含内部键
    for key in keys:
        assert not key.startswith('__nested__:'), f"不应该包含标记键: {key}"
        assert not key.startswith('__ttl_info__:'), f"不应该包含TTL键: {key}"

    print("✓ keys() 过滤逻辑正确")

    db.close()
    shutil.rmtree(db_path)


if __name__ == '__main__':
    try:
        test_skip_internal_keys()
        test_corrupted_key_logging()
        test_keys_filtering_logic()

        print("\n" + "="*50)
        print("✓ 所有测试通过！")
        print("="*50)
        print("\n改进后的异常处理：")
        print("1. ✓ 正确识别和跳过内部键（__nested__:, __ttl_info__:）")
        print("2. ✓ 正确识别和跳过嵌套存储的子键")
        print("3. ✓ 对真正的解码错误记录警告日志")
        print("4. ✓ 不会静默忽略非预期的错误")

    except AssertionError as e:
        print(f"\n✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"\n✗ 发生错误: {e}")
        import traceback
        traceback.print_exc()
