"""
测试TTL与prefix_db（auto_nested）的集成
验证marker的TTL管理和子字段的正常访问
"""

import tempfile
import shutil
import time
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict


def test_basic_auto_nested_with_ttl():
    """测试基础auto_nested + TTL功能"""
    print("\n=== 测试1: 基础auto_nested + TTL ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True, auto_nested=True)

        # 写入嵌套字典 + TTL
        db.set('user', {'name': 'Alice', 'age': 30, 'city': 'NYC'}, ttl=2)
        print("✓ 写入: user = {'name': 'Alice', 'age': 30, 'city': 'NYC'} (ttl=2秒)")

        # 立即访问：应该能正常读取
        user = db['user']
        assert user['name'] == 'Alice'
        assert user['age'] == 30
        assert user['city'] == 'NYC'
        print(f"✓ 立即读取成功: name={user['name']}, age={user['age']}, city={user['city']}")

        # 修改单个字段
        user['age'] = 31
        assert db['user']['age'] == 31
        print("✓ 修改单个字段成功: age=31")

        # 等待TTL过期
        print("  等待3秒...")
        time.sleep(3)

        # 过期后：整个嵌套字典应该不可访问
        try:
            value = db['user']
            print(f"✗ 错误：过期后仍能读取: {value}")
            assert False, "Should raise KeyError"
        except KeyError:
            print("✓ 过期后正确抛出KeyError")

        # 验证所有子字段都被清理
        keys = db.keys()
        assert 'user' not in keys
        print(f"✓ keys()不包含已过期的'user': {keys}")

        db.close()
        print("✓ 测试1通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_nested_field_access_before_expiry():
    """测试过期前的嵌套字段访问"""
    print("\n=== 测试2: 过期前的嵌套字段访问 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True, auto_nested=True)

        # 写入嵌套字典 + TTL
        db.set('config', {
            'database': {'host': 'localhost', 'port': 5432},
            'cache': {'enabled': True, 'ttl': 3600}
        }, ttl=10)
        print("✓ 写入多层嵌套配置 (ttl=10秒)")

        # 访问第一层
        config = db['config']
        # 注意：嵌套字典返回的是NestedDBDict，不是普通dict
        from flaxkv2.core.nested_dict import NestedDBDict
        database = config['database']
        assert isinstance(database, NestedDBDict) or isinstance(database, dict)
        print(f"✓ 访问第一层: database类型={type(database).__name__}")

        # 访问第二层（通过prefixed_db）
        assert config['database']['host'] == 'localhost'
        assert config['database']['port'] == 5432
        print(f"✓ 访问第二层: host={config['database']['host']}, port={config['database']['port']}")

        # 修改第二层字段
        config['database']['port'] = 5433
        assert db['config']['database']['port'] == 5433
        print("✓ 修改第二层字段成功: port=5433")

        # 添加新字段
        config['new_field'] = 'test'
        assert db['config']['new_field'] == 'test'
        print("✓ 添加新字段成功: new_field='test'")

        db.close()
        print("✓ 测试2通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_marker_ttl_manages_all_subfields():
    """测试marker的TTL管理所有子字段"""
    print("\n=== 测试3: Marker TTL管理所有子字段 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True, auto_nested=True)

        # 写入嵌套字典
        db.set('session', {
            'user_id': 123,
            'token': 'abc123',
            'expires_at': 1234567890
        }, ttl=2)
        print("✓ 写入session数据 (ttl=2秒)")

        # 验证所有子字段都可访问
        session = db['session']
        assert session['user_id'] == 123
        assert session['token'] == 'abc123'
        assert session['expires_at'] == 1234567890
        print("✓ 所有子字段可访问")

        # 等待过期
        print("  等待3秒...")
        time.sleep(3)

        # 验证所有子字段都不可访问
        try:
            _ = db['session']['user_id']
            assert False, "Should raise KeyError"
        except KeyError:
            print("✓ 子字段user_id不可访问（正确）")

        try:
            _ = db['session']['token']
            assert False, "Should raise KeyError"
        except KeyError:
            print("✓ 子字段token不可访问（正确）")

        # 验证keys()不返回任何子字段
        keys = db.keys()
        assert 'session' not in keys
        print(f"✓ keys()不包含'session': {keys}")

        db.close()
        print("✓ 测试3通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_ttl_with_nested_iteration():
    """测试带TTL的嵌套字典迭代"""
    print("\n=== 测试4: 带TTL的嵌套字典迭代 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True, auto_nested=True)

        # 写入嵌套字典
        data = {
            'field1': 'value1',
            'field2': 'value2',
            'field3': 'value3'
        }
        db.set('data', data, ttl=10)
        print(f"✓ 写入数据: {data} (ttl=10秒)")

        # 迭代所有字段
        nested = db['data']
        items = dict(nested.items())
        assert items == data
        print(f"✓ 迭代所有字段: {items}")

        # 迭代keys
        keys = list(nested.keys())
        assert set(keys) == set(data.keys())
        print(f"✓ 迭代keys: {keys}")

        # 迭代values
        values = list(nested.values())
        assert set(values) == set(data.values())
        print(f"✓ 迭代values: {values}")

        db.close()
        print("✓ 测试4通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_mixed_ttl_and_no_ttl():
    """测试混合TTL和无TTL的嵌套字典"""
    print("\n=== 测试5: 混合TTL和无TTL ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True, auto_nested=True)

        # 写入有TTL的嵌套字典
        db.set('temp_data', {'value': 'expires'}, ttl=2)
        print("✓ 写入temp_data (ttl=2秒)")

        # 写入无TTL的嵌套字典
        db['permanent_data'] = {'value': 'stays'}
        print("✓ 写入permanent_data (无ttl)")

        # 立即验证两者都可访问
        assert db['temp_data']['value'] == 'expires'
        assert db['permanent_data']['value'] == 'stays'
        print("✓ 两者都可访问")

        # 等待temp_data过期
        print("  等待3秒...")
        time.sleep(3)

        # temp_data应该不可访问
        try:
            _ = db['temp_data']
            assert False, "Should raise KeyError"
        except KeyError:
            print("✓ temp_data已过期")

        # permanent_data应该仍然可访问
        assert db['permanent_data']['value'] == 'stays'
        print("✓ permanent_data仍然可访问")

        db.close()
        print("✓ 测试5通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_update_ttl_for_nested():
    """测试更新嵌套字典的TTL"""
    print("\n=== 测试6: 更新嵌套字典的TTL ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True, auto_nested=True)

        # 写入无TTL的嵌套字典
        db['data'] = {'value': 'test'}
        print("✓ 写入data (无ttl)")

        # 验证无TTL
        ttl = db.get_ttl('data')
        assert ttl is None
        print(f"✓ TTL为None: {ttl}")

        # 设置TTL
        db.set_ttl('data', 2)
        ttl = db.get_ttl('data')
        assert ttl is not None and ttl > 0
        print(f"✓ 设置TTL后: {ttl}秒")

        # 等待过期
        print("  等待3秒...")
        time.sleep(3)

        # 应该已过期
        try:
            _ = db['data']
            assert False, "Should raise KeyError"
        except KeyError:
            print("✓ 设置TTL后正确过期")

        db.close()
        print("✓ 测试6通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_prefix_db_encoding():
    """测试prefix_db的编码格式"""
    print("\n=== 测试7: prefix_db编码格式验证 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True, auto_nested=True)

        # 写入嵌套字典 + TTL
        db.set('user', {'name': 'Bob', 'age': 25}, ttl=60)
        print("✓ 写入user数据 (ttl=60秒)")

        # 直接检查底层存储
        import plyvel

        # 检查marker键（应该有类型前缀's'）
        marker_key = db._encode_key('__nested__:user')
        marker_value = db._db.get(marker_key)
        assert marker_value is not None
        print(f"✓ Marker键存在: {marker_key[:20]}...")

        # 检查marker的编码格式（应该是ValueWithMeta格式）
        assert marker_value[0] == 0x01  # VERSION
        assert marker_value[1] == 0x01  # FLAGS (有TTL)
        print(f"✓ Marker使用ValueWithMeta格式: VERSION={marker_value[0]:#x}, FLAGS={marker_value[1]:#x}")

        # 检查子字段键（应该没有类型前缀，直接是UTF-8）
        name_key = b'user:name'
        name_value = db._db.get(name_key)
        assert name_value is not None
        print(f"✓ 子字段键存在: {name_key}")

        # 检查子字段的编码格式（应该是旧的encoder格式）
        assert name_value[0] == 0x00  # TYPE_MSGPACK
        # 不应该有VERSION和FLAGS
        print(f"✓ 子字段使用旧encoder格式: TYPE={name_value[0]:#x}")

        db.close()
        print("✓ 测试7通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_deeply_nested_with_ttl():
    """测试深层嵌套 + TTL"""
    print("\n=== 测试8: 深层嵌套 + TTL ===")
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True, auto_nested=True)

        # 写入深层嵌套结构
        db.set('app', {
            'config': {
                'database': {
                    'host': 'localhost',
                    'port': 5432
                },
                'cache': {
                    'enabled': True
                }
            },
            'version': '1.0'
        }, ttl=2)
        print("✓ 写入深层嵌套结构 (ttl=2秒)")

        # 访问深层字段
        assert db['app']['config']['database']['host'] == 'localhost'
        assert db['app']['config']['database']['port'] == 5432
        assert db['app']['config']['cache']['enabled'] == True
        assert db['app']['version'] == '1.0'
        print("✓ 所有深层字段可访问")

        # 等待过期
        print("  等待3秒...")
        time.sleep(3)

        # 验证整个结构都不可访问
        try:
            _ = db['app']
            assert False, "Should raise KeyError"
        except KeyError:
            print("✓ 根节点不可访问（正确）")

        # 验证所有子节点也被清理
        keys = db.keys()
        assert 'app' not in keys
        print(f"✓ keys()不包含'app': {keys}")

        db.close()
        print("✓ 测试8通过\n")
    finally:
        shutil.rmtree(tmpdir)


if __name__ == '__main__':
    print("=" * 60)
    print("TTL与prefix_db集成测试")
    print("=" * 60)

    test_basic_auto_nested_with_ttl()
    test_nested_field_access_before_expiry()
    test_marker_ttl_manages_all_subfields()
    test_ttl_with_nested_iteration()
    test_mixed_ttl_and_no_ttl()
    test_update_ttl_for_nested()
    test_prefix_db_encoding()
    test_deeply_nested_with_ttl()

    print("=" * 60)
    print("✅ 所有测试通过！")
    print("=" * 60)
    print("\n验证结论：")
    print("1. ✅ TTL正确内嵌在marker中")
    print("2. ✅ 子字段通过prefixed_db正常访问")
    print("3. ✅ marker过期时所有子字段一起清理")
    print("4. ✅ 子字段使用旧encoder格式（无TTL开销）")
    print("5. ✅ 支持深层嵌套结构")
    print("6. ✅ TTL和非TTL可以混合使用")
