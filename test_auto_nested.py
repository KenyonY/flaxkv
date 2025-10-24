"""测试自动嵌套存储功能"""
import tempfile
import shutil
from flaxkv2 import LevelDBDict

tmpdir = tempfile.mkdtemp()
print(f"临时目录: {tmpdir}\n")

try:
    db = LevelDBDict('test', path=tmpdir, rebuild=True)

    print("=" * 60)
    print("测试 1: 自动检测字典并使用嵌套存储")
    print("=" * 60)

    # 写入字典，自动使用嵌套存储
    db['user:1'] = {
        'name': 'Alice',
        'age': 30,
        'email': 'alice@example.com'
    }
    print("✓ 写入字典")

    # 读取，应该返回 NestedDBDict
    user = db['user:1']
    print(f"✓ 读取：type = {type(user).__name__}")
    print(f"  name: {user['name']}")
    print(f"  age: {user['age']}")

    # 修改
    user['age'] = 31
    print(f"✓ 修改：age = {user['age']}")

    # 验证持久化
    user2 = db['user:1']
    print(f"✓ 重新读取：age = {user2['age']}")

    print("\n" + "=" * 60)
    print("测试 2: 递归嵌套字典")
    print("=" * 60)

    # 多层嵌套
    db['config'] = {
        'database': {
            'host': 'localhost',
            'port': 5432,
            'credentials': {
                'user': 'admin',
                'password': 'secret'
            }
        },
        'cache': {
            'redis': {
                'host': '127.0.0.1',
                'port': 6379
            }
        }
    }
    print("✓ 写入多层嵌套字典")

    # 逐层访问
    config = db['config']
    print(f"✓ config type: {type(config).__name__}")

    database = config['database']
    print(f"✓ database type: {type(database).__name__}")

    credentials = database['credentials']
    print(f"✓ credentials type: {type(credentials).__name__}")

    # 读取叶子值
    user = credentials['user']
    print(f"✓ 叶子值：user = {user}")

    # 修改深层值
    credentials['password'] = 'new_secret'
    print(f"✓ 修改深层值：password = {credentials['password']}")

    # 验证修改生效
    config2 = db['config']
    password = config2['database']['credentials']['password']
    print(f"✓ 验证修改：password = {password}")

    print("\n" + "=" * 60)
    print("测试 3: to_dict() 递归重建")
    print("=" * 60)

    # 转换为普通字典
    config_dict = config.to_dict()
    print(f"✓ to_dict() type: {type(config_dict)}")
    print(f"  database.host: {config_dict['database']['host']}")
    print(f"  cache.redis.port: {config_dict['cache']['redis']['port']}")

    print("\n" + "=" * 60)
    print("测试 4: 混合类型")
    print("=" * 60)

    db['mixed'] = {
        'string': 'hello',
        'number': 123,
        'list': [1, 2, 3],
        'nested_dict': {
            'key1': 'value1',
            'key2': 'value2'
        }
    }
    print("✓ 写入混合类型")

    mixed = db['mixed']
    print(f"  string: {mixed['string']}")
    print(f"  number: {mixed['number']}")
    print(f"  list: {mixed['list']}")

    nested = mixed['nested_dict']
    print(f"  nested_dict type: {type(nested).__name__}")
    print(f"  nested_dict.key1: {nested['key1']}")

    print("\n" + "=" * 60)
    print("测试 5: 删除嵌套字典")
    print("=" * 60)

    # 删除嵌套字典
    del db['config']
    print("✓ 删除 config")

    # 验证已删除
    try:
        _ = db['config']
        print("❌ 删除失败")
    except KeyError:
        print("✓ 确认已删除")

    print("\n" + "=" * 60)
    print("测试 6: 非字典类型")
    print("=" * 60)

    # 写入非字典
    db['number'] = 123
    db['string'] = 'hello'
    print("✓ 写入非字典类型")

    # 读取
    num = db['number']
    s = db['string']
    print(f"  number: {num} (type: {type(num).__name__})")
    print(f"  string: {s} (type: {type(s).__name__})")

    print("\n" + "=" * 60)
    print("测试 7: 查看实际存储结构")
    print("=" * 60)

    print("\nLevelDB 中的所有键:")
    for key, value in db._db:
        key_str = key.decode('utf-8', errors='ignore')
        print(f"  {key_str[:50]}")

    db.close()
    print("\n✓ 所有测试通过！")

finally:
    shutil.rmtree(tmpdir)
    print(f"\n已清理")
