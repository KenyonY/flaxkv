"""
自动嵌套存储 - 演示脚本

展示 Simple and Stupid 的自动嵌套功能
"""
from flaxkv2 import LevelDBDict

print("=" * 70)
print(" FlaxKV2 自动嵌套存储演示 - Simple and Stupid")
print("=" * 70)

db = LevelDBDict('demo_db', rebuild=True)

print("\n【演示 1】 自动检测字典类型")
print("-" * 70)

# 像普通字典一样使用！
db['user:1'] = {
    'name': 'Alice',
    'age': 30,
    'email': 'alice@example.com'
}
print("✓ 写入: db['user:1'] = {'name': 'Alice', 'age': 30, ...}")

# 自动返回 NestedDBDict
user = db['user:1']
print(f"✓ 读取: type = {type(user).__name__}")
print(f"  user['name'] = {user['name']}")
print(f"  user['age'] = {user['age']}")

# 修改自动同步
user['age'] = 31
print(f"✓ 修改: user['age'] = 31")
print(f"  验证: db['user:1']['age'] = {db['user:1']['age']}")

print("\n【演示 2】 递归嵌套（任意层级）")
print("-" * 70)

db['config'] = {
    'database': {
        'mysql': {
            'host': 'localhost',
            'port': 3306,
            'credentials': {
                'user': 'root',
                'password': 'secret'
            }
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
mysql = config['database']['mysql']
credentials = mysql['credentials']
user = credentials['user']
print(f"✓ 深层访问: config -> database -> mysql -> credentials -> user")
print(f"  user = '{user}'")

# 修改深层值
credentials['password'] = 'new_password'
print(f"✓ 修改深层值: credentials['password'] = 'new_password'")
print(f"  验证: {db['config']['database']['mysql']['credentials']['password']}")

print("\n【演示 3】 混合类型")
print("-" * 70)

db['mixed'] = {
    'string': 'hello',
    'number': 123,
    'list': [1, 2, 3],
    'dict': {
        'key': 'value'
    }
}

mixed = db['mixed']
print(f"✓ string: {mixed['string']} (type: {type(mixed['string']).__name__})")
print(f"✓ number: {mixed['number']} (type: {type(mixed['number']).__name__})")
print(f"✓ list: {mixed['list']} (type: {type(mixed['list']).__name__})")
print(f"✓ dict: {type(mixed['dict']).__name__}")

print("\n【演示 4】 转换为普通字典")
print("-" * 70)

config_dict = config.to_dict()
print(f"✓ config.to_dict() -> {type(config_dict)}")
print(f"  database.mysql.host = {config_dict['database']['mysql']['host']}")
print(f"  cache.redis.port = {config_dict['cache']['redis']['port']}")

print("\n【演示 5】 实际存储结构")
print("-" * 70)

print("\nLevelDB 中实际存储的键（部分）:")
count = 0
for key, _ in db._db:
    key_str = key.decode('utf-8', errors='ignore')
    if count < 10:
        print(f"  {key_str}")
        count += 1
if count >= 10:
    print("  ...")

print("\n【演示 6】 性能对比")
print("-" * 70)

import time

# 传统方式模拟
print("\n传统方式（整个字典序列化）:")
data_dict = {'field_' + str(i): i for i in range(1000)}
db['traditional'] = data_dict

start = time.time()
for i in range(100):
    data = db['traditional']
    data['field_0'] = i
    db['traditional'] = data
elapsed_trad = time.time() - start
print(f"  修改 100 次: {elapsed_trad:.4f}秒")

# 自动嵌套方式
print("\n自动嵌套方式（单字段序列化）:")
db['nested_data'] = {'field_' + str(i): i for i in range(1000)}
nested_data = db['nested_data']

start = time.time()
for i in range(100):
    nested_data['field_0'] = i
elapsed_nested = time.time() - start
print(f"  修改 100 次: {elapsed_nested:.4f}秒")

if elapsed_trad > 0:
    print(f"\n性能提升: {elapsed_trad / elapsed_nested:.1f}x 更快")

print("\n" + "=" * 70)
print(" 总结")
print("=" * 70)
print("\n✅ Simple: 只有一个规则 - 遇到 dict 就递归展开")
print("✅ Stupid: 不做复杂判断，统一处理")
print("✅ 高效: 部分更新只序列化修改的字段")
print("✅ 易用: 像普通字典一样使用")
print("\n🚀 开始使用 FlaxKV2 自动嵌套存储！")

db.close()
