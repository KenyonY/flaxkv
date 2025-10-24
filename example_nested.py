"""
FlaxKV2 NestedDBDict 使用示例

解决嵌套数据频繁序列化的性能问题
"""
from flaxkv2 import LevelDBDict

# 创建数据库
db = LevelDBDict('example_db', rebuild=True)

print("=" * 60)
print(" 示例 1: 基础用法")
print("=" * 60)

# 创建嵌套字典（使用前缀 'user:1'）
user = db.nested('user:1')

# 像普通字典一样使用
user['name'] = 'Alice'
user['age'] = 30
user['email'] = 'alice@example.com'
user['preferences'] = {'theme': 'dark', 'language': 'en'}

print(f"\n用户姓名: {user['name']}")
print(f"用户年龄: {user['age']}")

# 修改单个字段（高效！）
user['age'] = 31
print(f"更新后年龄: {user['age']}")

print("\n" + "=" * 60)
print(" 示例 2: 迭代所有字段")
print("=" * 60)

print("\n所有字段:")
for key, value in user.items():
    print(f"  {key}: {value}")

print("\n" + "=" * 60)
print(" 示例 3: 多个嵌套字典")
print("=" * 60)

# 为不同用户创建独立的嵌套字典
user1 = db.nested('user:1')
user2 = db.nested('user:2')

user1['name'] = 'Alice'
user2['name'] = 'Bob'

print(f"\nUser 1: {user1['name']}")
print(f"User 2: {user2['name']}")

print("\n" + "=" * 60)
print(" 示例 4: 转换为普通字典")
print("=" * 60)

# 需要一次性获取所有数据时，转换为普通字典
user_dict = user.to_dict()
print(f"\n转换为字典: {user_dict}")

print("\n" + "=" * 60)
print(" 示例 5: 对比传统方式")
print("=" * 60)

print("\n❌ 传统方式（低效）:")
print("  db['user:1'] = {'name': 'Alice', 'age': 30, ...}")
print("  data = db['user:1']  # 反序列化整个字典")
print("  data['age'] = 31")
print("  db['user:1'] = data  # 重新序列化整个字典")

print("\n✅ NestedDBDict 方式（高效）:")
print("  user = db.nested('user:1')")
print("  user['age'] = 31  # 只序列化 age 的值！")

print("\n" + "=" * 60)
print(" 示例 6: 实际存储结构")
print("=" * 60)

print("\n实际存储在 LevelDB 中的键:")
print("  user:1:name -> 'Alice'")
print("  user:1:age -> 31")
print("  user:1:email -> 'alice@example.com'")
print("  user:1:preferences -> {'theme': 'dark', 'language': 'en'}")

print("\n优势:")
print("  ✓ 每个字段独立存储")
print("  ✓ 修改单个字段不影响其他字段")
print("  ✓ 利用 LevelDB 的前缀查询高效迭代")

# 清理
db.close()
print("\n✓ 数据库已关闭")
