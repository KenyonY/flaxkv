"""FlaxKV打印和统计功能使用示例"""

from flaxkv2 import FlaxKV
import json

print("=" * 60)
print("FlaxKV 打印和统计功能演示")
print("=" * 60)
print()

# 示例1：直接打印数据库对象
print("【示例1】直接打印数据库对象")
print("-" * 60)
db = FlaxKV('demo_db', 'example_db', rebuild=True)

# 添加一些数据
db['user:1'] = {'name': 'Alice', 'age': 30}
db['user:2'] = {'name': 'Bob', 'age': 25}
db['config'] = {'debug': True, 'timeout': 30}

print("使用 print(db):")
print(db)
print()

print("使用 repr(db):")
print(repr(db))
print()

# 示例2：查看数据库统计信息
print("【示例2】查看数据库统计信息")
print("-" * 60)
stats = db.stat()

print("基本统计信息:")
print(f"  路径: {stats['path']}")
print(f"  后端: {stats['backend']}")
print(f"  键值对数量: {stats['count']}")
print(f"  近似大小: {stats.get('approximate_size', 'N/A')}")
print()

if 'leveldb_stats' in stats and stats['leveldb_stats'] != 'unavailable':
    print("LevelDB详细统计:")
    print(stats['leveldb_stats'])
print()

# 示例3：大数据库的打印效果
print("【示例3】大数据库的打印效果")
print("-" * 60)
db_large = FlaxKV('large_demo', 'example_db', rebuild=True)

# 添加100个键值对
for i in range(100):
    db_large[f'item_{i:03d}'] = {
        'id': i,
        'name': f'Item {i}',
        'value': i * 100
    }

print(f"包含{len(db_large)}个键值对的数据库:")
print(db_large)
print()

# 查看大数据库的统计
print("大数据库统计信息:")
stats_large = db_large.stat()
print(f"  键值对数量: {stats_large['count']}")
print(f"  近似大小: {stats_large.get('approximate_size', 'N/A')}")
print()

# 示例4：空数据库的打印
print("【示例4】空数据库的打印")
print("-" * 60)
db_empty = FlaxKV('empty_demo', 'example_db', rebuild=True)
print("空数据库:")
print(db_empty)
print()

# 示例5：关闭后的数据库打印
print("【示例5】关闭后的数据库打印")
print("-" * 60)
db_empty.close()
print("关闭后的数据库:")
print(db_empty)
print(repr(db_empty))
print()

# 清理
db.close()
db_large.close()

print("=" * 60)
print("演示完成！")
print("=" * 60)
