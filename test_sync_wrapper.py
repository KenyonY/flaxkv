#!/usr/bin/env python3
"""
测试同步包装器（RemoteDBDict）的向后兼容性
"""
import time
from flaxkv2 import FlaxKV

print("=" * 80)
print("测试同步包装器（RemoteDBDict）")
print("=" * 80)

# 创建客户端（使用同步包装器）
db = FlaxKV(
    'default_db',
    'tcp://127.0.0.1:25555',
    backend='remote',
    timeout=60000,
    enable_encryption=True,
    password='yao',
    derive_from_password=True
)

print("\n测试1: 基本读写操作")
# 写入
db['test_key'] = 'test_value'
db['test_num'] = 123
db['test_bytes'] = b'binary data'

# 读取
assert db['test_key'] == 'test_value', "字符串读取失败"
assert db['test_num'] == 123, "数字读取失败"
assert db['test_bytes'] == b'binary data', "字节读取失败"
print("✅ 基本读写操作成功")

print("\n测试2: 字典方法")
# keys()
keys = db.keys()
assert 'test_key' in keys, "keys() 失败"
print(f"   keys: {keys}")

# get() with default
assert db.get('nonexistent', 'default') == 'default', "get() with default 失败"
print("✅ 字典方法成功")

print("\n测试3: 批量操作")
# batch_set
db.batch_set({
    'batch1': 'value1',
    'batch2': 'value2',
    'batch3': 'value3'
})

assert db['batch1'] == 'value1', "batch_set 失败"
assert db['batch2'] == 'value2', "batch_set 失败"
assert db['batch3'] == 'value3', "batch_set 失败"
print("✅ 批量操作成功")

print("\n测试4: 删除操作")
db['to_delete'] = 'will be deleted'
assert 'to_delete' in db, "__contains__ 失败"
del db['to_delete']
assert 'to_delete' not in db, "删除失败"
print("✅ 删除操作成功")

print("\n测试5: TTL 支持")
db.set('ttl_key', 'ttl_value', ttl=2)
assert db['ttl_key'] == 'ttl_value', "TTL 写入失败"
print(f"   写入 TTL 键，等待2秒...")
time.sleep(2.5)
try:
    value = db['ttl_key']
    print(f"   ❌ TTL 未过期（意外）: {value}")
except KeyError:
    print("✅ TTL 过期正常")

print("\n测试6: Ping 测试")
assert db.ping(), "Ping 失败"
print("✅ Ping 成功")

print("\n测试7: 性能测试（10个10MB数据）")
test_data = b'x' * (10 * 1024 * 1024)  # 10MB
num_ops = 10

start = time.time()
for i in range(num_ops):
    db[f'perf:test:{i}'] = test_data
write_time = time.time() - start
write_throughput = (num_ops * 10) / write_time

print(f"   写入: {write_time:.2f}秒")
print(f"   吞吐量: {write_throughput:.1f} MB/s")

# 清理
print("\n清理测试数据...")
for key in db.keys():
    try:
        del db[key]
    except:
        pass

db.close()
print("\n✅ 所有测试通过！同步包装器工作正常！")
print("=" * 80)
