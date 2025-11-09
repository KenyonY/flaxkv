"""
测试读缓存和写缓冲的一致性问题
"""
from flaxkv2 import CachedLevelDBDict
import time

print("=" * 70)
print("测试读缓存一致性Bug")
print("=" * 70)

# 创建数据库：同时启用读缓存和写缓冲
db = CachedLevelDBDict(
    "test_consistency_bug",
    rebuild=True,
    enable_ttl_cleanup=False,
    read_cache_size=100,         # 启用读缓存
    enable_write_buffer=True,    # 启用写缓冲
    write_buffer_size=10
)

print("\n步骤1：写入key1='old_value'，不使用写缓冲")
db._write_buffer_enabled = False  # 临时禁用写缓冲
db['key1'] = 'old_value'
db._write_buffer_enabled = True   # 重新启用

print("步骤2：读取key1，应该进入读缓存")
val = db['key1']
print(f"  读取结果: {val}")
assert val == 'old_value', "第一次读取失败"

# 检查读缓存
if db._cache_enabled:
    cached = db._cache.get('key1')
    print(f"  读缓存中的值: {cached}")

print("\n步骤3：写入key1='new_value'，使用写缓冲")
db['key1'] = 'new_value'

# 检查状态
print("  当前状态：")
if db._write_buffer_enabled:
    buffered = db._write_buffer.get('key1')
    if buffered:
        print(f"    写缓冲: key1='{buffered[0]}'")

if db._cache_enabled:
    cached = db._cache.get('key1')
    print(f"    读缓存: key1='{cached}' {'(过期数据!)' if cached == 'old_value' else ''}")

print("\n步骤4：读取key1（flush前）")
val = db['key1']
print(f"  读取结果: {val}")
assert val == 'new_value', "从写缓冲读取失败"

print("\n步骤5：flush写缓冲")
db.flush()
time.sleep(0.1)  # 等待flush完成

# 检查状态
print("  flush后状态：")
if db._write_buffer_enabled:
    buffered = db._write_buffer.get('key1')
    print(f"    写缓冲: {'空' if buffered is None else buffered}")

if db._cache_enabled:
    cached = db._cache.get('key1')
    print(f"    读缓存: key1='{cached}' {'(过期数据!)' if cached == 'old_value' else ''}")

print("\n步骤6：读取key1（flush后）- 关键测试！")
val = db['key1']
print(f"  读取结果: {val}")

if val == 'old_value':
    print("\n❌ Bug确认：读到了读缓存中的过期数据！")
    print("   写缓冲flush后，读缓存中仍是旧值")
elif val == 'new_value':
    print("\n✅ 没有Bug：正确读取了新值")
    print("   可能的原因：")
    print("   1. 写入时更新了读缓存")
    print("   2. flush时更新了读缓存")
    print("   3. 读取时从数据库重新加载")
else:
    print(f"\n⚠️ 未预期的值: {val}")

db.close()

print("\n" + "=" * 70)
print("测试完成")
print("=" * 70)
