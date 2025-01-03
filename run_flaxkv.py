from flaxkv import FlaxKV
import time

# 创建数据库实例
db = FlaxKV('test_db')

# print(db.get('key1', "null"))

# print("key1的初始TTL值:", db.ttl('key1'))

# # 设置带过期时间的键值对
# print("\n设置key1的过期时间为6秒")
# db.set('key1', 'value1', ex=6)  # 60秒后过期
# print("key1的值:", db.get('key1'))
# time.sleep(2)
# # 检查剩余时间
# ttl = db.ttl('key1')
# print("\n2秒后，key1的TTL值:", ttl)


# # 设置已存在键的过期时间
# print("\n将过期时间改为30秒")
# db.expire('key1', 30)  # 修改为30秒后过期
# time.sleep(1)
# print("1秒后，key1的TTL值:", db.ttl('key1'))

# # 移除过期时间
# print("\n移除过期时间")
# db.persist('key1')
# time.sleep(0.1) # 等待一下确保写入完成
# print("移除过期时间后，key1的TTL值:", db.ttl('key1'))
# print("key1的值:", db.get('key1'))

# # 普通操作仍然可用
# print("\n设置不带过期时间的key2")
# db['key2'] = 'value2'  # 无过期时间
# print("key2的TTL值:", db.ttl('key2'))
# print("key2的值:", db.get('key2'))

# print(f'{db.pop("key1")=}')

# 设置3秒过期时间，并在3秒后检查键值是否存在
print("\n设置key3的过期时间为3秒")
print(db)
ttl=  db.ttl("tt")
print(ttl)
if ttl:
    time.sleep(ttl + 0.1)
    print("sleep", db)
db.clear(wait=True)
# db['key3'] = 'v3'
print(db)
db.set('key3', 'value3', ex=3)
print(db)
print(len(db))
print("key3的值:", db.get('key3'))
time.sleep(1)
print("key3的TTL值:", db.ttl('key3'))
print(db['key3'])
for key, value in db.items():
    print(key, value)
print("等待3秒...")
time.sleep(2)
print("3秒后，key3的值:", db.get('key3',))
# print(db['key3'])

db.set("tt", 'ttt', 10)