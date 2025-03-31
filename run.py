from flaxkv2 import FlaxKV


db = FlaxKV("my_db", "./data", default_ttl=20)
print(db.stat())

# 设置新键值对和TTL
# db["key1"] = "value1"
# db["key2"] = "value2"
# db.set_ttl("key1", 50)  # 设置5秒后过期
# 再次打印
print(f"键值: {db.get('key1')}")
print(f"TTL: {db.get_ttl('key1')}")
print(f"键值: {db.get('key2')}")
print(f"TTL: {db.get_ttl('key2')}")

# db.close()
# 提示用户立即再次运行程序
print("\n请立即再次运行此程序，检查TTL是否持久化")
