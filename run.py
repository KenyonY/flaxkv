from flaxkv2 import FlaxKV
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict
from flaxkv2.core.leveldb_dict import LevelDBDict

# db = FlaxKV("my_db", "./data", default_ttl=20)
# db = RawLevelDBDict("my_db", "./data", default_ttl=20)
db = LevelDBDict("my_db", "./data", default_ttl=20)
print(db.stat())

# set_key = True
set_key = 0
# 设置新键值对和TTL
if set_key:
    db["key1"] = "value1"
    db["key2"] = "value2"
    db.set_ttl("key1", 50)  # 设置5秒后过期
else:
    pass
# 再次打印
print(f"键值: {db.get('key1')}")
print(f"TTL: {db.get_ttl('key1')}")
print(f"键值: {db.get('key2')}")
print(f"TTL: {db.get_ttl('key2')}")

db.close()
# 提示用户立即再次运行程序
print("\n请立即再次运行此程序，检查TTL是否持久化")
