from flaxkv2 import FlaxKV
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict
# 注意: LevelDBDict 已弃用，已移除导入
import pandas as pd

# 推荐使用 FlaxKV 工厂类
# db = FlaxKV("my_db", "./data", default_ttl=20)

# 或直接使用 RawLevelDBDict（推荐）
db = RawLevelDBDict("my_db", "./data", default_ttl=20, auto_nested=True)

# db = {}
db['df'] = pd.DataFrame([1,2,3])
db['dict'] = {'a': 1, 'b': 2, 'c': 3}
print(db)

# 不再推荐使用 LevelDBDict（已弃用）
# 如需使用，请直接导入: from flaxkv2.core.leveldb_dict import LevelDBDict
# db = LevelDBDict("my_db", "./data", default_ttl=20)
# print(db.stat())
# print(db.items())
for key,value in db.items():
    print(f"{key=}: {value=}")
print(type(db['dict']))
print(dict(db['dict']))
# db.destroy()

exit()
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
