
from flaxkv import FlaxKV
from flaxkv2 import RawLevelDBDict, CachedLevelDBDict
import numpy as np
from sparrow import MeasureTime

mt = MeasureTime()


# 生成测试数据集：2000条1000维的随机numpy数组
n = 10000
dim = 1000
test_data = np.random.rand(n, dim)

db1 = FlaxKV("flaxkv_1", rebuild=True)
db2 = RawLevelDBDict("flaxkv_2", rebuild=True,enable_ttl_cleanup=False)
# db2 = CachedLevelDBDict("flaxkv_2_cached", read_cache_size=n, performance_profile='read_optimized', enable_ttl_cleanup=False, rebuild=True)

mt.start()
for i in range(n):
    db1[f"key_{i}"] = test_data[i]
mt.show_interval()

for i in range(n):
    db2[f"key_{i}"] = test_data[i]
mt.show_interval()
# db1.destroy()
# db2.destroy()


