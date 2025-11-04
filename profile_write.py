"""
性能分析：精确定位写入瓶颈
"""
import cProfile
import pstats
import io
import numpy as np
from flaxkv import FlaxKV
from flaxkv2 import CachedLevelDBDict

# 生成测试数据
n = 1000  # 使用较小的数据集快速测试
dim = 1000
test_data = np.random.rand(n, dim)

print("=" * 60)
print("FlaxKV 性能分析")
print("=" * 60)

# Profile FlaxKV
pr1 = cProfile.Profile()
db1 = FlaxKV("profile_flaxkv_1", rebuild=True)
pr1.enable()
for i in range(n):
    db1[f"key_{i}"] = test_data[i]
pr1.disable()
db1.close()

s1 = io.StringIO()
ps1 = pstats.Stats(pr1, stream=s1).sort_stats('cumulative')
ps1.print_stats(20)
print(s1.getvalue())

print("\n" + "=" * 60)
print("FlaxKV2 (带写缓冲) 性能分析")
print("=" * 60)

# Profile FlaxKV2
pr2 = cProfile.Profile()
db2 = CachedLevelDBDict("profile_flaxkv_2", rebuild=True, enable_ttl_cleanup=False,
                         enable_write_buffer=True, write_buffer_size=100)
pr2.enable()
for i in range(n):
    db2[f"key_{i}"] = test_data[i]
pr2.disable()
db2.close()

s2 = io.StringIO()
ps2 = pstats.Stats(pr2, stream=s2).sort_stats('cumulative')
ps2.print_stats(20)
print(s2.getvalue())
