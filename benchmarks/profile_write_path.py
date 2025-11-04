"""
分析写入路径的性能瓶颈
"""

import cProfile
import pstats
import io
from flaxkv import FlaxKV
from flaxkv2 import CachedLevelDBDict
import numpy as np

# 生成测试数据
n = 1000  # 减少数量以便分析
dim = 1000
test_data = np.random.rand(n, dim)

print("=" * 70)
print("写入路径性能分析")
print("=" * 70)

# 分析 FlaxKV
print("\n【FlaxKV 性能分析】")
print("-" * 70)
pr1 = cProfile.Profile()
pr1.enable()

db1 = FlaxKV("profile_flaxkv", rebuild=True)
for i in range(n):
    db1[f"key_{i}"] = test_data[i]
db1.close()

pr1.disable()
s1 = io.StringIO()
ps1 = pstats.Stats(pr1, stream=s1).sort_stats('cumulative')
ps1.print_stats(20)
print(s1.getvalue())

# 分析 FlaxKV2 (无缓冲)
print("\n【FlaxKV2 无缓冲 性能分析】")
print("-" * 70)
pr2 = cProfile.Profile()
pr2.enable()

db2 = CachedLevelDBDict("profile_no_buffer", rebuild=True,
                        enable_ttl_cleanup=False,
                        enable_write_buffer=False)
for i in range(n):
    db2[f"key_{i}"] = test_data[i]
db2.close()

pr2.disable()
s2 = io.StringIO()
ps2 = pstats.Stats(pr2, stream=s2).sort_stats('cumulative')
ps2.print_stats(20)
print(s2.getvalue())

# 分析 FlaxKV2 (写缓冲)
print("\n【FlaxKV2 写缓冲 性能分析】")
print("-" * 70)
pr3 = cProfile.Profile()
pr3.enable()

db3 = CachedLevelDBDict("profile_with_buffer", rebuild=True,
                        enable_ttl_cleanup=False,
                        enable_write_buffer=True,
                        write_buffer_size=100)
for i in range(n):
    db3[f"key_{i}"] = test_data[i]
db3.close()

pr3.disable()
s3 = io.StringIO()
ps3 = pstats.Stats(pr3, stream=s3).sort_stats('cumulative')
ps3.print_stats(20)
print(s3.getvalue())
