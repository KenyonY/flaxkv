"""
测试异步flush性能对比
"""
from flaxkv import FlaxKV
from flaxkv2 import CachedLevelDBDict
import numpy as np
from sparrow import MeasureTime
import time

mt = MeasureTime()

# 生成测试数据
n = 10000
dim = 1000
test_data = np.random.rand(n, dim)

print("=" * 70)
print("异步Flush性能对比测试")
print("=" * 70)
print(f"测试数据: {n}条, 每条{dim}维numpy数组")
print()

# 测试1: FlaxKV (异步flush)
print("1. FlaxKV (异步flush - 参考基准)")
db1 = FlaxKV("test_flaxkv_async", rebuild=True)
mt.start()
for i in range(n):
    db1[f"key_{i}"] = test_data[i]
time1 = mt.show_interval()
db1.close()

# 测试2: FlaxKV2 无缓冲 (直接写入)
print("\n2. FlaxKV2 (无写缓冲 - 安全但慢)")
db2 = CachedLevelDBDict("test_flaxkv2_nobuffer",
                        rebuild=True,
                        enable_ttl_cleanup=False,
                        enable_write_buffer=False)
mt.start()
for i in range(n):
    db2[f"key_{i}"] = test_data[i]
time2 = mt.show_interval()
db2.close()

# 测试3: FlaxKV2 同步flush
print("\n3. FlaxKV2 (写缓冲 + 同步flush - 默认安全模式)")
db3 = CachedLevelDBDict("test_flaxkv2_sync",
                        rebuild=True,
                        enable_ttl_cleanup=False,
                        enable_write_buffer=True,
                        write_buffer_size=100,
                        async_flush=False)
mt.start()
for i in range(n):
    db3[f"key_{i}"] = test_data[i]
time3 = mt.show_interval()
db3.close()

# 测试4: FlaxKV2 异步flush
print("\n4. FlaxKV2 (写缓冲 + 异步flush - 极速模式) ⚡")
db4 = CachedLevelDBDict("test_flaxkv2_async",
                        rebuild=True,
                        enable_ttl_cleanup=False,
                        enable_write_buffer=True,
                        write_buffer_size=100,
                        async_flush=True)
mt.start()
for i in range(n):
    db4[f"key_{i}"] = test_data[i]
time4 = mt.show_interval()
db4.close()

# 性能对比
print("\n" + "=" * 70)
print("性能对比总结")
print("=" * 70)
print(f"1. FlaxKV (异步):                  {time1:.2f}秒 (基准)")
print(f"2. FlaxKV2 (无缓冲):               {time2:.2f}秒 ({time2/time1:.1f}x)")
print(f"3. FlaxKV2 (同步flush):            {time3:.2f}秒 ({time3/time1:.1f}x)")
print(f"4. FlaxKV2 (异步flush):            {time4:.2f}秒 ({time4/time1:.1f}x) ⚡")
print()

# 异步flush vs 同步flush
if time3 > time4:
    improvement = (time3 - time4) / time3 * 100
    speedup = time3 / time4
    print(f"✅ 异步flush性能提升: {improvement:.1f}% (提速 {speedup:.1f}x)")
else:
    print(f"⚠️ 异步flush未带来预期提升")

# 异步flush vs FlaxKV
if abs(time4 - time1) / time1 < 0.2:  # 20%误差内
    print(f"✅ FlaxKV2异步模式达到FlaxKV性能水平！")
else:
    ratio = time4 / time1
    if ratio < 1:
        print(f"🎉 FlaxKV2异步模式比FlaxKV更快 {1/ratio:.1f}x！")
    else:
        print(f"⚠️ FlaxKV2异步模式仍比FlaxKV慢 {ratio:.1f}x")

print("=" * 70)
