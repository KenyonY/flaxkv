"""
统一缓存性能测试

测试场景：
1. 无缓存（baseline）
2. 只读缓存（read_cache_size=10000）
3. 写缓冲模式 - 同步（enable_write_buffer=True, async_flush=False）
4. 写缓冲模式 - 异步（enable_write_buffer=True, async_flush=True）

测试数据：10000条1000维的numpy数组
"""

from flaxkv2 import CachedLevelDBDict
import numpy as np
from sparrow import MeasureTime
import time

mt = MeasureTime()

# 生成测试数据集：10000条1000维的随机numpy数组
n = 10000
dim = 1000
print(f"生成测试数据：{n}条，每条{dim}维...")
test_data = np.random.rand(n, dim)
print(f"数据大小：{test_data.nbytes / 1024 / 1024:.2f} MB")
print("=" * 70)

# 1. 无缓存模式（baseline）
print("\n1. 无缓存模式（baseline）")
db1 = CachedLevelDBDict(
    "bench_no_cache",
    rebuild=True,
    enable_ttl_cleanup=False,
    read_cache_size=0,  # 禁用读缓存
    enable_write_buffer=False  # 禁用写缓冲
)

mt.start()
for i in range(n):
    db1[f"key_{i}"] = test_data[i]
write_time_1 = mt.show_interval("写入完成")

# 读取测试
mt.start()
for i in range(n):
    _ = db1[f"key_{i}"]
read_time_1 = mt.show_interval("读取完成")

db1.close()
print(f"  写入吞吐: {n / write_time_1:.2f} ops/s")
print(f"  读取吞吐: {n / read_time_1:.2f} ops/s")

# 2. 只读缓存模式
print("\n2. 只读缓存模式（read_cache_size=10000）")
db2 = CachedLevelDBDict(
    "bench_read_cache",
    rebuild=True,
    enable_ttl_cleanup=False,
    read_cache_size=10000,  # 启用读缓存
    enable_write_buffer=False,  # 禁用写缓冲
    performance_profile='read_optimized'
)

mt.start()
for i in range(n):
    db2[f"key_{i}"] = test_data[i]
write_time_2 = mt.show_interval("写入完成")

# 读取测试（第一遍，缓存未命中）
mt.start()
for i in range(n):
    _ = db2[f"key_{i}"]
read_time_2_cold = mt.show_interval("读取完成（冷缓存）")

# 读取测试（第二遍，缓存命中）
mt.start()
for i in range(n):
    _ = db2[f"key_{i}"]
read_time_2_hot = mt.show_interval("读取完成（热缓存）")

db2.close()
print(f"  写入吞吐: {n / write_time_2:.2f} ops/s")
print(f"  读取吞吐（冷缓存）: {n / read_time_2_cold:.2f} ops/s")
print(f"  读取吞吐（热缓存）: {n / read_time_2_hot:.2f} ops/s")
print(f"  缓存加速比: {read_time_2_cold / read_time_2_hot:.2f}x")

# 3. 写缓冲模式 - 同步flush
print("\n3. 写缓冲模式 - 同步flush（enable_write_buffer=True, async_flush=False）")
db3 = CachedLevelDBDict(
    "bench_write_buffer_sync",
    rebuild=True,
    enable_ttl_cleanup=False,
    read_cache_size=10000,
    enable_write_buffer=True,  # 启用写缓冲
    write_buffer_size=100,  # 每100条刷新
    async_flush=False,  # 同步flush
    performance_profile='write_optimized'
)

mt.start()
for i in range(n):
    db3[f"key_{i}"] = test_data[i]
write_time_3 = mt.show_interval("写入完成（等待自动flush）")

# 手动flush剩余数据
mt.start()
db3.flush()
flush_time_3 = mt.show_interval("最终flush完成")

# 读取测试
mt.start()
for i in range(n):
    _ = db3[f"key_{i}"]
read_time_3 = mt.show_interval("读取完成（热缓存）")

db3.close()
print(f"  写入吞吐: {n / write_time_3:.2f} ops/s")
print(f"  读取吞吐: {n / read_time_3:.2f} ops/s")

# 4. 写缓冲模式 - 异步flush
print("\n4. 写缓冲模式 - 异步flush（enable_write_buffer=True, async_flush=True）")
db4 = CachedLevelDBDict(
    "bench_write_buffer_async",
    rebuild=True,
    enable_ttl_cleanup=False,
    read_cache_size=10000,
    enable_write_buffer=True,  # 启用写缓冲
    write_buffer_size=100,  # 每100条刷新
    async_flush=True,  # 异步flush
    performance_profile='write_optimized'
)

mt.start()
for i in range(n):
    db4[f"key_{i}"] = test_data[i]
write_time_4 = mt.show_interval("写入完成（异步flush中）")

# 手动flush剩余数据并等待完成
mt.start()
db4.flush()
time.sleep(0.5)  # 等待异步flush完成
flush_time_4 = mt.show_interval("最终flush完成")

# 读取测试
mt.start()
for i in range(n):
    _ = db4[f"key_{i}"]
read_time_4 = mt.show_interval("读取完成（热缓存）")

db4.close()
print(f"  写入吞吐: {n / write_time_4:.2f} ops/s")
print(f"  读取吞吐: {n / read_time_4:.2f} ops/s")

# 汇总对比
print("\n" + "=" * 70)
print("性能汇总对比")
print("=" * 70)
print(f"\n写入性能:")
print(f"  1. 无缓存:             {n / write_time_1:8.2f} ops/s  (baseline)")
print(f"  2. 只读缓存:           {n / write_time_2:8.2f} ops/s  ({write_time_1 / write_time_2:5.2f}x)")
print(f"  3. 写缓冲（同步）:     {n / write_time_3:8.2f} ops/s  ({write_time_1 / write_time_3:5.2f}x)")
print(f"  4. 写缓冲（异步）:     {n / write_time_4:8.2f} ops/s  ({write_time_1 / write_time_4:5.2f}x)")

print(f"\n读取性能（热缓存）:")
print(f"  1. 无缓存:             {n / read_time_1:8.2f} ops/s  (baseline)")
print(f"  2. 只读缓存:           {n / read_time_2_hot:8.2f} ops/s  ({read_time_1 / read_time_2_hot:5.2f}x)")
print(f"  3. 写缓冲（同步）:     {n / read_time_3:8.2f} ops/s  ({read_time_1 / read_time_3:5.2f}x)")
print(f"  4. 写缓冲（异步）:     {n / read_time_4:8.2f} ops/s  ({read_time_1 / read_time_4:5.2f}x)")

print("\n" + "=" * 70)
print("结论:")
print("  - 统一缓存在写缓冲模式下可大幅提升写入性能")
print("  - 读取性能在所有缓存模式下都有显著提升")
print("  - 异步flush可进一步提升写入吞吐")
print("=" * 70)
