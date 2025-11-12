"""
FlaxKV2 快速性能测试 - 简化版

用较小数据量快速展示主要性能指标
"""

import os
import sys
import time
import random
import numpy as np
import pandas as pd

from flaxkv2 import FlaxKV
from flaxkv2.core.cached_leveldb_dict import CachedLevelDBDict
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict


def cleanup_dir(path):
    """清理测试目录"""
    import shutil
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def print_separator(title: str, width=80):
    """打印分隔线"""
    print("\n" + "=" * width)
    print(f" {title} ".center(width, "="))
    print("=" * width + "\n")


def main():
    print_separator("FlaxKV2 快速性能测试")

    base_dir = "./quick_benchmark_data"
    cleanup_dir(base_dir)

    n = 5000  # 使用较小数据量便于快速测试

    # 生成测试数据
    test_data = [f"value_{i}_{'x' * 50}" for i in range(n)]

    results = []

    # ==================== 测试 1: 无缓存 ====================
    print("\n测试 1: 无缓存 (RawLevelDBDict)")
    print("-" * 80)

    db = RawLevelDBDict("test1", base_dir, rebuild=True)

    start_time = time.time()
    for i in range(n):
        db[f"key_{i}"] = test_data[i]
    write_time = time.time() - start_time

    start_time = time.time()
    for i in range(n):
        _ = db[f"key_{i}"]
    read_time = time.time() - start_time

    db.close()

    print(f"  写入 {n} 条: {write_time:.4f}秒 ({n/write_time:.0f} ops/s)")
    print(f"  读取 {n} 条: {read_time:.4f}秒 ({n/read_time:.0f} ops/s)")
    results.append(("无缓存", write_time, read_time))

    # ==================== 测试 2: 只读缓存 ====================
    print("\n测试 2: 只读缓存 (5000条)")
    print("-" * 80)

    db = CachedLevelDBDict("test2", base_dir, rebuild=True,
                           read_cache_size=5000,
                           enable_write_buffer=False)

    start_time = time.time()
    for i in range(n):
        db[f"key_{i}"] = test_data[i]
    write_time = time.time() - start_time

    start_time = time.time()
    for i in range(n):
        _ = db[f"key_{i}"]
    read_time = time.time() - start_time

    # 获取缓存统计
    stats = db._cache.stats()
    hit_rate = stats.get('hit_rate', 0.0)

    db.close()

    print(f"  写入 {n} 条: {write_time:.4f}秒 ({n/write_time:.0f} ops/s)")
    print(f"  读取 {n} 条: {read_time:.4f}秒 ({n/read_time:.0f} ops/s)")
    print(f"  缓存命中率: {hit_rate:.2%}")
    results.append(("只读缓存", write_time, read_time))

    # ==================== 测试 3: 读缓存+同步写缓冲 ====================
    print("\n测试 3: 读缓存+同步写缓冲 (5000+100)")
    print("-" * 80)

    db = CachedLevelDBDict("test3", base_dir, rebuild=True,
                           read_cache_size=5000,
                           enable_write_buffer=True,
                           write_buffer_size=100,
                           async_flush=False)

    start_time = time.time()
    for i in range(n):
        db[f"key_{i}"] = test_data[i]
    write_time = time.time() - start_time

    start_time = time.time()
    for i in range(n):
        _ = db[f"key_{i}"]
    read_time = time.time() - start_time

    stats = db._cache.stats()
    hit_rate = stats.get('hit_rate', 0.0)

    db.close()

    print(f"  写入 {n} 条: {write_time:.4f}秒 ({n/write_time:.0f} ops/s)")
    print(f"  读取 {n} 条: {read_time:.4f}秒 ({n/read_time:.0f} ops/s)")
    print(f"  缓存命中率: {hit_rate:.2%}")
    results.append(("读缓存+同步写缓冲", write_time, read_time))

    # ==================== 测试 4: 读缓存+异步写缓冲 ====================
    print("\n测试 4: 读缓存+异步写缓冲 (5000+100)")
    print("-" * 80)

    db = CachedLevelDBDict("test4", base_dir, rebuild=True,
                           read_cache_size=5000,
                           enable_write_buffer=True,
                           write_buffer_size=100,
                           async_flush=True)

    start_time = time.time()
    for i in range(n):
        db[f"key_{i}"] = test_data[i]
    write_time = time.time() - start_time

    start_time = time.time()
    for i in range(n):
        _ = db[f"key_{i}"]
    read_time = time.time() - start_time

    stats = db._cache.stats()
    hit_rate = stats.get('hit_rate', 0.0)

    db.close()

    print(f"  写入 {n} 条: {write_time:.4f}秒 ({n/write_time:.0f} ops/s)")
    print(f"  读取 {n} 条: {read_time:.4f}秒 ({n/read_time:.0f} ops/s)")
    print(f"  缓存命中率: {hit_rate:.2%}")
    results.append(("读缓存+异步写缓冲", write_time, read_time))

    # ==================== 测试 5: 嵌套结构对比 ====================
    print("\n测试 5: 嵌套结构对比")
    print("-" * 80)

    n_nested = 50  # 使用更小数量
    big_list = list(range(1000))
    big_dict = {f"lst{i}": big_list for i in range(100)}

    # 非递归
    db_no_nested = FlaxKV("test5_no_nested", base_dir,
                          auto_nested=False, rebuild=True,
                          read_cache_size=512 * 1024 * 1024,
                          write_buffer_size=512 * 1024 * 1024)

    start_time = time.time()
    for i in range(n_nested):
        db_no_nested[f'dict_{i}'] = big_dict
    no_nested_write = time.time() - start_time

    start_time = time.time()
    for i in range(n_nested):
        _ = db_no_nested[f'dict_{i}']['lst2'][2]
    no_nested_read = time.time() - start_time

    db_no_nested.close()

    # 递归
    db_nested = FlaxKV("test5_nested", base_dir,
                       auto_nested=True, rebuild=True,
                       read_cache_size=512 * 1024 * 1024,
                       write_buffer_size=512 * 1024 * 1024)

    start_time = time.time()
    for i in range(n_nested):
        db_nested[f'dict_{i}'] = big_dict
    nested_write = time.time() - start_time

    start_time = time.time()
    for i in range(n_nested):
        _ = db_nested[f'dict_{i}']['lst2'][2]
    nested_read = time.time() - start_time

    db_nested.close()

    print(f"  非递归写入: {no_nested_write:.4f}秒")
    print(f"  递归写入:   {nested_write:.4f}秒 ({nested_write/no_nested_write:.2f}x)")
    print(f"  非递归读取: {no_nested_read:.4f}秒")
    print(f"  递归读取:   {nested_read:.4f}秒 ({nested_read/no_nested_read:.2f}x)")

    # ==================== 测试 6: 不同数据类型 ====================
    print("\n测试 6: 不同数据类型性能")
    print("-" * 80)

    n_types = 1000
    db = CachedLevelDBDict("test6", base_dir, rebuild=True,
                           read_cache_size=10000,
                           enable_write_buffer=True,
                           write_buffer_size=100)

    # 字符串
    start_time = time.time()
    for i in range(n_types):
        db[f"str_{i}"] = f"string_value_{i}_" + "x" * 100
    str_time = time.time() - start_time
    print(f"  字符串: {n_types/str_time:.0f} ops/s")

    # 整数
    start_time = time.time()
    for i in range(n_types):
        db[f"int_{i}"] = i * 12345
    int_time = time.time() - start_time
    print(f"  整数:   {n_types/int_time:.0f} ops/s")

    # 列表
    test_list = list(range(100))
    start_time = time.time()
    for i in range(n_types):
        db[f"list_{i}"] = test_list
    list_time = time.time() - start_time
    print(f"  列表:   {n_types/list_time:.0f} ops/s")

    # NumPy数组
    test_array = np.random.randn(100, 100)
    start_time = time.time()
    for i in range(min(n_types, 200)):
        db[f"numpy_{i}"] = test_array
    numpy_time = time.time() - start_time
    print(f"  NumPy:  {min(n_types, 200)/numpy_time:.0f} ops/s")

    # Pandas DataFrame
    test_df = pd.DataFrame({
        'A': np.random.randn(100),
        'B': np.random.randint(0, 100, 100),
        'C': ['text'] * 100
    })
    start_time = time.time()
    for i in range(min(n_types, 200)):
        db[f"df_{i}"] = test_df
    df_time = time.time() - start_time
    print(f"  Pandas: {min(n_types, 200)/df_time:.0f} ops/s")

    db.close()

    # ==================== 结果汇总 ====================
    print_separator("性能汇总")

    print("写入性能排名:")
    print(f"{'配置':<25} {'吞吐量':<15} {'相对提升':<10}")
    print("-" * 50)
    sorted_writes = sorted(results, key=lambda x: n/x[1], reverse=True)
    baseline_write = sorted_writes[-1][1]
    for name, write_t, _ in sorted_writes:
        throughput = n/write_t
        speedup = baseline_write/write_t
        print(f"{name:<25} {throughput:>10.0f} ops/s   {speedup:>5.2f}x")

    print("\n读取性能排名:")
    print(f"{'配置':<25} {'吞吐量':<15} {'相对提升':<10}")
    print("-" * 50)
    sorted_reads = sorted(results, key=lambda x: n/x[2], reverse=True)
    baseline_read = sorted_reads[-1][2]
    for name, _, read_t in sorted_reads:
        throughput = n/read_t
        speedup = baseline_read/read_t
        print(f"{name:<25} {throughput:>10.0f} ops/s   {speedup:>5.2f}x")

    # 清理
    print("\n清理测试数据...")
    cleanup_dir(base_dir)

    print_separator("测试完成")


if __name__ == "__main__":
    main()
