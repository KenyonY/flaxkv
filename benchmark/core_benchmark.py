"""
FlaxKV 核心功能性能测试脚本
"""

import asyncio
import time
import os
import string
import random
import tempfile
from pathlib import Path
import flaxkv

# --- 测试参数 ---
NUM_OPERATIONS = 10_000  # 总操作次数
VALUE_SIZE = 128       # 每个 value 的大小（字节）
BATCH_SIZE = 100       # 批量操作的批次大小

def generate_random_string(size):
    """生成指定大小的随机字符串"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

async def benchmark_set(db, keys, value):
    """测试单键写入性能"""
    start_time = time.monotonic()
    for key in keys:
        await db.set(key, value)
    end_time = time.monotonic()
    
    duration = end_time - start_time
    ops_per_sec = NUM_OPERATIONS / duration
    return "Set (Single)", ops_per_sec, duration

async def benchmark_get(db, keys):
    """测试单键读取性能"""
    # 确保数据已存在
    value = generate_random_string(VALUE_SIZE)
    for key in keys:
        await db.set(key, value)
    await db.flush()

    start_time = time.monotonic()
    for key in keys:
        await db.get(key)
    end_time = time.monotonic()

    duration = end_time - start_time
    ops_per_sec = NUM_OPERATIONS / duration
    return "Get (Single)", ops_per_sec, duration

async def benchmark_mset(db, keys, value):
    """测试批量写入性能"""
    chunks = [dict() for _ in range(NUM_OPERATIONS // BATCH_SIZE)]
    for i, key in enumerate(keys):
        chunk_index = i // BATCH_SIZE
        chunks[chunk_index][key] = value

    start_time = time.monotonic()
    for chunk in chunks:
        await db.mset(chunk)
    end_time = time.monotonic()

    duration = end_time - start_time
    ops_per_sec = NUM_OPERATIONS / duration
    return f"Set (Batch, size={BATCH_SIZE})", ops_per_sec, duration

async def benchmark_mget(db, keys):
    """测试批量读取性能"""
    value = generate_random_string(VALUE_SIZE)
    await db.mset({key: value for key in keys})
    await db.flush()

    key_chunks = [keys[i:i + BATCH_SIZE] for i in range(0, NUM_OPERATIONS, BATCH_SIZE)]

    start_time = time.monotonic()
    for chunk in key_chunks:
        await db.mget(chunk)
    end_time = time.monotonic()

    duration = end_time - start_time
    ops_per_sec = NUM_OPERATIONS / duration
    return f"Get (Batch, size={BATCH_SIZE})", ops_per_sec, duration

async def benchmark_delete(db, keys):
    """测试单键删除性能"""
    value = generate_random_string(VALUE_SIZE)
    await db.mset({key: value for key in keys})
    await db.flush()

    start_time = time.monotonic()
    for key in keys:
        await db.delete(key)
    end_time = time.monotonic()

    duration = end_time - start_time
    ops_per_sec = NUM_OPERATIONS / duration
    return "Delete (Single)", ops_per_sec, duration

async def benchmark_scan(db, keys):
    """测试前缀扫描性能"""
    value = generate_random_string(VALUE_SIZE)
    await db.mset({key: value for key in keys})
    await db.flush()

    start_time = time.monotonic()
    # 将异步迭代器转换为列表以消耗所有结果
    _ = [item async for item in db.scan(prefix="key_")]
    end_time = time.monotonic()

    duration = end_time - start_time
    ops_per_sec = NUM_OPERATIONS / duration
    return "Scan (Prefix)", ops_per_sec, duration


async def main():
    """主函数，运行所有基准测试"""
    print("FlaxKV Core Benchmark")
    print("=" * 40)
    print(f"操作总数: {NUM_OPERATIONS}")
    print(f"Value 大小: {VALUE_SIZE} 字节")
    print(f"批量大小: {BATCH_SIZE}")
    print("-" * 40)

    results = []
    
    # 准备测试数据
    keys = [f"key_{i:06d}" for i in range(NUM_OPERATIONS)]
    value = generate_random_string(VALUE_SIZE)

    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "benchmark_db"
        config = flaxkv.FlaxKVConfig.for_high_performance()

        # --- 运行各项测试 ---
        
        # SET
        async with flaxkv.FlaxKV("benchmark", "local", db_path, config) as db:
            results.append(await benchmark_set(db, keys, value))
        
        # GET
        async with flaxkv.FlaxKV("benchmark", "local", db_path, config) as db:
            results.append(await benchmark_get(db, keys))

        # MSET
        async with flaxkv.FlaxKV("benchmark", "local", db_path, config) as db:
            results.append(await benchmark_mset(db, keys, value))

        # MGET
        async with flaxkv.FlaxKV("benchmark", "local", db_path, config) as db:
            results.append(await benchmark_mget(db, keys))

        # DELETE
        async with flaxkv.FlaxKV("benchmark", "local", db_path, config) as db:
            results.append(await benchmark_delete(db, keys))
            
        # SCAN
        async with flaxkv.FlaxKV("benchmark", "local", db_path, config) as db:
            results.append(await benchmark_scan(db, keys))

    # --- 打印结果 ---
    print("\n--- Benchmark Results ---")
    print(f"{ 'Operation':<25} | {'Operations/sec':>18} | {'Time Taken (s)':>18}")
    print("-" * 70)
    for name, ops_sec, duration in results:
        print(f"{name:<25} | {ops_sec:>18,.2f} | {duration:>18.4f}")
    print("-" * 70)


if __name__ == "__main__":
    asyncio.run(main())
