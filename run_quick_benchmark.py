#!/usr/bin/env python
"""
快速基准测试 - 缩小规模用于快速验证
"""

import os
import shutil
import tempfile
import time
import sys

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict
from flaxkv2.core.leveldb_dict import LevelDBDict


def benchmark_writes():
    """快速写入基准测试"""
    print("\n" + "="*60)
    print("快速写入基准测试 (1000个操作)")
    print("="*60)

    num_ops = 1000

    # 1. Raw LevelDB
    print("\n测试1: Raw LevelDB (无缓冲)")
    temp_dir1 = tempfile.mkdtemp()
    try:
        db1 = RawLevelDBDict("bench", temp_dir1, raw=True)
        start = time.time()
        for i in range(num_ops):
            db1[f"key{i}"] = f"value{i}"
        elapsed1 = time.time() - start
        db1.close()
        print(f"  耗时: {elapsed1:.3f}秒")
        print(f"  吞吐量: {num_ops/elapsed1:.2f} ops/sec")
    finally:
        shutil.rmtree(temp_dir1)

    # 2. LevelDBDict (缓冲, 无缓存)
    print("\n测试2: LevelDBDict (缓冲, 无缓存)")
    temp_dir2 = tempfile.mkdtemp()
    try:
        db2 = LevelDBDict("bench", temp_dir2, raw=True, cache=False, max_buffer_size=50)
        start = time.time()
        for i in range(num_ops):
            db2[f"key{i}"] = f"value{i}"
        db2.write_immediately(write=True, block=True)
        elapsed2 = time.time() - start
        db2.close(write=True, wait=True)
        print(f"  耗时: {elapsed2:.3f}秒")
        print(f"  吞吐量: {num_ops/elapsed2:.2f} ops/sec")
        print(f"  性能提升: {elapsed1/elapsed2:.2f}x")
    finally:
        shutil.rmtree(temp_dir2)

    # 3. LevelDBDict (缓冲, 有缓存)
    print("\n测试3: LevelDBDict (缓冲, 有缓存)")
    temp_dir3 = tempfile.mkdtemp()
    try:
        db3 = LevelDBDict("bench", temp_dir3, raw=True, cache=True, max_buffer_size=50)
        start = time.time()
        for i in range(num_ops):
            db3[f"key{i}"] = f"value{i}"
        db3.write_immediately(write=True, block=True)
        elapsed3 = time.time() - start
        db3.close(write=True, wait=True)
        print(f"  耗时: {elapsed3:.3f}秒")
        print(f"  吞吐量: {num_ops/elapsed3:.2f} ops/sec")
        print(f"  性能提升: {elapsed1/elapsed3:.2f}x")
    finally:
        shutil.rmtree(temp_dir3)


def benchmark_reads():
    """快速读取基准测试"""
    print("\n" + "="*60)
    print("快速读取基准测试 (1000个操作)")
    print("="*60)

    num_keys = 1000
    num_reads = 1000

    # 准备数据
    def prepare_db(db_class, temp_dir, **kwargs):
        db = db_class("bench", temp_dir, raw=True, **kwargs)
        items = {f"key{i}": f"value{i}" for i in range(num_keys)}
        db.update(items)
        if hasattr(db, 'write_immediately'):
            db.write_immediately(write=True, block=True)
        return db

    # 1. Raw LevelDB
    print("\n测试1: Raw LevelDB (无缓冲)")
    temp_dir1 = tempfile.mkdtemp()
    try:
        db1 = prepare_db(RawLevelDBDict, temp_dir1)
        start = time.time()
        for i in range(num_reads):
            _ = db1[f"key{i % num_keys}"]
        elapsed1 = time.time() - start
        db1.close()
        print(f"  耗时: {elapsed1:.3f}秒")
        print(f"  吞吐量: {num_reads/elapsed1:.2f} ops/sec")
    finally:
        shutil.rmtree(temp_dir1)

    # 2. LevelDBDict (无缓存)
    print("\n测试2: LevelDBDict (缓冲, 无缓存)")
    temp_dir2 = tempfile.mkdtemp()
    try:
        db2 = prepare_db(LevelDBDict, temp_dir2, cache=False, max_buffer_size=50)
        start = time.time()
        for i in range(num_reads):
            _ = db2[f"key{i % num_keys}"]
        elapsed2 = time.time() - start
        db2.close(write=True, wait=True)
        print(f"  耗时: {elapsed2:.3f}秒")
        print(f"  吞吐量: {num_reads/elapsed2:.2f} ops/sec")
        print(f"  性能提升: {elapsed1/elapsed2:.2f}x")
    finally:
        shutil.rmtree(temp_dir2)

    # 3. LevelDBDict (有缓存)
    print("\n测试3: LevelDBDict (缓冲, 有缓存)")
    temp_dir3 = tempfile.mkdtemp()
    try:
        db3 = prepare_db(LevelDBDict, temp_dir3, cache=True, max_buffer_size=50)
        start = time.time()
        for i in range(num_reads):
            _ = db3[f"key{i % num_keys}"]
        elapsed3 = time.time() - start
        db3.close(write=True, wait=True)
        print(f"  耗时: {elapsed3:.3f}秒")
        print(f"  吞吐量: {num_reads/elapsed3:.2f} ops/sec")
        print(f"  性能提升: {elapsed1/elapsed3:.2f}x")
    finally:
        shutil.rmtree(temp_dir3)


if __name__ == "__main__":
    print("FlaxKV2 快速基准测试")
    print("(缩小规模以便快速验证)")

    try:
        benchmark_writes()
        benchmark_reads()

        print("\n" + "="*60)
        print("测试完成！")
        print("="*60)
        print("\n提示：运行完整基准测试以获得更准确的结果：")
        print("  python run_benchmark.py")

    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
