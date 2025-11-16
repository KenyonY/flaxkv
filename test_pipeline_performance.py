#!/usr/bin/env python3
"""
测试流水线传输性能

对比：
1. 普通异步上传（有锁，串行）
2. 流水线并发上传（连接池，真并发）
"""

import asyncio
import time
import os
from pathlib import Path

from flaxkv2.utils.async_file_transfer import upload_large_file_async, format_size
from flaxkv2.client.connection_pool import upload_large_file_with_pool


async def create_test_file(file_path: Path, size_mb: int):
    """创建测试文件"""
    if file_path.exists():
        print(f"✓ 测试文件已存在: {file_path}")
        return

    print(f"\n📝 创建测试文件: {size_mb}MB...")
    with open(file_path, 'wb') as f:
        for _ in range(size_mb):
            f.write(os.urandom(1024 * 1024))
    print(f"✓ 测试文件创建完成")


async def main():
    """主函数"""
    # 配置
    server_url = "tcp://127.0.0.1:25555"
    db_name = "default_db"
    password = "yao"
    test_dir = Path("test_data")
    test_dir.mkdir(exist_ok=True)

    # 测试不同文件大小
    test_sizes = [50, 100]  # MB

    print("="*70)
    print("FlaxKV2 流水线传输性能测试")
    print("="*70)

    results = {}

    for size_mb in test_sizes:
        test_file = test_dir / f"test_{size_mb}mb.bin"

        # 创建测试文件
        await create_test_file(test_file, size_mb)

        print(f"\n{'='*70}")
        print(f"测试文件: {size_mb}MB")
        print(f"{'='*70}")

        # 测试1：普通异步上传（优化前）
        print(f"\n[测试 1/2] 普通异步上传 (max_concurrency=16)")
        print("-"*70)

        start = time.time()
        try:
            await upload_large_file_async(
                db_name,
                server_url,
                f"test_normal_{size_mb}mb",
                str(test_file),
                max_concurrency=16,
                show_progress=True,
                verify=False,  # 跳过哈希验证加快测试
                password=password,
                enable_encryption=True
            )
            time1 = time.time() - start
            throughput1 = size_mb / time1
            print(f"\n✓ 完成: {time1:.2f}秒, {throughput1:.1f} MB/s")
            results[f'{size_mb}MB_normal'] = (time1, throughput1)
        except Exception as e:
            print(f"\n✗ 失败: {e}")
            results[f'{size_mb}MB_normal'] = (None, None)

        # 等待一下
        await asyncio.sleep(1)

        # 测试2：流水线并发上传（连接池）
        print(f"\n[测试 2/2] 流水线并发上传 (pool_size=8)")
        print("-"*70)

        start = time.time()
        try:
            await upload_large_file_with_pool(
                db_name,
                server_url,
                f"test_pipeline_{size_mb}mb",
                str(test_file),
                pool_size=8,
                show_progress=True,
                verify=False,  # 跳过哈希验证加快测试
                password=password,
                enable_encryption=True
            )
            time2 = time.time() - start
            throughput2 = size_mb / time2
            print(f"\n✓ 完成: {time2:.2f}秒, {throughput2:.1f} MB/s")
            results[f'{size_mb}MB_pipeline'] = (time2, throughput2)
        except Exception as e:
            print(f"\n✗ 失败: {e}")
            results[f'{size_mb}MB_pipeline'] = (None, None)

        # 性能对比
        if results[f'{size_mb}MB_normal'][0] and results[f'{size_mb}MB_pipeline'][0]:
            improvement = (time1 - time2) / time1 * 100
            speedup = throughput2 / throughput1

            print(f"\n📊 性能对比 ({size_mb}MB):")
            print(f"{'方式':<20} {'耗时(秒)':<12} {'吞吐量(MB/s)':<15} {'相对性能'}")
            print("-"*70)
            print(f"{'普通异步上传':<20} {time1:<12.2f} {throughput1:<15.1f} 100%")
            print(f"{'流水线并发上传':<20} {time2:<12.2f} {throughput2:<15.1f} {speedup*100:.1f}%")
            print(f"\n✅ 性能提升: {improvement:+.1f}% | 加速比: {speedup:.2f}x")

    # 总结
    print(f"\n{'='*70}")
    print("测试总结")
    print(f"{'='*70}\n")

    for size_mb in test_sizes:
        normal_time, normal_throughput = results.get(f'{size_mb}MB_normal', (None, None))
        pipeline_time, pipeline_throughput = results.get(f'{size_mb}MB_pipeline', (None, None))

        if normal_time and pipeline_time:
            speedup = pipeline_throughput / normal_throughput
            print(f"{size_mb}MB 文件:")
            print(f"  普通上传: {normal_throughput:.1f} MB/s")
            print(f"  流水线上传: {pipeline_throughput:.1f} MB/s")
            print(f"  加速比: {speedup:.2f}x ({speedup*100:.1f}%)")
            print()


if __name__ == "__main__":
    asyncio.run(main())
