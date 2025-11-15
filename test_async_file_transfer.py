#!/usr/bin/env python3
"""
测试异步文件传输性能
对比：同步并发 vs 异步并发
"""
import asyncio
import time
import tempfile
from pathlib import Path
from flaxkv2 import FlaxKV
from flaxkv2.utils.file_transfer import upload_large_file, download_large_file
from flaxkv2.utils.async_file_transfer import upload_large_file_async, download_large_file_async

# 创建测试文件（200MB）
print("=" * 80)
print("准备测试文件（200MB）...")
print("=" * 80)

test_file = Path(tempfile.gettempdir()) / "test_async_200mb.bin"
file_size = 200 * 1024 * 1024

with open(test_file, 'wb') as f:
    chunk_size = 10 * 1024 * 1024
    for i in range(20):
        f.write(bytes(range(256)) * (chunk_size // 256))

print(f"测试文件: {test_file}")
print(f"大小: {file_size / (1024 * 1024):.1f} MB\n")

# ==================== 测试 1: 同步上传 ====================
print("=" * 80)
print("测试 1: 同步上传（顺序执行）")
print("=" * 80)

db_sync = FlaxKV(
    'default_db',
    'tcp://127.0.0.1:25555',
    backend='remote',
    timeout=60000,
    enable_encryption=True,
    password='yao',
    derive_from_password=True
)

start = time.time()
metadata1 = upload_large_file(
    db_sync,
    "sync_upload",
    str(test_file),
    chunk_size=10 * 1024 * 1024,
    show_progress=True,
    verify=False
)
time1 = time.time() - start
throughput1 = file_size / time1 / (1024 * 1024)

print(f"同步上传:")
print(f"  耗时: {time1:.2f}秒")
print(f"  吞吐量: {throughput1:.1f} MB/s\n")

db_sync.close()
time.sleep(1)

# ==================== 测试 2: 异步上传 ====================
print("=" * 80)
print("测试 2: 异步上传（并发执行）")
print("=" * 80)

async def async_upload_test():
    metadata = await upload_large_file_async(
        'default_db',
        'tcp://127.0.0.1:25555',
        'async_upload',
        str(test_file),
        chunk_size=10 * 1024 * 1024,
        max_concurrency=8,
        show_progress=True,
        verify=False,
        password='yao',
        enable_encryption=True
    )
    return metadata

start = time.time()
metadata2 = asyncio.run(async_upload_test())
time2 = time.time() - start
throughput2 = file_size / time2 / (1024 * 1024)

print(f"异步上传:")
print(f"  耗时: {time2:.2f}秒")
print(f"  吞吐量: {throughput2:.1f} MB/s\n")

# ==================== 性能对比 ====================
print("=" * 80)
print("上传性能对比")
print("=" * 80)

speedup = time1 / time2
improvement = (time1 - time2) / time1 * 100

print(f"\n同步上传: {time1:.2f}秒 ({throughput1:.1f} MB/s)")
print(f"异步上传: {time2:.2f}秒 ({throughput2:.1f} MB/s)")
print(f"\n加速比: {speedup:.2f}x")
print(f"性能提升: {improvement:.1f}%")

if speedup >= 2.0:
    print(f"\n🎉 优秀！异步并发提升 {speedup:.1f}x")
elif speedup >= 1.5:
    print(f"\n✅ 很好！异步并发提升 {speedup:.1f}x")
elif speedup >= 1.2:
    print(f"\n⚠️  一般。异步并发提升 {speedup:.1f}x")
else:
    print(f"\n❌ 提升有限 ({speedup:.1f}x)")

print("=" * 80)

# ==================== 清理 ====================
print("\n清理测试数据...")

async def cleanup():
    async with AsyncRemoteDBDict(
        'default_db',
        'tcp://127.0.0.1:25555',
        timeout=60000,
        enable_encryption=True,
        password='yao',
        derive_from_password=True
    ) as db:
        for prefix in ['sync_upload', 'async_upload']:
            try:
                await db.delete(f'{prefix}:meta')
                for i in range(20):
                    try:
                        await db.delete(f'{prefix}:chunk:{i}')
                    except:
                        pass
            except:
                pass

from flaxkv2.client.async_zmq_client import AsyncRemoteDBDict
asyncio.run(cleanup())

test_file.unlink()
print("清理完成！\n")
