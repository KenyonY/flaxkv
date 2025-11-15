#!/usr/bin/env python3
"""
测试异步客户端性能
对比：同步客户端 vs 异步客户端
"""
import asyncio
import time
from flaxkv2 import FlaxKV
from flaxkv2.client.async_zmq_client import AsyncRemoteDBDict

test_data = b'x' * (10 * 1024 * 1024)  # 10MB
NUM_REQUESTS = 20  # 20个请求，共200MB

print("=" * 80)
print("异步客户端性能测试")
print(f"配置: {NUM_REQUESTS} 个 10MB 请求 = {NUM_REQUESTS * 10} MB")
print("=" * 80)

# ==================== 测试 1: 同步客户端（顺序）====================
print("\n测试 1: 同步客户端（顺序执行）")

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
for i in range(NUM_REQUESTS):
    db_sync[f'sync:test:{i}'] = test_data

time1 = time.time() - start
throughput1 = (NUM_REQUESTS * 10) / time1

print(f"  耗时: {time1:.2f}秒")
print(f"  吞吐量: {throughput1:.1f} MB/s")

db_sync.close()
time.sleep(0.5)

# ==================== 测试 2: 异步客户端（并发）====================
print("\n测试 2: 异步客户端（并发执行）")

async def async_test():
    """异步测试"""
    async with AsyncRemoteDBDict(
        'default_db',
        'tcp://127.0.0.1:25555',
        timeout=60000,
        enable_encryption=True,
        password='yao',
        derive_from_password=True
    ) as db:

        # 并发发送所有请求
        tasks = []
        for i in range(NUM_REQUESTS):
            task = db.set(f'async:test:{i}', test_data)
            tasks.append(task)

        # 等待所有请求完成
        await asyncio.gather(*tasks)

start = time.time()
asyncio.run(async_test())
time2 = time.time() - start
throughput2 = (NUM_REQUESTS * 10) / time2

print(f"  耗时: {time2:.2f}秒")
print(f"  吞吐量: {throughput2:.1f} MB/s")

# ==================== 性能对比 ====================
print("\n" + "=" * 80)
print("性能对比")
print("=" * 80)

speedup = time1 / time2
improvement = (time1 - time2) / time1 * 100

print(f"\n同步客户端（顺序）: {time1:.2f}秒 ({throughput1:.1f} MB/s)")
print(f"异步客户端（并发）: {time2:.2f}秒 ({throughput2:.1f} MB/s)")
print(f"\n加速比: {speedup:.2f}x")
print(f"性能提升: {improvement:.1f}%")

if speedup >= 3.0:
    print(f"\n🎉 优秀！异步并发提升 {speedup:.1f}x")
elif speedup >= 2.0:
    print(f"\n✅ 很好！异步并发提升 {speedup:.1f}x")
elif speedup >= 1.5:
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
        for prefix in ['sync', 'async']:
            for i in range(NUM_REQUESTS):
                try:
                    await db.delete(f'{prefix}:test:{i}')
                except:
                    pass

asyncio.run(cleanup())
print("清理完成！\n")
