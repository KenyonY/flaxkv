#!/usr/bin/env python3
"""
大文件传输详细性能分析脚本 (行级分析)

需要安装额外依赖:
    pip install line_profiler memory_profiler
"""

import os
import sys
import time
import asyncio
from pathlib import Path

# 导入项目模块
from flaxkv2 import FlaxKV
from flaxkv2.utils.async_file_transfer import upload_large_file_async


def analyze_upload_performance():
    """
    详细分析上传性能

    手动添加性能监控点来分析各个阶段的耗时
    """

    test_file = Path("test_data/test_200mb.bin")
    server_url = "tcp://127.0.0.1:25555"
    db_name = "default_db"
    password = "yao"
    chunk_size = 10 * 1024 * 1024

    # 性能监控点
    timings = {}

    async def monitored_upload():
        """带性能监控的上传函数"""
        from flaxkv2.client.async_zmq_client import AsyncRemoteDBDict

        # 1. 文件读取阶段
        t0 = time.time()
        file_size = test_file.stat().st_size
        total_chunks = (file_size + chunk_size - 1) // chunk_size

        chunks_data = []
        with open(test_file, 'rb') as f:
            for i in range(total_chunks):
                chunk = f.read(chunk_size)
                chunks_data.append((i, chunk))

        timings['文件读取'] = time.time() - t0
        print(f"✓ 文件读取耗时: {timings['文件读取']:.2f}秒")

        # 2. 建立连接阶段
        t0 = time.time()
        db = AsyncRemoteDBDict(
            db_name,
            server_url,
            timeout=60000,
            enable_encryption=True,
            password=password,
            derive_from_password=True
        )
        await db.connect()
        timings['建立连接'] = time.time() - t0
        print(f"✓ 建立连接耗时: {timings['建立连接']:.2f}秒")

        try:
            # 3. 存储元数据阶段
            t0 = time.time()
            metadata = {
                'type': 'chunked_file',
                'filename': test_file.name,
                'size': file_size,
                'chunks': total_chunks,
                'chunk_size': chunk_size,
            }
            await db.set("profile_test:meta", metadata)
            timings['存储元数据'] = time.time() - t0
            print(f"✓ 存储元数据耗时: {timings['存储元数据']:.2f}秒")

            # 4. 上传分块阶段 (逐个监控)
            chunk_times = []

            for idx, chunk_data in chunks_data:
                t0 = time.time()
                await db.set(f"profile_test:chunk:{idx}", chunk_data)
                chunk_time = time.time() - t0
                chunk_times.append(chunk_time)

                if idx % 5 == 0:  # 每5个chunk打印一次
                    avg_time = sum(chunk_times[-5:]) / min(5, len(chunk_times))
                    throughput = chunk_size / avg_time / (1024 * 1024)
                    print(f"  chunk {idx}/{total_chunks}: {chunk_time:.3f}秒, "
                          f"平均: {avg_time:.3f}秒/chunk, "
                          f"吞吐量: {throughput:.1f} MB/s")

            timings['上传分块总耗时'] = sum(chunk_times)
            timings['上传分块平均'] = sum(chunk_times) / len(chunk_times)
            timings['上传分块最大'] = max(chunk_times)
            timings['上传分块最小'] = min(chunk_times)

            print(f"\n✓ 上传分块统计:")
            print(f"  总耗时: {timings['上传分块总耗时']:.2f}秒")
            print(f"  平均: {timings['上传分块平均']:.3f}秒/chunk")
            print(f"  最大: {timings['上传分块最大']:.3f}秒")
            print(f"  最小: {timings['上传分块最小']:.3f}秒")

        finally:
            # 5. 关闭连接
            t0 = time.time()
            await db.close()
            timings['关闭连接'] = time.time() - t0
            print(f"\n✓ 关闭连接耗时: {timings['关闭连接']:.2f}秒")

    # 运行监控上传
    print("="*60)
    print("详细性能分析: 异步上传")
    print("="*60)

    total_start = time.time()
    asyncio.run(monitored_upload())
    total_time = time.time() - total_start

    # 打印汇总
    print("\n" + "="*60)
    print("性能分析汇总")
    print("="*60)

    overhead = total_time - sum([
        timings.get('文件读取', 0),
        timings.get('建立连接', 0),
        timings.get('存储元数据', 0),
        timings.get('上传分块总耗时', 0),
        timings.get('关闭连接', 0),
    ])

    print(f"\n总耗时: {total_time:.2f}秒\n")
    print(f"{'阶段':<20} {'耗时(秒)':<12} {'占比(%)':<10}")
    print("-" * 50)

    for name in ['文件读取', '建立连接', '存储元数据', '上传分块总耗时', '关闭连接']:
        if name in timings:
            t = timings[name]
            pct = t / total_time * 100
            print(f"{name:<20} {t:<12.2f} {pct:<10.1f}")

    print(f"{'其他开销':<20} {overhead:<12.2f} {overhead/total_time*100:<10.1f}")

    # 性能瓶颈分析
    print("\n" + "="*60)
    print("瓶颈分析")
    print("="*60)

    bottlenecks = sorted(
        [(name, t) for name, t in timings.items() if '分块' in name or name in ['文件读取', '建立连接', '存储元数据', '关闭连接']],
        key=lambda x: x[1] if not isinstance(x[1], list) else sum(x[1]),
        reverse=True
    )

    print("\n主要耗时操作:")
    for name, t in bottlenecks[:5]:
        if name == '上传分块总耗时':
            pct = t / total_time * 100
            print(f"  {name}: {t:.2f}秒 ({pct:.1f}%)")
            print(f"    → 这是主要瓶颈,建议:")
            print(f"      1. 增加并发数 (max_concurrency)")
            print(f"      2. 调整 chunk_size")
            print(f"      3. 检查网络带宽")
            print(f"      4. 如果启用加密,考虑加密开销")

    # 保存详细报告
    report_file = Path("profile_results/detailed_analysis.txt")
    report_file.parent.mkdir(exist_ok=True)

    with open(report_file, 'w') as f:
        f.write("="*60 + "\n")
        f.write("大文件传输详细性能分析报告\n")
        f.write("="*60 + "\n\n")

        f.write(f"测试文件: {test_file}\n")
        f.write(f"文件大小: {test_file.stat().st_size / (1024*1024):.1f} MB\n")
        f.write(f"chunk大小: {chunk_size / (1024*1024):.1f} MB\n")
        f.write(f"总耗时: {total_time:.2f}秒\n\n")

        f.write("阶段性能分析:\n")
        f.write("-" * 60 + "\n")
        for name, t in timings.items():
            if not isinstance(t, list):
                pct = t / total_time * 100
                f.write(f"{name:<20} {t:<12.2f}秒 ({pct:.1f}%)\n")

        f.write("\n" + "="*60 + "\n")
        f.write("优化建议:\n")
        f.write("="*60 + "\n\n")

        upload_pct = timings.get('上传分块总耗时', 0) / total_time * 100

        if upload_pct > 80:
            f.write("1. **主要瓶颈**: 网络传输 ({:.1f}%)\n".format(upload_pct))
            f.write("   - 建议增加并发数 (当前chunk平均传输时间: {:.3f}秒)\n".format(timings.get('上传分块平均', 0)))
            f.write("   - 检查网络带宽是否饱和\n")
            f.write("   - 考虑调整chunk_size大小\n\n")

        if timings.get('文件读取', 0) / total_time > 0.1:
            f.write("2. **次要瓶颈**: 文件读取\n")
            f.write("   - 考虑使用SSD\n")
            f.write("   - 预读文件到内存 (如果内存足够)\n\n")

        if timings.get('建立连接', 0) > 1.0:
            f.write("3. **连接建立慢**: {:.2f}秒\n".format(timings.get('建立连接', 0)))
            f.write("   - 检查网络延迟\n")
            f.write("   - 重用连接 (对于批量传输)\n\n")

    print(f"\n✓ 详细报告已保存: {report_file}")


def analyze_component_performance():
    """
    分析各个组件的性能
    """
    print("\n" + "="*60)
    print("组件性能分析")
    print("="*60)

    # 1. 序列化性能
    print("\n1. 序列化性能测试")
    print("-" * 60)

    import msgpack
    test_data = b"x" * (10 * 1024 * 1024)  # 10MB数据

    # msgpack序列化
    t0 = time.time()
    for _ in range(10):
        serialized = msgpack.packb(test_data, use_bin_type=True)
    msgpack_time = (time.time() - t0) / 10
    print(f"msgpack序列化 (10MB): {msgpack_time*1000:.2f}ms")
    print(f"吞吐量: {10/msgpack_time:.1f} MB/s")

    # msgpack反序列化
    t0 = time.time()
    for _ in range(10):
        deserialized = msgpack.unpackb(serialized, raw=False)
    msgpack_unpack_time = (time.time() - t0) / 10
    print(f"msgpack反序列化 (10MB): {msgpack_unpack_time*1000:.2f}ms")
    print(f"吞吐量: {10/msgpack_unpack_time:.1f} MB/s")

    # 2. 加密性能 (如果可用)
    print("\n2. 加密性能测试")
    print("-" * 60)

    try:
        from cryptography.fernet import Fernet
        import hashlib

        # 生成密钥
        password = "yao"
        key = hashlib.sha256(password.encode()).digest()
        key_b64 = Fernet.generate_key()
        cipher = Fernet(key_b64)

        # 加密
        t0 = time.time()
        for _ in range(10):
            encrypted = cipher.encrypt(test_data)
        encrypt_time = (time.time() - t0) / 10
        print(f"Fernet加密 (10MB): {encrypt_time*1000:.2f}ms")
        print(f"吞吐量: {10/encrypt_time:.1f} MB/s")

        # 解密
        t0 = time.time()
        for _ in range(10):
            decrypted = cipher.decrypt(encrypted)
        decrypt_time = (time.time() - t0) / 10
        print(f"Fernet解密 (10MB): {decrypt_time*1000:.2f}ms")
        print(f"吞吐量: {10/decrypt_time:.1f} MB/s")

    except ImportError:
        print("未安装 cryptography 库,跳过加密测试")

    # 3. 哈希计算性能
    print("\n3. 哈希计算性能测试")
    print("-" * 60)

    import hashlib

    t0 = time.time()
    for _ in range(10):
        h = hashlib.sha256()
        h.update(test_data)
        _ = h.hexdigest()
    hash_time = (time.time() - t0) / 10
    print(f"SHA256 (10MB): {hash_time*1000:.2f}ms")
    print(f"吞吐量: {10/hash_time:.1f} MB/s")

    # 4. ZeroMQ 性能 (如果服务器可用)
    print("\n4. ZeroMQ 通信性能测试")
    print("-" * 60)

    try:
        import zmq

        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://127.0.0.1:25555")
        socket.setsockopt(zmq.RCVTIMEO, 5000)
        socket.setsockopt(zmq.SNDTIMEO, 5000)

        # 测试小消息往返时延
        small_msg = msgpack.packb(["PING", "default_db"], use_bin_type=True)

        t0 = time.time()
        for _ in range(100):
            socket.send(small_msg)
            _ = socket.recv()
        ping_time = (time.time() - t0) / 100
        print(f"PING往返时延: {ping_time*1000:.2f}ms")

        # 测试大消息传输
        large_msg = msgpack.packb(["SET", "default_db", "test_key", test_data], use_bin_type=True)

        t0 = time.time()
        for _ in range(5):
            socket.send(large_msg)
            _ = socket.recv()
        large_msg_time = (time.time() - t0) / 5
        print(f"大消息传输 (10MB): {large_msg_time*1000:.2f}ms")
        print(f"吞吐量: {10/large_msg_time:.1f} MB/s")

        socket.close()
        context.term()

    except Exception as e:
        print(f"ZeroMQ测试失败: {e}")
        print("(请确保服务器正在运行)")


def main():
    """主函数"""
    print("="*60)
    print("FlaxKV2 详细性能分析工具")
    print("="*60)

    if len(sys.argv) > 1 and sys.argv[1] == "--components":
        # 仅分析组件性能
        analyze_component_performance()
    else:
        # 完整分析
        analyze_upload_performance()
        analyze_component_performance()


if __name__ == "__main__":
    main()
