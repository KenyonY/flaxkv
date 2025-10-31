"""
RemoteDBDict 性能测试
测试场景：客户端缓存启用/禁用对性能的影响
"""

import tempfile
import shutil
import time
import threading
import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from flaxkv2.client.zmq_client import RemoteDBDict


class ServerProcess:
    """管理测试服务器线程"""

    def __init__(self, port=5555):
        self.port = port
        self.server = None
        self.server_thread = None
        self.tmpdir = tempfile.mkdtemp()

    def start(self):
        """启动服务器"""
        from flaxkv2.server.zmq_server import FlaxKVServer

        print(f"启动服务器 (端口 {self.port})...")

        # 创建并启动服务器
        self.server = FlaxKVServer(
            host='127.0.0.1',
            port=self.port,
            data_dir=self.tmpdir
        )
        self.server.start(register_signals=False)

        # 在后台线程运行服务器
        self.server_thread = threading.Thread(target=self.server._server_loop, daemon=True)
        self.server_thread.start()
        time.sleep(0.5)  # 等待服务器启动

        print("服务器已启动")

    def stop(self):
        """停止服务器"""
        if self.server:
            self.server.stop()

        # 清理临时目录
        if os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()


def benchmark_sequential_write(cache_size=0, num_items=1000, port=5555):
    """顺序写入测试"""
    # 使用唯一的数据库名称避免冲突
    db_name = f'benchmark_write_{cache_size}_{int(time.time()*1000)}'
    db = RemoteDBDict(
        db_name,
        host='127.0.0.1',
        port=port,
        read_cache_size=cache_size
    )

    try:
        start = time.time()
        for i in range(num_items):
            db[f'key_{i}'] = f'value_{i}'
        elapsed = time.time() - start
        return elapsed
    finally:
        db.close()


def benchmark_sequential_read(cache_size=0, num_items=1000, port=5555):
    """顺序读取测试（测试缓存效果）"""
    # 使用唯一的数据库名称
    db_name = f'benchmark_read_{cache_size}_{int(time.time()*1000)}'

    # 先写入数据
    db = RemoteDBDict(
        db_name,
        host='127.0.0.1',
        port=port,
        read_cache_size=0  # 写入时不用缓存
    )

    for i in range(num_items):
        db[f'key_{i}'] = f'value_{i}'
    db.close()

    # 重新连接并读取
    db = RemoteDBDict(
        db_name,
        host='127.0.0.1',
        port=port,
        read_cache_size=cache_size
    )

    try:
        # 第一次读取（填充缓存）
        start = time.time()
        for i in range(num_items):
            _ = db[f'key_{i}']
        first_pass = time.time() - start

        # 第二次读取（从缓存）
        start = time.time()
        for i in range(num_items):
            _ = db[f'key_{i}']
        second_pass = time.time() - start

        return first_pass, second_pass
    finally:
        db.close()


def benchmark_random_read(cache_size=0, num_items=1000, num_reads=2000, port=5555):
    """随机读取测试（测试热点数据缓存）"""
    import random

    # 使用唯一的数据库名称
    db_name = f'benchmark_random_{cache_size}_{int(time.time()*1000)}'

    # 先写入数据
    db = RemoteDBDict(
        db_name,
        host='127.0.0.1',
        port=port,
        read_cache_size=0
    )

    for i in range(num_items):
        db[f'key_{i}'] = f'value_{i}'
    db.close()

    # 重新连接并随机读取
    db = RemoteDBDict(
        db_name,
        host='127.0.0.1',
        port=port,
        read_cache_size=cache_size
    )

    try:
        # 生成随机读取序列（80/20规则：80%请求访问20%的热点数据）
        keys = [f'key_{i}' for i in range(num_items)]
        hot_keys = keys[:num_items // 5]  # 前20%是热点
        cold_keys = keys[num_items // 5:]  # 后80%是冷数据

        read_sequence = []
        for _ in range(num_reads):
            if random.random() < 0.8:  # 80%概率访问热点
                read_sequence.append(random.choice(hot_keys))
            else:  # 20%概率访问冷数据
                read_sequence.append(random.choice(cold_keys))

        # 执行随机读取
        start = time.time()
        for key in read_sequence:
            _ = db[key]
        elapsed = time.time() - start

        return elapsed
    finally:
        db.close()


def benchmark_mixed_workload(cache_size=0, num_items=500, num_ops=1000, port=5555):
    """混合读写测试（80%读 + 20%写）"""
    import random

    # 使用唯一的数据库名称
    db_name = f'benchmark_mixed_{cache_size}_{int(time.time()*1000)}'

    # 先写入初始数据
    db = RemoteDBDict(
        db_name,
        host='127.0.0.1',
        port=port,
        read_cache_size=cache_size
    )

    try:
        for i in range(num_items):
            db[f'key_{i}'] = f'value_{i}'

        # 混合读写操作
        keys = [f'key_{i}' for i in range(num_items)]
        start = time.time()
        for _ in range(num_ops):
            key = random.choice(keys)
            if random.random() < 0.8:  # 80%读
                _ = db[key]
            else:  # 20%写
                db[key] = f'updated_{time.time()}'
        elapsed = time.time() - start

        return elapsed
    finally:
        db.close()


def print_result(name, elapsed, ops_count=None, speedup=None):
    """打印测试结果"""
    print(f"  {name}:")
    print(f"    耗时: {elapsed:.4f}秒")
    if ops_count:
        ops_per_sec = ops_count / elapsed if elapsed > 0 else 0
        print(f"    速度: {ops_per_sec:,.0f} ops/秒")
    if speedup:
        print(f"    提升: {speedup:.2f}x")


def main():
    print("=" * 70)
    print("RemoteDBDict 性能测试")
    print("=" * 70)

    PORT = 5556
    NUM_ITEMS = 1000

    # 启动测试服务器
    with ServerProcess(port=PORT):
        print("\n" + "=" * 70)
        print("1. 顺序写入测试 (缓存对写入性能影响)")
        print("=" * 70)

        print("\n无缓存:")
        elapsed_no_cache = benchmark_sequential_write(0, NUM_ITEMS, PORT)
        print_result("写入", elapsed_no_cache, NUM_ITEMS)

        print("\n有缓存 (1000条):")
        elapsed_with_cache = benchmark_sequential_write(1000, NUM_ITEMS, PORT)
        print_result("写入", elapsed_with_cache, NUM_ITEMS)

        print("\n" + "=" * 70)
        print("2. 顺序读取测试 (缓存对读取性能影响)")
        print("=" * 70)

        print("\n无缓存:")
        first, second = benchmark_sequential_read(0, NUM_ITEMS, PORT)
        print_result("第一次读取", first, NUM_ITEMS)
        print_result("第二次读取", second, NUM_ITEMS)

        print("\n有缓存 (1000条):")
        first_cached, second_cached = benchmark_sequential_read(1000, NUM_ITEMS, PORT)
        print_result("第一次读取 (填充缓存)", first_cached, NUM_ITEMS)
        print_result("第二次读取 (从缓存)", second_cached, NUM_ITEMS, speedup=second/second_cached if second_cached > 0 else 0)

        print("\n" + "=" * 70)
        print("3. 随机读取测试 (80/20热点数据访问)")
        print("=" * 70)

        NUM_READS = 2000
        print("\n无缓存:")
        elapsed_no_cache = benchmark_random_read(0, NUM_ITEMS, NUM_READS, PORT)
        print_result("随机读取", elapsed_no_cache, NUM_READS)

        print("\n有缓存 (1000条):")
        elapsed_with_cache = benchmark_random_read(1000, NUM_ITEMS, NUM_READS, PORT)
        print_result("随机读取", elapsed_with_cache, NUM_READS, speedup=elapsed_no_cache/elapsed_with_cache if elapsed_with_cache > 0 else 0)

        print("\n" + "=" * 70)
        print("4. 混合读写测试 (80%读 + 20%写)")
        print("=" * 70)

        NUM_OPS = 1000
        print("\n无缓存:")
        elapsed_no_cache = benchmark_mixed_workload(0, NUM_ITEMS // 2, NUM_OPS, PORT)
        print_result("混合操作", elapsed_no_cache, NUM_OPS)

        print("\n有缓存 (1000条):")
        elapsed_with_cache = benchmark_mixed_workload(1000, NUM_ITEMS // 2, NUM_OPS, PORT)
        print_result("混合操作", elapsed_with_cache, NUM_OPS, speedup=elapsed_no_cache/elapsed_with_cache if elapsed_with_cache > 0 else 0)

    print("\n" + "=" * 70)
    print("测试完成！")
    print("=" * 70)
    print("\n结论：")
    print("- 缓存对写入操作几乎无影响（write-through模式）")
    print("- 缓存对重复读取有显著提升")
    print("- 缓存对热点数据访问有显著提升（减少网络请求）")
    print("- 缓存对混合工作负载有明显提升")


if __name__ == '__main__':
    import random
    random.seed(42)
    main()
