"""
Benchmark: 对比加缓存和不加缓存的性能

测试场景：
1. 顺序写入
2. 随机读取（热数据）
3. 混合读写（80%读 + 20%写）
4. 带TTL的读写
"""

import tempfile
import shutil
import time
import random
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict


def benchmark_sequential_write(num_items=10000, cache_size=0):
    """Benchmark: 顺序写入"""
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True, read_cache_size=cache_size)

        start = time.time()
        for i in range(num_items):
            db[f'key_{i}'] = f'value_{i}'
        elapsed = time.time() - start

        db.close()
        return elapsed
    finally:
        shutil.rmtree(tmpdir)


def benchmark_random_read(num_items=10000, num_reads=10000, cache_size=0):
    """Benchmark: 随机读取（热数据）"""
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True, read_cache_size=cache_size)

        # 写入数据
        for i in range(num_items):
            db[f'key_{i}'] = f'value_{i}'

        # 随机读取（模拟热数据：20%的键被80%的请求访问）
        hot_keys = [f'key_{i}' for i in range(num_items // 5)]  # 20%热键
        cold_keys = [f'key_{i}' for i in range(num_items // 5, num_items)]  # 80%冷键

        start = time.time()
        for _ in range(num_reads):
            # 80%概率读取热键，20%概率读取冷键
            if random.random() < 0.8:
                key = random.choice(hot_keys)
            else:
                key = random.choice(cold_keys)
            _ = db[key]
        elapsed = time.time() - start

        db.close()
        return elapsed
    finally:
        shutil.rmtree(tmpdir)


def benchmark_mixed_workload(num_items=10000, num_ops=10000, cache_size=0):
    """Benchmark: 混合读写（80%读 + 20%写）"""
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True, read_cache_size=cache_size)

        # 写入初始数据
        for i in range(num_items):
            db[f'key_{i}'] = f'value_{i}'

        # 混合读写
        keys = [f'key_{i}' for i in range(num_items)]
        start = time.time()
        for _ in range(num_ops):
            key = random.choice(keys)
            if random.random() < 0.8:  # 80%读
                _ = db[key]
            else:  # 20%写
                db[key] = f'updated_{time.time()}'
        elapsed = time.time() - start

        db.close()
        return elapsed
    finally:
        shutil.rmtree(tmpdir)


def benchmark_with_ttl(num_items=5000, num_reads=5000, cache_size=0):
    """Benchmark: 带TTL的读写"""
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True, read_cache_size=cache_size)

        # 写入带TTL的数据
        start_write = time.time()
        for i in range(num_items):
            db.set(f'key_{i}', f'value_{i}', ttl=60)  # 60秒TTL
        write_time = time.time() - start_write

        # 随机读取
        keys = [f'key_{i}' for i in range(num_items)]
        start_read = time.time()
        for _ in range(num_reads):
            key = random.choice(keys)
            _ = db[key]
        read_time = time.time() - start_read

        db.close()
        return write_time, read_time
    finally:
        shutil.rmtree(tmpdir)


def benchmark_auto_nested(num_items=1000, num_reads=1000, cache_size=0):
    """Benchmark: auto_nested读取"""
    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True,
                            read_cache_size=cache_size, auto_nested=True)

        # 写入嵌套字典
        for i in range(num_items):
            db[f'user_{i}'] = {
                'name': f'user_{i}',
                'age': i % 100,
                'city': f'city_{i % 10}'
            }

        # 随机读取嵌套字典
        keys = [f'user_{i}' for i in range(num_items)]
        start = time.time()
        for _ in range(num_reads):
            key = random.choice(keys)
            user = db[key]
            _ = user['name']  # 访问字段
        elapsed = time.time() - start

        db.close()
        return elapsed
    finally:
        shutil.rmtree(tmpdir)


def print_comparison(name, time_no_cache, time_with_cache):
    """打印性能对比"""
    speedup = time_no_cache / time_with_cache if time_with_cache > 0 else 0
    improvement = ((time_no_cache - time_with_cache) / time_no_cache * 100) if time_no_cache > 0 else 0

    print(f"\n{name}:")
    print(f"  无缓存:   {time_no_cache:.4f}秒")
    print(f"  有缓存:   {time_with_cache:.4f}秒")
    print(f"  加速比:   {speedup:.2f}x")
    print(f"  性能提升: {improvement:.1f}%")


def main():
    print("=" * 70)
    print("SimpleLRUCache性能Benchmark")
    print("=" * 70)

    # 参数
    NUM_ITEMS = 10000
    NUM_READS = 10000
    CACHE_SIZE = 2000  # 缓存20%的数据

    print(f"\n配置:")
    print(f"  数据量: {NUM_ITEMS}")
    print(f"  读取次数: {NUM_READS}")
    print(f"  缓存大小: {CACHE_SIZE} 条目")

    # 1. 顺序写入（缓存对写入无明显影响）
    print("\n" + "=" * 70)
    print("1. 顺序写入性能")
    print("=" * 70)
    time_no_cache = benchmark_sequential_write(NUM_ITEMS, cache_size=0)
    time_with_cache = benchmark_sequential_write(NUM_ITEMS, cache_size=CACHE_SIZE)
    print_comparison("顺序写入", time_no_cache, time_with_cache)

    # 2. 随机读取（热数据，缓存效果显著）
    print("\n" + "=" * 70)
    print("2. 随机读取性能（热数据场景）")
    print("=" * 70)
    print("说明: 20%热键被80%请求访问")
    time_no_cache = benchmark_random_read(NUM_ITEMS, NUM_READS, cache_size=0)
    time_with_cache = benchmark_random_read(NUM_ITEMS, NUM_READS, cache_size=CACHE_SIZE)
    print_comparison("随机读取（热数据）", time_no_cache, time_with_cache)

    # 3. 混合读写（80%读 + 20%写）
    print("\n" + "=" * 70)
    print("3. 混合读写性能（80%读 + 20%写）")
    print("=" * 70)
    time_no_cache = benchmark_mixed_workload(NUM_ITEMS, NUM_READS, cache_size=0)
    time_with_cache = benchmark_mixed_workload(NUM_ITEMS, NUM_READS, cache_size=CACHE_SIZE)
    print_comparison("混合读写", time_no_cache, time_with_cache)

    # 4. 带TTL的读写
    print("\n" + "=" * 70)
    print("4. 带TTL的读写性能")
    print("=" * 70)
    write_no, read_no = benchmark_with_ttl(NUM_ITEMS // 2, NUM_READS // 2, cache_size=0)
    write_with, read_with = benchmark_with_ttl(NUM_ITEMS // 2, NUM_READS // 2, cache_size=CACHE_SIZE)

    print("\n写入性能:")
    print(f"  无缓存: {write_no:.4f}秒")
    print(f"  有缓存: {write_with:.4f}秒")

    print_comparison("读取性能（TTL）", read_no, read_with)

    # 5. auto_nested读取
    print("\n" + "=" * 70)
    print("5. auto_nested读取性能")
    print("=" * 70)
    time_no_cache = benchmark_auto_nested(NUM_ITEMS // 10, NUM_READS // 10, cache_size=0)
    time_with_cache = benchmark_auto_nested(NUM_ITEMS // 10, NUM_READS // 10, cache_size=CACHE_SIZE // 10)
    print_comparison("auto_nested读取", time_no_cache, time_with_cache)

    # 总结
    print("\n" + "=" * 70)
    print("总结")
    print("=" * 70)
    print("缓存在以下场景下效果显著：")
    print("  ✅ 随机读取（热数据场景）：缓存命中率高")
    print("  ✅ 混合读写（读密集型）：减少LevelDB读取和解码开销")
    print("  ✅ 带TTL的读取：缓存自动处理TTL检查")
    print("  ✅ auto_nested读取：避免重复创建NestedDBDict实例")
    print("\n缓存对写入性能影响：")
    print("  📝 写入性能基本不变（write-through策略）")
    print("\n推荐配置：")
    print("  - 读密集型场景：read_cache_size=1000-10000")
    print("  - 写密集型场景：read_cache_size=0（禁用缓存）")
    print("=" * 70)


if __name__ == '__main__':
    random.seed(42)  # 固定随机种子，确保结果可重现
    main()
