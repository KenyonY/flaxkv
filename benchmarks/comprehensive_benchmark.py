"""
FlaxKV2 综合性能测试

这个 benchmark 系统地测试以下场景：
1. 基础性能：读写、遍历、删除
2. 缓存性能：不同缓存配置的影响
3. 嵌套结构：auto_nested vs 非递归
4. 数据类型：字符串、数字、列表、字典、NumPy、Pandas
5. 并发性能：多线程读写
6. TTL 性能：带过期时间的操作
7. 内存占用：不同配置的内存使用
"""

import os
import sys
import time
import random
import threading
import numpy as np
import pandas as pd
from typing import Dict, List, Any
from dataclasses import dataclass, field

from flaxkv2 import FlaxKV
from flaxkv2.core.cached_leveldb_dict import CachedLevelDBDict
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict

try:
    from sparrow import MeasureTime
    mt = MeasureTime()
except ImportError:
    # 如果没有 sparrow，使用简单的计时器
    class SimpleMeasureTime:
        def __init__(self):
            self.start_time = None

        def start(self):
            self.start_time = time.time()

        def show_interval(self, label):
            if self.start_time:
                elapsed = time.time() - self.start_time
                print(f"  {label}: {elapsed:.4f}秒")
                self.start_time = time.time()

    mt = SimpleMeasureTime()


@dataclass
class BenchmarkResult:
    """单个测试结果"""
    name: str
    write_time: float = 0.0
    read_time: float = 0.0
    random_read_time: float = 0.0
    update_time: float = 0.0
    delete_time: float = 0.0
    iteration_time: float = 0.0
    operations: int = 0
    memory_mb: float = 0.0
    cache_hit_rate: float = 0.0
    extra_info: Dict[str, Any] = field(default_factory=dict)

    def write_throughput(self) -> float:
        return self.operations / self.write_time if self.write_time > 0 else 0

    def read_throughput(self) -> float:
        return self.operations / self.read_time if self.read_time > 0 else 0

    def random_read_throughput(self) -> float:
        return self.operations / self.random_read_time if self.random_read_time > 0 else 0


class FlaxKVBenchmark:
    """FlaxKV2 综合性能测试"""

    def __init__(self, base_dir: str = "./benchmark_data", num_operations: int = 10000):
        self.base_dir = base_dir
        self.num_operations = num_operations
        self.results: List[BenchmarkResult] = []

        # 确保测试目录存在
        os.makedirs(base_dir, exist_ok=True)

    def cleanup(self):
        """清理测试数据"""
        import shutil
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)
        os.makedirs(self.base_dir, exist_ok=True)

    def get_memory_usage_mb(self) -> float:
        """获取当前进程内存使用（MB）"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except ImportError:
            return 0.0

    def print_separator(self, title: str):
        """打印分隔线"""
        width = 80
        print("\n" + "=" * width)
        print(f" {title} ".center(width, "="))
        print("=" * width + "\n")

    # ==================== 基础性能测试 ====================

    def benchmark_basic_operations(self, config_name: str, db_factory, n: int = None):
        """
        基础操作性能测试：写入、读取、随机读取、更新、删除、遍历

        Args:
            config_name: 配置名称
            db_factory: 数据库工厂函数 (name, path) -> db
            n: 操作数量，默认使用 self.num_operations
        """
        if n is None:
            n = self.num_operations

        print(f"\n测试: {config_name}")
        print("-" * 80)

        result = BenchmarkResult(name=config_name, operations=n)

        # 生成测试数据
        test_data = [f"value_{i}_{'x' * 50}" for i in range(n)]

        # 创建数据库
        db = db_factory(f"bench_basic_{config_name}", self.base_dir)

        try:
            # 1. 顺序写入
            start_time = time.time()
            for i in range(n):
                db[f"key_{i}"] = test_data[i]
            result.write_time = time.time() - start_time
            print(f"  ✓ 写入 {n} 条: {result.write_time:.4f}秒 ({result.write_throughput():.0f} ops/s)")

            # 2. 顺序读取
            start_time = time.time()
            for i in range(n):
                _ = db[f"key_{i}"]
            result.read_time = time.time() - start_time
            print(f"  ✓ 顺序读取 {n} 条: {result.read_time:.4f}秒 ({result.read_throughput():.0f} ops/s)")

            # 3. 随机读取
            random_keys = [f"key_{random.randint(0, n-1)}" for _ in range(n)]
            start_time = time.time()
            for key in random_keys:
                _ = db[key]
            result.random_read_time = time.time() - start_time
            print(f"  ✓ 随机读取 {n} 条: {result.random_read_time:.4f}秒 ({result.random_read_throughput():.0f} ops/s)")

            # 4. 更新
            start_time = time.time()
            for i in range(min(n, 1000)):  # 只更新部分数据
                db[f"key_{i}"] = f"updated_{i}"
            result.update_time = time.time() - start_time
            print(f"  ✓ 更新 {min(n, 1000)} 条: {result.update_time:.4f}秒")

            # 5. 遍历
            start_time = time.time()
            count = 0
            for k, v in db.items():
                count += 1
            result.iteration_time = time.time() - start_time
            print(f"  ✓ 遍历 {count} 条: {result.iteration_time:.4f}秒")

            # 6. 删除
            start_time = time.time()
            for i in range(min(n, 1000)):  # 只删除部分数据
                del db[f"key_{i}"]
            result.delete_time = time.time() - start_time
            print(f"  ✓ 删除 {min(n, 1000)} 条: {result.delete_time:.4f}秒")

            # 获取缓存统计（如果支持）
            if hasattr(db, '_cache') and hasattr(db._cache, 'stats'):
                stats = db._cache.stats()
                result.cache_hit_rate = stats.get('hit_rate', 0.0)
                result.extra_info['cache_stats'] = stats
                print(f"  ✓ 缓存命中率: {result.cache_hit_rate:.2%}")

            # 内存使用
            result.memory_mb = self.get_memory_usage_mb()
            if result.memory_mb > 0:
                print(f"  ✓ 内存使用: {result.memory_mb:.2f} MB")

        finally:
            db.close()

        self.results.append(result)
        return result

    # ==================== 缓存配置对比 ====================

    def benchmark_cache_configs(self):
        """测试不同缓存配置的性能"""
        self.print_separator("缓存配置对比测试")

        configs = [
            ("无缓存 (RawLevelDBDict)",
             lambda name, path: RawLevelDBDict(name, path, rebuild=True)),

            ("只读缓存 (5000条)",
             lambda name, path: CachedLevelDBDict(name, path, rebuild=True,
                                                  read_cache_size=5000,
                                                  enable_write_buffer=False)),

            ("读缓存+同步写缓冲 (5000+100)",
             lambda name, path: CachedLevelDBDict(name, path, rebuild=True,
                                                  read_cache_size=5000,
                                                  enable_write_buffer=True,
                                                  write_buffer_size=100,
                                                  async_flush=False)),

            ("读缓存+异步写缓冲 (5000+100)",
             lambda name, path: CachedLevelDBDict(name, path, rebuild=True,
                                                  read_cache_size=5000,
                                                  enable_write_buffer=True,
                                                  write_buffer_size=100,
                                                  async_flush=True)),

            ("大缓存 (50000条)",
             lambda name, path: CachedLevelDBDict(name, path, rebuild=True,
                                                  read_cache_size=50000,
                                                  enable_write_buffer=True,
                                                  write_buffer_size=1000,
                                                  async_flush=False)),
        ]

        for config_name, db_factory in configs:
            self.benchmark_basic_operations(config_name, db_factory)

    # ==================== 嵌套结构测试 ====================

    def benchmark_nested_structures(self):
        """测试嵌套结构性能（auto_nested vs 非递归）"""
        self.print_separator("嵌套结构性能测试")

        n = 100  # 嵌套结构测试用较小数据量

        # 生成测试数据
        big_list = list(range(1000))
        big_dict = {f"lst{i}": big_list for i in range(100)}

        # 测试 1: 非递归模式
        print("\n测试: 非递归存储")
        print("-" * 80)
        db_no_nested = FlaxKV(
            "bench_no_nested", self.base_dir,
            auto_nested=False, rebuild=True,
            read_cache_size=512 * 1024 * 1024,
            write_buffer_size=512 * 1024 * 1024
        )

        result_no_nested = BenchmarkResult(name="非递归存储", operations=n)

        try:
            # 写入
            start_time = time.time()
            for i in range(n):
                db_no_nested[f'dict_{i}'] = big_dict
            result_no_nested.write_time = time.time() - start_time
            print(f"  ✓ 写入 {n} 个大字典: {result_no_nested.write_time:.4f}秒")

            # 读取
            start_time = time.time()
            for i in range(n):
                _ = db_no_nested[f'dict_{i}']['lst2'][2]
            result_no_nested.read_time = time.time() - start_time
            print(f"  ✓ 读取 {n} 次嵌套访问: {result_no_nested.read_time:.4f}秒")

        finally:
            db_no_nested.close()

        # 测试 2: 递归嵌套模式
        print("\n测试: 递归嵌套存储 (auto_nested=True)")
        print("-" * 80)
        db_nested = FlaxKV(
            "bench_nested", self.base_dir,
            auto_nested=True, rebuild=True,
            read_cache_size=512 * 1024 * 1024,
            write_buffer_size=512 * 1024 * 1024
        )

        result_nested = BenchmarkResult(name="递归嵌套存储", operations=n)

        try:
            # 写入
            start_time = time.time()
            for i in range(n):
                db_nested[f'dict_{i}'] = big_dict
            result_nested.write_time = time.time() - start_time
            print(f"  ✓ 写入 {n} 个大字典: {result_nested.write_time:.4f}秒")

            # 读取
            start_time = time.time()
            for i in range(n):
                _ = db_nested[f'dict_{i}']['lst2'][2]
            result_nested.read_time = time.time() - start_time
            print(f"  ✓ 读取 {n} 次嵌套访问: {result_nested.read_time:.4f}秒")

        finally:
            db_nested.close()

        self.results.extend([result_no_nested, result_nested])

        print(f"\n嵌套对比:")
        print(f"  非递归写入: {result_no_nested.write_time:.4f}秒")
        print(f"  递归写入:   {result_nested.write_time:.4f}秒 ({result_nested.write_time/result_no_nested.write_time:.2f}x)")
        print(f"  非递归读取: {result_no_nested.read_time:.4f}秒")
        print(f"  递归读取:   {result_nested.read_time:.4f}秒 ({result_nested.read_time/result_no_nested.read_time:.2f}x)")

    # ==================== 数据类型测试 ====================

    def benchmark_data_types(self):
        """测试不同数据类型的性能"""
        self.print_separator("数据类型性能测试")

        n = min(self.num_operations, 5000)  # 使用较小数据量

        db = CachedLevelDBDict(
            "bench_types", self.base_dir, rebuild=True,
            read_cache_size=10000,
            enable_write_buffer=True,
            write_buffer_size=100
        )

        try:
            # 1. 字符串
            print("\n测试: 字符串")
            start_time = time.time()
            for i in range(n):
                db[f"str_{i}"] = f"string_value_{i}_" + "x" * 100
            str_time = time.time() - start_time
            print(f"  ✓ 写入 {n} 个字符串: {str_time:.4f}秒 ({n/str_time:.0f} ops/s)")

            # 2. 整数
            print("\n测试: 整数")
            start_time = time.time()
            for i in range(n):
                db[f"int_{i}"] = i * 12345
            int_time = time.time() - start_time
            print(f"  ✓ 写入 {n} 个整数: {int_time:.4f}秒 ({n/int_time:.0f} ops/s)")

            # 3. 列表
            print("\n测试: 列表")
            test_list = list(range(100))
            start_time = time.time()
            for i in range(n):
                db[f"list_{i}"] = test_list
            list_time = time.time() - start_time
            print(f"  ✓ 写入 {n} 个列表: {list_time:.4f}秒 ({n/list_time:.0f} ops/s)")

            # 4. 字典
            print("\n测试: 字典")
            test_dict = {f"key_{j}": j for j in range(50)}
            start_time = time.time()
            for i in range(n):
                db[f"dict_{i}"] = test_dict
            dict_time = time.time() - start_time
            print(f"  ✓ 写入 {n} 个字典: {dict_time:.4f}秒 ({n/dict_time:.0f} ops/s)")

            # 5. NumPy数组
            print("\n测试: NumPy数组")
            test_array = np.random.randn(100, 100)
            start_time = time.time()
            for i in range(min(n, 1000)):  # NumPy数组较大，用更小数量
                db[f"numpy_{i}"] = test_array
            numpy_time = time.time() - start_time
            print(f"  ✓ 写入 {min(n, 1000)} 个NumPy数组: {numpy_time:.4f}秒 ({min(n, 1000)/numpy_time:.0f} ops/s)")

            # 6. Pandas DataFrame
            print("\n测试: Pandas DataFrame")
            test_df = pd.DataFrame({
                'A': np.random.randn(100),
                'B': np.random.randint(0, 100, 100),
                'C': ['text'] * 100
            })
            start_time = time.time()
            for i in range(min(n, 1000)):  # DataFrame较大，用更小数量
                db[f"df_{i}"] = test_df
            df_time = time.time() - start_time
            print(f"  ✓ 写入 {min(n, 1000)} 个DataFrame: {df_time:.4f}秒 ({min(n, 1000)/df_time:.0f} ops/s)")

        finally:
            db.close()

    # ==================== 并发性能测试 ====================

    def benchmark_concurrency(self):
        """测试多线程并发性能"""
        self.print_separator("并发性能测试")

        n_threads = 4
        n_per_thread = self.num_operations // n_threads

        db = CachedLevelDBDict(
            "bench_concurrent", self.base_dir, rebuild=True,
            read_cache_size=10000,
            enable_write_buffer=True,
            write_buffer_size=100
        )

        try:
            # 先写入测试数据
            print(f"准备测试数据...")
            for i in range(n_per_thread * n_threads):
                db[f"key_{i}"] = f"value_{i}"

            # 并发写入测试
            print(f"\n测试: {n_threads} 线程并发写入")

            def write_worker(thread_id):
                start_idx = thread_id * n_per_thread
                for i in range(start_idx, start_idx + n_per_thread):
                    db[f"key_{i}"] = f"updated_by_thread_{thread_id}_{i}"

            start_time = time.time()
            threads = [threading.Thread(target=write_worker, args=(i,)) for i in range(n_threads)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            write_time = time.time() - start_time
            print(f"  ✓ {n_threads} 线程写入 {n_per_thread * n_threads} 条: {write_time:.4f}秒 ({(n_per_thread * n_threads)/write_time:.0f} ops/s)")

            # 并发读取测试
            print(f"\n测试: {n_threads} 线程并发读取")

            def read_worker(thread_id):
                start_idx = thread_id * n_per_thread
                for i in range(start_idx, start_idx + n_per_thread):
                    _ = db[f"key_{i}"]

            start_time = time.time()
            threads = [threading.Thread(target=read_worker, args=(i,)) for i in range(n_threads)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            read_time = time.time() - start_time
            print(f"  ✓ {n_threads} 线程读取 {n_per_thread * n_threads} 条: {read_time:.4f}秒 ({(n_per_thread * n_threads)/read_time:.0f} ops/s)")

        finally:
            db.close()

    # ==================== TTL 性能测试 ====================

    def benchmark_ttl(self):
        """测试 TTL 功能的性能影响"""
        self.print_separator("TTL 性能测试")

        n = min(self.num_operations, 5000)

        # 1. 无 TTL
        print("\n测试: 无TTL")
        db_no_ttl = CachedLevelDBDict(
            "bench_no_ttl", self.base_dir, rebuild=True,
            read_cache_size=5000,
            enable_ttl_cleanup=False
        )

        start_time = time.time()
        for i in range(n):
            db_no_ttl[f"key_{i}"] = f"value_{i}"
        no_ttl_time = time.time() - start_time
        db_no_ttl.close()
        print(f"  ✓ 写入 {n} 条: {no_ttl_time:.4f}秒 ({n/no_ttl_time:.0f} ops/s)")

        # 2. 带 TTL
        print("\n测试: 带TTL (60秒)")
        db_with_ttl = CachedLevelDBDict(
            "bench_with_ttl", self.base_dir, rebuild=True,
            read_cache_size=5000,
            default_ttl=60,
            enable_ttl_cleanup=True,
            cleanup_interval=10
        )

        start_time = time.time()
        for i in range(n):
            db_with_ttl.set(f"key_{i}", f"value_{i}", ttl=60)
        with_ttl_time = time.time() - start_time
        db_with_ttl.close()
        print(f"  ✓ 写入 {n} 条(带TTL): {with_ttl_time:.4f}秒 ({n/with_ttl_time:.0f} ops/s)")

        print(f"\nTTL 开销: {(with_ttl_time/no_ttl_time - 1) * 100:.1f}%")

    # ==================== 结果汇总 ====================

    def print_summary(self):
        """打印测试结果汇总"""
        self.print_separator("测试结果汇总")

        if not self.results:
            print("没有测试结果")
            return

        # 按写入吞吐量排序
        sorted_results = sorted(self.results, key=lambda r: r.write_throughput(), reverse=True)

        print("\n写入性能排名:")
        print(f"{'排名':<4} {'配置':<35} {'吞吐量':<15} {'耗时':<10}")
        print("-" * 70)
        for idx, result in enumerate(sorted_results, 1):
            if result.write_time > 0:
                print(f"{idx:<4} {result.name:<35} {result.write_throughput():>10.0f} ops/s {result.write_time:>8.4f}s")

        # 按读取吞吐量排序
        sorted_results = sorted(self.results, key=lambda r: r.read_throughput(), reverse=True)

        print("\n读取性能排名:")
        print(f"{'排名':<4} {'配置':<35} {'吞吐量':<15} {'耗时':<10}")
        print("-" * 70)
        for idx, result in enumerate(sorted_results, 1):
            if result.read_time > 0:
                print(f"{idx:<4} {result.name:<35} {result.read_throughput():>10.0f} ops/s {result.read_time:>8.4f}s")

        # 随机读取性能
        sorted_results = sorted(self.results, key=lambda r: r.random_read_throughput(), reverse=True)

        print("\n随机读取性能排名:")
        print(f"{'排名':<4} {'配置':<35} {'吞吐量':<15} {'耗时':<10}")
        print("-" * 70)
        for idx, result in enumerate(sorted_results, 1):
            if result.random_read_time > 0:
                print(f"{idx:<4} {result.name:<35} {result.random_read_throughput():>10.0f} ops/s {result.random_read_time:>8.4f}s")

        # 缓存命中率
        cache_results = [r for r in self.results if r.cache_hit_rate > 0]
        if cache_results:
            print("\n缓存命中率:")
            print(f"{'配置':<35} {'命中率':<10}")
            print("-" * 50)
            for result in sorted(cache_results, key=lambda r: r.cache_hit_rate, reverse=True):
                print(f"{result.name:<35} {result.cache_hit_rate:>8.2%}")


def main():
    """运行完整的 benchmark 测试套件"""
    print("=" * 80)
    print(" FlaxKV2 综合性能测试 ".center(80, "="))
    print("=" * 80)

    # 创建 benchmark 实例（使用较小数据量以便快速完成）
    benchmark = FlaxKVBenchmark(num_operations=5000)

    # 清理旧数据
    print("\n清理旧测试数据...")
    benchmark.cleanup()

    try:
        # 运行各项测试
        benchmark.benchmark_cache_configs()
        benchmark.benchmark_nested_structures()
        benchmark.benchmark_data_types()
        benchmark.benchmark_concurrency()
        benchmark.benchmark_ttl()

        # 打印汇总
        benchmark.print_summary()

    except KeyboardInterrupt:
        print("\n\n测试被用户中断")
    except Exception as e:
        print(f"\n\n测试出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 清理测试数据
        print("\n清理测试数据...")
        benchmark.cleanup()

    print("\n" + "=" * 80)
    print(" 测试完成！ ".center(80, "="))
    print("=" * 80)


if __name__ == "__main__":
    main()
