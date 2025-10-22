#!/usr/bin/env python
"""
FlaxKV2 NumPy和DataFrame性能基准测试

测试场景：
- NumPy数组读写性能（5000条）
- Pandas DataFrame读写性能（5000条）
对比Raw vs LevelDBDict
"""

import os
import tempfile
import time
import statistics
import shutil
from typing import List, Callable
from dataclasses import dataclass
import numpy as np
import pandas as pd

from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict
from flaxkv2.core.leveldb_dict import LevelDBDict


@dataclass
class BenchmarkResult:
    """基准测试结果"""
    name: str
    operation: str
    total_time: float
    ops_per_sec: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float


class BenchmarkRunner:
    """基准测试运行器"""

    def __init__(self, num_records: int = 5000):
        """
        初始化基准测试

        Args:
            num_records: 记录数量
        """
        self.num_records = num_records
        self.results: List[BenchmarkResult] = []

    def run_benchmark(
        self,
        name: str,
        operation: str,
        setup_func: Callable,
        test_func: Callable,
        cleanup_func: Callable
    ) -> BenchmarkResult:
        """
        运行单个基准测试

        Args:
            name: 测试名称
            operation: 操作类型
            setup_func: 设置函数
            test_func: 测试函数（返回延迟列表）
            cleanup_func: 清理函数

        Returns:
            BenchmarkResult: 测试结果
        """
        print(f"\n{'='*60}")
        print(f"运行测试: {name} - {operation}")
        print(f"{'='*60}")

        # 设置
        db, temp_dir = setup_func()

        # 运行测试
        latencies = test_func(db)

        # 计算统计
        total_time = sum(latencies)
        ops_per_sec = len(latencies) / total_time if total_time > 0 else 0
        avg_latency = statistics.mean(latencies) * 1000  # 转为ms
        sorted_latencies = sorted(latencies)
        p50 = sorted_latencies[len(sorted_latencies) // 2] * 1000
        p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)] * 1000
        p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)] * 1000
        min_lat = min(latencies) * 1000
        max_lat = max(latencies) * 1000

        result = BenchmarkResult(
            name=name,
            operation=operation,
            total_time=total_time,
            ops_per_sec=ops_per_sec,
            avg_latency_ms=avg_latency,
            p50_latency_ms=p50,
            p95_latency_ms=p95,
            p99_latency_ms=p99,
            min_latency_ms=min_lat,
            max_latency_ms=max_lat
        )

        # 打印结果
        print(f"\n结果:")
        print(f"  总时间: {total_time:.2f}秒")
        print(f"  吞吐量: {ops_per_sec:.2f} ops/sec")
        print(f"  平均延迟: {avg_latency:.3f}ms")
        print(f"  P50延迟: {p50:.3f}ms")
        print(f"  P95延迟: {p95:.3f}ms")
        print(f"  P99延迟: {p99:.3f}ms")
        print(f"  最小/最大延迟: {min_lat:.3f}ms / {max_lat:.3f}ms")

        # 清理
        cleanup_func(db, temp_dir)

        self.results.append(result)
        return result

    # ============================================================
    # NumPy数组测试
    # ============================================================

    def test_numpy_write_raw(self):
        """Raw LevelDB - NumPy数组写入"""

        def setup():
            temp_dir = tempfile.mkdtemp()
            return RawLevelDBDict("bench_db", temp_dir, rebuild=True, raw=False), temp_dir

        def test(db):
            latencies = []
            for i in range(self.num_records):
                # 创建不同大小的numpy数组 (100-1000个元素)
                size = 100 + (i % 900)
                arr = np.random.rand(size)

                start = time.perf_counter()
                db[f"numpy_{i}"] = arr
                end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "Raw LevelDB",
            "NumPy数组写入 (5000条)",
            setup, test, cleanup
        )

    def test_numpy_write_leveldb(self):
        """LevelDBDict - NumPy数组写入"""

        def setup():
            temp_dir = tempfile.mkdtemp()
            return LevelDBDict("bench_db", temp_dir, rebuild=True, raw=False,
                             max_buffer_size=5000), temp_dir

        def test(db):
            latencies = []
            for i in range(self.num_records):
                size = 100 + (i % 900)
                arr = np.random.rand(size)

                start = time.perf_counter()
                db[f"numpy_{i}"] = arr
                end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close(write=True, wait=True)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "LevelDBDict",
            "NumPy数组写入 (5000条)",
            setup, test, cleanup
        )

    def test_numpy_read_raw(self):
        """Raw LevelDB - NumPy数组读取"""

        def setup():
            temp_dir = tempfile.mkdtemp()
            db = RawLevelDBDict("bench_db", temp_dir, rebuild=True, raw=False)

            # 预填充数据
            for i in range(self.num_records):
                size = 100 + (i % 900)
                arr = np.random.rand(size)
                db[f"numpy_{i}"] = arr

            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(self.num_records):
                start = time.perf_counter()
                _ = db[f"numpy_{i}"]
                end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "Raw LevelDB",
            "NumPy数组读取 (5000条)",
            setup, test, cleanup
        )

    def test_numpy_read_leveldb(self):
        """LevelDBDict - NumPy数组读取"""

        def setup():
            temp_dir = tempfile.mkdtemp()
            db = LevelDBDict("bench_db", temp_dir, rebuild=True, raw=False,
                           max_buffer_size=5000)

            # 预填充数据
            for i in range(self.num_records):
                size = 100 + (i % 900)
                arr = np.random.rand(size)
                db[f"numpy_{i}"] = arr

            db.close(write=True, wait=True)

            # 重新打开
            db = LevelDBDict("bench_db", temp_dir, rebuild=False, raw=False)
            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(self.num_records):
                start = time.perf_counter()
                _ = db[f"numpy_{i}"]
                end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close(write=True, wait=True)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "LevelDBDict",
            "NumPy数组读取 (5000条)",
            setup, test, cleanup
        )

    # ============================================================
    # DataFrame测试
    # ============================================================

    def test_dataframe_write_raw(self):
        """Raw LevelDB - DataFrame写入"""

        def setup():
            temp_dir = tempfile.mkdtemp()
            return RawLevelDBDict("bench_db", temp_dir, rebuild=True, raw=False), temp_dir

        def test(db):
            latencies = []
            for i in range(self.num_records):
                # 创建不同大小的DataFrame (10-100行, 5列)
                rows = 10 + (i % 90)
                df = pd.DataFrame({
                    'col1': np.random.rand(rows),
                    'col2': np.random.randint(0, 100, rows),
                    'col3': np.random.choice(['A', 'B', 'C'], rows),
                    'col4': np.random.rand(rows),
                    'col5': np.random.randint(0, 1000, rows)
                })

                start = time.perf_counter()
                db[f"df_{i}"] = df
                end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "Raw LevelDB",
            "DataFrame写入 (5000条)",
            setup, test, cleanup
        )

    def test_dataframe_write_leveldb(self):
        """LevelDBDict - DataFrame写入"""

        def setup():
            temp_dir = tempfile.mkdtemp()
            return LevelDBDict("bench_db", temp_dir, rebuild=True, raw=False,
                             max_buffer_size=5000), temp_dir

        def test(db):
            latencies = []
            for i in range(self.num_records):
                rows = 10 + (i % 90)
                df = pd.DataFrame({
                    'col1': np.random.rand(rows),
                    'col2': np.random.randint(0, 100, rows),
                    'col3': np.random.choice(['A', 'B', 'C'], rows),
                    'col4': np.random.rand(rows),
                    'col5': np.random.randint(0, 1000, rows)
                })

                start = time.perf_counter()
                db[f"df_{i}"] = df
                end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close(write=True, wait=True)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "LevelDBDict",
            "DataFrame写入 (5000条)",
            setup, test, cleanup
        )

    def test_dataframe_read_raw(self):
        """Raw LevelDB - DataFrame读取"""

        def setup():
            temp_dir = tempfile.mkdtemp()
            db = RawLevelDBDict("bench_db", temp_dir, rebuild=True, raw=False)

            # 预填充数据
            for i in range(self.num_records):
                rows = 10 + (i % 90)
                df = pd.DataFrame({
                    'col1': np.random.rand(rows),
                    'col2': np.random.randint(0, 100, rows),
                    'col3': np.random.choice(['A', 'B', 'C'], rows),
                    'col4': np.random.rand(rows),
                    'col5': np.random.randint(0, 1000, rows)
                })
                db[f"df_{i}"] = df

            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(self.num_records):
                start = time.perf_counter()
                _ = db[f"df_{i}"]
                end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "Raw LevelDB",
            "DataFrame读取 (5000条)",
            setup, test, cleanup
        )

    def test_dataframe_read_leveldb(self):
        """LevelDBDict - DataFrame读取"""

        def setup():
            temp_dir = tempfile.mkdtemp()
            db = LevelDBDict("bench_db", temp_dir, rebuild=True, raw=False,
                           max_buffer_size=5000)

            # 预填充数据
            for i in range(self.num_records):
                rows = 10 + (i % 90)
                df = pd.DataFrame({
                    'col1': np.random.rand(rows),
                    'col2': np.random.randint(0, 100, rows),
                    'col3': np.random.choice(['A', 'B', 'C'], rows),
                    'col4': np.random.rand(rows),
                    'col5': np.random.randint(0, 1000, rows)
                })
                db[f"df_{i}"] = df

            db.close(write=True, wait=True)

            # 重新打开
            db = LevelDBDict("bench_db", temp_dir, rebuild=False, raw=False)
            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(self.num_records):
                start = time.perf_counter()
                _ = db[f"df_{i}"]
                end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close(write=True, wait=True)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "LevelDBDict",
            "DataFrame读取 (5000条)",
            setup, test, cleanup
        )

    def print_summary(self):
        """打印性能汇总"""
        print("\n" + "="*80)
        print("性能对比汇总")
        print("="*80)

        # 按操作类型分组
        operations = {}
        for result in self.results:
            if result.operation not in operations:
                operations[result.operation] = []
            operations[result.operation].append(result)

        # 打印每个操作的对比
        for operation, results in operations.items():
            print(f"\n{operation}:")
            print("-"*80)
            print(f"{'实现':<30} {'吞吐量 (ops/s)':<25} {'平均延迟 (ms)':<20} {'P95延迟 (ms)':<15}")
            print("-"*80)

            # 找到最快的作为基准
            fastest = max(results, key=lambda r: r.ops_per_sec)

            for result in results:
                relative = result.ops_per_sec / fastest.ops_per_sec if fastest.ops_per_sec > 0 else 0
                throughput_str = f"{result.ops_per_sec:,.2f}"
                if result == fastest:
                    throughput_str += " (基准)"
                else:
                    throughput_str += f" ({relative:.2f}x)"

                print(f"{result.name:<30} {throughput_str:<25} {result.avg_latency_ms:<20.3f} {result.p95_latency_ms:<15.3f}")

    def save_results(self, filename="benchmark_numpy_dataframe_results.csv"):
        """保存结果到CSV"""
        import csv

        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Name', 'Operation', 'Total Time (s)', 'Ops/sec',
                'Avg Latency (ms)', 'P50 (ms)', 'P95 (ms)', 'P99 (ms)',
                'Min (ms)', 'Max (ms)'
            ])

            for result in self.results:
                writer.writerow([
                    result.name,
                    result.operation,
                    f"{result.total_time:.2f}",
                    f"{result.ops_per_sec:.2f}",
                    f"{result.avg_latency_ms:.3f}",
                    f"{result.p50_latency_ms:.3f}",
                    f"{result.p95_latency_ms:.3f}",
                    f"{result.p99_latency_ms:.3f}",
                    f"{result.min_latency_ms:.3f}",
                    f"{result.max_latency_ms:.3f}"
                ])

        print(f"\n结果已保存到: {filename}")


def main():
    """运行所有基准测试"""
    print("="*80)
    print("FlaxKV2 NumPy和DataFrame性能基准测试")
    print("="*80)

    print(f"\n测试配置:")
    print(f"  - 每个测试的记录数: 5,000")
    print(f"  - NumPy数组大小: 100-1000个元素")
    print(f"  - DataFrame大小: 10-100行 × 5列")

    print(f"\n开始基准测试...")

    runner = BenchmarkRunner(num_records=5000)

    # NumPy测试
    runner.test_numpy_write_raw()
    runner.test_numpy_write_leveldb()
    runner.test_numpy_read_raw()
    runner.test_numpy_read_leveldb()

    # DataFrame测试
    runner.test_dataframe_write_raw()
    runner.test_dataframe_write_leveldb()
    runner.test_dataframe_read_raw()
    runner.test_dataframe_read_leveldb()

    # 打印汇总
    runner.print_summary()

    # 保存结果
    runner.save_results()

    print(f"\n基准测试完成!")


if __name__ == "__main__":
    main()
