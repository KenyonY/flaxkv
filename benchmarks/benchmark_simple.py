#!/usr/bin/env python
"""
FlaxKV2 简化性能基准测试

对比两种实现：
1. RawLevelDBDict - 原始LevelDB（无缓冲）
2. LevelDBDict - 带缓冲（无缓存，cache参数已移除）

测试场景：
- 单个写入性能
- 批量写入性能
- 随机读取性能
- 混合读写性能
"""

import os
import shutil
import tempfile
import time
import random
import statistics
from typing import List, Dict, Callable
from dataclasses import dataclass

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

    def __init__(self, num_operations: int = 10000, batch_size: int = 1000):
        """
        初始化基准测试

        Args:
            num_operations: 每个测试的操作数
            batch_size: 批量操作的大小
        """
        self.num_operations = num_operations
        self.batch_size = batch_size
        self.results: List[BenchmarkResult] = []

    def run_benchmark(
        self,
        name: str,
        operation: str,
        setup_func: Callable,
        test_func: Callable,
        cleanup_func: Callable
    ) -> BenchmarkResult:
        """运行单个基准测试"""
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
    # 单个写入测试
    # ============================================================

    def test_single_write_raw(self):
        """Raw LevelDB - 单个写入"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            return RawLevelDBDict("bench_db", temp_dir, rebuild=True, raw=True), temp_dir

        def test(db):
            latencies = []
            for i in range(self.num_operations):
                start = time.perf_counter()
                db[f"key{i}"] = f"value{i}"
                end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "Raw LevelDB",
            "单个写入",
            setup, test, cleanup
        )

    def test_single_write_leveldb(self):
        """LevelDBDict - 单个写入"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            return LevelDBDict("bench_db", temp_dir, rebuild=True, raw=True,
                             max_buffer_size=5000), temp_dir

        def test(db):
            latencies = []
            for i in range(self.num_operations):
                start = time.perf_counter()
                db[f"key{i}"] = f"value{i}"
                end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close(write=True, wait=True)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "LevelDBDict",
            "单个写入",
            setup, test, cleanup
        )

    # ============================================================
    # 批量写入测试
    # ============================================================

    def test_batch_write_raw(self):
        """Raw LevelDB - 批量写入"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            return RawLevelDBDict("bench_db", temp_dir, rebuild=True, raw=True), temp_dir

        def test(db):
            latencies = []
            for batch_start in range(0, self.num_operations, self.batch_size):
                batch = {
                    f"key{i}": f"value{i}"
                    for i in range(batch_start, min(batch_start + self.batch_size, self.num_operations))
                }

                start = time.perf_counter()
                db.update(batch)
                end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "Raw LevelDB",
            f"批量写入 (batch={self.batch_size})",
            setup, test, cleanup
        )

    def test_batch_write_leveldb(self):
        """LevelDBDict - 批量写入"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            return LevelDBDict("bench_db", temp_dir, rebuild=True, raw=True,
                             max_buffer_size=5000), temp_dir

        def test(db):
            latencies = []
            for batch_start in range(0, self.num_operations, self.batch_size):
                batch = {
                    f"key{i}": f"value{i}"
                    for i in range(batch_start, min(batch_start + self.batch_size, self.num_operations))
                }

                start = time.perf_counter()
                db.update(batch)
                end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close(write=True, wait=True)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "LevelDBDict",
            f"批量写入 (batch={self.batch_size})",
            setup, test, cleanup
        )

    # ============================================================
    # 随机读取测试
    # ============================================================

    def test_random_read_raw(self):
        """Raw LevelDB - 随机读取"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            db = RawLevelDBDict("bench_db", temp_dir, rebuild=True, raw=True)

            # 预填充数据
            for i in range(self.num_operations):
                db[f"key{i}"] = f"value{i}"

            return db, temp_dir

        def test(db):
            latencies = []
            keys = [f"key{i}" for i in range(self.num_operations)]
            random.shuffle(keys)

            for key in keys:
                start = time.perf_counter()
                _ = db[key]
                end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "Raw LevelDB",
            "随机读取",
            setup, test, cleanup
        )

    def test_random_read_leveldb(self):
        """LevelDBDict - 随机读取"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            db = LevelDBDict("bench_db", temp_dir, rebuild=True, raw=True,
                           max_buffer_size=5000)

            # 预填充数据
            for i in range(self.num_operations):
                db[f"key{i}"] = f"value{i}"

            db.close(write=True, wait=True)

            # 重新打开
            db = LevelDBDict("bench_db", temp_dir, rebuild=False, raw=True)
            return db, temp_dir

        def test(db):
            latencies = []
            keys = [f"key{i}" for i in range(self.num_operations)]
            random.shuffle(keys)

            for key in keys:
                start = time.perf_counter()
                _ = db[key]
                end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close(write=True, wait=True)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "LevelDBDict",
            "随机读取",
            setup, test, cleanup
        )

    # ============================================================
    # 混合读写测试
    # ============================================================

    def test_mixed_workload_raw(self):
        """Raw LevelDB - 混合读写"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            db = RawLevelDBDict("bench_db", temp_dir, rebuild=True, raw=True)

            # 预填充数据
            for i in range(self.num_operations):
                db[f"key{i}"] = f"value{i}"

            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(self.num_operations):
                if random.random() < 0.8:  # 80% 读取
                    key = f"key{random.randint(0, self.num_operations - 1)}"
                    start = time.perf_counter()
                    _ = db[key]
                    end = time.perf_counter()
                else:  # 20% 写入
                    key = f"key{random.randint(0, self.num_operations - 1)}"
                    start = time.perf_counter()
                    db[key] = f"value{i}"
                    end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "Raw LevelDB",
            "混合读写 (80%读/20%写)",
            setup, test, cleanup
        )

    def test_mixed_workload_leveldb(self):
        """LevelDBDict - 混合读写"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            db = LevelDBDict("bench_db", temp_dir, rebuild=True, raw=True,
                           max_buffer_size=5000)

            # 预填充数据
            for i in range(self.num_operations):
                db[f"key{i}"] = f"value{i}"

            db.close(write=True, wait=True)

            # 重新打开
            db = LevelDBDict("bench_db", temp_dir, rebuild=False, raw=True)
            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(self.num_operations):
                if random.random() < 0.8:  # 80% 读取
                    key = f"key{random.randint(0, self.num_operations - 1)}"
                    start = time.perf_counter()
                    _ = db[key]
                    end = time.perf_counter()
                else:  # 20% 写入
                    key = f"key{random.randint(0, self.num_operations - 1)}"
                    start = time.perf_counter()
                    db[key] = f"value{i}"
                    end = time.perf_counter()
                latencies.append(end - start)
            return latencies

        def cleanup(db, temp_dir):
            db.close(write=True, wait=True)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

        return self.run_benchmark(
            "LevelDBDict",
            "混合读写 (80%读/20%写)",
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

    def save_results(self, filename="benchmark_simple_results.csv"):
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
    print("FlaxKV2 性能基准测试")
    print("="*80)

    print(f"\n测试配置:")
    print(f"  - 每个测试的操作数: 10,000")
    print(f"  - 批量大小: 1,000")

    print(f"\n开始基准测试...")

    runner = BenchmarkRunner(num_operations=10000, batch_size=1000)

    # 运行所有测试
    runner.test_single_write_raw()
    runner.test_single_write_leveldb()
    runner.test_batch_write_raw()
    runner.test_batch_write_leveldb()
    runner.test_random_read_raw()
    runner.test_random_read_leveldb()
    runner.test_mixed_workload_raw()
    runner.test_mixed_workload_leveldb()

    # 打印汇总
    runner.print_summary()

    # 保存结果
    runner.save_results()

    print(f"\n基准测试完成!")


if __name__ == "__main__":
    main()
