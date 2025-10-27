#!/usr/bin/env python
"""
FlaxKV2 缓冲 vs 非缓冲性能对比测试

测试场景：
1. 字符串数据（10000条）
   - 写入性能
   - 读取性能
   
2. NumPy数组数据（2000条，每条长度1000）
   - 写入性能
   - 读取性能

对比：
- RawLevelDBDict（无缓冲，直接写入）
- LevelDBDict + 自动缓冲（达到阈值才写入）
- LevelDBDict + write_immediately()（每次写入后立即刷新）
"""

import os
import tempfile
import time
import statistics
import shutil
from typing import List, Callable, Tuple
from dataclasses import dataclass
import numpy as np

from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict
from flaxkv2.core.leveldb_dict import LevelDBDict


@dataclass
class BenchmarkResult:
    """基准测试结果"""
    name: str
    operation: str
    data_type: str
    total_time: float
    ops_per_sec: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float


class BufferedVsRawBenchmark:
    """缓冲 vs 非缓冲性能对比测试"""

    def __init__(self):
        """初始化测试"""
        self.results: List[BenchmarkResult] = []

    def run_benchmark(
        self,
        name: str,
        operation: str,
        data_type: str,
        setup_func: Callable,
        test_func: Callable,
        cleanup_func: Callable
    ) -> BenchmarkResult:
        """
        运行单个基准测试

        Args:
            name: 测试名称
            operation: 操作类型（write/read）
            data_type: 数据类型（string/numpy）
            setup_func: 设置函数，返回 (db, temp_dir)
            test_func: 测试函数，返回延迟列表
            cleanup_func: 清理函数

        Returns:
            BenchmarkResult: 测试结果
        """
        print(f"\n{'='*70}")
        print(f"测试: {name} - {operation} - {data_type}")
        print(f"{'='*70}")

        # 设置
        db, temp_dir = setup_func()

        try:
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
                data_type=data_type,
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
            print(f"总时间: {total_time:.3f}s")
            print(f"吞吐量: {ops_per_sec:,.0f} ops/sec")
            print(f"平均延迟: {avg_latency:.3f}ms")
            print(f"P50延迟: {p50:.3f}ms")
            print(f"P95延迟: {p95:.3f}ms")
            print(f"P99延迟: {p99:.3f}ms")

            self.results.append(result)
            return result

        finally:
            # 清理
            cleanup_func(db, temp_dir)

    # ========== 字符串测试 ==========

    def test_string_write_raw(self, num_records: int = 10000):
        """测试 RawLevelDBDict 字符串写入"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            db = RawLevelDBDict("test_db", temp_dir, rebuild=True)
            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(num_records):
                key = f"key_{i}"
                value = f"value_{i}_" + "x" * 100  # 约100字节
                
                start = time.perf_counter()
                db[key] = value
                latencies.append(time.perf_counter() - start)
            
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

        return self.run_benchmark(
            "RawLevelDBDict",
            "write",
            "string",
            setup,
            test,
            cleanup
        )

    def test_string_write_leveldb_buffered(self, num_records: int = 10000):
        """测试 LevelDBDict 字符串写入（自动缓冲）"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            # 设置较大的缓冲区，避免测试期间自动刷新
            db = LevelDBDict("test_db", temp_dir, rebuild=True, 
                           max_buffer_size=num_records + 1000)
            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(num_records):
                key = f"key_{i}"
                value = f"value_{i}_" + "x" * 100
                
                start = time.perf_counter()
                db[key] = value
                latencies.append(time.perf_counter() - start)
            
            # 最后刷新缓冲区
            db.write_immediately(write=True, block=True)
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

        return self.run_benchmark(
            "LevelDBDict (buffered)",
            "write",
            "string",
            setup,
            test,
            cleanup
        )

    def test_string_write_leveldb_immediate(self, num_records: int = 10000):
        """测试 LevelDBDict 字符串写入（每次立即刷新）"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            db = LevelDBDict("test_db", temp_dir, rebuild=True)
            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(num_records):
                key = f"key_{i}"
                value = f"value_{i}_" + "x" * 100
                
                start = time.perf_counter()
                db[key] = value
                db.write_immediately(write=True, block=True)  # 立即刷新
                latencies.append(time.perf_counter() - start)
            
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

        return self.run_benchmark(
            "LevelDBDict (immediate)",
            "write",
            "string",
            setup,
            test,
            cleanup
        )

    def test_string_read_raw(self, num_records: int = 10000):
        """测试 RawLevelDBDict 字符串读取"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            db = RawLevelDBDict("test_db", temp_dir, rebuild=True)
            # 预先写入数据
            for i in range(num_records):
                db[f"key_{i}"] = f"value_{i}_" + "x" * 100
            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(num_records):
                key = f"key_{i}"
                
                start = time.perf_counter()
                _ = db[key]
                latencies.append(time.perf_counter() - start)
            
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

        return self.run_benchmark(
            "RawLevelDBDict",
            "read",
            "string",
            setup,
            test,
            cleanup
        )

    def test_string_read_leveldb(self, num_records: int = 10000):
        """测试 LevelDBDict 字符串读取"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            db = LevelDBDict("test_db", temp_dir, rebuild=True)
            # 预先写入数据并刷新
            for i in range(num_records):
                db[f"key_{i}"] = f"value_{i}_" + "x" * 100
            db.write_immediately(write=True, block=True)
            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(num_records):
                key = f"key_{i}"
                
                start = time.perf_counter()
                _ = db[key]
                latencies.append(time.perf_counter() - start)
            
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

        return self.run_benchmark(
            "LevelDBDict",
            "read",
            "string",
            setup,
            test,
            cleanup
        )

    # ========== NumPy数组测试 ==========

    def test_numpy_write_raw(self, num_records: int = 2000, array_size: int = 1000):
        """测试 RawLevelDBDict NumPy数组写入"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            db = RawLevelDBDict("test_db", temp_dir, rebuild=True)
            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(num_records):
                key = f"array_{i}"
                value = np.random.rand(array_size)
                
                start = time.perf_counter()
                db[key] = value
                latencies.append(time.perf_counter() - start)
            
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

        return self.run_benchmark(
            "RawLevelDBDict",
            "write",
            "numpy",
            setup,
            test,
            cleanup
        )

    def test_numpy_write_leveldb_buffered(self, num_records: int = 2000, array_size: int = 1000):
        """测试 LevelDBDict NumPy数组写入（自动缓冲）"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            db = LevelDBDict("test_db", temp_dir, rebuild=True,
                           max_buffer_size=num_records + 1000)
            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(num_records):
                key = f"array_{i}"
                value = np.random.rand(array_size)
                
                start = time.perf_counter()
                db[key] = value
                latencies.append(time.perf_counter() - start)
            
            # 最后刷新缓冲区
            db.write_immediately(write=True, block=True)
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

        return self.run_benchmark(
            "LevelDBDict (buffered)",
            "write",
            "numpy",
            setup,
            test,
            cleanup
        )

    def test_numpy_write_leveldb_immediate(self, num_records: int = 2000, array_size: int = 1000):
        """测试 LevelDBDict NumPy数组写入（每次立即刷新）"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            db = LevelDBDict("test_db", temp_dir, rebuild=True)
            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(num_records):
                key = f"array_{i}"
                value = np.random.rand(array_size)
                
                start = time.perf_counter()
                db[key] = value
                db.write_immediately(write=True, block=True)
                latencies.append(time.perf_counter() - start)
            
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

        return self.run_benchmark(
            "LevelDBDict (immediate)",
            "write",
            "numpy",
            setup,
            test,
            cleanup
        )

    def test_numpy_read_raw(self, num_records: int = 2000, array_size: int = 1000):
        """测试 RawLevelDBDict NumPy数组读取"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            db = RawLevelDBDict("test_db", temp_dir, rebuild=True)
            # 预先写入数据
            for i in range(num_records):
                db[f"array_{i}"] = np.random.rand(array_size)
            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(num_records):
                key = f"array_{i}"
                
                start = time.perf_counter()
                _ = db[key]
                latencies.append(time.perf_counter() - start)
            
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

        return self.run_benchmark(
            "RawLevelDBDict",
            "read",
            "numpy",
            setup,
            test,
            cleanup
        )

    def test_numpy_read_leveldb(self, num_records: int = 2000, array_size: int = 1000):
        """测试 LevelDBDict NumPy数组读取"""
        def setup():
            temp_dir = tempfile.mkdtemp()
            db = LevelDBDict("test_db", temp_dir, rebuild=True)
            # 预先写入数据并刷新
            for i in range(num_records):
                db[f"array_{i}"] = np.random.rand(array_size)
            db.write_immediately(write=True, block=True)
            return db, temp_dir

        def test(db):
            latencies = []
            for i in range(num_records):
                key = f"array_{i}"
                
                start = time.perf_counter()
                _ = db[key]
                latencies.append(time.perf_counter() - start)
            
            return latencies

        def cleanup(db, temp_dir):
            db.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

        return self.run_benchmark(
            "LevelDBDict",
            "read",
            "numpy",
            setup,
            test,
            cleanup
        )

    def print_summary(self):
        """打印测试总结"""
        print("\n" + "="*100)
        print("测试总结")
        print("="*100)
        
        # 按数据类型和操作分组
        for data_type in ["string", "numpy"]:
            for operation in ["write", "read"]:
                print(f"\n{'='*100}")
                print(f"{data_type.upper()} - {operation.upper()}")
                print(f"{'='*100}")
                
                # 筛选相关结果
                relevant_results = [
                    r for r in self.results 
                    if r.data_type == data_type and r.operation == operation
                ]
                
                if not relevant_results:
                    continue
                
                # 打印表头
                print(f"{'实现':<30} {'总时间(s)':<12} {'吞吐量(ops/s)':<15} "
                      f"{'平均延迟(ms)':<15} {'P95延迟(ms)':<15} {'P99延迟(ms)':<15}")
                print("-" * 100)
                
                # 打印每个结果
                for result in relevant_results:
                    print(f"{result.name:<30} {result.total_time:<12.3f} "
                          f"{result.ops_per_sec:<15,.0f} {result.avg_latency_ms:<15.3f} "
                          f"{result.p95_latency_ms:<15.3f} {result.p99_latency_ms:<15.3f}")
                
                # 计算性能对比
                if len(relevant_results) >= 2:
                    print("\n性能对比（以 RawLevelDBDict 为基准）:")
                    raw_result = next((r for r in relevant_results if "Raw" in r.name), None)
                    if raw_result:
                        for result in relevant_results:
                            if result.name != raw_result.name:
                                ratio = result.ops_per_sec / raw_result.ops_per_sec
                                print(f"  {result.name}: {ratio:.2%} "
                                      f"({'faster' if ratio > 1 else 'slower'})")

    def run_all_tests(self):
        """运行所有测试"""
        print("\n" + "="*100)
        print("FlaxKV2 缓冲 vs 非缓冲性能对比测试")
        print("="*100)
        
        # 字符串测试
        print("\n" + "="*100)
        print("第一部分: 字符串测试 (10000条)")
        print("="*100)
        
        self.test_string_write_raw()
        self.test_string_write_leveldb_buffered()
        self.test_string_write_leveldb_immediate()
        self.test_string_read_raw()
        self.test_string_read_leveldb()
        
        # NumPy数组测试
        print("\n" + "="*100)
        print("第二部分: NumPy数组测试 (2000条，每条长度1000)")
        print("="*100)
        
        self.test_numpy_write_raw()
        self.test_numpy_write_leveldb_buffered()
        self.test_numpy_write_leveldb_immediate()
        self.test_numpy_read_raw()
        self.test_numpy_read_leveldb()
        
        # 打印总结
        self.print_summary()


def main():
    """主函数"""
    benchmark = BufferedVsRawBenchmark()
    benchmark.run_all_tests()
    
    print("\n" + "="*100)
    print("测试完成！")
    print("="*100)


if __name__ == "__main__":
    main()

