"""
缓存配置性能基准测试

对比不同缓存配置的性能：
1. 无缓存（RawLevelDBDict）
2. 只读缓存
3. 读缓存 + 同步写缓冲
4. 读缓存 + 异步写缓冲（极致性能）
"""

import os
import shutil
import time
import tempfile
import numpy as np
from pathlib import Path

from flaxkv2 import FlaxKV


class CacheBenchmark:
    """缓存配置性能基准测试"""

    def __init__(self, num_items=10000, value_size=1000):
        """
        Args:
            num_items: 测试数据项数量
            value_size: 每个值的大小（字节）
        """
        self.num_items = num_items
        self.value_size = value_size
        self.test_data = self._generate_test_data()
        self.temp_dirs = []

    def _generate_test_data(self):
        """生成测试数据"""
        print(f"生成 {self.num_items} 个测试数据项...")
        data = {}
        for i in range(self.num_items):
            # 生成随机数据
            key = f"key_{i:06d}"
            value = {
                'id': i,
                'data': np.random.bytes(self.value_size),
                'timestamp': time.time(),
                'metadata': {'type': 'benchmark', 'index': i}
            }
            data[key] = value
        return data

    def _create_temp_dir(self):
        """创建临时目录"""
        temp_dir = tempfile.mkdtemp()
        self.temp_dirs.append(temp_dir)
        return temp_dir

    def _cleanup(self):
        """清理临时目录"""
        for temp_dir in self.temp_dirs:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
        self.temp_dirs = []

    def benchmark_no_cache(self):
        """基准测试：无缓存（RawLevelDBDict）"""
        print("\n" + "=" * 70)
        print("配置1: 无缓存（RawLevelDBDict）")
        print("=" * 70)

        temp_dir = self._create_temp_dir()
        db = FlaxKV("bench_no_cache", temp_dir)

        # 写入测试
        write_start = time.time()
        for key, value in self.test_data.items():
            db[key] = value
        write_time = time.time() - write_start
        write_ops = self.num_items / write_time

        # 关闭并重新打开（清除任何可能的缓存）
        db.close()
        db = FlaxKV("bench_no_cache", temp_dir)

        # 顺序读取测试
        read_start = time.time()
        for key in self.test_data.keys():
            _ = db[key]
        read_time = time.time() - read_start
        read_ops = self.num_items / read_time

        # 随机读取测试（热数据）
        hot_keys = list(self.test_data.keys())[:1000]
        hot_read_start = time.time()
        for _ in range(100):
            for key in hot_keys:
                _ = db[key]
        hot_read_time = time.time() - hot_read_start
        hot_read_ops = (100 * 1000) / hot_read_time

        db.close()

        print(f"写入性能: {write_ops:.2f} ops/s ({write_time:.2f}s 总计)")
        print(f"顺序读取: {read_ops:.2f} ops/s ({read_time:.2f}s 总计)")
        print(f"热数据读取: {hot_read_ops:.2f} ops/s (100次 × 1000条)")

        return {
            'name': '无缓存',
            'write_ops': write_ops,
            'read_ops': read_ops,
            'hot_read_ops': hot_read_ops,
            'write_time': write_time,
            'read_time': read_time,
        }

    def benchmark_read_cache_only(self, cache_size=5000):
        """基准测试：只读缓存"""
        print("\n" + "=" * 70)
        print(f"配置2: 只读缓存（read_cache_size={cache_size}）")
        print("=" * 70)

        temp_dir = self._create_temp_dir()
        db = FlaxKV("bench_read_cache", temp_dir, read_cache_size=cache_size)

        # 写入测试（应该和无缓存类似）
        write_start = time.time()
        for key, value in self.test_data.items():
            db[key] = value
        write_time = time.time() - write_start
        write_ops = self.num_items / write_time

        # 顺序读取测试
        read_start = time.time()
        for key in self.test_data.keys():
            _ = db[key]
        read_time = time.time() - read_start
        read_ops = self.num_items / read_time

        # 随机读取测试（热数据，应该有缓存加速）
        hot_keys = list(self.test_data.keys())[:1000]
        hot_read_start = time.time()
        for _ in range(100):
            for key in hot_keys:
                _ = db[key]
        hot_read_time = time.time() - hot_read_start
        hot_read_ops = (100 * 1000) / hot_read_time

        db.close()

        print(f"写入性能: {write_ops:.2f} ops/s ({write_time:.2f}s 总计)")
        print(f"顺序读取: {read_ops:.2f} ops/s ({read_time:.2f}s 总计)")
        print(f"热数据读取: {hot_read_ops:.2f} ops/s (100次 × 1000条)")

        return {
            'name': f'只读缓存({cache_size})',
            'write_ops': write_ops,
            'read_ops': read_ops,
            'hot_read_ops': hot_read_ops,
            'write_time': write_time,
            'read_time': read_time,
        }

    def benchmark_sync_write_buffer(self, cache_size=5000, buffer_size=100):
        """基准测试：读缓存 + 同步写缓冲"""
        print("\n" + "=" * 70)
        print(f"配置3: 读缓存 + 同步写缓冲（cache={cache_size}, buffer={buffer_size}, async=False）")
        print("=" * 70)

        temp_dir = self._create_temp_dir()
        db = FlaxKV("bench_sync_buffer", temp_dir,
                   read_cache_size=cache_size,
                   write_buffer_size=buffer_size,
                   async_flush=False)

        # 写入测试（应该比无缓存快）
        write_start = time.time()
        for key, value in self.test_data.items():
            db[key] = value
        # 确保刷新
        db.close()
        write_time = time.time() - write_start
        write_ops = self.num_items / write_time

        # 重新打开读取测试
        db = FlaxKV("bench_sync_buffer", temp_dir,
                   read_cache_size=cache_size,
                   write_buffer_size=buffer_size,
                   async_flush=False)

        # 顺序读取测试
        read_start = time.time()
        for key in self.test_data.keys():
            _ = db[key]
        read_time = time.time() - read_start
        read_ops = self.num_items / read_time

        # 随机读取测试（热数据）
        hot_keys = list(self.test_data.keys())[:1000]
        hot_read_start = time.time()
        for _ in range(100):
            for key in hot_keys:
                _ = db[key]
        hot_read_time = time.time() - hot_read_start
        hot_read_ops = (100 * 1000) / hot_read_time

        db.close()

        print(f"写入性能: {write_ops:.2f} ops/s ({write_time:.2f}s 总计)")
        print(f"顺序读取: {read_ops:.2f} ops/s ({read_time:.2f}s 总计)")
        print(f"热数据读取: {hot_read_ops:.2f} ops/s (100次 × 1000条)")

        return {
            'name': f'同步写缓冲({cache_size}/{buffer_size})',
            'write_ops': write_ops,
            'read_ops': read_ops,
            'hot_read_ops': hot_read_ops,
            'write_time': write_time,
            'read_time': read_time,
        }

    def benchmark_async_write_buffer(self, cache_size=10000, buffer_size=500):
        """基准测试：读缓存 + 异步写缓冲（极致性能）"""
        print("\n" + "=" * 70)
        print(f"配置4: 读缓存 + 异步写缓冲（cache={cache_size}, buffer={buffer_size}, async=True）")
        print("=" * 70)

        temp_dir = self._create_temp_dir()
        db = FlaxKV("bench_async_buffer", temp_dir,
                   read_cache_size=cache_size,
                   write_buffer_size=buffer_size,
                   async_flush=True)

        # 写入测试（应该最快）
        write_start = time.time()
        for key, value in self.test_data.items():
            db[key] = value
        # 确保刷新
        db.close()
        write_time = time.time() - write_start
        write_ops = self.num_items / write_time

        # 重新打开读取测试
        db = FlaxKV("bench_async_buffer", temp_dir,
                   read_cache_size=cache_size,
                   write_buffer_size=buffer_size,
                   async_flush=True)

        # 顺序读取测试
        read_start = time.time()
        for key in self.test_data.keys():
            _ = db[key]
        read_time = time.time() - read_start
        read_ops = self.num_items / read_time

        # 随机读取测试（热数据）
        hot_keys = list(self.test_data.keys())[:1000]
        hot_read_start = time.time()
        for _ in range(100):
            for key in hot_keys:
                _ = db[key]
        hot_read_time = time.time() - hot_read_start
        hot_read_ops = (100 * 1000) / hot_read_time

        db.close()

        print(f"写入性能: {write_ops:.2f} ops/s ({write_time:.2f}s 总计)")
        print(f"顺序读取: {read_ops:.2f} ops/s ({read_time:.2f}s 总计)")
        print(f"热数据读取: {hot_read_ops:.2f} ops/s (100次 × 1000条)")

        return {
            'name': f'异步写缓冲({cache_size}/{buffer_size})',
            'write_ops': write_ops,
            'read_ops': read_ops,
            'hot_read_ops': hot_read_ops,
            'write_time': write_time,
            'read_time': read_time,
        }

    def run_all_benchmarks(self):
        """运行所有基准测试"""
        print("\n" + "=" * 70)
        print("FlaxKV2 缓存配置性能基准测试")
        print("=" * 70)
        print(f"测试数据: {self.num_items} 项，每项 ~{self.value_size} 字节")
        print()

        results = []

        try:
            # 1. 无缓存
            results.append(self.benchmark_no_cache())

            # 2. 只读缓存
            results.append(self.benchmark_read_cache_only(cache_size=5000))

            # 3. 同步写缓冲
            results.append(self.benchmark_sync_write_buffer(cache_size=5000, buffer_size=100))

            # 4. 异步写缓冲（极致性能）
            results.append(self.benchmark_async_write_buffer(cache_size=10000, buffer_size=500))

            # 打印汇总
            self._print_summary(results)

        finally:
            # 清理临时文件
            self._cleanup()

    def _print_summary(self, results):
        """打印汇总结果"""
        print("\n" + "=" * 70)
        print("性能汇总对比")
        print("=" * 70)

        # 找到基准（无缓存）
        baseline = results[0]

        print(f"\n{'配置':<25} {'写入':<15} {'读取':<15} {'热读':<15}")
        print("-" * 70)

        for result in results:
            write_speedup = result['write_ops'] / baseline['write_ops']
            read_speedup = result['read_ops'] / baseline['read_ops']
            hot_speedup = result['hot_read_ops'] / baseline['hot_read_ops']

            print(
                f"{result['name']:<25} "
                f"{result['write_ops']:>7.0f} ({write_speedup:>4.1f}x)  "
                f"{result['read_ops']:>7.0f} ({read_speedup:>4.1f}x)  "
                f"{result['hot_read_ops']:>7.0f} ({hot_speedup:>4.1f}x)"
            )

        print("\n推荐配置：")
        print("  • 无缓存           - 简单可靠，适合小数据量或对一致性要求高的场景")
        print("  • 只读缓存         - 读多写少的应用，安全且性能提升明显")
        print("  • 同步写缓冲       - 生产环境，可接受极小概率数据丢失")
        print("  • 异步写缓冲（默认）- 极致性能，开发环境或可容忍数据丢失的场景")


def main():
    """主函数"""
    benchmark = CacheBenchmark(num_items=10000, value_size=1000)
    benchmark.run_all_benchmarks()


if __name__ == "__main__":
    main()
