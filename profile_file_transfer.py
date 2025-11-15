#!/usr/bin/env python3
"""
大文件传输性能分析脚本

使用 cProfile 和其他性能工具分析文件传输过程中的瓶颈
"""

import os
import sys
import time
import cProfile
import pstats
import io
import asyncio
from pathlib import Path
from typing import Dict, Any

# 导入项目模块
from flaxkv2 import FlaxKV
from flaxkv2.utils.file_transfer import (
    upload_large_file,
    download_large_file,
    upload_large_file_parallel,
    download_large_file_parallel,
)
from flaxkv2.utils.async_file_transfer import (
    upload_large_file_async,
    download_large_file_async,
)


class PerformanceProfiler:
    """性能分析器"""

    def __init__(self, test_file_size_mb: int = 200, server_url: str = "tcp://127.0.0.1:25555"):
        self.test_file_size_mb = test_file_size_mb
        self.server_url = server_url
        self.db_name = "default_db"
        self.password = "yao"
        self.test_dir = Path("test_data")
        self.test_file = self.test_dir / f"test_{test_file_size_mb}mb.bin"
        self.output_dir = Path("profile_results")

        # 创建目录
        self.test_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)

    def create_test_file(self):
        """创建测试文件"""
        if self.test_file.exists():
            print(f"✓ 测试文件已存在: {self.test_file}")
            return

        print(f"\n📝 创建测试文件: {self.test_file_size_mb}MB...")
        with open(self.test_file, 'wb') as f:
            for _ in range(self.test_file_size_mb):
                f.write(os.urandom(1024 * 1024))
        print(f"✓ 测试文件创建完成")

    def cleanup_test_file(self):
        """清理测试文件"""
        if self.test_file.exists():
            self.test_file.unlink()
            print(f"✓ 测试文件已删除: {self.test_file}")

    def profile_function(self, func, func_name: str, *args, **kwargs):
        """
        使用 cProfile 分析函数性能

        Args:
            func: 要分析的函数
            func_name: 函数名称(用于报告)
            *args, **kwargs: 传递给函数的参数

        Returns:
            (函数返回值, 性能统计对象)
        """
        print(f"\n{'='*60}")
        print(f"开始性能分析: {func_name}")
        print(f"{'='*60}")

        # 创建 profiler
        profiler = cProfile.Profile()

        # 开始计时
        start_time = time.time()

        # 运行并分析
        profiler.enable()
        try:
            result = func(*args, **kwargs)
        finally:
            profiler.disable()

        # 计算总耗时
        total_time = time.time() - start_time

        # 生成统计报告
        stats = pstats.Stats(profiler)

        # 打印到控制台
        print(f"\n{'='*60}")
        print(f"性能分析结果: {func_name}")
        print(f"总耗时: {total_time:.2f} 秒")
        print(f"{'='*60}\n")

        # 按累计时间排序,显示前 20 个最耗时的函数
        print("📊 按累计时间排序的前 20 个函数:")
        print("-" * 60)
        stats.sort_stats('cumulative')
        stats.print_stats(20)

        # 按内部时间排序
        print("\n📊 按内部时间排序的前 20 个函数:")
        print("-" * 60)
        stats.sort_stats('time')
        stats.print_stats(20)

        # 保存详细报告到文件
        report_file = self.output_dir / f"{func_name}.txt"
        with open(report_file, 'w') as f:
            # 写入总体信息
            f.write(f"{'='*60}\n")
            f.write(f"性能分析报告: {func_name}\n")
            f.write(f"{'='*60}\n")
            f.write(f"总耗时: {total_time:.2f} 秒\n")
            f.write(f"测试文件: {self.test_file}\n")
            f.write(f"文件大小: {self.test_file_size_mb} MB\n")
            f.write(f"服务器: {self.server_url}\n")
            f.write(f"{'='*60}\n\n")

            # 写入详细统计
            s = io.StringIO()
            ps = pstats.Stats(profiler, stream=s)

            f.write("按累计时间排序:\n")
            f.write("-" * 60 + "\n")
            ps.sort_stats('cumulative')
            ps.print_stats(50)
            f.write(s.getvalue())

            s = io.StringIO()
            ps = pstats.Stats(profiler, stream=s)
            f.write("\n\n按内部时间排序:\n")
            f.write("-" * 60 + "\n")
            ps.sort_stats('time')
            ps.print_stats(50)
            f.write(s.getvalue())

            # 调用者/被调用者分析
            s = io.StringIO()
            ps = pstats.Stats(profiler, stream=s)
            f.write("\n\n调用关系分析:\n")
            f.write("-" * 60 + "\n")
            ps.print_callers(30)
            f.write(s.getvalue())

        print(f"\n✓ 详细报告已保存到: {report_file}")

        return result, stats, total_time

    def profile_sync_upload(self, chunk_size: int = 10 * 1024 * 1024):
        """分析同步上传"""
        def sync_upload():
            db = FlaxKV(
                self.db_name,
                self.server_url,
                backend='remote',
                enable_encryption=True,
                password=self.password
            )
            try:
                return upload_large_file(
                    db,
                    "profile_sync_upload",
                    str(self.test_file),
                    chunk_size=chunk_size,
                    show_progress=True,
                    verify=True
                )
            finally:
                db.close()

        return self.profile_function(
            sync_upload,
            "sync_upload"
        )

    def profile_parallel_upload(self, chunk_size: int = 10 * 1024 * 1024, max_workers: int = 16):
        """分析并行上传(多线程)"""
        def parallel_upload():
            db = FlaxKV(
                self.db_name,
                self.server_url,
                backend='remote',
                enable_encryption=True,
                password=self.password
            )
            try:
                return upload_large_file_parallel(
                    db,
                    "profile_parallel_upload",
                    str(self.test_file),
                    chunk_size=chunk_size,
                    max_workers=max_workers,
                    show_progress=True,
                    verify=True,
                    db_connection_params={
                        'db_name': self.db_name,
                        'url': self.server_url,
                        'backend': 'remote',
                        'enable_encryption': True,
                        'password': self.password,
                    }
                )
            finally:
                db.close()

        return self.profile_function(
            parallel_upload,
            f"parallel_upload_workers_{max_workers}"
        )

    def profile_async_upload(self, chunk_size: int = 10 * 1024 * 1024, max_concurrency: int = 16):
        """分析异步上传"""
        def async_upload():
            return asyncio.run(
                upload_large_file_async(
                    self.db_name,
                    self.server_url,
                    "profile_async_upload",
                    str(self.test_file),
                    chunk_size=chunk_size,
                    max_concurrency=max_concurrency,
                    show_progress=True,
                    verify=True,
                    password=self.password,
                    enable_encryption=True
                )
            )

        return self.profile_function(
            async_upload,
            f"async_upload_concurrency_{max_concurrency}"
        )

    def profile_sync_download(self):
        """分析同步下载"""
        output_file = self.test_dir / "download_sync.bin"

        def sync_download():
            db = FlaxKV(
                self.db_name,
                self.server_url,
                backend='remote',
                enable_encryption=True,
                password=self.password
            )
            try:
                return download_large_file(
                    db,
                    "profile_sync_upload",
                    str(output_file),
                    show_progress=True,
                    verify=True
                )
            finally:
                db.close()

        result = self.profile_function(
            sync_download,
            "sync_download"
        )

        # 清理下载文件
        if output_file.exists():
            output_file.unlink()

        return result

    def profile_parallel_download(self, max_workers: int = 16):
        """分析并行下载(多线程)"""
        output_file = self.test_dir / "download_parallel.bin"

        def parallel_download():
            db = FlaxKV(
                self.db_name,
                self.server_url,
                backend='remote',
                enable_encryption=True,
                password=self.password
            )
            try:
                return download_large_file_parallel(
                    db,
                    "profile_parallel_upload",
                    str(output_file),
                    max_workers=max_workers,
                    show_progress=True,
                    verify=True,
                    db_connection_params={
                        'db_name': self.db_name,
                        'url': self.server_url,
                        'backend': 'remote',
                        'enable_encryption': True,
                        'password': self.password,
                    }
                )
            finally:
                db.close()

        result = self.profile_function(
            parallel_download,
            f"parallel_download_workers_{max_workers}"
        )

        # 清理下载文件
        if output_file.exists():
            output_file.unlink()

        return result

    def profile_async_download(self, max_concurrency: int = 16):
        """分析异步下载"""
        output_file = self.test_dir / "download_async.bin"

        def async_download():
            return asyncio.run(
                download_large_file_async(
                    self.db_name,
                    self.server_url,
                    "profile_async_upload",
                    str(output_file),
                    max_concurrency=max_concurrency,
                    show_progress=True,
                    verify=True,
                    password=self.password,
                    enable_encryption=True
                )
            )

        result = self.profile_function(
            async_download,
            f"async_download_concurrency_{max_concurrency}"
        )

        # 清理下载文件
        if output_file.exists():
            output_file.unlink()

        return result

    def generate_summary_report(self, results: Dict[str, Dict[str, Any]]):
        """生成汇总报告"""
        report_file = self.output_dir / "summary_report.md"

        with open(report_file, 'w') as f:
            f.write("# 大文件传输性能分析报告\n\n")
            f.write(f"- 测试文件大小: {self.test_file_size_mb} MB\n")
            f.write(f"- 服务器地址: {self.server_url}\n")
            f.write(f"- 数据库名称: {self.db_name}\n")
            f.write(f"- 加密: 是\n\n")

            f.write("## 性能对比汇总\n\n")
            f.write("| 传输方式 | 总耗时(秒) | 吞吐量(MB/s) | 相对性能 |\n")
            f.write("|---------|-----------|-------------|----------|\n")

            # 找到最快的方式作为基准
            min_time = min(r['time'] for r in results.values())

            for name, data in results.items():
                total_time = data['time']
                throughput = self.test_file_size_mb / total_time
                relative = min_time / total_time * 100

                f.write(f"| {name} | {total_time:.2f} | {throughput:.1f} | {relative:.1f}% |\n")

            f.write("\n## 性能瓶颈分析\n\n")
            f.write("### 关键发现\n\n")

            # 分析每个测试的主要耗时函数
            for name, data in results.items():
                f.write(f"#### {name}\n\n")
                f.write(f"- 总耗时: {data['time']:.2f}秒\n")
                f.write(f"- 吞吐量: {self.test_file_size_mb / data['time']:.1f} MB/s\n")
                f.write(f"- 详细报告: [{name}.txt](./{name}.txt)\n\n")

            f.write("## 优化建议\n\n")
            f.write("根据性能分析结果,主要瓶颈可能在:\n\n")
            f.write("1. **网络I/O**: ZeroMQ 通信开销\n")
            f.write("2. **序列化/反序列化**: msgpack 编码解码\n")
            f.write("3. **磁盘I/O**: 文件读写操作\n")
            f.write("4. **加密开销**: 如果启用加密,Fernet 加密/解密\n")
            f.write("5. **哈希计算**: SHA256 文件校验\n")
            f.write("6. **线程/协程调度**: 并发控制开销\n\n")

            f.write("### 推荐配置\n\n")
            best_method = min(results.items(), key=lambda x: x[1]['time'])
            f.write(f"**最佳传输方式**: {best_method[0]}\n")
            f.write(f"**性能**: {self.test_file_size_mb / best_method[1]['time']:.1f} MB/s\n\n")

        print(f"\n✓ 汇总报告已保存到: {report_file}")


def main():
    """主函数"""
    print("="*60)
    print("FlaxKV2 大文件传输性能分析工具")
    print("="*60)

    # 检查参数
    if len(sys.argv) > 1:
        file_size_mb = int(sys.argv[1])
    else:
        file_size_mb = 200

    if len(sys.argv) > 2:
        server_url = sys.argv[2]
    else:
        server_url = "tcp://127.0.0.1:25555"

    print(f"\n配置:")
    print(f"  测试文件大小: {file_size_mb} MB")
    print(f"  服务器地址: {server_url}")
    print(f"  注意: 请确保服务器已启动 (端口 25555, 密码: yao)")

    # 创建分析器
    profiler = PerformanceProfiler(file_size_mb, server_url)

    # 创建测试文件
    profiler.create_test_file()

    # 存储结果
    results = {}

    try:
        # 1. 同步上传
        print("\n" + "="*60)
        print("测试 1/6: 同步上传")
        print("="*60)
        _, _, time1 = profiler.profile_sync_upload()
        results['同步上传'] = {'time': time1}

        # 2. 并行上传 (16线程)
        print("\n" + "="*60)
        print("测试 2/6: 并行上传 (16线程)")
        print("="*60)
        _, _, time2 = profiler.profile_parallel_upload(max_workers=16)
        results['并行上传(16线程)'] = {'time': time2}

        # 3. 异步上传 (16并发)
        print("\n" + "="*60)
        print("测试 3/6: 异步上传 (16并发)")
        print("="*60)
        _, _, time3 = profiler.profile_async_upload(max_concurrency=16)
        results['异步上传(16并发)'] = {'time': time3}

        # 4. 同步下载
        print("\n" + "="*60)
        print("测试 4/6: 同步下载")
        print("="*60)
        _, _, time4 = profiler.profile_sync_download()
        results['同步下载'] = {'time': time4}

        # 5. 并行下载 (16线程)
        print("\n" + "="*60)
        print("测试 5/6: 并行下载 (16线程)")
        print("="*60)
        _, _, time5 = profiler.profile_parallel_download(max_workers=16)
        results['并行下载(16线程)'] = {'time': time5}

        # 6. 异步下载 (16并发)
        print("\n" + "="*60)
        print("测试 6/6: 异步下载 (16并发)")
        print("="*60)
        _, _, time6 = profiler.profile_async_download(max_concurrency=16)
        results['异步下载(16并发)'] = {'time': time6}

        # 生成汇总报告
        print("\n" + "="*60)
        print("生成汇总报告")
        print("="*60)
        profiler.generate_summary_report(results)

        # 打印汇总
        print("\n" + "="*60)
        print("性能测试汇总")
        print("="*60)
        print(f"\n{'传输方式':<20} {'耗时(秒)':<12} {'吞吐量(MB/s)':<15}")
        print("-" * 60)
        for name, data in results.items():
            throughput = file_size_mb / data['time']
            print(f"{name:<20} {data['time']:<12.2f} {throughput:<15.1f}")

        print("\n✅ 所有性能分析完成!")
        print(f"✅ 详细报告保存在: {profiler.output_dir}/")

    except Exception as e:
        print(f"\n❌ 性能分析失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 清理测试文件
        print("\n" + "="*60)
        print("清理测试数据")
        print("="*60)
        # profiler.cleanup_test_file()  # 保留测试文件供后续使用


if __name__ == "__main__":
    main()
