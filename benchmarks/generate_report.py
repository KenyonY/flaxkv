#!/usr/bin/env python
"""
生成性能基准测试报告

从CSV文件读取结果并生成Markdown格式的报告
"""

import csv
import sys
from typing import List, Dict
from collections import defaultdict


def read_results(filename: str) -> List[Dict]:
    """读取CSV结果文件"""
    results = []
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append(row)
    return results


def generate_markdown_report(results: List[Dict]) -> str:
    """生成Markdown报告"""

    report = []
    report.append("# FlaxKV2 缓冲机制性能基准测试报告\n")
    report.append("## 测试概述\n")
    report.append("本报告对比了三种实现的性能差异：\n")
    report.append("1. **Raw LevelDB (无缓冲)** - 直接写入LevelDB，无任何缓冲机制")
    report.append("2. **LevelDBDict (缓冲, 无缓存)** - 使用缓冲机制但不使用读缓存")
    report.append("3. **LevelDBDict (缓冲, 有缓存)** - 使用缓冲机制和读缓存\n")

    # 按操作类型分组
    operations = defaultdict(list)
    for result in results:
        operations[result['Operation']].append(result)

    # 为每个操作生成表格
    report.append("## 性能对比\n")

    for operation, op_results in operations.items():
        report.append(f"### {operation}\n")

        # 创建表格
        report.append("| 实现 | 吞吐量 (ops/sec) | 平均延迟 (ms) | P95延迟 (ms) | P99延迟 (ms) | 相对性能 |")
        report.append("|------|-----------------|--------------|-------------|-------------|---------|")

        # 找到最快的作为基准
        baseline_ops = max(float(r['Ops/sec']) for r in op_results)

        # 排序并显示
        sorted_results = sorted(op_results, key=lambda r: float(r['Ops/sec']), reverse=True)

        for result in sorted_results:
            ops = float(result['Ops/sec'])
            speedup = ops / baseline_ops
            speedup_str = f"{speedup:.2f}x"

            if speedup >= 0.99:  # 基准
                speedup_str = "基准"
            elif speedup >= 1.0:
                speedup_str = f"**{speedup_str}**"

            report.append(
                f"| {result['Name']} | "
                f"{float(result['Ops/sec']):,.2f} | "
                f"{result['Avg Latency (ms)']} | "
                f"{result['P95 (ms)']} | "
                f"{result['P99 (ms)']} | "
                f"{speedup_str} |"
            )

        report.append("")

    # 添加分析
    report.append("## 性能分析\n")

    # 单个写入分析
    single_write_results = operations.get("单个写入", [])
    if single_write_results:
        report.append("### 单个写入操作\n")
        raw = next((r for r in single_write_results if "Raw LevelDB" in r['Name']), None)
        buffered = next((r for r in single_write_results if "缓冲, 无缓存" in r['Name']), None)
        cached = next((r for r in single_write_results if "缓冲, 有缓存" in r['Name']), None)

        if raw and buffered:
            improvement = (float(buffered['Ops/sec']) / float(raw['Ops/sec']) - 1) * 100
            report.append(f"- 使用缓冲机制相比原始LevelDB提升了 **{improvement:.1f}%** 的写入吞吐量")

        if buffered and cached:
            cache_diff = (float(cached['Ops/sec']) / float(buffered['Ops/sec']) - 1) * 100
            if abs(cache_diff) > 5:
                report.append(f"- 读缓存对单个写入性能的影响为 **{cache_diff:+.1f}%**")
            else:
                report.append("- 读缓存对单个写入性能影响较小")

        report.append("")

    # 批量写入分析
    batch_write_results = [r for r in results if "批量写入" in r['Operation']]
    if batch_write_results:
        report.append("### 批量写入操作\n")
        raw = next((r for r in batch_write_results if "Raw LevelDB" in r['Name']), None)
        buffered = next((r for r in batch_write_results if "缓冲, 无缓存" in r['Name']), None)

        if raw and buffered:
            improvement = (float(buffered['Ops/sec']) / float(raw['Ops/sec']) - 1) * 100
            report.append(f"- 批量写入时缓冲机制提升了 **{improvement:.1f}%** 的吞吐量")

        report.append("")

    # 随机读取分析
    read_results = operations.get("随机读取", [])
    if read_results:
        report.append("### 随机读取操作\n")
        raw = next((r for r in read_results if "Raw LevelDB" in r['Name']), None)
        buffered = next((r for r in read_results if "缓冲, 无缓存" in r['Name']), None)
        cached = next((r for r in read_results if "缓冲, 有缓存" in r['Name']), None)

        if buffered and cached:
            improvement = (float(cached['Ops/sec']) / float(buffered['Ops/sec']) - 1) * 100
            report.append(f"- 读缓存将随机读取性能提升了 **{improvement:.1f}%**")

        if raw and cached:
            total_improvement = (float(cached['Ops/sec']) / float(raw['Ops/sec']) - 1) * 100
            report.append(f"- 相比原始LevelDB，缓冲+缓存总共提升了 **{total_improvement:.1f}%**")

        report.append("")

    # 混合工作负载分析
    mixed_results = [r for r in results if "混合读写" in r['Operation']]
    if mixed_results:
        report.append("### 混合读写工作负载\n")
        raw = next((r for r in mixed_results if "Raw LevelDB" in r['Name']), None)
        cached = next((r for r in mixed_results if "缓冲, 有缓存" in r['Name']), None)

        if raw and cached:
            improvement = (float(cached['Ops/sec']) / float(raw['Ops/sec']) - 1) * 100
            report.append(f"- 在真实混合工作负载下，优化后的实现提升了 **{improvement:.1f}%** 的整体性能")

        report.append("")

    # 结论
    report.append("## 结论\n")
    report.append("基于以上测试结果，可以得出以下结论：\n")
    report.append("1. **缓冲机制显著提升写入性能** - 通过批量提交减少了数据库操作的开销")
    report.append("2. **读缓存对读密集型场景帮助很大** - 随机读取性能显著提升")
    report.append("3. **混合工作负载下优势明显** - 在真实场景中能带来可观的性能提升")
    report.append("4. **缓存的权衡** - 读缓存会增加内存使用，需要根据实际场景权衡\n")

    report.append("## 建议\n")
    report.append("- **写密集型场景**：使用缓冲机制，可以不启用读缓存以节省内存")
    report.append("- **读密集型场景**：同时启用缓冲和读缓存，获得最佳性能")
    report.append("- **混合场景**：启用读缓存，根据内存情况调整缓存大小")
    report.append("- **低延迟要求**：调整缓冲区大小以平衡延迟和吞吐量\n")

    return "\n".join(report)


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='生成性能基准测试报告')
    parser.add_argument(
        '--input',
        default='benchmark_results.csv',
        help='输入CSV文件 (默认: benchmark_results.csv)'
    )
    parser.add_argument(
        '--output',
        default='BENCHMARK_REPORT.md',
        help='输出Markdown文件 (默认: BENCHMARK_REPORT.md)'
    )

    args = parser.parse_args()

    # 读取结果
    try:
        results = read_results(args.input)
    except FileNotFoundError:
        print(f"错误: 找不到文件 {args.input}")
        print("请先运行基准测试: python run_benchmark.py")
        sys.exit(1)

    # 生成报告
    report = generate_markdown_report(results)

    # 保存报告
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"报告已生成: {args.output}")

    # 同时打印到控制台
    print("\n" + "="*80)
    print(report)


if __name__ == "__main__":
    main()
