"""
序列化性能基准测试

比较以下策略：
1. 当前混合策略（msgpack + pickle + numpy专用）
2. 纯pickle策略
3. 针对不同数据类型的性能差异
"""

import pickle
import msgpack
import numpy as np
import time
from typing import Any, List, Tuple
import sys

# 尝试导入pandas
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    pd = None

# 导入flaxkv2的序列化模块
from flaxkv2.serialization import encoder, decoder


# =================================================================
# 纯pickle编解码实现
# =================================================================

def encode_pure_pickle(value: Any) -> bytes:
    """纯pickle编码"""
    return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)


def decode_pure_pickle(data: bytes) -> Any:
    """纯pickle解码"""
    return pickle.loads(data)


# =================================================================
# 测试数据生成
# =================================================================

def generate_test_data():
    """生成各种类型的测试数据"""
    test_cases = {}

    # 1. 基础Python类型
    test_cases['int'] = 42
    test_cases['float'] = 3.14159
    test_cases['str_short'] = "hello world"
    test_cases['str_long'] = "a" * 10000
    test_cases['list_small'] = [1, 2, 3, 4, 5]
    test_cases['list_large'] = list(range(10000))
    test_cases['dict_small'] = {'a': 1, 'b': 2, 'c': 3}
    test_cases['dict_large'] = {f'key_{i}': i for i in range(1000)}
    test_cases['nested'] = {
        'level1': {
            'level2': {
                'level3': [1, 2, 3, 4, 5]
            }
        }
    }

    # 2. NumPy数组
    test_cases['numpy_small'] = np.array([1, 2, 3, 4, 5])
    test_cases['numpy_1d'] = np.random.randn(10000)
    test_cases['numpy_2d'] = np.random.randn(100, 100)
    test_cases['numpy_3d'] = np.random.randn(10, 10, 10)
    test_cases['numpy_int'] = np.arange(10000, dtype=np.int32)
    test_cases['numpy_float32'] = np.random.randn(10000).astype(np.float32)
    test_cases['numpy_float64'] = np.random.randn(10000).astype(np.float64)

    # 3. Pandas DataFrame（如果可用）
    if HAS_PANDAS:
        test_cases['pandas_small'] = pd.DataFrame({
            'a': [1, 2, 3],
            'b': [4, 5, 6]
        })
        test_cases['pandas_numeric'] = pd.DataFrame({
            'col1': np.random.randn(1000),
            'col2': np.random.randn(1000),
            'col3': np.random.randn(1000)
        })
        test_cases['pandas_mixed'] = pd.DataFrame({
            'int_col': np.arange(1000),
            'float_col': np.random.randn(1000),
            'str_col': [f'str_{i}' for i in range(1000)]
        })

    return test_cases


# =================================================================
# 性能测试函数
# =================================================================

def benchmark_encode(name: str, value: Any, encode_func, iterations: int = 1000) -> Tuple[float, int]:
    """
    测试编码性能

    Returns:
        (avg_time_ms, encoded_size)
    """
    # 预热
    for _ in range(10):
        encoded = encode_func(value)

    # 实际测试
    start = time.perf_counter()
    for _ in range(iterations):
        encoded = encode_func(value)
    end = time.perf_counter()

    avg_time_ms = (end - start) * 1000 / iterations
    encoded_size = len(encoded)

    return avg_time_ms, encoded_size


def benchmark_decode(name: str, encoded_data: bytes, decode_func, iterations: int = 1000) -> float:
    """
    测试解码性能

    Returns:
        avg_time_ms
    """
    # 预热
    for _ in range(10):
        decode_func(encoded_data)

    # 实际测试
    start = time.perf_counter()
    for _ in range(iterations):
        decode_func(encoded_data)
    end = time.perf_counter()

    avg_time_ms = (end - start) * 1000 / iterations

    return avg_time_ms


def run_comparison(test_cases: dict, iterations: int = 1000):
    """运行对比测试"""

    print("=" * 100)
    print("序列化性能对比测试")
    print("=" * 100)
    print(f"迭代次数: {iterations}")
    print("=" * 100)
    print()

    results = []

    for name, value in test_cases.items():
        print(f"测试案例: {name}")
        print("-" * 100)

        try:
            # 测试当前混合策略
            enc_time_current, size_current = benchmark_encode(
                name, value, encoder.encode, iterations
            )
            encoded_current = encoder.encode(value)
            dec_time_current = benchmark_decode(
                name, encoded_current, decoder.decode, iterations
            )

            # 测试纯pickle策略
            enc_time_pickle, size_pickle = benchmark_encode(
                name, value, encode_pure_pickle, iterations
            )
            encoded_pickle = encode_pure_pickle(value)
            dec_time_pickle = benchmark_decode(
                name, encoded_pickle, decode_pure_pickle, iterations
            )

            # 计算速度提升
            enc_speedup = enc_time_pickle / enc_time_current
            dec_speedup = dec_time_pickle / dec_time_current
            size_ratio = size_pickle / size_current

            # 打印结果
            print(f"  当前策略 (混合): 编码 {enc_time_current:.4f}ms | 解码 {dec_time_current:.4f}ms | 大小 {size_current:,} bytes")
            print(f"  纯Pickle策略:   编码 {enc_time_pickle:.4f}ms | 解码 {dec_time_pickle:.4f}ms | 大小 {size_pickle:,} bytes")
            print(f"  速度比 (pickle/current): 编码 {enc_speedup:.2f}x | 解码 {dec_speedup:.2f}x | 大小 {size_ratio:.2f}x")

            if enc_speedup > 1:
                print(f"  ⚠️  当前策略编码更快 {enc_speedup:.2f}x")
            else:
                print(f"  ⚠️  纯Pickle编码更快 {1/enc_speedup:.2f}x")

            if dec_speedup > 1:
                print(f"  ⚠️  当前策略解码更快 {dec_speedup:.2f}x")
            else:
                print(f"  ⚠️  纯Pickle解码更快 {1/dec_speedup:.2f}x")

            print()

            results.append({
                'name': name,
                'enc_current': enc_time_current,
                'dec_current': dec_time_current,
                'size_current': size_current,
                'enc_pickle': enc_time_pickle,
                'dec_pickle': dec_time_pickle,
                'size_pickle': size_pickle,
                'enc_speedup': enc_speedup,
                'dec_speedup': dec_speedup,
                'size_ratio': size_ratio,
            })

        except Exception as e:
            print(f"  ❌ 测试失败: {e}")
            print()

    # 打印总结
    print("=" * 100)
    print("总结")
    print("=" * 100)

    # 按类型分组统计
    categories = {
        'basic': ['int', 'float', 'str_short', 'str_long', 'list_small', 'list_large', 'dict_small', 'dict_large', 'nested'],
        'numpy': [k for k in test_cases.keys() if k.startswith('numpy_')],
        'pandas': [k for k in test_cases.keys() if k.startswith('pandas_')]
    }

    for cat_name, cat_keys in categories.items():
        cat_results = [r for r in results if r['name'] in cat_keys]
        if not cat_results:
            continue

        avg_enc_speedup = sum(r['enc_speedup'] for r in cat_results) / len(cat_results)
        avg_dec_speedup = sum(r['dec_speedup'] for r in cat_results) / len(cat_results)
        avg_size_ratio = sum(r['size_ratio'] for r in cat_results) / len(cat_results)

        print(f"\n{cat_name.upper()} 类型平均:")
        print(f"  编码速度比 (pickle/current): {avg_enc_speedup:.2f}x")
        print(f"  解码速度比 (pickle/current): {avg_dec_speedup:.2f}x")
        print(f"  大小比例 (pickle/current): {avg_size_ratio:.2f}x")

        if avg_enc_speedup < 1 and avg_dec_speedup < 1:
            print(f"  ✅ 结论: 纯Pickle在{cat_name}类型上更快")
        elif avg_enc_speedup > 1 and avg_dec_speedup > 1:
            print(f"  ✅ 结论: 当前混合策略在{cat_name}类型上更快")
        else:
            print(f"  ⚠️  结论: 两种策略各有优劣")

    # 整体统计
    total_enc_speedup = sum(r['enc_speedup'] for r in results) / len(results)
    total_dec_speedup = sum(r['dec_speedup'] for r in results) / len(results)
    total_size_ratio = sum(r['size_ratio'] for r in results) / len(results)

    print(f"\n整体平均:")
    print(f"  编码速度比 (pickle/current): {total_enc_speedup:.2f}x")
    print(f"  解码速度比 (pickle/current): {total_dec_speedup:.2f}x")
    print(f"  大小比例 (pickle/current): {total_size_ratio:.2f}x")

    print("\n最终建议:")
    if total_enc_speedup > 1.2 and total_dec_speedup > 1.2:
        print("  ✅ 当前混合策略整体性能更优，建议保持")
    elif total_enc_speedup < 0.8 and total_dec_speedup < 0.8:
        print("  ⚠️  纯Pickle策略整体性能更优，建议考虑切换")
    else:
        print("  ⚠️  两种策略性能接近，可根据具体场景选择")
        print("      - 如果主要存储基础类型和numpy数组，当前策略更优")
        print("      - 如果存储类型复杂多样，纯pickle可能更简单")


# =================================================================
# 主程序
# =================================================================

if __name__ == '__main__':
    # 清除类型缓存以确保公平测试
    encoder.clear_type_cache()

    # 生成测试数据
    print("生成测试数据...")
    test_cases = generate_test_data()
    print(f"生成了 {len(test_cases)} 个测试案例")
    print()

    # 运行测试
    iterations = 1000
    if len(sys.argv) > 1:
        iterations = int(sys.argv[1])

    run_comparison(test_cases, iterations=iterations)
