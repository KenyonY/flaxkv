"""
真实性能测试：对比传统方式 vs NestedDBDict

使用更大的数据集和更真实的场景
"""
import time
import tempfile
import shutil
import numpy as np
from flaxkv2 import LevelDBDict


def benchmark_traditional_large_dict():
    """传统方式：大字典频繁修改"""
    print("\n" + "=" * 60)
    print("测试 1: 传统方式 - 大字典（1000字段，修改100次）")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp()
    try:
        db = LevelDBDict('test_trad', path=tmpdir, rebuild=True)

        # 创建一个包含1000个字段的大字典
        print("初始化：创建包含1000个字段的字典...")
        large_dict = {f'field_{i}': np.random.randn(10) for i in range(1000)}
        db['user_data'] = large_dict
        db.write_immediately(block=True)

        # 测试：修改100次
        print("测试：修改单个字段100次...")
        start_time = time.time()

        for i in range(100):
            data = db['user_data']  # 反序列化1000个字段
            data['field_0'] = np.random.randn(10)  # 修改1个字段
            db['user_data'] = data  # 序列化1000个字段
            if i % 20 == 19:
                db.write_immediately(block=True)

        db.write_immediately(block=True)
        elapsed = time.time() - start_time

        print(f"\n结果:")
        print(f"  总耗时: {elapsed:.3f}秒")
        print(f"  平均每次: {elapsed/100*1000:.2f}毫秒")
        print(f"  操作: 每次反序列化+序列化整个1000字段字典")

        db.close()
        return elapsed

    finally:
        shutil.rmtree(tmpdir)


def benchmark_nested_large_dict():
    """优化方式：NestedDBDict"""
    print("\n" + "=" * 60)
    print("测试 2: NestedDBDict - 独立字段（1000字段，修改100次）")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp()
    try:
        db = LevelDBDict('test_nested', path=tmpdir, rebuild=True)

        # 使用 nested() 创建嵌套字典
        print("初始化：创建1000个独立字段...")
        nested = db.nested('user_data')
        for i in range(1000):
            nested[f'field_{i}'] = np.random.randn(10)

        # 没有 write_immediately，因为 nested 直接写入

        # 测试：修改100次
        print("测试：修改单个字段100次...")
        start_time = time.time()

        for i in range(100):
            nested['field_0'] = np.random.randn(10)  # 只序列化1个字段

        elapsed = time.time() - start_time

        print(f"\n结果:")
        print(f"  总耗时: {elapsed:.3f}秒")
        print(f"  平均每次: {elapsed/100*1000:.2f}毫秒")
        print(f"  操作: 每次只序列化1个字段")

        db.close()
        return elapsed

    finally:
        shutil.rmtree(tmpdir)


def benchmark_mixed_operations():
    """混合操作：读写混合"""
    print("\n" + "=" * 60)
    print("测试 3: 混合操作 - 读写混合（传统 vs NestedDBDict）")
    print("=" * 60)

    # 传统方式
    tmpdir1 = tempfile.mkdtemp()
    try:
        db1 = LevelDBDict('test_mixed_trad', path=tmpdir1, rebuild=True)
        large_dict = {f'field_{i}': i * 100 for i in range(500)}
        db1['data'] = large_dict
        db1.write_immediately(block=True)

        print("\n传统方式 - 100次随机读写...")
        start_time = time.time()
        for i in range(100):
            # 读
            data = db1['data']
            val = data[f'field_{i % 500}']
            # 写
            data[f'field_{i % 500}'] = val + 1
            db1['data'] = data
            if i % 20 == 19:
                db1.write_immediately(block=True)

        db1.write_immediately(block=True)
        time_trad = time.time() - start_time
        print(f"  耗时: {time_trad:.3f}秒")

        db1.close()
    finally:
        shutil.rmtree(tmpdir1)

    # NestedDBDict 方式
    tmpdir2 = tempfile.mkdtemp()
    try:
        db2 = LevelDBDict('test_mixed_nested', path=tmpdir2, rebuild=True)
        nested = db2.nested('data')
        for i in range(500):
            nested[f'field_{i}'] = i * 100

        print("\nNestedDBDict 方式 - 100次随机读写...")
        start_time = time.time()
        for i in range(100):
            # 读
            val = nested[f'field_{i % 500}']
            # 写
            nested[f'field_{i % 500}'] = val + 1

        time_nested = time.time() - start_time
        print(f"  耗时: {time_nested:.3f}秒")

        db2.close()
    finally:
        shutil.rmtree(tmpdir2)

    print(f"\n对比:")
    print(f"  性能提升: {time_trad/time_nested:.1f}x")
    print(f"  速度提升: {(time_trad - time_nested)/time_trad*100:.1f}%")


def main():
    print("\n" + "=" * 60)
    print(" FlaxKV2 嵌套数据真实性能测试")
    print("=" * 60)

    time_trad = benchmark_traditional_large_dict()
    time_nested = benchmark_nested_large_dict()

    print("\n" + "=" * 60)
    print(" 第一轮测试总结")
    print("=" * 60)
    print(f"\n传统方式:    {time_trad:.3f}秒")
    print(f"NestedDBDict: {time_nested:.3f}秒")
    if time_trad > 0:
        print(f"\n性能提升: {time_trad/time_nested:.1f}x")
        print(f"速度提升: {(time_trad - time_nested)/time_trad*100:.1f}%")

    benchmark_mixed_operations()

    print("\n" + "=" * 60)
    print(" 总结")
    print("=" * 60)
    print("\n✓ 对于包含大量字段的嵌套数据:")
    print("  - 传统方式每次修改都需要序列化/反序列化整个字典")
    print("  - NestedDBDict 只序列化/反序列化修改的字段")
    print("  - 字段越多，NestedDBDict 的优势越明显")
    print("\n✓ 推荐使用场景:")
    print("  - 用户配置、会话数据、缓存对象等有大量字段的结构")
    print("  - 需要频繁修改部分字段的场景")
    print("  - 字段数量 > 100 且修改频率较高时")


if __name__ == '__main__':
    main()
