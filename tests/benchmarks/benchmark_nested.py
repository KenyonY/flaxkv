"""
性能测试：对比传统方式 vs NestedDBDict

测试场景：频繁修改嵌套数据的单个字段
"""
import time
import tempfile
import shutil
from flaxkv2 import RawLevelDBDict


def benchmark_traditional_approach():
    """传统方式：整个字典序列化/反序列化"""
    print("\n" + "=" * 60)
    print("测试方式 1: 传统方式（整个字典序列化）")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test_traditional', tmpdir, rebuild=True)

        # 初始化一个大字典
        print("初始化大字典 (100个字段)...")
        large_dict = {f'field_{i}': i for i in range(100)}
        db['data'] = large_dict

        # 确保写入磁盘
        db.write_immediately(block=True)

        # 测试：修改1000次单个字段
        print("开始测试：修改1000次单个字段...")
        start_time = time.time()

        for i in range(1000):
            # 每次都要反序列化整个字典
            data = db['data']
            # 修改一个字段
            data['field_0'] = i
            # 重新序列化整个字典
            db['data'] = data

        # 确保写入磁盘
        db.write_immediately(block=True)
        elapsed = time.time() - start_time

        print(f"\n✓ 完成！")
        print(f"  总耗时: {elapsed:.3f}秒")
        print(f"  平均每次修改: {elapsed/1000*1000:.3f}毫秒")
        print(f"  每次操作: 反序列化100个字段 + 修改1个字段 + 序列化100个字段")

        db.close()
        return elapsed

    finally:
        shutil.rmtree(tmpdir)


def benchmark_nested_approach():
    """优化方式：使用 NestedDBDict"""
    print("\n" + "=" * 60)
    print("测试方式 2: NestedDBDict（单字段序列化）")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test_nested', tmpdir, rebuild=True)

        # 使用 nested() 创建嵌套字典
        print("初始化嵌套字典 (100个字段)...")
        nested = db.nested('data')
        for i in range(100):
            nested[f'field_{i}'] = i

        # 确保写入磁盘
        db.write_immediately(block=True)

        # 测试：修改1000次单个字段
        print("开始测试：修改1000次单个字段...")
        start_time = time.time()

        for i in range(1000):
            # 只序列化/反序列化单个字段！
            nested['field_0'] = i

        # 确保写入磁盘
        db.write_immediately(block=True)
        elapsed = time.time() - start_time

        print(f"\n✓ 完成！")
        print(f"  总耗时: {elapsed:.3f}秒")
        print(f"  平均每次修改: {elapsed/1000*1000:.3f}毫秒")
        print(f"  每次操作: 只序列化1个字段的值")

        db.close()
        return elapsed

    finally:
        shutil.rmtree(tmpdir)


def benchmark_nested_batch_read():
    """测试：批量读取的性能"""
    print("\n" + "=" * 60)
    print("额外测试: 批量读取性能")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp()
    try:
        db = RawLevelDBDict('test_read', tmpdir, rebuild=True)

        # 初始化100个字段
        nested = db.nested('data')
        for i in range(100):
            nested[f'field_{i}'] = i * 100

        db.write_immediately()

        # 测试1: 读取单个字段
        print("\n测试 1: 读取单个字段...")
        start_time = time.time()
        for _ in range(1000):
            value = nested['field_50']
        elapsed_single = time.time() - start_time
        print(f"  1000次读取单字段: {elapsed_single:.3f}秒")

        # 测试2: 迭代所有字段
        print("\n测试 2: 迭代所有字段...")
        start_time = time.time()
        for _ in range(10):
            for key, value in nested.items():
                pass
        elapsed_iter = time.time() - start_time
        print(f"  10次迭代100个字段: {elapsed_iter:.3f}秒")

        # 测试3: 转换为字典
        print("\n测试 3: 转换为普通字典...")
        start_time = time.time()
        for _ in range(10):
            d = nested.to_dict()
        elapsed_to_dict = time.time() - start_time
        print(f"  10次转换为字典: {elapsed_to_dict:.3f}秒")
        print(f"  注意: to_dict() 会反序列化所有字段，适合一次性读取")

        db.close()

    finally:
        shutil.rmtree(tmpdir)


def main():
    print("\n" + "=" * 60)
    print(" FlaxKV2 嵌套数据性能测试")
    print("=" * 60)
    print("\n场景: 有一个包含100个字段的大字典")
    print("任务: 频繁修改其中一个字段（1000次）")

    # 运行测试
    time_traditional = benchmark_traditional_approach()
    time_nested = benchmark_nested_approach()

    # 额外测试
    benchmark_nested_batch_read()

    # 总结
    print("\n" + "=" * 60)
    print(" 性能对比总结")
    print("=" * 60)
    print(f"\n传统方式总耗时:    {time_traditional:.3f}秒")
    print(f"NestedDBDict总耗时: {time_nested:.3f}秒")
    print(f"\n性能提升: {time_traditional/time_nested:.1f}x")
    print(f"速度提升: {(time_traditional - time_nested)/time_traditional*100:.1f}%")

    print("\n结论:")
    print("  ✓ 对于频繁修改嵌套数据的场景，NestedDBDict 显著更快")
    print("  ✓ 字段越多、修改越频繁，优势越明显")
    print("  ✓ 推荐在需要频繁修改部分字段时使用 db.nested() 方法")


if __name__ == '__main__':
    main()
