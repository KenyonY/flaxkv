#!/usr/bin/env python
"""
快速测试脚本 - 验证重构修复
"""

import os
import shutil
import tempfile
import time

print("导入FlaxKV...")
from flaxkv2 import FlaxKV

def test_buffering():
    """测试缓冲机制"""
    print("\n=== 测试1: 缓冲机制 ===")
    temp_dir = tempfile.mkdtemp()

    try:
        # 创建小缓冲区
        db = FlaxKV("test_db", temp_dir, max_buffer_size=5)

        # 添加数据
        print("添加10个键值对...")
        for i in range(10):
            db[f"key{i}"] = f"value{i}"

        # 等待缓冲区刷新
        time.sleep(0.5)

        # 关闭并重新打开
        print("关闭并重新打开数据库...")
        db.close(write=True, wait=True)

        db2 = FlaxKV("test_db", temp_dir)

        # 验证数据
        print("验证数据...")
        for i in range(10):
            assert db2[f"key{i}"] == f"value{i}", f"key{i} 数据不匹配"

        db2.close()
        print("✓ 缓冲机制测试通过")

    finally:
        shutil.rmtree(temp_dir)


def test_default_ttl():
    """测试默认TTL"""
    print("\n=== 测试2: 默认TTL ===")
    temp_dir = tempfile.mkdtemp()

    try:
        # 创建数据库，设置默认TTL为1秒
        print("创建数据库，默认TTL=1秒...")
        db = FlaxKV("test_db", temp_dir, default_ttl=1)

        # 设置键值
        print("设置键值...")
        db["temp_key"] = "value"

        # 刷新缓冲区
        db.write_immediately(write=True, block=True)

        # 检查TTL
        ttl = db.get_ttl("temp_key")
        print(f"TTL: {ttl:.2f}秒")
        assert ttl is not None and ttl <= 1.0, "TTL设置失败"

        # 等待过期
        print("等待1.2秒...")
        time.sleep(1.2)

        # 键应该已过期
        assert "temp_key" not in db, "键未过期"

        db.close()
        print("✓ 默认TTL测试通过")

    finally:
        shutil.rmtree(temp_dir)


def test_context_manager():
    """测试上下文管理器"""
    print("\n=== 测试3: 上下文管理器 ===")
    temp_dir = tempfile.mkdtemp()

    try:
        # 使用上下文管理器
        print("使用上下文管理器写入数据...")
        with FlaxKV("test_db", temp_dir) as db:
            db["key1"] = "value1"
            db["key2"] = "value2"

        # 验证数据
        print("验证数据...")
        with FlaxKV("test_db", temp_dir) as db:
            assert db["key1"] == "value1"
            assert db["key2"] == "value2"

        print("✓ 上下文管理器测试通过")

    finally:
        shutil.rmtree(temp_dir)


if __name__ == "__main__":
    print("开始运行快速测试...")

    try:
        test_buffering()
        test_default_ttl()
        test_context_manager()

        print("\n" + "="*50)
        print("✓ 所有测试通过!")
        print("="*50)

    except Exception as e:
        print(f"\n✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
