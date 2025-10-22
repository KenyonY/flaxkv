"""
FlaxKV2 重构功能测试
专注于测试本次重构中修复的关键问题
"""

import os
import shutil
import tempfile
import time
from pathlib import Path

import pytest

from flaxkv2 import FlaxKV


class TestBufferingMechanism:
    """测试缓冲机制"""

    @pytest.fixture
    def db_path(self):
        """创建临时数据库路径"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    def test_buffering_enabled(self, db_path):
        """测试缓冲机制是否正常工作"""
        # 创建小缓冲区以便测试
        db = FlaxKV("test_db", db_path, max_buffer_size=5)

        # 添加数据到缓冲区
        for i in range(3):
            db[f"key{i}"] = f"value{i}"

        # 数据应该在缓冲区中，还未写入数据库
        # 关闭数据库但不写入
        db.close(write=False)

        # 重新打开，数据应该丢失（因为没有写入）
        db2 = FlaxKV("test_db", db_path)
        assert "key0" not in db2
        assert "key1" not in db2
        db2.close()

    def test_buffer_overflow_triggers_write(self, db_path):
        """测试缓冲区溢出时触发写入"""
        # 创建小缓冲区
        db = FlaxKV("test_db", db_path, max_buffer_size=5)

        # 添加超过缓冲区大小的数据
        for i in range(10):
            db[f"key{i}"] = f"value{i}"

        # 等待一下确保写入完成
        time.sleep(0.1)

        # 关闭数据库
        db.close(write=True, wait=True)

        # 重新打开，数据应该存在
        db2 = FlaxKV("test_db", db_path)
        for i in range(10):
            assert db2[f"key{i}"] == f"value{i}"
        db2.close()

    def test_manual_flush(self, db_path):
        """测试手动刷新缓冲区"""
        db = FlaxKV("test_db", db_path, max_buffer_size=100)

        # 添加数据
        for i in range(5):
            db[f"key{i}"] = f"value{i}"

        # 手动刷新
        db.write_immediately(write=True, block=True)

        # 关闭数据库但不写入
        db.close(write=False)

        # 重新打开，数据应该存在（因为已手动刷新）
        db2 = FlaxKV("test_db", db_path)
        for i in range(5):
            assert db2[f"key{i}"] == f"value{i}"
        db2.close()

    def test_update_uses_buffering(self, db_path):
        """测试批量更新使用缓冲机制"""
        db = FlaxKV("test_db", db_path, max_buffer_size=100)

        # 批量更新
        items = {f"key{i}": f"value{i}" for i in range(10)}
        db.update(items)

        # 确保数据在缓冲区
        db.close(write=True, wait=True)

        # 重新打开验证
        db2 = FlaxKV("test_db", db_path)
        for i in range(10):
            assert db2[f"key{i}"] == f"value{i}"
        db2.close()


class TestDefaultTTL:
    """测试默认TTL功能"""

    @pytest.fixture
    def db_path(self):
        """创建临时数据库路径"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    def test_default_ttl_on_set(self, db_path):
        """测试设置默认TTL后，新键自动应用TTL"""
        # 创建数据库，设置默认TTL为1秒
        db = FlaxKV("test_db", db_path, default_ttl=1)

        # 设置键值
        db["key1"] = "value1"

        # 刷新缓冲区
        db.write_immediately(write=True, block=True)

        # 检查TTL是否设置
        ttl = db.get_ttl("key1")
        assert ttl is not None
        assert ttl <= 1.0 and ttl > 0

        # 等待过期
        time.sleep(1.2)

        # 键应该已过期
        assert "key1" not in db

        db.close()

    def test_default_ttl_on_update(self, db_path):
        """测试批量更新时应用默认TTL"""
        db = FlaxKV("test_db", db_path, default_ttl=1)

        # 批量更新
        items = {"key1": "value1", "key2": "value2"}
        db.update(items)

        # 刷新缓冲区
        db.write_immediately(write=True, block=True)

        # 检查TTL
        assert db.get_ttl("key1") is not None
        assert db.get_ttl("key2") is not None

        # 等待过期
        time.sleep(1.2)

        # 键应该已过期
        assert "key1" not in db
        assert "key2" not in db

        db.close()

    def test_change_default_ttl(self, db_path):
        """测试动态修改默认TTL"""
        db = FlaxKV("test_db", db_path)

        # 初始没有默认TTL
        assert db.get_default_ttl() is None

        # 设置键，不应该有TTL
        db["key1"] = "value1"
        db.write_immediately(write=True, block=True)
        assert db.get_ttl("key1") is None

        # 设置默认TTL
        db.set_default_ttl(2)
        assert db.get_default_ttl() == 2

        # 新键应该有TTL
        db["key2"] = "value2"
        db.write_immediately(write=True, block=True)
        ttl = db.get_ttl("key2")
        assert ttl is not None
        assert ttl <= 2.0

        # 旧键不应该受影响
        assert db.get_ttl("key1") is None

        db.close()

    def test_no_default_ttl(self, db_path):
        """测试没有设置默认TTL时的行为"""
        db = FlaxKV("test_db", db_path)

        # 设置键
        db["key1"] = "value1"
        db.write_immediately(write=True, block=True)

        # 不应该有TTL
        assert db.get_ttl("key1") is None

        # 键应该永久存在
        time.sleep(2)
        assert "key1" in db

        db.close()


class TestContextManager:
    """测试上下文管理器"""

    @pytest.fixture
    def db_path(self):
        """创建临时数据库路径"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    def test_context_manager_basic(self, db_path):
        """测试基本的上下文管理器使用"""
        # 使用上下文管理器
        with FlaxKV("test_db", db_path) as db:
            db["key1"] = "value1"
            db["key2"] = "value2"

        # 数据应该已保存
        with FlaxKV("test_db", db_path) as db:
            assert db["key1"] == "value1"
            assert db["key2"] == "value2"

    def test_context_manager_exception(self, db_path):
        """测试上下文管理器在异常时的行为"""
        try:
            with FlaxKV("test_db", db_path) as db:
                db["key1"] = "value1"
                raise ValueError("Test exception")
        except ValueError:
            pass

        # 数据应该已保存（即使发生异常）
        with FlaxKV("test_db", db_path) as db:
            assert db["key1"] == "value1"


class TestDataPersistence:
    """测试数据持久化"""

    @pytest.fixture
    def db_path(self):
        """创建临时数据库路径"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    def test_close_with_write(self, db_path):
        """测试关闭时写入数据"""
        db = FlaxKV("test_db", db_path)
        db["key1"] = "value1"
        db.close(write=True, wait=True)

        # 重新打开验证
        db2 = FlaxKV("test_db", db_path)
        assert db2["key1"] == "value1"
        db2.close()

    def test_close_without_write(self, db_path):
        """测试关闭时不写入数据"""
        db = FlaxKV("test_db", db_path, max_buffer_size=100)
        db["key1"] = "value1"
        db.close(write=False)

        # 重新打开，数据应该不存在
        db2 = FlaxKV("test_db", db_path)
        assert "key1" not in db2
        db2.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
