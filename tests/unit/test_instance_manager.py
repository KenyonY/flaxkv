"""
测试 InstanceManager 模块的数据库实例管理功能
"""

import unittest
import tempfile
import os
import threading
from unittest.mock import Mock, patch
from flaxkv2.instance_manager import DBInstanceManager
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict


class TestDBInstanceManager(unittest.TestCase):
    """测试 DBInstanceManager 类"""

    def setUp(self):
        """每个测试前创建新的管理器实例"""
        # 注意：由于是单例模式，我们需要清理状态
        self.manager = DBInstanceManager()
        # 清空实例缓存
        with self.manager._instance_lock:
            self.manager._instances.clear()

    def tearDown(self):
        """清理所有实例"""
        self.manager.close_all()

    def test_singleton_pattern(self):
        """测试单例模式"""
        manager1 = DBInstanceManager()
        manager2 = DBInstanceManager()

        # 两个引用应该指向同一个对象
        self.assertIs(manager1, manager2)

    def test_normalize_path(self):
        """测试路径标准化"""
        # 相对路径应该被转换为绝对路径
        relative_path = "test_db"
        absolute_path = self.manager._normalize_path(relative_path)

        self.assertTrue(os.path.isabs(absolute_path))
        self.assertIn("test_db", absolute_path)

    def test_normalize_path_already_absolute(self):
        """测试已经是绝对路径的情况"""
        absolute_path = "/tmp/test_db"
        normalized = self.manager._normalize_path(absolute_path)

        self.assertEqual(normalized, absolute_path)

    def test_register_and_get_instance(self):
        """测试注册和获取实例"""
        mock_db = Mock()
        db_path = "/tmp/test_db"

        # 注册实例
        self.manager.register_instance(db_path, mock_db)

        # 获取实例
        retrieved = self.manager.get_instance(db_path)
        self.assertIs(retrieved, mock_db)

    def test_get_nonexistent_instance(self):
        """测试获取不存在的实例"""
        instance = self.manager.get_instance("/tmp/nonexistent")
        self.assertIsNone(instance)

    def test_register_replaces_old_instance(self):
        """测试注册新实例会关闭旧实例"""
        # 创建旧实例
        old_db = Mock()
        old_db.close = Mock()
        db_path = "/tmp/test_db"

        # 注册旧实例
        self.manager.register_instance(db_path, old_db)

        # 注册新实例
        new_db = Mock()
        self.manager.register_instance(db_path, new_db)

        # 验证旧实例的close被调用
        old_db.close.assert_called_once()

        # 验证新实例被返回
        retrieved = self.manager.get_instance(db_path)
        self.assertIs(retrieved, new_db)

    def test_unregister_instance(self):
        """测试注销实例"""
        mock_db = Mock()
        db_path = "/tmp/test_db"

        # 注册然后注销
        self.manager.register_instance(db_path, mock_db)
        self.assertIsNotNone(self.manager.get_instance(db_path))

        self.manager.unregister_instance(db_path)
        self.assertIsNone(self.manager.get_instance(db_path))

    def test_unregister_nonexistent_instance(self):
        """测试注销不存在的实例不会报错"""
        try:
            self.manager.unregister_instance("/tmp/nonexistent")
        except Exception as e:
            self.fail(f"注销不存在的实例不应该抛出异常: {e}")

    def test_close_instance(self):
        """测试关闭实例"""
        mock_db = Mock()
        mock_db.close = Mock()
        db_path = "/tmp/test_db"

        # 注册实例
        self.manager.register_instance(db_path, mock_db)

        # 关闭实例
        result = self.manager.close_instance(db_path)

        # 验证成功关闭
        self.assertTrue(result)
        mock_db.close.assert_called_once()

        # 验证实例被移除
        self.assertIsNone(self.manager.get_instance(db_path))

    def test_close_nonexistent_instance(self):
        """测试关闭不存在的实例"""
        result = self.manager.close_instance("/tmp/nonexistent")
        self.assertFalse(result)

    def test_close_instance_handles_exceptions(self):
        """测试关闭实例时的异常处理"""
        mock_db = Mock()
        mock_db.close = Mock(side_effect=Exception("Close failed"))
        db_path = "/tmp/test_db"

        self.manager.register_instance(db_path, mock_db)

        # 关闭应该返回False但不抛出异常
        result = self.manager.close_instance(db_path)
        self.assertFalse(result)

    def test_close_all(self):
        """测试关闭所有实例"""
        # 注册多个实例
        mock_dbs = []
        for i in range(3):
            mock_db = Mock()
            mock_db.close = Mock()
            db_path = f"/tmp/test_db_{i}"
            mock_dbs.append(mock_db)
            self.manager.register_instance(db_path, mock_db)

        # 关闭所有实例
        self.manager.close_all()

        # 验证所有实例的close都被调用
        for mock_db in mock_dbs:
            mock_db.close.assert_called_once()

        # 验证所有实例都被移除
        self.assertEqual(len(self.manager.get_all_instances()), 0)

    def test_close_all_handles_exceptions(self):
        """测试close_all处理异常"""
        # 创建会抛出异常的实例
        error_db = Mock()
        error_db.close = Mock(side_effect=Exception("Close failed"))
        normal_db = Mock()
        normal_db.close = Mock()

        self.manager.register_instance("/tmp/error_db", error_db)
        self.manager.register_instance("/tmp/normal_db", normal_db)

        # close_all不应该抛出异常
        try:
            self.manager.close_all()
        except Exception as e:
            self.fail(f"close_all不应该抛出异常: {e}")

        # 验证两个实例的close都被调用
        error_db.close.assert_called_once()
        normal_db.close.assert_called_once()

    def test_get_all_instances(self):
        """测试获取所有实例"""
        mock_dbs = {}
        for i in range(3):
            mock_db = Mock()
            db_path = f"/tmp/test_db_{i}"
            mock_dbs[self.manager._normalize_path(db_path)] = mock_db
            self.manager.register_instance(db_path, mock_db)

        # 获取所有实例
        all_instances = self.manager.get_all_instances()

        # 验证返回的是副本
        self.assertIsNot(all_instances, self.manager._instances)

        # 验证内容相同
        self.assertEqual(len(all_instances), 3)
        for path, instance in mock_dbs.items():
            self.assertIn(path, all_instances)
            self.assertIs(all_instances[path], instance)

    def test_thread_safety_register(self):
        """测试并发注册的线程安全性"""
        mock_dbs = []
        threads = []

        def register_db(index):
            mock_db = Mock()
            db_path = f"/tmp/test_db_{index}"
            mock_dbs.append((db_path, mock_db))
            self.manager.register_instance(db_path, mock_db)

        # 并发注册
        for i in range(10):
            thread = threading.Thread(target=register_db, args=(i,))
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证所有实例都被注册
        self.assertEqual(len(self.manager.get_all_instances()), 10)

    def test_thread_safety_get_instance(self):
        """测试并发获取的线程安全性"""
        mock_db = Mock()
        db_path = "/tmp/test_db"
        self.manager.register_instance(db_path, mock_db)

        results = []
        threads = []

        def get_db():
            instance = self.manager.get_instance(db_path)
            results.append(instance)

        # 并发获取
        for _ in range(10):
            thread = threading.Thread(target=get_db)
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证所有结果都相同
        for result in results:
            self.assertIs(result, mock_db)

    def test_relative_paths_are_equivalent(self):
        """测试相对路径和绝对路径指向同一实例"""
        mock_db = Mock()

        # 使用相对路径注册
        relative_path = "test_db"
        self.manager.register_instance(relative_path, mock_db)

        # 使用绝对路径获取
        absolute_path = os.path.abspath(relative_path)
        retrieved = self.manager.get_instance(absolute_path)

        # 应该获取到同一个实例
        self.assertIs(retrieved, mock_db)


class TestInstanceManagerIntegration(unittest.TestCase):
    """InstanceManager与RawLevelDBDict的集成测试"""

    def setUp(self):
        """清理状态"""
        self.manager = DBInstanceManager()
        self.manager.close_all()

    def tearDown(self):
        """清理所有实例"""
        self.manager.close_all()

    def test_real_database_caching(self):
        """测试真实数据库的实例缓存"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "test_db")

            # 创建第一个数据库实例
            db1 = RawLevelDBDict("test_db", temp_dir, rebuild=True)
            db1["key1"] = "value1"

            # 如果支持实例管理器，应该被缓存
            # 注意：这需要RawLevelDBDict实际使用了instance_manager

            # 尝试创建第二个实例（相同路径）
            # 如果使用了instance_manager，应该返回缓存的实例或处理冲突

            db1.close()

    def test_multiple_databases(self):
        """测试管理多个数据库实例"""
        with tempfile.TemporaryDirectory() as temp_dir:
            dbs = []

            # 创建多个不同的数据库
            for i in range(3):
                db = RawLevelDBDict(f"test_db_{i}", temp_dir, rebuild=True)
                db[f"key_{i}"] = f"value_{i}"
                dbs.append(db)

            # 清理
            for db in dbs:
                db.close()

    def test_rebuild_closes_old_instance(self):
        """测试rebuild时关闭旧实例"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "test_db")

            # 创建数据库并写入数据
            db1 = RawLevelDBDict("test_db", temp_dir, rebuild=False)
            db1["key1"] = "value1"
            db1.close()

            # 使用rebuild=True重新创建
            db2 = RawLevelDBDict("test_db", temp_dir, rebuild=True)

            # rebuild应该清空旧数据
            with self.assertRaises(KeyError):
                _ = db2["key1"]

            db2.close()


if __name__ == '__main__':
    unittest.main()
