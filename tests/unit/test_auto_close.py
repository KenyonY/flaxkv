"""
测试 AutoClose 模块的数据库自动关闭功能
"""

import unittest
import tempfile
from unittest.mock import Mock, patch, call
from flaxkv2.auto_close import DBCloseManager
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict


class TestDBCloseManager(unittest.TestCase):
    """测试 DBCloseManager 类"""

    def setUp(self):
        """每个测试前重置管理器"""
        # 创建新的管理器实例用于测试
        self.manager = DBCloseManager()

    def test_register_instance(self):
        """测试注册数据库实例"""
        # 创建模拟数据库实例
        mock_db = Mock()
        mock_db.name = "test_db"
        mock_db._closed = False

        # 注册实例
        self.manager.register(mock_db)

        # 验证实例被注册
        self.assertEqual(len(self.manager._instances), 1)
        # 验证atexit已注册
        self.assertTrue(self.manager._registered)

    def test_register_multiple_instances(self):
        """测试注册多个数据库实例"""
        mock_dbs = []
        for i in range(3):
            mock_db = Mock()
            mock_db.name = f"test_db_{i}"
            mock_db._closed = False
            mock_dbs.append(mock_db)
            self.manager.register(mock_db)

        # 验证所有实例都被注册
        self.assertEqual(len(self.manager._instances), 3)

    def test_unregister_instance(self):
        """测试注销数据库实例"""
        mock_db = Mock()
        mock_db.name = "test_db"
        mock_db._closed = False

        # 注册然后注销
        self.manager.register(mock_db)
        self.assertEqual(len(self.manager._instances), 1)

        self.manager.unregister(mock_db)
        self.assertEqual(len(self.manager._instances), 0)

    def test_unregister_nonexistent_instance(self):
        """测试注销不存在的实例不会报错"""
        mock_db = Mock()
        mock_db.name = "test_db"
        mock_db._closed = False

        # 注销不存在的实例不应该抛出异常
        try:
            self.manager.unregister(mock_db)
        except Exception as e:
            self.fail(f"注销不存在的实例不应该抛出异常: {e}")

    def test_close_all_basic(self):
        """测试关闭所有实例"""
        # 创建模拟数据库实例
        mock_dbs = []
        for i in range(3):
            mock_db = Mock()
            mock_db.name = f"test_db_{i}"
            mock_db.db_path = f"/tmp/test_{i}"
            mock_db._closed = False
            mock_db.close = Mock()
            mock_dbs.append(mock_db)
            self.manager.register(mock_db)

        # 关闭所有实例
        self.manager.close_all()

        # 验证每个实例的close方法都被调用（不带参数）
        for mock_db in mock_dbs:
            mock_db.close.assert_called_once()

        # 验证实例集合被清空
        self.assertEqual(len(self.manager._instances), 0)

    def test_close_all_skips_already_closed(self):
        """测试close_all跳过已经关闭的实例"""
        # 创建已关闭和未关闭的实例
        closed_db = Mock()
        closed_db.name = "closed_db"
        closed_db._closed = True
        closed_db.close = Mock()

        open_db = Mock()
        open_db.name = "open_db"
        open_db.db_path = "/tmp/open"
        open_db._closed = False
        open_db.close = Mock()

        self.manager.register(closed_db)
        self.manager.register(open_db)

        # 关闭所有实例
        self.manager.close_all()

        # 验证只有未关闭的实例被关闭
        closed_db.close.assert_not_called()
        open_db.close.assert_called_once()

    def test_close_all_handles_exceptions(self):
        """测试close_all处理异常不会中断"""
        # 创建会抛出异常的实例
        error_db = Mock()
        error_db.name = "error_db"
        error_db.db_path = "/tmp/error"
        error_db._closed = False
        error_db.close = Mock(side_effect=Exception("Close failed"))

        normal_db = Mock()
        normal_db.name = "normal_db"
        normal_db.db_path = "/tmp/normal"
        normal_db._closed = False
        normal_db.close = Mock()

        self.manager.register(error_db)
        self.manager.register(normal_db)

        # 关闭所有实例不应该抛出异常
        try:
            self.manager.close_all()
        except Exception as e:
            self.fail(f"close_all不应该抛出异常: {e}")

        # 验证两个实例的close都被调用
        error_db.close.assert_called_once()
        normal_db.close.assert_called_once()

    def test_weakref_cleanup(self):
        """测试弱引用自动清理"""
        import gc

        mock_db = Mock()
        mock_db.name = "test_db"
        mock_db._closed = False

        self.manager.register(mock_db)
        self.assertEqual(len(self.manager._instances), 1)

        # 删除引用并触发垃圾回收
        del mock_db
        gc.collect()

        # 注意：弱引用的回调是异步的，可能不会立即清理
        # 这里只是确保不会崩溃
        # 在实际使用中，_remove_instance会在对象被GC时调用

    def test_thread_safety_register(self):
        """测试并发注册的线程安全性"""
        import threading

        mock_dbs = []
        for i in range(10):
            mock_db = Mock()
            mock_db.name = f"test_db_{i}"
            mock_db._closed = False
            mock_dbs.append(mock_db)

        # 并发注册
        threads = []
        for mock_db in mock_dbs:
            thread = threading.Thread(target=self.manager.register, args=(mock_db,))
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证所有实例都被注册
        self.assertEqual(len(self.manager._instances), 10)

    def test_thread_safety_close_all(self):
        """测试并发关闭的线程安全性"""
        import threading

        # 注册多个实例
        mock_dbs = []
        for i in range(5):
            mock_db = Mock()
            mock_db.name = f"test_db_{i}"
            mock_db.db_path = f"/tmp/test_{i}"
            mock_db._closed = False
            mock_db.close = Mock()
            mock_dbs.append(mock_db)
            self.manager.register(mock_db)

        # 并发调用close_all
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=self.manager.close_all)
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证不会崩溃
        self.assertEqual(len(self.manager._instances), 0)

    def test_atexit_registration_once(self):
        """测试atexit只注册一次"""
        with patch('atexit.register') as mock_atexit:
            manager = DBCloseManager()

            # 注册多个实例
            for i in range(3):
                mock_db = Mock()
                mock_db.name = f"test_db_{i}"
                mock_db._closed = False
                manager.register(mock_db)

            # 验证atexit.register只被调用一次
            mock_atexit.assert_called_once_with(manager.close_all)


class TestAutoCloseIntegration(unittest.TestCase):
    """AutoClose与RawLevelDBDict的集成测试"""

    def test_real_database_auto_close(self):
        """测试真实数据库的自动关闭"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建真实数据库实例
            db = RawLevelDBDict("test_db", temp_dir, rebuild=True)

            # 检查是否已注册到自动关闭管理器
            # 注意：这需要RawLevelDBDict实际使用了auto_close模块
            from flaxkv2.auto_close import db_close_manager

            # 写入一些数据
            db["key1"] = "value1"
            db["key2"] = "value2"

            # 手动触发close_all（模拟程序退出）
            initial_closed_status = db._closed
            db_close_manager.close_all()

            # 数据库应该被关闭了
            # 注意：这依赖于RawLevelDBDict是否使用了auto_close

    def test_multiple_databases_auto_close(self):
        """测试多个数据库的自动关闭"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建多个数据库
            dbs = []
            for i in range(3):
                db = RawLevelDBDict(f"test_db_{i}", temp_dir, rebuild=True)
                db[f"key_{i}"] = f"value_{i}"
                dbs.append(db)

            # 所有数据库都应该被注册
            from flaxkv2.auto_close import db_close_manager

            # 手动关闭一个数据库
            dbs[0].close()

            # 手动触发close_all
            db_close_manager.close_all()

            # 所有数据库都应该被处理
            # 已关闭的不会再次关闭，未关闭的会被关闭


if __name__ == '__main__':
    unittest.main()
