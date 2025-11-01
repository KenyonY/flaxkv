"""
并发安全测试

测试FlaxKV2在多线程和多进程环境下的安全性和稳定性
"""

import unittest
import tempfile
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict


class TestConcurrentReads(unittest.TestCase):
    """测试并发读取"""

    def setUp(self):
        """创建测试数据库"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = RawLevelDBDict("test_db", self.temp_dir.name, rebuild=True)

        # 预填充数据
        for i in range(100):
            self.db[f"key_{i}"] = f"value_{i}"

    def tearDown(self):
        """清理资源"""
        self.db.close()
        self.temp_dir.cleanup()

    def test_concurrent_reads_same_key(self):
        """测试多线程读取同一个键"""
        key = "key_50"
        expected_value = "value_50"
        results = []
        errors = []

        def read_key():
            try:
                value = self.db[key]
                results.append(value)
            except Exception as e:
                errors.append(e)

        # 启动100个线程同时读取
        threads = []
        for _ in range(100):
            thread = threading.Thread(target=read_key)
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证结果
        self.assertEqual(len(errors), 0, f"不应该有错误: {errors}")
        self.assertEqual(len(results), 100)
        # 所有结果都应该相同
        for value in results:
            self.assertEqual(value, expected_value)

    def test_concurrent_reads_different_keys(self):
        """测试多线程读取不同的键"""
        results = {}
        errors = []
        lock = threading.Lock()

        def read_key(key_num):
            try:
                key = f"key_{key_num}"
                value = self.db[key]
                with lock:
                    results[key] = value
            except Exception as e:
                with lock:
                    errors.append(e)

        # 启动100个线程读取不同的键
        threads = []
        for i in range(100):
            thread = threading.Thread(target=read_key, args=(i,))
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证结果
        self.assertEqual(len(errors), 0, f"不应该有错误: {errors}")
        self.assertEqual(len(results), 100)
        # 验证每个值都正确
        for i in range(100):
            key = f"key_{i}"
            self.assertEqual(results[key], f"value_{i}")


class TestConcurrentWrites(unittest.TestCase):
    """测试并发写入"""

    def setUp(self):
        """创建测试数据库"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = RawLevelDBDict("test_db", self.temp_dir.name, rebuild=True)

    def tearDown(self):
        """清理资源"""
        self.db.close()
        self.temp_dir.cleanup()

    def test_concurrent_writes_different_keys(self):
        """测试多线程写入不同的键"""
        errors = []

        def write_key(key_num):
            try:
                key = f"key_{key_num}"
                value = f"value_{key_num}"
                self.db[key] = value
            except Exception as e:
                errors.append(e)

        # 启动100个线程写入不同的键
        threads = []
        for i in range(100):
            thread = threading.Thread(target=write_key, args=(i,))
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证结果
        self.assertEqual(len(errors), 0, f"不应该有错误: {errors}")

        # 验证所有键都被正确写入
        for i in range(100):
            key = f"key_{i}"
            self.assertIn(key, self.db)
            self.assertEqual(self.db[key], f"value_{i}")

    def test_concurrent_writes_same_key(self):
        """测试多线程写入同一个键"""
        key = "test_key"
        write_count = 100
        errors = []
        success_count = [0]
        lock = threading.Lock()

        def write_key(thread_id):
            try:
                value = f"value_from_thread_{thread_id}"
                self.db[key] = value
                with lock:
                    success_count[0] += 1
            except Exception as e:
                errors.append(e)

        # 启动100个线程写入同一个键
        threads = []
        for i in range(write_count):
            thread = threading.Thread(target=write_key, args=(i,))
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证结果
        self.assertEqual(len(errors), 0, f"不应该有错误: {errors}")
        self.assertEqual(success_count[0], write_count)

        # 最终值应该是某个线程写入的值
        final_value = self.db[key]
        self.assertTrue(final_value.startswith("value_from_thread_"))


class TestConcurrentMixedOperations(unittest.TestCase):
    """测试并发混合操作（读写删除）"""

    def setUp(self):
        """创建测试数据库"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = RawLevelDBDict("test_db", self.temp_dir.name, rebuild=True)

        # 预填充数据
        for i in range(100):
            self.db[f"key_{i}"] = f"value_{i}"

    def tearDown(self):
        """清理资源"""
        self.db.close()
        self.temp_dir.cleanup()

    def test_concurrent_read_write_delete(self):
        """测试并发读写删除操作"""
        operations = []
        errors = []
        lock = threading.Lock()

        def read_operation(key_num):
            try:
                key = f"key_{key_num}"
                try:
                    value = self.db[key]
                    with lock:
                        operations.append(('read', key, value))
                except KeyError:
                    # 键可能已被删除
                    with lock:
                        operations.append(('read_miss', key, None))
            except Exception as e:
                with lock:
                    errors.append(('read', e))

        def write_operation(key_num):
            try:
                key = f"key_{key_num}"
                value = f"new_value_{key_num}_{time.time()}"
                self.db[key] = value
                with lock:
                    operations.append(('write', key, value))
            except Exception as e:
                with lock:
                    errors.append(('write', e))

        def delete_operation(key_num):
            try:
                key = f"key_{key_num}"
                try:
                    del self.db[key]
                    with lock:
                        operations.append(('delete', key, None))
                except KeyError:
                    # 键可能已被删除或不存在
                    pass
            except Exception as e:
                with lock:
                    errors.append(('delete', e))

        # 启动混合操作
        threads = []
        for i in range(100):
            op_type = random.choice(['read', 'write', 'delete'])
            key_num = random.randint(0, 99)

            if op_type == 'read':
                thread = threading.Thread(target=read_operation, args=(key_num,))
            elif op_type == 'write':
                thread = threading.Thread(target=write_operation, args=(key_num,))
            else:  # delete
                thread = threading.Thread(target=delete_operation, args=(key_num,))

            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证没有异常错误（KeyError不算）
        self.assertEqual(len(errors), 0, f"不应该有错误: {errors}")

    def test_concurrent_iteration(self):
        """测试并发迭代和修改"""
        errors = []
        iteration_results = []
        lock = threading.Lock()

        def iterate_db():
            try:
                keys = []
                for key in self.db.keys():
                    keys.append(key)
                with lock:
                    iteration_results.append(len(keys))
            except Exception as e:
                with lock:
                    errors.append(('iterate', e))

        def modify_db(key_num):
            try:
                key = f"new_key_{key_num}"
                self.db[key] = f"new_value_{key_num}"
            except Exception as e:
                with lock:
                    errors.append(('modify', e))

        # 启动迭代和修改线程
        threads = []

        # 3个迭代线程
        for _ in range(3):
            thread = threading.Thread(target=iterate_db)
            threads.append(thread)
            thread.start()

        # 10个修改线程
        for i in range(10):
            thread = threading.Thread(target=modify_db, args=(i,))
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证没有致命错误
        fatal_errors = [e for e in errors if not isinstance(e[1], (KeyError, RuntimeError))]
        self.assertEqual(len(fatal_errors), 0, f"不应该有致命错误: {fatal_errors}")


class TestConcurrentWithTTL(unittest.TestCase):
    """测试并发TTL操作"""

    def setUp(self):
        """创建测试数据库"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = RawLevelDBDict("test_db", self.temp_dir.name, rebuild=True)

    def tearDown(self):
        """清理资源"""
        self.db.close()
        self.temp_dir.cleanup()

    def test_concurrent_ttl_operations(self):
        """测试并发TTL设置和读取"""
        errors = []

        def write_with_ttl(key_num):
            try:
                key = f"key_{key_num}"
                value = f"value_{key_num}"
                self.db[key] = value
                self.db.set_ttl(key, 10)  # 10秒TTL
            except Exception as e:
                errors.append(e)

        def read_with_ttl_check(key_num):
            try:
                key = f"key_{key_num}"
                try:
                    value = self.db[key]
                    ttl = self.db.get_ttl(key)
                    # TTL可能是None（未设置）或一个数字
                except KeyError:
                    # 键可能不存在
                    pass
            except Exception as e:
                errors.append(e)

        # 启动混合操作
        threads = []
        for i in range(50):
            # 写入操作
            thread = threading.Thread(target=write_with_ttl, args=(i,))
            threads.append(thread)
            thread.start()

            # 读取操作
            thread = threading.Thread(target=read_with_ttl_check, args=(i,))
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证没有错误
        self.assertEqual(len(errors), 0, f"不应该有错误: {errors}")


class TestHighConcurrencyStress(unittest.TestCase):
    """高并发压力测试"""

    def setUp(self):
        """创建测试数据库"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = RawLevelDBDict("test_db", self.temp_dir.name, rebuild=True)

    def tearDown(self):
        """清理资源"""
        self.db.close()
        self.temp_dir.cleanup()

    def test_high_concurrency_write_stress(self):
        """测试高并发写入压力"""
        num_threads = 50
        ops_per_thread = 100
        errors = []

        def stress_write(thread_id):
            try:
                for i in range(ops_per_thread):
                    key = f"key_{thread_id}_{i}"
                    value = f"value_{thread_id}_{i}"
                    self.db[key] = value
            except Exception as e:
                errors.append(e)

        # 使用线程池执行
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(stress_write, i) for i in range(num_threads)]
            for future in as_completed(futures):
                future.result()  # 等待完成

        # 验证没有错误
        self.assertEqual(len(errors), 0, f"不应该有错误: {errors}")

        # 验证所有数据都写入成功
        expected_count = num_threads * ops_per_thread
        actual_count = len(list(self.db.keys()))
        self.assertEqual(actual_count, expected_count)

    def test_read_write_mixed_stress(self):
        """测试读写混合压力"""
        num_threads = 30
        ops_per_thread = 50
        errors = []

        # 预填充一些数据
        for i in range(100):
            self.db[f"initial_key_{i}"] = f"initial_value_{i}"

        def stress_operations(thread_id):
            try:
                for i in range(ops_per_thread):
                    # 随机选择操作类型
                    op = random.choice(['read', 'write'])

                    if op == 'read':
                        key = f"initial_key_{random.randint(0, 99)}"
                        try:
                            _ = self.db[key]
                        except KeyError:
                            pass  # 键可能不存在
                    else:  # write
                        key = f"key_{thread_id}_{i}"
                        value = f"value_{thread_id}_{i}"
                        self.db[key] = value
            except Exception as e:
                errors.append(e)

        # 使用线程池执行
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(stress_operations, i) for i in range(num_threads)]
            for future in as_completed(futures):
                future.result()

        # 验证没有错误
        self.assertEqual(len(errors), 0, f"不应该有错误: {errors}")


if __name__ == '__main__':
    unittest.main()
