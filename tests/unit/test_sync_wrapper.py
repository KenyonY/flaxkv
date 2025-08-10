"""
同步包装器单元测试
"""

import pytest
import tempfile
import shutil
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from flaxkv.core.sync_wrapper import ThreadSafeFlaxKV, AsyncEventLoopThread
from flaxkv.core.config import FlaxKVConfig
from flaxkv.transaction.manager import IsolationLevel


@pytest.fixture
def temp_db_path():
    """临时数据库路径。"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def test_config():
    """测试配置。"""
    return FlaxKVConfig.for_testing()


@pytest.fixture
def sync_flaxkv(temp_db_path, test_config):
    """同步FlaxKV实例。"""
    db = ThreadSafeFlaxKV("test_sync_db", "local", temp_db_path, test_config)
    yield db
    db.close()


class TestAsyncEventLoopThread:
    """测试异步事件循环线程。"""
    
    def test_start_and_stop(self):
        """测试启动和停止事件循环。"""
        loop_thread = AsyncEventLoopThread()
        
        # 初始状态
        assert loop_thread.loop is None
        assert loop_thread.thread is None
        assert not loop_thread.is_running()
        
        # 启动
        loop_thread.start()
        assert loop_thread.loop is not None
        assert loop_thread.thread is not None
        assert loop_thread.is_running()
        
        # 等待线程完全启动
        time.sleep(0.1)
        assert loop_thread.thread.is_alive()
        
        # 停止
        loop_thread.stop()
        assert not loop_thread.is_running()
        
        # 等待线程完全停止
        time.sleep(0.2)
        if loop_thread.thread:
            assert not loop_thread.thread.is_alive()
    
    def test_multiple_start_stop(self):
        """测试多次启动停止。"""
        loop_thread = AsyncEventLoopThread()
        
        # 多次启动应该是安全的
        loop_thread.start()
        loop_thread.start()
        loop_thread.start()
        
        assert loop_thread.is_running()
        
        # 多次停止应该是安全的
        loop_thread.stop()
        loop_thread.stop()
        loop_thread.stop()
        
        assert not loop_thread.is_running()
    
    def test_context_manager(self):
        """测试上下文管理器。"""
        with AsyncEventLoopThread() as loop_thread:
            assert loop_thread.is_running()
            assert loop_thread.loop is not None
        
        # 退出上下文后应该停止
        assert not loop_thread.is_running()
    
    def test_run_coroutine(self):
        """测试运行协程。"""
        loop_thread = AsyncEventLoopThread()
        loop_thread.start()
        
        try:
            # 测试简单协程
            async def simple_coro():
                return "hello"
            
            result = loop_thread.run_coroutine(simple_coro())
            assert result == "hello"
            
            # 测试带参数的协程
            async def add_coro(a, b):
                return a + b
            
            result = loop_thread.run_coroutine(add_coro(3, 4))
            assert result == 7
            
        finally:
            loop_thread.stop()
    
    def test_run_coroutine_timeout(self):
        """测试协程超时。"""
        loop_thread = AsyncEventLoopThread()
        loop_thread.start()
        
        try:
            import asyncio
            
            async def slow_coro():
                await asyncio.sleep(2)
                return "done"
            
            # 应该超时
            with pytest.raises(Exception):  # 可能是TimeoutError或其他异常
                loop_thread.run_coroutine(slow_coro(), timeout=0.1)
                
        finally:
            loop_thread.stop()
    
    def test_exception_handling(self):
        """测试异常处理。"""
        loop_thread = AsyncEventLoopThread()
        loop_thread.start()
        
        try:
            async def failing_coro():
                raise ValueError("test error")
            
            with pytest.raises(ValueError, match="test error"):
                loop_thread.run_coroutine(failing_coro())
                
        finally:
            loop_thread.stop()


class TestThreadSafeFlaxKVBasic:
    """测试ThreadSafeFlaxKV基本功能。"""
    
    def test_initialization(self, temp_db_path, test_config):
        """测试初始化。"""
        db = ThreadSafeFlaxKV("init_test", "local", temp_db_path, test_config)
        
        # 应该未初始化
        assert not db._initialized
        
        # 确保初始化
        db._ensure_initialized()
        assert db._initialized
        assert db._loop_thread.is_running()
        
        db.close()
    
    def test_auto_initialization(self, temp_db_path, test_config):
        """测试自动初始化。"""
        db = ThreadSafeFlaxKV("auto_init_test", "local", temp_db_path, test_config, auto_initialize=True)
        
        # 应该自动初始化
        assert db._initialized
        assert db._loop_thread.is_running()
        
        db.close()
    
    def test_context_manager(self, temp_db_path, test_config):
        """测试上下文管理器。"""
        with ThreadSafeFlaxKV("context_test", "local", temp_db_path, test_config) as db:
            assert db._initialized
            assert db._loop_thread.is_running()
            
            # 在上下文中进行操作
            db.set("context_key", "context_value")
            assert db.get("context_key") == "context_value"
        
        # 退出上下文后应该关闭
        assert not db._loop_thread.is_running()
    
    def test_double_close(self, sync_flaxkv):
        """测试重复关闭。"""
        sync_flaxkv.close()
        sync_flaxkv.close()  # 重复关闭不应该出错


class TestThreadSafeFlaxKVOperations:
    """测试ThreadSafeFlaxKV操作。"""
    
    def test_basic_crud(self, sync_flaxkv):
        """测试基本CRUD操作。"""
        # Set
        sync_flaxkv.set("key1", "value1")
        sync_flaxkv.set("key2", {"nested": "data"})
        
        # Get
        assert sync_flaxkv.get("key1") == "value1"
        assert sync_flaxkv.get("key2") == {"nested": "data"}
        
        # Get with default
        assert sync_flaxkv.get("nonexistent", "default") == "default"
        
        # Contains
        assert sync_flaxkv.contains("key1") is True
        assert sync_flaxkv.contains("nonexistent") is False
        
        # Delete
        success = sync_flaxkv.delete("key1")
        assert success is True
        assert sync_flaxkv.contains("key1") is False
        
        # Delete nonexistent
        success = sync_flaxkv.delete("nonexistent")
        assert success is False
    
    def test_dict_interface(self, sync_flaxkv):
        """测试字典接口。"""
        # __setitem__
        sync_flaxkv["dict_key1"] = "dict_value1"
        sync_flaxkv["dict_key2"] = [1, 2, 3]
        
        # __getitem__
        assert sync_flaxkv["dict_key1"] == "dict_value1"
        assert sync_flaxkv["dict_key2"] == [1, 2, 3]
        
        # __contains__
        assert "dict_key1" in sync_flaxkv
        assert "nonexistent" not in sync_flaxkv
        
        # __delitem__
        del sync_flaxkv["dict_key1"]
        assert "dict_key1" not in sync_flaxkv
        
        # __len__
        original_size = len(sync_flaxkv)
        sync_flaxkv["new_key"] = "new_value"
        assert len(sync_flaxkv) == original_size + 1
    
    def test_batch_operations(self, sync_flaxkv):
        """测试批量操作。"""
        # mset
        items = {"batch1": "value1", "batch2": [1, 2, 3], "batch3": {"nested": "data"}}
        sync_flaxkv.mset(items)
        
        # mget
        result = sync_flaxkv.mget(["batch1", "batch3", "nonexistent"])
        assert result == {"batch1": "value1", "batch3": {"nested": "data"}}
        
        # mdelete
        deleted_count = sync_flaxkv.mdelete(["batch1", "batch3", "nonexistent"])
        assert deleted_count == 2
        assert "batch1" not in sync_flaxkv
        assert "batch3" not in sync_flaxkv
        assert "batch2" in sync_flaxkv
    
    def test_ttl_operations(self, sync_flaxkv):
        """测试TTL操作。"""
        # setex
        sync_flaxkv.setex("ttl_key1", "ttl_value1", 3600)
        assert sync_flaxkv.get("ttl_key1") == "ttl_value1"
        
        # expire
        sync_flaxkv.set("ttl_key2", "ttl_value2")
        success = sync_flaxkv.expire("ttl_key2", 3600)
        assert success is True
        
        # expire nonexistent
        success = sync_flaxkv.expire("nonexistent", 3600)
        assert success is False
    
    def test_scan_operations(self, sync_flaxkv):
        """测试扫描操作。"""
        # 准备数据
        test_data = {
            "user:001": {"name": "Alice"},
            "user:002": {"name": "Bob"},
            "product:001": {"name": "Book"},
            "product:002": {"name": "Pen"}
        }
        sync_flaxkv.mset(test_data)
        
        # 前缀扫描
        user_items = list(sync_flaxkv.scan(prefix="user:"))
        assert len(user_items) >= 2  # 可能有其他测试数据
        user_keys = [key for key, value in user_items]
        assert "user:001" in user_keys
        assert "user:002" in user_keys
        
        # 限制数量扫描
        limited_items = list(sync_flaxkv.scan(limit=2))
        assert len(limited_items) == 2
    
    def test_iterator_methods(self, sync_flaxkv):
        """测试迭代器方法。"""
        # 清空并添加测试数据
        sync_flaxkv.clear()
        test_data = {"iter1": "value1", "iter2": "value2", "iter3": "value3"}
        sync_flaxkv.mset(test_data)
        
        # keys
        keys = sync_flaxkv.keys()
        assert set(keys) == set(test_data.keys())
        
        # values
        values = sync_flaxkv.values()
        assert set(values) == set(test_data.values())
        
        # items
        items = sync_flaxkv.items()
        assert dict(items) == test_data
    
    def test_management_operations(self, sync_flaxkv):
        """测试管理操作。"""
        # 添加数据
        sync_flaxkv.mset({"mgmt1": "value1", "mgmt2": "value2"})
        
        # size
        size = sync_flaxkv.size()
        assert size >= 2
        
        # flush
        result = sync_flaxkv.flush()
        assert isinstance(result, dict)
        
        # get_stats
        stats = sync_flaxkv.get_stats()
        assert isinstance(stats, dict)
        
        # get_info
        info = sync_flaxkv.get_info()
        assert isinstance(info, dict)
        assert info["name"] == "test_sync_db"
        
        # clear
        sync_flaxkv.clear()
        assert sync_flaxkv.size() == 0


class TestThreadSafeFlaxKVTransactions:
    """测试ThreadSafeFlaxKV事务。"""
    
    def test_transaction_context_manager(self, sync_flaxkv):
        """测试事务上下文管理器。"""
        # 成功的事务
        with sync_flaxkv.transaction() as tx:
            tx.set("tx_key1", "tx_value1")
            tx.set("tx_key2", "tx_value2")
        
        # 验证提交
        assert sync_flaxkv.get("tx_key1") == "tx_value1"
        assert sync_flaxkv.get("tx_key2") == "tx_value2"
    
    def test_transaction_rollback(self, sync_flaxkv):
        """测试事务回滚。"""
        sync_flaxkv.set("rollback_key", "original_value")
        
        # 失败的事务
        try:
            with sync_flaxkv.transaction() as tx:
                tx.set("rollback_key", "modified_value")
                tx.set("new_key", "new_value")
                raise ValueError("Simulated error")
        except ValueError:
            pass
        
        # 验证回滚
        assert sync_flaxkv.get("rollback_key") == "original_value"
        assert "new_key" not in sync_flaxkv
    
    def test_transaction_isolation_levels(self, sync_flaxkv):
        """测试事务隔离级别。"""
        sync_flaxkv.set("isolation_key", "initial_value")
        
        with sync_flaxkv.transaction(IsolationLevel.READ_COMMITTED) as tx:
            value = tx.get("isolation_key")
            assert value == "initial_value"
            tx.set("isolation_key", "modified_value")
        
        assert sync_flaxkv.get("isolation_key") == "modified_value"


class TestThreadSafeFlaxKVConcurrency:
    """测试ThreadSafeFlaxKV并发安全性。"""
    
    def test_concurrent_reads_writes(self, sync_flaxkv):
        """测试并发读写。"""
        # 准备初始数据
        sync_flaxkv.mset({f"concurrent_{i}": f"value_{i}" for i in range(10)})
        
        def worker(worker_id: int, operations: int):
            results = []
            for i in range(operations):
                key = f"worker_{worker_id}_{i}"
                value = f"value_{worker_id}_{i}"
                
                # 写入
                sync_flaxkv.set(key, value)
                
                # 读取
                retrieved = sync_flaxkv.get(key)
                results.append((key, value, retrieved))
                
                # 更新已存在的键
                existing_key = f"concurrent_{i % 10}"
                sync_flaxkv.set(existing_key, f"updated_by_{worker_id}")
            
            return results
        
        # 启动多个worker并发执行
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(worker, worker_id, 20) for worker_id in range(4)]
            
            all_results = []
            for future in as_completed(futures):
                results = future.result()
                all_results.extend(results)
        
        # 验证所有操作都成功
        for key, expected_value, actual_value in all_results:
            assert actual_value == expected_value
    
    def test_concurrent_batch_operations(self, sync_flaxkv):
        """测试并发批量操作。"""
        def batch_worker(worker_id: int):
            # 批量写入
            items = {f"batch_{worker_id}_{i}": f"value_{i}" for i in range(50)}
            sync_flaxkv.mset(items)
            
            # 批量读取
            keys = list(items.keys())
            result = sync_flaxkv.mget(keys)
            
            # 验证结果
            assert len(result) == len(items)
            for key, value in items.items():
                assert result[key] == value
            
            return len(result)
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(batch_worker, worker_id) for worker_id in range(3)]
            
            results = [future.result() for future in as_completed(futures)]
            assert all(result == 50 for result in results)
    
    def test_concurrent_transactions(self, sync_flaxkv):
        """测试并发事务。"""
        # 准备初始数据
        sync_flaxkv.mset({f"tx_key_{i}": i for i in range(10)})
        
        def transaction_worker(worker_id: int):
            try:
                with sync_flaxkv.transaction() as tx:
                    # 读取并更新多个键
                    for i in range(3):
                        key = f"tx_key_{i}"
                        current_value = tx.get(key)
                        new_value = current_value + worker_id * 100
                        tx.set(key, new_value)
                    
                    # 创建worker特定的键
                    tx.set(f"worker_{worker_id}", f"completed_{worker_id}")
                
                return True
            except Exception as e:
                print(f"Worker {worker_id} failed: {e}")
                return False
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(transaction_worker, worker_id) for worker_id in range(3)]
            results = [future.result() for future in as_completed(futures)]
        
        # 至少有一些事务应该成功
        assert any(results)
        
        # 验证worker特定的键
        for worker_id in range(3):
            worker_key = f"worker_{worker_id}"
            if sync_flaxkv.contains(worker_key):
                assert sync_flaxkv.get(worker_key) == f"completed_{worker_id}"


class TestThreadSafeFlaxKVErrorHandling:
    """测试ThreadSafeFlaxKV错误处理。"""
    
    def test_key_errors(self, sync_flaxkv):
        """测试KeyError处理。"""
        with pytest.raises(KeyError):
            sync_flaxkv.get("nonexistent_key")
        
        with pytest.raises(KeyError):
            _ = sync_flaxkv["nonexistent_key"]
        
        with pytest.raises(KeyError):
            del sync_flaxkv["nonexistent_key"]
    
    def test_operation_after_close(self, temp_db_path, test_config):
        """测试关闭后的操作。"""
        db = ThreadSafeFlaxKV("test_close", "local", temp_db_path, test_config)
        db.set("test_key", "test_value")
        
        db.close()
        
        # 关闭后操作应该失败
        with pytest.raises(Exception):
            db.set("another_key", "another_value")
        
        with pytest.raises(Exception):
            db.get("test_key")
    
    def test_timeout_handling(self, temp_db_path, test_config):
        """测试超时处理。"""
        db = ThreadSafeFlaxKV("test_timeout", "local", temp_db_path, test_config, timeout=0.1)
        
        try:
            # 正常操作应该成功
            db.set("quick_key", "quick_value")
            assert db.get("quick_key") == "quick_value"
            
        finally:
            db.close()


class TestThreadSafeFlaxKVDataTypes:
    """测试ThreadSafeFlaxKV数据类型支持。"""
    
    def test_various_data_types(self, sync_flaxkv):
        """测试各种数据类型。"""
        test_data = {
            "string": "hello world",
            "integer": 42,
            "float": 3.14159,
            "boolean": True,
            "none_value": None,
            "list": [1, "two", 3.0, None],
            "dict": {"nested": {"data": [1, 2, 3]}},
            "tuple": (1, 2, "three"),
        }
        
        # 设置各种数据类型
        for key, value in test_data.items():
            sync_flaxkv[key] = value
        
        # 验证数据类型和值
        for key, expected_value in test_data.items():
            actual_value = sync_flaxkv[key]
            assert actual_value == expected_value
            assert type(actual_value) == type(expected_value)
    
    def test_pandas_dataframe_support(self, sync_flaxkv):
        """测试pandas DataFrame支持。"""
        pytest.importorskip("pandas")
        import pandas as pd
        
        # 创建测试DataFrame
        df = pd.DataFrame({
            'A': [1, 2, 3, 4],
            'B': ['a', 'b', 'c', 'd'],
            'C': [1.1, 2.2, 3.3, 4.4]
        })
        
        # 存储和检索DataFrame
        sync_flaxkv.set("dataframe", df)
        retrieved_df = sync_flaxkv.get("dataframe")
        
        assert isinstance(retrieved_df, pd.DataFrame)
        assert df.equals(retrieved_df)


class TestThreadSafeFlaxKVPerformance:
    """测试ThreadSafeFlaxKV性能相关功能。"""
    
    def test_bulk_operations_performance(self, sync_flaxkv):
        """测试批量操作性能。"""
        # 大批量数据
        large_batch = {f"perf_key_{i}": f"perf_value_{i}" for i in range(1000)}
        
        # 批量设置
        start_time = time.time()
        sync_flaxkv.mset(large_batch)
        set_time = time.time() - start_time
        
        # 批量获取
        keys = list(large_batch.keys())
        start_time = time.time()
        result = sync_flaxkv.mget(keys)
        get_time = time.time() - start_time
        
        # 验证结果
        assert len(result) == len(large_batch)
        
        # 性能应该在合理范围内（这里只是基本检查）
        assert set_time < 5.0  # 5秒内完成1000次设置
        assert get_time < 5.0  # 5秒内完成1000次获取
        
        print(f"批量设置1000项用时: {set_time:.3f}秒")
        print(f"批量获取1000项用时: {get_time:.3f}秒")
    
    def test_concurrent_performance(self, sync_flaxkv):
        """测试并发性能。"""
        def performance_worker(worker_id: int, operations: int):
            start_time = time.time()
            
            for i in range(operations):
                key = f"perf_{worker_id}_{i}"
                value = f"value_{i}"
                
                sync_flaxkv.set(key, value)
                retrieved = sync_flaxkv.get(key)
                assert retrieved == value
            
            return time.time() - start_time
        
        # 并发性能测试
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(performance_worker, worker_id, 100) for worker_id in range(4)]
            times = [future.result() for future in as_completed(futures)]
        
        # 所有worker应该在合理时间内完成
        for worker_time in times:
            assert worker_time < 10.0  # 10秒内完成100次读写
        
        avg_time = sum(times) / len(times)
        print(f"4个并发worker平均完成时间: {avg_time:.3f}秒")