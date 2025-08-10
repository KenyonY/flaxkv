"""
FlaxKV事务功能集成测试
"""

import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path

import flaxkv
from flaxkv.transaction.manager import IsolationLevel


@pytest.fixture
def temp_db_dir():
    """临时数据库目录。"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
class TestFlaxKVTransactions:
    """测试FlaxKV事务功能。"""
    
    async def test_transaction_context_manager(self, temp_db_dir):
        """测试事务上下文管理器。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("tx_test", "local", temp_db_dir, config) as db:
            # 成功的事务
            async with db.transaction() as tx:
                await tx.set("tx_key1", "tx_value1")
                await tx.set("tx_key2", "tx_value2")
            
            # 验证事务提交
            assert await db.get("tx_key1") == "tx_value1"
            assert await db.get("tx_key2") == "tx_value2"
    
    async def test_transaction_rollback_on_exception(self, temp_db_dir):
        """测试异常时事务回滚。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("tx_test", "local", temp_db_dir, config) as db:
            # 设置初始值
            await db.set("rollback_key", "original_value")
            
            # 失败的事务（应该回滚）
            with pytest.raises(ValueError):
                async with db.transaction() as tx:
                    await tx.set("rollback_key", "modified_value")
                    await tx.set("new_key", "new_value")
                    raise ValueError("Simulated error")
            
            # 验证事务回滚
            assert await db.get("rollback_key") == "original_value"
            assert await db.contains("new_key") is False
    
    async def test_transaction_isolation_levels(self, temp_db_dir):
        """测试不同的事务隔离级别。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("tx_test", "local", temp_db_dir, config) as db:
            await db.set("isolation_key", "initial_value")
            await db.flush()  # 强制刷新缓冲区
            
            # READ_COMMITTED 隔离级别
            async with db.transaction(IsolationLevel.READ_COMMITTED) as tx:
                value = await tx.get("isolation_key")
                assert value == "initial_value"
                await tx.set("isolation_key", "committed_value")
            
            # 验证提交
            assert await db.get("isolation_key") == "committed_value"
            
            # REPEATABLE_READ 隔离级别
            async with db.transaction(IsolationLevel.REPEATABLE_READ) as tx:
                value = await tx.get("isolation_key")
                assert value == "committed_value"
                await tx.set("isolation_key", "repeatable_value")
            
            # 验证提交
            assert await db.get("isolation_key") == "repeatable_value"
    
    async def test_manual_transaction_control(self, temp_db_dir):
        """测试手动事务控制。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("tx_test", "local", temp_db_dir, config) as db:
            # 手动开始事务
            tx = await db.begin_transaction()
            
            try:
                await tx.set("manual_key1", "manual_value1")
                await tx.set("manual_key2", "manual_value2")
                
                # 手动提交
                await db.tx_manager.commit_transaction(tx)
                
                # 验证提交
                assert await db.get("manual_key1") == "manual_value1"
                assert await db.get("manual_key2") == "manual_value2"
                
            except Exception:
                # 出错时回滚
                await db.tx_manager.abort_transaction(tx)
                raise
    
    @pytest.mark.asyncio
    async def test_transaction_read_write_operations(self, temp_db_dir):
        """测试事务中的读写操作。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("tx_test", "local", temp_db_dir, config) as db:
            # 准备初始数据
            await db.set("existing_key", "existing_value")
            await db.flush()  # 确保数据被刷新到存储引擎
            
            async with db.transaction() as tx:
                # 读取现有键
                value = await tx.get("existing_key")
                assert value == "existing_value"
                
                # 修改现有键
                await tx.set("existing_key", "modified_value")
                
                # 创建新键
                await tx.set("new_key", "new_value")
                
                # 删除键（先创建再删除）
                await tx.set("temp_key", "temp_value")
                await tx.delete("temp_key")
            
            # 验证事务结果
            assert await db.get("existing_key") == "modified_value"
            assert await db.get("new_key") == "new_value"
            assert await db.contains("temp_key") is False
    
    async def test_transaction_delete_nonexistent_key(self, temp_db_dir):
        """测试事务中删除不存在的键。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("tx_test", "local", temp_db_dir, config) as db:
            async with db.transaction() as tx:
                # 删除不存在的键（应该不抛出异常）
                await tx.delete("nonexistent_key")
                
                # 添加一些实际的操作
                await tx.set("actual_key", "actual_value")
            
            # 验证操作成功
            assert await db.get("actual_key") == "actual_value"
    
    async def test_transaction_state_validation(self, temp_db_dir):
        """测试事务状态验证。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("tx_test", "local", temp_db_dir, config) as db:
            tx = await db.begin_transaction()
            
            # 在活跃状态下可以执行操作
            assert tx.state.value == "active"
            await tx.set("state_key", "state_value")
            
            # 提交后状态应该改变
            await db.tx_manager.commit_transaction(tx)
            assert tx.state.value == "committed"
            
            # 提交后不能再执行操作
            with pytest.raises(RuntimeError, match="事务状态不正确"):
                await tx.set("another_key", "another_value")
    
    async def test_transaction_statistics(self, temp_db_dir):
        """测试事务统计信息。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("tx_test", "local", temp_db_dir, config) as db:
            # 获取初始统计
            initial_stats = db.tx_manager.get_stats()
            initial_started = initial_stats["transactions_started"]
            
            # 执行一个成功的事务
            async with db.transaction() as tx:
                await tx.set("stats_key", "stats_value")
            
            # 执行一个失败的事务
            try:
                async with db.transaction() as tx:
                    await tx.set("fail_key", "fail_value")
                    raise ValueError("Test error")
            except ValueError:
                pass
            
            # 检查统计更新
            final_stats = db.tx_manager.get_stats()
            assert final_stats["transactions_started"] == initial_started + 2
            assert final_stats["transactions_committed"] >= 1
            assert final_stats["transactions_aborted"] >= 1
    
    @pytest.mark.asyncio
    async def test_transaction_info(self, temp_db_dir):
        """测试事务信息获取。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("tx_test", "local", temp_db_dir, config) as db:
            tx = await db.begin_transaction(IsolationLevel.REPEATABLE_READ)
            
            try:
                # 执行一些操作
                await tx.set("info_key1", "info_value1")
                await tx.get("info_key1")
                await tx.set("info_key2", "info_value2")
                
                # 获取事务信息
                info = tx.get_info()
                
                assert info["tx_id"] == tx.tx_id
                assert info["state"] == "active"
                assert info["isolation_level"] == "repeatable_read"
                assert info["operations_count"] == 3  # 2个写操作 + 1个读操作
                assert info["write_set_size"] == 2
                assert info["read_set_size"] == 1
                assert info["stats"]["reads"] == 1
                assert info["stats"]["writes"] == 2
                
                await db.tx_manager.commit_transaction(tx)
                
            except Exception:
                await db.tx_manager.abort_transaction(tx)
                raise
    
    async def test_active_transactions_list(self, temp_db_dir):
        """测试活跃事务列表。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("tx_test", "local", temp_db_dir, config) as db:
            # 开始两个事务
            tx1 = await db.begin_transaction()
            tx2 = await db.begin_transaction(IsolationLevel.SERIALIZABLE)
            
            try:
                # 获取活跃事务列表
                active_txs = db.tx_manager.get_active_transactions()
                
                assert len(active_txs) == 2
                tx_ids = [tx_info["tx_id"] for tx_info in active_txs]
                assert tx1.tx_id in tx_ids
                assert tx2.tx_id in tx_ids
                
                # 提交一个事务
                await db.tx_manager.commit_transaction(tx1)
                
                # 活跃事务应该减少
                active_txs = db.tx_manager.get_active_transactions()
                assert len(active_txs) == 1
                assert active_txs[0]["tx_id"] == tx2.tx_id
                
                await db.tx_manager.commit_transaction(tx2)
                
            except Exception:
                await db.tx_manager.abort_transaction(tx1)
                await db.tx_manager.abort_transaction(tx2)
                raise
    
    @pytest.mark.asyncio
    async def test_transaction_sync_interface(self, temp_db_dir):
        """测试事务的错误处理：在异步上下文中不能使用同步接口。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("tx_test", "local", temp_db_dir, config) as db:
            async with db.transaction() as tx:
                # 在异步上下文中使用同步接口应该抛出错误
                with pytest.raises(RuntimeError, match="不能在异步上下文中使用同步接口"):
                    tx["sync_key1"] = "sync_value1"
                
                # 但异步接口应该正常工作
                await tx.set("async_key", "async_value")
                value = await tx.get("async_key")
                assert value == "async_value"
            
            # 验证事务结果
            assert await db.get("async_key") == "async_value"


@pytest.mark.asyncio
class TestTransactionConcurrency:
    """测试事务并发性（简化版，因为我们使用单进程）。"""
    
    async def test_concurrent_transactions_different_keys(self, temp_db_dir):
        """测试操作不同键的并发事务。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("tx_test", "local", temp_db_dir, config) as db:
            async def transaction1():
                async with db.transaction() as tx:
                    await tx.set("concurrent_key1", "value1")
                    await asyncio.sleep(0.1)  # 模拟处理时间
                    await tx.set("concurrent_key1", "final_value1")
            
            async def transaction2():
                async with db.transaction() as tx:
                    await tx.set("concurrent_key2", "value2")
                    await asyncio.sleep(0.1)  # 模拟处理时间
                    await tx.set("concurrent_key2", "final_value2")
            
            # 并发执行两个事务
            await asyncio.gather(transaction1(), transaction2())
            
            # 验证两个事务都成功提交
            assert await db.get("concurrent_key1") == "final_value1"
            assert await db.get("concurrent_key2") == "final_value2"