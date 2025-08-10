"""
FlaxKV基本功能集成测试
"""

import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path

import flaxkv


@pytest.fixture
def temp_db_dir():
    """临时数据库目录。"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
class TestFlaxKVBasicOperations:
    """测试FlaxKV基本操作。"""
    
    async def test_create_and_close(self, temp_db_dir):
        """测试创建和关闭数据库。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        # 创建数据库
        db = flaxkv.FlaxKV("test_db", "local", temp_db_dir, config)
        
        # 初始化
        await db.initialize()
        assert db._initialized is True
        assert db._closed is False
        
        # 关闭
        await db.close()
        assert db._closed is True
    
    async def test_context_manager(self, temp_db_dir):
        """测试上下文管理器。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("test_db", "local", temp_db_dir, config) as db:
            assert db._initialized is True
            assert db._closed is False
            
            # 在上下文中执行操作
            await db.set("test_key", "test_value")
            value = await db.get("test_key")
            assert value == "test_value"
        
        # 上下文退出后应该被关闭
        assert db._closed is True
    
    async def test_basic_crud_operations(self, temp_db_dir):
        """测试基本CRUD操作。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("test_db", "local", temp_db_dir, config) as db:
            # Create/Update
            await db.set("key1", "value1")
            await db.set("key2", {"nested": "object"})
            await db.set("key3", [1, 2, 3])
            
            # Read
            assert await db.get("key1") == "value1"
            assert await db.get("key2") == {"nested": "object"}
            assert await db.get("key3") == [1, 2, 3]
            
            # 测试默认值
            assert await db.get("nonexistent", "default") == "default"
            
            # Contains
            assert await db.contains("key1") is True
            assert await db.contains("nonexistent") is False
            
            # Size
            assert await db.size() == 3
            
            # Delete
            success = await db.delete("key1")
            assert success is True
            assert await db.contains("key1") is False
            assert await db.size() == 2
            
            # 删除不存在的键
            success = await db.delete("nonexistent")
            assert success is False
    
    async def test_key_not_found(self, temp_db_dir):
        """测试键不存在的情况。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("test_db", "local", temp_db_dir, config) as db:
            # 获取不存在的键应该抛出KeyError
            with pytest.raises(KeyError):
                await db.get("nonexistent_key")
    
    async def test_mset_and_mget(self, temp_db_dir):
        """测试批量设置和获取。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        async with flaxkv.FlaxKV("test_db", "local", temp_db_dir, config) as db:
            items = {
                "batch1": "value1",
                "batch2": {"nested": "data"},
                "batch3": [1, 2, 3],
            }
            await db.mset(items)

            result = await db.mget(["batch1", "batch2", "nonexistent", "batch3"])
            assert len(result) == 3
            assert result["batch1"] == "value1"
            assert result["batch2"] == {"nested": "data"}
            assert result["batch3"] == [1, 2, 3]
            assert "nonexistent" not in result

    async def test_mdelete(self, temp_db_dir):
        """测试批量删除。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        async with flaxkv.FlaxKV("test_db", "local", temp_db_dir, config) as db:
            items = {"batch1": "v1", "batch2": "v2", "batch3": "v3"}
            await db.mset(items)

            deleted_count = await db.mdelete(["batch1", "batch3", "nonexistent"])
            assert deleted_count == 2
            assert await db.contains("batch1") is False
            assert await db.contains("batch2") is True
            assert await db.contains("batch3") is False

    async def test_scan_operations(self, temp_db_dir):
        """测试扫描操作。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("test_db", "local", temp_db_dir, config) as db:
            # 准备测试数据
            test_data = {
                "user:001": {"name": "Alice"},
                "user:002": {"name": "Bob"},
                "user:003": {"name": "Charlie"},
                "product:001": {"name": "Book"},
                "product:002": {"name": "Pen"},
            }
            
            await db.mset(test_data)
            
            # 前缀扫描
            user_items = []
            async for key, value in db.scan(prefix="user:"):
                user_items.append((key, value))
            
            assert len(user_items) == 3
            assert all(key.startswith("user:") for key, _ in user_items)
            
            # 范围扫描
            range_items = []
            async for key, value in db.scan(start="user:001", end="user:003"):
                range_items.append((key, value))
            
            assert len(range_items) == 2
            
            # 限制数量扫描
            limited_items = []
            async for key, value in db.scan(limit=2):
                limited_items.append((key, value))
            
            assert len(limited_items) == 2
    
    async def test_iterator_methods(self, temp_db_dir):
        """测试迭代器方法。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("test_db", "local", temp_db_dir, config) as db:
            # 准备数据
            test_data = {"key1": "value1", "key2": "value2", "key3": "value3"}
            await db.mset(test_data)
            
            # 测试keys()
            keys = []
            async for key in db.keys():
                keys.append(key)
            assert len(keys) == 3
            assert set(keys) == set(test_data.keys())
            
            # 测试values()
            values = []
            async for value in db.values():
                values.append(value)
            assert len(values) == 3
            assert set(values) == set(test_data.values())
            
            # 测试items()
            items = []
            async for key, value in db.items():
                items.append((key, value))
            assert len(items) == 3
            assert dict(items) == test_data

    @pytest.mark.parametrize("ttl", [0.1, 0.2])
    async def test_ttl_operations(self, temp_db_dir, ttl):
        """测试TTL操作和实际过期。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        # For TTL tests, we need a smaller cleanup interval
        config.cache_cleanup_interval = 0.05

        async with flaxkv.FlaxKV("test_db", "local", temp_db_dir, config) as db:
            # 1. 测试 setex
            await db.setex("ttl_key_1", "value1", ttl)
            assert await db.get("ttl_key_1") == "value1"

            # 2. 测试 expire
            await db.set("ttl_key_2", "value2")
            await db.expire("ttl_key_2", ttl)
            assert await db.get("ttl_key_2") == "value2"

            # 等待超过过期时间
            await asyncio.sleep(ttl + 0.1)

            # 验证键已过期
            with pytest.raises(KeyError):
                await db.get("ttl_key_1")
            with pytest.raises(KeyError):
                await db.get("ttl_key_2")
            assert await db.contains("ttl_key_1") is False
            assert await db.contains("ttl_key_2") is False

            # 3. 测试设置不存在的键的过期时间
            success = await db.expire("nonexistent_key", 60)
            assert success is False
    
    async def test_data_persistence(self, temp_db_dir):
        """测试数据持久化。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        db_name = "persistence_test"
        
        # 第一个会话：写入数据
        async with flaxkv.FlaxKV(db_name, "local", temp_db_dir, config) as db:
            await db.set("persistent_key", "persistent_value")
            await db.flush()  # 强制刷新
        
        # 第二个会话：读取数据
        async with flaxkv.FlaxKV(db_name, "local", temp_db_dir, config) as db:
            value = await db.get("persistent_key")
            assert value == "persistent_value"
    
    async def test_clear_operation(self, temp_db_dir):
        """测试清空操作。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("test_db", "local", temp_db_dir, config) as db:
            # 添加一些数据
            await db.mset({"key1": "value1", "key2": "value2", "key3": "value3"})
            assert await db.size() == 3
            
            # 清空数据
            await db.clear()
            assert await db.size() == 0
            
            # 验证键不存在
            assert await db.contains("key1") is False
    
    async def test_flush_operation(self, temp_db_dir):
        """测试刷新操作。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("test_db", "local", temp_db_dir, config) as db:
            # 添加数据
            await db.set("flush_key", "flush_value")
            
            # 刷新缓冲区
            flush_result = await db.flush()
            assert isinstance(flush_result, dict)
            assert "flushed_count" in flush_result
    
    async def test_backup_and_restore(self, temp_db_dir):
        """测试备份和恢复。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        backup_path = temp_db_dir / "backup.flaxkv"
        
        original_data = {
            "backup_key1": "backup_value1",
            "backup_key2": {"nested": "data"},
            "backup_key3": [1, 2, 3]
        }
        
        async with flaxkv.FlaxKV("test_db", "local", temp_db_dir, config) as db:
            # 添加数据
            await db.mset(original_data)
            
            # 备份
            backup_stats = await db.backup(str(backup_path))
            assert isinstance(backup_stats, dict)
            assert backup_stats["total_keys"] == len(original_data)
            
            # 清空数据
            await db.clear()
            assert await db.size() == 0
            
            # 恢复
            restore_stats = await db.restore(str(backup_path))
            assert isinstance(restore_stats, dict)
            assert restore_stats["restored_keys"] == len(original_data)
            
            # 验证恢复的数据
            for key, expected_value in original_data.items():
                actual_value = await db.get(key)
                assert actual_value == expected_value
    
    async def test_statistics(self, temp_db_dir):
        """测试统计功能。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        async with flaxkv.FlaxKV("test_db", "local", temp_db_dir, config) as db:
            # 执行一些操作
            await db.set("stats_key", "stats_value")
            await db.get("stats_key")
            await db.contains("stats_key")
            
            # 获取统计信息
            stats = db.get_stats()
            assert isinstance(stats, dict)
            assert stats["total_operations"] > 0
            
            # 获取详细信息
            info = db.get_info()
            assert isinstance(info, dict)
            assert info["name"] == "test_db"
            assert info["backend"] == "local"
            assert info["initialized"] is True
            assert info["closed"] is False


@pytest.mark.asyncio
class TestFlaxKVFactoryFunction:
    """测试FlaxKV工厂函数。"""
    
    async def test_create_flaxkv(self, temp_db_dir):
        """测试create_flaxkv工厂函数。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        db = await flaxkv.create_flaxkv("factory_test", "local", temp_db_dir, config)
        
        try:
            # 数据库应该已经初始化
            assert db._initialized is True
            
            # 测试基本操作
            await db.set("factory_key", "factory_value")
            value = await db.get("factory_key")
            assert value == "factory_value"
            
        finally:
            await db.close()


class TestFlaxKVSyncInterface:
    """测试FlaxKV同步接口（不推荐使用）。"""
    
    def test_sync_dict_operations(self, temp_db_dir):
        """测试同步字典操作。"""
        config = flaxkv.FlaxKVConfig.for_testing()
        
        # 使用同步接口而不是异步接口
        sync_db = flaxkv.create_sync_flaxkv("sync_test", "local", temp_db_dir, config)
        
        try:
            # 同步设置
            sync_db["sync_key"] = "sync_value"
            
            # 同步获取
            value = sync_db["sync_key"]
            assert value == "sync_value"
            
            # 同步包含检查
            assert "sync_key" in sync_db
            
            # 同步大小
            size = len(sync_db)
            assert size == 1
            
            # 同步删除
            del sync_db["sync_key"]
            assert "sync_key" not in sync_db
            
        finally:
            sync_db.close()