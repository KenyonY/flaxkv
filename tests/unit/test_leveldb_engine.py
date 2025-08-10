"""
LevelDB存储引擎单元测试
"""

import pytest
import pytest_asyncio
import tempfile
import shutil
from pathlib import Path

from flaxkv.core.config import FlaxKVConfig
from flaxkv.storage.leveldb_engine import LevelDBEngine, LevelDBOptimizer


@pytest.fixture
def temp_db_path():
    """临时数据库路径。"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir) / "test_db"
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def test_config():
    """测试配置。"""
    return FlaxKVConfig.for_testing()


@pytest_asyncio.fixture
async def leveldb_engine(temp_db_path, test_config):
    """LevelDB引擎实例。"""
    engine = LevelDBEngine(str(temp_db_path), test_config)
    await engine.initialize()
    yield engine
    await engine.close()


@pytest.mark.asyncio
class TestLevelDBEngine:
    """测试LevelDB存储引擎。"""
    
    async def test_initialization(self, temp_db_path, test_config):
        """测试引擎初始化。"""
        engine = LevelDBEngine(str(temp_db_path), test_config)
        
        # 初始化前数据库应该是None
        assert engine.db is None
        
        await engine.initialize()
        
        # 初始化后数据库应该存在
        assert engine.db is not None
        assert engine._optimizer is not None
        
        await engine.close()
    
    async def test_basic_operations(self, leveldb_engine):
        """测试基本CRUD操作。"""
        # 设置键值对
        await leveldb_engine.set("test_key", "test_value")
        
        # 获取值
        value = await leveldb_engine.get("test_key")
        assert value == "test_value"
        
        # 检查键存在
        assert await leveldb_engine.contains("test_key") is True
        assert await leveldb_engine.contains("nonexistent_key") is False
        
        # 删除键
        deleted = await leveldb_engine.delete("test_key")
        assert deleted is True
        
        # 确认删除
        assert await leveldb_engine.contains("test_key") is False
        
        # 删除不存在的键
        deleted = await leveldb_engine.delete("nonexistent_key")
        assert deleted is False
    
    async def test_key_not_found(self, leveldb_engine):
        """测试键不存在的情况。"""
        with pytest.raises(KeyError):
            await leveldb_engine.get("nonexistent_key")
    
    async def test_data_types(self, leveldb_engine):
        """测试不同数据类型的存储。"""
        test_data = {
            "string": "hello",
            "integer": 42,
            "float": 3.14,
            "list": [1, 2, 3],
            "dict": {"nested": "value"},
            "boolean": True,
            "none": None,
        }
        
        # 存储不同类型的数据
        for key, value in test_data.items():
            await leveldb_engine.set(key, value)
        
        # 验证数据正确性
        for key, expected_value in test_data.items():
            actual_value = await leveldb_engine.get(key)
            assert actual_value == expected_value
    
    async def test_size_operation(self, leveldb_engine):
        """测试大小统计。"""
        # 初始大小应该是0
        assert await leveldb_engine.size() == 0
        
        # 添加几个键
        keys = ["key1", "key2", "key3"]
        for key in keys:
            await leveldb_engine.set(key, f"value_{key}")
        
        # 大小应该是3
        assert await leveldb_engine.size() == len(keys)
        
        # 删除一个键
        await leveldb_engine.delete("key1")
        assert await leveldb_engine.size() == len(keys) - 1
    
    async def test_batch_set_and_get(self, leveldb_engine):
        """测试批量设置和批量获取。"""
        items = {
            "batch_key1": "batch_value1",
            "batch_key2": 2,
            "batch_key3": [1, 2, 3],
        }
        await leveldb_engine.batch_set(items)

        # 批量获取全部存在的键
        keys = list(items.keys())
        result = await leveldb_engine.batch_get(keys)
        assert result == items

        # 批量获取部分存在的键
        keys_with_missing = ["batch_key1", "missing_key", "batch_key3"]
        result = await leveldb_engine.batch_get(keys_with_missing)
        assert len(result) == 2
        assert result["batch_key1"] == "batch_value1"
        assert result["batch_key3"] == [1, 2, 3]
        assert "missing_key" not in result

    async def test_batch_delete(self, leveldb_engine):
        """测试批量删除。"""
        items = {"key1": "v1", "key2": "v2", "key3": "v3"}
        await leveldb_engine.batch_set(items)

        # 删除部分存在的键
        deleted_count = await leveldb_engine.batch_delete(["key1", "key3", "key4"])
        assert deleted_count == 2
        assert await leveldb_engine.contains("key1") is False
        assert await leveldb_engine.contains("key2") is True
        assert await leveldb_engine.contains("key3") is False

        # 再次删除，应该没有键被删除
        deleted_count = await leveldb_engine.batch_delete(["key1", "key3"])
        assert deleted_count == 0

    async def test_scan_operations(self, leveldb_engine):
        """测试扫描操作。"""
        test_data = {
            "user:001": {"name": "Alice"},
            "user:002": {"name": "Bob"},
            "user:003": {"name": "Charlie"},
            "product:001": {"name": "Book"},
            "product:002": {"name": "Pen"},
        }
        await leveldb_engine.batch_set(test_data)

        # 前缀扫描
        user_items = [item async for item in leveldb_engine.scan(prefix="user:")]
        assert len(user_items) == 3
        assert user_items[0] == ("user:001", {"name": "Alice"})
        assert user_items[2] == ("user:003", {"name": "Charlie"})

        # 范围扫描（注意字典序：product 在 user 之前）
        range_items = [item async for item in leveldb_engine.scan(start="product:001", end="user:002")]
        assert len(range_items) == 3  # product:001, product:002, user:001
        assert range_items[0][0] == "product:001"
        assert range_items[1][0] == "product:002"
        assert range_items[2][0] == "user:001"

        # 限制数量扫描
        limited_items = [item async for item in leveldb_engine.scan(limit=2)]
        assert len(limited_items) == 2
        assert limited_items[0][0] == "product:001" # Default order is lexicographical
        assert limited_items[1][0] == "product:002"

    async def test_scan_with_nonexistent_prefix(self, leveldb_engine):
        """测试使用不存在的前缀进行扫描。"""
        items = [item async for item in leveldb_engine.scan(prefix="nonexistent:")]
        assert len(items) == 0
    
    async def test_iterate_all(self, leveldb_engine):
        """测试迭代所有项。"""
        # 添加测试数据
        test_items = {f"item_{i}": f"value_{i}" for i in range(5)}
        await leveldb_engine.batch_set(test_items)
        
        # 迭代所有项
        all_items = {}
        async for key, value in leveldb_engine._iterate_all():
            all_items[key] = value
        
        assert len(all_items) == len(test_items)
        for key, value in test_items.items():
            assert all_items[key] == value
    
    async def test_compact_range(self, leveldb_engine):
        """测试范围压缩。"""
        # 添加一些数据
        for i in range(100):
            await leveldb_engine.set(f"key_{i:03d}", f"value_{i}")
        
        # 压缩范围（应该不抛出异常）
        await leveldb_engine.compact_range("key_000", "key_050")
        await leveldb_engine.compact_range()  # 压缩全部
    
    async def test_leveldb_stats(self, leveldb_engine):
        """测试LevelDB统计信息。"""
        # 添加一些数据
        for i in range(10):
            await leveldb_engine.set(f"stats_key_{i}", f"stats_value_{i}")
        
        # 获取统计信息
        stats = leveldb_engine.get_leveldb_stats()
        
        assert isinstance(stats, dict)
        # 统计信息中应该包含我们的计数
        assert 'compactions' in stats
    
    async def test_error_handling(self, leveldb_engine):
        """测试错误处理。"""
        # 关闭数据库后尝试操作
        await leveldb_engine.close()
        
        with pytest.raises(Exception):  # 应该抛出某种异常
            await leveldb_engine.get("test_key")
    
    async def test_context_manager(self, temp_db_path, test_config):
        """测试上下文管理器。"""
        async with LevelDBEngine(str(temp_db_path), test_config) as engine:
            await engine.set("context_key", "context_value")
            value = await engine.get("context_key")
            assert value == "context_value"
        
        # 上下文管理器退出后，数据库应该被关闭
        assert engine.is_closed()


@pytest.mark.asyncio  
class TestLevelDBOptimizer:
    """测试LevelDB优化器。"""
    
    async def test_optimizer_creation(self, test_config):
        """测试优化器创建。"""
        # 由于需要真实的数据库连接，这里只测试配置设置
        fake_db = None
        optimizer = LevelDBOptimizer(fake_db, test_config)
        
        assert optimizer.config == test_config
        assert optimizer.encoder is not None
        assert optimizer.decoder is not None
    
    async def test_value_encoding_decoding(self, test_config):
        """测试值编码解码。"""
        optimizer = LevelDBOptimizer(None, test_config)
        
        test_values = [
            "string",
            42,
            3.14,
            [1, 2, 3],
            {"key": "value"},
            True,
            None,
        ]
        
        for original_value in test_values:
            # 编码
            encoded = optimizer.encode_value(original_value)
            assert isinstance(encoded, bytes)
            
            # 解码
            decoded = optimizer.decode_value(encoded)
            assert decoded == original_value
    
    async def test_optimization_suggestions(self, test_config):
        """测试优化建议。"""
        optimizer = LevelDBOptimizer(None, test_config)
        suggestions = optimizer.get_optimization_suggestions()
        
        assert isinstance(suggestions, list)
        # 测试配置有小缓存，应该有相关建议
        cache_suggestions = [s for s in suggestions if "缓存大小" in s]
        assert len(cache_suggestions) > 0