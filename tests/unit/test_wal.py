"""
WAL (Write-Ahead Log) 功能单元测试
"""

import pytest
import tempfile
import shutil
import os
import asyncio
import pickle
from pathlib import Path

from flaxkv.buffer.wal import WriteAheadLog
from flaxkv.core.config import FlaxKVConfig


@pytest.fixture
def temp_wal_dir():
    """临时WAL目录。"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def test_config():
    """测试配置。"""
    return FlaxKVConfig.for_testing()


@pytest.fixture
async def wal(temp_wal_dir, test_config):
    """WAL实例。"""
    wal_path = temp_wal_dir / "test.wal"
    wal = WriteAheadLog(str(wal_path), test_config)
    await wal.initialize()
    yield wal
    await wal.close()


class TestWALBasic:
    """测试WAL基本功能。"""
    
    async def test_initialization(self, temp_wal_dir, test_config):
        """测试WAL初始化。"""
        wal_path = temp_wal_dir / "init_test.wal"
        wal = WriteAheadLog(str(wal_path), test_config)
        
        # 初始化前文件不存在
        assert not wal_path.exists()
        
        await wal.initialize()
        
        # 初始化后文件应该存在
        assert wal_path.exists()
        assert wal._wal_file is not None
        assert wal._sequence_number == 0
        
        await wal.close()
    
    async def test_directory_creation(self, temp_wal_dir, test_config):
        """测试目录自动创建。"""
        nested_path = temp_wal_dir / "nested" / "dir" / "test.wal"
        wal = WriteAheadLog(str(nested_path), test_config)
        
        # 目录不存在
        assert not nested_path.parent.exists()
        
        await wal.initialize()
        
        # 目录应该被创建
        assert nested_path.parent.exists()
        assert nested_path.exists()
        
        await wal.close()


class TestWALOperations:
    """测试WAL操作。"""
    
    async def test_append_single_record(self, wal):
        """测试追加单条记录。"""
        seq_num = await wal.append("test_key", "test_value", "set")
        
        assert seq_num == 1
        assert wal._sequence_number == 1
        
        # 验证文件内容
        records = await wal.read_all_records()
        assert len(records) == 1
        assert records[0]['sequence'] == 1
        assert records[0]['key'] == "test_key"
        assert records[0]['value'] == "test_value"
        assert records[0]['operation'] == "set"
    
    async def test_append_multiple_records(self, wal):
        """测试追加多条记录。"""
        operations = [
            ("key1", "value1", "set"),
            ("key2", "value2", "set"),
            ("key1", None, "delete"),
            ("key3", {"nested": "data"}, "set"),
        ]
        
        for i, (key, value, op) in enumerate(operations, 1):
            seq_num = await wal.append(key, value, op)
            assert seq_num == i
        
        assert wal._sequence_number == len(operations)
        
        # 验证所有记录
        records = await wal.read_all_records()
        assert len(records) == len(operations)
        
        for i, (expected_key, expected_value, expected_op) in enumerate(operations):
            record = records[i]
            assert record['sequence'] == i + 1
            assert record['key'] == expected_key
            assert record['value'] == expected_value
            assert record['operation'] == expected_op
    
    async def test_append_different_data_types(self, wal):
        """测试追加不同数据类型。"""
        test_data = [
            ("string_key", "string_value"),
            ("int_key", 42),
            ("float_key", 3.14),
            ("list_key", [1, 2, 3]),
            ("dict_key", {"nested": "data"}),
            ("bool_key", True),
            ("none_key", None),
        ]
        
        for key, value in test_data:
            await wal.append(key, value, "set")
        
        # 验证数据类型保持正确
        records = await wal.read_all_records()
        for i, (expected_key, expected_value) in enumerate(test_data):
            record = records[i]
            assert record['key'] == expected_key
            assert record['value'] == expected_value
            assert type(record['value']) == type(expected_value)
    
    async def test_concurrent_appends(self, wal):
        """测试并发追加操作。"""
        async def append_task(task_id: int, count: int):
            for i in range(count):
                await wal.append(f"task_{task_id}_key_{i}", f"value_{i}", "set")
        
        # 并发运行多个追加任务
        tasks = [append_task(task_id, 10) for task_id in range(3)]
        await asyncio.gather(*tasks)
        
        # 验证所有记录都被正确写入
        records = await wal.read_all_records()
        assert len(records) == 30  # 3个任务 * 每个10条记录
        
        # 验证序列号的唯一性和连续性
        sequences = [record['sequence'] for record in records]
        assert len(set(sequences)) == 30  # 所有序列号都是唯一的
        assert sequences == list(range(1, 31))  # 序列号连续


class TestWALRecovery:
    """测试WAL恢复功能。"""
    
    async def test_recovery_from_existing_wal(self, temp_wal_dir, test_config):
        """测试从现有WAL文件恢复。"""
        wal_path = temp_wal_dir / "recovery_test.wal"
        
        # 第一个WAL实例：写入一些数据
        wal1 = WriteAheadLog(str(wal_path), test_config)
        await wal1.initialize()
        
        await wal1.append("key1", "value1", "set")
        await wal1.append("key2", "value2", "set")
        await wal1.append("key1", "updated_value", "set")
        
        await wal1.close()
        
        # 第二个WAL实例：恢复并继续
        wal2 = WriteAheadLog(str(wal_path), test_config)
        await wal2.initialize()
        
        # 序列号应该恢复到正确值
        assert wal2._sequence_number == 3
        
        # 继续追加新记录
        seq_num = await wal2.append("key3", "value3", "set")
        assert seq_num == 4
        
        # 验证所有记录
        records = await wal2.read_all_records()
        assert len(records) == 4
        assert records[3]['sequence'] == 4
        assert records[3]['key'] == "key3"
        
        await wal2.close()
    
    async def test_replay_operations(self, wal):
        """测试操作重放。"""
        # 写入一系列操作
        operations = [
            ("key1", "initial_value", "set"),
            ("key2", "value2", "set"),
            ("key1", "updated_value", "set"),
            ("key3", [1, 2, 3], "set"),
            ("key2", None, "delete"),
        ]
        
        for key, value, op in operations:
            await wal.append(key, value, op)
        
        # 模拟重放操作
        final_state = {}
        records = await wal.read_all_records()
        
        for record in records:
            key = record['key']
            value = record['value']
            operation = record['operation']
            
            if operation == "set":
                final_state[key] = value
            elif operation == "delete":
                final_state.pop(key, None)
        
        # 验证最终状态
        expected_state = {
            "key1": "updated_value",
            "key3": [1, 2, 3]
        }
        assert final_state == expected_state
    
    async def test_corruption_handling(self, temp_wal_dir, test_config):
        """测试WAL文件损坏处理。"""
        wal_path = temp_wal_dir / "corruption_test.wal"
        
        # 创建有效的WAL
        wal = WriteAheadLog(str(wal_path), test_config)
        await wal.initialize()
        
        await wal.append("key1", "value1", "set")
        await wal.append("key2", "value2", "set")
        await wal.close()
        
        # 模拟文件损坏：在文件末尾添加无效数据
        with open(wal_path, 'ab') as f:
            f.write(b'corrupted_data_here')
        
        # 重新打开WAL并尝试读取
        wal2 = WriteAheadLog(str(wal_path), test_config)
        await wal2.initialize()
        
        # 应该能够读取有效记录，忽略损坏部分
        records = await wal2.read_all_records()
        assert len(records) == 2
        assert records[0]['key'] == "key1"
        assert records[1]['key'] == "key2"
        
        await wal2.close()


class TestWALMaintenance:
    """测试WAL维护功能。"""
    
    async def test_checkpoint(self, wal):
        """测试检查点功能。"""
        # 写入一些数据
        for i in range(10):
            await wal.append(f"key_{i}", f"value_{i}", "set")
        
        # 创建检查点
        checkpoint_info = await wal.checkpoint()
        
        assert isinstance(checkpoint_info, dict)
        assert checkpoint_info['records_before_checkpoint'] == 10
        assert 'checkpoint_time' in checkpoint_info
        
        # 检查点后继续写入
        await wal.append("after_checkpoint", "value", "set")
        
        # 验证记录数量
        records = await wal.read_all_records()
        assert len(records) == 11
    
    async def test_truncate(self, wal):
        """测试截断功能。"""
        # 写入数据
        for i in range(10):
            await wal.append(f"key_{i}", f"value_{i}", "set")
        
        original_size = wal.wal_path.stat().st_size
        assert original_size > 0
        
        # 截断到序列号5
        await wal.truncate(5)
        
        # 验证文件变小了
        new_size = wal.wal_path.stat().st_size
        assert new_size < original_size
        
        # 验证只有前5条记录
        records = await wal.read_all_records()
        assert len(records) == 5
        assert records[-1]['sequence'] == 5
    
    async def test_compact(self, wal):
        """测试压缩功能。"""
        # 写入大量数据，包括一些删除操作
        for i in range(20):
            await wal.append(f"key_{i}", f"value_{i}", "set")
        
        # 删除一些键
        for i in range(0, 10, 2):  # 删除偶数索引的键
            await wal.append(f"key_{i}", None, "delete")
        
        original_records = await wal.read_all_records()
        original_count = len(original_records)
        
        # 压缩WAL
        compact_stats = await wal.compact()
        
        assert isinstance(compact_stats, dict)
        assert compact_stats['original_records'] == original_count
        assert compact_stats['compacted_records'] < original_count
        
        # 验证压缩后的数据完整性
        compacted_records = await wal.read_all_records()
        
        # 应用所有操作得到最终状态
        final_state = {}
        for record in compacted_records:
            key = record['key']
            if record['operation'] == 'set':
                final_state[key] = record['value']
            elif record['operation'] == 'delete':
                final_state.pop(key, None)
        
        # 验证最终状态包含预期的键
        expected_keys = {f"key_{i}" for i in range(1, 20, 2)}  # 奇数索引的键
        assert set(final_state.keys()) == expected_keys


class TestWALStatistics:
    """测试WAL统计功能。"""
    
    async def test_get_stats(self, wal):
        """测试获取统计信息。"""
        # 写入一些数据
        for i in range(5):
            await wal.append(f"key_{i}", f"value_{i}", "set")
        
        await wal.append("key_0", None, "delete")  # 删除操作
        
        stats = await wal.get_stats()
        
        assert isinstance(stats, dict)
        assert stats['total_records'] == 6
        assert stats['set_operations'] == 5
        assert stats['delete_operations'] == 1
        assert stats['file_size'] > 0
        assert 'last_sequence_number' in stats
    
    async def test_record_count(self, wal):
        """测试记录计数。"""
        assert await wal.get_record_count() == 0
        
        for i in range(3):
            await wal.append(f"key_{i}", f"value_{i}", "set")
            assert await wal.get_record_count() == i + 1


class TestWALErrorHandling:
    """测试WAL错误处理。"""
    
    async def test_initialization_errors(self, temp_wal_dir, test_config):
        """测试初始化错误处理。"""
        # 使用只读目录
        readonly_dir = temp_wal_dir / "readonly"
        readonly_dir.mkdir()
        readonly_dir.chmod(0o444)  # 只读权限
        
        try:
            wal_path = readonly_dir / "test.wal"
            wal = WriteAheadLog(str(wal_path), test_config)
            
            # 初始化应该失败
            with pytest.raises(Exception):
                await wal.initialize()
        finally:
            # 恢复权限以便清理
            readonly_dir.chmod(0o755)
    
    async def test_append_errors(self, wal):
        """测试追加操作错误处理。"""
        # 关闭WAL文件句柄
        if wal._wal_file:
            wal._wal_file.close()
            wal._wal_file = None
        
        # 尝试追加应该失败
        with pytest.raises(Exception):
            await wal.append("key", "value", "set")
    
    async def test_close_multiple_times(self, wal):
        """测试多次关闭。"""
        # 第一次关闭
        await wal.close()
        
        # 多次关闭不应该抛出异常
        await wal.close()
        await wal.close()
        
        # 关闭后不能进行操作
        with pytest.raises(Exception):
            await wal.append("key", "value", "set")


class TestWALConfiguration:
    """测试WAL配置相关功能。"""
    
    async def test_buffer_size_config(self, temp_wal_dir):
        """测试缓冲区大小配置。"""
        config = FlaxKVConfig(
            buffer_size=1000,
            buffer_timeout=1.0
        )
        
        wal_path = temp_wal_dir / "buffer_test.wal"
        wal = WriteAheadLog(str(wal_path), config)
        await wal.initialize()
        
        # 写入数据应该使用配置的缓冲区大小
        for i in range(10):
            await wal.append(f"key_{i}", f"value_{i}", "set")
        
        stats = await wal.get_stats()
        assert stats['total_records'] == 10
        
        await wal.close()
    
    async def test_sync_mode_config(self, temp_wal_dir):
        """测试同步模式配置。"""
        # 异步模式
        async_config = FlaxKVConfig(sync_mode="async")
        wal_path = temp_wal_dir / "async_test.wal"
        wal = WriteAheadLog(str(wal_path), async_config)
        await wal.initialize()
        
        await wal.append("async_key", "async_value", "set")
        await wal.close()
        
        # 同步模式
        sync_config = FlaxKVConfig(sync_mode="sync")
        wal_path = temp_wal_dir / "sync_test.wal"
        wal = WriteAheadLog(str(wal_path), sync_config)
        await wal.initialize()
        
        await wal.append("sync_key", "sync_value", "set")
        await wal.close()


class TestWALIntegration:
    """测试WAL集成场景。"""
    
    async def test_restart_recovery_scenario(self, temp_wal_dir, test_config):
        """测试重启恢复场景。"""
        wal_path = temp_wal_dir / "restart_test.wal"
        
        # 模拟正常运行：写入数据
        wal1 = WriteAheadLog(str(wal_path), test_config)
        await wal1.initialize()
        
        operations = [
            ("user:1", {"name": "Alice", "age": 30}, "set"),
            ("user:2", {"name": "Bob", "age": 25}, "set"),
            ("config:theme", "dark", "set"),
            ("user:1", {"name": "Alice", "age": 31}, "set"),  # 更新
        ]
        
        for key, value, op in operations:
            await wal1.append(key, value, op)
        
        await wal1.close()
        
        # 模拟重启：新的WAL实例从同一文件恢复
        wal2 = WriteAheadLog(str(wal_path), test_config)
        await wal2.initialize()
        
        # 验证恢复的序列号
        assert wal2._sequence_number == 4
        
        # 读取所有记录并重放
        records = await wal2.read_all_records()
        assert len(records) == 4
        
        # 重放操作
        state = {}
        for record in records:
            if record['operation'] == 'set':
                state[record['key']] = record['value']
        
        # 验证最终状态
        expected_state = {
            "user:1": {"name": "Alice", "age": 31},
            "user:2": {"name": "Bob", "age": 25},
            "config:theme": "dark",
        }
        assert state == expected_state
        
        # 继续写入新数据
        await wal2.append("user:3", {"name": "Charlie", "age": 28}, "set")
        assert wal2._sequence_number == 5
        
        await wal2.close()
    
    async def test_large_data_handling(self, wal):
        """测试大数据处理。"""
        # 创建大数据对象
        large_data = {
            "data": "x" * 10000,  # 10KB字符串
            "list": list(range(1000)),
            "nested": {f"key_{i}": f"value_{i}" for i in range(100)}
        }
        
        # 写入大数据
        await wal.append("large_key", large_data, "set")
        
        # 读取并验证
        records = await wal.read_all_records()
        assert len(records) == 1
        assert records[0]['value'] == large_data
        
        # 验证数据完整性
        recovered_data = records[0]['value']
        assert len(recovered_data['data']) == 10000
        assert len(recovered_data['list']) == 1000
        assert len(recovered_data['nested']) == 100