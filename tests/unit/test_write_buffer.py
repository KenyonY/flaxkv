"""
WriteBuffer 类的单元测试
"""

import pytest
import time
import threading
from flaxkv2.utils.write_buffer import WriteBuffer


class TestWriteBuffer:
    """WriteBuffer 基本功能测试"""

    def test_basic_put_get(self):
        """测试基本的 put 和 get 操作"""
        buffer = WriteBuffer(max_size=10, flush_interval=60, auto_flush=False)

        # 写入数据
        buffer.put('key1', 'value1', ttl=None)
        buffer.put('key2', 'value2', ttl=60)

        # 读取数据
        result1 = buffer.get('key1')
        result2 = buffer.get('key2')
        result3 = buffer.get('key3')

        assert result1 == ('value1', None)
        assert result2 == ('value2', 60)
        assert result3 is None

        buffer.stop()

    def test_delete_marker(self):
        """测试删除标记"""
        buffer = WriteBuffer(max_size=10, flush_interval=60, auto_flush=False)

        # 写入后删除
        buffer.put('key1', 'value1', ttl=None)
        buffer.delete('key1')

        # 检查删除标记
        result = buffer.get('key1')
        assert result is WriteBuffer._DELETED
        assert buffer.is_deleted('key1')

        buffer.stop()

    def test_size_threshold_flush(self):
        """测试大小阈值触发刷新"""
        flush_called = {'count': 0, 'writes': {}, 'deletes': set()}

        def callback(writes, deletes):
            flush_called['count'] += 1
            flush_called['writes'] = writes
            flush_called['deletes'] = deletes

        buffer = WriteBuffer(
            max_size=3,
            flush_interval=60,
            flush_callback=callback,
            auto_flush=False
        )

        # 写入3条数据，应该触发刷新
        buffer.put('key1', 'value1', ttl=None)
        buffer.put('key2', 'value2', ttl=None)
        buffer.put('key3', 'value3', ttl=None)

        assert flush_called['count'] == 1
        assert len(flush_called['writes']) == 3
        assert 'key1' in flush_called['writes']

        buffer.stop()

    def test_manual_flush(self):
        """测试手动刷新"""
        flush_called = {'count': 0, 'writes': {}, 'deletes': set()}

        def callback(writes, deletes):
            flush_called['count'] += 1
            flush_called['writes'] = writes.copy()
            flush_called['deletes'] = deletes.copy()

        buffer = WriteBuffer(
            max_size=100,
            flush_interval=60,
            flush_callback=callback,
            auto_flush=False
        )

        # 写入数据但不触发自动刷新
        buffer.put('key1', 'value1', ttl=None)
        buffer.put('key2', 'value2', ttl=None)

        assert flush_called['count'] == 0

        # 手动刷新
        buffer.flush()

        assert flush_called['count'] == 1
        assert len(flush_called['writes']) == 2

        buffer.stop()

    def test_auto_flush_timer(self):
        """测试定时自动刷新"""
        flush_called = {'count': 0}

        def callback(writes, deletes):
            flush_called['count'] += 1

        # 使用很短的刷新间隔
        buffer = WriteBuffer(
            max_size=100,
            flush_interval=1,  # 1秒
            flush_callback=callback,
            auto_flush=True
        )

        # 写入数据
        buffer.put('key1', 'value1', ttl=None)

        # 等待定时刷新
        time.sleep(1.5)

        assert flush_called['count'] >= 1

        buffer.stop()

    def test_stop_flush(self):
        """测试 stop() 会刷新剩余数据"""
        flush_called = {'count': 0, 'writes': {}}

        def callback(writes, deletes):
            flush_called['count'] += 1
            flush_called['writes'] = writes.copy()

        buffer = WriteBuffer(
            max_size=100,
            flush_interval=60,
            flush_callback=callback,
            auto_flush=False
        )

        # 写入数据
        buffer.put('key1', 'value1', ttl=None)
        buffer.put('key2', 'value2', ttl=None)

        # 停止前未刷新
        assert flush_called['count'] == 0

        # 停止应该触发刷新
        buffer.stop()

        assert flush_called['count'] == 1
        assert len(flush_called['writes']) == 2

    def test_overwrite_key(self):
        """测试覆盖写入"""
        buffer = WriteBuffer(max_size=10, flush_interval=60, auto_flush=False)

        # 写入后覆盖
        buffer.put('key1', 'value1', ttl=None)
        buffer.put('key1', 'value2', ttl=None)

        result = buffer.get('key1')
        assert result == ('value2', None)

        buffer.stop()

    def test_delete_then_put(self):
        """测试删除后再写入"""
        buffer = WriteBuffer(max_size=10, flush_interval=60, auto_flush=False)

        # 写入、删除、再写入
        buffer.put('key1', 'value1', ttl=None)
        buffer.delete('key1')
        buffer.put('key1', 'value3', ttl=None)

        result = buffer.get('key1')
        assert result == ('value3', None)
        assert not buffer.is_deleted('key1')

        buffer.stop()

    def test_thread_safety(self):
        """测试线程安全"""
        buffer = WriteBuffer(max_size=1000, flush_interval=60, auto_flush=False)

        def writer(start, end):
            for i in range(start, end):
                buffer.put(f'key_{i}', f'value_{i}', ttl=None)

        # 启动多个线程写入
        threads = []
        for i in range(5):
            t = threading.Thread(target=writer, args=(i*100, (i+1)*100))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # 检查数据完整性
        assert len(buffer._buffer_dict) == 500

        buffer.stop()

    def test_stats(self):
        """测试统计信息"""
        buffer = WriteBuffer(max_size=10, flush_interval=60, auto_flush=False)

        buffer.put('key1', 'value1', ttl=None)
        buffer.put('key2', 'value2', ttl=None)
        buffer.delete('key3')

        stats = buffer.stats()

        assert stats['buffered_writes'] == 2
        assert stats['buffered_deletes'] == 1
        assert stats['total_buffered'] == 3
        assert stats['max_size'] == 10

        buffer.stop()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
