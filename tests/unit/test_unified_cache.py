"""
UnifiedCache核心功能单元测试

测试unified_cache.py中的UnifiedCache类的核心功能：
- 基本的put/get/delete操作
- LRU淘汰机制（包括dirty数据淘汰）
- TTL过期处理
- Flush机制（同步/异步/定时）
- 线程安全性
- 边界条件
"""

import pytest
import time
import threading
from collections import Counter
from flaxkv2.utils.unified_cache import UnifiedCache, CacheEntry


class TestCacheEntry:
    """测试CacheEntry类"""

    def test_cache_entry_creation(self):
        """测试创建缓存条目"""
        entry = CacheEntry('value1', expire_time=None, dirty=True)
        assert entry.value == 'value1'
        assert entry.expire_time is None
        assert entry.dirty is True
        assert entry.timestamp > 0

    def test_cache_entry_no_ttl(self):
        """测试无TTL的条目不会过期"""
        entry = CacheEntry('value1', expire_time=None, dirty=False)
        assert not entry.is_expired()

        # 等待一小段时间后仍不过期
        time.sleep(0.1)
        assert not entry.is_expired()

    def test_cache_entry_with_ttl(self):
        """测试有TTL的条目会过期"""
        # 创建1秒后过期的条目
        expire_time = time.time() + 1.0
        entry = CacheEntry('value1', expire_time=expire_time, dirty=False)

        # 立即检查，不应该过期
        assert not entry.is_expired()

        # 等待1.1秒后应该过期
        time.sleep(1.1)
        assert entry.is_expired()


class TestUnifiedCacheBasic:
    """测试UnifiedCache基本功能"""

    def test_cache_initialization(self):
        """测试缓存初始化"""
        cache = UnifiedCache(
            maxsize=100,
            flush_threshold=10,
            flush_interval=60,
            auto_flush=False  # 禁用自动flush以简化测试
        )

        assert cache._maxsize == 100
        assert cache._flush_threshold == 10
        assert cache._flush_interval == 60
        assert len(cache) == 0

        cache.stop()

    def test_invalid_parameters(self):
        """测试无效参数"""
        # maxsize <= 0
        with pytest.raises(ValueError, match="maxsize must be > 0"):
            UnifiedCache(maxsize=0)

        # flush_threshold <= 0
        with pytest.raises(ValueError, match="flush_threshold must be > 0"):
            UnifiedCache(flush_threshold=0)

        # flush_interval <= 0
        with pytest.raises(ValueError, match="flush_interval must be > 0"):
            UnifiedCache(flush_interval=0)

    def test_basic_put_get(self):
        """测试基本的put/get操作"""
        cache = UnifiedCache(maxsize=10, auto_flush=False)

        # Put操作
        cache.put('key1', 'value1', dirty=False)
        cache.put('key2', 'value2', dirty=False)

        # Get操作
        assert cache.get('key1') == 'value1'
        assert cache.get('key2') == 'value2'
        assert cache.get('key3') is None
        assert cache.get('key3', 'default') == 'default'

        assert len(cache) == 2
        cache.stop()

    def test_put_overwrite(self):
        """测试覆盖写入"""
        cache = UnifiedCache(maxsize=10, auto_flush=False)

        # 写入原值
        cache.put('key1', 'value1', dirty=False)
        assert cache.get('key1') == 'value1'

        # 覆盖
        cache.put('key1', 'value2', dirty=False)
        assert cache.get('key1') == 'value2'

        # 缓存大小应该仍然是1
        assert len(cache) == 1
        cache.stop()

    def test_delete(self):
        """测试删除操作"""
        cache = UnifiedCache(maxsize=10, auto_flush=False)

        # 写入数据
        cache.put('key1', 'value1', dirty=False)
        cache.put('key2', 'value2', dirty=False)
        assert len(cache) == 2

        # 删除key1
        cache.delete('key1')
        assert cache.get('key1') is None
        assert cache.get('key2') == 'value2'
        assert len(cache) == 1

        # 验证删除标记
        assert 'key1' in cache._delete_keys

        cache.stop()

    def test_delete_nonexistent_key(self):
        """测试删除不存在的键"""
        cache = UnifiedCache(maxsize=10, auto_flush=False)

        # 删除不存在的键不应该报错
        cache.delete('nonexistent')
        assert 'nonexistent' in cache._delete_keys

        cache.stop()

    def test_clear(self):
        """测试清空缓存"""
        cache = UnifiedCache(maxsize=10, auto_flush=False)

        # 写入数据
        cache.put('key1', 'value1', dirty=True)
        cache.put('key2', 'value2', dirty=True)
        assert len(cache) == 2
        assert len(cache._dirty_keys) == 2

        # 清空
        cache.clear()
        assert len(cache) == 0
        assert len(cache._dirty_keys) == 0
        assert len(cache._delete_keys) == 0

        cache.stop()

    def test_contains(self):
        """测试__contains__操作"""
        cache = UnifiedCache(maxsize=10, auto_flush=False)

        cache.put('key1', 'value1', dirty=False)

        # 注意：UnifiedCache的__contains__实现是检查get(key) is not None
        # 但这不是标准的contains行为
        assert 'key1' in cache
        assert 'key2' not in cache

        cache.stop()


class TestUnifiedCacheTTL:
    """测试UnifiedCache的TTL功能"""

    def test_put_with_ttl(self):
        """测试写入带TTL的数据"""
        cache = UnifiedCache(maxsize=10, auto_flush=False)

        # 写入2秒TTL
        cache.put('key1', 'value1', ttl=2, dirty=False)

        # 立即读取应该成功
        assert cache.get('key1') == 'value1'

        # 等待2.1秒后应该过期
        time.sleep(2.1)
        assert cache.get('key1') is None

        # 过期后缓存应该自动清除
        assert len(cache) == 0

        cache.stop()

    def test_ttl_no_expire(self):
        """测试无TTL的数据不会过期"""
        cache = UnifiedCache(maxsize=10, auto_flush=False)

        # 写入无TTL
        cache.put('key1', 'value1', ttl=None, dirty=False)

        # 等待后仍然存在
        time.sleep(0.5)
        assert cache.get('key1') == 'value1'

        cache.stop()

    def test_ttl_update(self):
        """测试更新TTL"""
        cache = UnifiedCache(maxsize=10, auto_flush=False)

        # 写入1秒TTL
        cache.put('key1', 'value1', ttl=1, dirty=False)
        time.sleep(0.5)

        # 更新为10秒TTL
        cache.put('key1', 'value2', ttl=10, dirty=False)
        time.sleep(0.7)  # 原来的TTL应该已经过期

        # 但由于更新了TTL，应该仍然有效
        assert cache.get('key1') == 'value2'

        cache.stop()


class TestUnifiedCacheLRU:
    """测试UnifiedCache的LRU淘汰机制"""

    def test_lru_eviction_basic(self):
        """测试基本的LRU淘汰"""
        cache = UnifiedCache(maxsize=3, auto_flush=False)

        # 写入3个键（maxsize=3）
        cache.put('key1', 'value1', dirty=False)
        cache.put('key2', 'value2', dirty=False)
        cache.put('key3', 'value3', dirty=False)
        assert len(cache) == 3

        # 写入第4个键，应该淘汰key1（最旧的）
        cache.put('key4', 'value4', dirty=False)
        assert len(cache) == 3
        assert cache.get('key1') is None  # key1被淘汰
        assert cache.get('key2') == 'value2'
        assert cache.get('key3') == 'value3'
        assert cache.get('key4') == 'value4'

        cache.stop()

    def test_lru_eviction_access_order(self):
        """测试LRU淘汰时考虑访问顺序"""
        cache = UnifiedCache(maxsize=3, auto_flush=False)

        # 写入3个键
        cache.put('key1', 'value1', dirty=False)
        cache.put('key2', 'value2', dirty=False)
        cache.put('key3', 'value3', dirty=False)

        # 访问key1（移到末尾）
        _ = cache.get('key1')

        # 写入key4，应该淘汰key2（现在最旧的）
        cache.put('key4', 'value4', dirty=False)
        assert cache.get('key1') == 'value1'  # key1保留（最近访问）
        assert cache.get('key2') is None       # key2被淘汰
        assert cache.get('key3') == 'value3'
        assert cache.get('key4') == 'value4'

        cache.stop()

    def test_lru_eviction_dirty_data(self):
        """测试LRU淘汰dirty数据时会先flush"""
        flush_called = []

        def mock_flush(writes, deletes):
            flush_called.append({'writes': writes, 'deletes': deletes})

        cache = UnifiedCache(
            maxsize=2,
            flush_threshold=100,  # 高阈值，避免自动flush
            auto_flush=False,
            flush_callback=mock_flush
        )

        # 写入2个dirty键
        cache.put('key1', 'value1', dirty=True)
        cache.put('key2', 'value2', dirty=True)
        assert len(cache._dirty_keys) == 2

        # 写入第3个键，应该淘汰key1并flush它
        cache.put('key3', 'value3', dirty=False)

        # 验证flush被调用，且包含key1
        assert len(flush_called) == 1
        assert 'key1' in flush_called[0]['writes']
        assert flush_called[0]['writes']['key1'][0] == 'value1'

        # key1应该从dirty_keys中移除
        assert 'key1' not in cache._dirty_keys
        assert len(cache._dirty_keys) == 1  # 只剩key2

        cache.stop()


class TestUnifiedCacheFlush:
    """测试UnifiedCache的flush机制"""

    def test_dirty_tracking(self):
        """测试dirty标记追踪"""
        cache = UnifiedCache(maxsize=10, auto_flush=False)

        # dirty=True时应该加入dirty_keys
        cache.put('key1', 'value1', dirty=True)
        assert 'key1' in cache._dirty_keys

        # dirty=False时不应该加入dirty_keys
        cache.put('key2', 'value2', dirty=False)
        assert 'key2' not in cache._dirty_keys

        assert len(cache._dirty_keys) == 1
        cache.stop()

    def test_manual_flush(self):
        """测试手动flush"""
        flushed_data = []

        def mock_flush(writes, deletes):
            flushed_data.append({'writes': writes, 'deletes': deletes})

        cache = UnifiedCache(
            maxsize=10,
            flush_threshold=100,
            auto_flush=False,
            flush_callback=mock_flush
        )

        # 写入dirty数据
        cache.put('key1', 'value1', dirty=True)
        cache.put('key2', 'value2', dirty=True)
        cache.delete('key3')

        assert len(cache._dirty_keys) == 2
        assert len(cache._delete_keys) == 1

        # 手动flush
        cache.flush()

        # 验证flush被调用
        assert len(flushed_data) == 1
        assert len(flushed_data[0]['writes']) == 2
        assert 'key1' in flushed_data[0]['writes']
        assert 'key2' in flushed_data[0]['writes']
        assert 'key3' in flushed_data[0]['deletes']

        # dirty和delete标记应该被清除
        assert len(cache._dirty_keys) == 0
        assert len(cache._delete_keys) == 0

        cache.stop()

    def test_flush_threshold_trigger(self):
        """测试达到阈值时自动flush"""
        flushed_data = []

        def mock_flush(writes, deletes):
            flushed_data.append({'writes': writes, 'deletes': deletes})

        cache = UnifiedCache(
            maxsize=100,
            flush_threshold=3,  # 3个dirty条目触发flush
            auto_flush=False,
            flush_callback=mock_flush
        )

        # 写入2个dirty数据，不应该flush
        cache.put('key1', 'value1', dirty=True)
        cache.put('key2', 'value2', dirty=True)
        assert len(flushed_data) == 0

        # 写入第3个，应该触发flush
        cache.put('key3', 'value3', dirty=True)
        assert len(flushed_data) == 1
        assert len(flushed_data[0]['writes']) == 3

        cache.stop()

    def test_flush_skip_expired(self):
        """测试flush时跳过已过期的数据"""
        flushed_data = []

        def mock_flush(writes, deletes):
            flushed_data.append({'writes': writes, 'deletes': deletes})

        cache = UnifiedCache(
            maxsize=10,
            flush_threshold=100,
            auto_flush=False,
            flush_callback=mock_flush
        )

        # 写入一个很快过期的数据
        cache.put('key1', 'value1', ttl=1, dirty=True)
        cache.put('key2', 'value2', dirty=True)

        # 等待key1过期
        time.sleep(1.1)

        # Flush
        cache.flush()

        # 验证只flush了key2
        assert len(flushed_data) == 1
        assert 'key1' not in flushed_data[0]['writes']
        assert 'key2' in flushed_data[0]['writes']

        cache.stop()

    def test_flush_with_ttl_remaining(self):
        """测试flush时保留剩余TTL"""
        flushed_data = []

        def mock_flush(writes, deletes):
            flushed_data.append({'writes': writes, 'deletes': deletes})

        cache = UnifiedCache(
            maxsize=10,
            flush_threshold=100,
            auto_flush=False,
            flush_callback=mock_flush
        )

        # 写入带TTL的数据
        cache.put('key1', 'value1', ttl=10, dirty=True)

        # 立即flush
        cache.flush()

        # 验证TTL被传递（应该接近10秒）
        assert len(flushed_data) == 1
        ttl = flushed_data[0]['writes']['key1'][1]
        assert ttl is not None
        assert 9 <= ttl <= 10  # 允许一些时间误差

        cache.stop()

    def test_empty_flush(self):
        """测试空flush（没有dirty数据）"""
        flushed_data = []

        def mock_flush(writes, deletes):
            flushed_data.append({'writes': writes, 'deletes': deletes})

        cache = UnifiedCache(
            maxsize=10,
            auto_flush=False,
            flush_callback=mock_flush
        )

        # Flush空缓存，不应该调用callback
        cache.flush()
        assert len(flushed_data) == 0

        cache.stop()


class TestUnifiedCacheAutoFlush:
    """测试UnifiedCache的自动flush功能"""

    def test_auto_flush_disabled(self):
        """测试禁用自动flush"""
        cache = UnifiedCache(maxsize=10, auto_flush=False)

        # 验证flush线程没有启动
        assert cache._flush_thread is None

        cache.stop()

    def test_auto_flush_enabled(self):
        """测试启用自动flush"""
        cache = UnifiedCache(
            maxsize=10,
            flush_interval=60,
            auto_flush=True
        )

        # 验证flush线程已启动
        assert cache._flush_thread is not None
        assert cache._flush_thread.is_alive()

        # 保存线程引用（stop()会将其设为None）
        flush_thread = cache._flush_thread

        cache.stop()

        # 验证线程已停止
        time.sleep(0.1)
        assert not flush_thread.is_alive()

    def test_timed_flush(self):
        """测试定时flush"""
        flushed_data = []

        def mock_flush(writes, deletes):
            flushed_data.append({'writes': writes, 'deletes': deletes})

        cache = UnifiedCache(
            maxsize=10,
            flush_threshold=100,  # 高阈值，避免阈值触发
            flush_interval=1,     # 1秒触发
            auto_flush=True,
            flush_callback=mock_flush
        )

        # 写入dirty数据
        cache.put('key1', 'value1', dirty=True)

        # 等待自动flush触发
        time.sleep(1.5)

        # 验证flush被调用
        assert len(flushed_data) >= 1
        assert 'key1' in flushed_data[0]['writes']

        cache.stop()


class TestUnifiedCacheAsyncFlush:
    """测试UnifiedCache的异步flush功能"""

    def test_async_flush_worker_start(self):
        """测试异步flush工作线程启动"""
        cache = UnifiedCache(
            maxsize=10,
            auto_flush=False,
            async_flush=True
        )

        # 验证异步flush工作线程已启动
        assert cache._flush_worker_thread is not None
        assert cache._flush_worker_thread.is_alive()

        # 保存线程引用（stop()会将其设为None）
        flush_worker_thread = cache._flush_worker_thread

        cache.stop()

        # 验证线程已停止
        time.sleep(0.1)
        assert not flush_worker_thread.is_alive()

    def test_async_flush_threshold_trigger(self):
        """测试异步flush在达到阈值时触发"""
        flushed_data = []
        flush_event = threading.Event()

        def mock_flush(writes, deletes):
            flushed_data.append({'writes': writes, 'deletes': deletes})
            flush_event.set()

        cache = UnifiedCache(
            maxsize=100,
            flush_threshold=3,
            auto_flush=False,
            async_flush=True,
            flush_callback=mock_flush
        )

        # 写入3个dirty数据触发异步flush
        cache.put('key1', 'value1', dirty=True)
        cache.put('key2', 'value2', dirty=True)
        cache.put('key3', 'value3', dirty=True)

        # 等待异步flush完成
        assert flush_event.wait(timeout=2.0)

        # 验证flush被调用
        assert len(flushed_data) == 1
        assert len(flushed_data[0]['writes']) == 3

        cache.stop()

    def test_async_flush_non_blocking(self):
        """测试异步flush不阻塞读写操作"""
        flush_started = threading.Event()
        flush_completed = threading.Event()

        def slow_flush(writes, deletes):
            flush_started.set()
            time.sleep(0.5)  # 模拟慢速flush
            flush_completed.set()

        cache = UnifiedCache(
            maxsize=100,
            flush_threshold=2,
            auto_flush=False,
            async_flush=True,
            flush_callback=slow_flush
        )

        # 触发异步flush
        cache.put('key1', 'value1', dirty=True)
        cache.put('key2', 'value2', dirty=True)

        # 等待flush开始
        assert flush_started.wait(timeout=2.0)

        # 在flush期间，应该仍能读写
        cache.put('key3', 'value3', dirty=False)
        assert cache.get('key3') == 'value3'

        # 等待flush完成
        assert flush_completed.wait(timeout=2.0)

        cache.stop()


class TestUnifiedCacheThreadSafety:
    """测试UnifiedCache的线程安全性"""

    def test_concurrent_put(self):
        """测试并发写入"""
        cache = UnifiedCache(maxsize=1000, auto_flush=False)

        def writer(start_key, count):
            for i in range(count):
                key = f'key_{start_key}_{i}'
                value = f'value_{start_key}_{i}'
                cache.put(key, value, dirty=False)

        # 启动10个线程，每个写入100条
        threads = []
        for i in range(10):
            t = threading.Thread(target=writer, args=(i, 100))
            threads.append(t)
            t.start()

        # 等待所有线程完成
        for t in threads:
            t.join()

        # 验证所有数据都写入成功
        assert len(cache) == 1000

        # 验证数据正确性
        for i in range(10):
            for j in range(100):
                key = f'key_{i}_{j}'
                expected = f'value_{i}_{j}'
                assert cache.get(key) == expected

        cache.stop()

    def test_concurrent_read_write(self):
        """测试并发读写"""
        cache = UnifiedCache(maxsize=100, auto_flush=False)

        # 预填充数据
        for i in range(50):
            cache.put(f'key_{i}', f'value_{i}', dirty=False)

        errors = []

        def reader():
            try:
                for _ in range(100):
                    key = f'key_{hash(threading.current_thread()) % 50}'
                    value = cache.get(key)
                    if value is not None:
                        assert value.startswith('value_')
            except Exception as e:
                errors.append(e)

        def writer():
            try:
                for i in range(100):
                    key = f'key_{hash(threading.current_thread()) % 50}'
                    value = f'value_{threading.current_thread().name}_{i}'
                    cache.put(key, value, dirty=False)
            except Exception as e:
                errors.append(e)

        # 启动5个读线程和5个写线程
        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=reader, name=f'reader_{i}'))
            threads.append(threading.Thread(target=writer, name=f'writer_{i}'))

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # 验证没有错误
        assert len(errors) == 0

        cache.stop()

    def test_concurrent_flush(self):
        """测试并发flush"""
        flush_count = []
        flush_lock = threading.Lock()

        def mock_flush(writes, deletes):
            with flush_lock:
                flush_count.append(len(writes))

        cache = UnifiedCache(
            maxsize=1000,
            flush_threshold=100,
            auto_flush=False,
            flush_callback=mock_flush
        )

        # 写入数据
        for i in range(100):
            cache.put(f'key_{i}', f'value_{i}', dirty=True)

        # 多个线程同时flush
        def flusher():
            cache.flush()

        threads = [threading.Thread(target=flusher) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 验证数据被flush（可能多次，但总和应该>=100）
        total_flushed = sum(flush_count)
        assert total_flushed >= 100

        cache.stop()


class TestUnifiedCacheStats:
    """测试UnifiedCache的统计功能"""

    def test_stats_basic(self):
        """测试基本统计信息"""
        cache = UnifiedCache(
            maxsize=100,
            flush_threshold=10,
            flush_interval=60,
            auto_flush=False
        )

        stats = cache.stats()
        assert stats['total_entries'] == 0
        assert stats['dirty_entries'] == 0
        assert stats['delete_entries'] == 0
        assert stats['maxsize'] == 100
        assert stats['flush_threshold'] == 10
        assert stats['flush_interval'] == 60
        assert stats['auto_flush_enabled'] is False
        assert stats['async_flush_enabled'] is False

        cache.stop()

    def test_stats_with_data(self):
        """测试有数据时的统计信息"""
        cache = UnifiedCache(maxsize=100, auto_flush=False)

        # 写入数据
        cache.put('key1', 'value1', dirty=True)
        cache.put('key2', 'value2', dirty=False)
        cache.delete('key3')

        stats = cache.stats()
        assert stats['total_entries'] == 2
        assert stats['dirty_entries'] == 1
        assert stats['delete_entries'] == 1
        assert stats['time_since_last_flush'] >= 0

        cache.stop()

    def test_stats_after_flush(self):
        """测试flush后的统计信息"""
        cache = UnifiedCache(
            maxsize=100,
            auto_flush=False,
            flush_callback=lambda w, d: None
        )

        # 写入dirty数据
        cache.put('key1', 'value1', dirty=True)
        assert cache.stats()['dirty_entries'] == 1

        # Flush
        cache.flush()

        # dirty应该被清除
        stats = cache.stats()
        assert stats['dirty_entries'] == 0
        assert stats['time_since_last_flush'] < 1.0  # 刚flush完

        cache.stop()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
