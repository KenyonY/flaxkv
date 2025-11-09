"""
统一缓存实现（读写合一，遵循KISS原则）

设计理念：
- Write-back Cache：读写使用同一个缓存
- Dirty标记：标记需要刷新的数据
- LRU淘汰：自动淘汰最少使用的数据
- TTL支持：自动处理过期数据
- 批量刷新：达到阈值或定时刷新

优势：
- 概念简单：单一缓存结构
- 一致性天然保证：数据只在一个地方
- 传统设计：write-back cache经过数十年验证
- 易于维护：逻辑清晰，不易出bug
"""

import time
import threading
from typing import Any, Dict, Set, Optional, Tuple, Callable
from collections import OrderedDict
from flaxkv2.utils.log import get_logger

logger = get_logger(__name__)


class CacheEntry:
    """缓存条目"""
    __slots__ = ('value', 'expire_time', 'dirty', 'timestamp')

    def __init__(self, value: Any, expire_time: Optional[float] = None, dirty: bool = False):
        self.value = value
        self.expire_time = expire_time  # None表示无TTL
        self.dirty = dirty  # 是否需要刷新到持久化存储
        self.timestamp = time.time()  # 创建/访问时间

    def is_expired(self) -> bool:
        """检查是否过期"""
        if self.expire_time is None:
            return False
        return time.time() > self.expire_time


class UnifiedCache:
    """
    统一缓存：读写合一的write-back cache

    功能：
    1. 读操作：从缓存读取，未命中则回调加载
    2. 写操作：写入缓存并标记dirty
    3. 自动刷新：达到阈值或定时刷新dirty数据
    4. LRU淘汰：自动淘汰最少使用的数据（淘汰前先flush dirty数据）
    5. TTL支持：自动清理过期数据

    使用示例：
        cache = UnifiedCache(
            maxsize=1000,
            flush_threshold=100,
            flush_interval=60,
            flush_callback=lambda writes, deletes: db.batch_write(writes, deletes)
        )

        # 读取
        value = cache.get('key1')
        if value is None:
            value = db.load('key1')
            cache.put('key1', value, dirty=False)  # 加载到缓存，非dirty

        # 写入
        cache.put('key1', 'new_value', dirty=True)  # 写入缓存，标记dirty

        # 删除
        cache.delete('key1')  # 标记为删除

        # 手动刷新
        cache.flush()
    """

    # 删除标记（哨兵对象）
    _DELETED = object()

    def __init__(
        self,
        maxsize: int = 1000,
        flush_threshold: int = 100,
        flush_interval: int = 60,
        flush_callback: Optional[Callable[[Dict, Set], None]] = None,
        auto_flush: bool = True,
        async_flush: bool = False,
    ):
        """
        初始化统一缓存

        Args:
            maxsize: 缓存最大大小（条目数）
            flush_threshold: 达到此数量的dirty条目时触发刷新
            flush_interval: 刷新间隔（秒）
            flush_callback: 刷新回调，签名：callback(writes: Dict, deletes: Set)
                - writes: {key: (value, ttl)} 待写入的数据
                - deletes: {key1, key2, ...} 待删除的键
            auto_flush: 是否启动后台定时刷新线程
            async_flush: 是否异步flush（默认False，同步flush）
        """
        if maxsize <= 0:
            raise ValueError(f"maxsize must be > 0, got {maxsize}")
        if flush_threshold <= 0:
            raise ValueError(f"flush_threshold must be > 0, got {flush_threshold}")
        if flush_interval <= 0:
            raise ValueError(f"flush_interval must be > 0, got {flush_interval}")

        self._maxsize = maxsize
        self._flush_threshold = flush_threshold
        self._flush_interval = flush_interval
        self._flush_callback = flush_callback
        self._auto_flush = auto_flush
        self._async_flush = async_flush

        # 缓存数据：OrderedDict实现LRU
        self._cache: OrderedDict[Any, CacheEntry] = OrderedDict()

        # Dirty跟踪：需要刷新的key
        self._dirty_keys: Set[Any] = set()

        # 删除跟踪：标记为删除的key
        self._delete_keys: Set[Any] = set()

        # 线程安全
        self._lock = threading.RLock()

        # 刷新状态
        self._last_flush_time = time.time()
        self._flush_thread = None
        self._flush_worker_thread = None
        self._stop_event = threading.Event()
        self._flush_event = threading.Event()

        # 启动后台刷新线程
        if self._auto_flush:
            self._start_flush_thread()

        # 启动异步flush工作线程
        if self._async_flush:
            self._start_async_flush_worker()

        logger.debug(f"UnifiedCache initialized: maxsize={maxsize}, flush_threshold={flush_threshold}, async_flush={async_flush}")

    def get(self, key: Any, default: Any = None) -> Any:
        """
        获取值

        Args:
            key: 键
            default: 默认值（未找到时返回）

        Returns:
            值，如果不存在或已过期或已删除，返回default
        """
        with self._lock:
            # 检查是否标记为删除
            if key in self._delete_keys:
                return default

            # 检查缓存
            if key not in self._cache:
                return default

            entry = self._cache[key]

            # 检查是否过期
            if entry.is_expired():
                # 过期：移除
                del self._cache[key]
                self._dirty_keys.discard(key)
                return default

            # LRU：移到末尾
            self._cache.move_to_end(key)

            return entry.value

    def put(self, key: Any, value: Any, ttl: Optional[int] = None, dirty: bool = True):
        """
        写入缓存

        Args:
            key: 键
            value: 值
            ttl: TTL秒数（None表示无TTL）
            dirty: 是否标记为dirty（需要刷新到持久化存储）
                - True: 需要刷新（写入操作）
                - False: 不需要刷新（从持久化存储加载）
        """
        with self._lock:
            # 从删除集合中移除
            self._delete_keys.discard(key)

            # 计算过期时间
            expire_time = None
            if ttl is not None:
                expire_time = time.time() + ttl

            # 创建缓存条目
            entry = CacheEntry(value, expire_time, dirty=dirty)

            # 添加到缓存
            self._cache[key] = entry
            self._cache.move_to_end(key)

            # 标记dirty
            if dirty:
                self._dirty_keys.add(key)

            # LRU淘汰
            self._evict_if_needed()

            # 检查是否需要刷新
            if len(self._dirty_keys) >= self._flush_threshold:
                logger.debug(f"Flush threshold reached: {len(self._dirty_keys)}/{self._flush_threshold}")
                if self._async_flush:
                    # 异步模式：发送信号
                    self._flush_event.set()
                else:
                    # 同步模式：立即flush
                    self._do_flush()

    def delete(self, key: Any):
        """
        删除键

        Args:
            key: 键
        """
        with self._lock:
            # 从缓存中移除
            if key in self._cache:
                del self._cache[key]

            # 从dirty集合中移除
            self._dirty_keys.discard(key)

            # 添加到删除集合（需要同步到持久化存储）
            self._delete_keys.add(key)

            # 检查是否需要刷新
            if len(self._delete_keys) >= self._flush_threshold:
                logger.debug(f"Delete threshold reached")
                if self._async_flush:
                    self._flush_event.set()
                else:
                    self._do_flush()

    def _evict_if_needed(self):
        """LRU淘汰（假设已持有锁）"""
        while len(self._cache) > self._maxsize:
            # 弹出最旧的条目（OrderedDict的第一个）
            key, entry = self._cache.popitem(last=False)

            # 如果是dirty数据，需要先flush
            if entry.dirty:
                logger.warning(f"Evicting dirty entry: {key}, flushing first")
                # 单独flush这个条目
                writes = {key: (entry.value, self._get_ttl(entry))}
                deletes = set()
                if self._flush_callback:
                    try:
                        self._flush_callback(writes, deletes)
                    except Exception as e:
                        logger.error(f"Error flushing evicted entry: {e}", exc_info=True)

            # 从dirty集合中移除
            self._dirty_keys.discard(key)

    def _get_ttl(self, entry: CacheEntry) -> Optional[int]:
        """计算剩余TTL（秒）"""
        if entry.expire_time is None:
            return None
        remaining = entry.expire_time - time.time()
        return int(max(0, remaining))

    def flush(self):
        """手动刷新所有dirty数据"""
        with self._lock:
            self._do_flush()

    def _do_flush(self):
        """执行刷新（假设已持有锁）"""
        if len(self._dirty_keys) == 0 and len(self._delete_keys) == 0:
            return

        # 准备刷新数据
        writes = {}
        for key in self._dirty_keys:
            if key in self._cache:
                entry = self._cache[key]
                if not entry.is_expired():
                    ttl = self._get_ttl(entry)
                    writes[key] = (entry.value, ttl)

        deletes = self._delete_keys.copy()

        write_count = len(writes)
        delete_count = len(deletes)

        # 清除dirty和delete标记
        self._dirty_keys.clear()
        self._delete_keys.clear()

        # 更新刷新时间
        self._last_flush_time = time.time()

        # 调用刷新回调
        if self._flush_callback and (write_count > 0 or delete_count > 0):
            try:
                logger.debug(f"Flushing: {write_count} writes, {delete_count} deletes")
                self._flush_callback(writes, deletes)
                logger.debug(f"Flush completed")
            except Exception as e:
                logger.error(f"Error in flush callback: {e}", exc_info=True)

    def _start_flush_thread(self):
        """启动后台定时刷新线程"""
        if self._flush_thread is not None:
            return

        self._stop_event.clear()
        self._flush_thread = threading.Thread(
            target=self._flush_worker,
            name="UnifiedCache-FlushThread",
            daemon=True
        )
        self._flush_thread.start()
        logger.debug("UnifiedCache flush thread started")

    def _flush_worker(self):
        """后台刷新线程工作函数"""
        while not self._stop_event.is_set():
            try:
                if self._stop_event.wait(timeout=self._flush_interval):
                    break

                # 定时刷新
                with self._lock:
                    elapsed = time.time() - self._last_flush_time
                    if elapsed >= self._flush_interval and (len(self._dirty_keys) > 0 or len(self._delete_keys) > 0):
                        logger.debug(f"Auto flush triggered: {len(self._dirty_keys)} dirty, {len(self._delete_keys)} deletes")
                        self._do_flush()

            except Exception as e:
                logger.error(f"Error in flush worker: {e}", exc_info=True)

    def _start_async_flush_worker(self):
        """启动异步flush工作线程"""
        if self._flush_worker_thread is not None:
            return

        self._flush_event.clear()
        self._flush_worker_thread = threading.Thread(
            target=self._async_flush_worker,
            name="UnifiedCache-AsyncFlushWorker",
            daemon=True
        )
        self._flush_worker_thread.start()
        logger.debug("UnifiedCache async flush worker started")

    def _async_flush_worker(self):
        """异步flush工作线程（双缓冲）"""
        while not self._stop_event.is_set():
            try:
                if self._flush_event.wait(timeout=1.0):
                    self._flush_event.clear()

                    # 阶段1：持锁快速复制
                    with self._lock:
                        if len(self._dirty_keys) == 0 and len(self._delete_keys) == 0:
                            continue

                        # 快速复制数据
                        writes = {}
                        for key in self._dirty_keys:
                            if key in self._cache:
                                entry = self._cache[key]
                                if not entry.is_expired():
                                    ttl = self._get_ttl(entry)
                                    writes[key] = (entry.value, ttl)

                        deletes = self._delete_keys.copy()

                        write_count = len(writes)
                        delete_count = len(deletes)

                        # 清除标记
                        self._dirty_keys.clear()
                        self._delete_keys.clear()
                        self._last_flush_time = time.time()

                    # 阶段2：锁外执行flush
                    if self._flush_callback and (write_count > 0 or delete_count > 0):
                        try:
                            logger.debug(f"Async flush: {write_count} writes, {delete_count} deletes")
                            self._flush_callback(writes, deletes)
                            logger.debug(f"Async flush completed")
                        except Exception as e:
                            logger.error(f"Error in async flush callback: {e}", exc_info=True)

            except Exception as e:
                logger.error(f"Error in async flush worker: {e}", exc_info=True)

    def clear(self):
        """清空缓存（不刷新）"""
        with self._lock:
            self._cache.clear()
            self._dirty_keys.clear()
            self._delete_keys.clear()
            logger.warning("UnifiedCache cleared (data lost if not flushed)")

    def stop(self):
        """停止缓存（刷新并停止后台线程）"""
        # 刷新剩余数据
        with self._lock:
            if len(self._dirty_keys) > 0 or len(self._delete_keys) > 0:
                logger.debug(f"Flushing before stop: {len(self._dirty_keys)} dirty, {len(self._delete_keys)} deletes")
                self._do_flush()

        # 停止后台线程
        logger.debug("Stopping background threads...")
        self._stop_event.set()

        if self._flush_thread is not None:
            self._flush_thread.join(timeout=5)
            self._flush_thread = None
            logger.debug("Flush thread stopped")

        if self._flush_worker_thread is not None:
            self._flush_event.set()
            self._flush_worker_thread.join(timeout=5)
            self._flush_worker_thread = None
            logger.debug("Async flush worker stopped")

    def stats(self) -> dict:
        """返回缓存统计信息"""
        with self._lock:
            return {
                'total_entries': len(self._cache),
                'dirty_entries': len(self._dirty_keys),
                'delete_entries': len(self._delete_keys),
                'maxsize': self._maxsize,
                'flush_threshold': self._flush_threshold,
                'flush_interval': self._flush_interval,
                'time_since_last_flush': time.time() - self._last_flush_time,
                'auto_flush_enabled': self._auto_flush,
                'async_flush_enabled': self._async_flush,
            }

    def __len__(self) -> int:
        """返回缓存大小"""
        with self._lock:
            return len(self._cache)

    def __contains__(self, key: Any) -> bool:
        """检查key是否在缓存中（未过期）"""
        return self.get(key) is not None
