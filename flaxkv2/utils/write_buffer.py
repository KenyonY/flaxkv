"""
通用写缓冲区实现（支持本地和远程后端）

设计理念：
- Write-back 策略：延迟写入，批量刷新
- 线程安全：使用锁保护
- 触发条件：大小阈值、时间间隔、手动刷新、关闭时
- 读写一致性：读取时优先检查缓冲区

性能优势：
- 减少磁盘I/O次数（本地）
- 减少网络请求次数（远程）
- 批量操作提升吞吐量
"""

import time
import threading
from typing import Any, Dict, Set, Optional, Tuple, Callable
from flaxkv2.utils.log import get_logger

logger = get_logger(__name__)


class WriteBuffer:
    """
    写缓冲区（支持本地和远程后端）

    功能：
    1. 缓冲写入操作（set/delete）
    2. 达到阈值时自动刷新
    3. 定时刷新（后台线程）
    4. 手动刷新
    5. 支持读取时合并缓冲区数据

    线程安全：
    - 所有操作都使用锁保护
    - 支持多线程并发访问

    使用示例：
        # 创建写缓冲区
        buffer = WriteBuffer(
            max_size=100,
            flush_interval=60,
            flush_callback=lambda writes, deletes: db._do_flush(writes, deletes)
        )

        # 写入数据
        buffer.put('key1', 'value1', ttl=None)
        buffer.put('key2', 'value2', ttl=60)

        # 删除数据
        buffer.delete('key1')

        # 读取数据（优先返回缓冲区中的数据）
        value = buffer.get('key1')

        # 手动刷新
        buffer.flush()

        # 停止（刷新并停止后台线程）
        buffer.stop()
    """

    def __init__(
        self,
        max_size: int = 100,
        flush_interval: int = 60,
        flush_callback: Optional[Callable[[Dict, Set], None]] = None,
        auto_flush: bool = True,
        async_flush: bool = False,
    ):
        """
        初始化写缓冲区

        Args:
            max_size: 缓冲区最大大小（条目数），达到此值时触发刷新
            flush_interval: 刷新间隔（秒），定时触发刷新
            flush_callback: 刷新回调函数，签名：callback(writes: Dict, deletes: Set)
                - writes: {key: (value, ttl)} 字典
                - deletes: {key1, key2, ...} 集合
            auto_flush: 是否启动后台定时刷新线程
            async_flush: 是否使用异步flush（默认False）
                - False（默认）: 同步flush，达到阈值时阻塞执行flush（安全，但慢）
                - True: 异步flush，达到阈值时仅发送信号不阻塞（快，但有风险）
                ⚠️ async_flush=True 警告：
                    - 进程崩溃可能丢失更多数据
                    - flush错误无法立即捕获
                    - 仅适合可容忍数据丢失的场景
        """
        if max_size <= 0:
            raise ValueError(f"max_size must be > 0, got {max_size}")
        if flush_interval <= 0:
            raise ValueError(f"flush_interval must be > 0, got {flush_interval}")

        self._max_size = max_size
        self._flush_interval = flush_interval
        self._flush_callback = flush_callback
        self._auto_flush = auto_flush
        self._async_flush = async_flush

        # 缓冲区数据结构
        # _buffer_dict: {key: (value, ttl, timestamp)}
        # - value: 要写入的值
        # - ttl: TTL秒数（None表示无TTL）
        # - timestamp: 写入缓冲区的时间戳（用于调试）
        self._buffer_dict: Dict[Any, Tuple[Any, Optional[int], float]] = {}

        # 删除集合：{key1, key2, ...}
        self._delete_set: Set[Any] = set()

        # 锁
        self._lock = threading.RLock()

        # 刷新线程
        self._flush_thread = None
        self._stop_event = threading.Event()
        self._last_flush_time = time.time()

        # 异步flush支持
        self._flush_event = threading.Event()  # 信号：需要执行flush
        self._flush_worker_thread = None

        # 启动后台刷新线程
        if self._auto_flush:
            self._start_flush_thread()

        # 启动异步flush工作线程
        if self._async_flush:
            self._start_async_flush_worker()

        logger.debug(f"WriteBuffer initialized: max_size={max_size}, flush_interval={flush_interval}s, async_flush={async_flush}")

    def _start_flush_thread(self):
        """启动后台刷新线程（定时刷新）"""
        if self._flush_thread is not None:
            return

        self._stop_event.clear()
        self._flush_thread = threading.Thread(
            target=self._flush_worker,
            name=f"WriteBuffer-FlushThread",
            daemon=True
        )
        self._flush_thread.start()
        logger.debug("WriteBuffer flush thread started")

    def _flush_worker(self):
        """后台刷新线程工作函数（定时刷新）"""
        while not self._stop_event.is_set():
            try:
                # 等待刷新间隔
                if self._stop_event.wait(timeout=self._flush_interval):
                    # 收到停止信号
                    break

                # 检查是否需要刷新
                with self._lock:
                    elapsed = time.time() - self._last_flush_time
                    if elapsed >= self._flush_interval and len(self._buffer_dict) > 0:
                        logger.debug(f"Auto flush triggered: {len(self._buffer_dict)} buffered writes")
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
            name=f"WriteBuffer-AsyncFlushWorker",
            daemon=True
        )
        self._flush_worker_thread.start()
        logger.debug("WriteBuffer async flush worker started")

    def _async_flush_worker(self):
        """异步flush工作线程（响应信号立即flush）

        使用双缓冲技术：快速持锁复制数据，然后在锁外执行flush
        """
        while not self._stop_event.is_set():
            try:
                # 等待flush信号
                if self._flush_event.wait(timeout=1.0):
                    # 收到flush信号
                    self._flush_event.clear()

                    # ===== 阶段1：持锁快速复制（毫秒级） =====
                    with self._lock:
                        if len(self._buffer_dict) == 0 and len(self._delete_set) == 0:
                            continue

                        # 快速复制数据
                        writes = {k: (v, ttl) for k, (v, ttl, _) in self._buffer_dict.items()}
                        deletes = self._delete_set.copy()

                        write_count = len(writes)
                        delete_count = len(deletes)

                        # 清空缓冲区
                        self._buffer_dict.clear()
                        self._delete_set.clear()
                        self._last_flush_time = time.time()

                    # ===== 阶段2：锁外执行flush（秒级，不阻塞put操作）=====
                    if self._flush_callback is not None:
                        try:
                            logger.debug(f"Async flush: {write_count} writes, {delete_count} deletes")
                            self._flush_callback(writes, deletes)
                            logger.debug(f"Async flush completed")
                        except Exception as e:
                            logger.error(f"Error in async flush callback: {e}", exc_info=True)

            except Exception as e:
                logger.error(f"Error in async flush worker: {e}", exc_info=True)

    def put(self, key: Any, value: Any, ttl: Optional[int] = None):
        """
        写入数据到缓冲区

        Args:
            key: 键
            value: 值
            ttl: TTL秒数（None表示无TTL）
        """
        with self._lock:
            # 从删除集合中移除（如果之前被标记删除）
            self._delete_set.discard(key)

            # 添加到缓冲区
            timestamp = time.time()
            self._buffer_dict[key] = (value, ttl, timestamp)

            # 检查是否需要刷新
            if len(self._buffer_dict) >= self._max_size:
                logger.debug(f"Buffer size threshold reached: {len(self._buffer_dict)}/{self._max_size}")
                if self._async_flush:
                    # 异步模式：发送信号，不阻塞
                    self._flush_event.set()
                else:
                    # 同步模式：阻塞执行flush
                    self._do_flush()

    def delete(self, key: Any):
        """
        标记键为删除

        Args:
            key: 键
        """
        with self._lock:
            # 从缓冲区中移除（如果存在）
            self._buffer_dict.pop(key, None)

            # 添加到删除集合
            self._delete_set.add(key)

            # 检查是否需要刷新
            if len(self._delete_set) + len(self._buffer_dict) >= self._max_size:
                logger.debug(f"Buffer size threshold reached (including deletes)")
                if self._async_flush:
                    # 异步模式：发送信号，不阻塞
                    self._flush_event.set()
                else:
                    # 同步模式：阻塞执行flush
                    self._do_flush()

    def get(self, key: Any) -> Optional[Tuple[Any, Optional[int]]]:
        """
        从缓冲区获取数据

        Args:
            key: 键

        Returns:
            (value, ttl) 如果键在缓冲区中
            None 如果键不在缓冲区中

        特殊返回值：
            如果键在删除集合中，返回一个特殊的标记对象 _DELETED
        """
        with self._lock:
            # 检查是否在删除集合中
            if key in self._delete_set:
                return WriteBuffer._DELETED

            # 检查是否在缓冲区中
            if key in self._buffer_dict:
                value, ttl, _ = self._buffer_dict[key]
                return (value, ttl)

            return None

    # 删除标记（哨兵对象）
    _DELETED = object()

    def is_deleted(self, key: Any) -> bool:
        """
        检查键是否在删除集合中

        Args:
            key: 键

        Returns:
            True 如果键被标记为删除
        """
        with self._lock:
            return key in self._delete_set

    def should_flush(self) -> bool:
        """
        检查是否应该刷新

        Returns:
            True 如果应该刷新
        """
        with self._lock:
            # 检查大小阈值
            if len(self._buffer_dict) + len(self._delete_set) >= self._max_size:
                return True

            # 检查时间阈值
            elapsed = time.time() - self._last_flush_time
            if elapsed >= self._flush_interval and len(self._buffer_dict) > 0:
                return True

            return False

    def flush(self):
        """
        手动刷新缓冲区
        """
        with self._lock:
            self._do_flush()

    def _do_flush(self):
        """
        执行刷新（内部方法，假设已持有锁）

        注意：调用此方法前必须已持有 self._lock
        使用双缓冲技术：快速持锁复制数据，然后在锁外执行flush
        """
        # 检查是否有数据需要刷新
        if len(self._buffer_dict) == 0 and len(self._delete_set) == 0:
            return

        # ===== 第一阶段：持锁快速复制 =====
        # 准备刷新数据（浅拷贝，非常快）
        writes = {k: (v, ttl) for k, (v, ttl, _) in self._buffer_dict.items()}
        deletes = self._delete_set.copy()

        write_count = len(writes)
        delete_count = len(deletes)

        # 清空缓冲区（持锁期间完成）
        self._buffer_dict.clear()
        self._delete_set.clear()
        self._last_flush_time = time.time()

        # ===== 第二阶段：释放锁，执行昂贵的flush操作 =====
        # 注意：_do_flush被调用时已持有锁，这里需要释放锁来执行flush
        # 但这违反了"假设已持有锁"的约定，所以我们需要重构
        # 暂时保持原有行为，在锁内flush（性能较差但安全）

        # 调用刷新回调
        if self._flush_callback is not None:
            try:
                logger.debug(f"Flushing: {write_count} writes, {delete_count} deletes")
                self._flush_callback(writes, deletes)
                logger.debug(f"Flush completed successfully")
            except Exception as e:
                logger.error(f"Error in flush callback: {e}", exc_info=True)
                # 注意：这里不重新放回缓冲区，避免无限循环
                # 如果需要更强的数据安全保证，可以考虑实现 WAL

    def __len__(self) -> int:
        """返回缓冲区大小（写入 + 删除）"""
        with self._lock:
            return len(self._buffer_dict) + len(self._delete_set)

    def stats(self) -> dict:
        """
        返回缓冲区统计信息

        Returns:
            统计信息字典
        """
        with self._lock:
            return {
                'buffered_writes': len(self._buffer_dict),
                'buffered_deletes': len(self._delete_set),
                'total_buffered': len(self._buffer_dict) + len(self._delete_set),
                'max_size': self._max_size,
                'flush_interval': self._flush_interval,
                'time_since_last_flush': time.time() - self._last_flush_time,
                'auto_flush_enabled': self._auto_flush,
            }

    def stop(self):
        """
        停止写缓冲区（刷新并停止后台线程）
        """
        # 刷新剩余数据
        with self._lock:
            if len(self._buffer_dict) > 0 or len(self._delete_set) > 0:
                logger.debug(f"Flushing remaining data before stop: {len(self._buffer_dict)} writes, {len(self._delete_set)} deletes")
                self._do_flush()

        # 停止后台线程
        logger.debug("Stopping background threads...")
        self._stop_event.set()

        if self._flush_thread is not None:
            self._flush_thread.join(timeout=5)
            self._flush_thread = None
            logger.debug("Flush thread stopped")

        if self._flush_worker_thread is not None:
            self._flush_event.set()  # 唤醒可能在等待的线程
            self._flush_worker_thread.join(timeout=5)
            self._flush_worker_thread = None
            logger.debug("Async flush worker stopped")

    def clear(self):
        """
        清空缓冲区（不刷新）

        警告：此操作会丢失缓冲区中的数据！
        """
        with self._lock:
            self._buffer_dict.clear()
            self._delete_set.clear()
            logger.warning("WriteBuffer cleared (data lost)")
