"""
TTL过期数据自动清理

KISS原则实现：定期批量扫描 + 删除过期键
"""

import time
import threading
from typing import Any
from flaxkv2.utils.log import get_logger
from flaxkv2.serialization.value_meta import ValueWithMeta

logger = get_logger(__name__)


class TTLCleanup:
    """
    TTL过期数据自动清理器

    采用简单的批量扫描策略：
    1. 后台线程定期扫描数据库
    2. 每次扫描固定数量的键（batch_size）
    3. 发现过期键则删除
    4. 休眠一段时间后继续

    特点：
    - 极简设计（~50行）
    - 低CPU开销
    - 持续扫描，保持迭代器状态
    """

    def __init__(self, db: Any, cleanup_interval: int = 60, batch_size: int = 1000):
        """
        初始化TTL清理器

        Args:
            db: 数据库对象（必须支持迭代和delete方法）
            cleanup_interval: 扫描间隔（秒），默认60秒
            batch_size: 每次扫描的键数量，默认1000
        """
        self.db = db
        self.cleanup_interval = cleanup_interval
        self.batch_size = batch_size
        self.running = False
        self.thread = None
        self._stop_event = threading.Event()  # 用于可中断的休眠

        logger.debug(f"TTLCleanup initialized: interval={cleanup_interval}s, batch_size={batch_size}")

    def start(self):
        """启动清理线程"""
        if self.running:
            logger.warning("TTLCleanup already running")
            return

        self.running = True
        self.thread = threading.Thread(target=self._cleanup_loop, daemon=True, name="TTLCleanup")
        self.thread.start()
        logger.info("TTLCleanup thread started")

    def stop(self):
        """停止清理线程"""
        if not self.running:
            return

        self.running = False
        self._stop_event.set()  # 立即唤醒休眠中的线程
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("TTLCleanup thread stopped")

    def _cleanup_loop(self):
        """清理循环主逻辑"""
        iterator = None

        while self.running:
            count = 0
            cycle_complete = False

            try:
                # 创建或重用迭代器
                if iterator is None:
                    iterator = iter(self.db._db)

                # 扫描一批键
                for _ in range(self.batch_size):
                    if not self.running:
                        break

                    try:
                        key_bytes, value_bytes = next(iterator)

                        # 检查是否包含TTL元数据并已过期
                        if ValueWithMeta.has_meta(value_bytes):
                            if ValueWithMeta.is_expired_fast(value_bytes):
                                try:
                                    self.db._db.delete(key_bytes)
                                    count += 1
                                except Exception as e:
                                    logger.debug(f"Failed to delete expired key: {e}")

                    except StopIteration:
                        # 遍历完成，重新开始
                        iterator = None
                        cycle_complete = True
                        break

                # 记录清理结果
                if count > 0:
                    suffix = " (scan cycle complete)" if cycle_complete else ""
                    logger.info(f"TTLCleanup: deleted {count} expired keys{suffix}")

            except Exception as e:
                logger.error(f"TTLCleanup error: {e}", exc_info=True)
                # 发生错误时重置迭代器
                iterator = None

            # 可中断的休眠：使用 Event.wait() 替代 time.sleep()
            # 当调用 stop() 时，_stop_event.set() 会立即唤醒等待
            if self.running:
                self._stop_event.wait(timeout=self.cleanup_interval)

        logger.debug("TTLCleanup loop exited")
