"""
读写锁（RWLock）实现

提供读写分离的锁机制，提升并发读取性能：
- 多个读操作可以同时进行
- 写操作独占访问
- 写操作等待所有读操作完成
- 读操作等待写操作完成
"""

import threading


class RWLock:
    """
    读写锁实现

    特性：
    - 多读者：多个线程可以同时持有读锁
    - 单写者：只有一个线程可以持有写锁
    - 写优先：当有写者等待时，新的读者需要等待

    使用示例：
        lock = RWLock()

        # 读操作
        with lock.read_lock():
            data = db.get(key)

        # 写操作
        with lock.write_lock():
            db.put(key, value)
    """

    def __init__(self):
        self._read_ready = threading.Condition(threading.RLock())
        self._readers = 0
        self._writers = 0
        self._write_waiters = 0

    def acquire_read(self):
        """获取读锁"""
        with self._read_ready:
            # 等待所有写者完成
            while self._writers > 0 or self._write_waiters > 0:
                self._read_ready.wait()
            self._readers += 1

    def release_read(self):
        """释放读锁"""
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                # 最后一个读者离开，通知等待的写者
                self._read_ready.notify_all()

    def acquire_write(self):
        """获取写锁"""
        with self._read_ready:
            self._write_waiters += 1
            # 等待所有读者和写者完成
            while self._readers > 0 or self._writers > 0:
                self._read_ready.wait()
            self._write_waiters -= 1
            self._writers += 1

    def release_write(self):
        """释放写锁"""
        with self._read_ready:
            self._writers -= 1
            # 通知所有等待的读者和写者
            self._read_ready.notify_all()

    def read_lock(self):
        """返回读锁上下文管理器"""
        return _ReadLock(self)

    def write_lock(self):
        """返回写锁上下文管理器"""
        return _WriteLock(self)


class _ReadLock:
    """读锁上下文管理器"""

    def __init__(self, rwlock):
        self._rwlock = rwlock

    def __enter__(self):
        self._rwlock.acquire_read()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._rwlock.release_read()
        return False


class _WriteLock:
    """写锁上下文管理器"""

    def __init__(self, rwlock):
        self._rwlock = rwlock

    def __enter__(self):
        self._rwlock.acquire_write()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._rwlock.release_write()
        return False
