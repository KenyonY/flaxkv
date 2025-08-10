"""
FlaxKV 2.0 异步事件循环管理器

管理后台异步事件循环线程，为同步接口提供异步支持。
"""

import asyncio
import threading
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class AsyncEventLoopThread:
    """后台异步事件循环线程管理器。"""
    
    def __init__(self):
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.thread: Optional[threading.Thread] = None
        self._shutdown_event = threading.Event()
        self._loop_ready = threading.Event()
        self._exception: Optional[Exception] = None
        
    def start(self):
        """启动后台事件循环线程。"""
        if self.thread and self.thread.is_alive():
            return
        
        self._shutdown_event.clear()
        self._loop_ready.clear()
        self._exception = None
        
        self.thread = threading.Thread(
            target=self._run_event_loop, 
            daemon=True,
            name="FlaxKV-AsyncLoop"
        )
        self.thread.start()
        
        # 等待事件循环就绪
        if not self._loop_ready.wait(timeout=10.0):
            raise RuntimeError("AsyncEventLoop启动超时")
        
        if self._exception:
            raise self._exception
        
        logger.debug("AsyncEventLoop后台线程已启动")
    
    def stop(self):
        """停止后台事件循环线程。"""
        if not self.thread or not self.thread.is_alive():
            return
        
        # 发送关闭信号
        self._shutdown_event.set()
        
        if self.loop:
            # 在事件循环中调度停止
            self.loop.call_soon_threadsafe(self.loop.stop)
        
        # 等待线程结束
        self.thread.join(timeout=5.0)
        if self.thread.is_alive():
            logger.warning("AsyncEventLoop线程未能正常停止")
        
        self.loop = None
        self.thread = None
        logger.debug("AsyncEventLoop后台线程已停止")
    
    def _run_event_loop(self):
        """事件循环线程主函数。"""
        try:
            # 创建新的事件循环
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            # 标记事件循环就绪
            self._loop_ready.set()
            
            # 运行事件循环直到收到停止信号
            while not self._shutdown_event.is_set():
                try:
                    # 运行事件循环，每100ms检查一次停止信号
                    self.loop.run_until_complete(
                        asyncio.sleep(0.1)
                    )
                except Exception as e:
                    if not self._shutdown_event.is_set():
                        logger.error(f"事件循环执行错误: {e}")
            
        except Exception as e:
            self._exception = e
            self._loop_ready.set()  # 即使出错也要设置，让主线程知道
            logger.error(f"AsyncEventLoop线程启动失败: {e}")
        finally:
            if self.loop:
                # 清理事件循环
                try:
                    # 取消所有未完成的任务
                    pending = asyncio.all_tasks(self.loop)
                    for task in pending:
                        task.cancel()
                    
                    if pending:
                        self.loop.run_until_complete(
                            asyncio.gather(*pending, return_exceptions=True)
                        )
                    
                    self.loop.close()
                except Exception as e:
                    logger.error(f"清理事件循环失败: {e}")
    
    def run_coroutine(self, coro, timeout: Optional[float] = None) -> Any:
        """在后台事件循环中运行协程。
        
        Args:
            coro: 要执行的协程
            timeout: 超时时间（秒）
            
        Returns:
            协程的执行结果
            
        Raises:
            RuntimeError: 事件循环未启动
            TimeoutError: 执行超时
            Exception: 协程执行中的异常
        """
        if not self.loop or not self.thread or not self.thread.is_alive():
            raise RuntimeError("AsyncEventLoop未启动或已停止")
        
        # 使用 asyncio.run_coroutine_threadsafe 在后台循环中执行协程
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        
        try:
            return future.result(timeout=timeout)
        except asyncio.TimeoutError:
            future.cancel()
            raise TimeoutError(f"协程执行超时 ({timeout}s)")


# 全局事件循环线程实例
_global_async_loop = None
_global_loop_lock = threading.Lock()


def get_global_async_loop() -> AsyncEventLoopThread:
    """获取全局异步事件循环线程实例。"""
    global _global_async_loop
    
    with _global_loop_lock:
        if _global_async_loop is None:
            _global_async_loop = AsyncEventLoopThread()
            _global_async_loop.start()
            
            # 注册清理函数
            import atexit
            atexit.register(lambda: _global_async_loop.stop() if _global_async_loop else None)
    
    return _global_async_loop


class SyncAsyncIteratorWrapper:
    """同步迭代器包装器。
    
    将异步迭代器转换为同步迭代器。
    """
    
    def __init__(self, async_iterator, loop_manager: AsyncEventLoopThread):
        self.async_iterator = async_iterator
        self.loop_manager = loop_manager
        
    def __iter__(self):
        return self
    
    def __next__(self):
        """获取下一个元素。"""
        try:
            # 在后台事件循环中获取下一个元素
            return self.loop_manager.run_coroutine(
                self.async_iterator.__anext__()
            )
        except StopAsyncIteration:
            raise StopIteration