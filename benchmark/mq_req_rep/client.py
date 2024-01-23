import zmq
from sparrow import MeasureTime
import asyncio
import time
import zmq.asyncio
try:
    import uvloop
    uvloop.install()
except:
    ...

use_tcp = 0
multi_thread = 1

if not multi_thread:
    context = zmq.Context()
    socket = context.socket(zmq.REQ)  # REQ (REQUEST) socket for request-reply pattern
    if use_tcp:
        socket.connect("tcp://localhost:5555")
    else:
        socket.connect("ipc:///tmp/feeds/0")

    N = 10000
    mt = MeasureTime()
    for request in range(N):
        socket.send(b"Hello")
        message = socket.recv()
    dt = mt.show_interval(f"GET {N} keys")

    print(f"{N/dt} /s")

else:
    import threading

    T = 0
    lock = threading.Lock()

    def worker_thread(thread_id, num_requests):
        global T
        # 需要为每个线程创建独立的ZeroMQ上下文和socket。
        # 这是因为ZeroMQ的socket在多线程环境中不是线程安全的。
        # 每个线程都应该有自己的独立socket来发送和接收消息。
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        if use_tcp:
            socket.connect("tcp://localhost:5555")
        else:
            socket.connect("ipc:///tmp/feeds/0")

        t0 = time.time()
        for request in range(num_requests):
            socket.send(b"Hello")
            message = socket.recv()
        dt = time.time() - t0
        # with lock:
        T += dt

        socket.close()
        context.term()


    thread_count = 10
    requests_per_thread = 1000

    threads = []
    for i in range(thread_count):
        thread = threading.Thread(target=worker_thread, args=(i, requests_per_thread))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print(f"{T=}, {thread_count * requests_per_thread / T}/s")

    # 结论： 多线程的方式很慢...





