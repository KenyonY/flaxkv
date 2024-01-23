# 异步客户端
import asyncio
import zmq.asyncio
import time

use_tcp = 0
T = 0

async def zmq_requester(id, num_requests):
    global T
    context = zmq.asyncio.Context()
    socket = context.socket(zmq.REQ)
    if use_tcp:
        socket.connect("tcp://localhost:5555")
    else:
        socket.connect("ipc:///tmp/feeds/0")

    t0 = time.time()
    for _ in range(num_requests):
        await socket.send(b"Hello")
        message = await socket.recv()

    T += time.time() - t0

    socket.close()

async def main():
    num_clients = 100
    requests_per_client = 100

    tasks = [zmq_requester(i, requests_per_client) for i in range(num_clients)]
    await asyncio.gather(*tasks)

    elapsed_time = T
    print(f"{num_clients * requests_per_client / elapsed_time} /s")

if __name__ == "__main__":
    asyncio.run(main())
