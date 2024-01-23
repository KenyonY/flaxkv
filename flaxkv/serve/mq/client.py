# import zmq
import time
import pickle
# context = zmq.Context()

import asyncio
import zmq.asyncio

import uvloop
uvloop.install()

context = zmq.asyncio.Context()
# socket = context.socket(zmq.REQ)
socket = context.socket(zmq.DEALER)

socket.connect("tcp://localhost:5555")


async def send_request(socket, route, repeat_times=1):
    for _ in range(repeat_times):
        await socket.send_multipart([route.encode(), b""])
        reply = await socket.recv_multipart()
        # print(reply)
    return True


async def main():

    tasks = []
    route = "/set_data"
    N = 100
    repeat_times = N // 2
    for _ in range(N):
        task = asyncio.create_task(send_request(socket, route, repeat_times))
        tasks.append(task)

    start_time = time.time()
    responses = await asyncio.gather(*tasks)

    # for response in responses:
    #     print(f"Response: {response}")

    end_time = time.time()
    print(f"{end_time - start_time=}")
    print(f"{N* repeat_times/(end_time - start_time)=}")
    socket.close()
    context.term()

if __name__ == "__main__":
    # asyncio.run(main())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

