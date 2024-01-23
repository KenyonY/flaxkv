from sparrow import MeasureTime
import asyncio
import zmq.asyncio
try:
    import uvloop
    uvloop.install()
except:
    ...

context = zmq.asyncio.Context()
socket = context.socket(zmq.DEALER)
use_tcp = 1
if use_tcp:
    socket.connect("tcp://localhost:5555")
else:
    socket.connect("ipc:///tmp/feeds/0")

async def send_request(route, repeat_times=1):
    for _ in range(repeat_times):
        await socket.send_multipart([route, b""])
        reply = await socket.recv_multipart()
    return True


async def main():

    tasks = []
    route = "/set_data"
    N = 100
    repeat_times = 20
    for _ in range(N):
        task = asyncio.create_task(send_request(route.encode(), repeat_times))
        tasks.append(task)

    mt = MeasureTime()
    responses = await asyncio.gather(*tasks)

    dt = mt.show_interval()
    print(f"{N* repeat_times/dt=}")


if __name__ == "__main__":

    # asyncio.run(main())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

