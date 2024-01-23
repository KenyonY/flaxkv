import zmq.asyncio
import asyncio
import os
try:
    import uvloop
    uvloop.install()
except:
    ...

context = zmq.asyncio.Context()
socket = context.socket(zmq.ROUTER)
use_tcp = 1
if use_tcp:
    socket.bind("tcp://*:5555")
else:
    ipc_path = "/tmp/feeds/0"
    os.makedirs(os.path.dirname(ipc_path), exist_ok=True)
    socket.bind(f"ipc://{ipc_path}")

async def main():

    while True:
        message = await socket.recv_multipart()
        await socket.send_multipart([message[0], b"success"])


if __name__ == "__main__":
    # asyncio.run(main())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
