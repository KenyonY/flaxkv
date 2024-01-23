import os

use_tcp = 0
sync = 0

if sync:
    import zmq
    context = zmq.Context()
    socket = context.socket(zmq.REP)  # REP (REPLY) socket for request-reply pattern
else:
    import zmq.asyncio
    import asyncio
    import os

    try:
        import uvloop

        uvloop.install()
    except:
        ...
    context = zmq.asyncio.Context()
    socket = context.socket(zmq.REP)

if use_tcp:
    socket.bind("tcp://*:5555")
else:
    ipc_path = "/tmp/feeds/0"
    os.makedirs(os.path.dirname(ipc_path), exist_ok=True)
    socket.bind(f"ipc://{ipc_path}")

if sync:
    while True:
        message = socket.recv()  # Receive a message
        socket.send(b"World")    # Send a reply

else:
    async def main():

        while True:
            message = await socket.recv()  # Receive a message
            await socket.send(b"World")  # Send a reply

    asyncio.run(main())
