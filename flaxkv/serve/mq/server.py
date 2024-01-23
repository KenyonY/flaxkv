# import zmq
import zmq.asyncio
import pickle
import asyncio
from route import *


# context = zmq.Context()
context = zmq.asyncio.Context()
# socket = context.socket(zmq.REP)
socket = context.socket(zmq.ROUTER)
socket.bind("tcp://*:5555")



data_storage = {}

def process_set_data(data: bytes):
    # data_storage.update(pickle.loads(data))
    # print(f"{data_storage=}")
    return "Data updated successfully"

async def main():

    while True:
        message = await socket.recv_multipart()
        identity, route = message[0], message[1]

        data = message[2]
        # print(f"{identity=}")
        # print(route, data)

        if route == b"/set_data":
            # print('lala')
            # await socket.send_string(process_set_data(data))
            await socket.send_multipart([identity, b"Response"])
        elif route == b"healthz":
            await socket.send_string(healthz())
        elif route == b"/disconnect":
            await socket.send(disconnect(data))
        else:
            await socket.send_string("Unknown command")


if __name__ == "__main__":
    import uvloop

    uvloop.install()
    # asyncio.run(main())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
