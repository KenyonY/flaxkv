import asyncio

import zmq.asyncio

try:
    import uvloop

    uvloop.install()
except:
    ...


async def run_publisher():
    context = zmq.asyncio.Context()
    socket = context.socket(zmq.ROUTER)
    socket.bind("tcp://*:5555")

    subscriber_info = {}  # 存储订阅者信息

    try:
        while True:
            # 异步接收来自DEALER的消息
            identity, message = await socket.recv_multipart()
            subscriber_info[identity] = message

            # 假设逻辑：如果订阅者发送的信息包含"special", 则向其发送消息
            for subscriber, info in subscriber_info.items():
                if b"special" in info:
                    await socket.send_multipart(
                        [subscriber, b"Special message for you"]
                    )
    except KeyboardInterrupt:
        pass

    socket.close()
    context.term()


if __name__ == "__main__":
    asyncio.run(run_publisher())
