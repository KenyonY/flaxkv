import time

import zmq

sync = 0
if sync:

    def run_subscriber(subscriber_id, num_messages):
        context = zmq.Context()
        socket = context.socket(zmq.DEALER)
        socket.connect("tcp://localhost:5555")

        # 向ROUTER发送订阅信息
        socket.send(subscriber_id.encode())

        total_delay = 0

        for _ in range(num_messages):
            # 发送消息并记录发送时间
            start_time = time.time()
            # socket.send(b"Test message")
            socket.send(subscriber_id.encode())

            # 接收来自ROUTER的消息
            message = socket.recv()
            # print(message)
            end_time = time.time()

            delay = end_time - start_time
            total_delay += delay

            # 为了避免影响测试结果，不在循环中打印每个消息的延迟

        avg_delay = total_delay / num_messages
        print(
            f"Final {total_delay=} Average Delay after {num_messages} messages: {avg_delay:.6f} seconds"
        )

        socket.close()
        context.term()

    if __name__ == "__main__":
        subscriber_id = "special"
        num_messages = 10000
        run_subscriber(subscriber_id, num_messages)

else:
    import asyncio

    import zmq
    import zmq.asyncio

    try:
        import uvloop

        uvloop.install()
    except:
        ...

    async def send_and_receive(socket, subscriber_id):
        # 发送消息并记录发送时间
        # start_time = asyncio.get_event_loop().time()
        await socket.send(subscriber_id.encode())

        # 接收来自ROUTER的消息
        message = await socket.recv()
        # end_time = asyncio.get_event_loop().time()

        # delay = end_time - start_time
        # return delay

    async def run_subscriber(subscriber_id, num_messages):
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.DEALER)
        socket.connect("tcp://localhost:5555")

        t0 = time.time()
        for _ in range(num_messages // 2):
            delay = await send_and_receive(socket, subscriber_id)
        for _ in range(num_messages // 2):
            delay = await send_and_receive(socket, subscriber_id)

        total_delay = time.time() - t0
        avg_delay = total_delay / num_messages
        print(
            f"Final {total_delay=} Average Delay after {num_messages} messages: {avg_delay:.6f} seconds"
        )

        socket.close()
        context.term()

    if __name__ == "__main__":
        subscriber_id = "special"
        num_messages = 10000

        asyncio.run(run_subscriber(subscriber_id, num_messages))
