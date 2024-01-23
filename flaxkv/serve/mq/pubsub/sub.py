import sys

import zmq


def run_subscriber(subscriber_id):
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.connect("tcp://localhost:5555")

    # 向ROUTER发送订阅信息
    socket.send(subscriber_id.encode())

    while True:
        try:
            # 接收来自ROUTER的消息
            message = socket.recv()
            print(f"Subscriber {subscriber_id} received: {message.decode()}")
        except KeyboardInterrupt:
            break

    socket.close()
    context.term()


if __name__ == "__main__":
    subscriber_id = "special"
    run_subscriber(subscriber_id)
