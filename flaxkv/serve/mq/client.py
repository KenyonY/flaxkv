import zmq

context = zmq.Context()
socket = context.socket(zmq.SUB)
import os

# ipc_path = "ipc:///tmp/" + os.path.join(os.getcwd(), "my_ipc_socket")
ipc_path = "ipc:///tmp/zmqtest"
socket.connect(ipc_path)

# 订阅所有消息
finance_topic = b'\x01'
socket.setsockopt(zmq.SUBSCRIBE, finance_topic)

while True:
    message = socket.recv()
    print(f"Received message: {message}")
