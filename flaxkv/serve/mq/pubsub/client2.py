import zmq

context = zmq.Context()
socket = context.socket(zmq.SUB)
import os

# ipc_path = "ipc:///tmp/" + os.path.join(os.getcwd(), "my_ipc_socket")
ipc_path = "ipc:///tmp/zmqtest"
socket.connect(ipc_path)


sports_topic = b'\x02'  # 假设 '\x02' 表示体育新闻
socket.setsockopt(zmq.SUBSCRIBE, sports_topic)

while True:
    message = socket.recv()
    print(f"Received message: {message}")
