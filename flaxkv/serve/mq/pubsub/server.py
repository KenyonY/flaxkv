import os
import time

import zmq

context = zmq.Context()
socket = context.socket(zmq.PUB)
# ipc_path = "ipc://" + os.path.join(os.getcwd(), "my_ipc_socket")
ipc_path = "ipc:///tmp/zmqtest"
socket.bind(ipc_path)

finance_topic = b'\x01'  # 假设 '\x01' 表示财经新闻
sports_topic = b'\x02'  # 假设 '\x02' 表示体育新闻
i = 0
while True:
    # 发送消息
    msg1 = f"111 World!{i}".encode()
    msg2 = f"222 World!{i}".encode()
    socket.send(finance_topic + msg1)
    socket.send(sports_topic + msg2)
    i += 1
    time.sleep(1)
