#!/usr/bin/env python3
"""
ZMQ 流式文件传输服务器 - TCP优化版

适用场景: 跨网络传输（不同服务器）

性能优化:
1. ✅ 使用 PULL socket (简单高效)
2. ✅ 批量接收消息 (减少上下文切换)
3. ✅ 大缓冲区 (128MB)
4. ✅ TCP特定优化
5. ✅ 极简消息协议

预期性能:
- 本地测试: 2000-2500 MB/s
- 1Gbps网络: 110-125 MB/s
- 10Gbps网络: 1000-1200 MB/s

启动方式:
    # 监听所有网卡（允许远程连接）
    python zmq_streaming_server_tcp_optimized.py --host 0.0.0.0 --port 25555

    # 仅本地（安全）
    python zmq_streaming_server_tcp_optimized.py --host 127.0.0.1 --port 25555
"""

import os
import zmq
import time
import hashlib
import json
from pathlib import Path
from typing import Optional
import argparse

# 配置
STORAGE_DIR = Path("zmq_streaming_storage")
STORAGE_DIR.mkdir(exist_ok=True)

# 统计信息
stats = {
    'uploads': 0,
    'downloads': 0,
    'bytes_uploaded': 0,
    'bytes_downloaded': 0,
}


class FlaxFileServer:
    """TCP优化版ZMQ流式文件传输服务器"""

    def __init__(
        self,
        host: str = "0.0.0.0",
        upload_port: int = 25555,
        download_port: int = 25556,
        control_port: int = 25557,
    ):
        self.host = host
        self.upload_port = upload_port
        self.download_port = download_port
        self.control_port = control_port

        # ZMQ上下文
        self.context = zmq.Context()

        # Sockets
        self.upload_socket = None
        self.download_socket = None
        self.control_socket = None

    def start(self):
        """启动服务器"""
        print("="*70)
        print("ZMQ 流式文件传输服务器 (TCP优化版)")
        print("="*70)
        print(f"存储目录: {STORAGE_DIR.absolute()}")
        print(f"上传地址: tcp://{self.host}:{self.upload_port}")
        print(f"下载地址: tcp://{self.host}:{self.download_port}")
        print(f"控制地址: tcp://{self.host}:{self.control_port}")
        print()
        print("优化特性:")
        print("  ✅ PUSH/PULL模式 (单向高速)")
        print("  ✅ 批量接收 (减少上下文切换)")
        print("  ✅ 128MB缓冲区")
        print("  ✅ TCP优化参数")
        print("  ✅ 零拷贝发送")
        print("  ✅ 支持跨网络传输")
        print("="*70)

        # 创建上传socket (PULL)
        self.upload_socket = self.context.socket(zmq.PULL)

        # TCP优化设置
        self.upload_socket.setsockopt(zmq.RCVBUF, 128 * 1024 * 1024)  # 128MB接收缓冲
        self.upload_socket.setsockopt(zmq.RCVHWM, 0)  # 无限高水位标记
        self.upload_socket.setsockopt(zmq.LINGER, 0)
        self.upload_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)  # 启用TCP keepalive
        self.upload_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)  # 5分钟
        self.upload_socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 60)  # 间隔60秒

        self.upload_socket.bind(f"tcp://{self.host}:{self.upload_port}")

        # 创建下载socket (PUSH)
        self.download_socket = self.context.socket(zmq.PUSH)
        self.download_socket.setsockopt(zmq.SNDBUF, 128 * 1024 * 1024)  # 128MB发送缓冲
        self.download_socket.setsockopt(zmq.SNDHWM, 0)
        self.download_socket.setsockopt(zmq.LINGER, 0)
        self.download_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.download_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)
        self.download_socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 60)

        self.download_socket.bind(f"tcp://{self.host}:{self.download_port}")

        # 创建控制socket (REP - 用于下载请求)
        self.control_socket = self.context.socket(zmq.REP)
        self.control_socket.bind(f"tcp://{self.host}:{self.control_port}")

        print(f"\n✓ 服务器已启动，监听 {self.host}")
        if self.host == "0.0.0.0":
            print("  ⚠️  监听所有网卡，允许远程连接")
        else:
            print(f"  监听 {self.host}（仅本地）")
        print("\n等待客户端连接...\n")

        try:
            # 使用poller同时监听上传和控制消息
            poller = zmq.Poller()
            poller.register(self.upload_socket, zmq.POLLIN)
            poller.register(self.control_socket, zmq.POLLIN)

            current_upload = None  # {'file_key': str, 'file': handle, 'bytes': int, 'hash': obj, 'start': time}

            while True:
                socks = dict(poller.poll(timeout=1000))

                # 处理上传数据
                if self.upload_socket in socks:
                    self.handle_upload_data(current_upload)

                # 处理控制消息（下载请求等）
                if self.control_socket in socks:
                    msg = self.control_socket.recv()
                    cmd = json.loads(msg.decode('utf-8'))

                    if cmd['type'] == 'UPLOAD_START':
                        # 开始新的上传
                        current_upload = self.start_upload(cmd['file_key'], cmd['file_size'])
                        self.control_socket.send(b'OK')

                    elif cmd['type'] == 'UPLOAD_END':
                        # 结束上传
                        result = self.finish_upload(current_upload)
                        self.control_socket.send(json.dumps(result).encode('utf-8'))
                        current_upload = None

                    elif cmd['type'] == 'DOWNLOAD':
                        # 处理下载
                        result = self.handle_download(cmd['file_key'])
                        self.control_socket.send(json.dumps(result).encode('utf-8'))

                    elif cmd['type'] == 'DELETE':
                        # 删除文件
                        result = self.handle_delete(cmd['file_key'])
                        self.control_socket.send(json.dumps(result).encode('utf-8'))

                    elif cmd['type'] == 'PING':
                        self.control_socket.send(b'PONG')

        except KeyboardInterrupt:
            print("\n\n服务器停止")
        finally:
            self.stop()

    def start_upload(self, file_key: str, file_size: int) -> dict:
        """开始上传"""
        file_path = STORAGE_DIR / file_key
        file_path.parent.mkdir(parents=True, exist_ok=True)

        f = open(file_path, 'wb')
        hash_obj = hashlib.sha256()

        print(f"📤 开始接收: {file_key} ({file_size/(1024*1024):.1f} MB)")

        return {
            'file_key': file_key,
            'file_path': file_path,
            'file': f,
            'bytes_received': 0,
            'expected_size': file_size,
            'hash': hash_obj,
            'start_time': time.time()
        }

    def handle_upload_data(self, upload_state: Optional[dict]):
        """
        批量接收上传数据

        优化: 使用NOBLOCK批量接收，减少recv调用次数
        """
        if not upload_state:
            # 没有活跃的上传，丢弃数据
            try:
                while True:
                    self.upload_socket.recv(zmq.NOBLOCK)
            except zmq.Again:
                pass
            return

        # 批量接收
        batch_count = 0
        batch_bytes = 0

        try:
            while batch_count < 100:  # 最多批量接收100个消息
                data = self.upload_socket.recv(zmq.NOBLOCK)

                # 写入文件
                upload_state['file'].write(data)
                upload_state['hash'].update(data)
                upload_state['bytes_received'] += len(data)

                batch_count += 1
                batch_bytes += len(data)

        except zmq.Again:
            # 没有更多数据，继续
            pass

        # 可选: 打印进度
        if batch_bytes > 0 and upload_state['expected_size'] > 0:
            progress = upload_state['bytes_received'] / upload_state['expected_size'] * 100
            if int(progress) % 10 == 0 and progress > 0:  # 每10%打印一次
                print(f"  上传进度: {progress:.0f}%", end='\r')

    def finish_upload(self, upload_state: dict) -> dict:
        """完成上传"""
        upload_state['file'].close()

        upload_time = time.time() - upload_state['start_time']
        throughput = (upload_state['bytes_received'] / (1024 * 1024)) / upload_time if upload_time > 0 else 0

        # 更新统计
        stats['uploads'] += 1
        stats['bytes_uploaded'] += upload_state['bytes_received']

        result = {
            'status': 'ok',
            'file_key': upload_state['file_key'],
            'size': upload_state['bytes_received'],
            'time': upload_time,
            'throughput': throughput,
            'sha256': upload_state['hash'].hexdigest()
        }

        print(f"\n✓ 上传完成: {upload_state['file_key']} ({upload_state['bytes_received']/(1024*1024):.1f} MB, {throughput:.2f} MB/s)")

        return result

    def handle_download(self, file_key: str) -> dict:
        """
        处理下载

        流程:
        1. 发送元数据响应
        2. 客户端连接download socket
        3. 服务器流式发送数据
        """
        file_path = STORAGE_DIR / file_key

        if not file_path.exists():
            return {'status': 'error', 'message': 'File not found'}

        file_size = file_path.stat().st_size

        print(f"📥 开始发送: {file_key} ({file_size/(1024*1024):.1f} MB)")

        # 返回元数据
        metadata = {
            'status': 'ok',
            'file_key': file_key,
            'size': file_size,
        }

        # 启动异步发送任务
        import threading
        threading.Thread(target=self._send_file, args=(file_path, file_size), daemon=True).start()

        return metadata

    def _send_file(self, file_path: Path, file_size: int):
        """流式发送文件"""
        start_time = time.time()
        chunk_size = 4 * 1024 * 1024  # 4MB chunks（更大减少往返）

        try:
            with open(file_path, 'rb') as f:
                bytes_sent = 0
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break

                    # 使用零拷贝发送
                    self.download_socket.send(chunk, copy=False, track=False)
                    bytes_sent += len(chunk)

                    # 打印进度
                    if bytes_sent % (40 * 1024 * 1024) == 0:  # 每40MB打印一次
                        progress = bytes_sent / file_size * 100
                        print(f"  下载进度: {progress:.0f}%", end='\r')

                # 发送EOF标记
                self.download_socket.send(b'__EOF__')

            download_time = time.time() - start_time
            throughput = (bytes_sent / (1024 * 1024)) / download_time if download_time > 0 else 0

            # 更新统计
            stats['downloads'] += 1
            stats['bytes_downloaded'] += bytes_sent

            print(f"\n✓ 下载完成: {file_path.name} ({bytes_sent/(1024*1024):.1f} MB, {throughput:.2f} MB/s)")

        except Exception as e:
            print(f"\n✗ 下载失败: {file_path.name} - {e}")

    def handle_delete(self, file_key: str) -> dict:
        """删除文件"""
        file_path = STORAGE_DIR / file_key

        if not file_path.exists():
            return {'status': 'error', 'message': 'File not found'}

        try:
            file_size = file_path.stat().st_size
            file_path.unlink()
            print(f"✓ 删除: {file_key} ({file_size/(1024*1024):.1f} MB)")
            return {'status': 'ok', 'size': file_size}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    def stop(self):
        """停止服务器"""
        if self.upload_socket:
            self.upload_socket.close()

        if self.download_socket:
            self.download_socket.close()

        if self.control_socket:
            self.control_socket.close()

        self.context.term()

        print(f"\n统计信息:")
        print(f"  上传: {stats['uploads']} 个文件, {stats['bytes_uploaded']/(1024*1024):.1f} MB")
        print(f"  下载: {stats['downloads']} 个文件, {stats['bytes_downloaded']/(1024*1024):.1f} MB")


def main():
    parser = argparse.ArgumentParser(description="ZMQ TCP Optimized Streaming File Server")
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind (0.0.0.0 for all interfaces, 127.0.0.1 for local only)')
    parser.add_argument('--upload-port', type=int, default=25555, help='Upload port')
    parser.add_argument('--download-port', type=int, default=25556, help='Download port')
    parser.add_argument('--control-port', type=int, default=25557, help='Control port')

    args = parser.parse_args()

    server = FlaxFileServer(
        host=args.host,
        upload_port=args.upload_port,
        download_port=args.download_port,
        control_port=args.control_port
    )

    server.start()


if __name__ == "__main__":
    main()
