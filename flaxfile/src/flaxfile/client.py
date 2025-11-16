#!/usr/bin/env python3
"""
ZMQ 流式文件传输客户端 - TCP优化版

适用场景: 跨网络传输（不同服务器）

性能优化:
1. ✅ 使用 PUSH socket (单向高速)
2. ✅ 零拷贝发送 (copy=False)
3. ✅ 大缓冲区 (128MB)
4. ✅ 大chunk减少往返 (4MB)
5. ✅ TCP优化参数

预期性能:
- 本地测试: 2000-2500 MB/s
- 1Gbps网络: 110-125 MB/s
- 10Gbps网络: 1000-1200 MB/s

使用方法:
    # 上传
    python zmq_streaming_client_tcp_optimized.py upload test.bin myfile --server 192.168.1.100

    # 下载
    python zmq_streaming_client_tcp_optimized.py download myfile output.bin --server 192.168.1.100

    # 性能测试
    python zmq_streaming_client_tcp_optimized.py benchmark --server 127.0.0.1
"""

import sys
import zmq
import time
import json
import hashlib
from pathlib import Path
from typing import Dict, Any
import argparse


class FlaxFileClient:
    """TCP优化版ZMQ流式文件传输客户端"""

    def __init__(
        self,
        server_host: str = "127.0.0.1",
        upload_port: int = 25555,
        download_port: int = 25556,
        control_port: int = 25557,
    ):
        self.server_host = server_host
        self.upload_port = upload_port
        self.download_port = download_port
        self.control_port = control_port

        # ZMQ上下文
        self.context = zmq.Context()

        # Socket连接
        self.upload_socket = None
        self.download_socket = None
        self.control_socket = None

    def connect(self):
        """连接到服务器"""
        if self.control_socket:
            return  # 已连接

        # 控制socket (REQ)
        self.control_socket = self.context.socket(zmq.REQ)
        self.control_socket.setsockopt(zmq.RCVTIMEO, 60000)  # 60秒超时
        self.control_socket.setsockopt(zmq.SNDTIMEO, 60000)
        self.control_socket.connect(f"tcp://{self.server_host}:{self.control_port}")

        # 测试连接
        self.ping()
        print(f"✓ 已连接到服务器: {self.server_host}")

    def ping(self):
        """测试连接"""
        cmd = {'type': 'PING'}
        self.control_socket.send(json.dumps(cmd).encode('utf-8'))
        response = self.control_socket.recv()
        if response != b'PONG':
            raise Exception("连接失败")

    def upload_file(
        self,
        file_path: str,
        file_key: str,
        chunk_size: int = 4 * 1024 * 1024,  # 4MB chunks (更大减少往返)
        show_progress: bool = True
    ) -> Dict[str, Any]:
        """
        流式上传文件

        Args:
            file_path: 本地文件路径
            file_key: 服务器端存储键名
            chunk_size: chunk大小 (默认4MB)
            show_progress: 是否显示进度

        Returns:
            上传结果信息
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")

        file_size = file_path.stat().st_size

        if show_progress:
            print(f"\n📤 上传文件: {file_path.name}")
            print(f"   大小: {file_size / (1024*1024):.1f} MB")
            print(f"   服务器: {self.server_host}")

        self.connect()

        # 创建上传socket (PUSH)
        if not self.upload_socket:
            self.upload_socket = self.context.socket(zmq.PUSH)

            # TCP优化设置
            self.upload_socket.setsockopt(zmq.SNDBUF, 128 * 1024 * 1024)  # 128MB发送缓冲
            self.upload_socket.setsockopt(zmq.SNDHWM, 0)  # 无限高水位标记
            self.upload_socket.setsockopt(zmq.LINGER, 0)
            self.upload_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
            self.upload_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)
            self.upload_socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 60)

            self.upload_socket.connect(f"tcp://{self.server_host}:{self.upload_port}")

        start_time = time.time()

        # 1. 发送上传开始请求
        cmd = {
            'type': 'UPLOAD_START',
            'file_key': file_key,
            'file_size': file_size
        }
        self.control_socket.send(json.dumps(cmd).encode('utf-8'))
        response = self.control_socket.recv()

        if response != b'OK':
            raise Exception(f"服务器未就绪: {response}")

        # 2. 流式发送文件数据
        bytes_sent = 0
        last_progress = -1

        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break

                # 零拷贝发送
                self.upload_socket.send(chunk, copy=False, track=False)
                bytes_sent += len(chunk)

                if show_progress:
                    progress = int(bytes_sent / file_size * 100)
                    if progress != last_progress and progress % 5 == 0:  # 每5%打印
                        print(f"   进度: {progress}%", end='\r')
                        last_progress = progress

        if show_progress and last_progress < 100:
            print(f"   进度: 100%", end='\r')

        # 3. 发送上传结束请求
        cmd = {'type': 'UPLOAD_END'}
        self.control_socket.send(json.dumps(cmd).encode('utf-8'))
        response = self.control_socket.recv()

        result = json.loads(response.decode('utf-8'))

        upload_time = time.time() - start_time
        throughput = (file_size / (1024 * 1024)) / upload_time if upload_time > 0 else 0

        if show_progress:
            print(f"\n✓ 上传完成:")
            print(f"   耗时: {upload_time:.2f}秒")
            print(f"   吞吐量: {throughput:.2f} MB/s")
            print(f"   SHA256: {result.get('sha256', 'N/A')[:16]}...")

        return {
            'file_key': file_key,
            'size': file_size,
            'upload_time': upload_time,
            'throughput': throughput,
            'sha256': result.get('sha256')
        }

    def download_file(
        self,
        file_key: str,
        output_path: str,
        show_progress: bool = True
    ) -> Dict[str, Any]:
        """
        流式下载文件

        Args:
            file_key: 服务器端文件键名
            output_path: 本地保存路径
            show_progress: 是否显示进度

        Returns:
            下载结果信息
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if show_progress:
            print(f"\n📥 下载文件: {file_key}")
            print(f"   服务器: {self.server_host}")

        self.connect()

        # 创建下载socket (PULL)
        if not self.download_socket:
            self.download_socket = self.context.socket(zmq.PULL)
            self.download_socket.setsockopt(zmq.RCVBUF, 128 * 1024 * 1024)  # 128MB接收缓冲
            self.download_socket.setsockopt(zmq.RCVHWM, 0)
            self.download_socket.setsockopt(zmq.LINGER, 0)
            self.download_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
            self.download_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)
            self.download_socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 60)

            self.download_socket.connect(f"tcp://{self.server_host}:{self.download_port}")

        start_time = time.time()

        # 1. 发送下载请求
        cmd = {'type': 'DOWNLOAD', 'file_key': file_key}
        self.control_socket.send(json.dumps(cmd).encode('utf-8'))
        response = self.control_socket.recv()

        metadata = json.loads(response.decode('utf-8'))

        if metadata['status'] != 'ok':
            raise Exception(f"下载失败: {metadata.get('message')}")

        file_size = metadata['size']

        if show_progress:
            print(f"   大小: {file_size / (1024*1024):.1f} MB")

        # 2. 流式接收数据
        bytes_received = 0
        hash_obj = hashlib.sha256()
        last_progress = -1

        with open(output_path, 'wb') as f:
            while True:
                data = self.download_socket.recv()

                # 检查EOF
                if data == b'__EOF__':
                    break

                f.write(data)
                bytes_received += len(data)
                hash_obj.update(data)

                if show_progress:
                    progress = int(bytes_received / file_size * 100)
                    if progress != last_progress and progress % 5 == 0:  # 每5%打印
                        print(f"   进度: {progress}%", end='\r')
                        last_progress = progress

        if show_progress and last_progress < 100:
            print(f"   进度: 100%", end='\r')

        download_time = time.time() - start_time
        throughput = (bytes_received / (1024 * 1024)) / download_time if download_time > 0 else 0

        if show_progress:
            print(f"\n✓ 下载完成:")
            print(f"   耗时: {download_time:.2f}秒")
            print(f"   吞吐量: {throughput:.2f} MB/s")
            print(f"   SHA256: {hash_obj.hexdigest()[:16]}...")

        return {
            'file_key': file_key,
            'size': bytes_received,
            'download_time': download_time,
            'throughput': throughput,
            'sha256': hash_obj.hexdigest()
        }

    def delete_file(self, file_key: str) -> bool:
        """删除文件"""
        self.connect()

        cmd = {'type': 'DELETE', 'file_key': file_key}
        self.control_socket.send(json.dumps(cmd).encode('utf-8'))
        response = self.control_socket.recv()

        result = json.loads(response.decode('utf-8'))
        return result['status'] == 'ok'

    def close(self):
        """关闭连接"""
        if self.upload_socket:
            self.upload_socket.close()
            self.upload_socket = None

        if self.download_socket:
            self.download_socket.close()
            self.download_socket = None

        if self.control_socket:
            self.control_socket.close()
            self.control_socket = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def benchmark(
    server_host: str = "127.0.0.1",
    upload_port: int = 25555,
    download_port: int = 25556,
    control_port: int = 25557
):
    """性能测试"""
    import os

    print("="*70)
    print("ZMQ TCP优化版流式传输性能测试")
    print("="*70)

    # 使用已有的500MB测试文件
    test_file = Path("test_data/test_500mb.bin")
    if not test_file.exists():
        print(f"\n创建500MB测试文件...")
        test_file.parent.mkdir(exist_ok=True)
        chunk_size = 10 * 1024 * 1024  # 10MB
        with open(test_file, 'wb') as f:
            for i in range(50):  # 50 * 10MB = 500MB
                f.write(os.urandom(chunk_size))
                if (i + 1) % 10 == 0:
                    print(f"  进度: {(i+1)/50*100:.0f}%", end='\r')
        print(f"\n✓ 测试文件创建完成")

    # 测试TCP优化版
    print("\n" + "="*70)
    print(f"测试: ZMQ流式传输 (TCP优化版) - 服务器: {server_host}:{control_port}")
    print("="*70)

    client = FlaxFileClient(
        server_host=server_host,
        upload_port=upload_port,
        download_port=download_port,
        control_port=control_port
    )

    try:
        # 上传
        upload_result = client.upload_file(
            str(test_file),
            'benchmark_tcp_opt',
            show_progress=True
        )

        # 下载
        download_output = Path("test_data/zmq_tcp_opt_download.bin")
        download_result = client.download_file(
            'benchmark_tcp_opt',
            str(download_output),
            show_progress=True
        )

        # 验证
        if upload_result['sha256'] == download_result['sha256']:
            print(f"\n✓ 哈希验证通过")
        else:
            print(f"\n✗ 哈希验证失败!")
            print(f"  上传: {upload_result['sha256']}")
            print(f"  下载: {download_result['sha256']}")

        # 清理
        client.delete_file('benchmark_tcp_opt')
        if download_output.exists():
            download_output.unlink()

        # 打印汇总
        print("\n" + "="*70)
        print("性能汇总 - ZMQ TCP优化版")
        print("="*70)
        print(f"\n上传:")
        print(f"  吞吐量: {upload_result['throughput']:.2f} MB/s")
        print(f"  耗时: {upload_result['upload_time']:.2f}秒")

        print(f"\n下载:")
        print(f"  吞吐量: {download_result['throughput']:.2f} MB/s")
        print(f"  耗时: {download_result['download_time']:.2f}秒")

        print(f"\n总耗时: {upload_result['upload_time'] + download_result['download_time']:.2f}秒")

    finally:
        client.close()


def main():
    """命令行接口"""
    import sys

    parser = argparse.ArgumentParser(description="ZMQ TCP Optimized Streaming File Client")
    parser.add_argument('command', choices=['upload', 'download', 'delete', 'benchmark'],
                       help='Command to execute')
    parser.add_argument('args', nargs='*', help='Command arguments')
    parser.add_argument('--server', default='127.0.0.1', help='Server host (IP or hostname)')
    parser.add_argument('--upload-port', type=int, default=25555, help='Upload port')
    parser.add_argument('--download-port', type=int, default=25556, help='Download port')
    parser.add_argument('--control-port', type=int, default=25557, help='Control port')

    args = parser.parse_args()

    client = FlaxFileClient(
        server_host=args.server,
        upload_port=args.upload_port,
        download_port=args.download_port,
        control_port=args.control_port
    )

    if args.command == 'upload':
        if len(args.args) < 2:
            print("Usage: upload <file_path> <file_key>")
            sys.exit(1)

        file_path, file_key = args.args[0], args.args[1]
        result = client.upload_file(file_path, file_key)
        print(f"\n结果: 上传成功，吞吐量 {result['throughput']:.2f} MB/s")

    elif args.command == 'download':
        if len(args.args) < 2:
            print("Usage: download <file_key> <output_path>")
            sys.exit(1)

        file_key, output_path = args.args[0], args.args[1]
        result = client.download_file(file_key, output_path)
        print(f"\n结果: 下载成功，吞吐量 {result['throughput']:.2f} MB/s")

    elif args.command == 'delete':
        if len(args.args) < 1:
            print("Usage: delete <file_key>")
            sys.exit(1)

        file_key = args.args[0]
        success = client.delete_file(file_key)
        print(f"\n删除{'成功' if success else '失败'}")

    elif args.command == 'benchmark':
        benchmark(
            server_host=args.server,
            upload_port=args.upload_port,
            download_port=args.download_port,
            control_port=args.control_port
        )

    client.close()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        # 无参数,运行benchmark
        benchmark()
    else:
        main()
