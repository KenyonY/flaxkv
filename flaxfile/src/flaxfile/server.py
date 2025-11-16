#!/usr/bin/env python3
"""
FlaxFile 异步单端口服务器 - 使用 DEALER/ROUTER 模式
"""

import zmq
import zmq.asyncio
import json
import hashlib
import time
import argparse
import logging
import asyncio
from pathlib import Path
from typing import Optional, Dict
from .crypto import get_password, configure_server_encryption, get_key_fingerprint, derive_server_keypair

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# 存储目录
STORAGE_DIR = Path("zmq_streaming_storage")
STORAGE_DIR.mkdir(exist_ok=True)

# 统计信息
stats = {
    'uploads': 0,
    'downloads': 0,
    'bytes_uploaded': 0,
    'bytes_downloaded': 0
}


class FlaxFileServer:
    """FlaxFile 异步单端口文件传输服务器"""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 25555,
        password: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.password = password

        self.context = zmq.asyncio.Context()
        self.socket = None

        # 存储每个客户端的上传状态
        self.upload_states: Dict[bytes, dict] = {}

    async def start(self):
        """启动服务器"""
        # 获取密码（如果未提供）
        if self.password is None:
            self.password = get_password(
                prompt="请输入服务器密码（用于加密传输）: ",
                allow_empty=True,
                env_var="FLAXFILE_PASSWORD"
            )

        logger.info("="*70)
        logger.info("FlaxFile 异步单端口文件传输服务器 (DEALER/ROUTER)")
        logger.info("="*70)
        logger.info(f"存储目录: {STORAGE_DIR.absolute()}")
        logger.info(f"服务地址: tcp://{self.host}:{self.port}")

        # 创建 ROUTER socket (单端口处理所有通信)
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.RCVBUF, 128 * 1024 * 1024)
        self.socket.setsockopt(zmq.SNDBUF, 128 * 1024 * 1024)
        self.socket.setsockopt(zmq.LINGER, 0)

        # 配置加密
        encryption_enabled = configure_server_encryption(self.socket, self.password)

        self.socket.bind(f"tcp://{self.host}:{self.port}")

        logger.info("="*70)
        logger.info(f"✓ 服务器已启动，监听 {self.host}:{self.port}")
        if self.host == "0.0.0.0":
            logger.warning("  监听所有网卡，允许远程连接")

        # 显示加密状态
        if encryption_enabled:
            _, server_public_key = derive_server_keypair(self.password)
            fingerprint = get_key_fingerprint(server_public_key)
            logger.info(f"🔒 已启用 CurveZMQ 加密")
            logger.info(f"   服务器公钥指纹: {fingerprint}")
        else:
            logger.warning("⚠️  未启用加密 - 数据将明文传输")
            logger.warning("   建议设置 FLAXFILE_PASSWORD 环境变量或交互输入密码")

        logger.info("="*70)
        logger.info("")

        try:
            while True:
                # 接收消息: [identity, b'', command_type, ...args]
                frames = await self.socket.recv_multipart()

                if len(frames) < 3:
                    logger.warning(f"收到无效消息: {len(frames)} frames")
                    continue

                identity = frames[0]
                # frames[1] 是空分隔符
                command = frames[2]

                # 异步处理命令
                asyncio.create_task(self.handle_command(identity, command, frames[3:]))

        except KeyboardInterrupt:
            logger.info("\n服务器停止")
        finally:
            await self.stop()

    async def handle_command(self, identity: bytes, command: bytes, args: list):
        """处理客户端命令"""
        try:
            if command == b'PING':
                await self.socket.send_multipart([identity, b'', b'PONG'])

            elif command == b'UPLOAD_START':
                await self.handle_upload_start(identity, args)

            elif command == b'UPLOAD_CHUNK':
                await self.handle_upload_chunk(identity, args)

            elif command == b'UPLOAD_END':
                await self.handle_upload_end(identity)

            elif command == b'DOWNLOAD':
                await self.handle_download(identity, args)

            elif command == b'DELETE':
                await self.handle_delete(identity, args)

            else:
                logger.warning(f"未知命令: {command}")
                await self.socket.send_multipart([identity, b'', b'ERROR', b'Unknown command'])

        except Exception as e:
            logger.error(f"处理命令失败: {e}")
            try:
                await self.socket.send_multipart([identity, b'', b'ERROR', str(e).encode('utf-8')])
            except:
                pass

    async def handle_upload_start(self, identity: bytes, args: list):
        """开始上传"""
        if len(args) < 2:
            await self.socket.send_multipart([identity, b'', b'ERROR', b'Missing arguments'])
            return

        file_key = args[0].decode('utf-8')
        file_size = int(args[1].decode('utf-8'))

        file_path = STORAGE_DIR / file_key
        file_path.parent.mkdir(parents=True, exist_ok=True)

        f = open(file_path, 'wb')
        hash_obj = hashlib.sha256()

        logger.info(f"📤 上传: {file_key} ({file_size/(1024*1024):.1f} MB)")

        self.upload_states[identity] = {
            'file_key': file_key,
            'file_path': file_path,
            'file': f,
            'bytes_received': 0,
            'expected_size': file_size,
            'hash': hash_obj,
            'start_time': time.time(),
            'chunks_received': 0
        }

        await self.socket.send_multipart([identity, b'', b'OK'])

    async def handle_upload_chunk(self, identity: bytes, args: list):
        """处理上传数据块"""
        if identity not in self.upload_states:
            await self.socket.send_multipart([identity, b'', b'ERROR', b'No active upload'])
            return

        if len(args) < 1:
            await self.socket.send_multipart([identity, b'', b'ERROR', b'No data'])
            return

        upload_state = self.upload_states[identity]
        data = args[0]

        # 写入文件
        upload_state['file'].write(data)
        upload_state['hash'].update(data)
        upload_state['bytes_received'] += len(data)
        upload_state['chunks_received'] += 1

        # 发送ACK确认
        await self.socket.send_multipart([identity, b'', b'ACK'])

        # 打印进度 (每10%)
        if upload_state['expected_size'] > 0:
            progress = upload_state['bytes_received'] / upload_state['expected_size'] * 100
            if int(progress) % 10 == 0 and upload_state['chunks_received'] % 100 == 1:
                logger.info(f"  进度: {progress:.0f}% ({upload_state['bytes_received']/(1024*1024):.1f} MB)")

    async def handle_upload_end(self, identity: bytes):
        """完成上传"""
        if identity not in self.upload_states:
            await self.socket.send_multipart([identity, b'', b'ERROR', b'No active upload'])
            return

        upload_state = self.upload_states.pop(identity)
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

        logger.info(f"✓ 上传完成: {upload_state['file_key']} "
                   f"({upload_state['bytes_received']/(1024*1024):.1f} MB, "
                   f"{throughput:.2f} MB/s, "
                   f"{upload_state['chunks_received']} chunks)")

        await self.socket.send_multipart([identity, b'', b'OK', json.dumps(result).encode('utf-8')])

    async def handle_download(self, identity: bytes, args: list):
        """处理下载请求"""
        if len(args) < 1:
            await self.socket.send_multipart([identity, b'', b'ERROR', b'Missing file_key'])
            return

        file_key = args[0].decode('utf-8')
        file_path = STORAGE_DIR / file_key

        if not file_path.exists():
            await self.socket.send_multipart([identity, b'', b'ERROR', b'File not found'])
            return

        file_size = file_path.stat().st_size
        logger.info(f"📥 下载: {file_key} ({file_size/(1024*1024):.1f} MB)")

        # 发送文件大小
        await self.socket.send_multipart([identity, b'', b'OK', str(file_size).encode('utf-8')])

        # 异步发送文件数据
        asyncio.create_task(self.send_file(identity, file_path, file_key))

    async def send_file(self, identity: bytes, file_path: Path, file_key: str):
        """异步发送文件数据"""
        start_time = time.time()
        bytes_sent = 0
        chunk_size = 4 * 1024 * 1024  # 4MB

        try:
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    await self.socket.send_multipart([identity, b'', b'CHUNK', chunk])
                    bytes_sent += len(chunk)
                    # 给客户端一点时间处理
                    await asyncio.sleep(0.001)

            # 发送结束标记
            await self.socket.send_multipart([identity, b'', b'EOF'])

            download_time = time.time() - start_time
            throughput = (bytes_sent / (1024 * 1024)) / download_time if download_time > 0 else 0

            stats['downloads'] += 1
            stats['bytes_downloaded'] += bytes_sent

            logger.info(f"✓ 下载完成: {file_key} ({bytes_sent/(1024*1024):.1f} MB, {throughput:.2f} MB/s)")

        except Exception as e:
            logger.error(f"下载失败: {e}")
            try:
                await self.socket.send_multipart([identity, b'', b'ERROR', str(e).encode('utf-8')])
            except:
                pass

    async def handle_delete(self, identity: bytes, args: list):
        """删除文件"""
        if len(args) < 1:
            await self.socket.send_multipart([identity, b'', b'ERROR', b'Missing file_key'])
            return

        file_key = args[0].decode('utf-8')
        file_path = STORAGE_DIR / file_key

        if not file_path.exists():
            await self.socket.send_multipart([identity, b'', b'ERROR', b'File not found'])
            return

        try:
            file_size = file_path.stat().st_size
            file_path.unlink()
            logger.info(f"✓ 删除: {file_key} ({file_size/(1024*1024):.1f} MB)")
            await self.socket.send_multipart([identity, b'', b'OK'])
        except Exception as e:
            logger.error(f"删除失败: {e}")
            await self.socket.send_multipart([identity, b'', b'ERROR', str(e).encode('utf-8')])

    async def stop(self):
        """停止服务器"""
        # 关闭所有活跃的上传
        for upload_state in self.upload_states.values():
            try:
                upload_state['file'].close()
            except:
                pass

        if self.socket:
            self.socket.close()
        self.context.term()

        logger.info("")
        logger.info("统计信息:")
        logger.info(f"  上传: {stats['uploads']} 个文件, {stats['bytes_uploaded']/(1024*1024):.1f} MB")
        logger.info(f"  下载: {stats['downloads']} 个文件, {stats['bytes_downloaded']/(1024*1024):.1f} MB")


def main():
    parser = argparse.ArgumentParser(description="FlaxFile Server")
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind')
    parser.add_argument('--port', type=int, default=25555, help='Port to bind')
    parser.add_argument('--password', default=None, help='Password for encryption (or set FLAXFILE_PASSWORD env var)')

    args = parser.parse_args()

    server = FlaxFileServer(host=args.host, port=args.port, password=args.password)
    asyncio.run(server.start())


if __name__ == "__main__":
    main()
