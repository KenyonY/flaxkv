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

# 存储目录（默认值，会在服务器启动时创建）
STORAGE_DIR = Path("zmq_streaming_storage")

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
                env_var="FLAXFILE_PASSWORD",
                is_server=True
            )

        # 创建存储目录（只在服务器启动时创建）
        STORAGE_DIR.mkdir(exist_ok=True)

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

            elif command == b'UPLOAD_START_CONCURRENT':
                await self.handle_upload_start_concurrent(identity, args)

            elif command == b'UPLOAD_CHUNK_CONCURRENT':
                await self.handle_upload_chunk_concurrent(identity, args)

            elif command == b'UPLOAD_END':
                await self.handle_upload_end(identity)

            elif command == b'DOWNLOAD_START_CONCURRENT':
                await self.handle_download_start_concurrent(identity, args)

            elif command == b'DOWNLOAD_CHUNK_CONCURRENT':
                await self.handle_download_chunk_concurrent(identity, args)

            elif command == b'DELETE':
                await self.handle_delete(identity, args)

            elif command == b'LIST':
                await self.handle_list(identity, args)

            else:
                logger.warning(f"未知命令: {command}")
                await self.socket.send_multipart([identity, b'', b'ERROR', b'Unknown command'])

        except Exception as e:
            logger.error(f"处理命令失败: {e}")
            try:
                await self.socket.send_multipart([identity, b'', b'ERROR', str(e).encode('utf-8')])
            except:
                pass

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

    async def handle_upload_start_concurrent(self, identity: bytes, args: list):
        """开始并发上传"""
        if len(args) < 3:
            await self.socket.send_multipart([identity, b'', b'ERROR', b'Missing arguments'])
            return

        file_key = args[0].decode('utf-8')
        file_size = int(args[1].decode('utf-8'))
        max_concurrency = int(args[2].decode('utf-8'))

        file_path = STORAGE_DIR / file_key
        file_path.parent.mkdir(parents=True, exist_ok=True)

        f = open(file_path, 'wb')
        hash_obj = hashlib.sha256()

        logger.info(f"📤 并发上传 (x{max_concurrency}): {file_key} ({file_size/(1024*1024):.1f} MB)")

        self.upload_states[identity] = {
            'file_key': file_key,
            'file_path': file_path,
            'file': f,
            'bytes_received': 0,
            'expected_size': file_size,
            'hash': hash_obj,
            'start_time': time.time(),
            'chunks_received': 0,
            'concurrent': True,
            'chunks': {},  # {chunk_id: data}
            'next_chunk_id': 0,  # 下一个要写入的chunk_id
            'max_concurrency': max_concurrency
        }

        await self.socket.send_multipart([identity, b'', b'OK'])

    async def handle_upload_chunk_concurrent(self, identity: bytes, args: list):
        """处理并发上传的chunk（可能乱序到达）"""
        if identity not in self.upload_states:
            await self.socket.send_multipart([identity, b'', b'ERROR', b'No active upload'])
            return

        if len(args) < 2:
            await self.socket.send_multipart([identity, b'', b'ERROR', b'No data'])
            return

        upload_state = self.upload_states[identity]
        chunk_id = int(args[0].decode('utf-8'))
        data = args[1]

        # 缓存chunk（可能乱序到达）
        upload_state['chunks'][chunk_id] = data

        # 按序写入chunk
        while upload_state['next_chunk_id'] in upload_state['chunks']:
            chunk_data = upload_state['chunks'].pop(upload_state['next_chunk_id'])
            upload_state['file'].write(chunk_data)
            upload_state['hash'].update(chunk_data)
            upload_state['bytes_received'] += len(chunk_data)
            upload_state['chunks_received'] += 1
            upload_state['next_chunk_id'] += 1

        # 发送ACK（带chunk_id）
        await self.socket.send_multipart([identity, b'', b'ACK', args[0]])

        # 打印进度（每10%）
        if upload_state['expected_size'] > 0:
            progress = upload_state['bytes_received'] / upload_state['expected_size'] * 100
            if int(progress) % 10 == 0 and upload_state['chunks_received'] % 100 == 1:
                logger.info(f"  进度: {progress:.0f}% ({upload_state['bytes_received']/(1024*1024):.1f} MB)")

    async def handle_download_start_concurrent(self, identity: bytes, args: list):
        """处理并发下载开始请求"""
        if len(args) < 1:
            await self.socket.send_multipart([identity, b'', b'ERROR', b'Missing file_key'])
            return

        file_key = args[0].decode('utf-8')
        file_path = STORAGE_DIR / file_key

        if not file_path.exists():
            await self.socket.send_multipart([identity, b'', b'ERROR', b'File not found'])
            return

        file_size = file_path.stat().st_size
        chunk_size = 4 * 1024 * 1024  # 4MB
        total_chunks = (file_size + chunk_size - 1) // chunk_size

        logger.info(f"📥 并发下载: {file_key} ({file_size/(1024*1024):.1f} MB, {total_chunks} chunks)")

        # 返回文件信息
        await self.socket.send_multipart([
            identity, b'', b'OK',
            str(file_size).encode('utf-8'),
            str(total_chunks).encode('utf-8'),
            str(chunk_size).encode('utf-8')
        ])

    async def handle_download_chunk_concurrent(self, identity: bytes, args: list):
        """处理并发下载chunk请求"""
        if len(args) < 2:
            await self.socket.send_multipart([identity, b'', b'ERROR', b'Missing arguments'])
            return

        file_key = args[0].decode('utf-8')
        chunk_id = int(args[1].decode('utf-8'))

        file_path = STORAGE_DIR / file_key

        if not file_path.exists():
            await self.socket.send_multipart([identity, b'', b'ERROR', b'File not found'])
            return

        try:
            chunk_size = 4 * 1024 * 1024  # 4MB
            offset = chunk_id * chunk_size

            with open(file_path, 'rb') as f:
                f.seek(offset)
                chunk_data = f.read(chunk_size)

            # 返回chunk数据
            await self.socket.send_multipart([
                identity, b'', b'CHUNK',
                str(chunk_id).encode('utf-8'),
                chunk_data
            ])

        except Exception as e:
            logger.error(f"读取chunk失败: {e}")
            await self.socket.send_multipart([identity, b'', b'ERROR', str(e).encode('utf-8')])

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

    async def handle_list(self, identity: bytes, args: list):
        """列出指定前缀下的所有文件"""
        # 获取前缀（可选）
        prefix = args[0].decode('utf-8') if args else ""

        try:
            files_info = []

            # 遍历存储目录
            for file_path in STORAGE_DIR.rglob('*'):
                if file_path.is_file():
                    # 计算相对路径
                    relative_path = file_path.relative_to(STORAGE_DIR)
                    key = str(relative_path)

                    # 如果指定了前缀，只返回匹配的文件
                    if prefix:
                        # 确保前缀以 / 结尾，避免匹配到前缀相似的其他目录
                        # 例如 'downloads' 应该匹配 'downloads/' 而不是 'downloads_bk/'
                        search_prefix = prefix if prefix.endswith('/') else prefix + '/'
                        if not key.startswith(search_prefix):
                            continue

                    # 获取文件信息
                    stat = file_path.stat()
                    files_info.append({
                        'key': key,
                        'size': stat.st_size,
                        'mtime': stat.st_mtime
                    })

            # 序列化文件列表
            import json
            files_json = json.dumps(files_info).encode('utf-8')

            logger.info(f"📋 列出文件: 前缀='{prefix}', 数量={len(files_info)}")
            await self.socket.send_multipart([identity, b'', b'OK', files_json])

        except Exception as e:
            logger.error(f"列出文件失败: {e}")
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
