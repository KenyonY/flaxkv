"""
异步文件传输工具（基于 asyncio）

提供异步并发的文件上传/下载功能，性能更高、代码更清晰
"""

import asyncio
import hashlib
from pathlib import Path
from typing import Dict, Optional
from flaxkv2.client.async_zmq_client import AsyncRemoteDBDict


def calculate_file_hash(file_path: str) -> str:
    """计算文件 SHA256 哈希值"""
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(1024 * 1024)  # 1MB chunks
            if not data:
                break
            sha256.update(data)
    return sha256.hexdigest()


def format_size(size_bytes: int) -> str:
    """格式化文件大小"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


async def upload_large_file_async(
    db_name: str,
    server_url: str,
    key: str,
    file_path: str,
    chunk_size: int = 10 * 1024 * 1024,
    max_concurrency: int = 16,
    show_progress: bool = True,
    verify: bool = True,
    password: Optional[str] = None,
    enable_encryption: bool = False
) -> Dict:
    """
    异步并发上传大文件

    Args:
        db_name: 数据库名称
        server_url: 服务器地址 (tcp://host:port)
        key: 存储键名
        file_path: 本地文件路径
        chunk_size: 分块大小（默认10MB）
        max_concurrency: 最大并发数（默认16）
        show_progress: 是否显示进度
        verify: 是否验证哈希
        password: 加密密码
        enable_encryption: 是否启用加密

    Returns:
        上传信息字典

    示例:
        import asyncio
        from flaxkv2.utils.async_file_transfer import upload_large_file_async

        async def main():
            info = await upload_large_file_async(
                'default_db',
                'tcp://127.0.0.1:25555',
                'my_file',
                '/path/to/file.bin',
                password='yao',
                enable_encryption=True
            )
            print(f"上传完成: {info['filename']}")

        asyncio.run(main())
    """
    import time

    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")

    if not file_path.is_file():
        raise ValueError(f"不是文件: {file_path}")

    # 文件信息
    file_size = file_path.stat().st_size
    total_chunks = (file_size + chunk_size - 1) // chunk_size

    # 计算哈希
    file_hash = None
    if verify:
        if show_progress:
            print(f"🔍 计算文件哈希值...")
        file_hash = calculate_file_hash(str(file_path))
        if show_progress:
            print(f"   SHA256: {file_hash}")

    # 元数据
    metadata = {
        'type': 'chunked_file',
        'filename': file_path.name,
        'size': file_size,
        'chunks': total_chunks,
        'chunk_size': chunk_size,
        'upload_time': None,
    }

    if file_hash:
        metadata['hash'] = file_hash
        metadata['hash_algorithm'] = 'sha256'

    # 连接数据库
    async with AsyncRemoteDBDict(
        db_name,
        server_url,
        timeout=60000,
        enable_encryption=enable_encryption,
        password=password,
        derive_from_password=True
    ) as db:

        # 保存元数据
        metadata['status'] = 'uploading'
        await db.set(f"{key}:meta", metadata)

        # 开始上传
        try:
            if show_progress:
                print(f"\n📤 异步并发上传文件: {file_path.name}")
                print(f"   大小: {format_size(file_size)}")
                print(f"   分块: {total_chunks} 个 (每块 {format_size(chunk_size)})")
                print(f"   并发数: {max_concurrency}\n")

            start_time = time.time()

            # 读取所有chunks
            chunks_data = []
            with open(file_path, 'rb') as f:
                for i in range(total_chunks):
                    chunk = f.read(chunk_size)
                    chunks_data.append((i, chunk))

            # 上传进度（使用锁保护计数器）
            uploaded_count = 0
            progress_lock = asyncio.Lock()

            async def upload_chunk(chunk_index: int, chunk_data: bytes):
                """上传单个chunk"""
                nonlocal uploaded_count

                # 上传数据（无锁，允许真正的并发）
                await db.set(f"{key}:chunk:{chunk_index}", chunk_data)

                # 更新进度（仅在显示进度时使用锁）
                if show_progress:
                    async with progress_lock:
                        uploaded_count += 1
                        progress = uploaded_count * 100 // total_chunks
                        size_uploaded = min(uploaded_count * chunk_size, file_size)
                        print(f"   进度: [{progress:3d}%] {format_size(size_uploaded)}/{format_size(file_size)}", end='\r')
                else:
                    # 不显示进度时，直接增加计数（可能不准确但无关紧要）
                    uploaded_count += 1

            # 并发上传（限制并发数）
            semaphore = asyncio.Semaphore(max_concurrency)

            async def upload_with_limit(idx, data):
                async with semaphore:
                    await upload_chunk(idx, data)

            tasks = [upload_with_limit(idx, data) for idx, data in chunks_data]
            await asyncio.gather(*tasks)

            if show_progress:
                print()

            # 更新元数据
            upload_time = time.time() - start_time
            metadata['status'] = 'completed'
            metadata['upload_time'] = upload_time
            await db.set(f"{key}:meta", metadata)

            if show_progress:
                throughput = file_size / upload_time / (1024 * 1024)
                print(f"\n✅ 异步上传完成!")
                print(f"   总耗时: {upload_time:.2f}秒")
                print(f"   吞吐量: {throughput:.1f} MB/s\n")

            return metadata

        except Exception as e:
            # 失败清理
            if show_progress:
                print(f"\n❌ 上传失败: {e}")
                print(f"   正在清理...")

            try:
                await db.delete(f"{key}:meta")
                for i in range(total_chunks):
                    try:
                        await db.delete(f"{key}:chunk:{i}")
                    except:
                        pass
            except:
                pass

            raise


async def download_large_file_async(
    db_name: str,
    server_url: str,
    key: str,
    output_path: str,
    max_concurrency: int = 8,
    show_progress: bool = True,
    verify: bool = True,
    password: Optional[str] = None,
    enable_encryption: bool = False
) -> Dict:
    """
    异步并发下载大文件

    Args:
        db_name: 数据库名称
        server_url: 服务器地址
        key: 存储键名
        output_path: 输出文件路径
        max_concurrency: 最大并发数
        show_progress: 是否显示进度
        verify: 是否验证哈希
        password: 加密密码
        enable_encryption: 是否启用加密

    Returns:
        文件信息字典
    """
    import time

    output_path = Path(output_path)

    # 连接数据库
    async with AsyncRemoteDBDict(
        db_name,
        server_url,
        timeout=60000,
        enable_encryption=enable_encryption,
        password=password,
        derive_from_password=True
    ) as db:

        # 获取元数据
        metadata = await db.get(f"{key}:meta")
        if metadata is None:
            raise FileNotFoundError(f"文件不存在: {key}")

        total_chunks = metadata['chunks']
        file_size = metadata['size']

        if show_progress:
            print(f"\n📥 异步并发下载文件: {metadata['filename']}")
            print(f"   大小: {format_size(file_size)}")
            print(f"   分块: {total_chunks} 个")
            print(f"   并发数: {max_concurrency}\n")

        start_time = time.time()

        # 下载进度
        downloaded_count = 0
        download_lock = asyncio.Lock()
        chunks_dict = {}

        async def download_chunk(chunk_index: int):
            """下载单个chunk"""
            nonlocal downloaded_count

            chunk_data = await db.get(f"{key}:chunk:{chunk_index}")

            async with download_lock:
                chunks_dict[chunk_index] = chunk_data
                downloaded_count += 1

                if show_progress:
                    progress = downloaded_count * 100 // total_chunks
                    size_downloaded = min(downloaded_count * metadata['chunk_size'], file_size)
                    print(f"   进度: [{progress:3d}%] {format_size(size_downloaded)}/{format_size(file_size)}", end='\r')

        # 并发下载
        semaphore = asyncio.Semaphore(max_concurrency)

        async def download_with_limit(idx):
            async with semaphore:
                await download_chunk(idx)

        tasks = [download_with_limit(i) for i in range(total_chunks)]
        await asyncio.gather(*tasks)

        if show_progress:
            print()

        # 写入文件
        with open(output_path, 'wb') as f:
            for i in range(total_chunks):
                f.write(chunks_dict[i])

        download_time = time.time() - start_time

        # 验证哈希
        if verify and 'hash' in metadata:
            if show_progress:
                print(f"   验证哈希值...")

            actual_hash = calculate_file_hash(str(output_path))

            if actual_hash != metadata['hash']:
                output_path.unlink()
                raise ValueError(f"文件哈希不匹配！期望: {metadata['hash']}, 实际: {actual_hash}")

            if show_progress:
                print(f"   ✓ 哈希验证通过")

        if show_progress:
            throughput = file_size / download_time / (1024 * 1024)
            print(f"\n✅ 异步下载完成!")
            print(f"   总耗时: {download_time:.2f}秒")
            print(f"   吞吐量: {throughput:.1f} MB/s\n")

        return metadata
