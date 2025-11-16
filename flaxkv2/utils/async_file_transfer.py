"""
异步文件传输工具（基于 asyncio）

提供异步并发的文件上传/下载功能，性能更高、代码更清晰
"""

import hashlib
from pathlib import Path
from typing import Dict, Optional


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
    enable_encryption: bool = False,
    connect_timeout: int = 5000
) -> Dict:
    """
    异步并发上传大文件（使用连接池）

    Args:
        db_name: 数据库名称
        server_url: 服务器地址 (tcp://host:port)
        key: 存储键名
        file_path: 本地文件路径
        chunk_size: 分块大小（默认10MB）
        max_concurrency: 最大并发数（默认16，即连接池大小）
        show_progress: 是否显示进度
        verify: 是否验证哈希
        password: 加密密码
        enable_encryption: 是否启用加密
        connect_timeout: 连接超时时间（毫秒，默认5000）

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
    from flaxkv2.client.connection_pool import upload_large_file_with_pool

    # 直接使用连接池版本
    return await upload_large_file_with_pool(
        db_name=db_name,
        server_url=server_url,
        key=key,
        file_path=file_path,
        chunk_size=chunk_size,
        pool_size=max_concurrency,
        show_progress=show_progress,
        verify=verify,
        password=password,
        enable_encryption=enable_encryption,
        connect_timeout=connect_timeout
    )


async def download_large_file_async(
    db_name: str,
    server_url: str,
    key: str,
    output_path: str,
    max_concurrency: int = 16,
    show_progress: bool = True,
    verify: bool = True,
    password: Optional[str] = None,
    enable_encryption: bool = False,
    connect_timeout: int = 5000
) -> Dict:
    """
    异步并发下载大文件（使用连接池）

    Args:
        db_name: 数据库名称
        server_url: 服务器地址
        key: 存储键名
        output_path: 输出文件路径
        max_concurrency: 最大并发数（默认16，即连接池大小）
        show_progress: 是否显示进度
        verify: 是否验证哈希
        password: 加密密码
        enable_encryption: 是否启用加密
        connect_timeout: 连接超时时间（毫秒，默认5000）

    Returns:
        文件信息字典
    """
    from flaxkv2.client.connection_pool import download_large_file_with_pool

    # 直接使用连接池版本
    return await download_large_file_with_pool(
        db_name=db_name,
        server_url=server_url,
        key=key,
        output_path=output_path,
        pool_size=max_concurrency,
        show_progress=show_progress,
        verify=verify,
        password=password,
        enable_encryption=enable_encryption,
        connect_timeout=connect_timeout
    )
