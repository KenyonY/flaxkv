"""
文件传输工具模块

提供文件和文件夹的打包、解包功能，用于通过 FlaxKV 进行远程传输
"""

import os
import tarfile
import json
import tempfile
from pathlib import Path
from typing import Dict, Union, Tuple


class FileTransferUtil:
    """文件传输工具类"""

    @staticmethod
    def pack_file(file_path: str) -> Tuple[bytes, Dict]:
        """
        打包单个文件

        Args:
            file_path: 文件路径

        Returns:
            (文件内容, 元数据字典)
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")

        if not file_path.is_file():
            raise ValueError(f"不是文件: {file_path}")

        # 读取文件内容
        with open(file_path, 'rb') as f:
            content = f.read()

        # 获取文件元数据
        stat = file_path.stat()
        metadata = {
            'type': 'file',
            'name': file_path.name,
            'size': stat.st_size,
            'mode': stat.st_mode,
        }

        return content, metadata

    @staticmethod
    def pack_folder(folder_path: str) -> Tuple[bytes, Dict]:
        """
        打包文件夹（使用 tar.gz 格式）

        Args:
            folder_path: 文件夹路径

        Returns:
            (打包后的内容, 元数据字典)
        """
        folder_path = Path(folder_path)

        if not folder_path.exists():
            raise FileNotFoundError(f"文件夹不存在: {folder_path}")

        if not folder_path.is_dir():
            raise ValueError(f"不是文件夹: {folder_path}")

        # 创建临时 tar.gz 文件
        with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as tmp_file:
            tmp_path = tmp_file.name

        try:
            # 打包文件夹
            with tarfile.open(tmp_path, 'w:gz') as tar:
                tar.add(folder_path, arcname=folder_path.name)

            # 读取打包后的内容
            with open(tmp_path, 'rb') as f:
                content = f.read()

            # 获取元数据
            metadata = {
                'type': 'folder',
                'name': folder_path.name,
                'size': len(content),
            }

            return content, metadata
        finally:
            # 删除临时文件
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    @staticmethod
    def pack(path: str) -> Tuple[bytes, Dict]:
        """
        智能打包文件或文件夹

        Args:
            path: 文件或文件夹路径

        Returns:
            (打包后的内容, 元数据字典)
        """
        path_obj = Path(path)

        if not path_obj.exists():
            raise FileNotFoundError(f"路径不存在: {path}")

        if path_obj.is_file():
            return FileTransferUtil.pack_file(path)
        elif path_obj.is_dir():
            return FileTransferUtil.pack_folder(path)
        else:
            raise ValueError(f"不支持的路径类型: {path}")

    @staticmethod
    def unpack_file(content: bytes, metadata: Dict, output_path: str):
        """
        解包文件

        Args:
            content: 文件内容
            metadata: 元数据字典
            output_path: 输出路径
        """
        output_path = Path(output_path)

        # 如果指定的是目录，使用原始文件名
        if output_path.is_dir():
            output_path = output_path / metadata['name']

        # 创建父目录
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # 写入文件
        with open(output_path, 'wb') as f:
            f.write(content)

        # 恢复文件权限
        if 'mode' in metadata:
            os.chmod(output_path, metadata['mode'])

    @staticmethod
    def unpack_folder(content: bytes, metadata: Dict, output_path: str):
        """
        解包文件夹（从 tar.gz 格式）

        Args:
            content: 打包后的内容
            metadata: 元数据字典
            output_path: 输出路径
        """
        output_path = Path(output_path)

        # 创建临时 tar.gz 文件
        with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            tmp_file.write(content)

        try:
            # 解包到指定目录
            output_path.mkdir(parents=True, exist_ok=True)

            with tarfile.open(tmp_path, 'r:gz') as tar:
                tar.extractall(output_path)
        finally:
            # 删除临时文件
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    @staticmethod
    def unpack(content: bytes, metadata: Dict, output_path: str):
        """
        智能解包文件或文件夹

        Args:
            content: 打包后的内容
            metadata: 元数据字典
            output_path: 输出路径
        """
        if metadata['type'] == 'file':
            FileTransferUtil.unpack_file(content, metadata, output_path)
        elif metadata['type'] == 'folder':
            FileTransferUtil.unpack_folder(content, metadata, output_path)
        else:
            raise ValueError(f"不支持的类型: {metadata['type']}")


def format_size(size_bytes: int) -> str:
    """
    格式化文件大小

    Args:
        size_bytes: 字节数

    Returns:
        格式化后的字符串（如 "1.23 MB"）
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"
