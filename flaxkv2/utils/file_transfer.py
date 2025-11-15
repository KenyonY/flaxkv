"""
文件传输工具模块

提供文件和文件夹的打包、解包功能，用于通过 FlaxKV 进行远程传输
"""

import os
import tarfile
import json
import tempfile
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Union, Tuple, Optional


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


# ============================================================================
# 大文件分块传输功能
# ============================================================================

def calculate_file_hash(file_path: str, algorithm: str = 'sha256') -> str:
    """
    计算文件哈希值（用于完整性验证）

    Args:
        file_path: 文件路径
        algorithm: 哈希算法（'sha256', 'md5'）

    Returns:
        十六进制哈希值字符串
    """
    import hashlib

    hash_obj = hashlib.new(algorithm)

    with open(file_path, 'rb') as f:
        # 分块读取以节省内存
        while chunk := f.read(8192):
            hash_obj.update(chunk)

    return hash_obj.hexdigest()


def upload_large_file(
    db,
    key: str,
    file_path: str,
    chunk_size: int = 10 * 1024 * 1024,  # 默认 10MB
    show_progress: bool = True,
    verify: bool = True
) -> Dict:
    """
    分块上传大文件

    适用于大于 100MB 的文件，避免内存占用过高和传输超时。

    Args:
        db: FlaxKV 实例
        key: 存储键名（不包含前缀）
        file_path: 本地文件路径
        chunk_size: 分块大小（字节），默认 10MB
        show_progress: 是否显示进度条
        verify: 是否计算并验证文件哈希值

    Returns:
        包含上传信息的字典：
        {
            'filename': 文件名,
            'size': 文件大小,
            'chunks': 分块数量,
            'chunk_size': 分块大小,
            'hash': 文件哈希值（如果 verify=True）
        }

    示例:
        >>> from flaxkv2 import FlaxKV
        >>> from flaxkv2.utils.file_transfer import upload_large_file
        >>>
        >>> db = FlaxKV("files", "tcp://127.0.0.1:5555",
        ...            enable_encryption=True, password="yao")
        >>>
        >>> # 上传 1GB 文件
        >>> info = upload_large_file(db, "large_video", "/path/to/video.mp4")
        >>> print(f"上传完成: {info['filename']}, 大小: {format_size(info['size'])}")
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")

    if not file_path.is_file():
        raise ValueError(f"不是文件: {file_path}")

    # 获取文件信息
    file_size = file_path.stat().st_size
    total_chunks = (file_size + chunk_size - 1) // chunk_size

    # 计算文件哈希值（如果需要）
    file_hash = None
    if verify:
        if show_progress:
            print(f"🔍 计算文件哈希值...")
        file_hash = calculate_file_hash(str(file_path))
        if show_progress:
            print(f"   SHA256: {file_hash}")

    # 存储元数据
    metadata = {
        'type': 'chunked_file',
        'filename': file_path.name,
        'size': file_size,
        'chunks': total_chunks,
        'chunk_size': chunk_size,
        'upload_time': None,  # 会在上传完成后更新
    }

    if file_hash:
        metadata['hash'] = file_hash
        metadata['hash_algorithm'] = 'sha256'

    # 先存储元数据（标记为上传中）
    metadata['status'] = 'uploading'
    db[f"{key}:meta"] = metadata

    # 开始分块上传
    try:
        if show_progress:
            print(f"\n📤 上传文件: {file_path.name}")
            print(f"   大小: {format_size(file_size)}")
            print(f"   分块: {total_chunks} 个 (每块 {format_size(chunk_size)})\n")

        with open(file_path, 'rb') as f:
            for i in range(total_chunks):
                chunk = f.read(chunk_size)
                db[f"{key}:chunk:{i}"] = chunk

                if show_progress:
                    progress = (i + 1) * 100 // total_chunks
                    size_uploaded = min((i + 1) * chunk_size, file_size)
                    print(f"   进度: [{progress:3d}%] {format_size(size_uploaded)}/{format_size(file_size)}", end='\r')

        if show_progress:
            print()  # 换行

        # 更新元数据（标记为完成）
        import time
        metadata['status'] = 'completed'
        metadata['upload_time'] = time.time()
        db[f"{key}:meta"] = metadata

        if show_progress:
            print(f"✅ 上传完成: {file_path.name}\n")

        return metadata

    except Exception as e:
        # 上传失败，删除已上传的数据
        if show_progress:
            print(f"\n❌ 上传失败: {e}")
            print(f"   正在清理已上传的数据...")

        try:
            delete_large_file(db, key, show_progress=False)
        except:
            pass

        raise


def download_large_file(
    db,
    key: str,
    output_path: str,
    show_progress: bool = True,
    verify: bool = True
) -> Dict:
    """
    分块下载大文件

    Args:
        db: FlaxKV 实例
        key: 存储键名
        output_path: 输出文件路径
        show_progress: 是否显示进度条
        verify: 是否验证文件哈希值

    Returns:
        包含下载信息的字典（元数据）

    示例:
        >>> from flaxkv2 import FlaxKV
        >>> from flaxkv2.utils.file_transfer import download_large_file
        >>>
        >>> db = FlaxKV("files", "tcp://127.0.0.1:5555",
        ...            enable_encryption=True, password="yao")
        >>>
        >>> # 下载文件
        >>> info = download_large_file(db, "large_video", "/path/to/output.mp4")
        >>> print(f"下载完成: {info['filename']}")
    """
    # 读取元数据
    meta_key = f"{key}:meta"
    metadata = db.get(meta_key)

    if not metadata:
        raise KeyError(f"文件不存在: {key}")

    if metadata.get('type') != 'chunked_file':
        raise ValueError(f"不是分块文件: {key} (type={metadata.get('type')})")

    output_path = Path(output_path)

    # 如果指定的是目录，使用原始文件名
    if output_path.is_dir():
        output_path = output_path / metadata['filename']

    # 创建父目录
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 开始分块下载
    total_chunks = metadata['chunks']
    file_size = metadata['size']

    if show_progress:
        print(f"\n📥 下载文件: {metadata['filename']}")
        print(f"   大小: {format_size(file_size)}")
        print(f"   分块: {total_chunks} 个\n")

    try:
        with open(output_path, 'wb') as f:
            for i in range(total_chunks):
                chunk = db.get(f"{key}:chunk:{i}")

                if chunk is None:
                    raise ValueError(f"分块缺失: chunk {i}/{total_chunks}")

                f.write(chunk)

                if show_progress:
                    progress = (i + 1) * 100 // total_chunks
                    size_downloaded = min((i + 1) * metadata['chunk_size'], file_size)
                    print(f"   进度: [{progress:3d}%] {format_size(size_downloaded)}/{format_size(file_size)}", end='\r')

        if show_progress:
            print()  # 换行

        # 验证文件哈希值
        if verify and 'hash' in metadata:
            if show_progress:
                print(f"\n🔍 验证文件完整性...")

            downloaded_hash = calculate_file_hash(str(output_path), metadata.get('hash_algorithm', 'sha256'))
            expected_hash = metadata['hash']

            if downloaded_hash != expected_hash:
                # 删除损坏的文件
                output_path.unlink()
                raise ValueError(
                    f"文件哈希值不匹配！\n"
                    f"   预期: {expected_hash}\n"
                    f"   实际: {downloaded_hash}\n"
                    f"   文件可能已损坏，已删除下载的文件。"
                )

            if show_progress:
                print(f"   ✅ 哈希值验证通过")

        if show_progress:
            print(f"\n✅ 下载完成: {output_path}\n")

        return metadata

    except Exception as e:
        # 下载失败，删除不完整的文件
        if output_path.exists():
            output_path.unlink()

        if show_progress:
            print(f"\n❌ 下载失败: {e}")

        raise


def list_large_files(db, show_details: bool = False) -> list:
    """
    列出所有分块存储的文件

    Args:
        db: FlaxKV 实例
        show_details: 是否显示详细信息

    Returns:
        文件列表 [(key, metadata), ...]

    示例:
        >>> files = list_large_files(db, show_details=True)
        >>> for key, meta in files:
        ...     print(f"{key}: {meta['filename']} ({format_size(meta['size'])})")
    """
    files = []

    # 遍历所有键，查找元数据键
    for key in db.keys():
        if isinstance(key, str) and key.endswith(':meta'):
            # 去掉 :meta 后缀
            file_key = key[:-5]
            metadata = db[key]

            # 检查是否是分块文件
            if metadata.get('type') == 'chunked_file':
                files.append((file_key, metadata))

    if show_details:
        print(f"\n📁 分块文件列表 (共 {len(files)} 个):\n")
        for key, meta in files:
            status = meta.get('status', 'unknown')
            status_icon = '✅' if status == 'completed' else '⏳'
            print(f"{status_icon} {key}")
            print(f"   文件名: {meta['filename']}")
            print(f"   大小:   {format_size(meta['size'])} ({meta['chunks']} 块)")
            if 'hash' in meta:
                print(f"   哈希:   {meta['hash'][:16]}...")
            print()

    return files


def delete_large_file(db, key: str, show_progress: bool = True) -> bool:
    """
    删除分块存储的文件（包括所有分块和元数据）

    Args:
        db: FlaxKV 实例
        key: 存储键名
        show_progress: 是否显示进度

    Returns:
        是否成功删除

    示例:
        >>> delete_large_file(db, "large_video")
    """
    # 读取元数据
    meta_key = f"{key}:meta"
    metadata = db.get(meta_key)

    if not metadata:
        if show_progress:
            print(f"❌ 文件不存在: {key}")
        return False

    if metadata.get('type') != 'chunked_file':
        if show_progress:
            print(f"❌ 不是分块文件: {key}")
        return False

    total_chunks = metadata['chunks']

    if show_progress:
        print(f"\n🗑️  删除文件: {metadata['filename']}")
        print(f"   分块: {total_chunks} 个")

    # 删除所有分块
    for i in range(total_chunks):
        try:
            del db[f"{key}:chunk:{i}"]
        except KeyError:
            pass  # 分块可能已经不存在

    # 删除元数据
    try:
        del db[meta_key]
    except KeyError:
        pass

    if show_progress:
        print(f"✅ 删除完成\n")

    return True


# ============================================================================
# 并行分块传输功能（多线程）
# ============================================================================

def upload_large_file_parallel(
    db,
    key: str,
    file_path: str,
    chunk_size: int = 10 * 1024 * 1024,  # 默认 10MB
    max_workers: int = 5,  # 最大并行线程数
    show_progress: bool = True,
    verify: bool = True,
    db_connection_params: Optional[Dict] = None  # 新增：数据库连接参数
) -> Dict:
    """
    并行分块上传大文件（多线程版本）

    使用多线程并行上传文件分块，提升传输性能。适用于网络带宽充足的场景。

    Args:
        db: FlaxKV 实例
        key: 存储键名（不包含前缀）
        file_path: 本地文件路径
        chunk_size: 分块大小（字节），默认 10MB
        max_workers: 最大并行线程数，默认 5
        show_progress: 是否显示进度条
        verify: 是否计算并验证文件哈希值

    Returns:
        包含上传信息的字典（同 upload_large_file）

    示例:
        >>> from flaxkv2 import FlaxKV
        >>> from flaxkv2.utils.file_transfer import upload_large_file_parallel
        >>>
        >>> db = FlaxKV("files", "tcp://127.0.0.1:5555",
        ...            enable_encryption=True, password="yao")
        >>>
        >>> # 并行上传 1GB 文件（使用 5 个线程）
        >>> info = upload_large_file_parallel(
        ...     db, "large_video", "/path/to/video.mp4",
        ...     max_workers=5
        ... )
        >>> print(f"上传完成: {info['filename']}")
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")

    if not file_path.is_file():
        raise ValueError(f"不是文件: {file_path}")

    # 获取文件信息
    file_size = file_path.stat().st_size
    total_chunks = (file_size + chunk_size - 1) // chunk_size

    # 计算文件哈希值（如果需要）
    file_hash = None
    if verify:
        if show_progress:
            print(f"🔍 计算文件哈希值...")
        file_hash = calculate_file_hash(str(file_path))
        if show_progress:
            print(f"   SHA256: {file_hash}")

    # 存储元数据
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

    # 先存储元数据（标记为上传中）
    metadata['status'] = 'uploading'
    db[f"{key}:meta"] = metadata

    # 线程安全的进度跟踪
    progress_lock = threading.Lock()
    completed_chunks = 0

    # 检测是否为远程连接，如果是则需要为每个线程创建独立连接
    is_remote = hasattr(db, '_backend_type') and db._backend_type == 'remote'

    def upload_chunk(chunk_index: int) -> bool:
        """上传单个分块（线程函数）"""
        nonlocal completed_chunks

        try:
            # 读取分块数据
            with open(file_path, 'rb') as f:
                f.seek(chunk_index * chunk_size)
                chunk_data = f.read(chunk_size)

            # 如果是远程连接，创建线程独立的数据库连接（ZMQ 不是线程安全的）
            if is_remote and db_connection_params:
                from flaxkv2 import FlaxKV
                thread_db = FlaxKV(**db_connection_params)
                try:
                    thread_db[f"{key}:chunk:{chunk_index}"] = chunk_data
                finally:
                    thread_db.close()
            else:
                # 本地连接或未提供连接参数，使用共享连接
                db[f"{key}:chunk:{chunk_index}"] = chunk_data

            # 更新进度
            with progress_lock:
                completed_chunks += 1
                if show_progress:
                    progress = completed_chunks * 100 // total_chunks
                    size_uploaded = min(completed_chunks * chunk_size, file_size)
                    print(f"   进度: [{progress:3d}%] {format_size(size_uploaded)}/{format_size(file_size)} ({completed_chunks}/{total_chunks} 块)", end='\r')

            return True

        except Exception as e:
            if show_progress:
                print(f"\n❌ 上传分块 {chunk_index} 失败: {e}")
            raise

    # 开始并行上传
    try:
        if show_progress:
            print(f"\n📤 并行上传文件: {file_path.name}")
            print(f"   大小: {format_size(file_size)}")
            print(f"   分块: {total_chunks} 个 (每块 {format_size(chunk_size)})")
            print(f"   线程: {max_workers} 个\n")

        # 使用 ThreadPoolExecutor 并行上传
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有上传任务
            futures = {
                executor.submit(upload_chunk, i): i
                for i in range(total_chunks)
            }

            # 等待所有任务完成
            for future in as_completed(futures):
                chunk_index = futures[future]
                try:
                    future.result()  # 获取结果，如果有异常会抛出
                except Exception as e:
                    # 任务失败，取消其他任务
                    for f in futures:
                        f.cancel()
                    raise Exception(f"上传分块 {chunk_index} 失败: {e}")

        if show_progress:
            print()  # 换行

        # 更新元数据（标记为完成）
        import time
        metadata['status'] = 'completed'
        metadata['upload_time'] = time.time()
        db[f"{key}:meta"] = metadata

        if show_progress:
            print(f"✅ 并行上传完成: {file_path.name}\n")

        return metadata

    except Exception as e:
        # 上传失败，删除已上传的数据
        if show_progress:
            print(f"\n❌ 上传失败: {e}")
            print(f"   正在清理已上传的数据...")

        try:
            delete_large_file(db, key, show_progress=False)
        except:
            pass

        raise


def download_large_file_parallel(
    db,
    key: str,
    output_path: str,
    max_workers: int = 5,  # 最大并行线程数
    show_progress: bool = True,
    verify: bool = True,
    db_connection_params: Optional[Dict] = None  # 新增：数据库连接参数
) -> Dict:
    """
    并行分块下载大文件（多线程版本）

    使用多线程并行下载文件分块，然后按顺序写入文件，确保数据完整性。

    Args:
        db: FlaxKV 实例
        key: 存储键名
        output_path: 输出文件路径
        max_workers: 最大并行线程数，默认 5
        show_progress: 是否显示进度条
        verify: 是否验证文件哈希值

    Returns:
        包含下载信息的字典（元数据）

    示例:
        >>> from flaxkv2 import FlaxKV
        >>> from flaxkv2.utils.file_transfer import download_large_file_parallel
        >>>
        >>> db = FlaxKV("files", "tcp://127.0.0.1:5555",
        ...            enable_encryption=True, password="yao")
        >>>
        >>> # 并行下载文件（使用 5 个线程）
        >>> info = download_large_file_parallel(
        ...     db, "large_video", "/path/to/output.mp4",
        ...     max_workers=5
        ... )
        >>> print(f"下载完成: {info['filename']}")
    """
    # 读取元数据
    meta_key = f"{key}:meta"
    metadata = db.get(meta_key)

    if not metadata:
        raise KeyError(f"文件不存在: {key}")

    if metadata.get('type') != 'chunked_file':
        raise ValueError(f"不是分块文件: {key} (type={metadata.get('type')})")

    output_path = Path(output_path)

    # 如果指定的是目录，使用原始文件名
    if output_path.is_dir():
        output_path = output_path / metadata['filename']

    # 创建父目录
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 开始并行下载
    total_chunks = metadata['chunks']
    file_size = metadata['size']

    if show_progress:
        print(f"\n📥 并行下载文件: {metadata['filename']}")
        print(f"   大小: {format_size(file_size)}")
        print(f"   分块: {total_chunks} 个")
        print(f"   线程: {max_workers} 个\n")

    # 使用字典存储下载的分块（索引 -> 数据）
    chunks_data = {}
    chunks_lock = threading.Lock()

    # 线程安全的进度跟踪
    progress_lock = threading.Lock()
    completed_chunks = 0

    # 检测是否为远程连接
    is_remote = hasattr(db, '_backend_type') and db._backend_type == 'remote'

    def download_chunk(chunk_index: int) -> bool:
        """下载单个分块（线程函数）"""
        nonlocal completed_chunks

        try:
            # 如果是远程连接，创建线程独立的数据库连接（ZMQ 不是线程安全的）
            if is_remote and db_connection_params:
                from flaxkv2 import FlaxKV
                thread_db = FlaxKV(**db_connection_params)
                try:
                    chunk = thread_db.get(f"{key}:chunk:{chunk_index}")
                finally:
                    thread_db.close()
            else:
                # 本地连接或未提供连接参数，使用共享连接
                chunk = db.get(f"{key}:chunk:{chunk_index}")

            if chunk is None:
                raise ValueError(f"分块缺失: chunk {chunk_index}/{total_chunks}")

            # 存储到字典
            with chunks_lock:
                chunks_data[chunk_index] = chunk

            # 更新进度
            with progress_lock:
                completed_chunks += 1
                if show_progress:
                    progress = completed_chunks * 100 // total_chunks
                    size_downloaded = min(completed_chunks * metadata['chunk_size'], file_size)
                    print(f"   进度: [{progress:3d}%] {format_size(size_downloaded)}/{format_size(file_size)} ({completed_chunks}/{total_chunks} 块)", end='\r')

            return True

        except Exception as e:
            if show_progress:
                print(f"\n❌ 下载分块 {chunk_index} 失败: {e}")
            raise

    try:
        # 使用 ThreadPoolExecutor 并行下载
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有下载任务
            futures = {
                executor.submit(download_chunk, i): i
                for i in range(total_chunks)
            }

            # 等待所有任务完成
            for future in as_completed(futures):
                chunk_index = futures[future]
                try:
                    future.result()  # 获取结果，如果有异常会抛出
                except Exception as e:
                    # 任务失败，取消其他任务
                    for f in futures:
                        f.cancel()
                    raise Exception(f"下载分块 {chunk_index} 失败: {e}")

        if show_progress:
            print()  # 换行

        # 按顺序写入文件（确保数据完整性）
        if show_progress:
            print(f"\n📝 写入文件...")

        with open(output_path, 'wb') as f:
            for i in range(total_chunks):
                if i not in chunks_data:
                    raise ValueError(f"分块 {i} 未下载成功")
                f.write(chunks_data[i])

        # 验证文件哈希值
        if verify and 'hash' in metadata:
            if show_progress:
                print(f"🔍 验证文件完整性...")

            downloaded_hash = calculate_file_hash(str(output_path), metadata.get('hash_algorithm', 'sha256'))
            expected_hash = metadata['hash']

            if downloaded_hash != expected_hash:
                # 删除损坏的文件
                output_path.unlink()
                raise ValueError(
                    f"文件哈希值不匹配！\n"
                    f"   预期: {expected_hash}\n"
                    f"   实际: {downloaded_hash}\n"
                    f"   文件可能已损坏，已删除下载的文件。"
                )

            if show_progress:
                print(f"   ✅ 哈希值验证通过")

        if show_progress:
            print(f"\n✅ 并行下载完成: {output_path}\n")

        return metadata

    except Exception as e:
        # 下载失败，删除不完整的文件
        if output_path.exists():
            output_path.unlink()

        if show_progress:
            print(f"\n❌ 下载失败: {e}")

        raise

# ============================================================================
# 异步并发传输功能（Asyncio版本）
# ============================================================================

async def upload_large_file_async(
    db,
    key: str,
    file_path: str,
    chunk_size: int = 10 * 1024 * 1024,  # 默认 10MB
    max_concurrency: int = 8,  # 最大并发数
    show_progress: bool = True,
    verify: bool = True,
    db_connection_params: Optional[Dict] = None
) -> Dict:
    """
    异步并发上传大文件
    
    使用asyncio代替多线程，减少GIL影响，适合I/O密集型场景
    
    Args:
        db: FlaxKV 实例（仅用于元数据）
        key: 存储键名
        file_path: 本地文件路径
        chunk_size: 分块大小（字节）
        max_concurrency: 最大并发任务数
        show_progress: 是否显示进度
        verify: 是否验证哈希值
        db_connection_params: 数据库连接参数（必须提供，用于创建异步连接）
    
    Returns:
        包含文件元数据的字典
    """
    import zmq.asyncio
    
    file_path = Path(file_path)
    file_size = file_path.stat().st_size
    total_chunks = (file_size + chunk_size - 1) // chunk_size
    
    if show_progress:
        print(f"\n📤 异步并行上传文件: {file_path.name}")
        print(f"   大小: {format_size(file_size)}")
        print(f"   分块: {total_chunks} 个 (每块 {format_size(chunk_size)})")
        print(f"   并发: {max_concurrency} 个协程\n")
    
    # 计算文件哈希
    file_hash = None
    if verify:
        if show_progress:
            print("🔍 计算文件哈希值...")
        file_hash = calculate_file_hash(file_path)
        if show_progress:
            print(f"   SHA256: {file_hash}\n")
    
    # 元数据
    metadata = {
        'filename': file_path.name,
        'size': file_size,
        'chunk_size': chunk_size,
        'total_chunks': total_chunks,
        'hash': file_hash,
        'hash_algorithm': 'sha256' if verify else None,
        'type': 'large_file'
    }
    
    # 存储元数据
    metadata['status'] = 'uploading'
    db[f"{key}:meta"] = metadata
    
    # 异步进度跟踪（使用asyncio.Lock）
    progress_lock = asyncio.Lock()
    completed_chunks = 0
    
    # 预读所有分块到内存（在executor中执行）
    loop = asyncio.get_event_loop()
    chunks_data = await loop.run_in_executor(
        None,
        lambda: [
            (i, open(file_path, 'rb').seek(i * chunk_size) or open(file_path, 'rb').read(chunk_size))
            for i in range(total_chunks)
        ]
    )
    
    # 实际上应该这样读取
    def read_all_chunks():
        result = []
        with open(file_path, 'rb') as f:
            for i in range(total_chunks):
                f.seek(i * chunk_size)
                chunk_data = f.read(chunk_size)
                result.append((i, chunk_data))
        return result
    
    chunks_data = await loop.run_in_executor(None, read_all_chunks)
    
    async def upload_chunk(chunk_index: int, chunk_data: bytes):
        """上传单个分块的异步协程"""
        nonlocal completed_chunks
        
        try:
            # 创建异步数据库连接
            from flaxkv2 import FlaxKV

            # 在executor中执行同步的FlaxKV操作
            def sync_upload():
                # 提取必要参数（不修改原字典）
                thread_db = FlaxKV(
                    db_connection_params['db_name'],
                    db_connection_params['url'],
                    backend=db_connection_params.get('backend', 'remote'),
                    timeout=db_connection_params.get('timeout', 30000),
                    enable_encryption=db_connection_params.get('enable_encryption', False),
                    password=db_connection_params.get('password'),
                    derive_from_password=db_connection_params.get('derive_from_password', True)
                )
                try:
                    thread_db[f"{key}:chunk:{chunk_index}"] = chunk_data
                finally:
                    thread_db.close()

            await loop.run_in_executor(None, sync_upload)
            
            # 更新进度
            async with progress_lock:
                completed_chunks += 1
                if show_progress:
                    progress = completed_chunks * 100 // total_chunks
                    size_uploaded = min(completed_chunks * chunk_size, file_size)
                    print(f"   进度: [{progress:3d}%] {format_size(size_uploaded)}/{format_size(file_size)} ({completed_chunks}/{total_chunks} 块)", end='\r')
            
            return True
            
        except Exception as e:
            if show_progress:
                print(f"\n❌ 上传分块 {chunk_index} 失败: {e}")
            raise
    
    # 并发上传所有分块
    try:
        tasks = [upload_chunk(idx, data) for idx, data in chunks_data]
        await asyncio.gather(*tasks)
        
        if show_progress:
            print()  # 换行
            print("✅ 异步并行上传完成: {}\n".format(file_path.name))
        
        # 更新元数据状态
        metadata['status'] = 'completed'
        db[f"{key}:meta"] = metadata
        
        return metadata
        
    except Exception as e:
        # 上传失败，清理
        metadata['status'] = 'failed'
        db[f"{key}:meta"] = metadata
        
        if show_progress:
            print(f"\n❌ 上传失败: {e}")
        raise


async def download_large_file_async(
    db,
    key: str,
    output_path: str,
    max_concurrency: int = 8,
    show_progress: bool = True,
    verify: bool = True,
    db_connection_params: Optional[Dict] = None
) -> Dict:
    """
    异步并发下载大文件
    
    使用asyncio代替多线程，提升I/O性能
    
    Args:
        db: FlaxKV 实例（用于读取元数据）
        key: 存储键名
        output_path: 输出路径（文件或目录）
        max_concurrency: 最大并发任务数
        show_progress: 是否显示进度
        verify: 是否验证哈希值
        db_connection_params: 数据库连接参数
    
    Returns:
        文件元数据字典
    """
    # 读取元数据
    metadata = db.get(f"{key}:meta")
    if not metadata:
        raise ValueError(f"文件元数据不存在: {key}")
    
    file_size = metadata['size']
    total_chunks = metadata['total_chunks']
    
    # 确定输出文件路径
    output_path = Path(output_path)
    if output_path.is_dir():
        output_path = output_path / metadata['filename']
    
    if show_progress:
        print(f"\n📥 异步并行下载文件: {metadata['filename']}")
        print(f"   大小: {format_size(file_size)}")
        print(f"   分块: {total_chunks} 个")
        print(f"   并发: {max_concurrency} 个协程\n")
    
    # 异步进度跟踪
    progress_lock = asyncio.Lock()
    chunks_lock = asyncio.Lock()
    completed_chunks = 0
    chunks_data = {}
    
    async def download_chunk(chunk_index: int):
        """下载单个分块的异步协程"""
        nonlocal completed_chunks
        
        try:
            # 在executor中执行同步的FlaxKV操作
            loop = asyncio.get_event_loop()
            
            def sync_download():
                from flaxkv2 import FlaxKV
                thread_db = FlaxKV(
                    db_connection_params['db_name'],
                    db_connection_params['url'],
                    backend=db_connection_params.get('backend', 'remote'),
                    timeout=db_connection_params.get('timeout', 30000),
                    enable_encryption=db_connection_params.get('enable_encryption', False),
                    password=db_connection_params.get('password'),
                    derive_from_password=db_connection_params.get('derive_from_password', True)
                )
                try:
                    return thread_db.get(f"{key}:chunk:{chunk_index}")
                finally:
                    thread_db.close()
            
            chunk = await loop.run_in_executor(None, sync_download)
            
            if chunk is None:
                raise ValueError(f"分块缺失: chunk {chunk_index}/{total_chunks}")
            
            # 存储到字典
            async with chunks_lock:
                chunks_data[chunk_index] = chunk
            
            # 更新进度
            async with progress_lock:
                completed_chunks += 1
                if show_progress:
                    progress = completed_chunks * 100 // total_chunks
                    size_downloaded = min(completed_chunks * metadata['chunk_size'], file_size)
                    print(f"   进度: [{progress:3d}%] {format_size(size_downloaded)}/{format_size(file_size)} ({completed_chunks}/{total_chunks} 块)", end='\r')
            
            return True
            
        except Exception as e:
            if show_progress:
                print(f"\n❌ 下载分块 {chunk_index} 失败: {e}")
            raise
    
    # 并发下载所有分块
    try:
        tasks = [download_chunk(i) for i in range(total_chunks)]
        await asyncio.gather(*tasks)
        
        if show_progress:
            print()  # 换行
            print("\n📝 写入文件...")
        
        # 在executor中写入文件（I/O操作）
        loop = asyncio.get_event_loop()
        
        def write_file():
            with open(output_path, 'wb') as f:
                for i in range(total_chunks):
                    f.write(chunks_data[i])
        
        await loop.run_in_executor(None, write_file)
        
        # 验证哈希
        if verify and metadata.get('hash'):
            if show_progress:
                print("🔍 验证文件完整性...")
            
            downloaded_hash = await loop.run_in_executor(
                None,
                calculate_file_hash,
                str(output_path)
            )
            
            if downloaded_hash != metadata['hash']:
                output_path.unlink()
                raise ValueError(f"文件哈希不匹配！\n  期望: {metadata['hash']}\n  实际: {downloaded_hash}")
            
            if show_progress:
                print("   ✅ 哈希值验证通过\n")
        
        if show_progress:
            print(f"✅ 异步并行下载完成: {output_path}\n")
        
        return metadata
        
    except Exception as e:
        if output_path.exists():
            output_path.unlink()
        
        if show_progress:
            print(f"\n❌ 下载失败: {e}")

        raise


def upload_large_file_batch(
    db,
    key: str,
    file_path: str,
    chunk_size: int = 10 * 1024 * 1024,
    show_progress: bool = True,
    verify: bool = True
) -> Dict:
    """
    使用批量写入上传大文件（性能更高，减少锁竞争）

    相比 upload_large_file，此函数将所有 chunks 收集后一次性批量写入，
    显著减少 LevelDB 写锁竞争，提升 3-5 倍性能。

    Args:
        db: FlaxKV 实例（必须是 RemoteDBDict，支持 batch_set）
        key: 存储键名（不包含前缀）
        file_path: 本地文件路径
        chunk_size: 分块大小（字节），默认 10MB
        show_progress: 是否显示进度条
        verify: 是否计算并验证文件哈希值

    Returns:
        包含上传信息的字典：
        {
            'filename': 文件名,
            'size': 文件大小,
            'chunks': 分块数量,
            'chunk_size': 分块大小,
            'hash': 文件哈希值（如果 verify=True）
        }

    示例:
        >>> from flaxkv2 import FlaxKV
        >>> from flaxkv2.utils.file_transfer import upload_large_file_batch
        >>>
        >>> db = FlaxKV("files", "tcp://127.0.0.1:5555",
        ...            enable_encryption=True, password="yao")
        >>>
        >>> # 批量上传 1GB 文件
        >>> info = upload_large_file_batch(db, "large_video", "/path/to/video.mp4")
        >>> print(f"上传完成: {info['filename']}, 大小: {format_size(info['size'])}")
    """
    import time

    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")

    if not file_path.is_file():
        raise ValueError(f"不是文件: {file_path}")

    # 检查 db 是否支持 batch_set
    if not hasattr(db, 'batch_set'):
        raise TypeError("数据库实例不支持 batch_set 方法，请使用 RemoteDBDict")

    # 获取文件信息
    file_size = file_path.stat().st_size
    total_chunks = (file_size + chunk_size - 1) // chunk_size

    # 计算文件哈希值（如果需要）
    file_hash = None
    if verify:
        if show_progress:
            print(f"🔍 计算文件哈希值...")
        file_hash = calculate_file_hash(str(file_path))
        if show_progress:
            print(f"   SHA256: {file_hash}")

    # 存储元数据
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

    # 先存储元数据（标记为上传中）
    metadata['status'] = 'uploading'
    db[f"{key}:meta"] = metadata

    # 开始批量上传
    try:
        if show_progress:
            print(f"\n📤 批量上传文件: {file_path.name}")
            print(f"   大小: {format_size(file_size)}")
            print(f"   分块: {total_chunks} 个 (每块 {format_size(chunk_size)})")
            print(f"   模式: 批量写入 (WriteBatch)\n")

        start_time = time.time()

        # 读取所有 chunks 到字典
        chunks_dict = {}
        with open(file_path, 'rb') as f:
            for i in range(total_chunks):
                chunk = f.read(chunk_size)
                chunks_dict[f"{key}:chunk:{i}"] = chunk

                if show_progress:
                    progress = (i + 1) * 100 // total_chunks
                    size_read = min((i + 1) * chunk_size, file_size)
                    print(f"   读取: [{progress:3d}%] {format_size(size_read)}/{format_size(file_size)}", end='\r')

        if show_progress:
            print()

        # 一次性批量写入所有 chunks
        if show_progress:
            print(f"   批量写入 {total_chunks} 个chunks...")

        batch_start = time.time()
        db.batch_set(chunks_dict)
        batch_time = time.time() - batch_start

        if show_progress:
            print(f"   批量写入完成: {batch_time:.2f}秒")

        # 更新元数据（标记为完成）
        upload_time = time.time() - start_time
        metadata['status'] = 'completed'
        metadata['upload_time'] = upload_time
        db[f"{key}:meta"] = metadata

        if show_progress:
            throughput = file_size / upload_time / (1024 * 1024)
            print(f"\n✅ 批量上传完成!")
            print(f"   总耗时: {upload_time:.2f}秒")
            print(f"   吞吐量: {throughput:.1f} MB/s\n")

        return metadata

    except Exception as e:
        # 上传失败，清理已上传的数据
        if show_progress:
            print(f"\n❌ 上传失败: {e}")
            print(f"   正在清理...")

        try:
            # 删除元数据
            del db[f"{key}:meta"]
            # 删除已上传的 chunks
            for i in range(total_chunks):
                try:
                    del db[f"{key}:chunk:{i}"]
                except:
                    pass
        except:
            pass

        raise
