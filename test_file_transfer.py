#!/usr/bin/env python3
"""
测试文件传输功能

使用示例:
1. 在一个终端启动服务器: python test_file_transfer.py server
2. 在另一个终端测试上传: python test_file_transfer.py upload
3. 在另一个终端测试下载: python test_file_transfer.py download
"""

import sys
import os
import tempfile
import shutil
from pathlib import Path

from flaxkv2 import FlaxKV
from flaxkv2.server.zmq_server import FlaxKVServer
from flaxkv2.utils.file_transfer import FileTransferUtil
import json


def start_server():
    """启动测试服务器"""
    print("启动 FlaxKV 服务器...")
    print("地址: 127.0.0.1:15555")
    print("按 Ctrl+C 停止")

    server = FlaxKVServer(
        host='127.0.0.1',
        port=15555,
        data_dir='/tmp/flaxkv_test_server',
        max_workers=4
    )
    server.run()


def test_upload():
    """测试上传文件和文件夹"""
    print("\n=== 测试上传功能 ===\n")

    # 创建测试文件
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # 创建测试文件
        test_file = tmpdir / "test_file.txt"
        test_file.write_text("这是一个测试文件\nHello FlaxKV!")

        # 创建测试文件夹
        test_folder = tmpdir / "test_folder"
        test_folder.mkdir()
        (test_folder / "file1.txt").write_text("文件1的内容")
        (test_folder / "file2.txt").write_text("文件2的内容")

        subfolder = test_folder / "subfolder"
        subfolder.mkdir()
        (subfolder / "file3.txt").write_text("子文件夹中的文件")

        # 连接到服务器
        db = FlaxKV('file_storage', '127.0.0.1:15555', backend='remote')

        # 上传文件
        print("1. 上传文件...")
        content, metadata = FileTransferUtil.pack(str(test_file))
        db['test_file'] = content
        db['test_file:meta'] = json.dumps(metadata)
        print(f"   ✓ 上传成功: {metadata['name']} ({metadata['size']} 字节)")

        # 上传文件夹
        print("\n2. 上传文件夹...")
        content, metadata = FileTransferUtil.pack(str(test_folder))
        db['test_folder'] = content
        db['test_folder:meta'] = json.dumps(metadata)
        print(f"   ✓ 上传成功: {metadata['name']} ({metadata['size']} 字节)")

        db.close()
        print("\n上传完成!")


def test_download():
    """测试下载文件和文件夹"""
    print("\n=== 测试下载功能 ===\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # 连接到服务器
        db = FlaxKV('file_storage', '127.0.0.1:15555', backend='remote')

        # 下载文件
        print("1. 下载文件...")
        if 'test_file' in db:
            content = db['test_file']
            metadata = json.loads(db['test_file:meta'])

            output_file = tmpdir / 'downloaded_file.txt'
            FileTransferUtil.unpack(content, metadata, str(output_file))

            print(f"   ✓ 下载成功: {output_file}")
            print(f"   内容: {output_file.read_text()}")
        else:
            print("   ✗ 文件不存在，请先运行上传测试")

        # 下载文件夹
        print("\n2. 下载文件夹...")
        if 'test_folder' in db:
            content = db['test_folder']
            metadata = json.loads(db['test_folder:meta'])

            FileTransferUtil.unpack(content, metadata, str(tmpdir))

            downloaded_folder = tmpdir / metadata['name']
            print(f"   ✓ 下载成功: {downloaded_folder}")

            # 列出文件夹内容
            print("\n   文件夹结构:")
            for root, dirs, files in os.walk(downloaded_folder):
                level = root.replace(str(downloaded_folder), '').count(os.sep)
                indent = ' ' * 4 * level
                print(f'{indent}{os.path.basename(root)}/')
                subindent = ' ' * 4 * (level + 1)
                for file in files:
                    print(f'{subindent}{file}')
        else:
            print("   ✗ 文件夹不存在，请先运行上传测试")

        db.close()
        print("\n下载完成!")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == 'server':
        start_server()
    elif command == 'upload':
        test_upload()
    elif command == 'download':
        test_download()
    else:
        print(f"未知命令: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == '__main__':
    main()
