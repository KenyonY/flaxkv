#!/usr/bin/env python3
"""
测试并行分块传输功能

使用小文件测试并行上传和下载功能，验证：
1. 多线程并行上传
2. 多线程并行下载
3. SHA256 哈希验证
4. 数据完整性
"""

import os
import tempfile
from pathlib import Path
from flaxkv2 import FlaxKV
from flaxkv2.utils.file_transfer import (
    upload_large_file_parallel,
    download_large_file_parallel,
    delete_large_file,
    format_size
)


def create_test_file(size_mb: int = 20) -> str:
    """创建测试文件"""
    # 创建临时文件
    temp_dir = Path("./test_data")
    temp_dir.mkdir(exist_ok=True)

    test_file = temp_dir / f"test_parallel_{size_mb}mb.bin"

    print(f"创建测试文件: {test_file} ({size_mb}MB)...")

    with open(test_file, 'wb') as f:
        chunk_size = 1024 * 1024  # 1MB
        for _ in range(size_mb):
            f.write(os.urandom(chunk_size))

    print(f"✅ 测试文件创建完成: {format_size(size_mb * 1024 * 1024)}\n")

    return str(test_file)


def test_parallel_upload_download():
    """测试并行上传和下载"""

    print("=" * 80)
    print("测试并行分块传输功能")
    print("=" * 80)

    # 创建测试文件（20MB）
    test_file_path = create_test_file(size_mb=20)

    # 连接到服务器
    print("连接到 FlaxKV 服务器...")
    db = FlaxKV(
        "test_parallel",
        "tcp://127.0.0.1:25555",
        enable_encryption=True,
        password="yao"
    )

    try:
        # 测试并行上传
        print("\n" + "=" * 80)
        print("1. 测试并行上传")
        print("=" * 80)

        upload_info = upload_large_file_parallel(
            db,
            key="parallel_test_file",
            file_path=test_file_path,
            chunk_size=5 * 1024 * 1024,  # 5MB 分块
            max_workers=4,  # 4 个线程
            show_progress=True,
            verify=True
        )

        print(f"\n上传信息:")
        print(f"  文件名: {upload_info['filename']}")
        print(f"  大小: {format_size(upload_info['size'])}")
        print(f"  分块数: {upload_info['chunks']}")
        print(f"  哈希: {upload_info['hash'][:16]}...")

        # 测试并行下载
        print("\n" + "=" * 80)
        print("2. 测试并行下载")
        print("=" * 80)

        download_dir = Path("./test_data/downloads")
        download_dir.mkdir(parents=True, exist_ok=True)

        download_info = download_large_file_parallel(
            db,
            key="parallel_test_file",
            output_path=str(download_dir),
            max_workers=4,  # 4 个线程
            show_progress=True,
            verify=True
        )

        print(f"\n下载信息:")
        print(f"  文件名: {download_info['filename']}")
        print(f"  大小: {format_size(download_info['size'])}")

        # 验证文件完整性
        print("\n" + "=" * 80)
        print("3. 验证文件完整性")
        print("=" * 80)

        from flaxkv2.utils.file_transfer import calculate_file_hash

        original_hash = calculate_file_hash(test_file_path)
        downloaded_file = download_dir / download_info['filename']
        downloaded_hash = calculate_file_hash(str(downloaded_file))

        print(f"  原始文件哈希: {original_hash}")
        print(f"  下载文件哈希: {downloaded_hash}")

        if original_hash == downloaded_hash:
            print(f"\n✅ 文件完整性验证通过！")
        else:
            print(f"\n❌ 文件完整性验证失败！")
            return False

        # 清理测试数据
        print("\n" + "=" * 80)
        print("4. 清理测试数据")
        print("=" * 80)

        delete_large_file(db, "parallel_test_file", show_progress=True)

        # 删除本地测试文件
        os.unlink(test_file_path)
        os.unlink(downloaded_file)
        print(f"✅ 本地测试文件已删除")

        print("\n" + "=" * 80)
        print("✅ 所有测试通过！")
        print("=" * 80)

        return True

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        db.close()


if __name__ == "__main__":
    print("""
╔════════════════════════════════════════════════════════════════════════════╗
║              FlaxKV2 并行分块传输功能测试                                     ║
╚════════════════════════════════════════════════════════════════════════════╝

前提条件：
1. 启动 FlaxKV2 服务器:
   flaxkv2 run --port 25555 --enable-encryption --password yao

测试内容：
✓ 创建 20MB 测试文件
✓ 并行上传（4 个线程，5MB 分块）
✓ 并行下载（4 个线程）
✓ SHA256 哈希验证
✓ 数据完整性验证
✓ 清理测试数据
""")

    input("按 Enter 开始测试...")

    success = test_parallel_upload_download()

    if success:
        print("\n🎉 测试成功完成！并行传输功能正常工作。")
    else:
        print("\n⚠️ 测试失败，请检查错误信息。")
