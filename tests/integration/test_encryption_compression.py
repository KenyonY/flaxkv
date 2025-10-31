"""
测试ZeroMQ加密和压缩功能
"""

import tempfile
import shutil
import time
import threading
from flaxkv2.server.zmq_server import FlaxKVServer
from flaxkv2.client.zmq_client import RemoteDBDict
from flaxkv2.utils.keygen import generate_curve_keypair


def test_basic_connection():
    """测试1: 基本连接（无加密、无压缩）"""
    print("\n=== 测试1: 基本连接（无加密、无压缩） ===")
    tmpdir = tempfile.mkdtemp()
    try:
        # 启动服务器
        server = FlaxKVServer(
            host="127.0.0.1",
            port=15555,
            data_dir=tmpdir,
            enable_encryption=False,
            enable_compression=False,
        )

        server_thread = threading.Thread(target=server.run)
        server_thread.daemon = True
        server_thread.start()
        time.sleep(0.5)

        # 连接客户端
        client = RemoteDBDict(
            'test_basic',
            host="127.0.0.1",
            port=15555,
            enable_encryption=False,
            enable_compression=False,
        )

        # 测试基本操作
        client['key1'] = 'value1'
        assert client['key1'] == 'value1'
        print("✓ 基本读写操作成功")

        client.close()
        server.stop()
        print("✓ 测试1通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_encryption_only():
    """测试2: 仅加密（无压缩）"""
    print("\n=== 测试2: 仅加密（无压缩） ===")
    tmpdir = tempfile.mkdtemp()
    try:
        # 生成密钥对
        keypair = generate_curve_keypair()
        server_public_key = keypair['public_key']
        server_secret_key = keypair['secret_key']

        print(f"服务器公钥: {server_public_key}")
        print(f"服务器私钥: {server_secret_key}")

        # 启动服务器（启用加密）
        server = FlaxKVServer(
            host="127.0.0.1",
            port=15556,
            data_dir=tmpdir,
            enable_encryption=True,
            server_secret_key=server_secret_key,
            enable_compression=False,
        )

        server_thread = threading.Thread(target=server.run)
        server_thread.daemon = True
        server_thread.start()
        time.sleep(0.5)

        # 连接客户端（启用加密）
        client = RemoteDBDict(
            'test_encryption',
            host="127.0.0.1",
            port=15556,
            enable_encryption=True,
            server_public_key=server_public_key,
            enable_compression=False,
        )

        # 测试加密通信
        client['secure_key'] = 'secure_value'
        assert client['secure_key'] == 'secure_value'
        print("✓ 加密读写操作成功")

        # 测试多次读写
        for i in range(10):
            client[f'key_{i}'] = f'value_{i}'

        for i in range(10):
            assert client[f'key_{i}'] == f'value_{i}'
        print("✓ 多次加密读写成功")

        client.close()
        server.stop()
        print("✓ 测试2通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_compression_only():
    """测试3: 仅压缩（无加密）"""
    print("\n=== 测试3: 仅压缩（无加密） ===")
    tmpdir = tempfile.mkdtemp()
    try:
        # 启动服务器（启用压缩）
        server = FlaxKVServer(
            host="127.0.0.1",
            port=15557,
            data_dir=tmpdir,
            enable_encryption=False,
            enable_compression=True,
        )

        server_thread = threading.Thread(target=server.run)
        server_thread.daemon = True
        server_thread.start()
        time.sleep(0.5)

        # 连接客户端（启用压缩）
        client = RemoteDBDict(
            'test_compression',
            host="127.0.0.1",
            port=15557,
            enable_encryption=False,
            enable_compression=True,
        )

        # 测试压缩（写入大数据）
        large_data = 'x' * 10000  # 10KB 数据
        client['large_key'] = large_data
        assert client['large_key'] == large_data
        print("✓ 压缩大数据读写成功")

        # 检查统计信息
        stats = server.stats
        print(f"✓ 压缩统计: 发送字节={stats['bytes_sent']}, 压缩字节={stats['bytes_compressed']}")

        client.close()
        server.stop()
        print("✓ 测试3通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_encryption_and_compression():
    """测试4: 加密+压缩"""
    print("\n=== 测试4: 加密+压缩 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        # 生成密钥对
        keypair = generate_curve_keypair()
        server_public_key = keypair['public_key']
        server_secret_key = keypair['secret_key']

        print(f"服务器公钥: {server_public_key}")

        # 启动服务器（启用加密和压缩）
        server = FlaxKVServer(
            host="127.0.0.1",
            port=15558,
            data_dir=tmpdir,
            enable_encryption=True,
            server_secret_key=server_secret_key,
            enable_compression=True,
        )

        server_thread = threading.Thread(target=server.run)
        server_thread.daemon = True
        server_thread.start()
        time.sleep(0.5)

        # 连接客户端（启用加密和压缩）
        client = RemoteDBDict(
            'test_both',
            host="127.0.0.1",
            port=15558,
            enable_encryption=True,
            server_public_key=server_public_key,
            enable_compression=True,
        )

        # 测试加密+压缩通信
        large_data = 'y' * 10000  # 10KB 数据
        client['secure_large_key'] = large_data
        assert client['secure_large_key'] == large_data
        print("✓ 加密+压缩大数据读写成功")

        # 测试多种数据类型
        test_data = {
            'int': 12345,
            'float': 3.14159,
            'list': [1, 2, 3, 4, 5],
            'dict': {'nested': 'data', 'count': 42},
            'bytes': b'binary data',
        }

        for key, value in test_data.items():
            client[key] = value
            assert client[key] == value
        print("✓ 多种数据类型加密+压缩成功")

        # 检查统计信息
        stats = server.stats
        print(f"✓ 统计: 请求数={stats['requests']}, 发送字节={stats['bytes_sent']}, 压缩字节={stats['bytes_compressed']}")

        client.close()
        server.stop()
        print("✓ 测试4通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_with_cache():
    """测试5: 加密+压缩+缓存"""
    print("\n=== 测试5: 加密+压缩+缓存 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        # 生成密钥对
        keypair = generate_curve_keypair()
        server_public_key = keypair['public_key']
        server_secret_key = keypair['secret_key']

        # 启动服务器
        server = FlaxKVServer(
            host="127.0.0.1",
            port=15559,
            data_dir=tmpdir,
            enable_encryption=True,
            server_secret_key=server_secret_key,
            enable_compression=True,
        )

        server_thread = threading.Thread(target=server.run)
        server_thread.daemon = True
        server_thread.start()
        time.sleep(0.5)

        # 连接客户端（启用缓存）
        client = RemoteDBDict(
            'test_cache',
            host="127.0.0.1",
            port=15559,
            enable_encryption=True,
            server_public_key=server_public_key,
            enable_compression=True,
            read_cache_size=100,  # 启用缓存
        )

        # 写入数据
        client['cached_key'] = 'cached_value'
        print("✓ 写入数据成功")

        # 第一次读取（从服务器）
        v1 = client['cached_key']
        assert v1 == 'cached_value'
        requests_1 = server.stats['requests']
        print(f"✓ 第一次读取成功，服务器请求数={requests_1}")

        # 第二次读取（从缓存）
        v2 = client['cached_key']
        assert v2 == 'cached_value'
        requests_2 = server.stats['requests']
        print(f"✓ 第二次读取成功（从缓存），服务器请求数={requests_2}")

        # 验证缓存生效（请求数不变）
        assert requests_2 == requests_1, "缓存未生效，请求数增加了"
        print("✓ 缓存验证成功，减少了网络请求")

        client.close()
        server.stop()
        print("✓ 测试5通过\n")
    finally:
        shutil.rmtree(tmpdir)


if __name__ == '__main__':
    print("=" * 70)
    print("ZeroMQ 加密和压缩功能测试")
    print("=" * 70)

    test_basic_connection()
    test_encryption_only()
    test_compression_only()
    test_encryption_and_compression()
    test_with_cache()

    print("=" * 70)
    print("✅ 所有测试通过！")
    print("=" * 70)
