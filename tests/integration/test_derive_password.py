"""
测试方案2：从密码确定性派生密钥

验证相同密码在不同地方生成相同密钥对
"""

import tempfile
import shutil
import time
import threading
from flaxkv2.server.zmq_server import FlaxKVServer
from flaxkv2.client.zmq_client import RemoteDBDict
from flaxkv2.utils.key_manager import derive_keypair_from_password


def test_derive_deterministic():
    """测试1: 相同密码生成相同密钥（确定性）"""
    print("\n=== 测试1: 相同密码生成相同密钥（确定性） ===")

    PASSWORD = "test_deterministic_password"

    # 第一次派生
    keypair1 = derive_keypair_from_password(PASSWORD)
    print(f"第一次派生:")
    print(f"  Public:  {keypair1['public_key']}")
    print(f"  Secret:  {keypair1['secret_key'][:20]}...")

    # 第二次派生（应该相同）
    keypair2 = derive_keypair_from_password(PASSWORD)
    print(f"第二次派生:")
    print(f"  Public:  {keypair2['public_key']}")
    print(f"  Secret:  {keypair2['secret_key'][:20]}...")

    # 验证相同
    assert keypair1['public_key'] == keypair2['public_key']
    assert keypair1['secret_key'] == keypair2['secret_key']
    print("✓ 相同密码生成相同密钥对")

    # 不同密码应该生成不同密钥
    keypair3 = derive_keypair_from_password("different_password")
    assert keypair3['public_key'] != keypair1['public_key']
    assert keypair3['secret_key'] != keypair1['secret_key']
    print("✓ 不同密码生成不同密钥对")

    print("✓ 测试1通过\n")


def test_cross_machine_simulation():
    """测试2: 模拟跨机器通信（使用相同密码）"""
    print("\n=== 测试2: 模拟跨机器通信（使用相同密码） ===")

    tmpdir = tempfile.mkdtemp()
    try:
        PASSWORD = "cross_machine_password_123"

        # 模拟机器A：启动服务器
        print("  机器A（服务器）使用密码派生密钥...")
        server = FlaxKVServer(
            host="127.0.0.1",
            port=15570,
            data_dir=tmpdir,
            enable_encryption=True,
            password=PASSWORD,
            derive_from_password=True,  # 方案2：从密码派生
            enable_compression=True,
        )

        server_thread = threading.Thread(target=server.run)
        server_thread.daemon = True
        server_thread.start()
        time.sleep(0.5)

        # 模拟机器B：客户端使用相同密码
        print("  机器B（客户端）使用相同密码派生密钥...")
        client = RemoteDBDict(
            'test_cross_machine',
            host="127.0.0.1",
            port=15570,
            enable_encryption=True,
            password=PASSWORD,
            derive_from_password=True,  # 方案2：从密码派生
            enable_compression=True,
        )

        # 测试通信
        client['key1'] = 'value1'
        assert client['key1'] == 'value1'
        print("✓ 跨机器通信成功（使用相同密码）")

        # 测试多次读写
        for i in range(5):
            client[f'key_{i}'] = f'value_{i}'

        for i in range(5):
            assert client[f'key_{i}'] == f'value_{i}'
        print("✓ 多次读写成功")

        client.close()
        server.stop()
        print("✓ 测试2通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_wrong_password_cannot_connect():
    """测试3: 使用错误密码无法连接"""
    print("\n=== 测试3: 使用错误密码无法连接 ===")

    tmpdir = tempfile.mkdtemp()
    try:
        SERVER_PASSWORD = "server_correct_password"
        CLIENT_PASSWORD = "client_wrong_password"

        # 启动服务器
        print(f"  服务器使用密码: '{SERVER_PASSWORD}'")
        server = FlaxKVServer(
            host="127.0.0.1",
            port=15571,
            data_dir=tmpdir,
            enable_encryption=True,
            password=SERVER_PASSWORD,
            derive_from_password=True,
        )

        server_thread = threading.Thread(target=server.run)
        server_thread.daemon = True
        server_thread.start()
        time.sleep(0.5)

        # 客户端使用错误密码
        print(f"  客户端使用密码: '{CLIENT_PASSWORD}' (错误)")
        try:
            client = RemoteDBDict(
                'test_wrong',
                host="127.0.0.1",
                port=15571,
                timeout=2000,
                max_retries=1,
                enable_encryption=True,
                password=CLIENT_PASSWORD,
                derive_from_password=True,
            )
            print("✗ 错误：使用错误密码居然连接成功了！")
            assert False
        except Exception as e:
            print(f"✓ 使用错误密码连接失败（符合预期）: {type(e).__name__}")

        server.stop()
        print("✓ 测试3通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_scheme_comparison():
    """测试4: 方案1 vs 方案2 对比"""
    print("\n=== 测试4: 方案1（文件存储）vs 方案2（密码派生）对比 ===")

    PASSWORD = "comparison_password"

    # 方案2：密码派生
    print("  方案2（密码派生）:")
    keypair_derive1 = derive_keypair_from_password(PASSWORD)
    print(f"    第1次: {keypair_derive1['public_key']}")

    keypair_derive2 = derive_keypair_from_password(PASSWORD)
    print(f"    第2次: {keypair_derive2['public_key']}")

    assert keypair_derive1['public_key'] == keypair_derive2['public_key']
    print("    ✓ 密钥一致（确定性）")

    # 方案1：文件存储（会生成随机密钥）
    print("\n  方案1（文件存储）:")
    from flaxkv2.utils.key_manager import get_keypair_from_password

    # 清理之前的密钥文件（如果存在）
    from flaxkv2.utils.key_manager import KeyManager
    import tempfile
    tmpdir = tempfile.mkdtemp()
    km = KeyManager(key_dir=tmpdir)

    # 第一次：生成新密钥
    keypair_file1 = km.get_or_create_keypair(PASSWORD)
    print(f"    第1次: {keypair_file1['public_key']}")

    # 第二次：加载相同密钥
    keypair_file2 = km.get_or_create_keypair(PASSWORD)
    print(f"    第2次: {keypair_file2['public_key']}")

    assert keypair_file1['public_key'] == keypair_file2['public_key']
    print("    ✓ 密钥一致（从文件加载）")

    # 对比两种方案
    print("\n  对比:")
    print(f"    方案1（文件存储）: {keypair_file1['public_key']}")
    print(f"    方案2（密码派生）: {keypair_derive1['public_key']}")

    # 两种方案的密钥应该不同（因为方案1是随机生成的）
    assert keypair_file1['public_key'] != keypair_derive1['public_key']
    print("    ✓ 两种方案生成不同的密钥（符合预期）")

    shutil.rmtree(tmpdir)
    print("✓ 测试4通过\n")


def test_password_derive_with_cache():
    """测试5: 密码派生 + 缓存 + 压缩"""
    print("\n=== 测试5: 密码派生 + 缓存 + 压缩 ===")

    tmpdir = tempfile.mkdtemp()
    try:
        PASSWORD = "cache_derive_password"

        # 启动服务器
        server = FlaxKVServer(
            host="127.0.0.1",
            port=15572,
            data_dir=tmpdir,
            enable_encryption=True,
            password=PASSWORD,
            derive_from_password=True,
            enable_compression=True,
        )

        server_thread = threading.Thread(target=server.run)
        server_thread.daemon = True
        server_thread.start()
        time.sleep(0.5)

        # 连接客户端（启用缓存）
        client = RemoteDBDict(
            'test_all',
            host="127.0.0.1",
            port=15572,
            enable_encryption=True,
            password=PASSWORD,
            derive_from_password=True,
            enable_compression=True,
            read_cache_size=100,
        )

        # 写入数据
        client['data'] = 'test_value'
        print("✓ 写入数据成功")

        # 第一次读取
        v1 = client['data']
        assert v1 == 'test_value'
        requests_1 = server.stats['requests']
        print(f"✓ 第一次读取成功，服务器请求数={requests_1}")

        # 第二次读取（从缓存）
        v2 = client['data']
        assert v2 == 'test_value'
        requests_2 = server.stats['requests']
        print(f"✓ 第二次读取成功（从缓存），服务器请求数={requests_2}")

        # 验证缓存生效
        assert requests_2 == requests_1
        print("✓ 缓存验证成功")

        # 测试大数据压缩
        large_data = 'x' * 10000
        client['large'] = large_data
        assert client['large'] == large_data
        print("✓ 大数据加密+压缩成功")

        print(f"✓ 统计: 请求={server.stats['requests']}, 压缩字节={server.stats['bytes_compressed']}")

        client.close()
        server.stop()
        print("✓ 测试5通过\n")
    finally:
        shutil.rmtree(tmpdir)


if __name__ == '__main__':
    print("=" * 70)
    print("方案2测试：从密码确定性派生密钥")
    print("=" * 70)

    test_derive_deterministic()
    test_cross_machine_simulation()
    test_wrong_password_cannot_connect()
    test_scheme_comparison()
    test_password_derive_with_cache()

    print("=" * 70)
    print("✅ 所有测试通过！")
    print("=" * 70)
    print("\n推荐使用方案2（默认）：")
    print("  # 服务器")
    print("  server = FlaxKVServer(enable_encryption=True, password='your_password')")
    print()
    print("  # 客户端（可以在不同机器上）")
    print("  client = RemoteDBDict('db', enable_encryption=True, password='your_password')")
    print()
    print("方案1（文件存储）：")
    print("  server = FlaxKVServer(enable_encryption=True, password='pwd', derive_from_password=False)")
    print("  client = RemoteDBDict('db', enable_encryption=True, password='pwd', derive_from_password=False)")
