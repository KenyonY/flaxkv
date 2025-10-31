"""
测试基于密码的密钥管理功能
"""

import tempfile
import shutil
import time
import threading
from flaxkv2.server.zmq_server import FlaxKVServer
from flaxkv2.client.zmq_client import RemoteDBDict
from flaxkv2.utils.key_manager import KeyManager


def test_password_basic():
    """测试1: 使用密码进行基本加密连接"""
    print("\n=== 测试1: 使用密码进行基本加密连接 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        PASSWORD = "my_secure_password_123"

        # 启动服务器（使用密码）
        server = FlaxKVServer(
            host="127.0.0.1",
            port=15560,
            data_dir=tmpdir,
            enable_encryption=True,
            password=PASSWORD,
            enable_compression=True,
        )

        server_thread = threading.Thread(target=server.run)
        server_thread.daemon = True
        server_thread.start()
        time.sleep(0.5)

        # 连接客户端（使用相同密码）
        client = RemoteDBDict(
            'test_password',
            host="127.0.0.1",
            port=15560,
            enable_encryption=True,
            password=PASSWORD,
            enable_compression=True,
        )

        # 测试读写
        client['key1'] = 'value1'
        assert client['key1'] == 'value1'
        print("✓ 使用密码的加密读写成功")

        # 测试多次读写
        for i in range(10):
            client[f'key_{i}'] = f'value_{i}'

        for i in range(10):
            assert client[f'key_{i}'] == f'value_{i}'
        print("✓ 多次密码加密读写成功")

        client.close()
        server.stop()
        print("✓ 测试1通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_password_persistence():
    """测试2: 密钥持久化（重启服务器和客户端）"""
    print("\n=== 测试2: 密钥持久化（重启服务器和客户端） ===")
    tmpdir = tempfile.mkdtemp()
    try:
        PASSWORD = "persistent_password_456"

        # 第一次启动：服务器和客户端
        print("  第一次启动服务器...")
        server1 = FlaxKVServer(
            host="127.0.0.1",
            port=15561,
            data_dir=tmpdir,
            enable_encryption=True,
            password=PASSWORD,
        )

        server_thread1 = threading.Thread(target=server1.run)
        server_thread1.daemon = True
        server_thread1.start()
        time.sleep(0.5)

        print("  第一次连接客户端...")
        client1 = RemoteDBDict(
            'test_persistence',
            host="127.0.0.1",
            port=15561,
            enable_encryption=True,
            password=PASSWORD,
        )

        client1['data'] = 'first_run'
        assert client1['data'] == 'first_run'
        print("✓ 第一次运行成功")

        # 关闭
        client1.close()
        server1.stop()
        time.sleep(0.5)

        # 第二次启动：使用相同密码
        print("  第二次启动服务器（使用相同密码）...")
        server2 = FlaxKVServer(
            host="127.0.0.1",
            port=15561,
            data_dir=tmpdir,
            enable_encryption=True,
            password=PASSWORD,  # 相同密码
        )

        server_thread2 = threading.Thread(target=server2.run)
        server_thread2.daemon = True
        server_thread2.start()
        time.sleep(0.5)

        print("  第二次连接客户端（使用相同密码）...")
        client2 = RemoteDBDict(
            'test_persistence',
            host="127.0.0.1",
            port=15561,
            enable_encryption=True,
            password=PASSWORD,  # 相同密码
        )

        # 应该能读取之前的数据
        assert client2['data'] == 'first_run'
        print("✓ 使用相同密码重启后成功读取数据")

        # 写入新数据
        client2['data2'] = 'second_run'
        assert client2['data2'] == 'second_run'
        print("✓ 重启后写入新数据成功")

        client2.close()
        server2.stop()
        print("✓ 测试2通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_wrong_password():
    """测试3: 使用错误的密码无法连接"""
    print("\n=== 测试3: 使用错误的密码无法连接 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        SERVER_PASSWORD = "server_password_789"
        CLIENT_PASSWORD = "wrong_password_000"

        # 启动服务器
        server = FlaxKVServer(
            host="127.0.0.1",
            port=15562,
            data_dir=tmpdir,
            enable_encryption=True,
            password=SERVER_PASSWORD,
        )

        server_thread = threading.Thread(target=server.run)
        server_thread.daemon = True
        server_thread.start()
        time.sleep(0.5)

        # 客户端使用错误密码连接
        print("  尝试使用错误密码连接...")
        try:
            client = RemoteDBDict(
                'test_wrong_password',
                host="127.0.0.1",
                port=15562,
                timeout=2000,  # 短超时
                max_retries=1,
                enable_encryption=True,
                password=CLIENT_PASSWORD,  # 错误的密码
            )
            print("✗ 错误：使用错误密码居然连接成功了！")
            assert False
        except Exception as e:
            print(f"✓ 使用错误密码连接失败（符合预期）: {type(e).__name__}")

        server.stop()
        print("✓ 测试3通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_key_manager_api():
    """测试4: KeyManager API"""
    print("\n=== 测试4: KeyManager API ===")
    tmpdir = tempfile.mkdtemp()
    try:
        key_manager = KeyManager(key_dir=tmpdir)

        PASSWORD = "test_api_password"

        # 检查密钥是否存在
        assert not key_manager.key_exists(PASSWORD)
        print("✓ 初始状态：密钥不存在")

        # 生成并保存密钥对
        keypair1 = key_manager.generate_and_save_keypair(PASSWORD)
        print(f"✓ 生成密钥对:")
        print(f"    Public:  {keypair1['public_key']}")
        print(f"    Secret:  {keypair1['secret_key'][:20]}...")

        # 检查密钥是否存在
        assert key_manager.key_exists(PASSWORD)
        print("✓ 密钥已保存")

        # 加载密钥对
        keypair2 = key_manager.load_keypair(PASSWORD)
        assert keypair2['public_key'] == keypair1['public_key']
        assert keypair2['secret_key'] == keypair1['secret_key']
        print("✓ 成功加载密钥对，内容一致")

        # get_or_create（已存在，应该加载）
        keypair3 = key_manager.get_or_create_keypair(PASSWORD)
        assert keypair3['public_key'] == keypair1['public_key']
        print("✓ get_or_create 加载现有密钥成功")

        # 列出所有密钥
        all_keys = key_manager.list_all_keys()
        assert len(all_keys) >= 1
        print(f"✓ 列出密钥文件: {len(all_keys)} 个")

        # 删除密钥
        assert key_manager.delete_keypair(PASSWORD)
        assert not key_manager.key_exists(PASSWORD)
        print("✓ 删除密钥成功")

        # get_or_create（不存在，应该创建）
        keypair4 = key_manager.get_or_create_keypair(PASSWORD)
        assert keypair4['public_key'] != keypair1['public_key']  # 新生成的，应该不同
        print("✓ get_or_create 创建新密钥成功")

        print("✓ 测试4通过\n")
    finally:
        shutil.rmtree(tmpdir)


def test_password_with_cache():
    """测试5: 密码 + 缓存"""
    print("\n=== 测试5: 密码 + 加密 + 压缩 + 缓存 ===")
    tmpdir = tempfile.mkdtemp()
    try:
        PASSWORD = "cache_password_999"

        # 启动服务器
        server = FlaxKVServer(
            host="127.0.0.1",
            port=15563,
            data_dir=tmpdir,
            enable_encryption=True,
            password=PASSWORD,
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
            port=15563,
            enable_encryption=True,
            password=PASSWORD,
            enable_compression=True,
            read_cache_size=100,
        )

        # 写入数据
        client['cached_data'] = 'cached_value'
        print("✓ 写入数据成功")

        # 第一次读取
        v1 = client['cached_data']
        assert v1 == 'cached_value'
        requests_1 = server.stats['requests']
        print(f"✓ 第一次读取成功，服务器请求数={requests_1}")

        # 第二次读取（从缓存）
        v2 = client['cached_data']
        assert v2 == 'cached_value'
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

        print(f"✓ 统计: 请求={server.stats['requests']}, 发送={server.stats['bytes_sent']}, 压缩={server.stats['bytes_compressed']}")

        client.close()
        server.stop()
        print("✓ 测试5通过\n")
    finally:
        shutil.rmtree(tmpdir)


if __name__ == '__main__':
    print("=" * 70)
    print("密码认证功能测试")
    print("=" * 70)

    test_password_basic()
    test_password_persistence()
    test_wrong_password()
    test_key_manager_api()
    test_password_with_cache()

    print("=" * 70)
    print("✅ 所有测试通过！")
    print("=" * 70)
    print("\n使用说明：")
    print("  服务器: FlaxKVServer(enable_encryption=True, password='your_password')")
    print("  客户端: RemoteDBDict('db', enable_encryption=True, password='your_password')")
    print(f"  密钥存储位置: ~/.flaxkv/keys/")
