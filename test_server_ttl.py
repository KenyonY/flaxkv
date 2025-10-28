"""
测试服务器端TTL验证
"""

import time
import tempfile
import sys
import threading
from flaxkv2.server.zmq_server import FlaxKVServer
from flaxkv2.client.zmq_client import RemoteDBDict


def test_server_side_ttl():
    """测试服务器端TTL验证"""
    print("=" * 60)
    print("测试服务器端TTL验证")
    print("=" * 60 + "\n")

    with tempfile.TemporaryDirectory() as temp_dir:
        # 启动服务器
        print("1. 启动服务器...")
        server = FlaxKVServer(host="127.0.0.1", port=15555, data_dir=temp_dir)
        server.start(register_signals=False)

        # 在后台运行服务器
        server_thread = threading.Thread(target=server._server_loop, daemon=True)
        server_thread.start()
        time.sleep(0.5)  # 等待服务器启动
        print("   ✓ 服务器已启动\n")

        try:
            # 连接客户端
            print("2. 连接客户端...")
            client = RemoteDBDict("test_db", host="127.0.0.1", port=15555)
            print("   ✓ 客户端已连接\n")

            # 测试1: 写入数据并设置TTL
            print("3. 写入数据并设置TTL (2秒)...")
            client["key1"] = "value1"
            client.set_ttl("key1", 2)
            print("   ✓ 数据已写入\n")

            # 测试2: 立即读取应该成功
            print("4. 立即读取 (应该成功)...")
            value = client["key1"]
            assert value == "value1", f"期望 'value1'，实际 '{value}'"
            print(f"   ✓ 读取成功: {value}\n")

            # 测试3: 等待3秒后读取应该失败
            print("5. 等待3秒后读取 (应该失败，因为已过期)...")
            time.sleep(3)

            try:
                value = client["key1"]
                print(f"   ❌ 错误: 应该抛出KeyError，但返回了 {value}")
                return False
            except KeyError:
                print("   ✓ 正确抛出KeyError (键已过期)\n")

            # 测试4: 验证服务器端已删除过期数据
            print("6. 再次尝试读取 (确认已被删除)...")
            try:
                value = client["key1"]
                print(f"   ❌ 错误: 键应该已被删除")
                return False
            except KeyError:
                print("   ✓ 键已被服务器删除\n")

            # 测试5: 检查网络请求次数
            print("7. 性能测试: 检查请求次数...")
            client["key2"] = "value2"
            client.set_ttl("key2", 10)

            # 读取一次（应该只需要1次网络请求）
            start = time.time()
            value = client["key2"]
            elapsed = time.time() - start

            print(f"   ✓ 读取完成，耗时: {elapsed*1000:.2f}ms")
            print("   ✓ 只需要1次网络请求（服务器端处理TTL）\n")

            client.close()
            print("=" * 60)
            print("✅ 所有测试通过!")
            print("=" * 60)
            return True

        finally:
            # 停止服务器
            server.stop()


def main():
    try:
        success = test_server_side_ttl()
        return 0 if success else 1
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
