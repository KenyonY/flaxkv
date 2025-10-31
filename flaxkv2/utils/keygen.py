"""
CurveZMQ密钥生成工具

用于生成服务器和客户端的加密密钥对
"""

import zmq


def generate_curve_keypair() -> dict:
    """
    生成CurveZMQ密钥对

    Returns:
        包含public_key和secret_key的字典（Z85编码）
    """
    public_key, secret_key = zmq.curve_keypair()
    return {
        'public_key': public_key.decode('utf-8'),
        'secret_key': secret_key.decode('utf-8'),
    }


def main():
    """命令行工具：生成密钥对"""
    print("=" * 60)
    print("CurveZMQ密钥生成工具")
    print("=" * 60)

    keypair = generate_curve_keypair()

    print("\n服务器密钥对：")
    print(f"  Public Key:  {keypair['public_key']}")
    print(f"  Secret Key:  {keypair['secret_key']}")

    print("\n使用方法：")
    print("  服务器启动时使用 secret_key：")
    print(f"    server = FlaxKVServer(enable_encryption=True, server_secret_key='{keypair['secret_key']}')")
    print("\n  客户端连接时使用 public_key：")
    print(f"    db = RemoteDBDict('mydb', enable_encryption=True, server_public_key='{keypair['public_key']}')")
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
