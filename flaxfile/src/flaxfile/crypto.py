"""
FlaxFile 加密工具 - 基于 CurveZMQ (Curve25519)
"""

import os
import hashlib
import getpass
from typing import Tuple, Optional
import zmq


def derive_server_keypair(password: str) -> Tuple[bytes, bytes]:
    """
    从密码派生服务器密钥对（确定性生成）

    Args:
        password: 用户密码

    Returns:
        (secret_key, public_key) 元组
    """
    # 使用 PBKDF2 派生 32 字节密钥
    secret_key_bytes = hashlib.pbkdf2_hmac(
        'sha256',
        password.encode('utf-8'),
        b'flaxfile-server-v1',  # 服务器盐
        iterations=100000,
        dklen=32
    )

    # 使用 ZMQ 的 curve_keypair 从种子生成
    # 注意：zmq.curve_keypair() 生成随机密钥对
    # 我们需要使用确定性方法
    from nacl.public import PrivateKey

    private_key = PrivateKey(secret_key_bytes)
    public_key = private_key.public_key

    return bytes(private_key), bytes(public_key)


def derive_client_keypair() -> Tuple[bytes, bytes]:
    """
    生成客户端临时密钥对（每次连接随机生成）

    Returns:
        (secret_key, public_key) 元组
    """
    public_key, secret_key = zmq.curve_keypair()
    return secret_key, public_key


def get_password(
    prompt: str = "请输入密码: ",
    allow_empty: bool = False,
    env_var: str = "FLAXFILE_PASSWORD"
) -> Optional[str]:
    """
    获取密码（优先级：环境变量 > 交互输入）

    Args:
        prompt: 输入提示
        allow_empty: 是否允许空密码
        env_var: 环境变量名

    Returns:
        密码字符串，如果允许为空且用户选择无加密则返回 None
    """
    # 1. 优先从环境变量读取
    password = os.getenv(env_var)
    if password:
        return password

    # 2. 交互式输入
    if allow_empty:
        # 使用简单的 input() 避免 questionary 的事件循环问题
        response = input("是否启用加密? (需要设置密码) [Y/n]: ").strip().lower()

        if response in ['n', 'no']:
            return None

    # 输入密码
    password = getpass.getpass(prompt)

    # 验证密码强度
    if password and len(password) < 8:
        from rich.console import Console
        Console().print("[yellow]⚠️  警告: 密码强度较弱，建议使用至少 16 个字符的强密码")

    return password if password else None


def configure_server_encryption(socket: zmq.Socket, password: Optional[str]) -> bool:
    """
    配置服务器端加密

    Args:
        socket: ZMQ ROUTER socket
        password: 密码（None 表示不加密）

    Returns:
        是否启用了加密
    """
    if not password:
        return False

    secret_key, public_key = derive_server_keypair(password)

    socket.curve_server = True
    socket.curve_secretkey = secret_key

    return True


def configure_client_encryption(
    socket: zmq.Socket,
    password: Optional[str]
) -> bool:
    """
    配置客户端加密

    Args:
        socket: ZMQ DEALER socket
        password: 密码（None 表示不加密）

    Returns:
        是否启用了加密
    """
    if not password:
        return False

    # 从密码计算出服务器公钥
    _, server_public_key = derive_server_keypair(password)

    # 生成客户端临时密钥对
    client_secret_key, client_public_key = derive_client_keypair()

    socket.curve_serverkey = server_public_key
    socket.curve_publickey = client_public_key
    socket.curve_secretkey = client_secret_key

    return True


def get_key_fingerprint(public_key: bytes) -> str:
    """
    计算公钥指纹（用于验证）

    Args:
        public_key: 公钥字节

    Returns:
        SHA256 指纹字符串
    """
    fingerprint = hashlib.sha256(public_key).hexdigest()
    return f"SHA256:{fingerprint[:32]}..."
