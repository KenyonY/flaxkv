"""
密钥管理模块

支持两种密钥派生方式：

方案1（文件存储）：基于密码自动管理CurveZMQ密钥对
- 密钥存储在 ~/.flaxkv/keys/ 目录
- 首次使用密码时自动生成并保存密钥对
- 后续使用相同密码时自动加载密钥对
- 私钥使用密码加密存储，确保安全性
- 优点：随机密钥，安全性更高
- 缺点：需要文件存储，不同机器需要复制密钥文件

方案2（密码派生，推荐）：从密码确定性派生密钥对
- 不需要文件存储
- 相同密码在任何机器上生成相同密钥对
- 优点：简单直观，跨机器使用方便
- 缺点：安全性依赖密码强度
"""

import os
import json
import hashlib
import base64
from pathlib import Path
from typing import Dict, Optional
import zmq

try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    Fernet = None
    hashes = None
    PBKDF2HMAC = None

# 尝试导入PyNaCl（用于确定性密钥生成）
try:
    import nacl.bindings
    import nacl.encoding
    NACL_AVAILABLE = True
except ImportError:
    NACL_AVAILABLE = False
    nacl = None


# 密钥派生缓存 - 避免重复计算PBKDF2 (性能优化)
# 格式: {password_hash: keypair_dict}
_keypair_cache: Dict[str, Dict[str, str]] = {}


class KeyManager:
    """密钥管理器"""

    # 默认密钥存储目录
    DEFAULT_KEY_DIR = Path.home() / '.flaxkv' / 'keys'

    def __init__(self, key_dir: Optional[str] = None):
        """
        初始化密钥管理器

        Args:
            key_dir: 密钥存储目录，默认为 ~/.flaxkv/keys/
        """
        self.key_dir = Path(key_dir) if key_dir else self.DEFAULT_KEY_DIR
        self.key_dir.mkdir(parents=True, exist_ok=True)

        # 检查加密库
        if not CRYPTO_AVAILABLE:
            raise RuntimeError(
                "Password-based key management requires cryptography package. "
                "Run: pip install cryptography"
            )

    def _password_to_key_id(self, password: str) -> str:
        """
        将密码转换为密钥文件ID（使用SHA256哈希）

        Args:
            password: 用户密码

        Returns:
            密钥文件ID（16进制字符串）
        """
        hash_obj = hashlib.sha256(password.encode('utf-8'))
        return hash_obj.hexdigest()

    def _derive_encryption_key(self, password: str, salt: bytes) -> bytes:
        """
        从密码派生加密密钥（用于加密私钥）

        Args:
            password: 用户密码
            salt: 盐值

        Returns:
            32字节的加密密钥
        """
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = kdf.derive(password.encode('utf-8'))
        return base64.urlsafe_b64encode(key)

    def _encrypt_secret_key(self, secret_key: str, password: str, salt: bytes) -> str:
        """
        加密私钥

        Args:
            secret_key: CurveZMQ私钥（Z85编码）
            password: 用户密码
            salt: 盐值

        Returns:
            加密后的私钥（base64编码）
        """
        encryption_key = self._derive_encryption_key(password, salt)
        fernet = Fernet(encryption_key)
        encrypted = fernet.encrypt(secret_key.encode('utf-8'))
        return base64.b64encode(encrypted).decode('utf-8')

    def _decrypt_secret_key(self, encrypted_secret_key: str, password: str, salt: bytes) -> str:
        """
        解密私钥

        Args:
            encrypted_secret_key: 加密后的私钥（base64编码）
            password: 用户密码
            salt: 盐值

        Returns:
            解密后的私钥（Z85编码）
        """
        encryption_key = self._derive_encryption_key(password, salt)
        fernet = Fernet(encryption_key)

        encrypted = base64.b64decode(encrypted_secret_key.encode('utf-8'))
        decrypted = fernet.decrypt(encrypted)
        return decrypted.decode('utf-8')

    def _get_key_file_path(self, password: str) -> Path:
        """
        获取密钥文件路径

        Args:
            password: 用户密码

        Returns:
            密钥文件路径
        """
        key_id = self._password_to_key_id(password)
        return self.key_dir / f"{key_id}.json"

    def generate_and_save_keypair(self, password: str) -> Dict[str, str]:
        """
        生成并保存密钥对

        Args:
            password: 用户密码

        Returns:
            包含 public_key 和 secret_key 的字典
        """
        # 生成CurveZMQ密钥对
        public_key, secret_key = zmq.curve_keypair()
        public_key = public_key.decode('utf-8')
        secret_key = secret_key.decode('utf-8')

        # 生成随机盐值
        salt = os.urandom(32)

        # 加密私钥
        encrypted_secret_key = self._encrypt_secret_key(secret_key, password, salt)

        # 保存到文件
        key_file = self._get_key_file_path(password)
        key_data = {
            'public_key': public_key,
            'encrypted_secret_key': encrypted_secret_key,
            'salt': base64.b64encode(salt).decode('utf-8'),
            'version': '1.0',
        }

        with open(key_file, 'w') as f:
            json.dump(key_data, f, indent=2)

        # 设置文件权限（只有所有者可读写）
        os.chmod(key_file, 0o600)

        return {
            'public_key': public_key,
            'secret_key': secret_key,
        }

    def load_keypair(self, password: str) -> Dict[str, str]:
        """
        加载密钥对

        Args:
            password: 用户密码

        Returns:
            包含 public_key 和 secret_key 的字典

        Raises:
            FileNotFoundError: 密钥文件不存在
            ValueError: 密码错误或密钥文件损坏
        """
        key_file = self._get_key_file_path(password)

        if not key_file.exists():
            raise FileNotFoundError(f"Key file not found for this password")

        # 读取密钥文件
        with open(key_file, 'r') as f:
            key_data = json.load(f)

        # 解析盐值
        salt = base64.b64decode(key_data['salt'].encode('utf-8'))

        # 解密私钥
        try:
            secret_key = self._decrypt_secret_key(
                key_data['encrypted_secret_key'],
                password,
                salt
            )
        except Exception as e:
            raise ValueError(f"Failed to decrypt secret key. Wrong password or corrupted key file: {e}")

        return {
            'public_key': key_data['public_key'],
            'secret_key': secret_key,
        }

    def get_or_create_keypair(self, password: str) -> Dict[str, str]:
        """
        获取或创建密钥对

        如果密钥文件存在则加载，否则生成新的密钥对

        Args:
            password: 用户密码

        Returns:
            包含 public_key 和 secret_key 的字典
        """
        try:
            # 尝试加载现有密钥
            return self.load_keypair(password)
        except FileNotFoundError:
            # 密钥不存在，生成新的
            return self.generate_and_save_keypair(password)

    def key_exists(self, password: str) -> bool:
        """
        检查密钥文件是否存在

        Args:
            password: 用户密码

        Returns:
            密钥文件是否存在
        """
        key_file = self._get_key_file_path(password)
        return key_file.exists()

    def delete_keypair(self, password: str) -> bool:
        """
        删除密钥对

        Args:
            password: 用户密码

        Returns:
            是否成功删除
        """
        key_file = self._get_key_file_path(password)
        if key_file.exists():
            key_file.unlink()
            return True
        return False

    def list_all_keys(self) -> list:
        """
        列出所有保存的密钥文件

        Returns:
            密钥文件路径列表
        """
        return list(self.key_dir.glob('*.json'))


# 全局密钥管理器实例
_default_key_manager = None


def get_default_key_manager() -> KeyManager:
    """获取默认的密钥管理器实例"""
    global _default_key_manager
    if _default_key_manager is None:
        _default_key_manager = KeyManager()
    return _default_key_manager


def get_keypair_from_password(password: str, derive_from_password: bool = True) -> Dict[str, str]:
    """
    从密码获取密钥对（便捷函数）

    Args:
        password: 用户密码
        derive_from_password: 是否直接从密码派生密钥（方案2，默认True）
                            False则使用文件存储方式（方案1）

    Returns:
        包含 public_key 和 secret_key 的字典
    """
    if derive_from_password:
        # 方案2：直接从密码派生（推荐）
        return derive_keypair_from_password(password)
    else:
        # 方案1：文件存储方式
        key_manager = get_default_key_manager()
        return key_manager.get_or_create_keypair(password)


def derive_keypair_from_password(password: str, salt: Optional[bytes] = None) -> Dict[str, str]:
    """
    方案2：从密码确定性派生CurveZMQ密钥对

    相同密码在任何机器上生成相同密钥对，无需文件存储

    Args:
        password: 用户密码
        salt: 可选的盐值（默认使用固定盐值以保证确定性）

    Returns:
        包含 public_key 和 secret_key 的字典

    Raises:
        RuntimeError: 如果缺少必要的库
    """
    # 使用固定盐值确保确定性（相同密码生成相同密钥）
    if salt is None:
        salt = b'FlaxKV_CurveZMQ_Salt_v1'

    # 生成缓存键（密码+盐的哈希）
    cache_key = hashlib.sha256(password.encode('utf-8') + salt).hexdigest()

    # 检查缓存
    if cache_key in _keypair_cache:
        return _keypair_cache[cache_key]

    # 检查是否有必要的库
    if not NACL_AVAILABLE:
        # 降级方案：使用基于密码的简单派生
        keypair = _derive_keypair_simple(password)
        _keypair_cache[cache_key] = keypair
        return keypair

    # 使用PBKDF2从密码派生32字节种子
    kdf = hashlib.pbkdf2_hmac(
        'sha256',
        password.encode('utf-8'),
        salt,
        iterations=100000,
        dklen=32
    )

    # 使用种子确定性生成Curve25519密钥对
    public_key_bytes, secret_key_bytes = nacl.bindings.crypto_box_seed_keypair(kdf)

    # 转换为ZMQ的Z85编码格式
    # PyZMQ 的 z85 编码直接在 zmq 命名空间下
    try:
        # 尝试新版本 PyZMQ 的 API
        public_key = zmq.z85.encode(public_key_bytes).decode('utf-8')
        secret_key = zmq.z85.encode(secret_key_bytes).decode('utf-8')
    except AttributeError:
        # 兼容旧版本 PyZMQ
        import zmq.utils.z85 as z85_module
        public_key = z85_module.encode(public_key_bytes).decode('utf-8')
        secret_key = z85_module.encode(secret_key_bytes).decode('utf-8')

    keypair = {
        'public_key': public_key,
        'secret_key': secret_key,
    }

    # 缓存结果
    _keypair_cache[cache_key] = keypair
    return keypair


def _derive_keypair_simple(password: str) -> Dict[str, str]:
    """
    降级方案：当PyNaCl不可用时，使用简单的密钥派生

    注意：此方案的安全性低于使用PyNaCl的方案，但足够大多数场景使用

    Args:
        password: 用户密码

    Returns:
        包含 public_key 和 secret_key 的字典
    """
    # 生成缓存键（密码的哈希）
    salt = b'FlaxKV_Simple_Salt_v1'
    cache_key = hashlib.sha256(password.encode('utf-8') + salt + b'_simple').hexdigest()

    # 检查缓存
    if cache_key in _keypair_cache:
        return _keypair_cache[cache_key]

    # 从密码派生64字节的数据（32字节secret_key + 32字节用于公钥派生）
    derived = hashlib.pbkdf2_hmac(
        'sha256',
        password.encode('utf-8'),
        salt,
        iterations=100000,
        dklen=64
    )

    # 前32字节作为私钥
    secret_key_bytes = derived[:32]

    # 使用ZMQ从私钥派生公钥
    # 注意：这不是标准的Curve25519派生，但对于演示目的足够
    # 我们需要确保密钥符合ZMQ的格式要求

    # 将私钥转换为Z85格式
    # PyZMQ 的 z85 编码直接在 zmq 命名空间下
    try:
        # 尝试新版本 PyZMQ 的 API
        secret_key = zmq.z85.encode(secret_key_bytes).decode('utf-8')
    except AttributeError:
        # 兼容旧版本 PyZMQ
        import zmq.utils.z85 as z85_module
        secret_key = z85_module.encode(secret_key_bytes).decode('utf-8')

    # 从私钥派生公钥
    public_key_bytes = zmq.curve_public(secret_key.encode('utf-8'))
    public_key = public_key_bytes.decode('utf-8')

    keypair = {
        'public_key': public_key,
        'secret_key': secret_key,
    }

    # 缓存结果
    _keypair_cache[cache_key] = keypair
    return keypair
