# FlaxKV2 密码认证使用指南

FlaxKV2 支持两种基于密码的加密认证方案。

## 方案对比

| 特性 | 方案2：密码派生（推荐） | 方案1：文件存储 |
|------|----------------------|----------------|
| **密钥生成** | 从密码确定性派生 | 随机生成并存储 |
| **跨机器使用** | ✅ 相同密码在任何机器生成相同密钥 | ❌ 需要手动复制密钥文件 |
| **文件存储** | ✅ 不需要文件 | ❌ 需要 ~/.flaxkv/keys/ |
| **安全性** | 依赖密码强度 | 随机密钥，更安全 |
| **易用性** | ⭐⭐⭐⭐⭐ 最简单 | ⭐⭐⭐ 需要管理文件 |
| **推荐场景** | 跨机器部署、简单部署 | 单机部署、高安全需求 |

## 快速开始

### 方案2：密码派生（推荐，默认）

**只需记住密码，不需要任何文件！**

```python
from flaxkv2.server.zmq_server import FlaxKVServer
from flaxkv2.client.zmq_client import RemoteDBDict

PASSWORD = "your_secure_password_123"

# 启动服务器（可以在机器A上）
server = FlaxKVServer(
    host="0.0.0.0",
    port=5555,
    enable_encryption=True,
    password=PASSWORD,  # 只需提供密码！
    enable_compression=True,
)
server.run()

# 连接客户端（可以在机器B上）
client = RemoteDBDict(
    'mydb',
    host="192.168.1.100",  # 服务器IP
    port=5555,
    enable_encryption=True,
    password=PASSWORD,  # 使用相同密码即可！
    enable_compression=True,
    read_cache_size=1000,  # 可选：启用缓存
)

# 正常使用
client['key'] = 'value'
print(client['key'])  # 输出: value
```

### 方案1：文件存储

**适合单机部署或高安全需求场景**

```python
# 启动服务器
server = FlaxKVServer(
    enable_encryption=True,
    password=PASSWORD,
    derive_from_password=False,  # 使用文件存储
)

# 连接客户端（同一台机器或需要复制密钥文件）
client = RemoteDBDict(
    'mydb',
    enable_encryption=True,
    password=PASSWORD,
    derive_from_password=False,  # 使用文件存储
)
```

密钥文件位置：`~/.flaxkv/keys/`

## 详细说明

### 方案2工作原理

1. **服务器启动**：
   - 使用密码通过PBKDF2 + Curve25519确定性派生密钥对
   - 相同密码 → 相同密钥对

2. **客户端连接**：
   - 使用相同密码派生出相同的密钥对
   - 能够与服务器建立加密连接

3. **跨机器部署**：
   - 无需复制任何文件
   - 只需确保服务器和客户端使用相同密码

### 方案1工作原理

1. **首次使用**：
   - 生成随机CurveZMQ密钥对
   - 使用密码加密私钥
   - 保存到 `~/.flaxkv/keys/[密码哈希].json`

2. **后续使用**：
   - 根据密码找到密钥文件
   - 使用密码解密私钥
   - 使用相同密钥对通信

3. **跨机器部署**：
   - 需要手动复制密钥文件到其他机器
   - 或者使用服务器公钥（只读场景）

## 安全建议

### 密码强度要求

**最低要求**：
- 长度 ≥ 16 字符
- 包含大小写字母、数字、特殊字符
- 不要使用常见密码

**推荐**：
```python
# 生成强密码示例
import secrets
import string

def generate_strong_password(length=32):
    alphabet = string.ascii_letters + string.digits + string.punctuation
    return ''.join(secrets.choice(alphabet) for _ in range(length))

PASSWORD = generate_strong_password()
print(f"Generated password: {PASSWORD}")
# 保存到环境变量或配置文件中
```

### 生产环境配置

```python
import os

# 从环境变量读取密码（推荐）
PASSWORD = os.getenv('FLAXKV_PASSWORD')
if not PASSWORD:
    raise ValueError("Please set FLAXKV_PASSWORD environment variable")

# 启动服务器
server = FlaxKVServer(
    host="0.0.0.0",
    port=5555,
    enable_encryption=True,
    password=PASSWORD,
    enable_compression=True,
)
```

### 配置文件示例

```python
# config.py
import os

class Config:
    # 服务器配置
    SERVER_HOST = "0.0.0.0"
    SERVER_PORT = 5555

    # 安全配置
    ENABLE_ENCRYPTION = True
    ENABLE_COMPRESSION = True
    PASSWORD = os.getenv('FLAXKV_PASSWORD', 'default_dev_password')

    # 性能配置
    READ_CACHE_SIZE = 10000

# 使用配置
server = FlaxKVServer(
    host=Config.SERVER_HOST,
    port=Config.SERVER_PORT,
    enable_encryption=Config.ENABLE_ENCRYPTION,
    password=Config.PASSWORD,
    enable_compression=Config.ENABLE_COMPRESSION,
)
```

## 常见问题

### Q: 两种方案可以互相切换吗？

A: 不能直接切换。它们生成的密钥不同：
- 方案1：随机生成（每次不同）
- 方案2：从密码派生（确定性）

切换方案需要重新建立连接。

### Q: 如果忘记密码怎么办？

- **方案1**：密钥文件仍在，但无法解密私钥 → 数据无法访问
- **方案2**：无法重新派生密钥 → 数据无法访问

**建议**：
- 妥善保存密码
- 定期备份数据库文件

### Q: 方案2的安全性如何？

A: 方案2使用工业级密码学：
- PBKDF2-HMAC-SHA256（100,000次迭代）
- Curve25519椭圆曲线密钥对
- ZMQ的CurveZMQ加密协议

安全性主要依赖密码强度。使用强密码（≥16字符）时，安全性足够高。

### Q: 生成密钥工具

```bash
# 生成CurveZMQ密钥对（用于查看或手动管理）
python -m flaxkv2.utils.keygen

# 或在代码中
from flaxkv2.utils.keygen import generate_curve_keypair
keypair = generate_curve_keypair()
print(f"Public:  {keypair['public_key']}")
print(f"Secret:  {keypair['secret_key']}")
```

## 性能建议

启用加密后的性能配置：

```python
# 服务器端
server = FlaxKVServer(
    enable_encryption=True,
    password=PASSWORD,
    enable_compression=True,  # 建议启用，减少50-70%网络流量
)

# 客户端
client = RemoteDBDict(
    'mydb',
    enable_encryption=True,
    password=PASSWORD,
    enable_compression=True,
    read_cache_size=10000,  # 建议启用缓存，减少网络请求
)
```

## 总结

**推荐方案**：
- 🥇 **方案2（密码派生）**：适合99%的使用场景
  - 简单易用，只需记住密码
  - 支持跨机器部署
  - 无需管理密钥文件

- 🥈 **方案1（文件存储）**：仅在以下情况使用
  - 需要最高级别的安全性
  - 单机部署
  - 愿意手动管理密钥文件
