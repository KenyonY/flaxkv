# FlaxKV2

FlaxKV2是一个高性能的键值数据库系统，提供类似于Python字典的接口来操作持久化存储。它基于LevelDB后端，具有高性能、易用性和强大的功能支持。

## 主要特点

- **易用性**：接口设计类似于Python字典，使用简单直观
- **高性能后端**：使用高性能的LevelDB作为后端存储
- **原子操作**：确保写操作的原子性，保障数据完整性
- **线程安全**：使用最少的锁确保并发访问安全，同时保持高性能
- **分布式支持**：支持远程数据库访问（基于ZeroMQ），可作为服务运行
- **TTL支持**：支持键的过期时间设置
- **嵌套存储**：支持高效的嵌套字典存储，避免频繁序列化

## 安装

```bash
pip install flaxkv2
```

## 使用方法

### 基本使用

```python
from flaxkv2 import FlaxKV

# 创建/打开数据库
db = FlaxKV("my_db", "./data")

# 写入数据
db["key1"] = "value1"
db["key2"] = 100
db["key3"] = {"nested": "data"}

# 批量写入
db.update({
    "key4": [1, 2, 3],
    "key5": "batch value"
})

# 读取数据
value = db["key1"]  # "value1"

# 检查键是否存在
if "key2" in db:
    print("Key exists!")

# 获取所有键值对
for k, v in db.items():
    print(f"{k}: {v}")

# 关闭数据库
db.close()
```

### 自动关闭功能

FlaxKV2具有自动关闭功能，当程序正常或异常退出时，会自动关闭所有打开的数据库实例，确保数据被正确保存。

```python
from flaxkv2 import FlaxKV

# 创建数据库
db = FlaxKV("my_db", "./data")

# 写入数据
db["key"] = "value"

# 无需显式调用close()，程序退出时会自动关闭
```

### 上下文管理器支持

FlaxKV2支持使用上下文管理器（with语句），离开上下文时会自动关闭数据库。

```python
from flaxkv2 import FlaxKV

# 使用上下文管理器
with FlaxKV("my_db", "./data") as db:
    # 在上下文中操作数据库
    db["key"] = "value"
    print(db["key"])
    
# 离开上下文后，数据库自动关闭
```

## 快速入门

### 基本用法

```python
from flaxkv2 import FlaxKV

# 创建数据库
db = FlaxKV("test_db", "./data")

# 写入数据
db["key1"] = "value1"
db["key2"] = {"name": "test", "value": 123}
db["key3"] = [1, 2, 3, 4, 5]

# 读取数据
value1 = db["key1"]  # "value1"
value2 = db["key2"]  # {"name": "test", "value": 123}

# 判断键是否存在
if "key1" in db:
    print("Key exists!")

# 获取所有键
keys = db.keys()

# 删除键
del db["key1"]

# 批量更新
db.update({"key4": 4, "key5": 5})

# 关闭数据库（自动保存）
db.close()
```

### 高级功能

#### TTL (生存时间)

```python
from flaxkv2 import FlaxKV

db = FlaxKV("test_db", "./data")

# 设置键值
db["temp_key"] = "This will expire"

# 设置TTL（10秒后过期）
db.set_ttl("temp_key", 10)

# 获取剩余过期时间
ttl = db.get_ttl("temp_key")
print(f"Key will expire in {ttl} seconds")

# 10秒后...
# db["temp_key"]  # 将抛出KeyError
```

### 远程数据库

#### 启动服务器

```bash
flaxkv2 run --host 0.0.0.0 --port 5555 --data-dir ./data
```

#### 连接远程数据库

```python
from flaxkv2 import FlaxKV

# 连接远程数据库（推荐显式指定 backend='remote'）
db = FlaxKV("remote_db", "127.0.0.1:5555", backend='remote')

# 或使用 tcp:// 前缀自动识别
db = FlaxKV("remote_db", "tcp://127.0.0.1:5555")

# 使用方式与本地数据库相同
db["remote_key"] = "Remote value"
value = db["remote_key"]
```

## 依赖

- Python >= 3.8
- plyvel (LevelDB绑定)
- msgpack (序列化)
- numpy, pandas (支持数组和DataFrame)
- loguru (日志)
- pyzmq (远程服务器和客户端)

## 许可证

MIT 