# FlaxKV2

FlaxKV2是一个高性能的键值数据库系统，提供类似于Python字典的接口来操作持久化存储。它基于LevelDB后端，具有高性能、易用性和强大的功能支持。

## 主要特点

- **无阻塞设计**：用户进程不会因写操作而阻塞，同时保证读取的数据始终是最新的
- **易用性**：接口设计类似于Python字典，使用简单直观
- **缓冲写入**：数据先缓存再批量写入数据库，减少频繁写入的开销
- **高性能后端**：使用高性能的LevelDB作为后端存储
- **原子操作**：确保写操作的原子性，保障数据完整性
- **线程安全**：使用最少的锁确保并发访问安全，同时保持高性能
- **分布式支持**：支持远程数据库访问，可作为服务运行
- **高级功能**：支持二级索引、TTL、布隆过滤器、LRU缓存等特性

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

#### 二级索引

```python
from flaxkv2 import FlaxKV

db = FlaxKV("users_db", "./data")

# 添加用户数据
users = [
    {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
    {"id": 2, "name": "Bob", "age": 25, "city": "Boston"},
    {"id": 3, "name": "Charlie", "age": 35, "city": "New York"},
    {"id": 4, "name": "David", "age": 28, "city": "Boston"},
]

# 添加数据到数据库
for user in users:
    db[f"user:{user['id']}"] = user

# 创建城市索引
db.create_hash_index("city_index", lambda x: x.get("city"))

# 创建年龄范围索引
db.create_range_index("age_index", lambda x: x.get("age"))

# 查询住在纽约的用户
ny_user_keys = db.query_index("city_index", "New York")
ny_users = [db[key] for key in ny_user_keys]

# 查询25-30岁的用户
young_user_keys = db.query_index("age_index", min_value=25, max_value=30, include_max=True)
young_users = [db[key] for key in young_user_keys]
```

### 远程数据库

#### 启动服务器

```bash
flaxkv2 run --host 0.0.0.0 --port 8000 --data-dir ./data
```

#### 连接远程数据库

```python
from flaxkv2 import FlaxKV

# 连接远程数据库
db = FlaxKV("remote_db", "http://localhost:8000")

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
- litestar, uvicorn (服务器)
- httpx (客户端)

## 许可证

MIT 