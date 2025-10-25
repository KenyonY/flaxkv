# FlaxKV2 技术文档

## 1. 项目概述

FlaxKV2 是一个高性能的键值数据库系统，提供类似于 Python 字典的接口来操作持久化存储。它具有高读写性能，同时保证数据持久性和并发安全，特别适合于机器学习等需要存储大量数据的应用场景。

### 1.1 主要特点

- **无阻塞设计**：用户进程不会因写操作而阻塞，同时保证读取的数据始终是最新的
- **易用性**：接口设计类似于 Python 字典，使用简单直观
- **缓冲写入**：数据先缓存再批量写入数据库，减少频繁写入的开销
- **高性能后端**：默认使用高性能的 LevelDB 作为后端存储
- **原子操作**：确保写操作的原子性，保障数据完整性
- **线程安全**：使用最少的锁确保并发访问安全，同时保持高性能
- **分布式支持**：支持远程数据库访问，可作为服务运行
- **高级功能**：支持二级索引、TTL、布隆过滤器、LRU缓存等特性
- **自动关闭**：程序退出时自动关闭数据库，确保数据正确保存
- **上下文管理器**：支持 `with` 语句管理数据库生命周期

## 2. 核心架构

FlaxKV2 的整体架构由以下几个主要部分组成：

1. **对外接口层**：提供类似字典的使用接口
2. **数据管理层**：管理数据的读写、缓冲和提交
3. **序列化层**：处理数据的编码和解码
4. **存储层**：实际的数据库后端(LevelDB)
5. **远程服务层**：提供网络访问接口
6. **高级功能层**：提供索引、TTL、布隆过滤器等高级功能

## 3. 模块详解

### 3.1 核心模块

核心模块定义了基本的数据库字典接口和实现，提供与 Python 字典相似的操作方法。

#### 3.1.1 `BaseDBDict` 类

这是所有数据库字典实现的基类，定义了通用的接口和行为：

- **缓冲区管理**：维护一个内存缓冲区，延迟写入数据库
- **后台工作线程**：定期将缓冲区数据提交到数据库
- **自动资源释放**：在程序退出时自动关闭数据库
- **字典接口**：提供 `__getitem__`, `__setitem__`, `__delitem__` 等方法

关键配置参数：
- `MAX_BUFFER_SIZE`: 缓冲区最大条目数，默认 100
- `COMMIT_TIME_INTERVAL`: 自动提交间隔，默认 10 分钟

#### 3.1.2 `LevelDBDict` 类

基于 LevelDB 实现的字典接口，作为默认的存储后端，提供高性能的键值存储：

- 使用 plyvel 库与 LevelDB 交互
- 实现批量写入操作，提高写入性能
- 支持高级功能如布隆过滤器、TTL、索引等

#### 3.1.3 `RemoteDBDict` 类

远程数据库字典实现，通过 HTTP 与远程 FlaxKV2 服务器通信：

- 使用 httpx 库进行网络通信
- 实现重试机制，提高可靠性
- 支持批量操作，减少网络请求次数

#### 3.1.4 `FlaxKV` 类

主接口类，提供工厂方法创建合适的数据库实现：

- 根据路径类型判断是本地模式还是远程模式
- 支持重建数据库
- 提供缓存和原始模式选项

### 3.2 缓冲和缓存模块

负责高效管理内存数据，提高读写性能。

#### 3.2.1 `LRUCache` 类

实现了 LRU (最近最少使用) 缓存策略：

- 使用 OrderedDict 维护访问顺序
- 支持容量限制，自动淘汰最久未使用的项
- 线程安全设计，支持并发访问

#### 3.2.2 `TieredBuffer` 类

分层缓冲区实现，支持热/冷数据分层：

- 将频繁访问的数据保存在热缓存
- 不常访问的数据移到冷缓存
- 数据从冷缓存升级到热缓存
- 优化内存使用和访问性能

### 3.3 序列化模块

负责数据的编码和解码，支持多种 Python 数据类型。

#### 3.3.1 数据编码

对各种数据类型进行序列化：

- **基础类型**：int, float, bool, str
- **容器类型**：list, dict, tuple
- **特殊类型**：bytes, numpy.ndarray
- 使用类型前缀确保编码的唯一性

#### 3.3.2 键编码

对键进行特殊编码，确保排序性质：

- 整数编码使用网络字节序，保证顺序性
- 浮点数编码保持排序特性
- 元组编码为连续的各元素编码，保持元素间关系

### 3.4 高级功能模块

提供增强的数据管理和查询能力。

#### 3.4.1 索引系统

支持多种索引类型，提高查询效率：

- **哈希索引**：快速点查询
- **范围索引**：支持范围查询
- 支持复合索引和自定义索引
- 自动维护索引一致性

#### 3.4.2 TTL 机制

支持键值对自动过期：

- 为键设置生存时间
- 支持惰性清理和主动清理两种模式
- 提供查询剩余生存时间的功能
- 支持过期时间更新
- **持久化TTL**：支持在程序重启后保持TTL生效
- **默认TTL**：可以为数据库实例设置默认TTL，自动应用于新添加的键

#### TTL 实现原理

TTL功能由 `TTLManager` 类实现，主要设计逻辑如下：

1. **内存管理**：
   - 使用内存字典存储键的过期时间
   - 通过线程安全锁确保并发访问安全
   - 定期清理过期键

2. **过期检测**：
   - 读取键时检查是否过期
   - 支持主动获取已过期键列表
   - 定期批量清理过期键

3. **持久化实现**：
   - 使用特殊前缀(`__ttl_info__:`)标记TTL信息键
   - 将过期时间戳作为值存储在底层数据库
   - 程序启动时加载TTL信息并更新内存字典
   - 程序关闭时确保TTL信息持久化保存

4. **默认TTL机制**：
   - 数据库实例可设置默认TTL值
   - 在`__setitem__`操作中自动应用默认TTL
   - 支持批量操作中应用默认TTL
   - 可动态更新或禁用默认TTL

5. **优化策略**：
   - 避免递归调用造成的栈溢出
   - 直接操作底层数据库对象进行TTL信息存储
   - 在加载TTL信息时自动清理已过期的键

#### TTL 代码设计

TTL功能的核心代码组织：

```python
# TTLManager类管理所有TTL相关功能
class TTLManager:
    # 设置键的TTL
    def set(self, key, ttl_seconds):
        with self._lock:
            expiry_time = time.time() + ttl_seconds
            self._expiry_dict[key] = expiry_time
            # 持久化TTL信息
            self._save_ttl_info(key, expiry_time)
    
    # 检查键是否过期
    def is_expired(self, key):
        with self._lock:
            expiry_time = self._expiry_dict.get(key)
            if expiry_time is None:
                return False
            return time.time() > expiry_time
    
    # 从数据库加载TTL信息
    def _load_ttl_data(self):
        # 遍历数据库中所有的TTL信息键
        for key in self._db.keys():
            if key.startswith("__ttl_info__:"):
                original_key = key[len("__ttl_info__:"):]
                expiry_time = float(self._db[key])
                
                # 检查是否已过期
                if expiry_time > time.time():
                    # 未过期，加载到内存
                    self._expiry_dict[original_key] = expiry_time
                else:
                    # 已过期，从数据库中删除
                    del self._db[key]
                    if original_key in self._db:
                        del self._db[original_key]
```

数据库类中集成TTL功能：

```python
class LevelDBDict(BaseDBDict):
    def __init__(self, ..., default_ttl=None, ...):
        # 保存默认TTL设置
        self._default_ttl = default_ttl
        # 初始化TTL管理器
        self._ttl_manager = TTLManager()
    
    # 重写__setitem__方法，应用默认TTL
    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        # 如果设置了默认TTL，则自动应用
        if self._default_ttl is not None:
            self._ttl_manager.set(key, self._default_ttl)
    
    # 设置默认TTL值
    def set_default_ttl(self, ttl_seconds):
        self._default_ttl = ttl_seconds
    
    # 获取默认TTL值
    def get_default_ttl(self):
        return self._default_ttl
    
    # 批量更新操作中应用默认TTL
    def update(self, d):
        # 获取新键列表
        new_keys = [key for key in d.keys() if key not in self]
        # 执行原始更新操作
        super().update(d)
        # 为新键应用默认TTL
        if self._default_ttl is not None:
            for key in new_keys:
                self._ttl_manager.set(key, self._default_ttl)
```

### 3.4.3 布隆过滤器

优化键存在性检查：

- 减少不必要的磁盘查询
- 支持容量调整
- 保证无假阴性

### 3.5 日志模块

提供详细的日志记录功能：

- 基于 loguru 库实现彩色日志输出
- 支持不同级别的日志记录
- 可通过环境变量控制日志级别
- 支持添加文件日志

### 3.6 远程服务模块

提供分布式数据库功能：

- 基于 HTTP 协议的远程访问
- 支持批量操作减少网络延迟
- 错误重试机制提高可靠性

## 4. 数据流程

### 4.1 写入流程

1. 调用 `__setitem__` 或相关方法
2. 数据添加到内存缓冲区 `_buffer_dict`
3. 当缓冲区达到阈值或定时器触发时，后台线程将数据写入数据库
4. 在数据写入前更新索引和TTL信息
5. 写入完成后，更新内存缓存和布隆过滤器

### 4.2 读取流程

1. 调用 `__getitem__` 或相关方法
2. 先检查内存缓冲区是否有数据
3. 如果没有，再检查缓存系统
4. 如果仍然没有，使用布隆过滤器快速检查键是否可能存在
5. 如果布隆过滤器表明键可能存在，从数据库读取
6. 检查TTL判断键是否过期
7. 解码数据并返回结果

### 4.3 远程操作流程

1. 客户端通过 HTTP 请求发送操作
2. 服务器接收请求并验证
3. 服务器执行相应的数据库操作
4. 结果返回给客户端

## 5. 使用示例

### 5.1 基本使用

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

### 5.2 上下文管理器

```python
from flaxkv2 import FlaxKV

# 使用上下文管理器自动关闭数据库
with FlaxKV("my_db", "./data") as db:
    db["key"] = "value"
    print(db["key"])
# 退出上下文后自动关闭数据库
```

### 5.3 远程数据库

#### 5.3.1 启动服务器

```python
# 服务器端代码示例 (server.py)
import os
import uvicorn
from flaxkv2.server.app import create_app

# 设置数据存储目录
os.environ["FLAXKV_DATA_DIR"] = "/path/to/data"

# 创建应用
app = create_app()

# 启动服务器
if __name__ == "__main__":
    # 启动HTTP服务器
    uvicorn.run(
        app, 
        host="0.0.0.0",  # 监听所有网络接口
        port=8766,       # 服务端口
        log_level="info" # 日志级别
    )
```

#### 5.3.2 连接远程数据库

```python
from flaxkv2 import FlaxKV

# 方式1: 连接远程数据库服务器
db = FlaxKV("my_remote_db", "http://server:8766")

# 方式2: 显式指定根路径
db = FlaxKV("my_remote_db", "http://server:8766", root_path="/path/on/server")

# 使用与本地数据库相同的接口
db["key1"] = "value1"
db["key2"] = {"nested": "data", "number": 42}

# 检查键是否存在
if "key1" in db:
    print("键存在:", db["key1"])  # 输出: 键存在: value1

# 获取值，不存在时返回默认值
value = db.get("nonexistent", "默认值")
print(value)  # 输出: 默认值

# 批量操作 - 更高效的远程批量更新
batch_data = {f"batch_key{i}": f"batch_value{i}" for i in range(10)}
db.update(batch_data)

# 获取所有键
keys = db.keys()
print(f"数据库中有 {len(keys)} 个键")

# 获取所有键值对
items = db.items()
print("前3个键值对:")
for k, v in list(items)[:3]:
    print(f"  {k}: {v}")

# 支持TTL功能
db["temp_key"] = "临时数据"
db.set_ttl("temp_key", 60)  # 60秒后过期
print(f"临时键剩余时间: {db.get_ttl('temp_key')}秒")

# 使用默认TTL
db_with_ttl = FlaxKV("ttl_remote_db", "http://server:8766", default_ttl=30)
db_with_ttl["auto_ttl_key"] = "自动过期数据"  # 30秒后自动过期

# 关闭连接
db.close()
```

#### 5.3.3 远程数据库连接配置

```python
from flaxkv2 import FlaxKV

# 配置超时和重试机制
db = FlaxKV(
    "reliable_remote_db", 
    "http://server:8766",
    # 高级连接选项
    timeout=5.0,       # 请求超时时间（秒）
    max_retries=5,     # 请求失败后最大重试次数
    retry_delay=1.0,   # 重试延迟（秒）
    root_path="/data", # 服务器上的数据存储路径
)

# 检查服务器连接状态
if db.ping():
    print("服务器连接正常")
else:
    print("无法连接到服务器")

# 使用数据库
db["key"] = "value"

# 关闭连接
db.close()
```

#### 5.3.4 完整的分布式应用示例

```python
# 演示完整的分布式应用流程

# 服务器端
# ----------
# 1. 安装依赖
# pip install flaxkv2[server]
#
# 2. 创建服务器启动脚本 (server.py)
'''
import os
from flaxkv2.server.app import create_app
import uvicorn

# 配置数据目录
os.environ["FLAXKV_DATA_DIR"] = "./data"
# 可选: 配置调试模式
os.environ["FLAXKV_DEBUG"] = "1"

# 创建应用
app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8766)
'''
# 3. 启动服务器
# python server.py

# 客户端
# --------
# 1. 安装依赖
# pip install flaxkv2
#
# 2. 客户端代码
'''
from flaxkv2 import FlaxKV
import time

# 连接远程数据库
db = FlaxKV("shared_db", "http://localhost:8766")

# 添加数据
db["app_name"] = "分布式示例应用"
db["timestamp"] = time.time()
db["config"] = {
    "max_users": 1000,
    "timeout": 30,
    "features": ["search", "export", "import"]
}

# 批量添加数据
db.update({
    "user:1": {"name": "张三", "role": "admin"},
    "user:2": {"name": "李四", "role": "user"},
    "user:3": {"name": "王五", "role": "user"}
})

# 读取并处理数据
users = [v for k, v in db.items() if k.startswith("user:")]
admin_users = [user for user in users if user.get("role") == "admin"]
print(f"管理员用户: {len(admin_users)}人")

# 关闭连接
db.close()
'''
```

### 5.4 高级功能

```python
from flaxkv2 import FlaxKV
import time

# 创建数据库
db = FlaxKV("advanced_db", "./data", cache=True)

# 使用TTL功能
db.set_with_ttl("temp_key", "temporary value", ttl=60)  # 60秒后过期

# 创建索引
db.create_hash_index("user_index", lambda x: x.get("username") if isinstance(x, dict) else None)

# 添加用户数据
db["user1"] = {"username": "alice", "age": 30}
db["user2"] = {"username": "bob", "age": 25}

# 使用索引查询
users = db.query_by_index("user_index", "alice")
print(users)  # [{"username": "alice", "age": 30}]

# 关闭数据库
db.close()
```

### 5.5 TTL 使用示例

以下示例展示如何使用 TTL 功能：

```python
from flaxkv2 import FlaxKV
import time

# 使用TTL
db = FlaxKV("ttl_demo", "./data")

# 设置键值对和TTL
db["temp_key"] = "temporary data"
db.set_ttl("temp_key", 5)  # 5秒后过期

# 检查TTL
print(f"剩余时间: {db.get_ttl('temp_key')}秒")  # 约5秒

# 等待TTL过期
time.sleep(5)
print("temp_key" in db)  # False，键已过期

# 使用持久化TTL
db["persist_key"] = "data"
db.set_ttl("persist_key", 60)  # 60秒后过期
db.close()

# ...程序重启...

# 再次打开数据库
db = FlaxKV("ttl_demo", "./data")
# TTL仍然有效，且已减少经过的时间
print(f"重启后剩余时间: {db.get_ttl('persist_key')}秒")  # 小于60秒

# 使用默认TTL
db_with_ttl = FlaxKV("default_ttl_db", "./data", default_ttl=30)

# 添加的所有键值对自动设置30秒TTL
db_with_ttl["key1"] = "value1"
db_with_ttl["key2"] = "value2"

print(f"key1 TTL: {db_with_ttl.get_ttl('key1')}秒")  # 约30秒
print(f"key2 TTL: {db_with_ttl.get_ttl('key2')}秒")  # 约30秒

# 更新默认TTL
db_with_ttl.set_default_ttl(10)
db_with_ttl["key3"] = "value3"  # 使用新的默认TTL

# 禁用默认TTL
db_with_ttl.set_default_ttl(None)
db_with_ttl["key4"] = "value4"  # 不应用TTL

print(f"key3 TTL: {db_with_ttl.get_ttl('key3')}秒")  # 约10秒
print(f"key4 TTL: {db_with_ttl.get_ttl('key4')}")    # None
```

### 5.6 默认TTL与批量操作

```python
from flaxkv2 import FlaxKV

# 创建带有默认TTL的数据库
db = FlaxKV("batch_ttl_db", "./data", default_ttl=60)

# 批量添加数据
batch_data = {
    "batch_key1": "value1",
    "batch_key2": "value2",
    "batch_key3": "value3"
}

# 使用update方法批量添加，默认TTL自动应用于所有新键
db.update(batch_data)

# 验证所有键都应用了默认TTL
for key in batch_data:
    ttl = db.get_ttl(key)
    print(f"{key} TTL: {ttl}秒")  # 所有键的TTL应接近60秒
```

## 6. 性能优化

FlaxKV2 采用了多种性能优化策略：

1. **缓冲写入**：减少磁盘I/O频率
2. **批量操作**：利用LevelDB的批量API提高吞吐量
3. **分层缓存**：优化数据访问模式，减少磁盘读取
4. **布隆过滤器**：减少不必要的磁盘查询
5. **并发控制**：细粒度锁减少线程竞争
6. **序列化优化**：针对不同数据类型优化编码
7. **异步提交**：非阻塞设计提高响应速度

## 7. 分布式功能

FlaxKV2 的分布式功能包括：

1. **远程访问**：通过HTTP协议访问远程数据库
2. **批量操作**：减少网络往返
3. **错误重试**：提高分布式环境下的可靠性
4. **健康检查**：监控服务器状态

## 8. 日志系统

FlaxKV2 采用了全面的日志系统：

1. **彩色日志**：使用loguru提供彩色格式化日志
2. **可配置级别**：通过环境变量`FLAXKV_LOG_LEVEL`设置日志级别
3. **详细信息**：记录关键操作如数据库打开、关闭、修改
4. **文件日志**：支持将日志写入文件并进行轮转

## 9. 自动关闭机制

FlaxKV2 实现了健壮的自动关闭机制：

1. **程序退出关闭**：使用atexit注册退出处理器
2. **上下文管理**：支持with语句自动关闭
3. **异常处理**：在出现异常时确保数据库正确关闭
4. **显式关闭**：提供close()方法手动关闭

## 10. 未来发展计划

FlaxKV2 计划在以下方向继续发展：

1. **增强分布式能力**：
   - 数据分片
   - 主从复制
   - 一致性哈希
   - 集群管理

2. **性能进一步优化**：
   - 更高效的缓存策略
   - 并行查询处理
   - 压缩算法改进

3. **功能扩展**：
   - 事务支持
   - 复杂查询语言
   - 更多数据类型支持

4. **生态系统**：
   - 监控工具
   - 管理界面
   - 备份工具

## 11. 限制和注意事项

使用FlaxKV2时需要注意：

1. **多进程限制**：由于延迟写入特性，多进程环境下一个进程可能无法立即读取另一进程的写入
2. **类型转换**：某些复杂类型可能会在序列化过程中转换，如Tuple会转为List
3. **内存消耗**：缓冲区和缓存会消耗内存，需根据实际情况调整大小
4. **磁盘空间**：LevelDB不会自动压缩文件，长期使用需要监控磁盘空间

## 12. 总结

FlaxKV2是一个功能强大、性能优异的键值存储系统，提供了类似Python字典的简单接口和丰富的高级功能。它适用于需要高性能数据存储的各种场景，特别是机器学习和数据处理应用。通过缓冲写入、自动关闭、上下文管理等特性，确保了数据的安全性和系统的稳定性。 
