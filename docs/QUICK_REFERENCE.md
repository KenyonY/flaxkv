# FlaxKV2 快速参考

## 基本操作

```python
from flaxkv2 import FlaxKV

# 创建数据库
db = FlaxKV("mydb", "./data")

# 写入
db["key"] = "value"

# 读取
value = db["key"]

# 删除
del db["key"]

# 检查
if "key" in db:
    pass

# 批量
db.update({"k1": "v1", "k2": "v2"})

# 关闭
db.close()
```

## 性能配置

```python
# 6 种预设配置
profiles = [
    'balanced',           # 默认，通用平衡
    'read_optimized',     # 读密集型
    'write_optimized',    # 写密集型
    'memory_constrained', # 内存受限
    'large_database',     # 大数据库
    'ml_workload'         # ML工作负载
]

db = FlaxKV("mydb", "./data", performance_profile='read_optimized')
```

## TTL（键过期）

```python
# 设置 TTL
db["token"] = "abc123"
db.set_ttl("token", 300)  # 5分钟后过期

# 获取剩余时间
ttl = db.get_ttl("token")

# 移除 TTL
db.remove_ttl("token")

# 默认 TTL
db = FlaxKV("cache", "./data", default_ttl=600)
```

## 嵌套存储

```python
# 启用嵌套
db = FlaxKV("mydb", "./data", auto_nested=True)

# 嵌套字典
db["config"] = {
    "server": {
        "host": "localhost",
        "port": 8080
    }
}

config = db["config"]
config["server"]["port"] = 9000  # 高效更新

# 嵌套列表
db["users"] = ["alice", "bob"]
users = db["users"]
users.append("charlie")  # 直接追加
```

## 远程数据库

```bash
# 启动服务器
flaxkv2 run --host 0.0.0.0 --port 5555 --data-dir ./data
```

```python
# 连接远程
db = FlaxKV("remote", "127.0.0.1:5555", backend='remote')
db["key"] = "value"
```

## 日志控制

```python
# 启用日志
from flaxkv2.utils.log import enable_logging
enable_logging(level="INFO")

# 禁用日志
from flaxkv2.utils.log import disable_logging
disable_logging()
```

```bash
# 环境变量
export FLAXKV_ENABLE_LOGGING=1
export FLAXKV_LOG_LEVEL=DEBUG
```

## 上下文管理器

```python
with FlaxKV("mydb", "./data") as db:
    db["key"] = "value"
    # 自动关闭
```

## 数据类型

```python
# 基本类型
db["str"] = "hello"
db["int"] = 42
db["float"] = 3.14
db["bool"] = True

# 容器
db["list"] = [1, 2, 3]
db["dict"] = {"a": 1}
db["tuple"] = (1, 2)
db["set"] = {1, 2, 3}

# NumPy
import numpy as np
db["array"] = np.array([1, 2, 3])

# Pandas
import pandas as pd
db["df"] = pd.DataFrame({"col": [1, 2]})
```

## 自定义配置

```python
db = FlaxKV(
    "mydb",
    "./data",
    performance_profile='balanced',
    lru_cache_size=512*1024*1024,      # 512MB
    write_buffer_size=256*1024*1024,   # 256MB
    bloom_filter_bits=12,
    block_size=32*1024,
    max_open_files=1000
)
```

## 查看配置

```python
from flaxkv2.config import PerformanceProfiles

# 列出所有配置
print(PerformanceProfiles.list_profiles())

# 查看特定配置
config = PerformanceProfiles.get_profile('read_optimized')
print(config)
```

## 数据库管理

```python
# 同步到磁盘
db.sync()

# 重建数据库
db.rebuild()

# 清空所有数据
db.clear()

# 统计
print(len(db))           # 键数量
print(list(db.keys()))   # 所有键
print(list(db.values())) # 所有值
print(list(db.items()))  # 所有键值对
```

## 错误处理

```python
# KeyError
try:
    value = db["nonexistent"]
except KeyError:
    print("Key not found")

# 使用 get() 避免异常
value = db.get("key", default="default_value")
```

## 性能技巧

```python
# ✅ 批量操作
db.update({f"k{i}": i for i in range(10000)})

# ❌ 逐个操作（慢）
for i in range(10000):
    db[f"k{i}"] = i

# ✅ 使用嵌套存储
db = FlaxKV("mydb", "./data", auto_nested=True)
db["config"]["port"] = 8080  # 只更新变化部分

# ❌ 完整序列化（慢）
config = db["config"]
config["port"] = 8080
db["config"] = config
```

## 安全最佳实践

```python
# ✅ 只存储简单类型（生产环境）
db["config"] = {"host": "localhost", "port": 8080}

# ⚠️ 谨慎使用 pickle（开发环境）
db["custom_object"] = MyCustomClass()

# ✅ 远程连接使用 SSH 隧道
# ssh -L 5555:localhost:5555 user@server
db = FlaxKV("remote", "127.0.0.1:5555", backend='remote')
```
