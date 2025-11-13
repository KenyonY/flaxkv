# FlaxKV2

<div align="center">

**高性能、易用的 Python 键值存储库**

基于 LevelDB | 线程安全 | 支持远程访问 | 丰富的数据类型

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[快速开始](#快速开始) • [安装](#安装) • [文档](#文档) • [示例](examples/)

</div>

---

## ✨ 特性亮点

FlaxKV2 是一个提供 **类字典接口** 的持久化键值存储库，将 LevelDB 的高性能与 Python 的易用性完美结合。

### 核心特性

- 🚀 **极致性能**：基于 LevelDB，提供 6 种预设性能配置，适应不同场景
- 🎯 **简单易用**：Python dict 风格的 API，上手即用
- 🔒 **线程安全**：内置线程安全支持，无需担心并发问题
- 📦 **丰富类型**：原生支持字符串、数字、列表、字典、NumPy 数组、Pandas DataFrame
- 🌐 **远程访问**：基于 ZeroMQ 的客户端/服务器架构，支持网络访问
- ⏰ **TTL 支持**：键自动过期功能，自动清理过期数据
- 🪆 **嵌套存储**：高效的嵌套字典/列表存储，避免频繁序列化
- 🔧 **灵活配置**：从内存受限到大数据库，多种配置文件可选
- 📝 **日志控制**：作为基础库默认静默，需要时可灵活启用
- 🛡️ **自动管理**：自动关闭、上下文管理器支持
- 📊 **可视化工具**：内置 Inspector 工具，提供 CLI 和 Web UI 两种界面，轻松管理和分析数据

## 📦 安装

### 基础安装

```bash
pip install flaxkv2
```

### 完整安装（推荐）

```bash
# 包含 Pandas、NumPy 和 Web UI 等所有可选依赖
pip install flaxkv2[full]

# 或按需安装特定功能
pip install flaxkv2[pandas]  # Pandas 支持
pip install flaxkv2[web]     # Web UI 可视化工具
```

### 从源码安装

```bash
git clone https://github.com/yourusername/flaxkv.git
cd flaxkv
pip install -e .
```

## 🚀 快速开始

### 基础用法（30 秒上手）

```python
from flaxkv2 import FlaxKV

# 创建/打开数据库
db = FlaxKV("my_database", "./data")

# 像使用字典一样使用
db["username"] = "alice"
db["user_data"] = {"age": 30, "city": "Beijing"}
db["scores"] = [95, 87, 92, 88]

# 读取数据
print(db["username"])        # "alice"
print(db["user_data"]["age"]) # 30

# 检查键是否存在
if "username" in db:
    print("User exists!")

# 遍历所有数据
for key, value in db.items():
    print(f"{key}: {value}")

# 删除数据
del db["username"]

# 关闭数据库
db.close()
```

### 使用上下文管理器（推荐）

```python
from flaxkv2 import FlaxKV

# 自动管理数据库生命周期
with FlaxKV("my_database", "./data") as db:
    db["key"] = "value"
    print(db["key"])
# 离开 with 块时自动关闭
```

---

## 🔥 核心功能

### 1. 性能配置文件

FlaxKV2 提供 **6 种预设配置**，针对不同场景优化：

```python
from flaxkv2 import FlaxKV

# 通用平衡配置（默认）
db = FlaxKV("mydb", "./data")  # 或显式指定：performance_profile='balanced'

# 读密集型优化（缓存服务、API查询）
db = FlaxKV("mydb", "./data", performance_profile='read_optimized')

# 写密集型优化（日志收集、批量导入）
db = FlaxKV("mydb", "./data", performance_profile='write_optimized')

# 内存受限环境（嵌入式设备、容器）
db = FlaxKV("mydb", "./data", performance_profile='memory_constrained')

# 大数据库（>100GB）
db = FlaxKV("mydb", "./data", performance_profile='large_database')

# 机器学习工作负载（存储 NumPy 数组、模型参数）
db = FlaxKV("mydb", "./data", performance_profile='ml_workload')
```

**自定义配置**：

```python
# 基于预设配置，覆盖特定参数
db = FlaxKV("mydb", "./data",
            performance_profile='balanced',
            lru_cache_size=512*1024*1024,  # 512MB 缓存
            write_buffer_size=256*1024*1024)  # 256MB 写缓冲
```

查看所有配置：

```python
from flaxkv2.config import PerformanceProfiles
print(PerformanceProfiles.list_profiles())
```

### 2. TTL（键过期）

```python
from flaxkv2 import FlaxKV

db = FlaxKV("mydb", "./data")

# 设置键值
db["session_token"] = "abc123"

# 设置 10 秒后过期
db.set_ttl("session_token", 10)

# 获取剩余时间
ttl = db.get_ttl("session_token")
print(f"剩余 {ttl} 秒")

# 10 秒后，键自动删除
# db["session_token"]  # KeyError

# 设置默认 TTL（所有新键都会应用）
db = FlaxKV("cache_db", "./data", default_ttl=300)  # 5分钟
db["key"] = "value"  # 自动 5 分钟后过期
```

### 3. 嵌套存储（高性能字典/列表）

```python
from flaxkv2 import FlaxKV

# 启用自动嵌套模式
db = FlaxKV("mydb", "./data", auto_nested=True)

# 嵌套字典
db["config"] = {
    "database": {
        "host": "localhost",
        "port": 5432,
        "credentials": {
            "user": "admin",
            "password": "secret"
        }
    }
}

# 直接访问嵌套键，避免整体反序列化
config = db["config"]
print(config["database"]["host"])  # "localhost"

# 修改嵌套值（高效，不需要读取整个对象）
config["database"]["port"] = 3306

# 嵌套列表
db["users"] = ["alice", "bob", "charlie"]
users = db["users"]
users.append("david")  # 直接操作，高效持久化
print(len(users))  # 4
```

### 4. 丰富的数据类型支持

```python
from flaxkv2 import FlaxKV
import numpy as np
import pandas as pd

db = FlaxKV("mydb", "./data")

# 基本类型
db["string"] = "hello"
db["integer"] = 42
db["float"] = 3.14
db["boolean"] = True

# 容器类型
db["list"] = [1, 2, 3, 4, 5]
db["dict"] = {"name": "Alice", "age": 30}
db["tuple"] = (1, 2, 3)
db["set"] = {1, 2, 3}

# NumPy 数组
db["array"] = np.array([[1, 2], [3, 4]])
db["matrix"] = np.random.randn(100, 100)

# Pandas DataFrame（需要安装 pandas）
db["dataframe"] = pd.DataFrame({
    "name": ["Alice", "Bob"],
    "age": [30, 25]
})

# 自定义对象（通过 pickle）
class MyClass:
    def __init__(self, value):
        self.value = value

db["custom"] = MyClass(42)
```

### 5. 远程数据库（ZeroMQ）

FlaxKV2 支持通过网络访问数据库，采用客户端/服务器架构。

#### 启动服务器

```bash
# 命令行启动
flaxkv2 run --host 0.0.0.0 --port 5555 --data-dir ./data

# 或通过 Python 启动
python -m flaxkv2 run --host 0.0.0.0 --port 5555 --data-dir ./data
```

服务器选项：
- `--host`: 监听地址（默认 `0.0.0.0`）
- `--port`: 监听端口（默认 `5555`）
- `--data-dir`: 数据存储目录（默认 `./data`）

#### 客户端连接

```python
from flaxkv2 import FlaxKV

# 方式1：显式指定 backend='remote'（推荐）
db = FlaxKV("remote_db", "127.0.0.1:5555", backend='remote')

# 方式2：使用 tcp:// 前缀自动识别
db = FlaxKV("remote_db", "tcp://127.0.0.1:5555")

# 使用方式与本地数据库完全相同
db["key"] = "value"
print(db["key"])  # "value"

# 配置超时和重试
db = FlaxKV("remote_db", "127.0.0.1:5555",
            backend='remote',
            timeout=5000,      # 5秒超时
            max_retries=3,     # 最多重试3次
            retry_delay=0.1)   # 重试间隔100ms
```

**使用场景**：
- 🔹 多进程共享数据库
- 🔹 微服务架构中的中央缓存
- 🔹 分布式机器学习参数存储

### 6. Inspector 可视化工具

FlaxKV2 内置强大的数据可视化和管理工具，提供 **CLI** 和 **Web UI** 两种方式。

#### CLI 工具

```bash
# 查看所有键
flaxkv2 inspect keys mydb --path /data

# 查看键详情
flaxkv2 inspect get mydb user123 --path /data

# 统计分析
flaxkv2 inspect stats mydb --path /data

# 搜索键（支持正则表达式）
flaxkv2 inspect search mydb "user_.*" --path /data

# 删除键
flaxkv2 inspect delete mydb temp_key --path /data

# 设置键值
flaxkv2 inspect set mydb name "John" --path /data
```

#### Web UI

启动 Web 界面进行可视化管理：

```bash
# 启动 Web UI（需要先安装: pip install flaxkv2[web]）
flaxkv2 web mydb --path /data --port 8080

# 然后访问 http://127.0.0.1:8080
```

Web UI 提供：
- 📂 **数据浏览**: 分页显示所有键值，搜索过滤
- 📊 **统计分析**: 类型分布、大小分布、TTL 状态可视化
- 🛠️ **数据管理**: 在线增删改查，支持 TTL 设置

详见 [Inspector 文档](docs/INSPECTOR.md)

### 7. 日志配置（作为基础库使用）

FlaxKV2 作为基础库，**默认不输出任何日志**，不会污染应用程序的终端。

```python
from flaxkv2 import FlaxKV

# 默认完全静默
db = FlaxKV("mydb", "./data")
db["key"] = "value"  # 没有任何日志输出

# 需要调试时，手动启用日志
from flaxkv2.utils.log import enable_logging
enable_logging(level="INFO")  # 或 "DEBUG", "WARNING", "ERROR"

# 使用完后可以禁用
from flaxkv2.utils.log import disable_logging
disable_logging()
```

**环境变量方式**：

```bash
# 启用日志
export FLAXKV_ENABLE_LOGGING=1
export FLAXKV_LOG_LEVEL=DEBUG

python your_script.py
```

详见 [日志配置文档](docs/LOGGING.md)

---

## 📚 API 参考

### FlaxKV 类

```python
FlaxKV(
    db_name: str,                          # 数据库名称
    root_path_or_url: str = ".",          # 本地路径或远程 URL
    backend: str = None,                   # 'local' 或 'remote'
    performance_profile: str = 'balanced', # 性能配置文件
    auto_nested: bool = False,             # 启用嵌套存储
    rebuild: bool = False,                 # 重建数据库
    raw: bool = False,                     # 原始模式（不序列化）
    default_ttl: int = None,               # 默认 TTL（秒）
    # LevelDB 参数（覆盖 profile）
    lru_cache_size: int = None,
    write_buffer_size: int = None,
    bloom_filter_bits: int = None,
    block_size: int = None,
    max_open_files: int = None,
    # 远程连接参数
    timeout: int = 5000,                   # 超时（毫秒）
    max_retries: int = 3,                  # 最大重试次数
    retry_delay: float = 0.1,              # 重试延迟（秒）
)
```

### 字典操作

```python
# 读写
db[key] = value              # 写入
value = db[key]              # 读取
value = db.get(key, default) # 安全读取
del db[key]                  # 删除

# 批量操作
db.update({k1: v1, k2: v2})  # 批量写入
db.update_many([(k1, v1), (k2, v2)])  # 批量写入（列表）

# 查询
key in db                    # 检查存在
len(db)                      # 键数量
db.keys()                    # 所有键
db.values()                  # 所有值
db.items()                   # 所有键值对

# 清空
db.clear()                   # 删除所有数据
```

### TTL 操作

```python
db.set_ttl(key, ttl_seconds)      # 设置 TTL
remaining = db.get_ttl(key)       # 获取剩余时间
db.remove_ttl(key)                # 移除 TTL
db.cleanup_expired()              # 手动清理过期键
```

### 数据库管理

```python
db.close()                   # 关闭数据库
db.sync()                    # 强制同步到磁盘
db.rebuild()                 # 重建数据库

# 上下文管理器
with FlaxKV("mydb", "./data") as db:
    db["key"] = "value"
```

---

## ⚠️ 安全注意事项

### 1. Pickle 序列化风险

FlaxKV2 对复杂对象使用 pickle 序列化。**pickle 存在安全风险**：

```python
# ⚠️ 危险：不要从不可信来源加载数据
# pickle 可以执行任意代码！

# ✅ 安全：只在可信环境中使用
db["trusted_data"] = my_custom_object

# ✅ 推荐：生产环境只存储简单类型
db["config"] = {"host": "localhost", "port": 8080}
db["users"] = ["alice", "bob", "charlie"]
```

**最佳实践**：
- ✅ 仅在可信环境中使用
- ✅ 生产环境优先使用 JSON、msgpack 等安全格式
- ✅ 对不可信数据进行验证

### 2. 远程连接安全

使用远程数据库时的安全考虑：

| 风险 | 说明 | 缓解措施 |
|------|------|----------|
| **无加密** | 数据明文传输 | 使用 VPN 或 SSH 隧道 |
| **无认证** | 任何人都可以连接 | 使用防火墙限制访问 |
| **DoS 攻击** | 恶意客户端可能耗尽资源 | 限制连接数、使用反向代理 |

**生产环境推荐配置**：

```bash
# 仅监听本地回环接口
flaxkv2 run --host 127.0.0.1 --port 5555 --data-dir ./data

# 通过 SSH 隧道安全访问
ssh -L 5555:localhost:5555 user@remote-server

# 或使用防火墙规则限制访问
sudo ufw allow from 192.168.1.0/24 to any port 5555
```

---

## 📊 性能提示

### 1. 选择合适的配置文件

```python
# 读多写少（90%+ 读操作）
db = FlaxKV("cache", "./data", performance_profile='read_optimized')

# 写多读少（70%+ 写操作）
db = FlaxKV("logs", "./data", performance_profile='write_optimized')

# 大数据库（>100GB）
db = FlaxKV("bigdata", "./data", performance_profile='large_database')
```

### 2. 批量操作

```python
# ❌ 慢：逐个写入
for i in range(10000):
    db[f"key{i}"] = f"value{i}"

# ✅ 快：批量写入
db.update({f"key{i}": f"value{i}" for i in range(10000)})
```

### 3. 使用嵌套存储

```python
# ❌ 慢：每次修改都需要完整序列化
config = db["config"]  # 完整反序列化
config["port"] = 8080
db["config"] = config  # 完整序列化

# ✅ 快：只修改变化的部分
db = FlaxKV("mydb", "./data", auto_nested=True)
db["config"]["port"] = 8080  # 只序列化修改的值
```

### 4. TTL 自动清理

```python
# TTL 清理是异步的，默认 60 秒一次
# 可以手动触发立即清理
db.cleanup_expired()
```

---

## 🔗 文档

- [日志配置指南](docs/LOGGING.md)
- [性能配置详解](docs/LEVELDB_CONFIGURATION_GUIDE.md)
- [嵌套存储使用](examples/auto_nested_improvements_demo.py)
- [性能配置示例](examples/performance_config_example.py)
- [日志使用示例](examples/logging_example.py)

---

## 🛠️ 依赖

### 核心依赖

- **Python** >= 3.8
- **plyvel** - LevelDB Python 绑定
- **msgpack** - 高效序列化
- **numpy** - 数组支持和布隆过滤器
- **loguru** - 结构化日志
- **pyzmq** - ZeroMQ 网络通信

### 可选依赖

- **pandas** - DataFrame 支持（`pip install flaxkv2[pandas]`）

---

## 🤝 贡献

欢迎贡献代码、报告问题或提出建议！

```bash
# Fork 项目
git clone https://github.com/yourusername/flaxkv.git
cd flaxkv

# 安装开发依赖
pip install -e .[dev]

# 运行测试
pytest tests/

# 提交 Pull Request
```

---

## 📄 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件

---

## 🌟 致谢

- 基于强大的 [LevelDB](https://github.com/google/leveldb)
- 使用 [plyvel](https://github.com/wbolster/plyvel) Python 绑定
- 网络通信基于 [ZeroMQ](https://zeromq.org/)

---

<div align="center">

**如果觉得有用，请给个 ⭐️ Star！**

[报告问题](https://github.com/yourusername/flaxkv/issues) • [功能建议](https://github.com/yourusername/flaxkv/issues)

</div> 