# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

FlaxKV2 是一个高性能的 Python 键值存储库，基于 LevelDB，提供类字典接口。核心特性包括：
- 🚀 本地和远程（ZeroMQ）两种后端
- 🎯 智能缓存系统（读缓存 + 写缓冲）
- 📦 支持丰富的数据类型（NumPy、Pandas、嵌套字典/列表）
- ⏰ TTL 自动过期功能
- 🔒 线程安全

**重要架构变化**: 项目已从 `LevelDBDict` 迁移到 `RawLevelDBDict` 和 `CachedLevelDBDict`，新架构提供 13-25% 性能提升。

## 开发命令

### 测试
```bash
# 运行所有测试
pytest -v -s

# 运行特定测试文件
pytest tests/unit/test_core.py -v

# 并行运行测试（需要 pytest-xdist）
pytest -n auto

# 运行测试并生成覆盖率报告
pytest --cov=flaxkv2 --cov-report=html

# 运行单个测试
pytest tests/unit/test_core.py::test_basic_operations -v
```

### 代码格式化
```bash
# 格式化代码（black）
black flaxkv2 tests

# 排序导入（isort，使用 black profile）
isort flaxkv2 tests --profile black

# 运行所有 pre-commit 钩子
pre-commit run --all-files

# 基础语法检查（flake8）
flake8 flaxkv2 --count --select=E9,F63,F7,F82 --show-source --statistics
```

### 启动服务器
```bash
# 启动本地服务器（仅本地访问）
flaxkv2 run --host 127.0.0.1 --port 5555 --data-dir ./data

# 启动加密服务器（推荐生产环境）
flaxkv2 run --host 0.0.0.0 --port 5555 --data-dir ./data --enable-encryption --password your_password

# 启动加密+压缩服务器
flaxkv2 run --host 0.0.0.0 --port 5555 --data-dir ./data --enable-encryption --password your_password --enable-compression

# 自定义配置启动
flaxkv2 run --host 127.0.0.1 --port 5555 --data-dir ./data --log-level DEBUG
```

### Inspector 可视化工具
```bash
# 查看所有键
flaxkv2 inspect keys mydb --path ./data

# 查看键详情
flaxkv2 inspect get mydb user123 --path ./data

# 统计分析
flaxkv2 inspect stats mydb --path ./data

# 启动 Web UI（需要安装 flask: pip install flaxkv2[web]）
flaxkv2 web mydb --path ./data --port 8080
```

### 实用工具命令
```bash
# 根据端口号 kill 进程（用于清理僵尸服务器）
flaxkv2 kill 5555           # kill 单个端口
flaxkv2 kill 5555 8080 3000 # kill 多个端口

# 查看版本
flaxkv2 version

# 生成示例配置文件
flaxkv2 config init
```

### Docker
```bash
# 构建镜像
make build

# 启动容器
make start

# 查看日志
make log

# 进入容器
make exec

# 删除容器
make rm
```

### 包管理
```bash
# 安装开发环境（可编辑模式）
pip install -e .

# 安装完整功能（包括 Pandas 和 Web UI）
pip install -e .[full]

# 安装测试依赖
pip install -e .[test]

# 构建包
python -m build
```

### 性能分析
```bash
# 一键性能分析（文件传输）
./quick_profile.sh

# 详细性能分析（需要先启动服务器）
python profile_file_transfer.py           # 对比所有传输方式
python profile_file_transfer_detailed.py  # 详细阶段分析

# 查看分析报告
cat profile_results/summary_report.md     # 汇总报告
cat profile_results/detailed_analysis.txt # 详细分析
```

详见 `README_PROFILING.md` 和 `PROFILE_GUIDE.md`

### CLI 命令别名

FlaxKV2 提供了多个命令别名，以下命令等价：
```bash
flaxkv2 run --port 5555
fkv run --port 5555      # 短别名
flx run --port 5555      # 更短别名
```

### 配置文件

FlaxKV2 支持配置文件（TOML格式），位置：`~/.flaxkv2/config.toml`

```bash
# 生成示例配置文件
flaxkv2 config init

# 使用配置文件中的 profile
flaxkv2 run --profile production

# 配置文件中可以定义服务器别名，使用 @ 前缀引用
# 例如配置了 @prod 服务器，可以这样使用：
flaxkv2 get @prod mydb mykey

# 命令行参数优先级高于配置文件
flaxkv2 run --profile production --port 8888  # 覆盖 profile 中的端口
```

详见 `docs/CONFIG_FILE_GUIDE.md`

## 核心架构

### 后端层次结构

```
FlaxKV (工厂类)
├── 本地后端
│   ├── RawLevelDBDict (无缓存，简单可靠)
│   └── CachedLevelDBDict (智能缓存，极致性能)
└── 远程后端
    ├── RemoteDBDict (同步包装器，向后兼容)
    │   └── AsyncRemoteDBDict (异步核心实现)
    └── FlaxKVServer (ZeroMQ 服务器)
```

**重要架构变化（2025-01）：异步重构**
- 远程客户端现在基于 asyncio，核心实现是 `AsyncRemoteDBDict`
- `RemoteDBDict` 变为薄薄的同步包装器（内部调用异步核心）
- 只维护一套核心代码（异步），减少维护成本
- 性能提升：~1.5-1.6x（异步并发 vs 同步顺序）
- 向后兼容：所有现有代码无需修改

### 关键组件位置

**核心实现**:
- `flaxkv2/__init__.py` - FlaxKV 工厂类，智能后端选择
- `flaxkv2/core/raw_leveldb_dict.py` - 无缓存本地后端（默认）
- `flaxkv2/core/cached_leveldb_dict.py` - 缓存本地后端（高性能）
- `flaxkv2/client/async_zmq_client.py` - 异步远程客户端（核心实现）
- `flaxkv2/client/zmq_client.py` - 同步远程客户端（包装器，向后兼容）
- `flaxkv2/server/zmq_server.py` - 远程服务器 (FlaxKVServer)

**支撑模块**:
- `flaxkv2/serialization/` - 编码器/解码器（msgpack, pickle, NumPy, Pandas）
- `flaxkv2/core/nested_structures.py` - 嵌套字典/列表实现
- `flaxkv2/utils/ttl_cleanup.py` - TTL 自动清理
- `flaxkv2/instance_manager.py` - 数据库实例缓存
- `flaxkv2/auto_close.py` - 程序退出时自动清理
- `flaxkv2/config.py` - 性能配置文件

**CLI 和工具**:
- `flaxkv2/cli.py` - 命令行接口（基于 Fire）
- `flaxkv2/inspector/` - 数据可视化和管理工具

### 后端选择逻辑

FlaxKV 根据参数自动选择最优后端：

1. **检测 URL 类型**:
   - `tcp://...` → RemoteDBDict (远程后端)
   - 其他 → 本地后端

2. **检测缓存参数**（本地后端）:
   - 无 `read_cache_size`/`write_buffer_size` → RawLevelDBDict（无缓存）
   - 有缓存参数 → CachedLevelDBDict（智能缓存）

### 数据流

**本地写入**:
```
db["key"] = value
→ Encoder.encode(value)
→ TTLManager (如果有 default_ttl)
→ 缓存层 (如果启用)
→ plyvel.DB.put()
→ LevelDB
```

**本地读取**:
```
value = db["key"]
→ 缓存层查找 (如果启用)
→ TTLManager.is_expired()
→ plyvel.DB.get()
→ Decoder.decode()
→ Python 对象
```

**远程通信**:
```
客户端: Encoder.encode() → ZeroMQ [CMD, db_name, args...] → 服务器
服务器: RawLevelDBDict 操作 → ZeroMQ [STATUS, data] → 客户端
```

## 关键技术细节

### 缓存系统

CachedLevelDBDict 提供两层缓存：

1. **读缓存**（LRU）:
   - 默认 10000 条目
   - 热数据读取性能提升 ~10-13x

2. **写缓冲**（批量写入优化）:
   - 默认 500 条目或 30 秒刷新
   - 支持异步（async_flush=True, 极致性能）和同步（async_flush=False, 更安全）两种模式

### 序列化策略

1. **msgpack** - 基本类型（最快）
2. **二进制序列化** - NumPy 数组（保留 dtype/shape）
3. **二进制格式** - Pandas DataFrame（可选依赖）
4. **pickle** - 复杂对象（⚠️ 安全风险，仅用于可信环境）

**类型缓存优化**（P1）:
- Encoder 缓存每个 Python 类型的最佳序列化方法
- 避免重复的 try-except 开销
- 99%+ 缓存命中率，15-30% 性能提升

### TTL 实现

- TTL 元数据以 `__ttl__:<原始键>` 前缀存储在 LevelDB
- 值为过期时间戳（float）
- 服务端验证减少 ~50% 网络请求
- 后台线程自动清理（默认 60 秒间隔）

### 嵌套字典/列表

- 基于 LevelDB 的 `prefixed_db()` 功能
- 键格式: `<prefix>:<field>`
- 每个字段独立序列化（避免整个对象序列化）
- 递归嵌套支持
- 启用方式: `auto_nested=True` 或 `db.nested(prefix)`

### 性能配置文件

6 种预设配置（在 `flaxkv2/config.py` 中定义）：

- `balanced` - 通用平衡（默认）
- `read_optimized` - 读密集型（512MB 缓存）
- `write_optimized` - 写密集型（256MB 写缓冲）
- `memory_constrained` - 内存受限（64MB 缓存）
- `large_database` - 大数据库 >100GB（1GB 缓存）
- `ml_workload` - 机器学习（512MB 缓存，64KB 块）

使用方式:
```python
db = FlaxKV("mydb", "./data", performance_profile='read_optimized')
```

### 远程协议（ZeroMQ）

- 客户端: REQ socket，服务器: ROUTER socket
- 消息格式: `[command_type, db_name, ...args]`
- 响应格式: `[status_code, data]`
- 支持命令: CONNECT, GET, SET, DELETE, KEYS, VALUES, ITEMS, UPDATE, PING, FILE_CHUNK, FILE_META
- 服务端仅处理二进制数据，客户端负责所有序列化
- 支持 CurveZMQ 加密（基于 NaCl/Curve25519）
- 支持 LZ4 压缩（降低网络带宽）
- 支持密码认证（两种方案：确定性派生密钥 vs 文件存储密钥）

## 测试结构

```
tests/
├── conftest.py                          # 共享 fixtures
├── unit/                                # 单元测试
│   ├── test_core.py                    # RawLevelDBDict 核心功能
│   ├── test_cached_*.py                # CachedLevelDBDict 测试
│   ├── test_unified_cache.py           # 统一缓存系统测试
│   ├── test_*_ttl.py                   # TTL 相关测试
│   ├── test_nested_list.py             # 嵌套列表测试
│   ├── test_special_values.py          # 边界情况和特殊值
│   ├── test_config_loader.py           # 配置文件加载测试
│   ├── test_inspector.py               # Inspector 工具测试
│   ├── test_instance_manager.py        # 实例管理器测试
│   └── test_auto_close.py              # 自动关闭测试
├── integration/                         # 集成测试
│   ├── test_zmq_remote.py              # 远程 ZeroMQ 后端
│   ├── test_encryption_compression.py  # 加密和压缩功能
│   ├── test_password_auth.py           # 密码认证
│   ├── test_derive_password.py         # 密码派生密钥
│   ├── test_nested_dict.py             # 嵌套字典
│   ├── test_auto_nested.py             # 自动嵌套功能
│   ├── test_remote_inspector.py        # 远程 Inspector
│   └── test_server_ttl.py              # 服务器端 TTL
├── stress/                              # 压力测试
│   └── test_concurrency.py             # 并发测试
└── benchmarks/                          # 性能基准测试
    ├── benchmark_cache.py              # 缓存性能
    ├── benchmark_nested.py             # 嵌套结构性能
    └── benchmark_remotedb.py           # 远程后端性能
```

运行远程测试需要测试服务器（在 fixtures 中自动启动/关闭）。

## 代码风格

- **格式化**: black (24.3.0)
- **导入排序**: isort (black profile)
- **行长度**: 127 字符
- **Pre-commit hooks**: 自动格式化

## 性能基准

### 基准性能（SSD）

**无缓存** (RawLevelDBDict):
- 热数据读取: 107K ops/s
- 写入: 1649 ops/s

**智能缓存** (CachedLevelDBDict):
- 只读缓存: 1064K ops/s (9.9x)
- 同步写缓冲: 926K ops/s (8.6x)
- 异步写缓冲: 1434K ops/s (13.4x)

### 近期优化（2025-10）

✅ **P0: LevelDB 配置优化**
- 启用 256MB LRU 缓存（默认 8MB）
- 添加布隆过滤器（10 bits/key）
- 优化块大小（16KB）
- 结果: 4-25% 性能提升

✅ **P1: 智能类型缓存**
- 缓存每个类型的最佳编码器
- 99%+ 缓存命中率
- 结果: 14-30% 编码性能提升

**综合影响**: 典型工作负载 ~20-50% 性能提升

## 安全注意事项

⚠️ **Pickle 序列化风险**:
- FlaxKV2 对复杂对象使用 pickle，可能执行任意代码
- 仅在可信环境中使用
- 生产环境建议仅存储简单数据类型

⚠️ **远程连接安全**:
- 支持 CurveZMQ 加密和密码认证（推荐启用）
- 支持 LZ4 压缩传输
- 默认无加密，建议生产环境启用：`enable_encryption=True, password='your_password'`
- 未加密时仅在可信网络使用，或通过防火墙、VPN、SSH 隧道保护

## 文档参考

项目文档位于 `docs/` 目录：

- `docs/design/ARCHITECTURE.md` - 详细架构设计
- `docs/design/CORE_DESIGN.md` - 核心设计文档
- `docs/LOGGING.md` - 日志配置指南
- `docs/INSPECTOR.md` - Inspector 工具使用
- `docs/CONFIG_FILE_GUIDE.md` - 配置文件指南
- `docs/LARGE_FILE_TRANSFER.md` - 大文件传输指南
- `docs/development/PASSWORD_AUTH_GUIDE.md` - 密码认证指南
- `docs/README.md` - 文档目录树

## 开发最佳实践

1. **使用上下文管理器**:
   ```python
   # 同步API（推荐用于简单脚本）
   with FlaxKV("mydb", "./data") as db:
       db["key"] = "value"
   # 自动关闭，确保缓冲区刷新
   ```

2. **使用异步API（推荐用于高性能/并发场景）**:
   ```python
   import asyncio
   from flaxkv2.client.async_zmq_client import AsyncRemoteDBDict
   from flaxkv2.utils.async_file_transfer import upload_large_file_async

   async def main():
       # 异步客户端（并发性能更高）
       async with AsyncRemoteDBDict(
           'default_db',
           'tcp://127.0.0.1:25555',
           password='yao',
           enable_encryption=True
       ) as db:
           # 并发写入
           await asyncio.gather(
               db.set('key1', 'value1'),
               db.set('key2', 'value2'),
               db.set('key3', 'value3')
           )

           # 并发读取
           results = await asyncio.gather(
               db.get('key1'),
               db.get('key2'),
               db.get('key3')
           )

           # 异步文件传输（并发上传chunk）
           await upload_large_file_async(
               'default_db',
               'tcp://127.0.0.1:25555',
               'my_file',
               '/path/to/large/file.bin',
               max_concurrency=8,  # 8个并发chunk
               password='yao'
           )

   asyncio.run(main())
   ```

3. **启用缓存后必须正常关闭**:
   - 使用 `with` 语句（推荐）
   - 或手动调用 `close()`

4. **生产环境建议**:
   - 对于性能敏感的应用，直接使用 `AsyncRemoteDBDict`
   - 对于简单脚本或向后兼容，使用 `RemoteDBDict`（同步包装器）
   - 远程连接启用加密：`enable_encryption=True, password='your_password'`

5. **性能调优**:
   ```python
   # 读密集型（本地后端）
   db = FlaxKV("cache", "./data",
               performance_profile='read_optimized',
               read_cache_size=10000)

   # 写密集型（本地后端）
   db = FlaxKV("logs", "./data",
               performance_profile='write_optimized',
               write_buffer_size=1000,
               async_flush=True)
   ```

6. **TTL 使用**:
   ```python
   # 设置默认 TTL
   db = FlaxKV("cache", "./data", default_ttl=3600)

   # 单个键设置 TTL
   db.set_ttl("session:123", 1800)
   ```

7. **大文件传输**（同步API）:
   ```python
   from flaxkv2.utils.file_transfer import upload_large_file, download_large_file

   # 上传大文件（自动分块，10MB/块）
   upload_large_file(
       db,
       key="my_video",
       file_path="/path/to/video.mp4",
       chunk_size=10 * 1024 * 1024,
       show_progress=True
   )

   # 下载文件
   download_large_file(
       db,
       key="my_video",
       output_path="./downloads/",
       show_progress=True
   )
   ```

8. **大文件传输**（异步API，性能更高）:
   ```python
   import asyncio
   from flaxkv2.utils.async_file_transfer import upload_large_file_async, download_large_file_async

   async def transfer_files():
       # 异步并发上传（8个并发chunk）
       await upload_large_file_async(
           'default_db',
           'tcp://127.0.0.1:25555',
           'my_file',
           '/path/to/large/file.bin',
           max_concurrency=8,
           password='yao'
       )

       # 异步并发下载
       await download_large_file_async(
           'default_db',
           'tcp://127.0.0.1:25555',
           'my_file',
           './downloads/',
           max_concurrency=8,
           password='yao'
       )

   asyncio.run(transfer_files())
   ```
