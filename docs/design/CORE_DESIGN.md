# FlaxKV2 核心设计文档

## 项目概述

FlaxKV2是一个高性能的Python键值数据库系统，提供类似字典的接口来操作持久化存储。它基于LevelDB构建，支持本地和远程访问模式，具有TTL过期、嵌套字典、加密传输等高级特性。

### 核心特点

- **易用性**: Python字典式API，零学习成本
- **高性能**: 基于LevelDB，SSD环境下可达500K+ ops/sec
- **灵活性**: 支持本地、远程两种后端，可自由切换
- **功能完整**: TTL、嵌套存储、类型自动序列化、缓存、压缩、加密
- **生产就绪**: 自动关闭、实例复用、线程安全、完整测试覆盖

---

## 设计理念

### 1. KISS原则 (Keep It Simple, Stupid)

**简单优于复杂** - FlaxKV2的所有设计决策都遵循这一原则：

- RawLevelDBDict取代LevelDBDict成为默认后端（移除不必要的缓冲层）
- TTL信息内嵌到value中，而非分离存储（减少50% I/O）
- 嵌套字典基于LevelDB的prefixed_db特性（避免复杂的索引管理）
- 序列化优先使用msgpack，只在必要时回退到pickle（平衡性能和兼容性）

### 2. 性能优先，但不过度优化

**80/20法则** - 关注影响最大的20%优化：

- ✅ P0: LevelDB配置优化（256MB缓存、布隆过滤器） - 4-25%性能提升
- ✅ P1: 序列化类型缓存 - 15-30%编码性能提升
- ✅ TTL内嵌编码 - 减少50% I/O操作
- ⏳ P2: 批量写入队列 - 预期30-100%写入性能提升（待实施）

### 3. Pythonic设计

- 完全兼容`dict` API：`db['key'] = value`、`del db['key']`、`'key' in db`
- 支持上下文管理器：`with FlaxKV(...) as db:`
- 类型提示友好：所有公开API都有类型注解
- 遵循Python命名规范和代码风格（black格式化）

---

## 核心组件

### 1. 后端架构

```
用户代码
    ↓
FlaxKV (工厂类)
    ↓
┌────────────┬────────────┐
│  本地后端   │  远程后端   │
└────────────┴────────────┘
     ↓              ↓
RawLevelDBDict  RemoteDBDict
     ↓              ↓
  LevelDB      ZeroMQ Client
                    ↓
               FlaxKVServer
                    ↓
               RawLevelDBDict
                    ↓
                 LevelDB
```

#### FlaxKV (flaxkv2/__init__.py)

**职责**: 统一入口，自动检测后端类型

```python
# 本地数据库
db = FlaxKV("mydb", "./data")                    # backend='local'
db = FlaxKV("mydb", "./data", backend='local')

# 远程数据库
db = FlaxKV("mydb", "tcp://127.0.0.1:5555")      # 自动识别
db = FlaxKV("mydb", "127.0.0.1:5555", backend='remote')
```

#### RawLevelDBDict (flaxkv2/core/raw_leveldb_dict.py)

**特点**:
- 直接写入LevelDB，无缓冲层
- 性能优于LevelDBDict 13-25%
- 支持TTL、auto_nested、性能配置
- 默认推荐使用

**核心API**:
```python
db = RawLevelDBDict('mydb', path='./data', auto_nested=True, default_ttl=3600)

# 基础操作
db['key'] = 'value'
value = db['key']
del db['key']
'key' in db

# TTL操作
db.set('key', 'value', ttl=60)
db.set_ttl('key', 60)
db.get_ttl('key')  # 返回剩余秒数

# 嵌套字典
db['user'] = {'name': 'Alice', 'age': 30}
db['user']['age'] = 31  # 只序列化31这个值
```

#### RemoteDBDict (flaxkv2/client/zmq_client.py)

**特点**:
- ZeroMQ REQ socket客户端
- 客户端负责序列化（服务器只处理bytes）
- 支持加密、压缩、缓存
- 自动重连、超时控制

**核心API**:
```python
db = RemoteDBDict(
    'mydb',
    host='127.0.0.1',
    port=5555,
    enable_encryption=True,      # CurveZMQ加密
    password='your_password',    # 密码派生密钥
    enable_compression=True,     # LZ4压缩
    read_cache_size=10000,      # LRU缓存
)
```

---

### 2. 序列化系统

**设计目标**: 自动化、高性能、类型安全

#### 编码策略 (flaxkv2/serialization/encoder.py)

```
Python对象 → 编码 → bytes

1. NumPy数组: 特殊处理（保留dtype和shape）
2. Pandas DataFrame: 特殊处理（可选依赖）
3. 基础类型: msgpack（快速）
4. 复杂对象: pickle（兼容性）
```

**类型缓存优化**:
```python
# 首次编码: 尝试msgpack → 成功则缓存
_type_cache[type(value)] = _encode_msgpack

# 后续编码: 直接使用缓存的编码器
encoder = _type_cache[type(value)]
return encoder(value)
```

**效果**: 重复类型编码性能提升15-30%，缓存命中率99%+

#### 键编码 (flaxkv2/serialization/key_encoder.py)

保证顺序性质，支持范围查询：

```python
encode_key('user')  → b'suser'      # 字符串
encode_key(123)     → b'i' + (8字节) # 整数（大端序）
encode_key(3.14)    → b'f' + (8字节) # 浮点数
```

---

### 3. TTL管理

**设计演进**: 分离式 → 内嵌式

#### 旧设计（已废弃）
```python
# 问题：两次I/O操作
db.put('key', value)                # 写数据
db.put('__ttl_info__:key', expire)  # 写TTL
```

#### 新设计（当前）
```python
# 优势：单次I/O，数据和TTL原子写入
value_bytes = ValueWithMeta.encode_value(value, ttl=60)
db.put('key', value_bytes)

# 读取时自动检查
value, expire_time, is_expired = ValueWithMeta.decode_value(value_bytes)
if is_expired:
    db.delete('key')
    raise KeyError('key')
```

**编码格式**:
```
[VERSION(1byte)][FLAGS(1byte)][EXPIRE_TIME(8bytes,可选)][VALUE_BYTES]

无TTL: 0x01 0x00 <value_bytes>
有TTL: 0x01 0x01 <8字节时间戳> <value_bytes>
```

**效果**: I/O操作减少50%，代码简化，为批量写入和缓存优化铺平道路

---

### 4. 嵌套字典 (auto_nested)

**设计理念**: 利用LevelDB的prefixed_db特性，每个字段独立存储

#### 普通模式 vs auto_nested

```python
# 普通模式：整个字典序列化
db['config'] = {'host': 'localhost', 'port': 8080}
# 存储: 'config' → msgpack({'host': 'localhost', 'port': 8080})
# 修改port需要重新序列化整个字典

# auto_nested模式：字段独立存储
db = RawLevelDBDict('mydb', auto_nested=True)
db['config'] = {'host': 'localhost', 'port': 8080}
# 存储:
#   '__nested__:config' → True (marker)
#   'config:host' → 'localhost'
#   'config:port' → 8080
# 修改port只序列化新值
db['config']['port'] = 3306  # 只写入 'config:port'
```

#### NestedDBDict实现

```python
class NestedDBDict(MutableMapping):
    """基于prefixed_db的嵌套字典实现"""

    def __init__(self, prefixed_db, prefix, ...):
        self._db = prefixed_db  # LevelDB.prefixed_db(b'config:')

    def __getitem__(self, key):
        # 读取 'config:key' → 只反序列化这个字段
        value_bytes = self._db.get(key.encode())
        return decode(value_bytes)

    def __setitem__(self, key, value):
        # 写入 'config:key' → 只序列化这个字段
        value_bytes = encode(value)
        self._db.put(key.encode(), value_bytes)
```

**优势**:
- 大字典部分修改性能优秀
- 支持递归嵌套
- TTL在marker中，整体过期

**权衡**:
- 小字典开销稍高（额外marker键）
- 需要更多键空间

**适用场景**:
- ✅ 配置对象、用户档案、大型嵌套结构
- ❌ 小字典（< 5个字段）、频繁整体替换

---

### 5. 远程访问 (ZeroMQ)

#### 协议设计

**消息格式**:
```python
# 请求: [命令, 数据库名, 参数...]
[CMD_GET, b'mydb', key_bytes]
[CMD_SET, b'mydb', key_bytes, value_bytes]

# 响应: [状态码, 数据]
[STATUS_OK, result_bytes]
[STATUS_NOT_FOUND, None]
[STATUS_ERROR, error_message_bytes]
```

**命令类型**:
- `CONNECT`: 连接数据库
- `GET`: 读取键值
- `SET`: 写入键值
- `DELETE`: 删除键
- `KEYS/VALUES/ITEMS`: 批量查询
- `UPDATE`: 批量更新
- `PING`: 健康检查

#### 服务器架构

```python
class FlaxKVServer:
    def __init__(self, host, port, data_dir):
        self.socket = zmq.ROUTER  # 支持多客户端
        self.databases = {}       # 数据库实例缓存

    def handle_request(self, identity, request):
        command, db_name, *args = request

        # 获取或创建数据库实例
        db = self.databases.get(db_name)
        if not db:
            db = RawLevelDBDict(db_name, self.data_dir)
            self.databases[db_name] = db

        # 处理命令（只操作bytes，不序列化）
        if command == CMD_GET:
            key_bytes = args[0]
            value_bytes = db._db.get(key_bytes)  # 直接从LevelDB读取
            return [STATUS_OK, value_bytes]
```

**关键设计**:
- 服务器只处理二进制数据，不理解数据类型
- 服务器理解键名（用于TTL检查）
- 服务器端TTL验证（减少50%网络请求）

#### 加密通信 (CurveZMQ + 密码认证)

**方案2：密码派生密钥（推荐）**

```python
# 服务器
server = FlaxKVServer(
    enable_encryption=True,
    password='your_secure_password',  # 使用PBKDF2派生密钥
    derive_from_password=True,        # 默认
)

# 客户端（可以在不同机器）
client = RemoteDBDict(
    'mydb',
    enable_encryption=True,
    password='your_secure_password',  # 相同密码生成相同密钥
)
```

**密钥派生流程**:
```python
salt = b'flaxkv2_curve25519_salt'
key_material = pbkdf2_hmac('sha256', password, salt, 100000, dklen=32)
secret_key, public_key = derive_curve25519_keypair(key_material)
```

**优势**:
- 跨机器部署：只需密码，无需复制文件
- 确定性：相同密码→相同密钥
- 简单：零文件管理

---

### 6. 性能配置

**预设配置文件** (flaxkv2/config.py):

```python
PROFILES = {
    'balanced': {           # 默认，通用场景
        'lru_cache_size': 256 * 1024 * 1024,
        'bloom_filter_bits': 10,
        'block_size': 16 * 1024,
    },
    'read_optimized': {     # 读密集型（API服务、缓存）
        'lru_cache_size': 512 * 1024 * 1024,
        'bloom_filter_bits': 12,
    },
    'write_optimized': {    # 写密集型（日志收集）
        'write_buffer_size': 256 * 1024 * 1024,
    },
    'memory_constrained': { # 嵌入式、容器
        'lru_cache_size': 64 * 1024 * 1024,
        'write_buffer_size': 32 * 1024 * 1024,
    },
}
```

**使用方式**:
```python
# 零配置：自动使用balanced
db = FlaxKV("mydb", "./data")

# 选择预设
db = FlaxKV("cache", "./data", performance_profile='read_optimized')

# 自定义
db = FlaxKV("mydb", "./data", lru_cache_size=512*1024*1024)
```

**性能提升**:
- 热数据读取: +22% (read_optimized)
- 单次写入: +25% (memory_constrained)
- 综合性能: +4-9% (所有配置 vs legacy)

---

## 数据流

### 本地写入流程

```
用户: db['key'] = value
  ↓
RawLevelDBDict.__setitem__
  ↓
1. 检查auto_nested
  ↓
2. encode_key('key') → key_bytes
  ↓
3. encode_value(value, ttl) → value_bytes
   [VERSION][FLAGS][EXPIRE_TIME?][VALUE]
  ↓
4. 获取锁 (self._db_lock)
  ↓
5. plyvel.DB.put(key_bytes, value_bytes)
  ↓
6. 释放锁
  ↓
完成
```

### 本地读取流程

```
用户: value = db['key']
  ↓
RawLevelDBDict.__getitem__
  ↓
1. encode_key('key') → key_bytes
  ↓
2. 检查auto_nested marker
  ↓
3. 获取锁 (self._db_lock)
  ↓
4. value_bytes = plyvel.DB.get(key_bytes)
  ↓
5. 释放锁
  ↓
6. decode_value(value_bytes)
   → (value, expire_time, is_expired)
  ↓
7. 检查is_expired
   如果过期 → 删除并抛出KeyError
  ↓
8. 返回value
```

### 远程写入流程

```
用户: db['key'] = value
  ↓
RemoteDBDict.__setitem__
  ↓
1. encode_key('key') → key_bytes
  ↓
2. encode_value(value) → value_bytes
  ↓
3. 构造请求
   [CMD_SET, db_name, key_bytes, value_bytes]
  ↓
4. 压缩（如果enable_compression且大于阈值）
  ↓
5. ZeroMQ.send → 网络
  ↓
FlaxKVServer接收
  ↓
6. 解压（如果需要）
  ↓
7. 解析请求
  ↓
8. db._db.put(key_bytes, value_bytes)
  ↓
9. 响应 [STATUS_OK]
  ↓
10. ZeroMQ.send → 网络
  ↓
RemoteDBDict接收响应
  ↓
完成
```

---

## 关键设计决策

### 1. 为什么移除LevelDBDict的缓冲机制？

**测试数据** (BUFFERED_VS_RAW_BENCHMARK_REPORT.md):
- 读取性能: RawLevelDBDict比LevelDBDict快62.9%
- 写入性能: 缓冲版快17.8%，但立即刷新慢50.5%
- **结论**: 现代SSD环境下，LevelDB自身的缓存足够高效，额外缓冲层反而增加开销

**决策**: RawLevelDBDict成为默认，LevelDBDict标记为deprecated

### 2. 为什么TTL内嵌而非分离存储？

**旧设计问题**:
- 每次读写TTL键都需要额外I/O
- 批量操作需要维护两个键的一致性
- 缓存需要存储两份数据

**新设计优势**:
- 减少50% I/O操作
- 批量写入简化（1个逻辑操作=1个物理操作）
- 缓存设计简化（1个key=1个缓存项）

### 3. 为什么选择ZeroMQ而非HTTP？

**ZeroMQ优势**:
- 更低延迟（本地环境<1ms）
- 内置多种通信模式（REQ/REP, ROUTER/DEALER）
- 原生支持CurveZMQ加密
- 无需解析HTTP头，纯二进制传输

**权衡**:
- 不如HTTP通用（需要安装pyzmq）
- 调试稍复杂（需要专门工具）

**决策**: 性能和效率优先，适合内网/可信环境

### 4. 为什么服务器只处理bytes？

**保持简单**:
- 服务器不需要理解数据类型
- 序列化逻辑集中在客户端
- 未来更换序列化方案无需修改服务器

**例外**:
- 服务器理解键名（用于构造TTL键）
- 服务器理解TTL值（用于过期检查）

**权衡**: 牺牲一点抽象性，换取简单性和性能

---

## 安全考虑

### 1. Pickle序列化风险

**风险**: pickle.loads()可以执行任意代码

**缓解措施**:
- 文档明确警告（README、ARCHITECTURE）
- 优先使用msgpack（安全）
- 仅在不可信环境禁用复杂对象存储
- 考虑未来添加`safe_mode`参数

### 2. 远程连接安全

**默认情况**:
- ❌ 无加密（数据明文传输）
- ❌ 无认证（任何人可连接）

**生产环境建议**:
```python
# 1. 启用CurveZMQ加密
server = FlaxKVServer(enable_encryption=True, password='strong_password')

# 2. 监听127.0.0.1而非0.0.0.0
server = FlaxKVServer(host='127.0.0.1')

# 3. 使用SSH隧道
ssh -L 5555:localhost:5555 user@remote-server
```

**密码强度要求**:
- 最低16字符
- 包含大小写字母、数字、特殊字符
- 推荐使用`secrets`模块生成

---

## 测试策略

### 测试结构

```
tests/
├── unit/              # 单元测试
│   ├── test_core.py                # RawLevelDBDict核心功能
│   ├── test_special_values.py      # 特殊值处理
│   └── test_*_ttl.py              # TTL相关测试
│
├── integration/       # 集成测试
│   ├── test_auto_nested.py        # 嵌套字典
│   ├── test_zmq_remote.py         # 远程访问
│   ├── test_encryption_compression.py
│   └── test_password_auth.py      # 密码认证
│
└── benchmarks/        # 性能测试
    ├── benchmark_cache.py
    └── benchmark_nested.py
```

### 测试覆盖

**核心功能** (test_core.py):
- 基础CRUD操作
- 批量操作
- 迭代器
- 上下文管理器

**TTL功能**:
- 基本TTL设置和过期
- 默认TTL
- TTL持久化
- auto_nested + TTL组合

**远程功能** (test_zmq_remote.py):
- 基础远程读写
- 加密通信
- 压缩传输
- 密码认证（两种方案）

**性能测试** (benchmarks/):
- 配置文件对比
- 缓存性能
- 嵌套字典性能

---

## 未来规划

### 已完成 ✅

1. **P0: LevelDB配置优化** - 4-25%性能提升
2. **P1: 序列化类型缓存** - 15-30%编码性能提升
3. **TTL内嵌重构** - 减少50% I/O
4. **auto_nested改进** - MutableMapping接口、copy()方法
5. **密码派生密钥** - 支持跨机器部署

### 短期计划 📋

1. **批量写入队列** (P1, 高优先级)
   - 自动批处理后台线程
   - 预期30-100%写入性能提升
   - 权衡: 增加写入延迟5-10ms

2. **远程传输压缩** (P2, 中优先级)
   - LZ4压缩大消息
   - 预期减少50-70%网络带宽

3. **读写锁分离** (P2, 中优先级)
   - RWLock替代RLock
   - 预期并发读性能提升100-200%

### 长期规划 🔮

（需求待评估）

1. **集群支持**
   - 主从复制
   - 数据分片
   - 一致性哈希

2. **事务支持**
   - 多键原子操作
   - 乐观锁/悲观锁

3. **高级查询**
   - 二级索引
   - 范围查询优化
   - 全文搜索

4. **监控系统**
   - Prometheus metrics
   - 性能分析工具
   - 实时监控面板

---

## 相关文档

- **架构设计**: [ARCHITECTURE.md](./ARCHITECTURE.md)
- **编码详解**: [KEY_VALUE_ENCODING_EXPLAINED.md](./KEY_VALUE_ENCODING_EXPLAINED.md)
- **性能优化**: [../performance/PERFORMANCE_OPTIMIZATION_PLAN.md](../performance/PERFORMANCE_OPTIMIZATION_PLAN.md)
- **开发指南**: [../development/CLAUDE.md](../development/CLAUDE.md)
- **用户指南**: [../../PASSWORD_AUTH_GUIDE.md](../../PASSWORD_AUTH_GUIDE.md)

---

**文档维护**: 随着项目演进持续更新此文档
**最后更新**: 2025-10-31
