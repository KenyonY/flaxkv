# FlaxKV2 统一缓存设计文档

## 概述

FlaxKV2 采用**统一缓存（Unified Cache）**设计，这是一个经典的 write-back cache 实现，完全遵循 KISS 原则。

**设计理念**：
- 单一缓存结构（读写合一）
- Dirty 标记机制（追踪需要刷新的数据）
- LRU 淘汰策略（自动管理缓存大小）
- TTL 支持（自动处理过期数据）
- 批量刷新（达到阈值或定时触发）

**核心优势**：
- ✅ 概念简单：单一数据结构，易于理解和维护
- ✅ 一致性天然保证：数据只在一个地方，不会出现不一致
- ✅ 经典设计模式：write-back cache 经过数十年验证
- ✅ 性能优异：支持同步/异步刷新，满足不同性能需求

---

## 架构设计

### 核心组件

```
┌─────────────────────────────────────────────┐
│           UnifiedCache（统一缓存）            │
├─────────────────────────────────────────────┤
│                                             │
│  ┌─────────────────────────────────────┐   │
│  │   OrderedDict[key, CacheEntry]      │   │
│  │   - value: 缓存的值                  │   │
│  │   - expire_time: TTL过期时间         │   │
│  │   - dirty: 是否需要刷新               │   │
│  │   - timestamp: 访问时间（LRU）        │   │
│  └─────────────────────────────────────┘   │
│                                             │
│  ┌─────────────────────────────────────┐   │
│  │   Dirty Tracking（脏数据追踪）       │   │
│  │   - dirty_keys: Set[key]            │   │
│  │   - delete_keys: Set[key]           │   │
│  └─────────────────────────────────────┘   │
│                                             │
│  ┌─────────────────────────────────────┐   │
│  │   Background Threads（后台线程）     │   │
│  │   - Auto flush thread（定时刷新）    │   │
│  │   - Async flush worker（异步刷新）   │   │
│  └─────────────────────────────────────┘   │
│                                             │
└─────────────────────────────────────────────┘
           ↓ flush_callback
┌─────────────────────────────────────────────┐
│      Persistent Storage（持久化存储）        │
│      - LevelDB（本地）                       │
│      - Remote Server（远程）                 │
└─────────────────────────────────────────────┘
```

### CacheEntry 结构

```python
class CacheEntry:
    value: Any              # 缓存的值
    expire_time: float      # TTL过期时间（None表示无TTL）
    dirty: bool            # 是否需要刷新到持久化存储
    timestamp: float       # 创建/访问时间（用于LRU）
```

---

## 核心机制

### 1. Dirty 标记机制

**Dirty 标记用于区分数据来源和状态**：

```python
# 写入操作：dirty=True（需要刷新到存储）
cache.put(key, value, ttl=60, dirty=True)

# 从存储加载：dirty=False（已在存储中）
cache.put(key, value, ttl=60, dirty=False)

# 删除操作：标记到 delete_keys
cache.delete(key)
```

**工作流程**：
1. 写入时：数据进入缓存，标记为 dirty
2. 达到阈值或定时：触发 flush
3. Flush 时：将所有 dirty 数据批量写入存储
4. Flush 后：清除 dirty 标记，数据保留在缓存中

### 2. LRU 淘汰策略

使用 `OrderedDict` 实现 LRU：

```python
# 读取时：移到末尾（最近使用）
self._cache.move_to_end(key)

# 淘汰时：弹出第一个（最少使用）
key, entry = self._cache.popitem(last=False)

# 淘汰前：如果是 dirty 数据，先 flush
if entry.dirty:
    self._flush_single_entry(key, entry)
```

### 3. TTL 支持

自动处理过期数据：

```python
# 写入时设置 TTL
cache.put(key, value, ttl=60, dirty=True)  # 60秒后过期

# 读取时检查过期
entry = cache.get(key)
if entry.is_expired():
    # 自动删除过期数据
    del cache[key]
    return None
```

### 4. 批量刷新

两种触发方式：

**阈值触发**：
```python
if len(self._dirty_keys) >= self._flush_threshold:
    self._flush()  # 达到阈值，立即刷新
```

**定时触发**：
```python
# 后台线程每隔 flush_interval 秒检查
if time.time() - self._last_flush_time >= self._flush_interval:
    self._flush()  # 定时刷新
```

---

## 使用模式

### 模式1：无缓存（最安全）

```python
db = CachedLevelDBDict(
    "mydb",
    read_cache_size=0,          # 禁用读缓存
    enable_write_buffer=False   # 禁用写缓冲
)
```

**特点**：
- 所有操作直接访问数据库
- 数据安全性最高
- 性能：~7,700 ops/s 写入，~38,000 ops/s 读取

**适用场景**：金融交易、关键配置

---

### 模式2：只读缓存（推荐默认）

```python
db = CachedLevelDBDict(
    "mydb",
    read_cache_size=10000,      # 启用读缓存
    enable_write_buffer=False,  # 禁用写缓冲
    performance_profile='read_optimized'
)
```

**特点**：
- 读取缓存，写入直达数据库
- Write-through 策略
- 性能：~7,700 ops/s 写入，~760,000 ops/s 读取（热缓存）

**适用场景**：配置读取、元数据查询、读多写少

---

### 模式3：写缓冲（同步flush）

```python
db = CachedLevelDBDict(
    "mydb",
    read_cache_size=10000,
    enable_write_buffer=True,   # 启用写缓冲
    write_buffer_size=100,      # 每100条刷新
    async_flush=False,          # 同步flush
    performance_profile='write_optimized'
)
```

**特点**：
- Write-back 策略
- 批量刷新，减少I/O
- 同步 flush：阻塞等待完成
- 性能：~12,800 ops/s 写入，~750,000 ops/s 读取

**适用场景**：需要批量优化，但要求数据可靠性

**风险**：进程崩溃可能丢失未刷新数据

---

### 模式4：写缓冲（异步flush）⚡

```python
db = CachedLevelDBDict(
    "mydb",
    read_cache_size=10000,
    enable_write_buffer=True,   # 启用写缓冲
    write_buffer_size=100,
    async_flush=True,           # 异步flush（极速）
    performance_profile='write_optimized'
)
```

**特点**：
- 双缓冲异步刷新
- 写入不阻塞，性能极致
- 性能：~83,000 ops/s 写入，~720,000 ops/s 读取

**适用场景**：日志收集、指标存储、可容忍少量数据丢失

**风险**：数据安全风险最大

---

## 性能对比

### 写入性能

| 模式 | 吞吐量 (ops/s) | vs 无缓存 |
|------|----------------|-----------|
| 无缓存 | 7,730 | 1.00x |
| 只读缓存 | 7,680 | 0.99x |
| 写缓冲（同步） | 12,800 | 1.66x ⭐ |
| 写缓冲（异步） | 83,200 | 10.76x ⚡ |

### 读取性能（热缓存）

| 模式 | 吞吐量 (ops/s) | vs 无缓存 |
|------|----------------|-----------|
| 无缓存 | 37,800 | 1.00x |
| 只读缓存 | 760,000 | 20.11x ⭐ |
| 写缓冲（同步） | 750,000 | 19.84x |
| 写缓冲（异步） | 720,000 | 19.05x |

---

## API 使用

### 基本操作

```python
from flaxkv2 import CachedLevelDBDict

# 创建数据库
db = CachedLevelDBDict(
    "mydb",
    read_cache_size=1000,
    enable_write_buffer=True,
    write_buffer_size=100
)

# 写入（自动缓存，标记 dirty）
db['key1'] = 'value1'
db.set('key2', 'value2', ttl=60)  # 带TTL

# 读取（从缓存读取，如果有）
value = db['key1']
value = db.get('key2', default=None)

# 删除（标记到 delete_keys）
del db['key1']

# 手动刷新
db.flush()

# 关闭（自动刷新剩余数据）
db.close()
```

### 统计信息

```python
stats = db._cache.stats()
print(f"总条目数: {stats['total_entries']}")
print(f"脏数据数: {stats['dirty_entries']}")
print(f"待删除数: {stats['delete_entries']}")
print(f"距上次刷新: {stats['time_since_last_flush']:.2f}秒")
```

---

## 内部实现

### UnifiedCache 类

位置：`flaxkv2/utils/unified_cache.py`

**核心方法**：

```python
class UnifiedCache:
    def get(self, key, default=None) -> Any:
        """读取缓存（自动处理TTL和LRU）"""

    def put(self, key, value, ttl=None, dirty=True):
        """
        写入缓存

        Args:
            dirty: True=需要刷新, False=已在存储中
        """

    def delete(self, key):
        """标记删除"""

    def flush(self):
        """手动刷新所有 dirty 数据"""

    def _do_flush(self):
        """执行批量刷新（调用 flush_callback）"""

    def _evict_if_needed(self):
        """LRU 淘汰（先 flush dirty 数据）"""
```

### Flush Callback

```python
def _flush_unified_cache_callback(self, writes: Dict, deletes: Set):
    """
    缓存刷新回调

    Args:
        writes: {key: (value, ttl)} 待写入数据
        deletes: {key, ...} 待删除键
    """
    # 批量写入存储
    batch = self._db.write_batch()

    for key, (value, ttl) in writes.items():
        key_bytes = self._encode_key(key)
        value_bytes = self._encode_value(value, ttl_seconds=ttl)
        batch.put(key_bytes, value_bytes)

    for key in deletes:
        key_bytes = self._encode_key(key)
        batch.delete(key_bytes)

    batch.write()
```

---

## 线程安全

UnifiedCache 使用 `threading.RLock` 保证线程安全：

```python
with self._lock:
    # 所有缓存操作都在锁保护下进行
    entry = self._cache.get(key)
    ...
```

**后台线程**：
- **Auto flush thread**：定时检查并刷新
- **Async flush worker**：异步执行刷新任务（双缓冲）

---

## 对比：统一缓存 vs 双缓存

### 旧设计（双缓存）❌

```
SimpleLRUCache (读缓存)
    ↓
    ↓  需要手动同步
    ↓
WriteBuffer (写缓冲)
```

**问题**：
- 两个独立结构，需要手动维护一致性
- 容易出现一致性 Bug
- 代码复杂，维护成本高
- 概念不清晰

### 新设计（统一缓存）✅

```
UnifiedCache (统一缓存)
    ├─ OrderedDict (数据)
    ├─ Dirty tracking (脏标记)
    └─ Background threads (后台刷新)
```

**优势**：
- 单一结构，一致性天然保证
- 经典 write-back cache 设计
- 代码简洁，易于维护
- 概念清晰，符合 KISS 原则

---

## 最佳实践

### 1. 选择合适的模式

**读多写少**：
```python
read_cache_size=10000
enable_write_buffer=False
```

**写多读少**：
```python
enable_write_buffer=True
async_flush=True  # 如果可容忍数据丢失
```

**平衡模式**：
```python
read_cache_size=1000
enable_write_buffer=True
async_flush=False
```

### 2. 设置合理的阈值

```python
write_buffer_size=100      # 根据写入频率调整
flush_interval=60          # 根据数据实时性要求调整
```

### 3. 监控缓存状态

```python
# 定期检查缓存统计
stats = db._cache.stats()
if stats['dirty_entries'] > threshold:
    db.flush()  # 主动刷新
```

### 4. 优雅关闭

```python
# 使用上下文管理器（推荐）
with CachedLevelDBDict("mydb") as db:
    db['key'] = 'value'
# 自动调用 flush() 和 close()

# 或手动关闭
db.flush()  # 确保数据刷新
db.close()
```

---

## 总结

FlaxKV2 的统一缓存设计是一个：
- ✅ **简单**：单一缓存结构，遵循 KISS 原则
- ✅ **可靠**：一致性天然保证，消除了双缓存的 Bug
- ✅ **高效**：性能优异，支持多种优化模式
- ✅ **灵活**：4种模式满足不同场景需求
- ✅ **经典**：基于经过验证的 write-back cache 设计

相比旧的双缓存设计，统一缓存在保持相同性能的同时，大幅降低了代码复杂度和维护成本。

---

**相关文件**：
- 实现：`flaxkv2/utils/unified_cache.py`
- 应用：`flaxkv2/core/cached_leveldb_dict.py`
- 应用：`flaxkv2/client/zmq_client.py`
- 测试：`test_unified_cache.py`
- 性能：`benchmarks/unified_cache_benchmark.py`
