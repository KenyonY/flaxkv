# FlaxKV2 智能缓存使用指南

## 🎯 核心理念

FlaxKV2 采用**智能自动检测**机制，通过参数的存在与否自动选择最优后端。不需要学习复杂的策略名称，只需传递你想要的缓存参数即可。

---

## ⚡ 快速开始

### 1. 无缓存（默认，简单可靠）

```python
from flaxkv2 import FlaxKV

# 默认就是无缓存，简单直接
db = FlaxKV("mydb", "./data")
db["key"] = "value"
db.close()
```

**特点：**
- ✅ 简单可靠，数据立即持久化
- ✅ 使用 `RawLevelDBDict`（性能优秀的无缓存实现）
- ✅ 适合小数据量或对一致性要求高的场景

---

### 2. 只读缓存（提升读性能）

```python
from flaxkv2 import FlaxKV

# 只需传递 read_cache_size，自动启用缓存
db = FlaxKV("mydb", "./data", read_cache_size=5000)
db["key"] = "value"
db.close()
```

**特点：**
- ✅ 读性能提升 ~10x（热数据）
- ✅ 写操作仍立即持久化，安全
- ✅ 自动使用 `CachedLevelDBDict`
- ✅ 适合读多写少的应用

**Benchmark 结果：**
- 热数据读取：**1,064,458 ops/s** (9.9x 提升)

---

### 3. 读缓存 + 写缓冲（同步，较安全）

```python
from flaxkv2 import FlaxKV

# 同时传递 read_cache_size 和 write_buffer_size
db = FlaxKV("mydb", "./data",
           read_cache_size=5000,
           write_buffer_size=100,
           async_flush=False)  # 同步 flush
db.close()  # 确保刷新数据
```

**特点：**
- ✅ 读性能提升 ~9x（热数据）
- ⚠️ 写性能可能降低（需要同步刷新）
- ⚠️ 进程崩溃可能丢失缓冲区数据
- ✅ 适合生产环境（可接受小概率数据丢失）

**Benchmark 结果：**
- 热数据读取：**926,459 ops/s** (8.6x 提升)
- 写入性能：848 ops/s

---

### 4. 读缓存 + 异步写缓冲（极致性能，默认配置）

```python
from flaxkv2 import FlaxKV

# 启用缓存后，默认就是极致性能配置！
db = FlaxKV("mydb", "./data",
           read_cache_size=10000,     # 大读缓存（默认）
           write_buffer_size=500)     # 大写缓冲（默认）
                                       # async_flush=True（默认）

# 必须确保正常关闭！
db.close()
```

**特点：**
- ✅ 极致读性能：~13x 提升
- ✅ 异步 flush，不阻塞操作
- ❌ 进程崩溃风险更高
- ⚠️ 必须确保正常关闭（否则数据可能全部丢失）
- ✅ 适合开发环境或可容忍数据丢失的场景

**Benchmark 结果：**
- 热数据读取：**1,434,470 ops/s** (13.4x 提升)
- 写入性能：846 ops/s

---

## 📊 性能对比表

| 配置 | 读性能 | 热读性能 | 写性能 | 数据安全性 |
|------|-------|---------|-------|-----------|
| **无缓存** | 78K ops/s | 107K ops/s (1x) | 1649 ops/s | ⭐⭐⭐⭐⭐ |
| **只读缓存** | 50K ops/s | 1064K ops/s (9.9x) | 1367 ops/s | ⭐⭐⭐⭐⭐ |
| **同步写缓冲** | 33K ops/s | 926K ops/s (8.6x) | 848 ops/s | ⭐⭐⭐⭐ |
| **异步写缓冲（默认）** | 41K ops/s | 1434K ops/s (13.4x) | 846 ops/s | ⭐⭐ |

---

## 🧠 智能检测规则

FlaxKV2 会根据你传递的参数**自动选择**最优后端：

```python
# 规则 1：没有缓存参数 → RawLevelDBDict（无缓存）
db = FlaxKV("mydb", "./data")

# 规则 2：有 read_cache_size → CachedLevelDBDict（启用缓存）
db = FlaxKV("mydb", "./data", read_cache_size=5000)

# 规则 3：有 write_buffer_size → CachedLevelDBDict（启用写缓冲）
db = FlaxKV("mydb", "./data", write_buffer_size=100)

# 规则 4：同时有两者 → CachedLevelDBDict（读缓存+写缓冲）
db = FlaxKV("mydb", "./data", read_cache_size=5000, write_buffer_size=100)
```

---

## 🎛️ 完整参数控制

你可以完全控制每个细节参数：

```python
db = FlaxKV("mydb", "./data",
    # 缓存参数
    read_cache_size=10000,              # 读缓存大小（条目数）
    write_buffer_size=500,              # 写缓冲大小（条目数）
    write_buffer_flush_interval=30,     # 刷新间隔（秒）
    async_flush=True,                   # 是否异步 flush（默认True）

    # TTL 参数
    default_ttl=3600,                   # 默认TTL（秒）
    enable_ttl_cleanup=True,            # 启用自动清理
    cleanup_interval=60,                # 清理间隔

    # LevelDB 性能参数
    performance_profile='balanced',     # 性能配置文件
    lru_cache_size=256*1024*1024,      # LevelDB LRU缓存
    bloom_filter_bits=10,               # 布隆过滤器位数

    # 其他参数
    auto_nested=True,                   # 自动嵌套（默认True）
    rebuild=False,                      # 是否重建数据库
)
```

---

## 💡 使用建议

### 如何选择配置？

```
你的应用场景是什么？
├─ 对数据一致性要求极高（金融、交易）
│  └─> 无缓存（默认）
│
├─ 读多写少（>90%读操作）
│  ├─ 不能接受数据丢失 → 只读缓存
│  └─ 可接受极小概率数据丢失 → 异步写缓冲
│
├─ 读写混合
│  ├─ 生产环境 → 同步写缓冲
│  └─ 开发/测试环境 → 异步写缓冲（默认）
│
└─ 需要极致性能
   └─> 异步写缓冲（默认配置）
```

### 最佳实践

#### 1. **始终使用上下文管理器**

```python
# ✅ 推荐（自动关闭，安全）
with FlaxKV("mydb", "./data", read_cache_size=5000) as db:
    db["key"] = "value"
# 自动调用 close()

# ❌ 不推荐（忘记 close 会丢数据）
db = FlaxKV("mydb", "./data", read_cache_size=5000)
db["key"] = "value"
# 如果程序崩溃，缓冲区数据丢失！
```

#### 2. **启用缓存后记得关闭**

```python
db = FlaxKV("mydb", "./data", write_buffer_size=100)

# 写入大量数据
for i in range(10000):
    db[f"key_{i}"] = f"value_{i}"

# ⚠️ 必须关闭以刷新缓冲区！
db.close()
```

#### 3. **开发环境使用默认极致性能**

```python
# 开发环境：快速迭代
db = FlaxKV("dev_db", "./data", read_cache_size=10000)  # 默认就是极致性能
```

#### 4. **生产环境使用同步 flush**

```python
# 生产环境：安全优先
db = FlaxKV("prod_db", "./data",
           read_cache_size=5000,
           write_buffer_size=100,
           async_flush=False)  # 同步，更安全
```

---

## 🔥 新特性亮点

### 1. **默认启用 auto_nested**

```python
# 默认 auto_nested=True，无需手动指定！
db = FlaxKV("mydb", "./data")

db["config"] = {
    "database": {
        "host": "localhost",
        "port": 5432
    }
}

# 自动转换为 NestedDBDict，高效访问
config = db["config"]
config["database"]["port"] = 3306  # 只修改这个字段，不需要重新序列化整个字典
```

### 2. **统一的参数名称**

```python
# FlaxKV 和 CachedLevelDBDict 使用完全相同的参数名称
from flaxkv2 import FlaxKV
from flaxkv2.core.cached_leveldb_dict import CachedLevelDBDict

# 两者参数完全一致
db1 = FlaxKV("db1", "./data", read_cache_size=5000)
db2 = CachedLevelDBDict("db2", "./data", read_cache_size=5000)
```

### 3. **极致性能为默认配置**

```python
# 只要传递缓存参数，默认就是极致性能！
db = FlaxKV("mydb", "./data", read_cache_size=10000)

# 等同于
db = FlaxKV("mydb", "./data",
           read_cache_size=10000,
           write_buffer_size=500,      # 默认大缓冲
           async_flush=True)           # 默认异步flush
```

---

## 🧪 运行 Benchmark

```bash
# 运行缓存配置对比测试
python benchmarks/cache_configs_benchmark.py

# 输出示例：
# 无缓存          1649 ( 1.0x)    78119 ( 1.0x)   107279 ( 1.0x)
# 只读缓存        1367 ( 0.8x)    49970 ( 0.6x)  1064458 ( 9.9x)
# 同步写缓冲       848 ( 0.5x)    33302 ( 0.4x)   926459 ( 8.6x)
# 异步写缓冲       846 ( 0.5x)    41271 ( 0.5x)  1434470 (13.4x)
```

---

## ❓ 常见问题

### Q: 默认配置是什么？
A: 默认是**无缓存**（`RawLevelDBDict`），简单可靠。

### Q: 如何启用缓存？
A: 传递 `read_cache_size` 或 `write_buffer_size` 参数即可自动启用。

### Q: 启用缓存后的默认配置是什么？
A: **极致性能配置**：`read_cache_size=10000`, `write_buffer_size=500`, `async_flush=True`

### Q: 异步 flush 安全吗？
A: 异步 flush 性能最高，但进程异常崩溃时可能丢失缓冲区数据。正常关闭不会丢数据。

### Q: 如何使用更安全的同步 flush？
A: 传递 `async_flush=False` 参数：
```python
db = FlaxKV("mydb", "./data",
           read_cache_size=5000,
           async_flush=False)
```

### Q: auto_nested 是什么？
A: 自动将字典/列表转换为嵌套存储（`NestedDBDict`/`NestedDBList`），避免频繁完整序列化，性能更高。默认已启用。

### Q: 如何禁用 auto_nested？
A: 传递 `auto_nested=False`：
```python
db = FlaxKV("mydb", "./data", auto_nested=False)
```

---

## 📝 总结

**简化前的问题：**
- ❌ 需要学习 `use_cache`, `enable_write_buffer`, `async_flush` 等多个参数
- ❌ 不清楚什么时候用哪个后端
- ❌ 参数冗余，容易混淆

**简化后的优势：**
- ✅ **智能自动检测**：传递缓存参数自动选择后端
- ✅ **参数即文档**：参数名称清晰，见名知意
- ✅ **极致性能默认**：启用缓存后默认就是最快配置
- ✅ **完全控制**：每个参数都可以自定义
- ✅ **零学习成本**：无需学习策略名称，直接传参即可

**核心使用模式：**
```python
# 无缓存（默认）
db = FlaxKV("mydb", "./data")

# 只读缓存
db = FlaxKV("mydb", "./data", read_cache_size=5000)

# 极致性能（读缓存+异步写缓冲）
db = FlaxKV("mydb", "./data", read_cache_size=10000)

# 完全自定义
db = FlaxKV("mydb", "./data",
           read_cache_size=5000,
           write_buffer_size=100,
           async_flush=False)  # 同步，更安全
```

**就是这么简单！** 🎉
