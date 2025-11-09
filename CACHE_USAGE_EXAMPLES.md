# FlaxKV 缓存使用示例

本文档展示如何在 FlaxKV 中使用统一缓存（CachedLevelDBDict）。

## 快速开始

### 1. 默认模式（无缓存，最安全）

```python
from flaxkv2 import FlaxKV

# 默认使用 RawLevelDBDict（无缓存）
db = FlaxKV("mydb", "./data")

db['key1'] = 'value1'
print(db['key1'])  # 'value1'

db.close()
```

**特点**：
- ✅ 数据安全性最高
- ✅ 所有操作直接写入磁盘
- ✅ 进程崩溃不会丢失数据
- 📊 性能：~7,700 ops/s 写入，~38,000 ops/s 读取

---

### 2. 只读缓存模式（推荐用于读多写少场景）

```python
from flaxkv2 import FlaxKV

# 启用缓存，但不启用写缓冲
db = FlaxKV("mydb", "./data",
           use_cache=True,
           read_cache_size=10000,
           enable_write_buffer=False)

# 写入直接到磁盘
db['key1'] = 'value1'

# 读取从缓存
print(db['key1'])  # 从缓存读取，速度快

db.close()
```

**特点**：
- ✅ 读取性能极高（热数据）
- ✅ 写入安全（write-through）
- ✅ 数据不会丢失
- 📊 性能：~7,700 ops/s 写入，~760,000 ops/s 读取（热缓存）

**适用场景**：配置读取、元数据查询、读多写少

---

### 3. 写缓冲模式（同步 flush）

```python
from flaxkv2 import FlaxKV

# 启用缓存 + 写缓冲（同步）
db = FlaxKV("mydb", "./data",
           use_cache=True,
           enable_write_buffer=True,
           write_buffer_size=100,      # 每100条刷新
           async_flush=False)          # 同步flush（安全）

# 写入到缓冲区
for i in range(200):
    db[f'key{i}'] = f'value{i}'

# 手动刷新（可选）
db.flush()

db.close()
```

**特点**：
- ✅ 批量写入优化
- ✅ 同步 flush（阻塞等待完成）
- ⚠️ 进程崩溃可能丢失未刷新数据
- 📊 性能：~12,800 ops/s 写入，~750,000 ops/s 读取

**适用场景**：需要批量优化，但要求数据可靠性

---

### 4. 写缓冲模式（异步 flush）⚡ 极速模式

```python
from flaxkv2 import FlaxKV

# 启用缓存 + 写缓冲（异步）
db = FlaxKV("mydb", "./data",
           use_cache=True,
           enable_write_buffer=True,
           write_buffer_size=100,
           async_flush=True)           # 异步flush（极速）

# 极速写入
for i in range(10000):
    db[f'key{i}'] = f'value{i}'

# 确保数据刷新
db.flush()
db.close()
```

**特点**：
- ⚡ 写入性能极致
- ⚡ 异步刷新（不阻塞）
- ⚠️ 数据安全风险最大
- 📊 性能：~83,000 ops/s 写入，~720,000 ops/s 读取

**适用场景**：日志收集、指标存储、可容忍少量数据丢失

---

## 高级配置

### 性能配置文件

```python
from flaxkv2 import FlaxKV

# 读优化配置
db_read = FlaxKV("mydb", "./data",
                use_cache=True,
                performance_profile='read_optimized',
                read_cache_size=10000)

# 写优化配置
db_write = FlaxKV("mydb", "./data",
                 use_cache=True,
                 performance_profile='write_optimized',
                 enable_write_buffer=True,
                 async_flush=True)

# 内存受限配置
db_mem = FlaxKV("mydb", "./data",
               use_cache=True,
               performance_profile='memory_constrained',
               read_cache_size=500)
```

### 自定义缓存参数

```python
from flaxkv2 import FlaxKV

db = FlaxKV("mydb", "./data",
           use_cache=True,
           # 缓存大小
           read_cache_size=5000,
           # 写缓冲
           enable_write_buffer=True,
           write_buffer_size=200,
           write_buffer_flush_interval=30,  # 30秒刷新一次
           # 异步flush
           async_flush=False,
           # LevelDB配置
           lru_cache_size=16*1024*1024,     # 16MB
           bloom_filter_bits=10)

db.close()
```

---

## 性能对比

| 模式 | 写入 (ops/s) | 读取 (ops/s) | 数据安全 |
|------|--------------|--------------|----------|
| 无缓存 | 7,730 | 37,800 | ⭐⭐⭐⭐⭐ |
| 只读缓存 | 7,680 | 760,000 | ⭐⭐⭐⭐⭐ |
| 写缓冲（同步） | 12,800 | 750,000 | ⭐⭐⭐⭐ |
| 写缓冲（异步） | 83,200 | 720,000 | ⭐⭐ |

---

## 选择建议

### 🛡️ 数据安全优先
```python
db = FlaxKV("mydb", "./data")  # 默认无缓存
```

### ⚡ 读取性能优先
```python
db = FlaxKV("mydb", "./data", use_cache=True)
```

### 🚀 读写性能优先（可容忍数据丢失）
```python
db = FlaxKV("mydb", "./data",
           use_cache=True,
           enable_write_buffer=True,
           async_flush=True)
```

### 🎯 平衡模式
```python
db = FlaxKV("mydb", "./data",
           use_cache=True,
           enable_write_buffer=True,
           async_flush=False)
```

---

## 注意事项

1. **数据安全**：
   - 启用 `enable_write_buffer=True` 会增加数据丢失风险
   - 启用 `async_flush=True` 风险更大
   - 建议在关闭前调用 `db.flush()` 确保数据刷新

2. **缓存大小**：
   - `read_cache_size` 根据工作负载调整（推荐 1000-10000）
   - `write_buffer_size` 根据写入频率调整（推荐 100-1000）

3. **刷新间隔**：
   - `write_buffer_flush_interval` 根据数据实时性要求调整
   - 间隔越短，数据越安全，但性能越低

4. **监控缓存**：
   ```python
   stats = db._cache.stats()
   print(f"脏数据数: {stats['dirty_entries']}")
   print(f"待删除数: {stats['delete_entries']}")
   print(f"距上次刷新: {stats['time_since_last_flush']:.2f}秒")
   ```

---

## 完整示例

```python
from flaxkv2 import FlaxKV

# 创建数据库（启用缓存）
with FlaxKV("mydb", "./data",
           use_cache=True,
           enable_write_buffer=True,
           write_buffer_size=100,
           async_flush=False) as db:

    # 批量写入
    for i in range(1000):
        db[f'user:{i}'] = {
            'name': f'User{i}',
            'age': 20 + i % 50,
            'city': 'NYC'
        }

    # 读取（从缓存）
    user = db['user:100']
    print(user)  # {'name': 'User100', 'age': 70, 'city': 'NYC'}

    # 手动刷新
    db.flush()

# 自动关闭并刷新剩余数据
```

---

## 更多信息

详细设计文档请参考：[UNIFIED_CACHE_DESIGN.md](UNIFIED_CACHE_DESIGN.md)
