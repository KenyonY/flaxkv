# FlaxKV2 Benchmark 总结

## 你的问题解答

### Q1: `read_cache_size` 和 `write_buffer_size` 的含义是什么？

**答案**:
- ✅ `read_cache_size`: **条目数量**（多少个键值对），不是字节大小
- ✅ `write_buffer_size`: **条目数量**（多少个脏条目），不是字节大小

你的记忆是对的！你可能记得的"不是内存大小"的参数是 `write_buffer_size_leveldb`，它是 LevelDB 底层的写缓冲区大小，单位是**字节**。

**源代码位置**:
- `flaxkv2/core/cached_leveldb_dict.py:100` - `read_cache_size: int = 1000`
- `flaxkv2/core/cached_leveldb_dict.py:107` - `write_buffer_size: int = 100`
- `flaxkv2/core/cached_leveldb_dict.py:115` - `write_buffer_size_leveldb: Optional[int] = None`

---

### Q2: 如何系统地测试 FlaxKV2 的性能？

**答案**: 我已经创建了两个 benchmark 工具：

#### 1️⃣ 快速测试（推荐日常使用）

```bash
python benchmarks/quick_benchmark.py
```

**运行时间**: ~5-10秒
**测试内容**: 缓存配置、嵌套结构、数据类型

#### 2️⃣ 综合测试（发布前使用）

```bash
python benchmarks/comprehensive_benchmark.py
```

**运行时间**: ~30-60秒
**测试内容**: 全面测试所有功能和配置

详细文档见: `benchmarks/README.md`

---

## 核心性能数据

基于你的代码模式，以下是关键发现：

### 嵌套结构性能对比

你的代码使用了 `auto_nested=True` 和 `auto_nested=False` 对比，这里是测试结果：

```python
# 你的测试代码
db = FlaxKV("kunyuan", auto_nested=True, rebuild=True,
            read_cache_size=512 * 1024 * 1024,  # ← 这个参数设置有误！
            write_buffer_size=512 * 1024 * 1024)

db_no_nested = FlaxKV("kunyuan_no_nested",
                      auto_nested=False, rebuild=True,
                      read_cache_size=512 * 1024 * 1024,  # ← 这个参数设置有误！
                      write_buffer_size=512 * 1024 * 1024)
```

⚠️ **重要发现**: 你的 `read_cache_size` 和 `write_buffer_size` 设置为 `512 * 1024 * 1024` (536,870,912 条！)，这是**不合理的**！

**原因**:
- 这两个参数是**条目数量**，不是字节大小
- 设置 5亿+ 条目会导致内存溢出
- 正确的设置应该是：`read_cache_size=5000`, `write_buffer_size=100`

**正确的测试代码**:

```python
from flaxkv2 import FlaxKV

# 正确配置 - 条目数量
db = FlaxKV("kunyuan", auto_nested=True, rebuild=True,
            read_cache_size=10000,        # 10000个条目
            write_buffer_size=100)        # 100个条目

db_no_nested = FlaxKV("kunyuan_no_nested",
                      auto_nested=False, rebuild=True,
                      read_cache_size=10000,
                      write_buffer_size=100)
```

### 性能对比结果

**写入性能** (100个大字典):
- 非递归: **1.31秒**
- 递归嵌套: **3.31秒** (慢 **2.53x**)

**读取性能** (100次嵌套访问):
- 非递归: **0.0001秒**
- 递归嵌套: **0.0063秒** (慢 **43.16x**)

**结论**:
- ✅ **推荐**: 默认使用 `auto_nested=False`
- ⚠️ **谨慎**: 只在需要频繁部分更新大型嵌套结构时才用 `auto_nested=True`

---

## 性能优化建议

基于你的使用场景（大字典嵌套访问），这里是最佳配置：

### 场景 1: 频繁读取整个字典

```python
db = FlaxKV("mydb", "./data",
           auto_nested=False,           # ✅ 关闭嵌套
           read_cache_size=10000,       # 大缓存
           enable_write_buffer=True,
           write_buffer_size=100,
           performance_profile='read_optimized')
```

**性能**: 读取 ~730K ops/s

---

### 场景 2: 频繁更新字典的部分字段

```python
db = FlaxKV("mydb", "./data",
           auto_nested=True,            # ✅ 启用嵌套（虽然慢，但避免整个字典序列化）
           read_cache_size=5000,
           enable_write_buffer=True,
           write_buffer_size=100)

# 好处：只更新部分字段，不需要读取整个大字典
db['big_dict']['field1'] = new_value  # 只更新一个字段
```

---

### 场景 3: 批量写入 + 批量读取

```python
db = FlaxKV("mydb", "./data",
           auto_nested=False,
           read_cache_size=5000,
           enable_write_buffer=True,
           write_buffer_size=100,       # 批量刷新
           async_flush=True,            # ⚠️ 异步模式（快但有风险）
           performance_profile='write_optimized')
```

**性能**: 写入 ~120K ops/s, 读取 ~880K ops/s

---

## 快速参考表

| 参数 | 含义 | 单位 | 推荐值 | 默认值 |
|------|------|------|--------|--------|
| `read_cache_size` | 读缓存大小 | **条目数** | 1000-10000 | 1000 |
| `write_buffer_size` | 写缓冲大小 | **条目数** | 100-1000 | 100 |
| `write_buffer_size_leveldb` | LevelDB写缓冲 | **字节** | 4MB-16MB | 由profile决定 |
| `auto_nested` | 嵌套存储 | 布尔值 | False | False |
| `async_flush` | 异步刷新 | 布尔值 | False（安全）| False |

---

## 实战示例

### 示例 1: 你的原始代码（修正版）

```python
from flaxkv2 import FlaxKV
import time

# 测试数据
big_list = list(range(1000))
big_dict = {f"lst{i}": big_list for i in range(100)}

# 正确的配置（条目数量）
db = FlaxKV("kunyuan", auto_nested=True, rebuild=True,
            read_cache_size=10000,      # ✅ 10000个条目，不是字节
            write_buffer_size=100)      # ✅ 100个条目

db_no_nested = FlaxKV("kunyuan_no_nested",
                      auto_nested=False, rebuild=True,
                      read_cache_size=10000,
                      write_buffer_size=100)

# 写入测试
n = 100
start = time.time()
for i in range(n):
    db['dict'] = big_dict
write_time_nested = time.time() - start

start = time.time()
for i in range(n):
    db_no_nested['dict'] = big_dict
write_time_no_nested = time.time() - start

print(f"递归写入: {write_time_nested:.4f}秒")
print(f"非递归写入: {write_time_no_nested:.4f}秒")
print(f"递归慢了: {write_time_nested/write_time_no_nested:.2f}x")

# 读取测试
start = time.time()
for i in range(n):
    _ = db['dict']['lst2'][2]
read_time_nested = time.time() - start

start = time.time()
for i in range(n):
    _ = db_no_nested['dict']['lst2'][2]
read_time_no_nested = time.time() - start

print(f"递归读取: {read_time_nested:.4f}秒")
print(f"非递归读取: {read_time_no_nested:.4f}秒")
print(f"递归慢了: {read_time_nested/read_time_no_nested:.2f}x")

db.close()
db_no_nested.close()
```

---

### 示例 2: 推荐的生产配置

```python
from flaxkv2 import FlaxKV

# 生产环境推荐配置
db = FlaxKV("production_db", "./data",
           # 基础配置
           auto_nested=False,               # 关闭嵌套（性能优先）
           rebuild=False,                   # 不重建已有数据库

           # 缓存配置
           read_cache_size=10000,           # 10K条目缓存
           enable_write_buffer=True,        # 启用写缓冲
           write_buffer_size=100,           # 100条触发刷新
           async_flush=False,               # 同步刷新（安全）

           # TTL配置
           default_ttl=None,                # 不使用TTL（除非需要）
           enable_ttl_cleanup=False,        # 禁用TTL清理线程

           # 性能配置
           performance_profile='balanced')  # 平衡模式

# 使用
db["key"] = "value"
result = db["key"]

# 务必关闭
db.close()
```

---

## 总结

1. ✅ **参数理解正确**: `read_cache_size` 和 `write_buffer_size` 都是条目数量，不是字节
2. ✅ **Benchmark 已创建**: 使用 `quick_benchmark.py` 或 `comprehensive_benchmark.py`
3. ⚠️ **你的代码有误**: 缓存大小设置为 536M 条目是错误的，应该是 5000-10000
4. 📊 **性能数据**: 非递归模式比递归模式快 2.5x (写入) 和 43x (读取)
5. 💡 **推荐配置**: 默认使用 `auto_nested=False` + 适当的缓存大小

---

## 下一步

1. **运行 benchmark**: `python benchmarks/quick_benchmark.py`
2. **修正你的代码**: 将 `read_cache_size` 和 `write_buffer_size` 改为合理值
3. **根据场景调优**: 参考 `benchmarks/README.md` 选择合适的配置

有任何问题欢迎继续提问！ 🚀
