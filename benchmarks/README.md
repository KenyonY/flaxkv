# FlaxKV2 性能测试套件

本目录包含 FlaxKV2 的性能测试工具，用于评估不同配置下的性能表现。

## 测试脚本

### 1. `quick_benchmark.py` - 快速性能测试

**用途**: 快速评估主要性能指标，适合日常开发和CI/CD

**运行时间**: ~5-10秒

**测试内容**:
- 基础缓存配置对比（无缓存、只读缓存、读写缓存）
- 嵌套结构性能（auto_nested vs 非递归）
- 不同数据类型性能（字符串、整数、列表、NumPy、Pandas）

**使用方法**:
```bash
python benchmarks/quick_benchmark.py
```

**典型输出**:
```
写入性能排名:
配置                        吞吐量             相对提升
--------------------------------------------------
读缓存+异步写缓冲                     121874 ops/s    5.09x
读缓存+同步写缓冲                      87566 ops/s    3.66x
无缓存                            39668 ops/s    1.66x
只读缓存                           23927 ops/s    1.00x

读取性能排名:
配置                        吞吐量             相对提升
--------------------------------------------------
读缓存+同步写缓冲                     785274 ops/s    2.73x
读缓存+异步写缓冲                     769823 ops/s    2.68x
只读缓存                          441738 ops/s    1.54x
无缓存                           287632 ops/s    1.00x
```

---

### 2. `comprehensive_benchmark.py` - 综合性能测试

**用途**: 全面评估所有功能的性能，适合发布前和性能调优

**运行时间**: ~30-60秒（取决于数据量）

**测试内容**:
- ✅ 缓存配置对比（5种配置）
- ✅ 基础操作（写入、顺序读取、随机读取、更新、删除、遍历）
- ✅ 嵌套结构性能
- ✅ 数据类型性能（字符串、整数、列表、字典、NumPy、Pandas）
- ✅ 并发性能（多线程读写）
- ✅ TTL性能影响
- ✅ 内存使用统计
- ✅ 缓存命中率统计

**使用方法**:
```bash
# 默认配置（5000条数据）
python benchmarks/comprehensive_benchmark.py

# 修改数据量
# 编辑 comprehensive_benchmark.py 第549行：
# benchmark = FlaxKVBenchmark(num_operations=10000)
```

**典型输出**:
```
================================================================================
=================================== 缓存配置对比测试 ===================================
================================================================================

测试: 无缓存 (RawLevelDBDict)
--------------------------------------------------------------------------------
  ✓ 写入 5000 条: 0.1661秒 (30104 ops/s)
  ✓ 顺序读取 5000 条: 0.0325秒 (153620 ops/s)
  ✓ 随机读取 5000 条: 0.0360秒 (138856 ops/s)
  ✓ 更新 1000 条: 0.0266秒
  ✓ 遍历 5000 条: 0.0442秒
  ✓ 删除 1000 条: 0.0110秒
  ✓ 内存使用: 99.26 MB

测试: 只读缓存 (5000条)
--------------------------------------------------------------------------------
  ✓ 写入 5000 条: 0.2538秒 (19697 ops/s)
  ✓ 顺序读取 5000 条: 0.0068秒 (732783 ops/s)
  ✓ 随机读取 5000 条: 0.0055秒 (902583 ops/s)
  ...
```

---

### 3. 其他现有 Benchmark

- `unified_cache_benchmark.py` - 专注于统一缓存的性能测试
- `comprehensive_comparison.py` - FlaxKV v1 vs v2 对比
- `2_vs_1.py` - 简化版 v1 vs v2 对比
- `cache_configs_benchmark.py` - 缓存配置详细测试
- `profile_write_path.py` - 写入路径性能分析

## 关键性能发现

根据测试结果，我们发现：

### 1. 缓存配置影响

**写入性能**:
- 🥇 读缓存+同步写缓冲: **65,717 ops/s** (基准的 3.3x)
- 🥈 读缓存+异步写缓冲: **63,289 ops/s** (基准的 3.2x)
- 🥉 无缓存: **30,104 ops/s** (基准的 1.5x)
- ⚠️ 只读缓存: **19,697 ops/s** (基准)

**读取性能**:
- 🥇 读缓存+异步写缓冲: **881,156 ops/s** (基准的 5.7x)
- 🥈 只读缓存: **732,783 ops/s** (基准的 4.8x)
- 🥉 读缓存+同步写缓冲: **604,767 ops/s** (基准的 3.9x)
- ⚠️ 无缓存: **153,620 ops/s** (基准)

**随机读取性能**:
- 🥇 只读缓存: **902,583 ops/s** (基准的 6.5x)
- 🥈 大缓存: **816,934 ops/s** (基准的 5.9x)
- 🥉 读缓存+同步写缓冲: **670,810 ops/s** (基准的 4.8x)
- ⚠️ 无缓存: **138,856 ops/s** (基准)

### 2. 嵌套结构性能

**非递归 vs 递归嵌套 (auto_nested=True)**:

- 写入性能: 递归比非递归**慢 2.53x**
- 读取性能: 递归比非递归**慢 43.16x**

**结论**:
- ✅ **推荐**: 默认使用非递归模式 (`auto_nested=False`)
- ⚠️ **谨慎使用**: 只在需要频繁部分更新大型嵌套结构时才启用 `auto_nested=True`

### 3. 数据类型性能

不同数据类型的写入吞吐量（带缓存）:

- 整数: **65,941 ops/s** 🥇
- 字符串: **59,639 ops/s** 🥈
- 字典: **14,554 ops/s**
- 列表: **13,115 ops/s**
- Pandas DataFrame: **3,401 ops/s**
- NumPy 数组: **1,084 ops/s**

**结论**: 简单类型（整数、字符串）性能最好，复杂对象（NumPy、Pandas）需要更多序列化时间。

### 4. 并发性能

4线程并发测试:
- 并发写入: **38,931 ops/s**
- 并发读取: **131,284 ops/s**

**结论**: FlaxKV2 的锁机制在多线程场景下表现良好，读取性能优于写入。

### 5. TTL 性能开销

- 无TTL: **16,766 ops/s**
- 带TTL: **17,170 ops/s**

**TTL开销**: **-2.4%** (几乎无开销！)

**结论**: TTL功能的开销非常小，可以放心使用。

## 性能调优建议

### 场景 1: 读密集型应用（API缓存、配置读取）

**推荐配置**:
```python
db = FlaxKV("mydb", "./data",
           read_cache_size=10000,        # 大读缓存
           enable_write_buffer=False,     # 禁用写缓冲
           performance_profile='read_optimized')
```

**预期性能**: ~700K-900K ops/s 读取吞吐量

---

### 场景 2: 写密集型应用（日志、批量导入）

**推荐配置**:
```python
db = FlaxKV("mydb", "./data",
           read_cache_size=5000,
           enable_write_buffer=True,
           write_buffer_size=100,
           async_flush=False,              # 同步flush更安全
           performance_profile='write_optimized')
```

**预期性能**: ~65K-90K ops/s 写入吞吐量

---

### 场景 3: 读写均衡（通用应用）

**推荐配置**:
```python
db = FlaxKV("mydb", "./data",
           read_cache_size=5000,
           enable_write_buffer=True,
           write_buffer_size=100,
           async_flush=False,
           performance_profile='balanced')
```

**预期性能**:
- 写入: ~65K ops/s
- 读取: ~600K ops/s

---

### 场景 4: 极致性能（可容忍数据丢失）

**推荐配置**:
```python
db = FlaxKV("mydb", "./data",
           read_cache_size=10000,
           enable_write_buffer=True,
           write_buffer_size=100,
           async_flush=True,               # ⚠️ 异步flush，崩溃可能丢数据
           performance_profile='write_optimized')
```

**预期性能**:
- 写入: ~120K ops/s (极快！)
- 读取: ~880K ops/s

⚠️ **警告**: `async_flush=True` 会在进程崩溃时丢失未刷新的数据，仅在可容忍数据丢失的场景使用！

---

## 参数说明

根据你的问题，这里是关键参数的含义：

### `read_cache_size`
- **含义**: 读缓存的**条目数量**（不是字节大小）
- **默认值**: 1000
- **推荐值**: 1000-10000（根据工作负载调整）
- **作用**: 缓存最近读取的键值对，避免重复的 LevelDB 查询和反序列化

### `write_buffer_size`
- **含义**: 写缓冲区的**条目数量**（不是字节大小）
- **默认值**: 100
- **推荐值**: 100-1000
- **作用**: 达到此数量的脏条目时触发批量刷新到 LevelDB

⚠️ **注意**: 这两个参数都是**条目数量**，不是内存大小！

### `write_buffer_size_leveldb`（不要混淆！）
- **含义**: LevelDB 底层的写缓冲区大小（**字节**）
- **默认值**: 由 performance_profile 决定（通常是 4MB-16MB）
- **作用**: 传递给 LevelDB 内部使用的写缓冲区

这可能就是你记得的"不是内存大小"的参数 😊

## 运行自定义测试

你可以基于 `quick_benchmark.py` 或 `comprehensive_benchmark.py` 修改来创建自己的测试：

```python
from flaxkv2 import FlaxKV
import time

# 你的自定义配置
db = FlaxKV("mydb", "./data",
           read_cache_size=10000,
           write_buffer_size=200)

# 你的自定义测试
n = 10000
start = time.time()
for i in range(n):
    db[f"key_{i}"] = f"value_{i}"
write_time = time.time() - start

print(f"写入 {n} 条: {write_time:.4f}秒 ({n/write_time:.0f} ops/s)")

db.close()
```

## 性能分析工具

如果需要更详细的性能分析，可以使用：

```bash
# CPU profiling
python -m cProfile -o output.prof benchmarks/quick_benchmark.py
python -m pstats output.prof

# 或使用 line_profiler
kernprof -l -v benchmarks/quick_benchmark.py
```

## 贡献

欢迎添加新的 benchmark 测试场景！请确保：
1. 测试结果可重现
2. 包含清晰的文档说明
3. 运行时间合理（< 5分钟）
4. 清理测试数据

---

最后更新: 2025-11-12
