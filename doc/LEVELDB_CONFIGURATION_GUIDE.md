# LevelDB 配置参数详解

本文档详细说明 LevelDB (plyvel) 的各个配置参数的作用、适用场景、权衡以及推荐值。

---

## 目录
1. [读性能相关参数](#读性能相关参数)
2. [写性能相关参数](#写性能相关参数)
3. [存储优化参数](#存储优化参数)
4. [资源管理参数](#资源管理参数)
5. [场景化配置方案](#场景化配置方案)

---

## 读性能相关参数

### 1. `lru_cache_size` (最关键的读优化参数)

**作用机制**：
- LevelDB 的 **LRU (Least Recently Used) 块缓存**
- 缓存从磁盘读取的数据块在内存中
- 后续读取相同数据时直接从内存返回，无需磁盘I/O

**数据流**：
```
读取请求
    │
    ├─→ 检查 LRU Cache ─→ 命中 ─→ 直接返回 (快速路径，微秒级)
    │
    └─→ 未命中 ─→ 读磁盘 ─→ 加入Cache ─→ 返回 (慢速路径，毫秒级)
```

**性能影响**：
- **极大影响读性能**！未配置时默认只有 8MB
- 缓存命中率每提升10%，读性能提升约20-30%
- 典型场景：256MB缓存可使热数据读取速度提升 **3-5倍**

**内存占用**：
- 直接占用配置的内存大小
- 例如：`lru_cache_size=256*1024*1024` = 256MB常驻内存

**推荐值**：

| 场景 | 推荐值 | 说明 |
|-----|-------|------|
| **默认/平衡** | 256 MB | 适合大多数场景 |
| **读密集型** | 512 MB - 1 GB | 缓存更多热数据 |
| **写密集型** | 128 MB | 节省内存给写缓冲 |
| **内存受限** | 64 MB | 最低推荐值 |
| **大数据集** | 1-2 GB | 数据集>100GB时 |

**如何选择**：
```python
# 经验公式：
# lru_cache_size = min(
#     可用内存 * 0.3,              # 不超过可用内存的30%
#     热数据集大小 * 1.5,          # 覆盖1.5倍热数据
#     1 GB                         # 通常不超过1GB（除非数据集极大）
# )

# 示例计算
total_memory = 16 * 1024 * 1024 * 1024  # 16GB系统
hot_data_size = 500 * 1024 * 1024       # 500MB热数据

recommended_cache = min(
    total_memory * 0.3,      # 4.8GB
    hot_data_size * 1.5,     # 750MB
    1024 * 1024 * 1024       # 1GB
)
# 结果: 750MB
```

**监控指标**：
```python
# 可以通过以下方式监控缓存效率
# (需要实现统计功能)
cache_hits / (cache_hits + cache_misses) * 100  # 缓存命中率
# 目标: > 85% 为优秀
```

---

### 2. `bloom_filter_bits`

**作用机制**：
- **布隆过滤器**：一种概率型数据结构
- 用于快速判断一个key是否**不存在**
- 如果布隆过滤器说"不存在"，则一定不存在（无假阴性）
- 如果说"可能存在"，则需要实际查询确认（有假阳性）

**工作流程**：
```
查询 key="user:9999"
    │
    ├─→ 布隆过滤器检查 ─→ "不存在" ─→ 直接返回 KeyError (节省磁盘I/O)
    │
    └─→ 布隆过滤器检查 ─→ "可能存在" ─→ 实际查询磁盘
                                         │
                                         ├─→ 真的存在 ─→ 返回值
                                         └─→ 假阳性 ─→ KeyError
```

**性能影响**：
- 对于**不存在的key**查询性能提升显著（减少无效磁盘查找）
- 对于**存在的key**几乎无影响（只增加一次内存检查）
- 典型提升：不存在key的查询速度提升 **50-90%**

**内存占用**：
```python
# 内存占用 = 数据库总key数量 * bloom_filter_bits / 8 字节

# 示例：
# 100万个key，bloom_filter_bits=10
memory_usage = 1_000_000 * 10 / 8 / 1024 / 1024  # ≈ 1.2 MB

# 1000万个key，bloom_filter_bits=10
memory_usage = 10_000_000 * 10 / 8 / 1024 / 1024  # ≈ 12 MB
```

**参数含义**：

| bloom_filter_bits | 假阳性率 | 内存/百万key | 适用场景 |
|------------------|---------|-------------|---------|
| 0 | - | 0 MB (禁用) | 极少查询不存在的key |
| 8 | ~2% | 1 MB | 平衡 |
| 10 | ~1% | 1.2 MB | **推荐默认值** |
| 12 | ~0.5% | 1.5 MB | 对假阳性敏感 |
| 16 | ~0.1% | 2 MB | 极致优化 |

**推荐值**：

| 场景 | 推荐值 | 说明 |
|-----|-------|------|
| **默认** | 10 | 1%假阳性，性价比最高 |
| **频繁查询不存在key** | 12-16 | 降低假阳性 |
| **内存极度受限** | 8 | 最低推荐值 |
| **只查询已知存在key** | 0 | 禁用（节省内存） |

**权衡**：
- ✅ 优点：内存占用极小，性能提升明显
- ⚠️ 缺点：存在假阳性（但影响很小）
- 📊 建议：**几乎所有场景都应该启用**（至少设为10）

---

### 3. `block_size`

**作用机制**：
- LevelDB 将数据分成固定大小的**块 (block)**
- 每次读取时，整个块被加载到内存（即使只需要1个key）
- 块是缓存的基本单位

**数据组织**：
```
SSTable 文件结构：
┌─────────────────────────────────────┐
│ Block 1: [key1, key2, ..., key10]  │ ← 16KB
├─────────────────────────────────────┤
│ Block 2: [key11, key12, ..., key20]│ ← 16KB
├─────────────────────────────────────┤
│ Block 3: [key21, key22, ..., key30]│ ← 16KB
└─────────────────────────────────────┘

读取 key15 → 整个 Block 2 (16KB) 被加载到缓存
```

**性能影响**：

| block_size | 优点 | 缺点 | 适用场景 |
|-----------|------|------|---------|
| 4 KB (小) | • 点查询快<br>• 减少无关数据读取 | • 范围查询慢<br>• 索引开销大 | 随机点查询 |
| 16 KB (中) | • **平衡** | - | **通用场景** |
| 64 KB (大) | • 范围查询快<br>• 扫描高效 | • 点查询慢<br>• 缓存利用率低 | 顺序扫描 |

**推荐值**：

| 场景 | 推荐值 | 说明 |
|-----|-------|------|
| **默认** | 16 KB | 平衡点查询和范围查询 |
| **点查询为主** | 8-16 KB | 减少无关数据加载 |
| **范围扫描为主** | 32-64 KB | 减少I/O次数 |
| **小对象多** | 4-8 KB | 提高缓存效率 |
| **大对象多** | 32 KB | 减少块边界跨越 |

**如何选择**：
```python
# 经验公式：
# block_size ≈ 平均value大小 * 10-20

# 示例1：存储用户信息（平均500字节）
block_size = 500 * 10 = 5KB  # 取 4KB 或 8KB

# 示例2：存储图片元数据（平均2KB）
block_size = 2 * 1024 * 15 = 30KB  # 取 32KB

# 示例3：混合场景（FlaxKV通用）
block_size = 16 * 1024  # 16KB (推荐)
```

---

## 写性能相关参数

### 4. `write_buffer_size`

**作用机制**：
- LevelDB 的 **MemTable** 大小
- 写入先进入内存中的MemTable
- MemTable满后才刷入磁盘（生成SSTable文件）

**写入流程**：
```
写入操作
    │
    ├─→ 写入 MemTable (内存) ─→ 立即返回 (微秒级)
    │         │
    │         ├─→ MemTable < write_buffer_size ─→ 继续累积
    │         │
    │         └─→ MemTable >= write_buffer_size
    │                   │
    │                   └─→ 刷入磁盘 (生成SSTable)
    │                        │
    │                        └─→ 创建新 MemTable
    │
    └─→ (后台异步刷盘)
```

**性能影响**：

| write_buffer_size | 写性能 | 磁盘I/O | 内存占用 | 恢复时间 |
|------------------|--------|---------|---------|---------|
| 4 MB (小) | 低 | 频繁 | 低 | 快 |
| 64 MB (默认) | 中 | 中 | 中 | 中 |
| 128 MB (推荐) | **高** | 少 | 高 | 较慢 |
| 256 MB (大) | 极高 | 极少 | 极高 | 慢 |

**推荐值**：

| 场景 | 推荐值 | 说明 |
|-----|-------|------|
| **默认** | 128 MB | 平衡性能和内存 |
| **写密集型** | 256 MB | 最大化写性能 |
| **读密集型** | 64 MB | 节省内存给读缓存 |
| **内存受限** | 32 MB | 最低推荐值 |
| **嵌入式设备** | 8-16 MB | 极度节省内存 |

**权衡**：
- ✅ 越大 → 写性能越高，磁盘I/O越少
- ⚠️ 越大 → 内存占用越高，崩溃恢复越慢
- ⚠️ 越大 → 压缩操作延迟越大

**崩溃恢复影响**：
```python
# write_buffer_size = 64 MB
# 崩溃时最多丢失: 64MB 数据（如果没有WAL）
# 恢复时间: ~1-2秒

# write_buffer_size = 256 MB
# 崩溃时最多丢失: 256MB 数据
# 恢复时间: ~5-10秒

# 注意：LevelDB默认有WAL (Write-Ahead Log)，通常不会丢数据
```

---

## 资源管理参数

### 5. `max_open_files`

**作用机制**：
- LevelDB 使用多个SSTable文件存储数据
- `max_open_files` 限制同时打开的文件描述符数量
- 文件描述符缓存，避免频繁 open/close

**文件结构**：
```
数据库目录：
├── CURRENT
├── LOCK
├── LOG
├── MANIFEST-000001
├── 000003.log          ← MemTable对应的WAL
├── 000004.ldb          ← SSTable文件 (level 0)
├── 000005.ldb          ← SSTable文件 (level 0)
├── 000006.ldb          ← SSTable文件 (level 1)
├── ...
└── 000100.ldb          ← SSTable文件 (level N)

随着数据增长，SSTable文件数量增加
max_open_files 控制同时打开的文件数
```

**性能影响**：

| max_open_files | 优点 | 缺点 | 适用场景 |
|---------------|------|------|---------|
| 100 (小) | 节省FD资源 | 频繁open/close | 小数据库 |
| 500 (推荐) | **平衡** | - | **通用** |
| 1000 (大) | 减少open开销 | 占用FD资源 | 大数据库 |

**推荐值**：

| 场景 | 推荐值 | 说明 |
|-----|-------|------|
| **默认** | 500 | 平衡 |
| **大数据库** (>10GB) | 1000 | 减少文件打开开销 |
| **小数据库** (<1GB) | 100-300 | 节省资源 |
| **多数据库实例** | 300 | 避免FD耗尽 |

**系统限制检查**：
```bash
# 检查系统限制
ulimit -n  # 查看当前进程文件描述符限制

# 典型值：
# macOS: 256-4096
# Linux: 1024-65536

# 如果需要调整：
ulimit -n 65536  # 临时调整
```

**计算公式**：
```python
# max_open_files = min(
#     数据库中SSTable文件数 * 1.5,  # 预留余量
#     系统限制 / 同时运行的数据库实例数,
#     1000  # 通常不超过1000
# )
```

---

### 6. `compression`

**作用机制**：
- 数据写入SSTable文件前进行压缩
- 减少磁盘占用和I/O时间
- 读取时自动解压

**支持的压缩算法**：

| 算法 | 压缩率 | 压缩速度 | 解压速度 | CPU占用 |
|-----|--------|---------|---------|--------|
| `'snappy'` | 中 (2-3x) | **快** | **极快** | 低 |
| `'zlib'` | 高 (3-5x) | 慢 | 慢 | 高 |
| `None` | 无 | 极快 | 极快 | 极低 |

**性能影响**：

```python
# 场景1：SSD + Snappy (推荐)
读取速度: +++  (减少I/O时间 > 解压开销)
写入速度: ++   (减少I/O时间 > 压缩开销)
磁盘占用: 节省50-70%

# 场景2：HDD + Snappy (推荐)
读取速度: ++++  (机械盘I/O慢，压缩收益大)
写入速度: +++
磁盘占用: 节省50-70%

# 场景3：极致CPU (可考虑zlib)
读取速度: +
写入速度: +
磁盘占用: 节省60-80%

# 场景4：无压缩 (不推荐)
读取速度: 0 (基准)
写入速度: 0 (基准)
磁盘占用: 100%
```

**推荐值**：

| 场景 | 推荐值 | 说明 |
|-----|-------|------|
| **默认** | `'snappy'` | **几乎所有场景的最佳选择** |
| **极致压缩比** | `'zlib'` | 愿意牺牲CPU |
| **极度CPU敏感** | `None` | 仅在CPU是绝对瓶颈时 |

**决策树**：
```
是否有充足的磁盘空间？
    │
    ├─ 否 → 使用 'zlib' (高压缩率)
    │
    └─ 是 → CPU是瓶颈吗？
            │
            ├─ 是 → 使用 None (无压缩)
            │
            └─ 否 → 使用 'snappy' (推荐)
```

---

## 场景化配置方案

### 方案1：通用平衡配置（推荐默认）

```python
BALANCED_CONFIG = {
    # 读优化
    'lru_cache_size': 256 * 1024 * 1024,    # 256 MB
    'bloom_filter_bits': 10,                 # 1% 假阳性
    'block_size': 16 * 1024,                 # 16 KB

    # 写优化
    'write_buffer_size': 128 * 1024 * 1024, # 128 MB

    # 资源管理
    'max_open_files': 500,
    'compression': 'snappy',

    # 其他
    'create_if_missing': True,
}

# 适用场景：
# - 混合读写工作负载
# - 中等规模数据库 (1-100GB)
# - 16GB+ 内存服务器
# - SSD 存储
```

### 方案2：读密集型优化

```python
READ_OPTIMIZED_CONFIG = {
    # 最大化读性能
    'lru_cache_size': 512 * 1024 * 1024,    # 512 MB (加大!)
    'bloom_filter_bits': 12,                 # 0.5% 假阳性 (更精确)
    'block_size': 16 * 1024,                 # 16 KB

    # 适度写性能
    'write_buffer_size': 64 * 1024 * 1024,  # 64 MB (减小)

    # 资源管理
    'max_open_files': 1000,                  # 支持更多文件
    'compression': 'snappy',

    'create_if_missing': True,
}

# 适用场景：
# - 90%+ 读操作
# - 缓存服务、API查询
# - 32GB+ 内存服务器
# - SSD 存储

# 预期性能：
# - 热数据读取: 3-5倍提升
# - 冷数据读取: 1.5-2倍提升
# - 写入性能: 与默认持平
```

### 方案3：写密集型优化

```python
WRITE_OPTIMIZED_CONFIG = {
    # 适度读性能
    'lru_cache_size': 128 * 1024 * 1024,     # 128 MB (减小)
    'bloom_filter_bits': 10,                  # 10 bits
    'block_size': 32 * 1024,                  # 32 KB (增大，减少块数)

    # 最大化写性能
    'write_buffer_size': 256 * 1024 * 1024,  # 256 MB (加大!)

    # 资源管理
    'max_open_files': 500,
    'compression': 'snappy',

    'create_if_missing': True,
}

# 适用场景：
# - 70%+ 写操作
# - 日志收集、监控数据
# - 批量数据导入
# - 16GB+ 内存服务器

# 预期性能：
# - 批量写入: 50-100%提升
# - 单次写入: 20-30%提升
# - 读取性能: 略低于默认
```

### 方案4：内存受限配置

```python
MEMORY_CONSTRAINED_CONFIG = {
    # 最小化内存占用
    'lru_cache_size': 64 * 1024 * 1024,      # 64 MB (最小推荐)
    'bloom_filter_bits': 8,                   # 8 bits (节省内存)
    'block_size': 8 * 1024,                   # 8 KB

    'write_buffer_size': 32 * 1024 * 1024,   # 32 MB

    # 资源管理
    'max_open_files': 200,                    # 减少FD占用
    'compression': 'snappy',                  # 保持压缩

    'create_if_missing': True,
}

# 适用场景：
# - 嵌入式设备
# - 容器化环境 (内存限制)
# - 多租户共享服务器
# - < 4GB 可用内存

# 总内存占用：
# - LRU cache: 64 MB
# - Write buffer: 32 MB
# - Bloom filter: ~1 MB (百万key)
# - 其他开销: ~20 MB
# 合计: ~120 MB
```

### 方案5：大数据库配置

```python
LARGE_DATABASE_CONFIG = {
    # 大容量缓存
    'lru_cache_size': 1024 * 1024 * 1024,    # 1 GB
    'bloom_filter_bits': 12,                  # 精确的布隆过滤器
    'block_size': 32 * 1024,                  # 32 KB

    # 大写缓冲
    'write_buffer_size': 256 * 1024 * 1024,  # 256 MB

    # 资源管理
    'max_open_files': 2000,                   # 支持大量文件
    'compression': 'snappy',

    'create_if_missing': True,
}

# 适用场景：
# - 数据库大小 > 100 GB
# - 热数据集 > 1 GB
# - 64GB+ 内存服务器
# - 企业级SSD

# 注意事项：
# - 需要调整系统 ulimit -n
# - 监控内存使用
# - 考虑分片策略
```

### 方案6：机器学习/科学计算

```python
ML_WORKLOAD_CONFIG = {
    # 针对大数组/模型参数
    'lru_cache_size': 512 * 1024 * 1024,     # 512 MB
    'bloom_filter_bits': 10,
    'block_size': 64 * 1024,                  # 64 KB (大对象)

    'write_buffer_size': 256 * 1024 * 1024,  # 256 MB

    # 资源管理
    'max_open_files': 500,
    'compression': 'snappy',  # NumPy数组压缩效果好

    'create_if_missing': True,
}

# 适用场景：
# - 存储 NumPy 数组、模型权重
# - 大对象为主 (>100KB)
# - 读写比 6:4

# 特点：
# - 大 block_size 适配大对象
# - Snappy对数值数组压缩比高
```

---

## 动态配置实现

### 实现可配置的LevelDB选项

```python
# flaxkv2/core/raw_leveldb_dict.py

class RawLevelDBDict:
    # 预定义配置文件
    PERFORMANCE_PROFILES = {
        'balanced': {
            'lru_cache_size': 256 * 1024 * 1024,
            'bloom_filter_bits': 10,
            'block_size': 16 * 1024,
            'write_buffer_size': 128 * 1024 * 1024,
            'max_open_files': 500,
        },
        'read_optimized': {
            'lru_cache_size': 512 * 1024 * 1024,
            'bloom_filter_bits': 12,
            'block_size': 16 * 1024,
            'write_buffer_size': 64 * 1024 * 1024,
            'max_open_files': 1000,
        },
        'write_optimized': {
            'lru_cache_size': 128 * 1024 * 1024,
            'bloom_filter_bits': 10,
            'block_size': 32 * 1024,
            'write_buffer_size': 256 * 1024 * 1024,
            'max_open_files': 500,
        },
        'memory_constrained': {
            'lru_cache_size': 64 * 1024 * 1024,
            'bloom_filter_bits': 8,
            'block_size': 8 * 1024,
            'write_buffer_size': 32 * 1024 * 1024,
            'max_open_files': 200,
        },
    }

    def __init__(
        self,
        name: str,
        path: str = ".",
        rebuild: bool = False,
        create_if_missing: bool = True,
        raw: bool = False,
        default_ttl: Optional[int] = None,
        auto_nested: bool = False,
        # 新增：性能配置参数
        performance_profile: str = 'balanced',  # 使用预设配置
        # 或者手动指定每个参数（会覆盖profile）
        lru_cache_size: Optional[int] = None,
        bloom_filter_bits: Optional[int] = None,
        block_size: Optional[int] = None,
        write_buffer_size: Optional[int] = None,
        max_open_files: Optional[int] = None,
        compression: str = 'snappy',
        **kwargs
    ):
        """
        performance_profile: 预设配置名称
            - 'balanced': 通用平衡 (默认)
            - 'read_optimized': 读密集型
            - 'write_optimized': 写密集型
            - 'memory_constrained': 内存受限

        手动参数会覆盖profile中的设置
        """
        # 加载预设配置
        if performance_profile in self.PERFORMANCE_PROFILES:
            profile = self.PERFORMANCE_PROFILES[performance_profile].copy()
        else:
            raise ValueError(f"Unknown profile: {performance_profile}")

        # 手动参数覆盖预设
        if lru_cache_size is not None:
            profile['lru_cache_size'] = lru_cache_size
        if bloom_filter_bits is not None:
            profile['bloom_filter_bits'] = bloom_filter_bits
        if block_size is not None:
            profile['block_size'] = block_size
        if write_buffer_size is not None:
            profile['write_buffer_size'] = write_buffer_size
        if max_open_files is not None:
            profile['max_open_files'] = max_open_files

        # 构建最终配置
        self._leveldb_options = {
            'create_if_missing': create_if_missing,
            'compression': compression,
            **profile  # 合并profile
        }

        # ... 其余初始化代码 ...
```

### 使用示例

```python
# 方式1：使用预设配置
db = FlaxKV("mydb", "./data", performance_profile='read_optimized')

# 方式2：在预设基础上微调
db = FlaxKV("mydb", "./data",
            performance_profile='balanced',
            lru_cache_size=512*1024*1024)  # 只覆盖缓存大小

# 方式3：完全自定义
db = FlaxKV("mydb", "./data",
            performance_profile='balanced',  # 作为基础
            lru_cache_size=300*1024*1024,
            bloom_filter_bits=12,
            block_size=20*1024)
```

---

## 监控和调优

### 1. 添加性能统计

```python
class RawLevelDBDict:
    def __init__(self, ...):
        # 统计信息
        self._stats = {
            'total_reads': 0,
            'total_writes': 0,
            'cache_hits': 0,      # 需要底层支持
            'cache_misses': 0,
        }

    def get_stats(self) -> dict:
        """获取性能统计"""
        # 可以通过 plyvel 的 get_property() 获取底层统计
        # db.get_property(b'leveldb.stats')
        return {
            **self._stats,
            'cache_hit_rate': self._calculate_hit_rate(),
        }
```

### 2. 性能测试脚本

```python
# 测试不同配置的性能
def benchmark_config(config_name, config):
    db = FlaxKV("bench", "./data",
                performance_profile='balanced',
                **config)

    # 运行benchmark
    # ...

    return results
```

---

## 快速决策表

| 你的场景 | 推荐配置 | 关键参数 |
|---------|---------|---------|
| 不确定，刚开始 | `balanced` | 默认即可 |
| 缓存服务 | `read_optimized` | lru_cache↑ |
| 日志收集 | `write_optimized` | write_buffer↑ |
| 树莓派/嵌入式 | `memory_constrained` | 全部↓ |
| 机器学习模型存储 | 自定义 | block_size=64KB |
| 大数据库(>100GB) | 自定义 | lru_cache=1GB |

---

## 下一步建议

1. **先使用 `balanced` 配置运行你的workload**
2. **监控实际性能瓶颈**（CPU？内存？磁盘I/O？）
3. **根据瓶颈调整配置**
4. **运行benchmark对比**
5. **逐步调优**

---

## 参考资源

- [LevelDB 官方文档](https://github.com/google/leveldb/blob/main/doc/index.md)
- [plyvel 文档](https://plyvel.readthedocs.io/)
- FlaxKV2 现有性能报告: `BUFFERED_VS_RAW_BENCHMARK_REPORT.md`
