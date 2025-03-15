请你查看本项目(flaxkv) 的doc.md  ，并重新设计出flaxkv2，以支持更加健壮的flaxkv。尤其是对doc.md中的未来开发计划需要作出良好的设计。先给出设计方案，然后逐步实现。（数据库后端引擎使用leveldb, 注意新版本的实现不必沿用flaxkv原有的方式，你无需查看目前的原始flaxkv的实现方案，只需查看doc.md中的相关功能，除了要实现前面重要基础的功能，还需要考虑doc中的 10. 开发计划和11 性能优化建议, ）
ps: 创建文件请使用文件编辑工具不要使用linux命令

# FlaxKV 技术文档

## 1. 项目概述

FlaxKV 是一个高性能的键值数据库系统，提供类似于 Python 字典的接口来操作持久化存储。它具有较高的读写性能，同时保证数据的持久性和并发安全，特别适合于机器学习等需要存储大量数据的应用场景。

### 1.1 主要特点

- **无阻塞设计**：用户进程不会因写操作而阻塞，同时保证读取的数据始终是最新的
- **易用性**：接口设计类似于 Python 字典，使用简单直观
- **缓冲写入**：数据先缓存再批量写入数据库，减少频繁写入的开销
- **高性能后端**：默认使用高性能的 LevelDB 作为后端存储
- **原子操作**：确保写操作的原子性，保障数据完整性
- **线程安全**：使用最少的锁确保并发访问安全，同时保持高性能
- **分布式支持**：支持远程数据库访问，可作为服务运行

## 2. 核心架构

FlaxKV 的整体架构由以下几个主要部分组成：

1. **对外接口层**：提供类似字典的使用接口
2. **数据管理层**：管理数据的读写、缓冲和提交
3. **序列化层**：处理数据的编码和解码
4. **存储层**：实际的数据库后端(LevelDB)
5. **远程服务层**：提供网络访问接口

## 3. 模块详解

### 3.1 核心模块 (core.py)

核心模块定义了基本的数据库字典接口和实现，提供与 Python 字典相似的操作方法。

#### 3.1.1 BaseDBDict 类

这是所有数据库字典实现的基类，定义了通用的接口和行为：

- **缓冲区管理**：维护一个内存缓冲区，延迟写入数据库
- **后台工作线程**：定期将缓冲区数据提交到数据库
- **自动资源释放**：注册 atexit 处理函数，确保程序退出时资源被释放
- **字典接口**：提供 `__getitem__`, `__setitem__`, `__delitem__` 等方法

关键配置参数：
- `MAX_BUFFER_SIZE`: 缓冲区最大条目数，默认 100
- `COMMIT_TIME_INTERVAL`: 自动提交间隔，默认 10 分钟

#### 3.1.2 LMDBDict 类 (已弃用，不需要再支持)

基于 LMDB 实现的字典接口，特点是高性能且支持事务操作。

#### 3.1.3 LevelDBDict 类

基于 LevelDB 实现的字典接口，作为默认的存储后端，提供高性能的键值存储。

#### 3.1.4 RemoteDBDict 类

远程数据库字典实现，通过 HTTP 与远程 FlaxKV 服务器通信。

### 3.2 数据库管理模块 (manager.py)

负责数据库的连接、事务管理和状态维护。

#### 3.2.1 DBManager 类

管理数据库连接和操作：
- 创建和维护数据库连接
- 处理事务操作
- 管理数据库视图
- 提供销毁和重建数据库的功能

#### 3.2.2 RemoteTransaction 类

实现与远程服务器的事务交互：
- 管理 HTTP 连接
- 实现批量操作
- 提供重试机制
- 处理连接错误和恢复

### 3.3 序列化模块 (pack.py)

负责数据的编码和解码，支持多种 Python 数据类型：

- **基础类型**：int, float, bool, str
- **容器类型**：list, dict
- **特殊类型**：numpy.ndarray, pandas.DataFrame

关键函数：
- `encode`: 将 Python 对象序列化为二进制数据
- `decode`: 将二进制数据反序列化为 Python 对象
- `decode_key`: 特定用于键的解码

特殊处理：
- 使用 msgpack 进行基础序列化
- 对 NumPy 数组进行专门优化
- 对不支持的复杂类型使用 pickle 作为后备

### 3.4 远程服务模块 (serve/)

#### 3.4.1 服务应用 (serve/app.py)

定义了 Web 服务器应用，基于 Litestar 框架实现 HTTP API。

#### 3.4.2 路由接口 (serve/route.py)

实现 HTTP API 的端点：
- 健康检查: `/healthz`
- 数据库操作: 
  - `/connect`: 连接数据库
  - `/disconnect`: 断开连接
  - `/get`, `/set`: 基本的读写操作
  - `/get_batch`, `/set_batch`: 批量读写
  - `/keys`, `/dict`: 获取键列表和全部数据
  - 流式接口: `*_stream` 支持大数据传输

#### 3.4.3 接口定义 (serve/interface.py)

定义了服务器与客户端之间的通信结构和协议。

### 3.5 日志模块 (log.py)

提供日志记录功能，基于 loguru 库实现：
- 支持不同级别的日志记录
- 可配置日志输出位置
- 提供格式化和过滤功能

### 3.6 辅助工具 (helper.py)

提供通用的辅助功能和工具函数。

## 4. 数据流程

### 4.1 写入流程

1. 调用 `__setitem__` 或相关方法
2. 数据添加到内存缓冲区 `_buffer_dict`
3. 当缓冲区达到阈值或定时器触发时，后台线程将数据写入数据库
4. 写入完成后，更新内存缓存

### 4.2 读取流程

1. 调用 `__getitem__` 或相关方法
2. 首先查找内存缓冲区
3. 如果未找到，再查找数据库
4. 将结果解码并返回

### 4.3 远程操作流程

1. 客户端通过 HTTP 请求发送操作
2. 服务器接收请求并验证
3. 服务器执行相应的数据库操作
4. 结果返回给客户端

## 5. 性能优化

- **批量操作**：缓冲写入减少数据库 I/O
- **延迟提交**：非阻塞设计提高响应速度
- **序列化优化**：专门处理 NumPy 数组等大型数据
- **并发控制**：细粒度锁减少竞争
- **连接池**：远程模式中重用连接

## 6. 使用限制

- 多进程环境下，由于延迟写入特性，一个进程可能无法实时读取另一个进程写入的数据
- 默认不支持 `Tuple` 和 `Set` 类型作为值，会被转换为 `List`
- 远程模式性能受网络影响

## 7. 命令行接口

FlaxKV 提供了命令行工具用于启动服务器：

```bash
flaxkv run --port 8000
```

支持的选项：
- `--port`: 指定服务器端口
- `--log`: 设置日志级别
- `--http2`: 启用 HTTP/2 支持

## 8. API 参考

### 8.1 FlaxKV 主接口

```python
FlaxKV(
    db_name: str,
    root_path_or_url: str = ".",
    backend='leveldb',
    rebuild=False,
    raw=False,
    cache=False,
    **kwargs,
)
```

参数：
- `db_name`: 数据库名称
- `root_path_or_url`: 数据库根路径或远程 URL
- `backend`: 后端类型，支持 'leveldb' 或 'lmdb'
- `rebuild`: 是否重建数据库
- `raw`: 是否使用原始模式（服务器专用）
- `cache`: 是否使用缓存模式

### 8.2 字典接口方法

所有常见的字典方法都可用：
- `db[key]`: 获取值
- `db[key] = value`: 设置值
- `del db[key]`: 删除键
- `key in db`: 检查键是否存在
- `db.get(key, default)`: 获取值，未找到返回默认值
- `db.pop(key, default)`: 弹出值，未找到返回默认值
- `db.keys()`: 获取所有键
- `db.values()`: 获取所有值
- `db.items()`: 获取所有键值对
- `db.update(dict)`: 批量更新
- `len(db)`: 获取条目数

### 8.3 特殊方法

- `db.write_immediately(write=True, block=False)`: 立即写入
- `db.wait_until_write_complete(timeout=None)`: 等待写入完成
- `db.close(write=True, wait=False)`: 关闭数据库
- `db.destroy()`: 销毁数据库
- `db.to_dict()`: 转换为普通字典
- `db.stat()`: 获取数据库统计信息

## 9. 最佳实践

1. **合理设置缓冲区大小**：根据应用场景调整 `MAX_BUFFER_SIZE`
2. **定期关闭**：长时间运行的应用应定期调用 `close()` 确保数据持久化
3. **异常处理**：捕获并处理可能的异常，尤其是远程模式下
4. **避免过大对象**：尽管支持大型数据，但建议将过大的对象分割存储
5. **善用批量操作**：使用 `update()` 代替多次 `__setitem__` 调用 

## 10. 未来开发计划

以下是FlaxKV项目接下来重点开发的方向：

### 10.1 高级功能扩展

#### 10.1.1 索引支持
- **二级索引**: 支持对非主键字段建立索引，提高查询效率
- **复合索引**: 支持多字段组合索引
- **索引更新机制**: 高效的索引维护算法，降低索引更新开销

#### 10.1.2 TTL(生存时间)机制
- **键过期策略**: 为键值对设置自动过期时间
- **惰性/主动清理**: 支持两种过期数据清理模式
- **更新重置**: 数据更新时自动刷新过期时间的选项

#### 10.1.3 增量备份与恢复
- **实时备份**: 写操作日志持久化，支持时间点恢复
- **增量同步**: 只传输变更数据，提高备份效率
- **压缩存储**: 备份数据的高效压缩算法
- **一键恢复**: 简化的数据恢复流程

### 10.2 分布式能力增强

#### 10.2.1 数据分片(Sharding)
- **一致性哈希**: 实现数据的均匀分布
- **虚拟节点**: 提高分片的平衡性和扩展性
- **动态重平衡**: 节点加入/离开时的数据迁移
- **分片感知客户端**: 智能路由请求到正确的分片

#### 10.2.2 复制与高可用
- **主从复制**: 支持一主多从的数据复制
- **读写分离**: 读请求分发到从节点，减轻主节点负担
- **自动故障转移**: 主节点故障时自动选举新主节点
- **数据一致性保证**: 提供不同级别的一致性选项

#### 10.2.3 分布式事务
- **两阶段提交**: 确保跨节点操作的原子性
- **乐观/悲观并发控制**: 支持不同的事务隔离级别
- **死锁检测与预防**: 自动识别和处理分布式死锁

#### 10.2.4 集群管理
- **成员发现**: 自动发现和注册节点
- **健康检查**: 持续监控节点状态
- **负载均衡**: 智能分配工作负载
- **集中式配置**: 统一管理集群配置

## 11. 性能优化建议

以下是针对FlaxKV核心实现的性能优化建议，按照优先级从高到低排列：

### 11.1 高优先级优化

#### 11.1.1 批量写入优化
当前在`_write_buffer_to_db`方法中对每个键值进行单独操作，可以改进为使用数据库后端的批量API：
```python
# 优化前
for key, value in buffer_dict_snapshot.items():
    key, value = self._encode_key(key), self._encode_value(value)
    wb.put(key, value)

# 优化后
batch_data = {self._encode_key(k): self._encode_value(v) for k, v in buffer_dict_snapshot.items()}
wb.put_batch(batch_data)
```

#### 11.1.2 写合并（Write Combining）
在高频写入场景下，合并对同一键的多次更新，只保留最后一次：
```python
def update(self, d: dict):
    # 对现有buffer中的键进行更新合并，而不是简单追加
    with self._buffer_lock:
        # 保留每个键的最新值
        self.buffer_dict.update(d)
```

#### 11.1.3 缓存淘汰策略
实现LRU或其他缓存淘汰策略，避免内存无限增长：
```python
class LRUCache:
    def __init__(self, capacity):
        self.cache = OrderedDict()
        self.capacity = capacity
        
    def get(self, key, default=None):
        if key not in self.cache:
            return default
        # 访问后移到末尾（最近使用）
        self.cache.move_to_end(key)
        return self.cache[key]
        
    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        # 超出容量，删除最久未使用的
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)
```

### 11.2 中优先级优化

#### 11.2.1 动态缓冲区大小
根据系统负载和内存使用情况动态调整`MAX_BUFFER_SIZE`：
```python
def _adjust_buffer_size(self):
    if self._buffered_count > self.MAX_BUFFER_SIZE * 0.9:
        # 当前缓冲区使用率高，可能需要扩容
        current_memory = sys.getsizeof(self.buffer_dict)
        if current_memory < self._memory_threshold:
            self.MAX_BUFFER_SIZE = min(self.MAX_BUFFER_SIZE * 2, self.ABSOLUTE_MAX_BUFFER_SIZE)
    elif self._buffered_count < self.MAX_BUFFER_SIZE * 0.3:
        # 缓冲区使用率低，可以适当缩小
        self.MAX_BUFFER_SIZE = max(self.MAX_BUFFER_SIZE // 2, self.MIN_BUFFER_SIZE)
```

#### 11.2.2 细粒度锁
将单一的`self._buffer_lock`替换为分片锁或读写锁，减少锁争用：
```python
class ShardedLock:
    def __init__(self, num_shards=16):
        self._locks = [threading.Lock() for _ in range(num_shards)]
    
    def lock_for_key(self, key):
        shard = hash(key) % len(self._locks)
        return self._locks[shard]
        
# 使用示例
def __getitem__(self, key):
    with self._locks.lock_for_key(key):
        value = self.get(key)
    return value
```

#### 11.2.3 布隆过滤器
使用布隆过滤器快速判断键是否存在，减少无效磁盘访问：
```python
# 初始化时创建布隆过滤器
self._bloom_filter = BloomFilter(capacity=1000000)

# 加载现有键
for key in self._static_view.keys():
    self._bloom_filter.add(key)
    
# 查询时先检查布隆过滤器
def __contains__(self, key):
    if not self._bloom_filter.check(key):
        return False  # 键肯定不存在
    # 继续常规检查...
```

### 11.3 长期优化

#### 11.3.1 异步预写日志（WAL）
增加预写日志机制，提高写入操作的响应速度和数据安全性：
```python
def _set(self, key, value):
    # 先写入WAL日志
    self._write_to_wal(key, value, op="SET")
    # 再更新内存缓冲区
    with self._buffer_lock:
        self.buffer_dict[key] = value
```

#### 11.3.2 分层缓存策略
实现热/冷数据分层管理，更高效地利用内存：
```python
class TieredCache:
    def __init__(self, hot_size=100, warm_size=1000):
        self.hot_cache = LRUCache(hot_size)  # 最常用数据
        self.warm_cache = LRUCache(warm_size)  # 次常用数据
        
    def get(self, key, default=None):
        # 先查热缓存
        value = self.hot_cache.get(key)
        if value is not None:
            return value
            
        # 再查温缓存
        value = self.warm_cache.get(key)
        if value is not None:
            # 提升到热缓存
            self.hot_cache.put(key, value)
            return value
            
        return default
```

#### 11.3.3 序列化优化
改进序列化策略，减少CPU和内存开销：
```python
# 延迟序列化
def _set(self, key, value):
    # 不立即序列化，只在写入数据库时序列化
    with self._buffer_lock:
        self.buffer_dict[key] = value  # 存储原始对象

# 序列化缓存
class SerializationCache:
    def __init__(self, capacity=100):
        self.cache = {}
        self.capacity = capacity
        
    def encode(self, obj):
        obj_id = id(obj)
        if obj_id in self.cache:
            return self.cache[obj_id]
            
        result = encode(obj)
        if len(self.cache) < self.capacity:
            self.cache[obj_id] = result
        return result
```

#### 11.3.4 线程池
替换单一工作线程为线程池，提升并行处理能力：
```python
from concurrent.futures import ThreadPoolExecutor

def _init(self):
    # 创建线程池
    self._thread_pool = ThreadPoolExecutor(max_workers=4)
    
def update(self, d: dict):
    # 使用线程池处理大批量更新
    if len(d) > 1000:
        chunks = [dict(list(d.items())[i:i+100]) for i in range(0, len(d), 100)]
        futures = [self._thread_pool.submit(self._update_chunk, chunk) for chunk in chunks]
        for future in futures:
            future.result()  # 等待所有任务完成
    else:
        # 小批量使用原有逻辑
        self._update_internal(d)
```

#### 11.3.5 Cython加速
对核心路径使用Cython实现，提高执行效率：
```python
# 在setup.py中添加Cython扩展
from setuptools import setup, Extension
from Cython.Build import cythonize

extensions = [
    Extension("flaxkv.fast_core", ["flaxkv/fast_core.pyx"]),
]

setup(
    # ...
    ext_modules=cythonize(extensions),
) 