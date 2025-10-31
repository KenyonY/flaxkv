# FlaxKV2 性能优化计划

## 概述

基于对代码库和现有性能测试报告的深入分析，本文档提出了系统的性能优化方案。这些优化预期可以带来显著的性能提升：
- **读性能**: 40-60% 提升
- **写性能**: 30-100% 提升（取决于工作负载）
- **序列化性能**: 15-30% 提升

---

## 🚀 P0 优先级：LevelDB配置优化（立即实施）

### 当前问题

```python
# flaxkv2/core/raw_leveldb_dict.py line 130-135
self._leveldb_options = {
    'create_if_missing': create_if_missing,
    'write_buffer_size': 64 * 1024 * 1024,  # 64MB
    'max_open_files': 100,
    'compression': 'snappy',
}
# ❌ 缺少关键的 block_cache 和 bloom_filter 配置
```

**影响**: LevelDB的读性能严重依赖block_cache，当前完全未配置导致每次读取都需要磁盘I/O。

### 优化方案

#### 方案1：通用优化配置
```python
self._leveldb_options = {
    'create_if_missing': create_if_missing,

    # 写性能优化
    'write_buffer_size': 128 * 1024 * 1024,  # 128MB (从64MB提升)

    # 读性能优化 - 关键！
    'block_cache_size': 256 * 1024 * 1024,   # 256MB 读缓存
    'block_size': 16 * 1024,                  # 16KB (优化小对象)

    # 查询优化
    'bloom_filter_bits': 10,                  # 布隆过滤器(减少磁盘查找)

    # 文件管理
    'max_open_files': 500,                    # 500 (从100提升)
    'compression': 'snappy',
}
```

#### 方案2：支持预设配置文件
```python
# 添加性能配置预设
PERFORMANCE_PROFILES = {
    'balanced': {
        'block_cache_size': 256 * 1024 * 1024,
        'write_buffer_size': 128 * 1024 * 1024,
    },
    'read_optimized': {
        'block_cache_size': 512 * 1024 * 1024,   # 更大的读缓存
        'write_buffer_size': 64 * 1024 * 1024,
    },
    'write_optimized': {
        'block_cache_size': 128 * 1024 * 1024,
        'write_buffer_size': 256 * 1024 * 1024,  # 更大的写缓冲
    },
    'memory_constrained': {
        'block_cache_size': 64 * 1024 * 1024,
        'write_buffer_size': 32 * 1024 * 1024,
    },
}

# 使用方式
db = FlaxKV("mydb", "./data", performance_profile='read_optimized')

# 或自定义
db = FlaxKV("mydb", "./data",
            block_cache_size=512*1024*1024,
            write_buffer_size=128*1024*1024)
```

### 预期收益
- 热数据读取速度: **3-5倍提升**
- 磁盘I/O减少: **60-80%**
- P99延迟降低: **40-70%**

### 实施难度
- **难度**: ⭐ (非常低)
- **工作量**: 1-2小时
- **风险**: 极低（仅配置变更）
- **测试需求**: 运行现有benchmark验证

---

## 🎯 P1 优先级：智能序列化类型缓存

### 当前问题

```python
# flaxkv2/serialization/encoder.py line 74-81
def encode(value: Any) -> bytes:
    # 每次都要尝试 msgpack，失败才用 pickle
    try:
        packed = msgpack.packb(value, use_bin_type=True)
        return bytes([TYPE_MSGPACK]) + packed
    except (TypeError, OverflowError):
        pickled = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        return bytes([TYPE_PICKLE]) + pickled
```

**问题**：
- 对于同一类型的对象，try-except会重复执行成千上万次
- 特别是机器学习场景，可能有100万个相同类型的NumPy数组
- 每次异常捕获都有性能开销

### 优化方案

```python
# flaxkv2/serialization/encoder.py
import threading
from typing import Any, Callable, Dict, Type

# 类型 -> 编码函数的缓存
_type_cache: Dict[Type, Callable] = {}
_cache_lock = threading.RLock()

# 预定义的快速编码函数
def _encode_msgpack(value: Any) -> bytes:
    """msgpack编码（已验证兼容）"""
    packed = msgpack.packb(value, use_bin_type=True)
    return bytes([TYPE_MSGPACK]) + packed

def _encode_pickle(value: Any) -> bytes:
    """pickle编码（已验证不兼容msgpack）"""
    pickled = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    return bytes([TYPE_PICKLE]) + pickled

def _encode_numpy(value: np.ndarray) -> bytes:
    """NumPy数组编码"""
    array_data = {
        'dtype': str(value.dtype),
        'shape': value.shape,
        'data': value.tobytes()
    }
    packed = msgpack.packb(array_data, use_bin_type=True)
    return bytes([TYPE_NUMPY]) + packed

def encode(value: Any) -> bytes:
    """
    智能编码：使用类型缓存避免重复尝试
    """
    value_type = type(value)

    # 1. 快速路径：已知类型
    with _cache_lock:
        if value_type in _type_cache:
            return _type_cache[value_type](value)

    # 2. NumPy特殊处理（高优先级）
    if isinstance(value, np.ndarray):
        with _cache_lock:
            _type_cache[value_type] = _encode_numpy
        return _encode_numpy(value)

    # 3. Pandas特殊处理
    if HAS_PANDAS and pd is not None and isinstance(value, pd.DataFrame):
        # ... 现有逻辑 ...
        with _cache_lock:
            _type_cache[value_type] = _encode_pandas
        return _encode_pandas(value)

    # 4. 尝试msgpack（仅第一次）
    try:
        packed = msgpack.packb(value, use_bin_type=True)
        # 成功 -> 缓存为msgpack类型
        with _cache_lock:
            _type_cache[value_type] = _encode_msgpack
        return bytes([TYPE_MSGPACK]) + packed
    except (TypeError, OverflowError):
        # 失败 -> 缓存为pickle类型
        with _cache_lock:
            _type_cache[value_type] = _encode_pickle
        return _encode_pickle(value)

# 提供缓存清理函数（用于测试或内存管理）
def clear_type_cache():
    """清理类型缓存"""
    with _cache_lock:
        _type_cache.clear()
```

### 优化策略

1. **类型级别缓存**：记住每个Python类型应该用哪个序列化器
2. **线程安全**：使用RLock保护缓存
3. **优先级处理**：NumPy/Pandas等高频类型优先判断
4. **预编译函数**：避免lambda带来的闭包开销

### 预期收益
- 重复类型编码速度: **20-30% 提升**
- 异常处理开销: **完全消除**（第一次后）
- ML场景（大量同类型数组）: **最高受益**

### 实施难度
- **难度**: ⭐⭐ (中等)
- **工作量**: 4-6小时（实现+测试）
- **风险**: 低（需要测试边缘情况）
- **测试需求**:
  - 单元测试各种类型
  - 并发测试
  - 内存泄漏测试（缓存是否无限增长）

---

## ⚡ P1 优先级：自适应批量写入队列

### 当前问题

```python
# flaxkv2/core/raw_leveldb_dict.py line 296-297
def __setitem__(self, key, value):
    # ...
    with self._db_lock:
        self._db.put(key_bytes, value_bytes)  # ❌ 每次独立写入
```

**问题分析**：
- 用户通常使用循环单次写入：`for i in range(10000): db[key] = value`
- 每次调用都触发一次磁盘I/O
- 虽然`update()`支持批量，但用户体验不佳
- 性能测试显示缓冲版本比直接写入快17.8%

### 优化方案：异步批处理队列

```python
# flaxkv2/core/raw_leveldb_dict.py

import queue
import threading
import time
from typing import Any, Dict, Optional, Tuple

class RawLevelDBDict:
    def __init__(
        self,
        name: str,
        path: str = ".",
        rebuild: bool = False,
        create_if_missing: bool = True,
        raw: bool = False,
        default_ttl: Optional[int] = None,
        auto_nested: bool = False,
        # 新增批处理参数
        auto_batch: bool = False,           # 是否启用自动批处理
        batch_size: int = 1000,             # 队列达到此大小立即提交
        batch_interval_ms: int = 10,        # 定时提交间隔(毫秒)
        **kwargs
    ):
        """
        auto_batch: 启用自动批处理（后台线程）
        batch_size: 队列达到此大小时立即提交
        batch_interval_ms: 定时刷新间隔（毫秒）
        """
        # ... 现有初始化代码 ...

        # 批处理配置
        self._auto_batch = auto_batch
        self._batch_size = batch_size
        self._batch_interval = batch_interval_ms / 1000.0

        if auto_batch:
            # 写入队列（线程安全）
            self._write_queue: queue.Queue = queue.Queue()
            self._batch_worker_running = True
            self._batch_event = threading.Event()

            # 启动后台批处理线程
            self._batch_thread = threading.Thread(
                target=self._batch_worker,
                daemon=True,
                name=f"FlaxKV-Batch-{name}"
            )
            self._batch_thread.start()

            logger.debug(f"Auto-batch enabled: size={batch_size}, interval={batch_interval_ms}ms")

    def __setitem__(self, key, value):
        """设置键值 - 支持自动批处理"""
        from flaxkv2.core.nested_dict import NestedDBDict

        # 处理特殊键和嵌套逻辑（与现有代码相同）
        # ...

        if self._auto_batch:
            # 异步模式：加入队列
            self._write_queue.put(('put', key, value))

            # 达到阈值立即触发提交
            if self._write_queue.qsize() >= self._batch_size:
                self._batch_event.set()
        else:
            # 同步模式：直接写入（现有逻辑）
            key_bytes = self._encode_key(key)
            value_bytes = self._encode_value(value)
            with self._db_lock:
                self._db.put(key_bytes, value_bytes)

            # 应用默认TTL
            if self._default_ttl is not None:
                self._ttl_manager.set(key, self._default_ttl)

    def __delitem__(self, key):
        """删除键 - 支持自动批处理"""
        if self._auto_batch:
            self._write_queue.put(('delete', key, None))
            if self._write_queue.qsize() >= self._batch_size:
                self._batch_event.set()
        else:
            # 现有同步逻辑
            # ...
            pass

    def _batch_worker(self):
        """后台批处理工作线程"""
        while self._batch_worker_running or not self._write_queue.empty():
            # 等待事件或超时
            self._batch_event.wait(timeout=self._batch_interval)
            self._batch_event.clear()

            # 收集队列中的操作
            operations = []
            while not self._write_queue.empty() and len(operations) < self._batch_size:
                try:
                    op = self._write_queue.get_nowait()
                    operations.append(op)
                except queue.Empty:
                    break

            # 批量提交
            if operations:
                self._flush_operations(operations)

    def _flush_operations(self, operations: list):
        """批量提交操作"""
        if not operations:
            return

        with self._db_lock:
            batch = self._db.write_batch()

            for op_type, key, value in operations:
                if op_type == 'put':
                    key_bytes = self._encode_key(key)
                    value_bytes = self._encode_value(value)
                    batch.put(key_bytes, value_bytes)
                elif op_type == 'delete':
                    key_bytes = self._encode_key(key)
                    batch.delete(key_bytes)

            # 原子提交
            batch.write()

        # 处理TTL（如果有默认TTL）
        if self._default_ttl is not None:
            for op_type, key, _ in operations:
                if op_type == 'put':
                    self._ttl_manager.set(key, self._default_ttl)
                elif op_type == 'delete':
                    self._ttl_manager.remove(key)

        logger.debug(f"Flushed {len(operations)} operations")

    def flush(self, wait: bool = True):
        """
        手动刷新写入队列

        Args:
            wait: 是否等待刷新完成
        """
        if not self._auto_batch:
            return

        # 触发立即刷新
        self._batch_event.set()

        if wait:
            # 等待队列清空
            while not self._write_queue.empty():
                time.sleep(0.001)

    def close(self):
        """关闭数据库 - 确保刷新队列"""
        if self._auto_batch and self._batch_worker_running:
            # 停止批处理线程
            self._batch_worker_running = False
            self._batch_event.set()

            # 等待线程结束
            if self._batch_thread.is_alive():
                self._batch_thread.join(timeout=5.0)

            # 确保队列清空
            if not self._write_queue.empty():
                logger.warning(f"Flushing {self._write_queue.qsize()} pending writes")
                self.flush(wait=True)

        # 现有关闭逻辑
        # ...
```

### 使用示例

```python
# 场景1：高吞吐写入（推荐启用批处理）
db = FlaxKV("mydb", "./data",
            auto_batch=True,          # 启用批处理
            batch_size=1000,          # 1000条或
            batch_interval_ms=10)     # 10ms 触发提交

for i in range(100000):
    db[f"key{i}"] = f"value{i}"  # 自动批处理，高性能

db.close()  # 自动刷新剩余数据

# 场景2：需要强一致性（禁用批处理）
db = FlaxKV("mydb", "./data", auto_batch=False)
db["critical_key"] = "value"  # 立即写入磁盘

# 场景3：混合模式
db = FlaxKV("mydb", "./data", auto_batch=True)
for i in range(10000):
    db[f"key{i}"] = f"value{i}"

# 关键操作前手动刷新
db.flush(wait=True)
db["checkpoint"] = "committed"
```

### 预期收益
- 大量单次写入: **50-100% 性能提升**
- 磁盘I/O次数: **减少 80-95%**
- 吞吐量: 接近`update()`批量方法的性能

### 权衡与风险
- ⚠️ **写入延迟**: 增加5-10ms（通常可接受）
- ⚠️ **崩溃风险**: 进程崩溃可能丢失队列中的数据
  - 缓解：默认禁用，由用户选择
  - 缓解：提供`flush()`手动刷新
  - 缓解：在关键操作前调用`flush()`
- ⚠️ **并发复杂度**: 需要仔细测试线程安全

### 实施难度
- **难度**: ⭐⭐⭐ (较高)
- **工作量**: 2-3天（实现+充分测试）
- **风险**: 中等
- **测试需求**:
  - 并发安全测试
  - 崩溃恢复测试
  - 性能回归测试
  - 压力测试（长时间运行）

---

## 🎁 额外优化项（Bonus）

### 4. ZeroMQ远程传输压缩

**当前问题**: 大对象通过网络传输带宽占用高

**优化方案**:
```python
# flaxkv2/client/zmq_client.py
import lz4.frame

class RemoteDBDict:
    def __init__(self, ..., compress=True, compress_threshold=1024):
        """
        compress: 是否启用压缩
        compress_threshold: 超过此大小才压缩（字节）
        """
        self._compress = compress
        self._compress_threshold = compress_threshold

    def _send_request(self, request: list):
        request_data = msgpack.packb(request, use_bin_type=True)

        # 大消息启用压缩
        if self._compress and len(request_data) > self._compress_threshold:
            compressed = lz4.frame.compress(request_data)
            # 添加压缩标识
            self.socket.send(b'\x01' + compressed)
        else:
            self.socket.send(b'\x00' + request_data)
```

**收益**: 大对象传输带宽节省 50-70%

---

### 5. TTL写入优化

**当前问题**:
```python
# 两次独立的写入操作
self._db.put(key_bytes, value_bytes)           # 第一次I/O
self._ttl_manager.set(key, self._default_ttl)  # 第二次I/O
```

**优化方案**:
```python
def __setitem__(self, key, value):
    key_bytes = self._encode_key(key)
    value_bytes = self._encode_value(value)

    with self._db_lock:
        if self._default_ttl is not None:
            # 合并到单个 WriteBatch
            batch = self._db.write_batch()
            batch.put(key_bytes, value_bytes)

            ttl_key = self._encode_key(f'__ttl_info__:{key}')
            ttl_value = self._encode_value(time.time() + self._default_ttl)
            batch.put(ttl_key, ttl_value)

            batch.write()  # 单次原子提交
        else:
            self._db.put(key_bytes, value_bytes)
```

**收益**: 带TTL的写入性能提升 ~40%

---

### 6. 读写锁分离

**当前问题**: `RLock`不区分读写，读操作之间会互相阻塞

**优化方案**:
```python
import threading

class RWLock:
    """读写锁实现"""
    def __init__(self):
        self._read_ready = threading.Condition(threading.RLock())
        self._readers = 0
        self._writers = 0
        self._write_waiters = 0

    def acquire_read(self):
        self._read_ready.acquire()
        while self._writers > 0 or self._write_waiters > 0:
            self._read_ready.wait()
        self._readers += 1
        self._read_ready.release()

    def release_read(self):
        self._read_ready.acquire()
        self._readers -= 1
        if self._readers == 0:
            self._read_ready.notifyAll()
        self._read_ready.release()

    def acquire_write(self):
        self._read_ready.acquire()
        self._write_waiters += 1
        while self._readers > 0 or self._writers > 0:
            self._read_ready.wait()
        self._write_waiters -= 1
        self._writers += 1
        self._read_ready.release()

    def release_write(self):
        self._read_ready.acquire()
        self._writers -= 1
        self._read_ready.notifyAll()
        self._read_ready.release()

# 使用
class RawLevelDBDict:
    def __init__(self, ...):
        self._db_lock = RWLock()

    def __getitem__(self, key):
        self._db_lock.acquire_read()
        try:
            # 读取操作
            pass
        finally:
            self._db_lock.release_read()

    def __setitem__(self, key, value):
        self._db_lock.acquire_write()
        try:
            # 写入操作
            pass
        finally:
            self._db_lock.release_write()
```

**收益**: 并发读取性能提升 2-3倍

---

## 📊 实施路线图

### Phase 1: 快速胜利（1周）
- ✅ **LevelDB配置优化** (P0, 1小时)
- ✅ **TTL写入合并** (Bonus, 2小时)

**预期收益**: 读性能 +40-60%, TTL写入 +40%

### Phase 2: 核心优化（2-3周）
- ✅ **序列化类型缓存** (P1, 1天)
- ✅ **批量写入队列** (P1, 2-3天)

**预期收益**: 编码 +15-30%, 写入 +30-100%

### Phase 3: 高级优化（1-2周）
- ✅ **读写锁分离** (Bonus, 2天)
- ✅ **远程传输压缩** (Bonus, 1天)

**预期收益**: 并发读 +100-200%, 网络带宽 -50-70%

---

## 🧪 性能测试计划

### 测试环境
- Python 3.10+
- SSD存储
- 多核CPU
- 独立benchmark环境

### 测试场景
1. **写入测试**
   - 单次写入 (10K ops)
   - 批量写入 (10K ops, batch=1000)
   - 大对象写入 (1K ops, 1MB each)

2. **读取测试**
   - 随机读取 (10K ops)
   - 顺序读取 (10K ops)
   - 热数据读取 (重复读100次)

3. **混合测试**
   - 80% 读 / 20% 写
   - 50% 读 / 50% 写
   - TTL场景测试

4. **并发测试**
   - 4线程并发读
   - 4线程并发写
   - 混合读写并发

### 基准对比
- 优化前后对比
- 与Redis/RocksDB对比
- 不同配置的性能profile

---

## 📝 注意事项

### 内存使用
- `block_cache_size=256MB` 会增加内存占用
- 类型缓存通常占用 < 1MB
- 批量队列占用取决于 `batch_size`

**建议**: 提供内存受限模式配置

### 向后兼容
- 所有优化都是可选的
- 默认行为保持不变
- 提供迁移指南

### 监控指标
建议添加以下metrics（配合监控TODO）：
- 批量队列长度
- 类型缓存命中率
- LevelDB cache命中率
- 平均写入延迟

---

## 🎯 成功指标

优化完成后预期达到：
- ✅ 读取吞吐量: 2-3M ops/sec (热数据)
- ✅ 写入吞吐量: 800K-1M ops/sec (批量模式)
- ✅ P99延迟: < 1ms (本地), < 10ms (远程)
- ✅ 内存效率: 256MB cache 支撑 90%+ 热数据命中率

---

## 参考文档

- [BUFFERED_VS_RAW_BENCHMARK_REPORT.md](./BUFFERED_VS_RAW_BENCHMARK_REPORT.md) - 现有性能分析
- [LevelDB Tuning Guide](https://github.com/google/leveldb/blob/main/doc/index.md)
- Python threading 文档
- msgpack vs pickle 性能对比

---

**文档维护**: 随着优化实施，更新此文档记录实际效果和经验教训。
