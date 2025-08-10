# FlaxKV 2.0 完整重构计划
> 从零开始构建下一代高性能持久化字典

## 项目愿景与定位

### 核心定位
FlaxKV 2.0 定位为**企业级高性能持久化字典**，提供：
- **极致的用户体验** - 如原生字典般简单易用
- **生产级的可靠性** - 数据安全、错误恢复、监控完整
- **卓越的性能** - 接近内存字典的访问速度
- **灵活的架构** - 本地/远程/集群模式无缝切换

### 继承的优秀设计理念
1. **原生字典接口** - 保持 `db['key'] = value` 的简洁性
2. **非阻塞写入** - 写操作永不阻塞用户进程
3. **自动资源管理** - 无需手动调用 close()
4. **透明远程访问** - 本地/远程使用方式一致
5. **LevelDB 专用优化** - 专注于 LevelDB 的极致优化

## 全新架构设计

### 1. 分层架构 (Layer Architecture)

```
┌─────────────────────────────────────────┐
│               API Layer                 │  ← 用户接口层
├─────────────────────────────────────────┤
│             Transaction Layer           │  ← 事务管理层
├─────────────────────────────────────────┤
│               Cache Layer               │  ← 智能缓存层
├─────────────────────────────────────────┤
│              Buffer Layer               │  ← 缓冲管理层
├─────────────────────────────────────────┤
│             Storage Layer               │  ← 存储抽象层
├─────────────────────────────────────────┤
│            Backend Layer                │  ← 后端实现层
└─────────────────────────────────────────┘
```

### 2. 核心组件设计

#### 2.1 统一接口层 (FlaxKV Core)
```python
# 新架构的核心接口
class FlaxKV:
    """高性能持久化字典 - 统一接口"""
    
    def __init__(
        self,
        name: str,
        backend: str = "local",             # local/remote (第一阶段)
        location: str | URL = ".",          # 本地路径或远程URL
        config: FlaxKVConfig | None = None  # 统一配置对象
    ):
        pass
    
    # === 字典接口 (保持不变) ===
    def __getitem__(self, key): ...
    def __setitem__(self, key, value): ...
    def __delitem__(self, key): ...
    def __contains__(self, key): ...
    def __len__(self): ...
    def __iter__(self): ...
    
    # === 增强接口 ===
    def transaction(self) -> Transaction: ...
    def batch(self) -> BatchOperation: ...
    def expire(self, key, seconds): ...
    def range(self, start, end): ...
    def backup(self, path): ...
    def info(self) -> DatabaseInfo: ...
```

#### 2.2 配置管理系统
```python
@dataclass
class FlaxKVConfig:
    """统一配置管理"""
    # === 性能配置 ===
    buffer_size: int = 1000                # 缓冲区大小
    buffer_timeout: float = 5.0            # 缓冲超时(秒)
    cache_size: str = "100MB"              # 缓存大小
    cache_policy: str = "lru"              # lru/lfu/arc
    
    # === 持久化配置 ===
    sync_mode: str = "async"               # async/sync/manual
    compression: bool = True               # 数据压缩
    encryption_key: str | None = None      # 数据加密
    
    # === 可靠性配置 ===
    backup_interval: int = 3600            # 自动备份间隔(秒)
    integrity_check: bool = True           # 数据完整性检查
    auto_repair: bool = True               # 自动修复
    
    # === 网络配置 (远程模式) ===
    network_protocol: str = "zmq"          # zmq/http (第一阶段)
    connection_pool_size: int = 10         # 连接池大小
    request_timeout: float = 30.0          # 请求超时
    retry_attempts: int = 3                # 重试次数
    
    # === ZeroMQ 专用配置 ===
    zmq_socket_type: str = "REQ"           # REQ/DEALER/PUSH
    zmq_high_water_mark: int = 10000       # 队列高水位标记
    zmq_linger: int = 0                    # 关闭延迟时间
    
    # === HTTP 专用配置 ===
    http_version: str = "2"                # HTTP 版本 (1.1/2/3)
    http_keepalive: bool = True            # 连接保持
    http_compression: bool = True          # 启用压缩
    
    # === 监控配置 ===
    metrics_enabled: bool = True           # 指标收集
    slow_query_threshold: float = 1.0      # 慢查询阈值(秒)
    
    @classmethod
    def for_development(cls) -> "FlaxKVConfig":
        """开发环境配置"""
        return cls(buffer_size=100, sync_mode="sync")
    
    @classmethod
    def for_production(cls) -> "FlaxKVConfig":
        """生产环境配置"""
        return cls(
            buffer_size=5000,
            cache_size="1GB",
            backup_interval=1800,
            encryption_key=os.getenv("FLAXKV_KEY")
        )
```

#### 2.3 事务管理系统
```python
class Transaction:
    """ACID 事务支持"""
    
    def __init__(self, storage: StorageEngine):
        self._storage = storage
        self._operations: List[Operation] = []
        self._snapshots: Dict[str, Any] = {}
        
    def __enter__(self):
        self._storage.begin_transaction()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
    
    def commit(self):
        """提交所有操作"""
        self._storage.commit_transaction(self._operations)
        
    def rollback(self):
        """回滚所有操作"""
        self._storage.rollback_transaction(self._snapshots)
    
    # 支持在事务中使用字典接口
    def __setitem__(self, key, value):
        self._operations.append(SetOperation(key, value))
    
    def __delitem__(self, key):
        self._operations.append(DeleteOperation(key))
```

#### 2.4 智能缓存层
```python
class IntelligentCache:
    """智能缓存管理"""
    
    def __init__(self, config: FlaxKVConfig):
        self._policy = self._create_policy(config.cache_policy)
        self._max_size = parse_size(config.cache_size)
        self._metrics = CacheMetrics()
        
    def get(self, key: str) -> CacheResult:
        """智能缓存获取"""
        result = self._policy.get(key)
        self._metrics.record_access(key, result.hit)
        
        if result.hit:
            return result
            
        # 预测性预加载
        if self._should_prefetch(key):
            self._schedule_prefetch(key)
            
        return result
    
    def put(self, key: str, value: Any, ttl: int = None):
        """智能缓存存储"""
        # 基于访问模式的智能替换
        if self._is_full():
            victim_key = self._policy.select_victim(key)
            self._evict(victim_key)
            
        self._policy.put(key, value, ttl)
    
    def _should_prefetch(self, key: str) -> bool:
        """基于访问模式的预测性预加载"""
        pattern = self._metrics.get_access_pattern(key)
        return pattern.suggests_sequential_access()
```

#### 2.5 高性能缓冲层
```python
class HighPerformanceBuffer:
    """高性能缓冲管理"""
    
    def __init__(self, config: FlaxKVConfig):
        self._buffer = {}  # 内存缓冲区
        self._buffer_lock = asyncio.Lock()
        self._flush_scheduler = FlushScheduler(config.buffer_timeout)
        self._write_ahead_log = WriteAheadLog()  # WAL 保证数据安全
        
    async def write(self, key: str, value: Any) -> None:
        """非阻塞写入"""
        async with self._buffer_lock:
            # 立即写入 WAL 确保不丢数据
            await self._write_ahead_log.append(key, value)
            
            # 写入内存缓冲区
            self._buffer[key] = value
            
            # 智能刷新策略
            if self._should_flush():
                asyncio.create_task(self._flush_to_storage())
    
    async def read(self, key: str) -> Any:
        """读取最新数据"""
        # 优先从缓冲区读取
        if key in self._buffer:
            return self._buffer[key]
            
        # 从持久化存储读取
        return await self._storage.get(key)
        
    async def _flush_to_storage(self):
        """批量刷新到存储层"""
        async with self._buffer_lock:
            batch = dict(self._buffer)
            self._buffer.clear()
            
        # 批量写入提高性能
        await self._storage.batch_write(batch)
        await self._write_ahead_log.checkpoint()
```

#### 2.6 存储引擎设计 (LevelDB 专用优化)
```python
class LevelDBEngine:
    """LevelDB 专用存储引擎 - 极致优化"""
    
    def __init__(self, path: str, config: FlaxKVConfig):
        self.db = plyvel.DB(
            path, 
            create_if_missing=True,
            # LevelDB 专项优化参数
            block_cache_size=config.cache_size_bytes,
            write_buffer_size=config.buffer_size * 1024 * 1024,
            max_open_files=1000,
            block_size=64 * 1024,  # 64KB 块大小
            compression='snappy',   # Snappy 压缩
            bloom_filter_bits=10,   # 布隆过滤器优化
        )
        
        # 专为 LevelDB 设计的优化器
        self.optimizer = LevelDBOptimizer(self.db, config)
        
    async def get(self, key: str) -> Any:
        """LevelDB 优化的单键获取"""
        try:
            data = self.db.get(key.encode('utf-8'))
            if data is None:
                raise KeyError(key)
            return self.optimizer.decode_value(data)
        except Exception:
            # LevelDB 特定错误处理
            await self.optimizer.check_and_repair()
            raise
    
    async def batch_get(self, keys: List[str]) -> Dict[str, Any]:
        """LevelDB 批量获取优化"""
        # 使用 LevelDB 的 iterator 批量读取
        result = {}
        with self.db.iterator() as it:
            for key in keys:
                it.seek(key.encode('utf-8'))
                if it.valid() and it.key().decode('utf-8') == key:
                    result[key] = self.optimizer.decode_value(it.value())
        return result
        
    async def range_query(self, start: str, end: str, limit: int = None) -> AsyncIterator[Tuple[str, Any]]:
        """LevelDB 范围查询优化"""
        count = 0
        start_key = start.encode('utf-8')
        end_key = end.encode('utf-8')
        
        with self.db.iterator(start=start_key) as it:
            for key, value in it:
                if limit and count >= limit:
                    break
                if key > end_key:
                    break
                    
                yield key.decode('utf-8'), self.optimizer.decode_value(value)
                count += 1
                
    def compact_range(self, start: str = None, end: str = None):
        """LevelDB 手动压缩"""
        start_key = start.encode('utf-8') if start else None
        end_key = end.encode('utf-8') if end else None
        self.db.compact_range(start_key, end_key)

class LevelDBOptimizer:
    """LevelDB 专用优化器"""
    
    def __init__(self, db, config):
        self.db = db
        self.config = config
        self._setup_optimizations()
        
    def _setup_optimizations(self):
        """设置 LevelDB 专用优化"""
        # 写入优化
        self.write_options = {
            'sync': self.config.sync_mode == 'sync',
            'fill_cache': True,
        }
        
        # 读取优化  
        self.read_options = {
            'fill_cache': True,
            'verify_checksums': self.config.integrity_check,
        }
        
    def encode_value(self, value: Any) -> bytes:
        """LevelDB 优化的值编码"""
        # 针对 LevelDB 的数据格式优化
        return msgspec.msgpack.encode(value)
        
    def decode_value(self, data: bytes) -> Any:
        """LevelDB 优化的值解码"""  
        return msgspec.msgpack.decode(data)
        
    async def check_and_repair(self):
        """LevelDB 健康检查和修复"""
        # LevelDB 特定的健康检查逻辑
        pass

class RemoteEngine:
    """远程存储引擎 - 多协议支持"""
    
    def __init__(self, url: str, config: FlaxKVConfig):
        self.url = url
        self.config = config
        
        # 根据配置选择最优网络协议
        self.transport = self._create_transport(config.network_protocol)
        
    def _create_transport(self, protocol: str):
        """创建传输层 - 支持多种高性能协议"""
        if protocol == "zmq":
            return ZeroMQTransport(self.url, self.config)
        elif protocol == "grpc":
            return GRPCTransport(self.url, self.config)
        elif protocol == "quic":
            return QUICTransport(self.url, self.config)
        else:  # http 作为默认
            return HTTPTransport(self.url, self.config)
```

### 高性能网络传输设计

#### ZeroMQ 传输层 (最高性能)
```python
class ZeroMQTransport:
    """ZeroMQ 传输 - 极致性能"""
    
    def __init__(self, url: str, config: FlaxKVConfig):
        import zmq
        import zmq.asyncio
        
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.REQ)
        
        # ZMQ 性能优化配置
        self.socket.setsockopt(zmq.RCVTIMEO, int(config.request_timeout * 1000))
        self.socket.setsockopt(zmq.SNDTIMEO, int(config.request_timeout * 1000))
        self.socket.setsockopt(zmq.LINGER, 0)
        
        # 高性能选项
        self.socket.setsockopt(zmq.SNDHWM, 10000)  # 发送队列大小
        self.socket.setsockopt(zmq.RCVHWM, 10000)  # 接收队列大小
        
        self.socket.connect(url)
        
    async def send_request(self, operation: str, data: Dict[str, Any]) -> Any:
        """发送请求 - ZMQ 二进制协议"""
        request = {
            'op': operation,
            'data': data,
            'id': uuid.uuid4().hex
        }
        
        # 使用 msgpack 序列化，比 JSON 快 2-3 倍
        packed_request = msgspec.msgpack.encode(request)
        
        await self.socket.send(packed_request)
        response_data = await self.socket.recv()
        
        response = msgspec.msgpack.decode(response_data)
        
        if response.get('error'):
            raise RemoteError(response['error'])
            
        return response['result']

#### QUIC 传输层 (低延迟)  
class QUICTransport:
    """QUIC 传输 - 低延迟，高可靠"""
    
    def __init__(self, url: str, config: FlaxKVConfig):
        # 使用 aioquic 实现 QUIC 协议
        from aioquic.asyncio import connect
        from aioquic.quic.configuration import QuicConfiguration
        
        self.config_quic = QuicConfiguration(is_client=True)
        self.url = url
        
    async def connect(self):
        """建立 QUIC 连接"""
        self.protocol = await connect(
            self.url.host,
            self.url.port,
            configuration=self.config_quic
        )
        
    async def send_request(self, operation: str, data: Dict[str, Any]) -> Any:
        """QUIC 请求 - 多路复用，0-RTT"""
        stream_id = self.protocol.get_next_available_stream_id()
        
        request_data = msgspec.msgpack.encode({
            'op': operation,
            'data': data
        })
        
        # QUIC 流式传输
        self.protocol.send_stream_data(stream_id, request_data, end_stream=True)
        
        # 异步接收响应
        response_data = await self.protocol.receive_stream_data(stream_id)
        return msgspec.msgpack.decode(response_data)

#### gRPC 传输层 (结构化高效)
class GRPCTransport:
    """gRPC 传输 - 结构化，流式支持"""
    
    def __init__(self, url: str, config: FlaxKVConfig):
        import grpc
        
        # gRPC 性能优化选项
        options = [
            ('grpc.keepalive_time_ms', 10000),
            ('grpc.keepalive_timeout_ms', 5000),
            ('grpc.http2.max_pings_without_data', 0),
            ('grpc.http2.min_time_between_pings_ms', 10000),
        ]
        
        self.channel = grpc.aio.insecure_channel(url, options=options)
        self.stub = FlaxKVServiceStub(self.channel)
        
    async def send_request(self, operation: str, data: Dict[str, Any]) -> Any:
        """gRPC 请求 - 强类型，高效序列化"""
        if operation == 'get':
            request = GetRequest(key=data['key'])
            response = await self.stub.Get(request)
            return response.value
            
        elif operation == 'batch_get':
            request = BatchGetRequest(keys=data['keys'])
            response = await self.stub.BatchGet(request)
            return {item.key: item.value for item in response.items}
            
        # ... 其他操作

### 智能协议选择完整实现

#### 网络环境检测器
```python
import ipaddress
import socket
import time
import asyncio
from typing import Dict, Any, Optional
from urllib.parse import urlparse

class NetworkEnvironmentDetector:
    """网络环境智能检测"""
    
    def __init__(self):
        self.cache = {}
        self.cache_ttl = 300  # 5分钟缓存
    
    async def detect_optimal_protocol(self, target_url: str) -> Dict[str, Any]:
        """检测最优协议"""
        cache_key = target_url
        current_time = time.time()
        
        # 检查缓存
        if cache_key in self.cache:
            cached_result, timestamp = self.cache[cache_key]
            if current_time - timestamp < self.cache_ttl:
                return cached_result
        
        # 执行检测
        result = await self._perform_detection(target_url)
        
        # 缓存结果
        self.cache[cache_key] = (result, current_time)
        
        return result
    
    async def _perform_detection(self, target_url: str) -> Dict[str, Any]:
        """执行网络检测"""
        try:
            # 1. 解析URL
            parsed = urlparse(target_url)
            host = parsed.hostname or target_url
            port = parsed.port or (5555 if 'zmq' in target_url else 8000)
            
            # 2. 解析IP地址
            try:
                ip_str = socket.gethostbyname(host)
                ip = ipaddress.ip_address(ip_str)
            except:
                ip_str = host
                ip = None
            
            # 3. 基础网络分类
            network_type = self._classify_network_type(ip, host)
            
            # 4. 延迟测试
            latency_ms = await self._measure_latency(host, port)
            
            # 5. 连通性测试
            connectivity = await self._test_connectivity(host, port)
            
            # 6. 协议推荐
            recommendation = self._recommend_protocol(
                network_type, latency_ms, connectivity
            )
            
            return {
                "target_url": target_url,
                "ip_address": ip_str,
                "network_type": network_type,
                "latency_ms": latency_ms,
                "connectivity": connectivity,
                "recommended_protocol": recommendation["protocol"],
                "confidence": recommendation["confidence"],
                "reason": recommendation["reason"],
                "timestamp": time.time()
            }
            
        except Exception as e:
            return {
                "target_url": target_url,
                "error": str(e),
                "recommended_protocol": "http",  # 安全默认选择
                "confidence": 0.1,
                "reason": f"检测失败，使用安全默认: {e}"
            }
    
    def _classify_network_type(self, ip: Optional[ipaddress.IPv4Address], host: str) -> str:
        """网络类型分类"""
        if not ip:
            return "unknown"
        
        if ip.is_loopback:
            return "local"
        elif ip.is_private:
            # 进一步分类私有网络
            if self._is_cloud_internal(host):
                return "cloud_internal"
            else:
                return "lan"
        else:
            return "wan"
    
    def _is_cloud_internal(self, host: str) -> bool:
        """判断是否为云厂商内网"""
        cloud_patterns = [
            '.amazonaws.com', '.aws.com',           # AWS
            '.aliyuncs.com', '.alibabacloud.com',   # 阿里云
            '.tencentcloudapi.com', '.qcloud.com',  # 腾讯云
            '.azure.com', '.microsoft.com',         # Azure
            '.googleapis.com', '.gcp.com',          # Google Cloud
            '.internal', '.local', '.corp'          # 企业内网
        ]
        return any(pattern in host.lower() for pattern in cloud_patterns)
    
    async def _measure_latency(self, host: str, port: int) -> float:
        """测量网络延迟"""
        try:
            start_time = time.perf_counter()
            
            # TCP 连接测试
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=5.0
            )
            
            end_time = time.perf_counter()
            writer.close()
            await writer.wait_closed()
            
            return (end_time - start_time) * 1000  # 转换为毫秒
            
        except asyncio.TimeoutError:
            return 5000.0  # 超时，设为高延迟
        except Exception:
            return float('inf')  # 连接失败
    
    async def _test_connectivity(self, host: str, port: int) -> Dict[str, Any]:
        """测试连通性"""
        tcp_ok = False
        
        # TCP 连通性测试
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), 
                timeout=3.0
            )
            tcp_ok = True
            writer.close()
            await writer.wait_closed()
        except:
            pass
        
        return {
            "tcp": tcp_ok,
            "firewall_friendly": tcp_ok  # TCP通则认为防火墙友好
        }
    
    def _recommend_protocol(self, network_type: str, latency_ms: float, 
                          connectivity: Dict[str, Any]) -> Dict[str, Any]:
        """协议推荐算法"""
        
        if network_type == "local":
            return {
                "protocol": "zmq",
                "confidence": 0.95,
                "reason": "本地环境，ZMQ性能最优"
            }
        
        elif network_type in ["lan", "cloud_internal"]:
            if latency_ms < 5.0 and connectivity["tcp"]:
                return {
                    "protocol": "zmq", 
                    "confidence": 0.90,
                    "reason": f"内网环境，低延迟({latency_ms:.1f}ms)"
                }
            else:
                return {
                    "protocol": "http",
                    "confidence": 0.70, 
                    "reason": f"内网但延迟较高({latency_ms:.1f}ms)"
                }
        
        elif network_type == "wan":
            if connectivity["firewall_friendly"]:
                return {
                    "protocol": "http",
                    "confidence": 0.85,
                    "reason": "广域网，HTTP防火墙兼容性好"
                }
            else:
                return {
                    "protocol": "zmq",
                    "confidence": 0.60,
                    "reason": "广域网但防火墙限制，ZMQ可能被阻止"
                }
        
        else:  # unknown
            return {
                "protocol": "http",
                "confidence": 0.50,
                "reason": "网络类型未知，使用安全默认"
            }

# 智能FlaxKV工厂
class SmartFlaxKV:
    """智能协议选择的FlaxKV"""
    
    def __init__(self, name: str, location: str, config: FlaxKVConfig = None):
        self.name = name
        self.location = location
        self.config = config or FlaxKVConfig()
        self.detector = NetworkEnvironmentDetector()
        
    async def create(self) -> 'FlaxKV':
        """创建优化的FlaxKV实例"""
        if self.config.network_protocol == "auto":
            # 智能检测
            detection = await self.detector.detect_optimal_protocol(self.location)
            optimal_protocol = detection["recommended_protocol"]
            
            print(f"🔍 网络检测结果:")
            print(f"   目标: {detection['target_url']}")
            print(f"   类型: {detection['network_type']}")
            print(f"   延迟: {detection.get('latency_ms', 'N/A'):.1f}ms")
            print(f"   推荐: {optimal_protocol} (置信度: {detection['confidence']:.0%})")
            print(f"   原因: {detection['reason']}")
            
            # 更新配置
            self.config.network_protocol = optimal_protocol
        
        # 创建FlaxKV实例
        return FlaxKV(self.name, "remote" if "://" in self.location else "local", 
                     self.location, self.config)
```

## 核心功能特性设计

### 1. ACID 事务支持

```python
# 使用示例
async with db.transaction() as tx:
    tx['user:123'] = {'name': 'Alice', 'age': 25}
    tx['user:124'] = {'name': 'Bob', 'age': 30}
    # 原子性：要么全部成功，要么全部失败

# 或者手动控制
tx = await db.begin_transaction()
try:
    await tx.set('key1', 'value1')
    await tx.set('key2', 'value2')
    await tx.commit()
except Exception:
    await tx.rollback()
```

### 2. 智能过期机制

```python
# TTL 支持
await db.setex('session:abc123', user_session, expire=3600)
await db.expire('temp_data', 300)

# 查询剩余时间
ttl = await db.ttl('session:abc123')

# 自动过期清理
db.enable_auto_expire(interval=60)  # 每60秒清理一次
```

### 3. 高级查询能力

```python
# 范围查询
async for key, value in db.range('user:1000', 'user:2000'):
    print(f"{key}: {value}")

# 前缀查询
users = [user async for user in db.scan(prefix='user:')]

# 模式匹配
sessions = [s async for s in db.scan(pattern='session:*')]

# 分页查询
page1 = await db.scan(prefix='user:', limit=100)
page2 = await db.scan(prefix='user:', cursor=page1.cursor, limit=100)
```

### 4. 高性能批量操作

```python
# 批量读取
values = await db.mget(['key1', 'key2', 'key3'])

# 批量写入
await db.mset({
    'key1': 'value1',
    'key2': 'value2',
    'key3': 'value3'
})

# 批量删除
deleted_count = await db.delete_many(['key1', 'key2'])

# 批量操作对象
async with db.batch() as batch:
    batch['key1'] = 'value1'
    batch['key2'] = 'value2'
    del batch['key3']
    # 一次性提交所有操作
```

### 5. 数据保护和恢复

```python
# 完整备份
await db.backup('/path/to/backup.flaxkv')

# 增量备份
await db.incremental_backup('/path/to/incremental/')

# 数据恢复
await db.restore('/path/to/backup.flaxkv')

# 数据导入导出
await db.export_json('/path/to/export.json')
await db.import_json('/path/to/import.json')

# 数据完整性检查
report = await db.check_integrity()
if not report.healthy:
    await db.repair(report.issues)
```

### 6. 监控和运维

```python
# 数据库信息
info = await db.info()
print(f"总键数: {info.key_count}")
print(f"存储大小: {info.storage_size}")
print(f"缓存命中率: {info.cache_hit_rate}")

# 性能指标
metrics = await db.metrics()
print(f"平均读延迟: {metrics.avg_read_latency}ms")
print(f"平均写延迟: {metrics.avg_write_latency}ms")

# 慢查询分析
slow_queries = await db.slow_queries(threshold=1.0)

# 数据库压缩
await db.compact()

# 缓存管理
await db.clear_cache()
await db.warm_cache(['frequently_used_key1', 'frequently_used_key2'])
```

### 7. 事件监听和钩子

```python
# 数据变更监听
@db.on_change('user:*')
async def handle_user_change(key, old_value, new_value):
    print(f"用户 {key} 数据发生变更")

# 生命周期钩子
@db.before_write
async def validate_data(key, value):
    if not is_valid(value):
        raise ValueError("数据格式不正确")

@db.after_read
async def log_access(key, value):
    logger.info(f"访问了键: {key}")
```

## 多模式架构支持

### 1. 本地模式 (Local Mode) - LevelDB 专用优化
```python
# 高性能本地存储 - LevelDB 专门优化
config = FlaxKVConfig(
    buffer_size=5000,
    cache_size="1GB", 
    compression=True,
    sync_mode="async"
)

db = FlaxKV("my_db", backend="local", location="./data/", config=config)
db['key'] = 'value'  # LevelDB 极致优化的本地存储

# LevelDB 专用操作
await db.compact_range("user:1000", "user:2000")  # 范围压缩
stats = db.leveldb_stats()  # LevelDB 内部统计
```

### 2. 远程模式 (Remote Mode) - ZMQ + HTTP 双协议
```python
# ZeroMQ 协议 - 极致性能 (推荐)
zmq_config = FlaxKVConfig(
    network_protocol="zmq",
    zmq_socket_type="REQ", 
    zmq_high_water_mark=10000,
    request_timeout=5.0
)

db = FlaxKV("my_db", 
    backend="remote", 
    location="zmq://db.example.com:5555",
    config=zmq_config
)
# 性能: 0.1-0.3ms 延迟，500K+ ops/s

# HTTP 协议 - 通用兼容性
http_config = FlaxKVConfig(
    network_protocol="http",
    http_version="2",           # HTTP/2 性能更好
    http_keepalive=True,
    http_compression=True,
    connection_pool_size=20
)

db = FlaxKV("my_db",
    backend="remote", 
    location="https://db.example.com:8000",
    config=http_config
)
# 性能: 1-3ms 延迟，60K-90K ops/s

# 智能协议选择
auto_config = FlaxKVConfig(network_protocol="auto")
db = FlaxKV("my_db", backend="remote", 
    location="https://db.example.com",  # 自动检测最优协议
    config=auto_config
)
```

### 3. 高可用模式 (第二阶段扩展)
```python
# 多节点高可用 (未来版本)
ha_config = FlaxKVConfig(
    network_protocol="zmq",  # 节点间使用 ZMQ 获得最高性能
    connection_pool_size=20,
    retry_attempts=3,
    failover_timeout=5.0
)

db = FlaxKV("my_db", backend="remote", 
    location=[
        "zmq://primary.example.com:5555",
        "zmq://backup1.example.com:5555", 
        "zmq://backup2.example.com:5555"
    ],
    config=ha_config
)

# 自动故障转移
db['key'] = 'value'  # 主节点故障时自动切换备用节点
```

### 4. 混合模式 (Hybrid Mode) - 本地缓存 + 远程存储
```python
# 本地 LevelDB 缓存 + 远程 ZMQ 存储
hybrid_config = FlaxKVConfig(
    # 本地缓存配置
    cache_size="2GB",
    buffer_size=10000,
    # 远程存储配置  
    network_protocol="zmq",
    request_timeout=10.0,
    # 混合模式配置
    cache_write_through=True,  # 写穿透模式
    cache_ttl=3600            # 缓存1小时过期
)

db = FlaxKV("my_db", 
    backend="hybrid",
    location={
        "cache": "./local_cache/",
        "storage": "zmq://storage.example.com:5555"
    },
    config=hybrid_config
)
```

## 服务器端架构

### 1. 现代异步服务器
```python
# 多协议异步服务器
from flaxkv.server import FlaxKVServer

# 多协议服务器配置
server_config = {
    # 数据库配置
    'databases': {
        'main': './data/main',
        'cache': './data/cache', 
        'sessions': './data/sessions'
    },
    
    # LevelDB 专用配置
    'leveldb_config': {
        'block_cache_size': 1024 * 1024 * 1024,  # 1GB
        'write_buffer_size': 64 * 1024 * 1024,   # 64MB
        'bloom_filter_bits': 10,
        'compression': 'snappy'
    },
    
    # 认证和中间件
    'auth': JWTAuth(),
    'middleware': [CORSMiddleware(), RateLimitMiddleware()],
    'monitoring': PrometheusMonitoring()
}

server = FlaxKVServer(**server_config)

# 启动双协议服务 (ZeroMQ + HTTP)
await asyncio.gather(
    # ZeroMQ 服务 (极致性能 - 主推)
    server.start_zmq(bind="tcp://*:5555"),
    
    # HTTP 服务 (通用兼容性)
    server.start_http(host="0.0.0.0", port=8000)
)
```

### 2. RESTful API 设计
```
GET    /api/v2/db/{db_name}/keys/{key}      # 获取单个键
PUT    /api/v2/db/{db_name}/keys/{key}      # 设置单个键
DELETE /api/v2/db/{db_name}/keys/{key}      # 删除单个键

POST   /api/v2/db/{db_name}/batch           # 批量操作
GET    /api/v2/db/{db_name}/range           # 范围查询
POST   /api/v2/db/{db_name}/transaction     # 事务操作

GET    /api/v2/db/{db_name}/info            # 数据库信息
POST   /api/v2/db/{db_name}/backup          # 数据备份
POST   /api/v2/db/{db_name}/compact         # 数据压缩

WebSocket /ws/v2/db/{db_name}/changes       # 实时数据变更流

# 协议选择建议 (第一阶段)
ZeroMQ    zmq://host:5555                   # 极致性能 (局域网推荐)
HTTP      https://host:8000/api/v2          # 通用兼容性 (广域网推荐)
```

### 3. 双协议性能对比
```
协议         延迟(P99)    吞吐量        CPU占用    推荐场景
ZeroMQ       0.1-0.3ms    500K+ ops/s   极低       高性能计算、实时系统
HTTP/2       1.0-3.0ms    60-90K ops/s  中等       Web 应用、微服务
HTTP/3       0.8-2.0ms    80-120K ops/s 中等       移动应用、CDN 边缘

智能选择策略:
- 内网/局域网 → 自动选择 ZeroMQ
- 公网/广域网 → 自动选择 HTTP  
- 调试/开发 → 强制使用 HTTP
```

### 4. niquests 客户端优势
```python
# niquests vs 其他 HTTP 客户端
client_comparison = {
    "niquests": {
        "API兼容": "100% requests 兼容",
        "学习成本": "零成本 (直接替换)",
        "HTTP版本": "HTTP/1.1, HTTP/2, HTTP/3",
        "性能": "比 requests 快 2-3 倍",
        "异步": "原生支持 + 同步兼容",
        "内存": "更低内存占用"
    },
    
    "httpx": {
        "API兼容": "全新 API 设计",
        "学习成本": "需要学习新语法", 
        "HTTP版本": "HTTP/1.1, HTTP/2",
        "性能": "比 requests 快 1.5-2 倍",
        "异步": "异步优先设计"
    }
}

# FlaxKV 客户端实现
class OptimizedHTTPClient:
    def __init__(self, base_url: str):
        import niquests
        
        self.session = niquests.Session()
        self.session.mount("http://", niquests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=100,
            pool_block=False
        ))
        
        # 自动启用 HTTP/3 (如果服务器支持)
        self.session.headers.update({
            "Accept": "application/msgpack, application/json",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        })
    
    async def get(self, key: str):
        """零学习成本的异步请求"""
        response = await self.session.get(f"{self.base_url}/keys/{key}")
        
        # 智能解码
        if response.headers.get("content-type") == "application/msgpack":
            return msgspec.msgpack.decode(response.content)
        return response.json()
    
    def get_sync(self, key: str):
        """同步兼容 API"""  
        response = self.session.get(f"{self.base_url}/keys/{key}")
        return response.json()

### 5. 完整的双协议服务器实现

#### ZeroMQ 服务器实现
```python
import asyncio
import zmq
import zmq.asyncio
import msgspec
import uuid
import time
from typing import Dict, Any, Optional
import logging

class ZeroMQServer:
    """高性能 ZeroMQ 服务器"""
    
    def __init__(self, databases: Dict[str, 'LevelDBEngine'], config: Dict[str, Any]):
        self.databases = databases
        self.config = config
        self.context = zmq.asyncio.Context()
        self.stats = {
            "requests_total": 0,
            "requests_success": 0,
            "requests_error": 0,
            "response_times": []
        }
        
    async def start(self, bind_address: str = "tcp://*:5555"):
        """启动ZMQ服务器"""
        socket = self.context.socket(zmq.REP)
        
        # 性能优化配置
        socket.setsockopt(zmq.SNDHWM, 10000)  # 发送队列大小
        socket.setsockopt(zmq.RCVHWM, 10000)  # 接收队列大小
        socket.setsockopt(zmq.LINGER, 0)      # 关闭延迟
        
        socket.bind(bind_address)
        
        logging.info(f"🚀 ZeroMQ 服务器启动: {bind_address}")
        
        try:
            while True:
                # 接收请求
                request_data = await socket.recv()
                
                # 处理请求
                response_data = await self._handle_request(request_data)
                
                # 发送响应
                await socket.send(response_data)
                
        except Exception as e:
            logging.error(f"ZMQ服务器错误: {e}")
        finally:
            socket.close()
    
    async def _handle_request(self, request_data: bytes) -> bytes:
        """处理ZMQ请求"""
        start_time = time.perf_counter()
        self.stats["requests_total"] += 1
        
        try:
            # 解析请求
            request = msgspec.msgpack.decode(request_data)
            
            # 路由到处理函数
            operation = request.get("op")
            data = request.get("data", {})
            
            if operation == "get":
                result = await self._handle_get(data)
            elif operation == "set": 
                result = await self._handle_set(data)
            elif operation == "delete":
                result = await self._handle_delete(data)
            elif operation == "batch_get":
                result = await self._handle_batch_get(data)
            elif operation == "batch_set":
                result = await self._handle_batch_set(data)
            elif operation == "range_query":
                result = await self._handle_range_query(data)
            elif operation == "transaction":
                result = await self._handle_transaction(data)
            else:
                raise ValueError(f"未知操作: {operation}")
            
            # 成功响应
            response = {
                "success": True,
                "result": result,
                "request_id": request.get("id")
            }
            
            self.stats["requests_success"] += 1
            
        except Exception as e:
            # 错误响应
            response = {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "request_id": request.get("id")
            }
            
            self.stats["requests_error"] += 1
            logging.error(f"处理请求错误: {e}")
        
        # 记录响应时间
        response_time = (time.perf_counter() - start_time) * 1000
        self.stats["response_times"].append(response_time)
        
        # 保持最近1000次请求的响应时间
        if len(self.stats["response_times"]) > 1000:
            self.stats["response_times"] = self.stats["response_times"][-1000:]
        
        return msgspec.msgpack.encode(response)
    
    async def _handle_get(self, data: Dict[str, Any]) -> Any:
        """处理GET请求"""
        db_name = data["db_name"]
        key = data["key"]
        
        if db_name not in self.databases:
            raise KeyError(f"数据库不存在: {db_name}")
        
        db = self.databases[db_name]
        return await db.get(key)
    
    async def _handle_batch_get(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """处理批量GET请求"""
        db_name = data["db_name"]
        keys = data["keys"]
        
        if db_name not in self.databases:
            raise KeyError(f"数据库不存在: {db_name}")
        
        db = self.databases[db_name]
        return await db.batch_get(keys)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取服务器统计信息"""
        response_times = self.stats["response_times"]
        
        if response_times:
            avg_time = sum(response_times) / len(response_times)
            p99_time = sorted(response_times)[int(len(response_times) * 0.99)]
        else:
            avg_time = 0
            p99_time = 0
        
        return {
            "requests_total": self.stats["requests_total"],
            "requests_success": self.stats["requests_success"],
            "requests_error": self.stats["requests_error"],
            "success_rate": (self.stats["requests_success"] / 
                           max(self.stats["requests_total"], 1) * 100),
            "avg_response_time_ms": avg_time,
            "p99_response_time_ms": p99_time
        }

class OptimizedHTTPServer:
    """优化的HTTP服务器"""
    
    def __init__(self, databases: Dict[str, 'LevelDBEngine']):
        from starlette.applications import Starlette
        from starlette.routing import Route
        from starlette.responses import Response
        import uvloop
        
        # 启用uvloop
        uvloop.install()
        
        self.databases = databases
        self.encoder = msgspec.msgpack.Encoder()
        self.decoder = msgspec.msgpack.Decoder()
        
        self.app = Starlette(routes=[
            Route("/api/v2/db/{db_name}/keys/{key}", self.handle_key, 
                  methods=["GET", "PUT", "DELETE"]),
            Route("/api/v2/db/{db_name}/batch", self.handle_batch, 
                  methods=["POST"]),
            Route("/api/v2/db/{db_name}/range", self.handle_range, 
                  methods=["GET"]),
            Route("/api/v2/stats", self.handle_stats, methods=["GET"]),
        ])
    
    async def handle_key(self, request):
        """处理单键操作"""
        db_name = request.path_params["db_name"]
        key = request.path_params["key"]
        
        if db_name not in self.databases:
            return Response("Database not found", status_code=404)
        
        db = self.databases[db_name]
        
        try:
            if request.method == "GET":
                value = await db.get(key)
                data = self.encoder.encode(value)
                return Response(data, media_type="application/msgpack")
            
            elif request.method == "PUT":
                body = await request.body()
                value = self.decoder.decode(body)
                await db.set(key, value)
                return Response("OK")
            
            elif request.method == "DELETE":
                await db.delete(key)
                return Response("OK")
                
        except KeyError:
            return Response("Key not found", status_code=404)
        except Exception as e:
            return Response(f"Error: {e}", status_code=500)
    
    async def start(self, host: str = "0.0.0.0", port: int = 8000):
        """启动HTTP服务器"""
        import uvicorn
        
        config = uvicorn.Config(
            app=self.app,
            host=host,
            port=port,
            loop="uvloop",
            http="httptools",
            access_log=False,  # 关闭访问日志提升性能
            backlog=2048,
        )
        
        server = uvicorn.Server(config)
        await server.serve()
```

## 安全性设计

### 1. 完整的JWT认证系统
```python
import jwt
import time
from typing import Optional, Dict, Any
from functools import wraps

class JWTAuth:
    """JWT认证管理器"""
    
    def __init__(self, secret_key: str, algorithm: str = "HS256"):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.token_expiry = 3600  # 1小时
    
    def generate_token(self, user_id: str, permissions: list = None) -> str:
        """生成JWT令牌"""
        payload = {
            "user_id": user_id,
            "permissions": permissions or ["read"],
            "exp": time.time() + self.token_expiry,
            "iat": time.time()
        }
        
        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
    
    def verify_token(self, token: str) -> Optional[Dict[str, Any]]:
        """验证JWT令牌"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except jwt.ExpiredSignatureError:
            raise AuthenticationError("令牌已过期")
        except jwt.InvalidTokenError:
            raise AuthenticationError("无效令牌")

def require_permission(permission: str):
    """权限装饰器"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # 从上下文获取用户权限
            user_permissions = get_current_user_permissions()
            
            if permission not in user_permissions:
                raise PermissionError(f"需要权限: {permission}")
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator

class AuthenticationError(Exception):
    pass

# 使用示例
auth = JWTAuth(secret_key="your-secret-key")

# 生成令牌
token = auth.generate_token("user123", ["read", "write"])

# 验证令牌
user_info = auth.verify_token(token)

# 权限控制使用
@require_permission('read')
async def get_user_data(user_id):
    return db[f'user:{user_id}']

@require_permission('write')  
async def update_user_data(user_id, data):
    db[f'user:{user_id}'] = data
```

### 2. 数据加密和安全传输
```python
# 静态数据加密
db = FlaxKV("encrypted_db", 
    config=FlaxKVConfig(
        encryption_key="your-secret-key",
        encryption_algorithm="AES-256-GCM"
    )
)

# ZeroMQ 安全传输 (CurveZMQ)
import zmq.auth

class SecureZMQTransport:
    """安全的ZMQ传输"""
    
    def __init__(self, server_key: str, client_key: str):
        # CurveZMQ 加密配置
        self.server_public, self.server_secret = zmq.auth.create_certificates("./certs", "server")
        self.client_public, self.client_secret = zmq.auth.create_certificates("./certs", "client")
    
    def create_secure_server(self, bind_endpoint: str):
        """创建加密的ZMQ服务器"""
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        
        # 启用CurveZMQ加密
        socket.curve_server = True
        socket.curve_secretkey = self.server_secret
        
        socket.bind(bind_endpoint)
        return socket
    
    def create_secure_client(self, server_endpoint: str):
        """创建加密的ZMQ客户端"""
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        
        # 配置客户端加密
        socket.curve_serverkey = self.server_public
        socket.curve_publickey = self.client_public
        socket.curve_secretkey = self.client_secret
        
        socket.connect(server_endpoint)
        return socket

# HTTPS 自动配置
secure_config = FlaxKVConfig(
    network_protocol="http",
    verify_ssl=True,
    auth_token="eyJhbGciOiJIUzI1NiIs..."
)

db = FlaxKV("secure_remote", 
    backend="remote",
    location="https://secure.example.com:8000",
    config=secure_config
)
```

### 3. 错误处理和监控系统
```python
from enum import Enum
import logging
from typing import Dict, Any
import traceback

class ErrorCategory(Enum):
    """错误分类"""
    NETWORK = "network"          # 网络错误
    STORAGE = "storage"          # 存储错误
    SERIALIZATION = "serialization"  # 序列化错误
    AUTHENTICATION = "auth"      # 认证错误
    PERMISSION = "permission"    # 权限错误
    VALIDATION = "validation"    # 数据验证错误
    SYSTEM = "system"           # 系统错误

class FlaxKVError(Exception):
    """FlaxKV基础异常类"""
    
    def __init__(self, message: str, category: ErrorCategory, 
                 details: Dict[str, Any] = None):
        super().__init__(message)
        self.message = message
        self.category = category
        self.details = details or {}
        self.timestamp = time.time()

class ErrorHandler:
    """错误处理器"""
    
    def __init__(self):
        self.error_stats = {}
    
    def handle_error(self, error: Exception, context: Dict[str, Any] = None):
        """统一错误处理"""
        error_info = {
            "type": type(error).__name__,
            "message": str(error),
            "timestamp": time.time(),
            "context": context or {},
            "traceback": traceback.format_exc()
        }
        
        # 记录错误统计
        error_type = error_info["type"]
        self.error_stats[error_type] = self.error_stats.get(error_type, 0) + 1
        
        # 记录日志
        logging.error(f"FlaxKV错误: {error_info}")
        
        # 错误恢复逻辑
        if isinstance(error, ConnectionError):
            return self._handle_connection_error(error, context)
        elif isinstance(error, KeyError):
            return self._handle_key_error(error, context)
        else:
            return self._handle_generic_error(error, context)
    
    def _handle_connection_error(self, error, context):
        """处理连接错误 - 重连逻辑"""
        pass
    
    def _handle_key_error(self, error, context):
        """处理键不存在错误"""
        pass

# 监控指标收集
class MetricsCollector:
    """指标收集器"""
    
    def __init__(self):
        self.metrics = {
            "requests_total": 0,
            "requests_success": 0, 
            "requests_error": 0,
            "response_times": [],
            "cache_hits": 0,
            "cache_misses": 0
        }
    
    def record_request(self, success: bool, response_time: float):
        """记录请求指标"""
        self.metrics["requests_total"] += 1
        
        if success:
            self.metrics["requests_success"] += 1
        else:
            self.metrics["requests_error"] += 1
        
        self.metrics["response_times"].append(response_time)
        
        # 保持最近1000次记录
        if len(self.metrics["response_times"]) > 1000:
            self.metrics["response_times"] = self.metrics["response_times"][-1000:]
    
    def get_prometheus_metrics(self) -> str:
        """获取Prometheus格式指标"""
        response_times = self.metrics["response_times"]
        
        if response_times:
            avg_time = sum(response_times) / len(response_times)
            p99_time = sorted(response_times)[int(len(response_times) * 0.99)]
        else:
            avg_time = 0
            p99_time = 0
        
        return f"""
# HELP flaxkv_requests_total Total number of requests
# TYPE flaxkv_requests_total counter
flaxkv_requests_total {self.metrics["requests_total"]}

# HELP flaxkv_response_time_avg Average response time in milliseconds  
# TYPE flaxkv_response_time_avg gauge
flaxkv_response_time_avg {avg_time}

# HELP flaxkv_response_time_p99 99th percentile response time
# TYPE flaxkv_response_time_p99 gauge  
flaxkv_response_time_p99 {p99_time}

# HELP flaxkv_cache_hit_rate Cache hit rate
# TYPE flaxkv_cache_hit_rate gauge
flaxkv_cache_hit_rate {self.metrics["cache_hits"] / max(self.metrics["cache_hits"] + self.metrics["cache_misses"], 1)}
"""
```

### 4. 版本迁移工具
```python
class FlaxKVMigrator:
    """FlaxKV版本迁移工具"""
    
    def __init__(self, old_db_path: str, new_db_path: str):
        self.old_db_path = old_db_path
        self.new_db_path = new_db_path
    
    async def migrate(self) -> Dict[str, Any]:
        """执行迁移"""
        migration_stats = {
            "total_keys": 0,
            "migrated_keys": 0,
            "failed_keys": 0,
            "start_time": time.time(),
            "errors": []
        }
        
        try:
            # 1. 打开旧数据库
            old_db = self._open_old_database()
            
            # 2. 创建新数据库
            new_db = await self._create_new_database()
            
            # 3. 迁移数据
            async for key, value in self._iterate_old_data(old_db):
                migration_stats["total_keys"] += 1
                
                try:
                    await new_db.set(key, value)
                    migration_stats["migrated_keys"] += 1
                except Exception as e:
                    migration_stats["failed_keys"] += 1
                    migration_stats["errors"].append({
                        "key": key,
                        "error": str(e)
                    })
                
                # 每1000个键报告一次进度
                if migration_stats["total_keys"] % 1000 == 0:
                    print(f"已迁移 {migration_stats['migrated_keys']} / {migration_stats['total_keys']} 键")
            
            # 4. 验证迁移结果
            await self._verify_migration(old_db, new_db)
            
            migration_stats["end_time"] = time.time()
            migration_stats["duration"] = migration_stats["end_time"] - migration_stats["start_time"]
            
            return migration_stats
            
        except Exception as e:
            migration_stats["errors"].append({"fatal_error": str(e)})
            raise
```

## 开发工具和生态

### 1. 命令行工具
```bash
# 数据库管理
flaxkv create mydb --backend leveldb
flaxkv info mydb
flaxkv backup mydb --output backup.flaxkv
flaxkv restore mydb --input backup.flaxkv

# 服务器管理
flaxkv server start --config server.yaml
flaxkv server status
flaxkv server stop

# 数据操作
flaxkv get mydb key1
flaxkv set mydb key1 value1
flaxkv scan mydb --prefix "user:"

# 性能测试
flaxkv benchmark --operations 100000 --concurrency 10
```

### 2. 配置文件支持
```yaml
# flaxkv.yaml
databases:
  main:
    backend: leveldb
    location: "./data/main"
    config:
      buffer_size: 5000
      cache_size: "1GB"
      compression: true
      
  cache:
    backend: local                    # ✅ 统一使用 local (LevelDB)
    location: "./data/cache" 
    config:
      buffer_size: 1000
      cache_size: "256MB"
      sync_mode: "async"
      leveldb_options:
        block_cache_size: 268435456   # 256MB
        write_buffer_size: 33554432   # 32MB
        bloom_filter_bits: 10

server:
  host: "0.0.0.0"
  port: 8000
  protocols: ["http", "websocket"]
  
auth:
  type: "jwt"
  secret_key: "${FLAXKV_JWT_SECRET}"
  
monitoring:
  metrics_enabled: true
  prometheus_endpoint: "/metrics"
```

### 3. 监控和可观察性
```python
# Prometheus 指标集成
from flaxkv.monitoring import PrometheusExporter

exporter = PrometheusExporter()
db = FlaxKV("monitored_db", monitoring=exporter)

# 自定义指标
@db.custom_metric("user_operations")
async def handle_user_operation(user_id, operation):
    # 业务逻辑
    pass

# OpenTelemetry 分布式追踪
from flaxkv.tracing import OpenTelemetryTracer

tracer = OpenTelemetryTracer()
db = FlaxKV("traced_db", tracer=tracer)
```

## 实施路线图

### 阶段 1: 核心架构 (4-6 周)
**目标**: 建立新的分层架构和核心接口

**主要任务**:
- [ ] 设计并实现分层架构
- [ ] 实现统一的 FlaxKV 接口
- [ ] 完成配置管理系统
- [ ] 实现基础的存储引擎抽象
- [ ] 完成 LevelDB 引擎实现

**交付标准**:
- 基本的字典操作正常工作
- 配置系统完整可用
- 单元测试覆盖率 > 80%

### 阶段 2: 核心功能 (6-8 周)
**目标**: 实现事务、缓存、缓冲等核心功能

**主要任务**:
- [ ] 实现 ACID 事务支持
- [ ] 完成智能缓存层
- [ ] 实现高性能缓冲层
- [ ] 添加 TTL 过期机制
- [ ] 实现范围查询和批量操作

**交付标准**:
- 事务功能完整可用
- 性能测试达标 (读写延迟 < 1ms)
- 支持基本的查询操作

### 阶段 3: 双协议服务端 (3-4 周)
**目标**: 实现 ZeroMQ + HTTP 双协议架构

**主要任务**:
- [ ] ZeroMQ 服务器实现 (pyzmq + 异步处理)
- [ ] HTTP 服务器实现 (Starlette + uvicorn + uvloop)
- [ ] niquests 客户端实现 (HTTP/3 支持)
- [ ] ZeroMQ 客户端实现 (连接池管理)
- [ ] 智能协议选择器 (自动检测网络环境)
- [ ] msgspec 序列化优化

**交付标准**:
- ZeroMQ 性能达到 300K+ ops/s
- HTTP 性能达到 60K+ ops/s  
- 双协议无缝切换
- 完整的错误处理和重试机制

### 阶段 4: 高级特性 (4-5 周)
**目标**: 实现监控、安全、运维等高级特性

**主要任务**:
- [ ] 监控指标和可观察性
- [ ] 认证授权系统
- [ ] 数据加密支持
- [ ] 备份恢复机制
- [ ] 命令行工具

**交付标准**:
- 生产级监控能力
- 基本安全保障
- 完整的运维工具

### 阶段 5: 生态和优化 (3-4 周)
**目标**: 完善生态系统和性能优化

**主要任务**:
- [ ] 性能调优和基准测试
- [ ] 文档和示例完善
- [ ] 插件系统设计
- [ ] 集群模式支持
- [ ] CI/CD 流水线

**交付标准**:
- 性能达到设计目标
- 文档完整易用
- 发布生产版本

## 技术栈选择

### 核心依赖
- **Python 3.10+**: 使用最新语言特性和类型提示
- **asyncio**: 异步 I/O 支持
- **pydantic**: 数据验证和配置管理  
- **msgspec**: 高性能序列化 (替代 msgpack/orjson)
- **plyvel**: LevelDB Python 绑定 (唯一后端)

### 网络传输 (第一阶段)
- **pyzmq**: ZeroMQ Python 绑定 (主协议 - 极致性能)
- **starlette**: 轻量 ASGI 框架 (HTTP 服务端)
- **uvicorn**: 高性能 ASGI 服务器 
- **niquests**: 现代 HTTP 客户端 (HTTP/3 原生支持)

### 高性能优化
- **uvloop**: 高性能事件循环 (Linux)
- **cython**: 性能关键代码优化
- **numba**: JIT 编译优化
- **snappy-python**: Snappy 压缩算法

### 监控和工具
- **prometheus-client**: Prometheus 指标
- **opentelemetry**: 分布式追踪
- **rich**: 美观的命令行输出
- **typer**: 现代 CLI 框架

### 开发工具
- **pytest**: 测试框架
- **black**: 代码格式化
- **mypy**: 静态类型检查
- **pre-commit**: Git 钩子

## 成功指标

### 分场景性能基准

#### 本地访问性能 (同机器/同进程)
- **ZeroMQ 读延迟** < 0.2ms (P99)
- **ZeroMQ 写延迟** < 0.5ms (P99)  
- **ZeroMQ 吞吐量** > 300K ops/s
- **目标**: 接近内存字典性能

#### 局域网访问性能 (< 5ms网络延迟)
- **ZeroMQ 读延迟** < 3ms (P99)
- **ZeroMQ 写延迟** < 5ms (P99)
- **HTTP 读延迟** < 8ms (P99)
- **HTTP 写延迟** < 12ms (P99)
- **ZeroMQ 吞吐量** > 50K ops/s
- **HTTP 吞吐量** > 20K ops/s

#### 广域网访问性能 (50ms+ 网络延迟)
- **协议开销** < 总延迟的10%
- **ZeroMQ 协议开销** < 0.5ms
- **HTTP 协议开销** < 5ms
- **目标**: 协议层不成为性能瓶颈

#### 批量操作性能
- **1000键批量读取**:
  - 本地: < 10ms
  - 局域网: < 50ms
  - 广域网: < 网络延迟 + 20ms
- **批量写入吞吐量** > 50K ops/s (本地)

### 可靠性指标
- **缓存命中率** > 95%
- **内存使用** 稳定，无泄漏
- **API 兼容性** 100% (向后兼容)
- **事务成功率** > 99.99%
- **数据一致性** 100%
- **故障恢复时间** < 10秒
- **智能协议选择准确率** > 90%

### 质量指标
- **单元测试覆盖率** > 90%
- **集成测试覆盖率** > 80%
- **文档覆盖率** 100%
- **零安全漏洞**

### 实际基准验证
```python
# 性能基准验证代码
PERFORMANCE_BENCHMARKS = {
    "本地访问": {
        "环境": "同进程/同机器",
        "ZeroMQ目标": {
            "读延迟_P99": "< 0.2ms",
            "写延迟_P99": "< 0.5ms", 
            "吞吐量": "> 300K ops/s"
        }
    },
    
    "局域网访问": {
        "环境": "同网段，<5ms网络延迟",
        "ZeroMQ目标": "< 3ms读，< 5ms写",
        "HTTP目标": "< 8ms读，< 12ms写"
    },
    
    "广域网访问": {
        "环境": "跨地域，50ms+网络延迟",
        "目标": "协议开销占总延迟 < 10%"
    }
}
```

FlaxKV 2.0 将成为 Python 生态中最优秀的持久化字典解决方案，在保持简单易用的同时，提供企业级的性能和可靠性。