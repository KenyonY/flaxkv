# FlaxKV2 键值编码详解

本文档详细说明普通键值对和auto_nested键值对的编码方式。

---

## 📋 目录

1. [键（Key）的编码](#键key的编码)
2. [值（Value）的编码](#值value的编码)
3. [普通键值对的存储](#普通键值对的存储)
4. [auto_nested键值对的存储](#auto_nested键值对的存储)
5. [完整示例对比](#完整示例对比)

---

## 1. 键（Key）的编码

### encode_key() 函数

键的编码需要保证**顺序性质**，特别是对于数值类型。

```python
def encode_key(key: Any) -> bytes:
    """将键编码为二进制数据"""
```

### 编码格式

| 类型 | 前缀 | 编码方式 | 示例 |
|-----|------|---------|------|
| **str** | `b's'` | UTF-8编码 | `'user'` → `b's' + b'user'` |
| **int** | `b'i'` | 8字节大端序 | `123` → `b'i' + (8字节)` |
| **float** | `b'f'` | 8字节double | `3.14` → `b'f' + (8字节)` |
| **bytes** | `b'b'` | 原样 | `b'data'` → `b'b' + b'data'` |
| **tuple** | `b't'` | 递归编码 | `('a', 1)` → `b't' + ...` |
| **其他** | `b'o'` | str()转换 | `object` → `b'o' + str(object).encode()` |

### 示例

```python
# 字符串键
encode_key('user')
# → b's' + b'user'
# → b'suser'

# 整数键
encode_key(123)
# → b'i' + b'\x00\x00\x00\x00\x00\x00\x00{'

# 特殊前缀键
encode_key('__nested__:user')
# → b's' + b'__nested__:user'
# → b's__nested__:user'
```

---

## 2. 值（Value）的编码

### 2.1 基础编码（无TTL）

#### encode() 函数

值的编码使用**类型标识**来支持多种数据类型。

```python
def encode(value: Any) -> bytes:
    """将Python对象编码为二进制数据"""
```

#### 类型标识

| 类型 | 标识字节 | 说明 |
|-----|---------|------|
| **msgpack** | `0x00` | 普通类型（int, str, list, dict等） |
| **numpy** | `0x01` | NumPy数组 |
| **pandas** | `0x02` | Pandas DataFrame |
| **pickle** | `0x03` | 复杂对象（fallback） |

#### 编码格式

```
[TYPE_BYTE(1byte)][SERIALIZED_DATA]
```

#### 示例

```python
# 字符串
encode('Alice')
# → b'\x00' + msgpack.packb('Alice')
# → b'\x00\xa5Alice'  (msgpack格式)

# 字典
encode({'name': 'Alice', 'age': 30})
# → b'\x00' + msgpack.packb(...)
# → b'\x00\x82\xa4name\xa5Alice\xa3age\x1e'
```

### 2.2 内嵌TTL编码（新设计）

#### ValueWithMeta.encode_value()

从TTL重构后，值可以内嵌TTL信息。

```python
def encode_value(value: Any, ttl_seconds: Optional[int]) -> bytes:
    """编码值（可选TTL）"""
```

#### 编码格式

```
无TTL: [VERSION(1byte)][FLAGS(1byte)][VALUE_BYTES]
有TTL: [VERSION(1byte)][FLAGS(1byte)][EXPIRE_TIME(8bytes)][VALUE_BYTES]
```

- **VERSION**: `0x01` (当前版本)
- **FLAGS**:
  - bit0: TTL标志 (1=有TTL, 0=无TTL)
  - bit1-7: 保留
- **EXPIRE_TIME**: 8字节double（Unix时间戳）
- **VALUE_BYTES**: encode()的结果

#### 示例

```python
# 无TTL
ValueWithMeta.encode_value('Alice', ttl=None)
# → b'\x01\x00' + encode('Alice')
# → b'\x01\x00\x00\xa5Alice'

# 有TTL (60秒)
ValueWithMeta.encode_value('Alice', ttl=60)
# → b'\x01\x01' + struct.pack('d', expire_time) + encode('Alice')
# → b'\x01\x01' + (8字节时间戳) + b'\x00\xa5Alice'
```

---

## 3. 普通键值对的存储

### 场景1：普通字符串（无TTL）

```python
db['name'] = 'Alice'
```

#### 存储在LevelDB中：

| Key (bytes) | Value (bytes) |
|------------|---------------|
| `b'sname'` | `b'\x01\x00\x00\xa5Alice'` |

**解析**：
- Key: `b's'` + `b'name'` = `b'sname'`
- Value:
  - `\x01` = VERSION
  - `\x00` = FLAGS (无TTL)
  - `\x00` = TYPE_MSGPACK
  - `\xa5Alice` = msgpack编码的字符串

### 场景2：普通字符串（有TTL）

```python
db.set('name', 'Alice', ttl=60)
```

#### 存储在LevelDB中：

| Key (bytes) | Value (bytes) |
|------------|---------------|
| `b'sname'` | `b'\x01\x01' + (8字节时间戳) + b'\x00\xa5Alice'` |

**解析**：
- Key: `b'sname'`
- Value:
  - `\x01` = VERSION
  - `\x01` = FLAGS (有TTL, bit0=1)
  - `(8字节)` = expire_time (例如：1730275200.0)
  - `\x00` = TYPE_MSGPACK
  - `\xa5Alice` = msgpack编码的字符串

### 场景3：字典（普通模式，无auto_nested）

```python
db['config'] = {'host': 'localhost', 'port': 8080}
```

#### 存储在LevelDB中：

| Key (bytes) | Value (bytes) |
|------------|---------------|
| `b'sconfig'` | `b'\x01\x00\x00' + msgpack({...})` |

**解析**：
- 整个字典作为一个value存储
- 修改任何字段都需要重新序列化整个字典

---

## 4. auto_nested键值对的存储

### 核心概念

当 `auto_nested=True` 时，字典值会被**拆分**成多个独立的键值对：

1. **Marker键**：标记这是一个嵌套字典
2. **数据键**：每个字段独立存储

### 场景1：嵌套字典（无TTL）

```python
db = RawLevelDBDict('test', auto_nested=True)
db['user'] = {'name': 'Alice', 'age': 30}
```

#### 存储在LevelDB中：

| Key (bytes) | Value (bytes) | 说明 |
|------------|---------------|------|
| `b's__nested__:user'` | `b'\x01\x00\x00\x01'` | **Marker**: 标记'user'是嵌套字典 |
| `b'user:name'` | `b'\x00\xa5Alice'` | **数据**: user的name字段 |
| `b'user:age'` | `b'\x00\x1e'` | **数据**: user的age字段 |

**解析**：

1. **Marker键**：
   - Key: `b's' + b'__nested__:user'`
   - Value: `b'\x01\x00'` (VERSION + FLAGS无TTL) + `b'\x00\x01'` (msgpack编码的True)

2. **数据键1**：
   - Key: `b'user:name'` (直接UTF-8，无类型前缀)
   - Value: `b'\x00\xa5Alice'` (TYPE_MSGPACK + msgpack字符串)

3. **数据键2**：
   - Key: `b'user:age'` (直接UTF-8，无类型前缀)
   - Value: `b'\x00\x1e'` (TYPE_MSGPACK + msgpack整数30)

### 场景2：嵌套字典（有TTL）

```python
db = RawLevelDBDict('test', auto_nested=True)
db.set('user', {'name': 'Alice', 'age': 30}, ttl=60)
```

#### 存储在LevelDB中：

| Key (bytes) | Value (bytes) | 说明 |
|------------|---------------|------|
| `b's__nested__:user'` | `b'\x01\x01' + (8字节) + b'\x00\x01'` | **Marker**: 带TTL |
| `b'user:name'` | `b'\x00\xa5Alice'` | **数据**: name字段（无TTL） |
| `b'user:age'` | `b'\x00\x1e'` | **数据**: age字段（无TTL） |

**关键设计**：
- **TTL只在Marker中**：子字段不单独设置TTL
- **整体过期**：当Marker过期时，整个嵌套字典过期
- **性能优势**：子字段无需检查TTL

### 场景3：多层嵌套

```python
db = RawLevelDBDict('test', auto_nested=True)
db['app'] = {
    'database': {
        'host': 'localhost',
        'port': 5432
    }
}
```

#### 存储在LevelDB中：

| Key (bytes) | Value (bytes) | 说明 |
|------------|---------------|------|
| `b's__nested__:app'` | `b'\x01\x00\x00\x01'` | **Marker**: app是嵌套字典 |
| `b's__nested__:app:database'` | `b'\x01\x00\x00\x01'` | **Marker**: database是嵌套字典 |
| `b'app:database:host'` | `b'\x00\xaalocalhost'` | **数据**: host字段 |
| `b'app:database:port'` | `b'\x00\xcd\x15\x3a'` | **数据**: port字段 |

---

## 5. 完整示例对比

### 示例：存储用户信息

```python
db = RawLevelDBDict('test', auto_nested=True)
db.set('user:1', {
    'name': 'Alice',
    'age': 30,
    'email': 'alice@example.com'
}, ttl=3600)
```

### LevelDB存储布局

```
┌─────────────────────────────────────────────────────────────┐
│ LevelDB Database                                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│ Key: b's__nested__:user:1'                                  │
│ Value: [0x01][0x01][expire_time(8bytes)][0x00][0x01]      │
│        └─VERSION └─TTL   └─过期时间     └─TYPE └─True      │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│ Key: b'user:1:name'                                        │
│ Value: [0x00][msgpack('Alice')]                            │
│        └─TYPE_MSGPACK                                       │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│ Key: b'user:1:age'                                         │
│ Value: [0x00][msgpack(30)]                                 │
│        └─TYPE_MSGPACK                                       │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│ Key: b'user:1:email'                                       │
│ Value: [0x00][msgpack('alice@example.com')]                │
│        └─TYPE_MSGPACK                                       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 读取流程

```python
# 1. 读取 user:1
value = db['user:1']

# 内部流程：
# Step 1: 检查 '__nested__:user:1' marker
marker_key = encode_key('__nested__:user:1')
marker_value = db._db.get(marker_key)

# Step 2: 解码marker并检查TTL
_, expire_time, is_expired = decode_value(marker_value)
if is_expired:
    raise KeyError('user:1')  # 已过期

# Step 3: marker存在且未过期，返回NestedDBDict
prefix_db = db._db.prefixed_db(b'user:1:')
return NestedDBDict(prefix_db, 'user:1:', ...)

# 2. 读取 user:1 的 name 字段
name = db['user:1']['name']

# 内部流程：
# Step 1: NestedDBDict使用prefixed_db查询
value_bytes = prefix_db.get(b'name')  # 实际查询 'user:1:name'

# Step 2: 解码value（注意：子字段无TTL）
value = decode(value_bytes)  # 'Alice'
```

### 修改单个字段

```python
# 修改age字段
db['user:1']['age'] = 31

# 优势：
# 1. 只序列化31这个值
# 2. 只写入 'user:1:age' 这一个key
# 3. 不影响其他字段
```

---

## 6. 关键编码对比总结

### 普通键值 vs auto_nested

| 维度 | 普通键值 | auto_nested |
|-----|---------|-------------|
| **存储Key数** | 1个 | 1个marker + N个字段 |
| **值的编码** | 完整字典 | 每个字段独立 |
| **TTL位置** | 内嵌在value中 | 内嵌在marker中 |
| **修改字段** | 重新序列化整个字典 | 只序列化该字段 |
| **读取字段** | 反序列化整个字典 | 只反序列化该字段 |
| **适用场景** | 小字典、读多写少 | 大字典、部分字段访问 |

### 键的编码格式对比

| 键类型 | 编码前 | 编码后 | 说明 |
|-------|-------|--------|------|
| **用户键** | `'name'` | `b'sname'` | 带类型前缀 |
| **Marker键** | `'__nested__:user'` | `b's__nested__:user'` | 带类型前缀 |
| **嵌套字段键** | `'user:name'` | `b'user:name'` | **无类型前缀** (UTF-8) |

**注意**：嵌套字段键（如`user:name`）直接使用UTF-8编码，**不添加类型前缀**，这是因为它们是通过`prefixed_db`访问的，不会与普通键冲突。

---

## 7. 实际字节示例

### 示例1：普通键值（无TTL）

```python
db['age'] = 30
```

**实际存储**：
```
Key:   73 61 67 65                        # 's' 'a' 'g' 'e'
Value: 01 00 00 1e                        # VERSION FLAGS TYPE_MSGPACK 30
```

### 示例2：嵌套键值（有TTL）

```python
db.set('user', {'name': 'Bob'}, ttl=60)
```

**实际存储**：

```
# Marker
Key:   73 5f 5f 6e 65 73 74 65 64 5f 5f 3a 75 73 65 72
       # 's' '_' '_' 'n' 'e' 's' 't' 'e' 'd' '_' '_' ':' 'u' 's' 'e' 'r'

Value: 01 01 41 d8 9f 3c a0 00 00 00 00 01
       # VERSION FLAGS EXPIRE_TIME(8bytes) TYPE_MSGPACK True

# 数据
Key:   75 73 65 72 3a 6e 61 6d 65
       # 'u' 's' 'e' 'r' ':' 'n' 'a' 'm' 'e'

Value: 00 a3 42 6f 62
       # TYPE_MSGPACK msgpack('Bob')
```

---

## 💡 总结

1. **普通键值**：一个逻辑操作 → 一个物理键值对
2. **auto_nested键值**：一个逻辑操作 → 1个marker + N个字段
3. **TTL内嵌**：TTL信息直接嵌入value字节流（普通键）或marker字节流（嵌套键）
4. **性能权衡**：
   - 普通模式：读写整体更快，适合小字典
   - auto_nested：部分访问更快，适合大字典

选择合适的模式取决于你的使用场景！
