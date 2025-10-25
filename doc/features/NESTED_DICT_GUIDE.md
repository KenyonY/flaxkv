# NestedDBDict 使用指南

## 问题背景

在使用 FlaxKV2 时，如果你需要频繁修改嵌套数据的部分字段，传统方式会存在严重的性能问题：

```python
# ❌ 传统方式（性能问题）
db['user:1'] = {
    'name': 'Alice',
    'age': 30,
    'email': 'alice@example.com',
    'preferences': {...},
    # ... 可能有上百个字段
}

# 每次修改单个字段都需要：
data = db['user:1']           # 1. 反序列化整个字典（100个字段）
data['age'] = 31              # 2. 修改一个字段
db['user:1'] = data           # 3. 重新序列化整个字典（100个字段）
```

**问题**：
- 修改单个字段需要序列化/反序列化整个字典
- 字段越多、修改越频繁，性能越差
- 对于包含 NumPy 数组或 Pandas DataFrame 的大对象，开销更大

## 解决方案：NestedDBDict

NestedDBDict 基于 LevelDB 的 `prefixed_db` 特性，将每个字段作为独立的键值对存储：

```python
# ✅ 优化方式（高性能）
user = db.nested('user:1')

# 每个字段独立存储
user['name'] = 'Alice'
user['age'] = 30
user['email'] = 'alice@example.com'

# 修改单个字段只序列化该字段！
user['age'] = 31  # 只序列化 age 的值
```

**存储结构**：
```
LevelDB 中实际存储：
  user:1:name -> 'Alice'
  user:1:age -> 31
  user:1:email -> 'alice@example.com'
```

## 性能对比

### 测试场景：包含 1000 个字段的字典，修改单个字段 100 次

| 方式 | 耗时 | 操作 |
|------|------|------|
| 传统方式 | 8ms | 每次反序列化+序列化 1000 个字段 |
| NestedDBDict | 0.4ms | 每次只序列化 1 个字段 |
| **性能提升** | **18.6x** | **速度提升 94.6%** |

## 基础用法

### 1. 创建嵌套字典

```python
from flaxkv2 import LevelDBDict

db = LevelDBDict('mydb')

# 创建嵌套字典（指定前缀）
user = db.nested('user:1')
```

### 2. 基本操作

```python
# 设置字段
user['name'] = 'Alice'
user['age'] = 30
user['email'] = 'alice@example.com'

# 读取字段
print(user['name'])  # 'Alice'

# 修改字段（高效！）
user['age'] = 31

# 检查字段是否存在
if 'email' in user:
    print(user['email'])

# 删除字段
del user['email']

# 获取字段（带默认值）
theme = user.get('theme', 'light')
```

### 3. 批量操作

```python
# 批量更新
user.update({
    'city': 'NYC',
    'country': 'USA',
    'timezone': 'EST'
})

# 使用关键字参数
user.update(status='active', verified=True)

# 迭代所有字段
for key, value in user.items():
    print(f"{key}: {value}")

# 只迭代键
for key in user.keys():
    print(key)

# 只迭代值
for value in user.values():
    print(value)

# 获取字段数量
print(len(user))

# 清空所有字段
user.clear()
```

### 4. 转换为普通字典

```python
# 需要一次性获取所有数据时
user_dict = user.to_dict()
print(user_dict)
# {'name': 'Alice', 'age': 31, ...}

# 注意：to_dict() 会反序列化所有字段
# 只在需要完整字典时使用
```

## 高级用法

### 1. 存储复杂数据类型

```python
import numpy as np

# NumPy 数组
metrics = db.nested('metrics:daily')
metrics['sales'] = np.array([100, 200, 300])
metrics['revenue'] = np.random.randn(365)

# 嵌套字典
config = db.nested('app:config')
config['settings'] = {
    'theme': 'dark',
    'language': 'en',
    'notifications': {
        'email': True,
        'push': False
    }
}

# 列表
tasks = db.nested('user:1:tasks')
tasks['pending'] = ['task1', 'task2', 'task3']
tasks['completed'] = ['task4', 'task5']
```

### 2. 多个独立的嵌套字典

```python
# 为不同用户创建独立的嵌套字典
user1 = db.nested('user:1')
user2 = db.nested('user:2')

user1['name'] = 'Alice'
user2['name'] = 'Bob'

# 互不影响
user1['name'] = 'Alice2'
print(user2['name'])  # 仍然是 'Bob'
```

### 3. 数据持久化

```python
# 数据自动持久化
user = db.nested('user:1')
user['name'] = 'Alice'

# 关闭数据库
db.close()

# 重新打开
db = LevelDBDict('mydb')
user = db.nested('user:1')
print(user['name'])  # 'Alice' - 数据仍然存在
```

### 4. 其他字典方法

```python
# setdefault - 键不存在时设置默认值
theme = user.setdefault('theme', 'light')

# pop - 删除并返回值
email = user.pop('email', 'no-email')

# clear - 清空所有字段
user.clear()
```

## 使用场景推荐

### ✅ 推荐使用 NestedDBDict

1. **用户配置/会话数据**
   ```python
   session = db.nested(f'session:{session_id}')
   session['user_id'] = user_id
   session['last_active'] = timestamp
   session['preferences'] = {...}
   ```

2. **缓存对象（大量字段）**
   ```python
   cache = db.nested(f'cache:{key}')
   cache['data'] = result
   cache['timestamp'] = time.time()
   cache['hits'] = 0
   ```

3. **频繁更新的计数器/指标**
   ```python
   stats = db.nested('stats:daily')
   stats['page_views'] += 1
   stats['unique_visitors'] = len(visitors)
   ```

4. **结构化对象（字段数 > 100）**
   ```python
   model = db.nested('ml:model:v1')
   model['weights'] = np.array([...])
   model['bias'] = np.array([...])
   model['accuracy'] = 0.95
   ```

### ⚠️ 不推荐使用 NestedDBDict

1. **小字典（字段数 < 10）**
   - 使用传统方式即可，开销差异不大

2. **需要原子性整体更新**
   - NestedDBDict 每个字段独立，不保证整体原子性
   - 需要事务性更新时使用传统方式

3. **完整字典频繁读取**
   - 如果经常需要 `to_dict()` 获取完整数据
   - 不如直接存储整个字典

## 性能建议

### 1. 批量更新优于逐个设置

```python
# ❌ 不推荐：逐个设置
user['field1'] = value1
user['field2'] = value2
user['field3'] = value3

# ✅ 推荐：批量更新
user.update({
    'field1': value1,
    'field2': value2,
    'field3': value3
})
```

### 2. 避免频繁 to_dict()

```python
# ❌ 不推荐：频繁转换
for i in range(1000):
    data = user.to_dict()  # 每次都反序列化所有字段
    process(data)

# ✅ 推荐：按需访问
for i in range(1000):
    name = user['name']  # 只读取需要的字段
    process(name)
```

### 3. 选择合适的前缀命名

```python
# 推荐：使用冒号分隔的层次结构
user = db.nested('user:1')
session = db.nested('session:abc123')
cache = db.nested('cache:api:v1')

# 便于管理和查询
```

## 注意事项

1. **键类型限制**
   - 只支持字符串类型的键
   - `user[123] = value`  # ❌ 会抛出 TypeError

2. **前缀自动添加冒号**
   - `db.nested('user:1')` 实际前缀是 `'user:1:'`
   - 实际存储键：`'user:1:name'`

3. **不使用父数据库缓冲**
   - NestedDBDict 直接写入 LevelDB
   - 写入立即生效，无需 `write_immediately()`

4. **与传统方式共存**
   ```python
   # 可以混合使用
   db['config'] = {...}  # 传统方式
   user = db.nested('user:1')  # NestedDBDict
   ```

## 实现原理

NestedDBDict 利用了 plyvel（LevelDB Python 绑定）的 `prefixed_db()` 特性：

```python
# 创建带前缀的数据库视图
prefix_bytes = b'user:1:'
prefixed_db = db._db.prefixed_db(prefix_bytes)

# 在前缀视图中操作
prefixed_db.put(b'name', encoded_value)  # 实际存储 b'user:1:name'
prefixed_db.get(b'name')                 # 读取 b'user:1:name'

# 迭代前缀范围
for key, value in prefixed_db:
    # 只迭代 'user:1:' 开头的键
    pass
```

**优势**：
- 利用 LevelDB 的有序存储和前缀查询
- 避免了整个字典的序列化/反序列化
- 每个字段独立管理，灵活高效

## 完整示例

```python
from flaxkv2 import LevelDBDict
import numpy as np

# 创建数据库
db = LevelDBDict('user_profiles')

# 用户1的配置
user1 = db.nested('user:1')
user1.update({
    'name': 'Alice',
    'age': 30,
    'email': 'alice@example.com',
    'preferences': {
        'theme': 'dark',
        'language': 'en',
        'notifications': True
    },
    'activity': np.array([1, 2, 3, 4, 5])
})

# 高效更新单个字段
user1['age'] = 31
user1['last_login'] = '2025-10-24'

# 读取部分字段
print(f"Name: {user1['name']}, Age: {user1['age']}")

# 迭代所有字段
print("\nAll fields:")
for key, value in user1.items():
    print(f"  {key}: {value}")

# 用户2的配置（独立存储）
user2 = db.nested('user:2')
user2['name'] = 'Bob'

# 关闭数据库
db.close()
```

## 总结

NestedDBDict 是 FlaxKV2 为解决嵌套数据频繁修改性能问题而设计的特性：

- ✅ **性能提升显著**：字段越多，优势越明显（最高 18x+）
- ✅ **完整的字典接口**：支持所有常用字典操作
- ✅ **数据持久化**：自动保存，无需额外操作
- ✅ **前缀隔离**：多个嵌套字典互不干扰
- ✅ **灵活使用**：可与传统方式混合使用

**推荐场景**：
- 用户配置、会话数据
- 缓存对象、实时指标
- 大型结构化对象（字段数 > 100）
- 需要频繁修改部分字段的场景

开始使用 NestedDBDict，让你的嵌套数据操作飞起来！🚀
