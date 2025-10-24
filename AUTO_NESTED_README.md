# 自动嵌套存储 - Simple and Stupid 实现

## 设计原则

遵循 **"Simple and Stupid"** 哲学：
- ✅ 简单规则：遇到 `dict` 就递归展开
- ✅ 自动检测：无需配置，自动识别字典类型
- ✅ 完全递归：处理所有嵌套层级
- ✅ 可控开关：通过 `auto_nested` 参数控制行为

## 控制自动嵌套

通过 `auto_nested` 参数控制是否启用自动嵌套存储：

### LevelDBDict（默认启用）

```python
from flaxkv2 import LevelDBDict

# 默认启用自动嵌套（推荐）
db = LevelDBDict('mydb')
db['config'] = {'key': 'value'}  # 自动嵌套存储

# 显式禁用自动嵌套（使用传统序列化）
db = LevelDBDict('mydb', auto_nested=False)
db['config'] = {'key': 'value'}  # 传统序列化，返回普通 dict
```

### RawLevelDBDict（默认禁用）

```python
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict

# 默认禁用自动嵌套（保持性能基准）
db = RawLevelDBDict('mydb')
db['config'] = {'key': 'value'}  # 传统序列化

# 显式启用自动嵌套
db = RawLevelDBDict('mydb', auto_nested=True)
db['config'] = {'key': 'value'}  # 自动嵌套存储
```

### 使用场景

**启用自动嵌套（`auto_nested=True`）适合：**
- 频繁部分更新嵌套字典的场景
- 需要高效访问深层字段
- 字典结构复杂、字段众多

**禁用自动嵌套（`auto_nested=False`）适合：**
- 性能基准测试
- 字典较小且不常更新
- 需要返回原生 dict 类型
- 对存储结构有特殊要求

## 核心机制

### 写入时：自动检测并递归展开

```python
db['user:1'] = {
    'name': 'Alice',
    'profile': {
        'avatar': 'url',
        'bio': 'text'
    }
}
```

**实际存储**：
```
user:1:name -> 'Alice'
user:1:profile:avatar -> 'url'
user:1:profile:bio -> 'text'

__nested__:user:1 -> True
__nested__:user:1:profile -> True
```

### 读取时：自动返回 NestedDBDict

```python
user = db['user:1']              # 返回 NestedDBDict
profile = user['profile']        # 返回 NestedDBDict（递归）
name = user['name']              # 返回 'Alice'（叶子值）
```

### 修改时：自动同步

```python
user['name'] = 'Bob'                    # 直接修改
user['profile']['avatar'] = 'new_url'   # 嵌套修改
```

## 使用示例

### 1. 基础用法

```python
from flaxkv2 import LevelDBDict

db = LevelDBDict('mydb')

# 写入：自动检测字典
db['config'] = {
    'database': {
        'host': 'localhost',
        'port': 5432
    },
    'cache': {
        'redis': {
            'host': '127.0.0.1'
        }
    }
}

# 读取：逐层访问
config = db['config']                   # NestedDBDict
database = config['database']           # NestedDBDict
host = database['host']                 # 'localhost'

# 修改：自动同步
database['port'] = 3306
config['cache']['redis']['host'] = '192.168.1.1'

# 转换为普通字典
config_dict = config.to_dict()
```

### 2. 混合类型

```python
db['data'] = {
    'string': 'hello',
    'number': 123,
    'list': [1, 2, 3],
    'nested': {
        'key': 'value'
    }
}

data = db['data']
print(data['string'])           # 'hello'
print(data['number'])           # 123
print(data['list'])             # [1, 2, 3]

nested = data['nested']
print(type(nested))             # NestedDBDict
print(nested['key'])            # 'value'
```

### 3. 删除操作

```python
# 删除叶子值
del user['name']

# 删除嵌套字典（递归删除所有子键）
del db['config']
```

## 技术细节

### 存储结构

```python
db['user:1'] = {
    'name': 'Alice',
    'settings': {
        'theme': 'dark'
    }
}
```

**LevelDB 存储**：
```
'user:1:name' -> serialized('Alice')
'user:1:settings:theme' -> serialized('dark')

'__nested__:user:1' -> True
'__nested__:user:1:settings' -> True
```

### 标记键规则

- **格式**：`__nested__:完整路径`
- **用途**：标识某个路径是字典（而不是叶子值）
- **检测**：读取时检查标记键，决定返回 NestedDBDict 还是值

### 递归规则

**写入**：
1. 检测到 `dict` → 创建标记键
2. 创建 prefixed_db（`key:`）
3. 递归写入每个字段（字段可能也是 dict）

**读取**：
1. 检查标记键
2. 存在 → 返回 NestedDBDict
3. 不存在 → 反序列化并返回值

**删除**：
1. 检查标记键
2. 存在 → 递归删除所有子键 + 删除标记
3. 不存在 → 删除叶子值

## 性能特点

### 优势

1. **部分更新高效**：
   ```python
   user['age'] = 31  # 只序列化 age，不影响其他字段
   ```

2. **深层访问高效**：
   ```python
   theme = db['config']['database']['credentials']['user']
   # 只读取一个叶子值，不加载整个字典
   ```

3. **内存友好**：
   - 按需加载字段
   - 不需要一次性加载整个字典

### 开销

1. **标记键**：
   - 每个嵌套层级一个标记键
   - 额外空间：约 20 字节/层级

2. **首次写入**：
   - 需要递归展开所有层级
   - 多次 LevelDB 写入

3. **类型限制**：
   - 返回 `NestedDBDict`，不是原生 `dict`
   - `isinstance(user, dict)` 为 `False`

## 对比不同方式

### 方式 1：手动嵌套（always available）

```python
# 不论 auto_nested 设置如何，都可以手动使用 nested()
user = db.nested('user:1')
user['name'] = 'Alice'
user['profile'] = {'bio': 'text'}  # 递归
```

### 方式 2：自动嵌套（需要 auto_nested=True）

```python
# LevelDBDict 默认启用
db = LevelDBDict('mydb')  # auto_nested=True by default
db['user:1'] = {'name': 'Alice', 'profile': {'bio': 'text'}}  # 自动检测
user = db['user:1']  # 返回 NestedDBDict
```

### 方式 3：传统序列化（auto_nested=False）

```python
# RawLevelDBDict 默认使用此方式
db = RawLevelDBDict('mydb')  # auto_nested=False by default
db['user:1'] = {'name': 'Alice'}  # 整个字典序列化为一个值
user = db['user:1']  # 返回普通 dict
```

## 注意事项

### 1. 返回类型

```python
user = db['user:1']
print(type(user))  # NestedDBDict，不是 dict

# 需要转换为 dict
user_dict = user.to_dict()
print(type(user_dict))  # dict
```

### 2. 空字典

```python
db['empty'] = {}
# 仍然创建标记键和嵌套存储
```

### 3. 非字典类型

```python
db['number'] = 123
db['string'] = 'hello'
# 使用传统存储，不创建嵌套结构
```

### 4. 类型切换

```python
# 从字典切换到非字典
db['key'] = {'a': 1}    # 嵌套存储
db['key'] = 123         # 切换到传统存储，自动删除标记和子键
```

## 完整示例

```python
from flaxkv2 import LevelDBDict

db = LevelDBDict('mydb', rebuild=True)

# 1. 写入多层嵌套
db['app'] = {
    'name': 'MyApp',
    'version': '1.0.0',
    'config': {
        'database': {
            'host': 'localhost',
            'port': 5432,
            'credentials': {
                'user': 'admin',
                'password': 'secret'
            }
        },
        'cache': {
            'enabled': True,
            'ttl': 3600
        }
    }
}

# 2. 读取和修改
app = db['app']
config = app['config']
database = config['database']
credentials = database['credentials']

# 修改深层值
credentials['password'] = 'new_secret'

# 验证修改
print(db['app']['config']['database']['credentials']['password'])
# 输出: new_secret

# 3. 转换为字典
app_dict = app.to_dict()
print(app_dict['config']['database']['host'])
# 输出: localhost

# 4. 删除
del config['cache']  # 删除整个 cache 分支
del db['app']        # 删除整个应用配置

db.close()
```

## 实现文件

### 核心修改

1. **NestedDBDict** (`flaxkv2/core/nested_dict.py`)
   - 添加 `root_db` 参数
   - `__getitem__`: 检查标记并递归返回
   - `__setitem__`: 检测 dict 并递归展开
   - `__delitem__`: 递归删除嵌套字典
   - `__iter__`: 只返回第一层键
   - `to_dict()`: 递归重建字典

2. **LevelDBDict** (`flaxkv2/core/leveldb_dict.py`)
   - 添加 `auto_nested=True` 参数（默认启用）
   - `__setitem__`: 检查 `auto_nested` 标志，自动检测 dict 类型
   - `__getitem__`: 检查 `auto_nested` 标志，返回 NestedDBDict 或普通值
   - `__delitem__`: 检查 `auto_nested` 标志，递归删除嵌套字典
   - `nested()`: 传入 `root_db` 参数

3. **RawLevelDBDict** (`flaxkv2/core/raw_leveldb_dict.py`)
   - 添加 `auto_nested=False` 参数（默认禁用，保持性能基准）
   - 实现与 LevelDBDict 相同的自动嵌套逻辑
   - 直接操作 LevelDB，无缓冲机制

### 测试文件

- `test_auto_nested.py` - 完整功能测试
- `test_auto_nested_modes.py` - auto_nested 参数测试
- `debug_to_dict.py` - to_dict() 调试
- `debug_delete.py` - 删除功能调试

## 性能对比

基于 `test_auto_nested_modes.py` 的测试结果：

### 完整读写性能

对 100 个键的字典进行 10 次完整读写：
- `auto_nested=False`: ~0.0000s（传统序列化）
- `auto_nested=True`: ~0.0111s（自动嵌套）

**结论**：首次完整写入和读取时，传统模式略快。

### 部分更新性能

对嵌套字典中的单个字段进行 100 次更新：
- `auto_nested=True`: ~0.0005s（只序列化修改的字段）

**结论**：部分更新场景下，自动嵌套模式有显著优势。

### 选择建议

| 场景 | 推荐模式 | 原因 |
|------|---------|------|
| 频繁部分更新 | `auto_nested=True` | 避免重复序列化整个字典 |
| 偶尔完整更新 | `auto_nested=False` | 更简单，性能略好 |
| 复杂嵌套结构 | `auto_nested=True` | 按需加载，内存友好 |
| 性能基准测试 | `auto_nested=False` | 保持原始性能特征 |

## 总结

### ✅ Simple
- 只有一个规则：遇到 dict 就展开
- 无需配置、阈值、复杂判断
- 通过简单的参数控制行为

### ✅ Stupid
- 不做智能判断
- 不管大小，统一处理
- 递归到底，不设层级限制

### ✅ Flexible
- `auto_nested` 参数控制行为
- LevelDBDict 默认启用（便利优先）
- RawLevelDBDict 默认禁用（性能优先）
- 手动 `nested()` 方法始终可用

### ✅ 高效
- 部分更新：只序列化修改的字段
- 按需加载：不需要一次性加载整个字典
- 递归访问：直接访问深层字段

### ⚠️ 权衡
- 返回 NestedDBDict（不是原生 dict）
- 标记键占用额外空间
- 首次写入需要递归展开

这个实现完美体现了"Simple and Stupid"原则，让用户可以像使用普通字典一样使用 FlaxKV2，同时自动获得性能优化！通过 `auto_nested` 参数，用户可以根据具体场景灵活选择最适合的存储方式。🚀
