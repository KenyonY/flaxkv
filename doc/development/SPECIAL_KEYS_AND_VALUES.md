# FlaxKV2 特殊键和值说明

## 概述

FlaxKV2 在内部使用一些特殊的键前缀和值标记来实现高级功能。本文档说明这些特殊标记，以及用户应该注意的事项。

## 特殊值标记

### 1. 删除标记 (`_DELETED`)

**用途**: 在缓冲区中标记已删除的键

**实现**: 
```python
class _DeletedMarker:
    """删除标记类，用于在缓冲区中标记已删除的键"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __repr__(self):
        return "<DELETED>"
    
    def __bool__(self):
        return False

_DELETED = _DeletedMarker()
```

**为什么不使用 None**:
- 用户可能需要存储 `None` 值
- 使用 `None` 作为删除标记会导致混淆
- 专用的 Sentinel 对象可以明确区分删除标记和实际的 `None` 值

**用户影响**: ✅ **无影响** - 用户可以正常存储和读取 `None` 值

## 特殊键前缀

### 1. `__nested__:` 前缀

**用途**: 标记嵌套字典

**使用场景**:
```python
db = LevelDBDict("mydb", auto_nested=True)

# 当存储字典时
db["config"] = {"host": "localhost", "port": 8080}

# 内部会创建标记键
# __nested__:config = True
```

**潜在问题**: ⚠️ **用户可能无意中使用这个前缀**

**示例**:
```python
# 用户代码
db["__nested__:mykey"] = "my value"  # 可以工作，但不推荐

# 可能与系统的标记键混淆
db["config"] = {"key": "value"}  # 创建 __nested__:config
db["__nested__:config"] = "other"  # 覆盖标记键！
```

**建议**:
1. ❌ **避免使用** `__nested__:` 作为键的前缀
2. ✅ 如果必须使用，请确保不会与嵌套字典的键名冲突
3. ✅ 考虑使用其他命名约定，如 `_nested_` 或 `nested_`

## 保留键前缀列表

以下键前缀被 FlaxKV2 内部使用，建议用户避免使用：

| 前缀 | 用途 | 风险等级 |
|------|------|---------|
| `__nested__:` | 嵌套字典标记 | ⚠️ 中等 |
| `__ttl__:` | TTL 元数据（未来可能使用） | ⚠️ 低 |
| `__index__:` | 索引元数据（未来可能使用） | ⚠️ 低 |

## 最佳实践

### ✅ 推荐做法

```python
# 1. 使用普通的键名
db["user_config"] = {"name": "Alice"}
db["settings"] = {"theme": "dark"}

# 2. 可以存储 None 值
db["optional_field"] = None
assert db["optional_field"] is None

# 3. 使用自己的命名约定
db["_my_prefix_key"] = "value"
db["my.dotted.key"] = "value"
```

### ❌ 不推荐做法

```python
# 1. 避免使用保留前缀
db["__nested__:mykey"] = "value"  # 可能混淆

# 2. 避免依赖内部实现
# 不要尝试直接操作标记键
del db["__nested__:config"]  # 可能破坏数据一致性
```

## 技术细节

### 删除标记的工作原理

1. **删除操作**:
   ```python
   del db["key"]
   # 内部: buffer["key"] = _DELETED
   ```

2. **读取操作**:
   ```python
   value = db["key"]
   # 内部: 
   # if buffer["key"] is _DELETED:
   #     raise KeyError
   ```

3. **刷新缓冲区**:
   ```python
   # 内部:
   for key, value in buffer.items():
       if value is _DELETED:
           leveldb.delete(key)
       else:
           leveldb.put(key, value)
   ```

### 嵌套字典标记的工作原理

1. **存储字典**:
   ```python
   db["config"] = {"host": "localhost"}
   # 内部:
   # db["__nested__:config"] = True
   # db.nested("config")["host"] = "localhost"
   ```

2. **读取字典**:
   ```python
   config = db["config"]
   # 内部:
   # if db["__nested__:config"]:
   #     return NestedDBDict("config")
   ```

3. **删除字典**:
   ```python
   del db["config"]
   # 内部:
   # nested = db.nested("config")
   # nested.clear()
   # del db["__nested__:config"]
   ```

## 未来改进

### 短期
- [ ] 添加键名验证，警告使用保留前缀
- [ ] 在文档中明确说明保留前缀

### 中期
- [ ] 使用更不可能冲突的前缀，如 `\x00__flaxkv_nested__:`
- [ ] 提供配置选项，允许用户自定义前缀

### 长期
- [ ] 使用完全独立的命名空间（如单独的 LevelDB column family）
- [ ] 实现键名冲突检测和自动处理

## 总结

✅ **已解决的问题**:
- None 值混淆 - 使用专用的 `_DELETED` 标记

⚠️ **需要注意的问题**:
- `__nested__:` 前缀 - 用户应避免使用

📝 **建议**:
- 遵循最佳实践，避免使用保留前缀
- 如果遇到问题，检查是否使用了保留前缀

