# 缓存模式下 keys() 方法修复

## 问题描述

在开启读写缓存后（`enable_write_buffer=True`），当使用 `print(db)` 时，缓存中的数据不会展示出来。

**问题示例**：
```python
db = FlaxKV("mydb", "./data", use_cache=True, enable_write_buffer=True)
db['key1'] = 'value1'
print(db)  # 打印 {} （空字典）
```

**原因**：
- 在写缓冲模式下，数据被写入缓存并标记为 `dirty`，但还未刷新到数据库
- `keys()` 方法只从数据库读取，不包含缓存中的 dirty 数据
- `print(db)` 调用 `__str__()`，而 `__str__()` 调用 `keys()` 和 `items()`，所以看不到缓存中的数据

## 修复方案

修改 `CachedLevelDBDict.keys()` 方法（`flaxkv2/core/cached_leveldb_dict.py:768-860`），使其：

1. **从数据库读取所有键**（原有逻辑）
2. **添加缓存中的 dirty keys**（新增逻辑）
   - 遍历 `self._cache._cache.keys()`
   - 只包含在 `self._cache._dirty_keys` 中的键（dirty 数据）
   - 检查是否过期
3. **排除已删除的键**（新增逻辑）
   - 排除 `self._cache._delete_keys` 中的键
4. **使用集合去重**（新增逻辑）
   - 避免数据库和缓存中的相同键重复出现

## 修复后的行为

```python
db = FlaxKV("mydb", "./data", use_cache=True, enable_write_buffer=True)

# 写入数据到缓存（dirty）
db['key1'] = 'value1'
db['key2'] = 'value2'
db['key3'] = 'value3'

# ✅ keys() 现在包含缓存中的数据
print(db.keys())  # ['key1', 'key2', 'key3']

# ✅ print(db) 现在正确显示所有数据
print(db)  # {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}

# 删除一个键（标记删除，但未刷新）
del db['key2']

# ✅ 删除的键被正确排除
print(db.keys())  # ['key1', 'key3']
print(db)  # {'key1': 'value1', 'key3': 'value3'}
```

## 边缘情况处理

### 1. 混合场景（数据库 + 缓存）
```python
db['key1'] = 'value1'
db.flush()  # key1 在数据库中

db['key2'] = 'value2'  # key2 在缓存中（dirty）

print(db.keys())  # ['key1', 'key2'] ✅
```

### 2. 去重（数据库和缓存中有相同的键）
```python
db['key1'] = 'value1'
db.flush()  # key1 在数据库中

db['key1'] = 'new_value1'  # 更新 key1（在缓存中标记为 dirty）

keys = db.keys()
assert keys.count('key1') == 1  # ✅ 只出现一次
```

### 3. 删除后再添加
```python
db['key1'] = 'value1'
db.flush()

del db['key1']  # 标记删除
assert 'key1' not in db.keys()  # ✅

db['key1'] = 'new_value1'  # 重新添加
assert 'key1' in db.keys()  # ✅
```

## 测试覆盖

添加了 9 个新的单元测试（`tests/unit/test_cached_keys_with_buffer.py`）：

1. ✅ `test_keys_with_dirty_cache` - 测试 dirty 数据被包含
2. ✅ `test_keys_exclude_deleted` - 测试删除的键被排除
3. ✅ `test_keys_mixed_db_and_cache` - 测试混合场景
4. ✅ `test_keys_no_duplicates` - 测试去重
5. ✅ `test_print_with_buffer` - 测试 print(db) 正确显示
6. ✅ `test_items_with_buffer` - 测试 items() 正确工作
7. ✅ `test_len_with_buffer` - 测试 len(db) 正确
8. ✅ `test_keys_after_delete_and_add` - 测试删除后再添加
9. ✅ `test_readonly_cache_mode` - 测试只读缓存模式

**所有单元测试通过**：112 passed ✅

## 影响范围

- 修改文件：`flaxkv2/core/cached_leveldb_dict.py`
- 影响方法：`keys()`, `values()`, `items()`, `__len__()`, `__str__()`, `__repr__()`
- 向后兼容：✅ 完全兼容（只是修复了缺失的功能）
- 性能影响：极小（只在启用缓存时增加少量检查）

## 总结

这个修复确保了 `CachedLevelDBDict` 在写缓冲模式下的正确行为，使得 `keys()`, `print(db)`, `len(db)` 等操作能够正确反映缓存中的数据状态，而不仅仅是数据库中的数据。

修复遵循 **KISS 原则**，逻辑简单清晰：
1. 收集数据库中的 keys
2. 添加缓存中的 dirty keys
3. 排除 delete keys
4. 去重并返回

用户现在可以放心使用写缓冲模式，不用担心 `print(db)` 显示不正确的问题。
