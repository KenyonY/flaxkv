# TTL + auto_nested Bug 修复报告

**修复日期**: 2025-10-29
**Bug严重性**: 中等（影响用户体验，但不影响数据正确性）
**修复时间**: ~30分钟

---

## 🐛 Bug描述

### 问题复现

```python
from flaxkv2 import FlaxKV

db = FlaxKV("kunyuan", auto_nested=True, rebuild=True)
db['a'] = {"ky": 10, "ll": 20}
db.set_ttl('a', 3)

# 等待3秒后...
print(db)  # 第一次打印
# 输出: RawLevelDBDict(name='kunyuan', path='...', error=KeyError('a'))

print(db)  # 第二次打印
# 输出: RawLevelDBDict(name='kunyuan', path='...', items={})
```

### 用户观察到的现象

1. **第一次打印**: 显示 `error=KeyError('a')`
2. **第二次打印**: 正常显示为空 `items={}`

### 预期行为

第一次打印就应该正常显示为空，不应该有任何错误。

---

## 🔍 根因分析

### 问题发生的流程

1. **设置数据** (`auto_nested=True`):
   ```
   db['a'] = {"ky": 10, "ll": 20}

   数据库中存储：
   - __nested__:a = True        (标记键)
   - a:ky = 10                  (嵌套数据)
   - a:ll = 20                  (嵌套数据)
   ```

2. **设置TTL**:
   ```
   db.set_ttl('a', 3)

   额外存储：
   - __ttl_info__:a = <过期时间>
   ```

3. **TTL过期后，调用 `repr(db)`**:
   ```python
   # __repr__() 方法:
   def __repr__(self):
       try:
           items = list(self.items())  # 调用 items()
           ...

   # items() 方法:
   def items(self):
       return [(key, self[key]) for key in self.keys()]

   # keys() 方法（修复前）:
   def keys(self):
       keys = []
       for key_bytes, _ in self._db:
           key = decode(key_bytes)
           if key.startswith('__nested__:'):
               actual_key = key[len('__nested__:'):]
               keys.append(actual_key)  # ❌ 添加了'a'，但没检查TTL
       return keys
   ```

4. **问题发生**:
   - `keys()` 看到 `__nested__:a`，将 'a' 添加到返回列表
   - `items()` 调用 `self['a']`
   - `__getitem__` 检查TTL，发现已过期
   - 抛出 `KeyError('a')`
   - `__repr__` 捕获异常，显示 `error=KeyError('a')`

5. **第二次打印正常**:
   - 第一次打印时，`__getitem__` 已经删除了 `__nested__:a` 标记键
   - 第二次调用 `keys()` 时，不再看到标记键
   - 正常返回空列表

### 根本原因

**`keys()` 方法没有检查TTL是否过期**，导致返回了已过期但标记键还存在的键。

---

## ✅ 修复方案

### 修改位置

文件：`flaxkv2/core/raw_leveldb_dict.py`
方法：`keys()` (第467-511行)

### 修复内容

在 `keys()` 方法中添加TTL过期检查：

```python
def keys(self) -> List:
    """
    获取所有键列表

    注意：会自动过滤已过期的TTL键
    """
    keys = []
    with self._db_lock:
        for key_bytes, _ in self._db:
            try:
                key = self._decode_key(key_bytes)

                # 检查是否是嵌套标记键
                if isinstance(key, str) and key.startswith('__nested__:'):
                    actual_key = key[len('__nested__:'):]
                    if ':' not in actual_key:
                        # ✅ 新增：检查TTL是否过期
                        if not self._ttl_manager.is_expired(actual_key):
                            keys.append(actual_key)
                    continue

                # 跳过 TTL 信息键
                if isinstance(key, str) and key.startswith('__ttl_info__:'):
                    continue

                # 正常的用户键 - ✅ 新增：检查TTL是否过期
                if not self._ttl_manager.is_expired(key):
                    keys.append(key)

            except (ValueError, UnicodeDecodeError):
                ...

    return keys
```

### 关键改动

1. **第487-488行**: 对嵌套标记键提取的实际键检查TTL
2. **第496-497行**: 对普通用户键检查TTL

---

## 🧪 测试验证

### 修复前

```python
设置TTL后立即查看：
RawLevelDBDict(name='kunyuan', path='/tmp/xxx/kunyuan', items={'a': NestedDBDict({'ky': 10, 'll': 20})})

等待3秒...

第一次打印（TTL过期后）：
RawLevelDBDict(name='kunyuan', path='/tmp/xxx/kunyuan', error=KeyError('a'))  # ❌ 错误

第二次打印：
RawLevelDBDict(name='kunyuan', path='/tmp/xxx/kunyuan', items={})  # ✅ 正常
```

### 修复后

```python
设置TTL后立即查看：
RawLevelDBDict(name='kunyuan', path='/tmp/xxx/kunyuan', items={'a': NestedDBDict({'ky': 10, 'll': 20})})

等待3秒...

第一次打印（TTL过期后）：
RawLevelDBDict(name='kunyuan', path='/tmp/xxx/kunyuan', items={})  # ✅ 正常

第二次打印：
RawLevelDBDict(name='kunyuan', path='/tmp/xxx/kunyuan', items={})  # ✅ 正常
```

### 测试用例

创建了7个专门的测试用例 (`tests/test_ttl_auto_nested_bug.py`):

1. ✅ `test_ttl_expired_keys_not_returned` - keys()不返回过期键
2. ✅ `test_ttl_expired_repr_no_error` - __repr__()不显示error
3. ✅ `test_ttl_expired_items_no_error` - items()不抛出异常
4. ✅ `test_ttl_expired_values_no_error` - values()不抛出异常
5. ✅ `test_ttl_expired_len_correct` - len()返回正确计数
6. ✅ `test_ttl_with_nested_marker_cleanup` - 标记键正确清理
7. ✅ `test_mixed_ttl_auto_nested_normal_keys` - 混合场景测试

**所有测试通过** ✅

---

## 📊 影响范围

### 受影响的场景

1. **使用 `auto_nested=True` + TTL**
2. **在TTL过期后调用**:
   - `repr(db)` / `str(db)`
   - `list(db.keys())`
   - `list(db.items())`
   - `list(db.values())`
   - `len(db)`

### 不受影响的场景

1. **`auto_nested=False`** - 不涉及嵌套标记键
2. **没有TTL** - 键永远不会过期
3. **TTL未过期** - 行为正常
4. **直接访问键** - `db['key']` 本身有TTL检查

---

## 🎯 修复效果

### 用户体验改善

| 操作 | 修复前 | 修复后 |
|-----|-------|-------|
| 第一次打印过期db | ❌ error=KeyError | ✅ 正常显示空 |
| `db.keys()` | ❌ 返回过期键 | ✅ 自动过滤 |
| `db.items()` | ❌ 抛出异常 | ✅ 正常工作 |
| `len(db)` | ❌ 计数错误 | ✅ 正确计数 |

### 性能影响

- **最小化**: TTL检查已经在内存中进行（`TTLManager`）
- **无额外I/O**: 不需要额外的数据库查询
- **时间复杂度**: O(1) 检查每个键（哈希表查找）

---

## 🔄 向后兼容性

✅ **完全兼容** - 此修复是纯粹的bug修复：

- 不改变任何API
- 不影响正常工作的代码
- 只修复了错误行为
- 所有现有测试保持通过

---

## 📝 相关代码

### 修改的文件

1. **flaxkv2/core/raw_leveldb_dict.py** (第467-511行)
   - 在 `keys()` 方法中添加TTL过期检查

### 新增的文件

1. **tests/test_ttl_auto_nested_bug.py** (新增)
   - 7个测试用例覆盖各种场景

### 相关方法

- `keys()` - 主要修改点
- `items()` - 依赖 `keys()`，间接受益
- `values()` - 依赖 `keys()`，间接受益
- `__len__()` - 依赖 `keys()`，间接受益
- `__repr__()` - 调用 `items()`，间接受益

---

## 💡 经验教训

### 设计原则

1. **一致性很重要**: `keys()` 应该和 `__getitem__` 有一致的过期检查逻辑
2. **尽早检查**: 在枚举键的时候就应该过滤，而不是等到访问时才抛出异常
3. **用户体验优先**: 避免让用户看到内部错误信息

### 最佳实践

1. **迭代器方法应该过滤过期数据**: `keys()`, `items()`, `values()`
2. **保持语义一致**: 如果 `key in db` 返回 False，那么 `key` 不应该出现在 `db.keys()` 中
3. **及早测试边界情况**: TTL + auto_nested 的组合应该有专门的测试

---

## ✅ 验收标准

- [x] Bug复现成功
- [x] 根因分析清楚
- [x] 修复方案实施
- [x] 所有新测试通过 (7/7)
- [x] 所有现有测试通过
- [x] 文档更新完成
- [x] 向后兼容性验证
- [x] 性能影响可接受

---

## 🎉 总结

此bug是 `auto_nested` 和 TTL 功能交互时的边缘情况导致的。通过在 `keys()` 方法中添加TTL过期检查，使其与 `__getitem__` 保持一致，成功修复了这个用户体验问题。

**修复核心**: 让 `keys()` 方法返回的键列表与实际可访问的键保持一致。

**修复完成！** ✅

---

**修复人**: Claude
**审核人**: 用户
**完成日期**: 2025-10-29
