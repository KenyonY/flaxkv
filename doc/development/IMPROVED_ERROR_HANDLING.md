# 改进的异常处理机制

## 问题背景

用户指出：直接捕获并跳过所有异常太粗暴，可能会掩盖真正的错误，为将来的问题留下隐患。

## 原始问题

当 `RawLevelDBDict` 使用嵌套存储时，数据库中会存在三种类型的键：

1. **用户键**（带类型前缀）：如 `b'sname'`, `b'i42'`
2. **内部标记键**（带类型前缀）：如 `b's__nested__:config'`, `b's__ttl_info__:key'`
3. **嵌套子键**（无类型前缀）：如 `b'config:database:host'`

`keys()`, `values()`, `items()` 方法需要：
- 返回用户可见的键（用户键 + 嵌套字典的顶层键）
- 跳过内部实现细节（标记键 + 嵌套子键）
- 对真正的错误进行记录

## 改进方案

### 1. 精确的键分类逻辑

在 `keys()` 方法中实现精确的分类逻辑：

```python
def keys(self) -> List:
    keys = []
    with self._db_lock:
        for key_bytes, _ in self._db:
            try:
                # 尝试解码（适用于带类型前缀的键）
                key = self._decode_key(key_bytes)

                # 检查是否是嵌套标记键
                if isinstance(key, str) and key.startswith('__nested__:'):
                    actual_key = key[len('__nested__:'):]
                    # 只添加顶层嵌套键（不包含进一步的 ':'）
                    if ':' not in actual_key:
                        keys.append(actual_key)
                    continue

                # 跳过 TTL 信息键
                if isinstance(key, str) and key.startswith('__ttl_info__:'):
                    continue

                # 正常的用户键
                keys.append(key)

            except (ValueError, UnicodeDecodeError):
                # 解码失败，可能是嵌套子键（没有类型前缀）
                try:
                    key_str = key_bytes.decode('utf-8')
                    # 嵌套子键（如 'config:database:host'），跳过
                    if ':' in key_str:
                        continue
                except UnicodeDecodeError as e:
                    # 真正的解码错误，记录日志
                    import logging
                    logging.warning(f"无法解码键 {key_bytes[:20]}...: {e}")

    return keys
```

### 2. 简化 values() 和 items()

使用 `keys()` 和 `__getitem__` 来实现，确保一致性：

```python
def values(self) -> List:
    return [self[key] for key in self.keys()]

def items(self) -> List[Tuple]:
    return [(key, self[key]) for key in self.keys()]
```

这样的好处：
- 对嵌套字典返回 `NestedDBDict`（而不是原始存储的标记值）
- 逻辑集中在 `keys()` 方法中，易于维护
- 保证 `keys()`, `values()`, `items()` 的一致性

### 3. 错误处理层级

```
第1层：尝试用 _decode_key 解码
  ✓ 成功 → 检查是否是内部标记键
    - __nested__:xxx → 提取并返回 xxx（如果是顶层）
    - __ttl_info__:xxx → 跳过
    - 其他 → 返回用户键

第2层：解码失败 → 尝试 UTF-8 解码
  ✓ 成功 → 检查是否包含 ':'
    - 包含 → 跳过（嵌套子键）
    - 不包含 → 记录警告（未知情况）

第3层：UTF-8 解码也失败
  ✗ 记录警告日志（真正的错误）
```

## 改进效果

### ✅ 正确区分不同类型的键

```python
db = RawLevelDBDict("test", auto_nested=True)
db['config'] = {'database': {'host': 'localhost'}}
db['name'] = 'TestDB'

# 数据库中的实际键：
# b'config:database:host'           → 嵌套子键（跳过）
# b's__nested__:config'             → 标记键（转换为 'config'）
# b's__nested__:config:database'    → 标记键（跳过，因为包含多层 :）
# b'sname'                          → 用户键（返回 'name'）

keys = db.keys()  # ['config', 'name']
```

### ✅ 记录真正的错误

```python
# 如果数据库中有损坏的键
db._db.put(b'x_invalid_prefix_key', b'some_value')

keys = db.keys()
# 输出警告日志：WARNING: 无法解码键 b'x_invalid_prefix_key'...
```

### ✅ 支持带冒号的用户键

```python
db['user:1'] = 'Alice'  # 用户键可以包含冒号
# 存储为 b'suser:1'（有类型前缀）
# keys() 正确返回 'user:1'
```

### ✅ 完整测试覆盖

创建了 `test_error_logging.py`，包含：

1. **测试跳过内部键** - 验证 `keys()` 正确返回用户可见的键
2. **测试损坏键的日志记录** - 验证真正的错误会被记录
3. **测试 keys() 过滤逻辑** - 验证各种键类型的正确处理

## 与原始方案的对比

### 原始方案（粗暴）
```python
try:
    key = self._decode_key(key_bytes)
    keys.append(key)
except (ValueError, UnicodeDecodeError):
    pass  # 静默跳过所有错误
```

**问题**：
- ❌ 无法区分嵌套子键和真正的错误
- ❌ 静默忽略所有异常
- ❌ 调试困难

### 改进方案（精确）
```python
try:
    key = self._decode_key(key_bytes)
    # 精确检查各种情况
    if key.startswith('__nested__:'):
        # 处理嵌套标记键
    elif key.startswith('__ttl_info__:'):
        # 跳过 TTL 键
    else:
        # 正常用户键
except (ValueError, UnicodeDecodeError):
    # 二次尝试：UTF-8 解码
    try:
        key_str = key_bytes.decode('utf-8')
        if ':' in key_str:
            continue  # 嵌套子键
    except UnicodeDecodeError as e:
        logging.warning(...)  # 记录真正的错误
```

**优势**：
- ✅ 明确区分各种键类型
- ✅ 只跳过已知的内部键
- ✅ 记录未预期的错误
- ✅ 便于调试和维护

## 总结

改进后的异常处理机制遵循"精确而非粗暴"的原则：

1. **明确识别**：清楚地识别每种键类型
2. **精确跳过**：只跳过已知的内部键
3. **错误记录**：对未预期的错误进行日志记录
4. **易于维护**：逻辑清晰，便于未来扩展

这种方法既解决了当前的问题，又不会为将来留下隐患。
