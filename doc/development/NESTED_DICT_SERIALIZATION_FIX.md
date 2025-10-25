# NestedDBDict 序列化问题修复

## 问题描述

在运行 `demo_auto_nested.py` 时出现序列化错误：

```
ERROR | flaxkv2.core.base:close:321 - 数据库 demo_db 最终写入时发生错误:
self._db,self.options cannot be converted to a Python object for pickling
```

## 问题根源

### 场景重现

在 `demo_auto_nested.py:123-125`：

```python
data = db['traditional']  # 返回 NestedDBDict（因为 auto_nested=True）
data['field_0'] = i
db['traditional'] = data  # ❌ 尝试序列化 NestedDBDict 对象
```

### 错误链条

1. **获取值**：`db['traditional']` 返回 `NestedDBDict` 对象（自动嵌套模式）
2. **修改值**：`data['field_0'] = i` 修改字段
3. **写回**：`db['traditional'] = data` 尝试将 NestedDBDict 设置回去
4. **编码**：`__setitem__` 调用 `super().__setitem__(key, value)`
5. **序列化失败**：`encoder.encode(value)` 尝试序列化 NestedDBDict
   - msgpack 失败（不支持自定义对象）
   - 回退到 pickle
   - pickle 尝试序列化 `_root_db` 属性
   - `_root_db` 包含数据库对象（`_db`, `options`）
   - **错误**：LevelDB 对象不能被 pickle 序列化

### NestedDBDict 的结构

```python
class NestedDBDict:
    def __init__(self, prefixed_db, prefix, parent_db=None, root_db=None):
        self._prefixed_db = prefixed_db  # plyvel PrefixedDB 对象
        self._prefix = prefix
        self._parent_db = parent_db
        self._root_db = root_db  # ❌ 包含对父数据库的引用
        # _root_db 包含 _db (plyvel.DB) 和 options，都不能被 pickle
```

## 解决方案

在 `__setitem__` 中检测 `NestedDBDict` 并自动转换为普通字典：

### LevelDBDict (leveldb_dict.py:496-498)

```python
def __setitem__(self, key, value):
    from flaxkv2.core.nested_dict import NestedDBDict

    # ... 特殊键处理 ...

    # 如果值是 NestedDBDict，转换为普通字典
    if isinstance(value, NestedDBDict):
        value = value.to_dict()

    # 如果启用了自动嵌套且值是字典类型
    if self._auto_nested and isinstance(value, dict):
        # 自动嵌套存储
        ...
```

### RawLevelDBDict (raw_leveldb_dict.py:194-196)

```python
def __setitem__(self, key, value):
    from flaxkv2.core.nested_dict import NestedDBDict

    # ... 特殊键处理 ...

    # 如果值是 NestedDBDict，转换为普通字典
    if isinstance(value, NestedDBDict):
        value = value.to_dict()

    # 后续逻辑...
```

## 修复效果

### 修复前

```
ERROR | flaxkv2.core.base:close:321 - 数据库 demo_db 最终写入时发生错误:
self._db,self.options cannot be converted to a Python object for pickling
```

### 修复后

```
✓ 所有测试通过！
```

## 性能影响

修复后的 `demo_auto_nested.py` 性能对比：

```
传统方式（整个字典序列化）:
  修改 100 次: 0.8761秒

自动嵌套方式（单字段序列化）:
  修改 100 次: 0.0003秒

性能提升: 2949.2x 更快
```

**说明**：修复使得传统方式的性能测试变得准确。之前由于序列化失败，性能测试不能真实反映开销。

修复后：
- 传统方式每次都需要：读取 NestedDBDict → 转换为 dict → 重新嵌套存储
- 自动嵌套方式：直接修改单个字段

## 测试验证

### 1. 基础功能测试

```bash
$ python test_auto_nested_modes.py
✓ LevelDBDict auto_nested=True 测试通过
✓ LevelDBDict auto_nested=False 测试通过
✓ RawLevelDBDict auto_nested=False 测试通过
✓ RawLevelDBDict auto_nested=True 测试通过
✓ 混合存储测试通过
✓ 性能对比测试完成
```

### 2. 嵌套字典测试

```bash
$ python test_raw_nested.py
✓ 创建嵌套字典成功
✓ 写入数据成功
✓ 修改数据成功
✓ 所有测试通过！
```

### 3. Demo 演示

```bash
$ python demo_auto_nested.py
✓ 演示 1: 自动检测字典类型
✓ 演示 2: 递归嵌套（任意层级）
✓ 演示 3: 混合类型
✓ 演示 4: 转换为普通字典
✓ 演示 5: 实际存储结构
✓ 演示 6: 性能对比
🚀 开始使用 FlaxKV2 自动嵌套存储！
```

## 修复的文件

1. **flaxkv2/core/leveldb_dict.py** (line 489-498)
   - 添加 NestedDBDict 检测和转换

2. **flaxkv2/core/raw_leveldb_dict.py** (line 184-196)
   - 添加 NestedDBDict 检测和转换

## 相关问题

这个修复还解决了一个潜在的用户体验问题：

**之前的行为**：
```python
nested = db['key']       # 返回 NestedDBDict
db['key'] = nested       # ❌ 序列化失败
```

**修复后的行为**：
```python
nested = db['key']       # 返回 NestedDBDict
db['key'] = nested       # ✓ 自动转换为 dict，重新嵌套存储
```

虽然这不是推荐的使用方式（没有实际意义），但修复后至少不会报错。

## 总结

### 问题

尝试序列化包含数据库引用的 NestedDBDict 对象导致 pickle 失败。

### 解决

在 `__setitem__` 中自动检测 NestedDBDict 并转换为普通字典。

### 优势

- ✅ 修复序列化错误
- ✅ 提升用户体验（容错性更强）
- ✅ 不影响现有功能
- ✅ 性能测试更准确
- ✅ 所有测试通过

### 影响范围

- LevelDBDict 的 `__setitem__` 方法
- RawLevelDBDict 的 `__setitem__` 方法
- 仅影响当值为 NestedDBDict 时的行为
- 对其他类型的值无影响
