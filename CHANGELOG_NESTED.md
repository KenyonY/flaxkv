# NestedDBDict 功能更新日志

## 版本：v0.2.0 (2025-10-24)

### 🚀 新功能

#### 1. 新增 NestedDBDict 类

基于 LevelDB 的 `prefixed_db` 特性，解决嵌套数据频繁序列化的性能问题。

**核心优势**：
- 每个字段独立存储，避免整个字典序列化/反序列化
- 性能提升最高 **18.6倍**（字段越多，优势越明显）
- 完整的 Python 字典接口
- 数据自动持久化

**使用方式**：
```python
# 创建嵌套字典
user = db.nested('user:1')

# 像普通字典一样使用
user['name'] = 'Alice'
user['age'] = 30

# 高效修改（只序列化单个字段）
user['age'] = 31
```

#### 2. 集成到数据库类

**LevelDBDict** 和 **RawLevelDBDict** 均支持 `nested()` 方法：

```python
# LevelDBDict
db = LevelDBDict('mydb')
nested = db.nested('prefix')

# RawLevelDBDict（更简单直接）
db = RawLevelDBDict('mydb')
nested = db.nested('prefix')
```

### 🔧 Bug 修复

#### 1. 布隆过滤器兼容性

修复 `LevelDBDict._init_bloom_filter()` 在加载 NestedDBDict 创建的键时的解码错误：

```python
# 修复前：遇到原始键会抛出 ValueError
# 修复后：自动跳过无法解码的键
try:
    decoded_key = self._decode_key(key)
    self._bloom_filter.add(decoded_key)
except (ValueError, UnicodeDecodeError):
    pass  # 跳过 NestedDBDict 的原始键
```

### 📊 性能数据

#### 测试场景 1：大字典频繁修改

- 配置：1000 个字段，修改单个字段 100 次
- 传统方式：8ms
- NestedDBDict：0.43ms
- **性能提升：18.6x**

#### 测试场景 2：混合读写

- 配置：500 个字段，100 次随机读写
- 传统方式：1.1ms
- NestedDBDict：0.6ms
- **性能提升：1.8x**

### 📝 新增文件

#### 核心代码
- `flaxkv2/core/nested_dict.py` - NestedDBDict 实现（265 行）

#### 测试文件
- `tests/test_nested_dict.py` - 17 个单元测试（全部通过 ✓）
- `test_raw_nested.py` - RawLevelDBDict 集成测试
- `test_prefixed_db.py` - prefixed_db 功能验证
- `benchmark_nested.py` - 基础性能测试
- `benchmark_nested_real.py` - 真实场景性能测试

#### 示例和文档
- `example_nested.py` - 完整使用示例
- `NESTED_DICT_GUIDE.md` - 详细使用指南（300+ 行）
- `NESTED_DICT_SUMMARY.md` - 技术总结
- `QUICKSTART_NESTED.md` - 5 分钟快速开始

#### 调试工具
- `debug_nested.py` - 调试脚本

### 🔄 修改文件

#### 1. flaxkv2/core/leveldb_dict.py
- 添加 `nested()` 方法（64 行文档 + 代码）
- 修复 `_init_bloom_filter()` 兼容性

#### 2. flaxkv2/core/raw_leveldb_dict.py
- 添加 `nested()` 方法（52 行文档 + 代码）

#### 3. flaxkv2/__init__.py
- 导出 `NestedDBDict` 类

### 📖 API 文档

#### NestedDBDict

**支持的操作**：

```python
# 基本操作
nested['key'] = value        # 设置
value = nested['key']        # 获取
del nested['key']            # 删除
'key' in nested              # 检查存在

# 字典方法
nested.get(key, default)     # 安全获取
nested.update(dict)          # 批量更新
nested.pop(key, default)     # 弹出
nested.setdefault(key, val)  # 设置默认值
nested.clear()               # 清空

# 迭代
for key in nested.keys()
for value in nested.values()
for k, v in nested.items()

# 转换
len(nested)                  # 长度
nested.to_dict()             # 转为字典
```

### 🎯 推荐使用场景

✅ **推荐**：
- 用户配置、会话数据
- 缓存对象（字段数 > 100）
- 实时指标、计数器
- ML 模型参数

⚠️ **不推荐**：
- 小字典（字段数 < 10）
- 需要原子性整体更新
- 频繁 `to_dict()` 完整读取

### 🔬 技术实现

**关键技术点**：

1. **利用 LevelDB prefixed_db**
   ```python
   prefixed_db = db._db.prefixed_db(b'user:1:')
   ```

2. **直接序列化，绕过编码层**
   ```python
   def __setitem__(self, key, value):
       key_bytes = key.encode('utf-8')
       value_bytes = encoder.encode(value)  # 直接使用 encoder
       self._prefixed_db.put(key_bytes, value_bytes)
   ```

3. **前缀隔离**
   - 实际存储键：`prefix:field`
   - 例如：`user:1:name`、`user:1:age`

### ✅ 测试覆盖

**单元测试**（17 个）：
- ✓ 基本操作（get/set/del）
- ✓ 迭代（keys/values/items）
- ✓ 字典方法（update/clear/pop 等）
- ✓ NumPy 数组支持
- ✓ 复杂数据类型
- ✓ 多个嵌套字典隔离
- ✓ 数据持久化
- ✓ 类型约束

**性能测试**：
- ✓ 大字典频繁修改
- ✓ 混合读写操作
- ✓ 批量读取性能

### 🔜 未来计划

- [ ] 支持批量操作优化（batch put/delete）
- [ ] 添加前缀范围查询
- [ ] 支持嵌套字典的原子事务
- [ ] 性能监控和统计

### 📚 参考资料

- [plyvel prefixed_db 文档](https://plyvel.readthedocs.io/en/latest/api.html#database)
- LevelDB 前缀查询原理
- Python 字典接口规范

### 👏 致谢

感谢 plyvel 提供的优秀 LevelDB Python 绑定！

---

**完整示例**：运行 `python3 example_nested.py` 查看效果
**详细文档**：查看 `NESTED_DICT_GUIDE.md`
**性能测试**：运行 `python3 benchmark_nested_real.py`
