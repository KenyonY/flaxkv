# NestedDBDict 快速开始

## 5 分钟上手

### 问题

```python
# ❌ 性能问题：修改单个字段需要序列化整个字典
db['user:1'] = {'name': 'Alice', 'age': 30, 'email': '...', ...}  # 假设有1000个字段
data = db['user:1']  # 反序列化整个字典
data['age'] = 31
db['user:1'] = data  # 重新序列化整个字典
```

### 解决方案

```python
# ✅ 高性能：每个字段独立存储
user = db.nested('user:1')
user['name'] = 'Alice'
user['age'] = 30
user['email'] = '...'

# 修改单个字段只序列化该字段
user['age'] = 31  # 快 18.6 倍！
```

## 基本用法

```python
from flaxkv2 import LevelDBDict

db = LevelDBDict('mydb')

# 1. 创建嵌套字典
user = db.nested('user:1')

# 2. 设置字段
user['name'] = 'Alice'
user['age'] = 30

# 3. 读取字段
print(user['name'])  # 'Alice'

# 4. 修改字段
user['age'] = 31

# 5. 迭代所有字段
for key, value in user.items():
    print(f"{key}: {value}")

# 6. 批量更新
user.update({'city': 'NYC', 'country': 'USA'})

# 7. 转换为字典
data = user.to_dict()
```

## 性能对比

| 场景 | 传统方式 | NestedDBDict | 提升 |
|------|---------|--------------|------|
| 1000字段，修改100次 | 8ms | 0.4ms | **18.6x** |
| 500字段，混合读写 | 1.1ms | 0.6ms | **1.8x** |

## 推荐场景

✅ 用户配置、会话数据
✅ 缓存对象（字段数 > 100）
✅ 实时指标、计数器
✅ ML 模型参数存储

## 完整示例

查看：
- `example_nested.py` - 完整示例
- `NESTED_DICT_GUIDE.md` - 详细指南
- `benchmark_nested_real.py` - 性能测试

## 立即体验

```bash
python3 example_nested.py
```

🚀 让你的嵌套数据飞起来！
