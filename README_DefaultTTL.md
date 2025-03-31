# FlaxKV 默认 TTL 功能

## 功能说明

FlaxKV 现在支持设置默认 TTL（生存时间）功能，通过这个功能，你可以为数据库实例设置一个全局默认的 TTL 值，使得所有新添加的键值对都会自动应用这个 TTL，无需手动为每个键单独设置。

## 主要特点

1. **全局默认设置**：一次设置，应用于所有新增的键值对
2. **动态可调整**：可以随时更新或禁用默认 TTL 值
3. **优先级灵活**：对于特殊键，仍可以单独设置 TTL 覆盖默认值
4. **持久化兼容**：与现有的 TTL 持久化机制完全兼容

## 使用方法

### 创建时设置默认 TTL

在创建 FlaxKV 实例时，可以通过 `default_ttl` 参数设置默认 TTL 值：

```python
from flaxkv2 import FlaxKV

# 创建带有默认 TTL=60 秒的数据库
db = FlaxKV("my_db", "./data", default_ttl=60)

# 添加键值对（自动应用默认 TTL=60 秒）
db["key1"] = "value1"
db["key2"] = "value2"

# 查看 TTL
print(f"key1 的 TTL: {db.get_ttl('key1')}")  # 输出：key1 的 TTL: 59.99...
print(f"key2 的 TTL: {db.get_ttl('key2')}")  # 输出：key2 的 TTL: 59.99...
```

### 动态更新默认 TTL

可以随时通过 `set_default_ttl` 方法更新默认 TTL 值：

```python
# 更新默认 TTL 为 30 秒
db.set_default_ttl(30)

# 添加新键值对（应用新的默认 TTL=30 秒）
db["key3"] = "value3"

# 查看 TTL
print(f"key3 的 TTL: {db.get_ttl('key3')}")  # 输出：key3 的 TTL: 29.99...

# 之前的键不受影响
print(f"key1 的 TTL: {db.get_ttl('key1')}")  # 输出原来设置的 TTL
```

### 禁用默认 TTL

如果不再需要默认 TTL，可以将其设置为 `None` 来禁用：

```python
# 禁用默认 TTL
db.set_default_ttl(None)

# 添加新键值对（不再应用默认 TTL）
db["key4"] = "value4"

# 查看 TTL
print(f"key4 的 TTL: {db.get_ttl('key4')}")  # 输出：None
```

### 获取当前默认 TTL 值

可以通过 `get_default_ttl` 方法获取当前的默认 TTL 值：

```python
# 获取当前默认 TTL
default_ttl = db.get_default_ttl()
print(f"当前默认 TTL: {default_ttl}")
```

### 覆盖默认 TTL

即使设置了默认 TTL，仍然可以为特定的键单独设置 TTL 值来覆盖默认设置：

```python
# 设置默认 TTL 为 60 秒
db.set_default_ttl(60)

# 添加键值对（应用默认 TTL=60 秒）
db["key5"] = "value5"

# 为特定键覆盖默认 TTL
db.set_ttl("key5", 10)  # 设置为 10 秒，覆盖默认的 60 秒

# 查看 TTL
print(f"key5 的 TTL: {db.get_ttl('key5')}")  # 输出：key5 的 TTL: 9.99...
```

## 注意事项

1. 默认 TTL 仅适用于新添加的键值对，对已存在的键值对没有影响。
2. 更新默认 TTL 也不会影响已存在的键值对，只影响之后新添加的键值对。
3. 默认 TTL 值必须是整数（秒），或者 `None`（表示禁用）。
4. 如果键已存在，更新其值不会自动应用默认 TTL，除非显式设置。
5. 如果使用 `update()` 方法批量添加键值对，也会应用默认 TTL。

## 技术细节

默认 TTL 功能在以下层面实现：

1. **FlaxKV 工厂函数**：支持 `default_ttl` 参数，并将其传递给实际的数据库实现。
2. **LevelDBDict 实现**：存储默认 TTL 值，并在 `__setitem__` 方法中自动应用。
3. **RemoteDBDict 实现**：同样支持默认 TTL，确保分布式环境下的一致行为。
4. **TTL 持久化**：与现有的 TTL 持久化机制完全兼容，确保即使在程序重启后，TTL 也能正常工作。 