# FlaxKV2 故障排查指南

本文档收集了 FlaxKV2 使用过程中常见的问题及解决方案。

---

## 目录

1. [安装问题](#安装问题)
2. [连接问题](#连接问题)
3. [性能问题](#性能问题)
4. [数据问题](#数据问题)
5. [TTL 相关问题](#ttl-相关问题)
6. [远程访问问题](#远程访问问题)
7. [加密和认证问题](#加密和认证问题)

---

## 安装问题

### Q: 安装 plyvel 失败

**错误信息**:
```
error: command 'gcc' failed with exit status 1
```

**解决方案**:

plyvel 需要 LevelDB 开发库和 C++ 编译器。

**macOS**:
```bash
brew install leveldb
pip install plyvel
```

**Ubuntu/Debian**:
```bash
sudo apt-get install libleveldb-dev python3-dev
pip install plyvel
```

**CentOS/RHEL**:
```bash
sudo yum install leveldb-devel python3-devel gcc-c++
pip install plyvel
```

### Q: ImportError: No module named 'zmq'

**解决方案**:
```bash
pip install pyzmq
```

如果仍然失败，尝试安装开发依赖：
```bash
# Ubuntu/Debian
sudo apt-get install libzmq3-dev

# macOS
brew install zeromq
```

### Q: 安装完整功能依赖

**解决方案**:
```bash
# 安装所有可选依赖（Pandas、Flask Web UI 等）
pip install flaxkv2[full]

# 只安装测试依赖
pip install flaxkv2[test]
```

---

## 连接问题

### Q: 无法连接到远程服务器

**错误信息**:
```
ConnectionError: Unable to connect to tcp://127.0.0.1:5555
```

**排查步骤**:

1. **确认服务器正在运行**:
   ```bash
   # 检查进程
   ps aux | grep flaxkv2

   # 检查端口
   lsof -i :5555
   ```

2. **确认地址和端口正确**:
   ```python
   # 本地连接
   db = FlaxKV('mydb', 'tcp://127.0.0.1:5555')

   # 远程连接（确保防火墙已开放端口）
   db = FlaxKV('mydb', 'tcp://192.168.1.100:5555')
   ```

3. **检查防火墙设置**:
   ```bash
   # Ubuntu
   sudo ufw allow 5555/tcp

   # CentOS
   sudo firewall-cmd --add-port=5555/tcp --permanent
   sudo firewall-cmd --reload
   ```

### Q: 连接超时

**错误信息**:
```
TimeoutError: Connection timed out
```

**解决方案**:

增加连接超时时间：
```python
from flaxkv2.client.zmq_client import RemoteDBDict

db = RemoteDBDict(
    'mydb',
    'tcp://server:5555',
    timeout=30000  # 30秒超时（毫秒）
)
```

### Q: 数据库已被其他进程锁定

**错误信息**:
```
plyvel.IOError: IO error: lock /path/to/db/LOCK: Resource temporarily unavailable
```

**解决方案**:

LevelDB 只允许单进程访问。检查是否有其他进程正在使用该数据库：
```bash
# 查找锁定进程
lsof /path/to/db/LOCK

# 如果确认可以安全终止，使用
flaxkv2 kill <pid>
```

或者使用远程模式，启动服务器后多个客户端可以共享访问：
```bash
# 启动服务器
flaxkv2 run --port 5555 --data-dir /path/to/data

# 客户端连接
db = FlaxKV('mydb', 'tcp://127.0.0.1:5555')
```

---

## 性能问题

### Q: 写入性能差

**排查步骤**:

1. **使用缓存模式**:
   ```python
   # 启用写缓冲，显著提升写入性能
   db = FlaxKV(
       'mydb', './data',
       write_buffer_size=1000,   # 缓冲1000条写入
       async_flush=True          # 异步刷新
   )
   ```

2. **使用批量写入**:
   ```python
   # 批量更新比逐条写入更快
   db.update({
       'key1': 'value1',
       'key2': 'value2',
       # ...
   })
   ```

3. **使用性能配置文件**:
   ```python
   db = FlaxKV('mydb', './data', performance_profile='write_optimized')
   ```

### Q: 读取性能差

**解决方案**:

1. **启用读缓存**:
   ```python
   db = FlaxKV(
       'mydb', './data',
       read_cache_size=10000,  # 缓存10000条热数据
   )
   ```

2. **使用读优化配置**:
   ```python
   db = FlaxKV('mydb', './data', performance_profile='read_optimized')
   ```

### Q: 内存使用过高

**解决方案**:

1. **使用内存受限配置**:
   ```python
   db = FlaxKV('mydb', './data', performance_profile='memory_constrained')
   ```

2. **减小缓存大小**:
   ```python
   db = FlaxKV(
       'mydb', './data',
       read_cache_size=1000,    # 减小读缓存
       write_buffer_size=100,   # 减小写缓冲
   )
   ```

3. **使用流式迭代**:
   ```python
   # 不要用 keys()，改用 keys_iter()
   for key in db.keys_iter():
       process(key)
   ```

### Q: 大数据库启动慢

**解决方案**:

1. **使用大数据库配置**:
   ```python
   db = FlaxKV('mydb', './data', performance_profile='large_database')
   ```

2. **避免启动时加载所有键**:
   ```python
   # 不要在启动时调用 len(db) 或 db.keys()
   # 使用 keys_iter() 进行增量处理
   ```

---

## 数据问题

### Q: 数据读取为 None

**排查步骤**:

1. **检查键是否存在**:
   ```python
   if 'mykey' in db:
       value = db['mykey']
   else:
       print('键不存在')
   ```

2. **检查 TTL 是否过期**:
   ```python
   ttl = db.get_ttl('mykey')
   if ttl is not None and ttl <= 0:
       print('键已过期')
   ```

3. **使用 Inspector 查看原始数据**:
   ```bash
   flaxkv2 inspect keys mydb --path ./data
   flaxkv2 inspect get mydb mykey --path ./data
   ```

### Q: 嵌套字典数据丢失

**可能原因**:

使用 `auto_nested=True` 时，字典值会自动转换为嵌套结构。修改后需要正确保存：

```python
# 错误方式（不会保存）
user = db['user:1']
user['name'] = 'new_name'  # 这只修改了内存中的对象

# 正确方式1：直接修改嵌套字段
db['user:1']['name'] = 'new_name'

# 正确方式2：重新赋值整个对象
user = dict(db['user:1'])  # 转为普通 dict
user['name'] = 'new_name'
db['user:1'] = user
```

### Q: 序列化错误

**错误信息**:
```
TypeError: Object of type 'XXX' is not serializable
```

**解决方案**:

FlaxKV2 支持以下类型：
- 基本类型：str, int, float, bool, None, bytes
- 容器类型：list, tuple, dict
- NumPy 数组
- Pandas DataFrame（需安装 pandas）

对于自定义对象，需要先转换：
```python
# 自定义类
class User:
    def __init__(self, name):
        self.name = name

    def to_dict(self):
        return {'name': self.name}

# 存储
db['user'] = User('Alice').to_dict()
```

---

## TTL 相关问题

### Q: TTL 不生效

**排查步骤**:

1. **确认设置了 default_ttl 或单独 TTL**:
   ```python
   # 方式1：默认 TTL
   db = FlaxKV('mydb', './data', default_ttl=3600)

   # 方式2：单独设置
   db.set('mykey', 'myvalue', ttl=3600)

   # 方式3：已有键设置 TTL
   db.set_ttl('existing_key', 1800)
   ```

2. **检查 TTL 值**:
   ```python
   remaining = db.get_ttl('mykey')
   print(f'剩余 TTL: {remaining} 秒')
   ```

### Q: 过期数据没有立即删除

**说明**:

FlaxKV2 使用惰性删除 + 后台清理的策略：
- **惰性删除**: 访问时检查是否过期，过期则返回 None
- **后台清理**: 定期清理过期数据（默认 60 秒间隔）

如需立即清理：
```python
# 手动触发清理
cleaned = db.cleanup_expired()
print(f'清理了 {cleaned} 个过期键')
```

---

## 远程访问问题

### Q: 服务器启动失败

**错误信息**:
```
zmq.error.ZMQError: Address already in use
```

**解决方案**:

端口已被占用，使用其他端口或终止占用进程：
```bash
# 查看端口占用
lsof -i :5555

# 使用 kill 命令终止
flaxkv2 kill 5555

# 或使用其他端口
flaxkv2 run --port 5556
```

### Q: 远程写入数据丢失

**可能原因**:

1. 服务器异常终止前没有 flush
2. 客户端没有正确关闭连接

**解决方案**:

使用上下文管理器确保正确关闭：
```python
with FlaxKV('mydb', 'tcp://server:5555') as db:
    db['key'] = 'value'
# 自动关闭并 flush
```

或手动关闭：
```python
db = FlaxKV('mydb', 'tcp://server:5555')
try:
    db['key'] = 'value'
finally:
    db.close()
```

---

## 加密和认证问题

### Q: 加密连接失败

**错误信息**:
```
zmq.error.ZMQError: No such file or directory
```

**排查步骤**:

1. **确认服务器启用了加密**:
   ```bash
   flaxkv2 run --enable-encryption --password your_password
   ```

2. **确认客户端使用相同密码**:
   ```python
   db = FlaxKV(
       'mydb', 'tcp://server:5555',
       enable_encryption=True,
       password='your_password'
   )
   ```

3. **检查密钥文件权限**（如果使用文件存储密钥）:
   ```bash
   ls -la ~/.flaxkv2/keys/
   # 应该只有所有者有读写权限
   chmod 600 ~/.flaxkv2/keys/*
   ```

### Q: 认证失败

**错误信息**:
```
AuthenticationError: Invalid credentials
```

**解决方案**:

确保服务器和客户端使用相同的密码：
```bash
# 服务器
flaxkv2 run --enable-encryption --password correct_password

# 客户端
db = FlaxKV('mydb', 'tcp://server:5555', password='correct_password')
```

详见 [密码认证指南](development/PASSWORD_AUTH_GUIDE.md)。

---

## 其他问题

### Q: 如何查看数据库状态

```python
# 获取统计信息
stats = db.stat()
print(stats)

# 使用 Inspector
flaxkv2 inspect stats mydb --path ./data
```

### Q: 如何备份数据库

```bash
# 方式1：直接复制数据库目录（需要先停止服务）
cp -r /path/to/data/mydb /backup/mydb

# 方式2：导出为 JSON
flaxkv2 inspect export mydb --path ./data --output backup.json
```

### Q: 如何升级数据库格式

FlaxKV2 保持向后兼容。升级版本后，旧数据可以直接读取。

如遇格式问题，可以导出后重新导入：
```bash
# 导出
flaxkv2 inspect export mydb --path ./data --output backup.json

# 删除旧数据库并重建
rm -rf /path/to/data/mydb

# 导入
flaxkv2 inspect import mydb --path ./data --input backup.json
```

---

## 获取帮助

如果上述方案没有解决您的问题：

1. **查看详细日志**:
   ```bash
   # 启用 DEBUG 日志
   export FLAXKV2_LOG_LEVEL=DEBUG
   ```

2. **提交 Issue**: [GitHub Issues](https://github.com/KenyonY/flaxkv2/issues)

提交 Issue 时请提供：
- FlaxKV2 版本 (`flaxkv2 version`)
- Python 版本
- 操作系统
- 完整错误信息
- 最小可复现代码

---

**最后更新**: 2025-11-28
