# FlaxKV2 Inspector - 可视化工具使用指南

FlaxKV2 Inspector 是一个强大而简洁的数据库可视化和管理工具，提供 CLI 和 Web UI 两种界面。

## 🎯 核心功能

- **数据浏览**: 查看所有键值对，支持搜索和分页
- **统计分析**: 数据类型分布、大小分布、TTL 状态等
- **数据管理**: 增删改查操作，支持 TTL 设置

## 📦 安装

Inspector 的 CLI 工具已经包含在 FlaxKV2 中，无需额外安装。

如果需要使用 Web UI，需要安装 Flask 依赖：

```bash
# 仅安装 Web UI 依赖
pip install flaxkv2[web]

# 或者安装所有可选依赖
pip install flaxkv2[full]
```

## 💻 CLI 工具使用

### 1. 查看所有键

```bash
# 基本用法
flaxkv2 inspect keys mydb --path /data

# 使用模式匹配
flaxkv2 inspect keys mydb --pattern "user.*" --path /data

# 分页显示
flaxkv2 inspect keys mydb --limit 50 --offset 100 --path /data

# 远程数据库
flaxkv2 inspect keys mydb --path 127.0.0.1:5555 --backend remote
```

### 2. 查看键详情

```bash
# 查看单个键的详细信息
flaxkv2 inspect get mydb user123 --path /data

# 远程数据库
flaxkv2 inspect get mydb config --path 127.0.0.1:5555 --backend remote
```

输出示例：
```
┌────────────── 键信息 ───────────────┐
│ 键名: user123                        │
│ 类型: dict                           │
│ 大小: 245 B                          │
│ TTL: 3599.42秒                       │
│ 过期时间: 2025-11-14T02:30:45        │
└──────────────────────────────────────┘

值:
{
  "name": "Alice",
  "age": 30,
  "email": "alice@example.com"
}
```

### 3. 查看统计信息

```bash
# 显示数据库统计
flaxkv2 inspect stats mydb --path /data
```

输出包括：
- 总键数和总大小
- 类型分布（string, integer, list, dict 等）
- 大小分布（tiny, small, medium, large, huge）
- TTL 状态（有 TTL、无 TTL、已过期）

### 4. 搜索键

```bash
# 使用正则表达式搜索
flaxkv2 inspect search mydb "user_.*" --path /data

# 限制结果数量
flaxkv2 inspect search mydb "config_.*" --limit 50 --path /data
```

### 5. 删除键

```bash
# 删除键（需要确认）
flaxkv2 inspect delete mydb temp_data --path /data

# 强制删除（不需要确认）
flaxkv2 inspect delete mydb temp_data --force --path /data
```

### 6. 设置键值

```bash
# 设置字符串值
flaxkv2 inspect set mydb name "John" --path /data

# 设置整数值
flaxkv2 inspect set mydb age "30" --value_type int --path /data

# 设置 JSON 对象
flaxkv2 inspect set mydb config '{"key":"value"}' --value_type json --path /data

# 设置带 TTL 的值
flaxkv2 inspect set mydb temp "data" --ttl 3600 --path /data
```

## 🌐 Web UI 使用

### 启动 Web UI

```bash
# 本地数据库
flaxkv2 web mydb --path /data

# 指定端口
flaxkv2 web mydb --path /data --port 8080

# 远程数据库
flaxkv2 web mydb --path 127.0.0.1:5555 --backend remote

# 自定义主机和端口
flaxkv2 web mydb --path /data --host 0.0.0.0 --port 8080
```

然后在浏览器访问 `http://127.0.0.1:8080` (或您指定的地址)。

### Web UI 界面功能

#### 1. 数据浏览标签页

- **键列表**: 左侧显示所有键，支持分页
- **搜索**: 输入正则表达式搜索键
- **键详情**: 点击键查看详细信息，包括值、类型、大小、TTL 等
- **刷新**: 手动刷新键列表

#### 2. 统计分析标签页

- **总览**: 总键数、总大小
- **类型分布**: 各种数据类型的数量和占比
- **大小分布**: 数据大小的分布情况
- **TTL 状态**: 有 TTL、无 TTL、已过期的键数量

#### 3. 数据管理标签页

- **添加/更新键值**:
  - 输入键名和值（支持 JSON 格式）
  - 可选设置 TTL（秒）
  - 点击保存按钮

- **删除键**:
  - 输入要删除的键名
  - 点击删除按钮（需要确认）

## 🔧 在 Python 代码中使用 Inspector

```python
from flaxkv2.inspector import Inspector

# 创建 Inspector 实例
with Inspector('mydb', '/data') as inspector:
    # 获取键总数
    total = inspector.count_keys()
    print(f"总键数: {total}")

    # 列出所有键
    keys = inspector.list_keys(limit=100)
    print(f"前100个键: {keys}")

    # 搜索键
    results = inspector.search_keys(r"user_\d+", limit=50)
    for key, info in results:
        print(f"{key}: {info['type']}, {info['size']} bytes")

    # 获取键详情
    info = inspector.get_value_info('user123')
    if info:
        print(f"键: {info['key']}")
        print(f"类型: {info['type']}")
        print(f"值: {info['value']}")

    # 获取统计信息
    stats = inspector.get_stats()
    print(f"类型分布: {stats['type_distribution']}")

    # 删除键
    inspector.delete_key('temp_key')

    # 设置键值
    inspector.set_value('new_key', 'new_value', ttl=3600)

    # 导出数据
    data = inspector.export_data(['key1', 'key2'])
```

## 🎨 设计特点

### KISS 原则实践

1. **简单的架构**: 核心 Inspector 模块 + CLI/Web 两个接口层
2. **最小依赖**: CLI 只需 Rich，Web UI 只需 Flask
3. **统一逻辑**: CLI 和 Web UI 共享同一个 Inspector 核心
4. **纯静态前端**: 不依赖复杂的前端框架，纯 HTML/CSS/JS

### 核心模块

- `flaxkv2/inspector/__init__.py`: Inspector 核心类，提供所有功能
- `flaxkv2/inspector/cli.py`: CLI 命令实现
- `flaxkv2/inspector/web.py`: Web UI 服务器
- `flaxkv2/static/`: 静态文件（HTML/CSS/JS）

## 📝 使用场景

### 1. 开发调试

```bash
# 快速查看数据库内容
flaxkv2 inspect keys mydb --path /data | head -20

# 检查某个键是否存在
flaxkv2 inspect get mydb important_key --path /data

# 查看数据库统计
flaxkv2 inspect stats mydb --path /data
```

### 2. 数据管理

```bash
# 批量查找并删除过期数据
flaxkv2 inspect search mydb "temp_.*" --path /data
# 然后手动确认后删除

# 快速设置配置值
flaxkv2 inspect set mydb config '{"debug":true}' --value_type json --path /data
```

### 3. 监控分析

启动 Web UI 进行实时监控：
```bash
flaxkv2 web mydb --path /data --host 0.0.0.0 --port 8080
```

然后在浏览器中：
- 定期刷新统计信息，监控数据增长
- 查看类型分布，了解数据结构
- 检查 TTL 状态，发现过期数据

## 🔒 安全注意事项

1. **Web UI 访问控制**:
   - 默认监听 `127.0.0.1`，仅本地访问
   - 生产环境建议使用 VPN/SSH 隧道或反向代理（如 Nginx）添加认证

2. **远程数据库**:
   - 确保 FlaxKV 服务器网络安全
   - 建议使用防火墙限制访问

3. **删除操作**:
   - CLI 默认需要确认，除非使用 `--force`
   - Web UI 有二次确认对话框

## 🐛 故障排除

### Web UI 无法启动

```bash
# 检查是否安装了 Flask
pip install flaxkv2[web]

# 检查端口是否被占用
lsof -i :8080
```

### 无法连接远程数据库

```bash
# 确保服务器正在运行
flaxkv2 run --host 0.0.0.0 --port 5555 --data-dir /data

# 测试连接
flaxkv2 inspect keys mydb --path 127.0.0.1:5555 --backend remote
```

### 性能问题

- 如果数据库很大（百万级键），统计操作可能较慢
- 建议使用搜索功能而不是列出所有键
- Web UI 的分页功能可以减少加载时间

## 📚 更多信息

- 主项目文档: [README.md](../README.md)
- 开发指南: [CLAUDE.md](../CLAUDE.md)
- 测试用例: [tests/unit/test_inspector.py](../tests/unit/test_inspector.py)
