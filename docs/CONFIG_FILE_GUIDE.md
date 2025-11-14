# FlaxKV2 配置文件指南

FlaxKV2 支持使用 TOML 格式的配置文件，让 CLI 工具的使用更加方便和灵活。

## 快速开始

### 1. 生成示例配置文件

```bash
# 在当前目录生成 flaxkv.toml
flaxkv2 config init

# 或指定路径
flaxkv2 config init --path ~/flaxkv.toml
```

### 2. 编辑配置文件

使用任何文本编辑器编辑生成的配置文件，根据您的环境调整配置。

### 3. 使用配置

配置文件会自动加载，命令行参数会覆盖配置文件中的值。

```bash
# 使用配置文件中的默认服务器
flaxkv2 run

# 使用配置文件中的 production profile
flaxkv2 run --profile production

# 命令行参数覆盖配置文件
flaxkv2 run --port 6666
```

## 配置文件位置

FlaxKV2 按以下顺序查找配置文件：

1. 当前目录：`flaxkv.toml` 或 `.flaxkv.toml`
2. 用户主目录：`~/flaxkv.toml` 或 `~/.flaxkv.toml`

如果找到多个配置文件，优先使用当前目录的配置。优先使用非隐藏文件（`flaxkv.toml`）。

## 配置文件结构

### 默认配置 (全局)

```toml
[defaults]
# 默认数据目录
data_dir = "./data"

# 默认日志级别
log_level = "INFO"

# 默认数据库名称
db_name = "default_db"
```

### 服务器端配置

```toml
[server]
# 监听地址
host = "127.0.0.1"

# 监听端口
port = 5555

# 数据目录
data_dir = "./data"

# 工作线程数
workers = 4

# 日志级别
log_level = "INFO"

# 启用加密 (CurveZMQ)
enable_encryption = false

# 服务器密码（启用加密时使用）
# password = "your-secure-password"

# 从密码派生密钥（推荐）
derive_from_password = true

# 启用压缩 (LZ4)
enable_compression = false

# 性能配置文件
performance_profile = "balanced"
```

### 服务器 Profiles

Profiles 允许您为不同的部署场景定义不同的配置：

```toml
[server.profiles.production]
host = "0.0.0.0"
port = 5555
workers = 8
log_level = "WARNING"
enable_encryption = true
enable_compression = true
performance_profile = "balanced"

[server.profiles.development]
host = "127.0.0.1"
port = 5555
workers = 2
log_level = "DEBUG"
enable_encryption = false
enable_compression = false
performance_profile = "memory_constrained"

[server.profiles.high_performance]
host = "0.0.0.0"
port = 5555
workers = 16
log_level = "WARNING"
enable_compression = true
performance_profile = "read_optimized"
```

使用 profile：

```bash
# 使用 production profile
flaxkv2 run --profile production

# 使用 development profile
flaxkv2 run --profile development
```

### 客户端配置

```toml
[client]
# 默认服务器地址
server = "127.0.0.1:5555"

# 默认数据库名称
db_name = "default_db"

# 后端类型 ('local', 'remote', 'auto')
backend = "auto"

# 本地数据库路径
path = "./data"

# 连接超时（秒）
timeout = 30
```

### 客户端 Profiles

```toml
[client.profiles.local]
backend = "local"
path = "./data"

[client.profiles.remote]
backend = "remote"
server = "127.0.0.1:5555"

[client.profiles.production]
backend = "remote"
server = "prod-server:5555"
timeout = 60
```

使用 profile：

```bash
# 使用 remote profile
flaxkv2 list --profile remote

# 使用 local profile
flaxkv2 inspect keys mydb --profile local
```

### 远程服务器定义

定义多个服务器，通过名称引用：

```toml
[servers.local]
host = "127.0.0.1"
port = 5555

[servers.production]
host = "192.168.1.100"
port = 5555
password = "prod-password"

[servers.staging]
host = "192.168.1.200"
port = 5555
password = "staging-password"

[servers.ml_cluster]
host = "ml.example.com"
port = 5555
password = "ml-password"
```

使用服务器名称（使用 `@` 前缀）：

```bash
# 连接到 production 服务器
flaxkv2 list --server @production

# 上传文件到 staging 服务器
flaxkv2 set myfile.txt --server @staging

# 从 ml_cluster 下载
flaxkv2 get model.pkl --server @ml_cluster
```

### Inspector Web UI 配置

```toml
[inspector]
# 默认监听地址
host = "127.0.0.1"

# 默认监听端口
port = 8080

# 调试模式
debug = false
```

## 配置管理命令

### 显示当前配置

```bash
flaxkv2 config show
```

显示当前加载的配置文件内容。

### 显示配置文件路径

```bash
flaxkv2 config path
```

显示当前使用的配置文件路径。

### 列出所有服务器

```bash
flaxkv2 config servers
```

显示配置文件中定义的所有远程服务器。

### 列出所有 Profiles

```bash
flaxkv2 config profiles
```

显示配置文件中定义的所有服务器和客户端 profiles。

## 使用场景示例

### 场景 1: 本地开发

配置文件：

```toml
[defaults]
data_dir = "./dev_data"
log_level = "DEBUG"

[server]
host = "127.0.0.1"
port = 5555
workers = 2
log_level = "DEBUG"
performance_profile = "memory_constrained"
```

使用：

```bash
# 直接使用默认配置
flaxkv2 run
```

### 场景 2: 多环境部署

配置文件：

```toml
[server.profiles.dev]
host = "127.0.0.1"
port = 5555
workers = 2
log_level = "DEBUG"

[server.profiles.staging]
host = "0.0.0.0"
port = 5555
workers = 4
log_level = "INFO"
enable_encryption = true

[server.profiles.prod]
host = "0.0.0.0"
port = 5555
workers = 16
log_level = "WARNING"
enable_encryption = true
enable_compression = true
performance_profile = "large_database"
```

使用：

```bash
# 开发环境
flaxkv2 run --profile dev

# 预发布环境
flaxkv2 run --profile staging

# 生产环境
flaxkv2 run --profile prod
```

### 场景 3: 多服务器管理

配置文件：

```toml
[client]
server = "127.0.0.1:5555"
db_name = "default_db"

[servers.local]
host = "127.0.0.1"
port = 5555

[servers.backup]
host = "192.168.1.100"
port = 5555

[servers.ml_server]
host = "ml.company.com"
port = 5555
```

使用：

```bash
# 列出本地服务器的文件
flaxkv2 list --server @local

# 上传到备份服务器
flaxkv2 set important_data.tar.gz --server @backup

# 从 ML 服务器下载模型
flaxkv2 get trained_model.pkl --server @ml_server
```

### 场景 4: 团队共享配置

将配置文件提交到 Git 仓库（注意：不要提交包含密码的配置）：

```toml
[servers.team_dev]
host = "dev.team.internal"
port = 5555

[servers.team_staging]
host = "staging.team.internal"
port = 5555

# 密码不提交，在本地环境变量中设置
# password = "${FLAXKV_STAGING_PASSWORD}"
```

团队成员可以：

```bash
# 克隆仓库后
cd project
flaxkv2 config show  # 查看团队配置

# 使用团队服务器
flaxkv2 list --server @team_dev
```

## 配置优先级

配置的优先级从高到低：

1. **命令行参数** - 最高优先级
2. **Profile 配置** - 覆盖默认配置
3. **节默认配置** - [server] 或 [client] 的值
4. **全局默认配置** - [defaults] 的值
5. **程序内置默认值** - 最低优先级

例如：

```toml
[defaults]
port = 9999

[server]
port = 5555

[server.profiles.prod]
port = 6666
```

运行命令时：

```bash
# 使用默认配置：port = 5555
flaxkv2 run

# 使用 prod profile：port = 6666
flaxkv2 run --profile prod

# 命令行覆盖：port = 7777
flaxkv2 run --profile prod --port 7777
```

## 安全建议

1. **不要将包含密码的配置文件提交到版本控制**

   使用 `.gitignore` 忽略包含敏感信息的配置：

   ```
   flaxkv.toml
   .flaxkv.toml
   *secret*.toml
   ```

2. **使用环境变量存储密码**

   虽然 TOML 不直接支持环境变量替换，但可以在应用程序中实现：

   ```python
   import os
   password = os.getenv('FLAXKV_PASSWORD')
   ```

3. **限制配置文件权限**

   ```bash
   chmod 600 ~/flaxkv.toml
   ```

4. **为不同环境使用不同的配置文件**

   ```bash
   # 开发环境
   flaxkv2 run  # 使用 ./flaxkv.toml

   # 生产环境
   flaxkv2 run  # 使用 ~/flaxkv.toml (不同的配置)
   ```

## 故障排查

### 配置文件未被加载

```bash
# 检查配置文件路径
flaxkv2 config path

# 如果未找到，生成一个
flaxkv2 config init
```

### 配置语法错误

TOML 语法错误会导致加载失败。使用在线 TOML 验证器检查语法：
- https://www.toml-lint.com/

常见错误：
- 缺少引号：`host = 127.0.0.1` → `host = "127.0.0.1"`
- 键名重复：同一节下有重复的键
- 节名错误：`[server.profiles-prod]` → `[server.profiles.prod]`

### Profile 不生效

```bash
# 确认 profile 存在
flaxkv2 config profiles

# 检查 profile 名称拼写
# 正确：--profile production
# 错误：--profile Production（区分大小写）
```

### 服务器引用不工作

```bash
# 确认服务器已定义
flaxkv2 config servers

# 使用 @ 前缀
# 正确：--server @production
# 错误：--server production
```

## 最佳实践

1. **为不同环境创建 profiles**

   为开发、测试、生产环境创建独立的 profiles。

2. **使用有意义的服务器名称**

   使用描述性的名称，如 `production-primary`、`backup-server-1`。

3. **文档化自定义配置**

   在配置文件中添加注释说明每个选项的用途。

4. **定期备份配置文件**

   特别是包含重要服务器信息的配置。

5. **使用版本控制管理团队配置**

   但要排除包含密码的配置。

6. **利用配置优先级**

   在配置文件中设置常用值，用命令行参数覆盖特殊情况。

## 相关文档

- [CLI 使用指南](CLI_GUIDE.md)
- [Inspector 工具文档](INSPECTOR.md)
- [性能配置指南](LEVELDB_CONFIGURATION_GUIDE.md)
- [安全最佳实践](SECURITY.md)
