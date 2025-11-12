# FlaxKV 文件传输功能使用指南

FlaxKV 现在支持通过 remote 特性在服务器之间传输文件和文件夹。

## 功能概述

- **上传 (`set`)**: 将本地文件或文件夹上传到远程 FlaxKV 服务器
- **下载 (`get`)**: 从远程 FlaxKV 服务器下载文件或文件夹到本地

## 使用场景

1. 服务器间文件同步
2. 备份重要文件到远程服务器
3. 共享文件夹给团队成员
4. 快速部署配置文件到多台服务器

## 快速开始

### 第一步: 在服务器 A 上启动 FlaxKV 服务

```bash
# 启动服务器，监听所有网络接口
flaxkv2 run --host 0.0.0.0 --port 5555 --data-dir /data/flaxkv
```

参数说明:
- `--host`: 监听地址 (0.0.0.0 表示监听所有接口，允许远程连接)
- `--port`: 监听端口 (默认 5555)
- `--data-dir`: 数据存储目录

### 第二步: 在服务器 B 上上传文件

```bash
# 上传单个文件
flaxkv2 set /path/to/file.txt --server 192.168.1.100:5555

# 上传文件并指定键名
flaxkv2 set /path/to/config.yaml --key production_config --server 192.168.1.100:5555

# 上传整个文件夹
flaxkv2 set /path/to/folder --key my_project --server 192.168.1.100:5555
```

### 第三步: 在服务器 C 上下载文件

```bash
# 下载文件到当前目录
flaxkv2 get production_config --server 192.168.1.100:5555

# 下载到指定目录
flaxkv2 get my_project --output /opt/projects --server 192.168.1.100:5555

# 下载并重命名
flaxkv2 get production_config --output /etc/myapp/config.yaml --server 192.168.1.100:5555
```

## 命令详解

### flaxkv2 set - 上传文件/文件夹

```bash
flaxkv2 set <path> [--key KEY] [--server SERVER] [--db_name DB_NAME]
```

参数:
- `path` (必需): 要上传的文件或文件夹路径
- `--key`: 存储的键名，默认使用文件/文件夹名称
- `--server`: 远程服务器地址，格式为 `host:port`，默认 `127.0.0.1:5555`
- `--db_name`: 数据库名称，默认 `file_storage`

示例:
```bash
# 上传配置文件
flaxkv2 set /etc/nginx/nginx.conf --key nginx_config --server 10.0.1.50:5555

# 上传项目文件夹
flaxkv2 set ~/my_project --key project_v1.0 --server 10.0.1.50:5555

# 上传到本地服务器
flaxkv2 set data.csv --key dataset_2024
```

### flaxkv2 get - 下载文件/文件夹

```bash
flaxkv2 get <key> [--output OUTPUT] [--server SERVER] [--db_name DB_NAME]
```

参数:
- `key` (必需): 要下载的键名
- `--output`: 保存路径，默认为当前目录
- `--server`: 远程服务器地址，格式为 `host:port`，默认 `127.0.0.1:5555`
- `--db_name`: 数据库名称，默认 `file_storage`

示例:
```bash
# 下载到当前目录
flaxkv2 get nginx_config --server 10.0.1.50:5555

# 下载到指定位置
flaxkv2 get project_v1.0 --output /var/www --server 10.0.1.50:5555

# 从本地服务器下载
flaxkv2 get dataset_2024 --output ~/data/
```

## 实际应用示例

### 示例 1: 配置文件同步

在主服务器上上传配置:
```bash
flaxkv2 set /etc/myapp/config.yaml --key app_config --server config.example.com:5555
```

在多台应用服务器上下载:
```bash
# 服务器 1
flaxkv2 get app_config --output /etc/myapp/config.yaml --server config.example.com:5555

# 服务器 2
flaxkv2 get app_config --output /etc/myapp/config.yaml --server config.example.com:5555

# 服务器 N...
```

### 示例 2: 代码部署

开发服务器上传代码:
```bash
cd /home/dev/myapp
flaxkv2 set ./dist --key myapp_v2.1.0 --server deploy.example.com:5555
```

生产服务器下载部署:
```bash
flaxkv2 get myapp_v2.1.0 --output /var/www/myapp --server deploy.example.com:5555
systemctl restart myapp
```

### 示例 3: 数据备份

备份重要数据到远程服务器:
```bash
# 备份数据库导出文件
flaxkv2 set /backup/db_dump_20240101.sql --key db_backup_latest --server backup.example.com:5555

# 备份整个配置目录
flaxkv2 set /etc/myapp --key myapp_config_backup --server backup.example.com:5555
```

恢复数据:
```bash
# 从备份服务器恢复
flaxkv2 get db_backup_latest --output /restore/ --server backup.example.com:5555
```

### 示例 4: 团队文件共享

上传共享文件:
```bash
# 共享文档
flaxkv2 set ~/Documents/team_handbook.pdf --key handbook --server share.example.com:5555

# 共享项目模板
flaxkv2 set ~/templates/project_template --key template_v1 --server share.example.com:5555
```

团队成员下载:
```bash
# 下载手册
flaxkv2 get handbook --server share.example.com:5555

# 下载模板
flaxkv2 get template_v1 --output ~/workspace --server share.example.com:5555
```

## 技术细节

### 文件打包方式

- **单个文件**: 直接存储文件内容和元数据
- **文件夹**: 使用 tar.gz 格式压缩后存储

### 元数据存储

每个上传的文件/文件夹会存储两个键:
- `<key>`: 文件内容或打包后的文件夹
- `<key>:meta`: 元数据 (JSON格式)，包含类型、名称、大小等信息

### 文件权限

上传时会保存文件权限 (Unix mode)，下载时会恢复

## 注意事项

### 安全建议

1. **网络安全**:
   - FlaxKV 不提供内置的加密和认证
   - 建议在生产环境中使用 VPN 或 SSH 隧道
   - 不要将服务暴露到公网

2. **SSH 隧道示例**:
   ```bash
   # 在本地机器上建立 SSH 隧道
   ssh -L 5555:localhost:5555 user@remote-server

   # 然后连接到 localhost:5555 即可安全访问远程服务
   flaxkv2 get myfile --server 127.0.0.1:5555
   ```

### 性能优化

1. **大文件传输**: 对于非常大的文件 (>1GB)，建议:
   - 使用性能配置文件: `--profile large_database`
   - 考虑分块上传或使用专门的文件传输工具

2. **批量操作**: 如需传输多个文件，建议先打包成文件夹再上传

### 限制

1. 文件大小受 LevelDB 和系统内存限制
2. 大文件会完整加载到内存，注意内存使用
3. 网络传输速度受带宽限制

## 故障排除

### 连接失败

```bash
错误: 无法连接到服务器
```

解决:
1. 检查服务器是否运行: `ss -tuln | grep 5555`
2. 检查防火墙设置
3. 验证服务器地址和端口是否正确

### 键不存在

```bash
错误: 键不存在: myfile
```

解决:
1. 使用 Python 客户端列出所有键:
   ```python
   from flaxkv2 import FlaxKV
   db = FlaxKV('file_storage', '127.0.0.1:5555', backend='remote')
   print(list(db.keys()))
   ```

### 元数据缺失

```bash
错误: 元数据不存在，该键可能不是通过 'flaxkv set' 上传的
```

解决:
- 确保使用 `flaxkv2 set` 命令上传的文件
- 不要手动修改或删除 `<key>:meta` 键

## 高级用法

### 使用不同的数据库

```bash
# 上传到 'images' 数据库
flaxkv2 set photo.jpg --key avatar --server 10.0.1.50:5555 --db_name images

# 从 'images' 数据库下载
flaxkv2 get avatar --server 10.0.1.50:5555 --db_name images
```

### Python API 集成

你也可以在 Python 代码中使用文件传输功能:

```python
from flaxkv2 import FlaxKV
from flaxkv2.utils.file_transfer import FileTransferUtil
import json

# 连接到远程服务器
db = FlaxKV('file_storage', '192.168.1.100:5555', backend='remote')

# 上传文件
content, metadata = FileTransferUtil.pack('/path/to/file.txt')
db['myfile'] = content
db['myfile:meta'] = json.dumps(metadata)

# 下载文件
content = db['myfile']
metadata = json.loads(db['myfile:meta'])
FileTransferUtil.unpack(content, metadata, '/output/path')

db.close()
```

## 总结

FlaxKV 的文件传输功能提供了一个简单但强大的方式在服务器之间传输文件和文件夹。通过 `set` 和 `get` 命令，你可以轻松实现配置同步、代码部署、数据备份等常见任务。

如有问题或建议，欢迎提交 Issue 或 Pull Request!
