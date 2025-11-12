# FlaxKV 文件传输功能

## 概述

基于 FlaxKV 的 remote 特性，实现了在服务器之间传输文件和文件夹的 CLI 命令。

## 新增功能

### 1. `flaxkv2 set` - 上传文件/文件夹

将本地文件或文件夹上传到远程 FlaxKV 服务器。

```bash
# 上传文件
flaxkv2 set /path/to/file.txt --server 192.168.1.100:5555

# 上传文件夹
flaxkv2 set /path/to/folder --key my_folder --server 192.168.1.100:5555
```

### 2. `flaxkv2 get` - 下载文件/文件夹

从远程 FlaxKV 服务器下载文件或文件夹。

```bash
# 下载文件
flaxkv2 get my_file --server 192.168.1.100:5555

# 下载到指定路径
flaxkv2 get my_folder --output /path/to/save --server 192.168.1.100:5555
```

### 3. `flaxkv2 list` - 列出服务器文件

查看服务器上存储的所有文件和文件夹。

```bash
flaxkv2 list --server 192.168.1.100:5555
```

## 实现细节

### 文件结构

```
flaxkv2/
├── cli.py                      # CLI 命令实现 (新增 set/get/list)
├── utils/
│   └── file_transfer.py        # 文件打包/解包工具 (新增)
└── ...

tests/
test_file_transfer.py           # 测试脚本
demo_file_transfer.sh           # 演示脚本

docs/
FILE_TRANSFER_GUIDE.md          # 详细使用指南
FILE_TRANSFER_README.md         # 本文件
```

### 核心组件

1. **FileTransferUtil** (`flaxkv2/utils/file_transfer.py`)
   - `pack()`: 打包文件或文件夹
   - `unpack()`: 解包文件或文件夹
   - 文件: 直接存储内容
   - 文件夹: tar.gz 压缩后存储

2. **CLI 命令** (`flaxkv2/cli.py`)
   - `set()`: 上传命令
   - `get()`: 下载命令
   - `list()`: 列表命令

### 数据存储格式

每个上传的文件/文件夹在服务器上存储为两个键:

- `<key>`: 文件内容或压缩后的文件夹内容
- `<key>:meta`: JSON 格式的元数据
  ```json
  {
    "type": "file|folder",
    "name": "原始文件/文件夹名称",
    "size": 123456,
    "mode": 33204  // Unix 文件权限 (仅文件)
  }
  ```

## 快速开始

### 服务器 A: 启动 FlaxKV 服务

```bash
flaxkv2 run --host 0.0.0.0 --port 5555 --data-dir /data/flaxkv
```

### 服务器 B: 上传文件

```bash
# 上传配置文件
flaxkv2 set /etc/myapp/config.yaml --key app_config --server 192.168.1.100:5555

# 上传项目文件夹
flaxkv2 set ~/my_project --key project_v1 --server 192.168.1.100:5555
```

### 服务器 C: 下载文件

```bash
# 列出可用文件
flaxkv2 list --server 192.168.1.100:5555

# 下载配置
flaxkv2 get app_config --output /etc/myapp/ --server 192.168.1.100:5555

# 下载项目
flaxkv2 get project_v1 --output /var/www/ --server 192.168.1.100:5555
```

## 测试

### 运行自动化测试

```bash
# 终端 1: 启动测试服务器
python test_file_transfer.py server

# 终端 2: 测试上传
python test_file_transfer.py upload

# 终端 3: 测试下载
python test_file_transfer.py download
```

### 运行演示脚本

```bash
chmod +x demo_file_transfer.sh
./demo_file_transfer.sh
```

## 使用场景

1. **配置同步**: 在多台服务器间同步配置文件
2. **代码部署**: 从开发服务器部署代码到生产服务器
3. **数据备份**: 备份重要文件到远程服务器
4. **文件共享**: 团队成员间共享文件和文件夹

## 安全注意事项

⚠️ **重要**: FlaxKV 不提供内置加密和认证

- 生产环境建议使用 VPN 或 SSH 隧道
- 不要将服务暴露到公网
- 大文件传输会占用内存，注意资源限制

### SSH 隧道示例

```bash
# 建立安全隧道
ssh -L 5555:localhost:5555 user@remote-server

# 通过隧道连接
flaxkv2 get myfile --server 127.0.0.1:5555
```

## 文档

- [FILE_TRANSFER_GUIDE.md](FILE_TRANSFER_GUIDE.md) - 详细使用指南和示例
- [CLAUDE.md](CLAUDE.md) - 项目整体文档

## 性能特点

- ✅ 支持单个文件和文件夹
- ✅ 自动压缩文件夹 (tar.gz)
- ✅ 保留文件权限
- ✅ 显示进度指示
- ✅ 友好的错误提示
- ✅ 支持自定义服务器地址和数据库名称

## 技术栈

- **LevelDB**: 持久化存储
- **ZeroMQ**: 网络通信
- **tar/gzip**: 文件夹压缩
- **msgpack**: 数据序列化
- **Rich**: 终端 UI (表格、进度条、颜色)
- **Fire**: CLI 框架

## 下一步

可能的改进方向:

- [ ] 添加删除命令 (`flaxkv2 delete`)
- [ ] 支持文件传输进度条 (显示百分比)
- [ ] 支持断点续传 (大文件)
- [ ] 添加文件校验 (MD5/SHA256)
- [ ] 支持文件加密 (可选)
- [ ] 批量上传/下载

## 贡献

欢迎提交 Issue 和 Pull Request!

## License

遵循项目主 License
