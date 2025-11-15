# FlaxKV2 大文件分块传输指南

## 📖 概述

FlaxKV2 提供了高效的大文件分块传输功能，适用于传输大于 100MB 的文件。通过将文件分成多个小块（默认 10MB/块）进行传输，可以：

- ✅ **降低内存占用**：只需加载一个分块到内存
- ✅ **避免超时**：小分块传输不易超时
- ✅ **完整性验证**：自动计算并验证 SHA256 哈希值
- ✅ **进度显示**：实时显示上传/下载进度
- ✅ **断点续传**：未来版本将支持

## 🚀 快速开始

### 1. 启动服务器

```bash
# 启动加密的服务器
flaxkv2 run --port 25555 --enable-encryption --password yao
```

### 2. 使用 Python API

```python
from flaxkv2 import FlaxKV
from flaxkv2.utils.file_transfer import upload_large_file, download_large_file

# 连接到服务器
db = FlaxKV(
    "large_files",
    "tcp://127.0.0.1:25555",
    enable_encryption=True,
    password="yao"
)

# 上传大文件（自动分块）
upload_large_file(
    db,
    key="my_video",
    file_path="/path/to/video.mp4",
    chunk_size=10 * 1024 * 1024,  # 10MB 分块
    show_progress=True,
    verify=True
)

# 下载文件
download_large_file(
    db,
    key="my_video",
    output_path="./downloads/",
    show_progress=True,
    verify=True
)

db.close()
```

### 3. 使用 CLI 命令

CLI 会自动检测文件/目录大小，超过 100MB 自动使用分块传输，**默认使用并行模式**：

#### 上传文件

```bash
# 上传文件（> 100MB 自动并行分块上传，默认 5 个线程）
flaxkv2 set /path/to/large_file.mp4

# 指定并行线程数（8 个线程）
flaxkv2 set /path/to/large_file.mp4 --max-workers 8

# 强制使用串行传输（稳定模式）
flaxkv2 set /path/to/large_file.mp4 --serial

# 自定义分块大小（20MB）
flaxkv2 set /path/to/large_file.mp4 --chunk-size 20971520
```

#### 上传目录

```bash
# 上传目录（自动打包成 tar.gz）
# 打包后 > 100MB 自动使用分块上传
flaxkv2 set /path/to/my_project

# 大目录使用并行上传
flaxkv2 set /path/to/large_project --max-workers 8

# 不稳定网络使用串行上传
flaxkv2 set /path/to/large_project --serial
```

**目录上传处理流程**：
1. 先将目录打包成临时 tar.gz 文件
2. 检查打包后文件大小
3. 如果 > 100MB，使用分块并行上传
4. 如果 < 100MB，使用传统一次性上传
5. 上传完成后自动删除临时文件

#### 下载文件/目录

```bash
# 下载文件（自动检测分块文件，默认并行下载）
flaxkv2 get my_video --output ./downloads/

# 下载目录（自动检测并解包）
flaxkv2 get my_project --output ./downloads/

# 串行下载
flaxkv2 get my_video --output ./downloads/ --serial

# 指定下载线程数
flaxkv2 get my_video --output ./downloads/ --max-workers 8
```

## ⚡ 串行 vs 并行传输

FlaxKV2 提供两种传输模式：

### 串行模式（默认）
- **函数**: `upload_large_file()`, `download_large_file()`
- **特点**: 按顺序逐个传输分块
- **适用场景**:
  - 网络带宽有限
  - 服务器负载较高
  - 追求稳定性优先
- **优势**: 稳定可靠，资源占用低

### 并行模式（多线程）
- **函数**: `upload_large_file_parallel()`, `download_large_file_parallel()`
- **特点**: 使用多线程同时传输多个分块
- **适用场景**:
  - 网络带宽充足（100Mbps+）
  - 追求传输速度
  - 服务器性能充足
- **优势**: 传输速度快，充分利用网络带宽
- **技术细节**:
  - **每个线程创建独立的数据库连接**，解决 ZMQ socket 线程安全问题
  - 真正的并行传输，而非串行队列
  - 下载时虽然并行获取分块，但写入文件是顺序的，确保数据完整性
  - 所有分块下载完成后才开始写入文件
  - 完整的 SHA256 哈希验证

## 📚 Python API 详解

### upload_large_file()（串行）

分块上传大文件。

**参数：**
- `db`: FlaxKV 实例
- `key`: 存储键名
- `file_path`: 本地文件路径
- `chunk_size`: 分块大小（字节），默认 10MB
- `show_progress`: 是否显示进度，默认 True
- `verify`: 是否计算并验证哈希值，默认 True

**返回：**
包含上传信息的字典：
```python
{
    'filename': '文件名',
    'size': 文件大小（字节）,
    'chunks': 分块数量,
    'chunk_size': 分块大小,
    'hash': 'SHA256 哈希值',
    'status': 'completed'
}
```

**示例：**
```python
info = upload_large_file(
    db,
    key="large_video",
    file_path="/path/to/video.mp4",
    chunk_size=20 * 1024 * 1024,  # 20MB
    verify=True
)

print(f"上传完成: {info['filename']}")
print(f"分块数: {info['chunks']}")
print(f"哈希值: {info['hash']}")
```

### download_large_file()（串行）

分块下载大文件。

**参数：**
- `db`: FlaxKV 实例
- `key`: 存储键名
- `output_path`: 输出路径（文件路径或目录路径）
- `show_progress`: 是否显示进度，默认 True
- `verify`: 是否验证哈希值，默认 True

**返回：**
包含下载信息的字典（元数据）

**示例：**
```python
# 下载到指定目录（使用原文件名）
info = download_large_file(
    db,
    key="large_video",
    output_path="./downloads/",
    verify=True
)

# 下载并重命名
info = download_large_file(
    db,
    key="large_video",
    output_path="./my_video.mp4",
    verify=True
)
```

### upload_large_file_parallel()（并行）

并行分块上传大文件（多线程版本）。

**参数：**
- `db`: FlaxKV 实例
- `key`: 存储键名
- `file_path`: 本地文件路径
- `chunk_size`: 分块大小（字节），默认 10MB
- `max_workers`: 最大并行线程数，默认 5
- `show_progress`: 是否显示进度，默认 True
- `verify`: 是否计算并验证哈希值，默认 True

**返回：**
包含上传信息的字典（同 upload_large_file）

**示例：**
```python
from flaxkv2.utils.file_transfer import upload_large_file_parallel

# 并行上传（使用 5 个线程）
info = upload_large_file_parallel(
    db,
    key="large_video",
    file_path="/path/to/video.mp4",
    chunk_size=20 * 1024 * 1024,  # 20MB
    max_workers=5,
    verify=True
)

print(f"上传完成: {info['filename']}")
print(f"分块数: {info['chunks']}")
```

### download_large_file_parallel()（并行）

并行分块下载大文件（多线程版本）。

**参数：**
- `db`: FlaxKV 实例
- `key`: 存储键名
- `output_path`: 输出路径（文件路径或目录路径）
- `max_workers`: 最大并行线程数，默认 5
- `show_progress`: 是否显示进度，默认 True
- `verify`: 是否验证哈希值，默认 True

**返回：**
包含下载信息的字典（元数据）

**注意：** 虽然下载是并行的，但写入文件是顺序的，确保数据完整性。

**示例：**
```python
from flaxkv2.utils.file_transfer import download_large_file_parallel

# 并行下载（使用 8 个线程）
info = download_large_file_parallel(
    db,
    key="large_video",
    output_path="./downloads/",
    max_workers=8,
    verify=True
)

print(f"下载完成: {info['filename']}")
```

### list_large_files()

列出所有分块存储的文件。

**参数：**
- `db`: FlaxKV 实例
- `show_details`: 是否显示详细信息，默认 False

**返回：**
文件列表 `[(key, metadata), ...]`

**示例：**
```python
# 显示详细信息
files = list_large_files(db, show_details=True)

# 编程方式获取文件列表
files = list_large_files(db, show_details=False)
for key, meta in files:
    print(f"{key}: {meta['filename']} ({meta['size']} bytes)")
```

### delete_large_file()

删除分块文件（包括所有分块和元数据）。

**参数：**
- `db`: FlaxKV 实例
- `key`: 存储键名
- `show_progress`: 是否显示进度，默认 True

**返回：**
`bool` - 是否成功删除

**示例：**
```python
success = delete_large_file(db, "large_video")
if success:
    print("删除成功")
```

## 🎯 使用场景

### 场景 1：视频文件传输

```python
# 上传 2GB 视频文件
with FlaxKV("videos", "tcp://server:5555",
           enable_encryption=True, password="secret") as db:
    upload_large_file(
        db,
        key="project_video_v2",
        file_path="/path/to/project_video.mp4",
        chunk_size=20 * 1024 * 1024  # 20MB 分块
    )
```

### 场景 2：数据备份

```python
# 备份大型数据库文件
with FlaxKV("backups", "tcp://backup-server:5555",
           enable_encryption=True, password="backup123") as db:
    upload_large_file(
        db,
        key=f"db_backup_{datetime.now().strftime('%Y%m%d')}",
        file_path="/var/lib/postgresql/data/backup.sql",
        chunk_size=50 * 1024 * 1024  # 50MB 分块
    )
```

### 场景 3：机器学习模型分发

```python
# 上传训练好的模型文件
with FlaxKV("ml_models", "tcp://ml-server:5555",
           enable_encryption=True, password="ml123") as db:
    # 上传模型权重文件
    upload_large_file(
        db,
        key="bert_large_weights",
        file_path="/models/bert-large-uncased.bin",
        chunk_size=10 * 1024 * 1024
    )
```

### 场景 4：并行传输大文件（高速网络）

```python
from flaxkv2.utils.file_transfer import (
    upload_large_file_parallel,
    download_large_file_parallel
)

# 在高速网络环境（如数据中心内部）使用并行传输
with FlaxKV("large_files", "tcp://high-speed-server:5555",
           enable_encryption=True, password="secret") as db:

    # 并行上传 5GB 文件（使用 8 个线程）
    info = upload_large_file_parallel(
        db,
        key="large_dataset",
        file_path="/data/dataset.tar.gz",
        chunk_size=20 * 1024 * 1024,  # 20MB 分块
        max_workers=8  # 8 个并行线程
    )

    print(f"上传完成: {info['filename']}, 速度更快！")
```

```python
# 并行下载
with FlaxKV("large_files", "tcp://high-speed-server:5555",
           enable_encryption=True, password="secret") as db:

    # 并行下载文件
    info = download_large_file_parallel(
        db,
        key="large_dataset",
        output_path="./downloads/",
        max_workers=8,
        verify=True  # 确保数据完整性
    )
```

## 📊 性能对比

| 文件大小 | 传统方式 | 分块传输 | 优势 |
|---------|---------|---------|------|
| 10MB | ✅ 推荐 | ⚠️ 开销大 | 传统方式更快 |
| 100MB | ⚠️ 可能超时 | ✅ 推荐 | 避免超时 |
| 500MB | ❌ 内存不足 | ✅ 推荐 | 内存占用低 |
| 1GB+ | ❌ 不支持 | ✅ 推荐 | 唯一选择 |

**内存占用对比：**
- 传统方式：文件大小 × 2-3（序列化开销）
- 分块传输：分块大小 × 2（10MB × 2 = 20MB）

**传输时间估算（100Mbps 网络）：**
- 100MB 文件：约 10 秒
- 1GB 文件：约 100 秒
- 10GB 文件：约 17 分钟

## ⚙️ 配置建议

### 分块大小选择

| 网络环境 | 推荐分块大小 | 原因 |
|---------|------------|------|
| 局域网（1Gbps+） | 20-50MB | 减少网络请求次数 |
| 互联网（100Mbps） | 10-20MB | 平衡传输效率和可靠性 |
| 移动网络（10Mbps） | 5-10MB | 避免网络波动导致超时 |
| 不稳定网络 | 1-5MB | 降低重传成本 |

### 并行线程数选择

| 网络环境 | 推荐线程数 | 原因 |
|---------|----------|------|
| 局域网（1Gbps+） | 8-10 | 充分利用高带宽 |
| 互联网（100Mbps） | 5-8 | 平衡速度和稳定性 |
| 互联网（10Mbps） | 3-5 | 避免过多并发导致拥塞 |
| 不稳定网络 | 1-3 | 优先使用串行传输 |

**注意事项**：
- 线程数过多可能导致网络拥塞，反而降低速度
- 服务器性能也会影响最佳线程数
- 建议根据实际测试调整线程数

### 内存限制

```python
# 内存受限环境（如树莓派）
upload_large_file(
    db, key, file_path,
    chunk_size=5 * 1024 * 1024  # 5MB 分块
)

# 高性能服务器
upload_large_file(
    db, key, file_path,
    chunk_size=50 * 1024 * 1024  # 50MB 分块
)
```

## 🔒 安全注意事项

1. **使用加密传输：**
   ```python
   db = FlaxKV("files", "tcp://server:5555",
              enable_encryption=True,
              password="strong_password")
   ```

2. **启用哈希验证：**
   ```python
   upload_large_file(db, key, file_path, verify=True)
   download_large_file(db, key, output, verify=True)
   ```

3. **访问控制：**
   - 服务器绑定到 127.0.0.1（仅本地访问）
   - 或使用防火墙限制访问
   - 或通过 VPN/SSH 隧道连接

## 🐛 故障排除

### 问题 1：上传/下载超时

**原因：** 分块太大或网络不稳定

**解决：**
```python
# 减小分块大小
upload_large_file(
    db, key, file_path,
    chunk_size=5 * 1024 * 1024  # 从 10MB 减小到 5MB
)

# 或增加超时时间
db = FlaxKV("files", "tcp://server:5555",
           timeout=60000)  # 60 秒超时
```

### 问题 2：哈希值验证失败

**原因：** 文件在传输过程中损坏

**解决：**
```python
# 重新上传文件
delete_large_file(db, key)
upload_large_file(db, key, file_path, verify=True)
```

### 问题 3：内存不足

**原因：** 分块太大

**解决：**
```python
# 使用更小的分块
upload_large_file(
    db, key, file_path,
    chunk_size=2 * 1024 * 1024  # 2MB 分块
)
```

## 📝 完整示例

查看 `examples/large_file_transfer.py` 获取完整的使用示例：

```bash
# 运行示例
python examples/large_file_transfer.py
```

## 🔮 未来特性（计划中）

- ✨ 断点续传支持
- ✨ 并行上传多个分块
- ✨ 自适应分块大小
- ✨ 压缩传输（自动检测文件类型）
- ✨ 增量更新（差分传输）

## 💡 最佳实践

1. **始终使用 `with` 语句：**
   ```python
   with FlaxKV(...) as db:
       upload_large_file(db, ...)
   # 自动关闭连接
   ```

2. **启用哈希验证：**
   ```python
   upload_large_file(db, key, file_path, verify=True)
   download_large_file(db, key, output, verify=True)
   ```

3. **根据网络环境选择传输模式：**

   **串行传输**（稳定优先）：
   ```python
   # 适用于网络带宽有限、不稳定的场景
   upload_large_file(db, key, file_path, chunk_size=10*1024*1024)
   download_large_file(db, key, output_path)
   ```

   **并行传输**（速度优先）：
   ```python
   # 适用于高速网络、服务器性能充足的场景
   upload_large_file_parallel(
       db, key, file_path,
       chunk_size=20*1024*1024,
       max_workers=8  # 根据网络环境调整
   )
   download_large_file_parallel(
       db, key, output_path,
       max_workers=8
   )
   ```

4. **根据网络环境调整参数：**

   | 网络环境 | 传输模式 | 分块大小 | 线程数 |
   |---------|---------|---------|-------|
   | 局域网（1Gbps+） | 并行 | 20-50MB | 8-10 |
   | 互联网（100Mbps） | 并行 | 10-20MB | 5-8 |
   | 互联网（10Mbps） | 串行 | 5-10MB | - |
   | 不稳定网络 | 串行 | 1-5MB | - |

5. **定期清理过期文件：**
   ```python
   # 删除不需要的文件
   delete_large_file(db, "old_backup")
   ```

6. **监控磁盘空间：**
   ```python
   # 检查文件列表
   files = list_large_files(db, show_details=True)
   total_size = sum(meta['size'] for _, meta in files)
   print(f"总占用: {format_size(total_size)}")
   ```

7. **并行传输注意事项：**
   - 线程数不是越多越好，建议不超过 10 个
   - 并行下载时，所有分块会先下载到内存，再顺序写入文件
   - 确保服务器和客户端都有足够的性能
   - 始终启用 `verify=True` 确保数据完整性

## 📞 获取帮助

- 查看示例：`examples/large_file_transfer.py`
- API 文档：`flaxkv2/utils/file_transfer.py`
- 问题反馈：https://github.com/anthropics/flaxkv2/issues
